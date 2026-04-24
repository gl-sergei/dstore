/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 *
 * ---------------------------------------------------------------------------------------
 *
 * dstore_wal_buffer.cpp
 *
 * Description:
 * src/wal/dstore_wal_buffer.cpp
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include <cstdint>

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual";
#endif
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#include "framework/dstore_instance.h"
#include "common/log/dstore_log.h"
#include "wal/dstore_wal_perf_statistic.h"
#include "wal/dstore_wal_logstream.h"
#include "wal/dstore_wal_buffer.h"
#ifdef DSTORE_USE_NUMA
#include "numa.h"
#endif

namespace DSTORE {
#ifdef UT
constexpr uint8 INSERT_STATUS_ENTRY_POW{16};
#else
constexpr uint8 INSERT_STATUS_ENTRY_POW{24};
#endif
constexpr uint32 INSERT_STATUS_ENTRY_CNT{1 << INSERT_STATUS_ENTRY_POW};
constexpr uint32 INSERT_STATUS_ENTRY_LAST_INDEX{INSERT_STATUS_ENTRY_CNT - 1};

static_assert(INSERT_STATUS_ENTRY_CNT < INT32_MAX, "wal buffer entry num is bigger than INT32_MAX!");

WalStreamBuffer::WalStreamBuffer(DstoreMemoryContext memoryContext, uint32 blockCount)
    : m_walBufferCtl({0, {0}, blockCount, nullptr}),
    m_walBufferInsertState({0, {0}, nullptr}),
    m_maxContinuousPlsn(0),
    m_lastScannedPlsn(0),
    m_bufferSize(static_cast<uint64>(blockCount) * WAL_BLCKSZ),
    m_memoryContext(memoryContext),
    m_alignedBufferSize(0),
    m_maxReserveSize(m_bufferSize - WAL_FILE_HDR_SIZE),
    m_walFileSize(0)
{
    m_insertCtl.Init();
}

WalStreamBuffer::~WalStreamBuffer()
{
    Clear();
}

RetStatus WalStreamBuffer::Init(uint64 lastEndPlsn, uint64 walFileSize)
{
    StorageAssert(m_memoryContext != nullptr);
    StorageAssert(m_walBufferCtl.blockPtrArr == nullptr);
    StorageAssert(m_walBufferInsertState.insertStateEntrys == nullptr);
    StorageAssert(m_insertCtl.walInsertLocks == nullptr);
    StorageAssert(walFileSize > WAL_FILE_HDR_SIZE);
    m_walFileSize = walFileSize;

    /* Step 1: malloc and init WalBufferCtlData */
    RetStatus retStatus = InitBufferCtlData(lastEndPlsn);
    if (STORAGE_FUNC_FAIL(retStatus)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Init buffer ctl data fail!"));
        return DSTORE_FAIL;
    }
    /* Step 2: malloc and init CtlInsert */
    m_insertCtl.endBytePos.store(WalPlsnToBytePos(lastEndPlsn), std::memory_order_release);

    if (InitInsertLocks() != DSTORE_SUCC) {
        Clear();
        return DSTORE_FAIL;
    }

    /* Step 3: malloc and init WalBufferInsertState */
    int32 retryCnt = 0;
    constexpr int32 retryMax = 1000;
    constexpr long sleepTime = 1000;
    uint32 entrySize = sizeof(WalBufferInsertStateEntry) * INSERT_STATUS_ENTRY_CNT;
RETRY:
    StorageExit0(retryCnt++ > retryMax, MODULE_WAL, ErrMsg("No memory WalStreamBuffer::Init, wait for next term."));
    m_walBufferInsertState.insertStateEntrys = (WalBufferInsertStateEntry *)DstorePallocAlignedHugeMemory(
        entrySize, DSTORE_CACHELINE_SIZE, m_memoryContext);
    if (STORAGE_VAR_NULL(m_walBufferInsertState.insertStateEntrys)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Alloc insertStateEntrys fail(%u).", entrySize));
        GaussUsleep(sleepTime);
        goto RETRY;
    }
    errno_t rc =
        memset_s(m_walBufferInsertState.insertStateEntrys, sizeof(WalBufferInsertStateEntry) * INSERT_STATUS_ENTRY_CNT,
                 0, sizeof(WalBufferInsertStateEntry) * INSERT_STATUS_ENTRY_CNT);
    storage_securec_check(rc, "\0", "\0");
    m_maxContinuousPlsn.store(lastEndPlsn, std::memory_order_release);
    m_lastScannedPlsn = lastEndPlsn;

    return DSTORE_SUCC;
}

RetStatus WalStreamBuffer::InitPlsn(uint64 lastEndPlsn, WalStream *walStream)
{
    if (lastEndPlsn == 0) {
        return DSTORE_SUCC;
    }
    uint64 lastSegHeaderStart = (lastEndPlsn / m_walFileSize) * m_walFileSize;
    uint64 lastSegHeaderEnd = lastSegHeaderStart + WAL_FILE_HDR_SIZE;
    if (unlikely(lastEndPlsn > lastSegHeaderStart && lastEndPlsn < lastSegHeaderEnd)) {
        /* lastEndPlsn is in segment file header, error */
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("lastEndPlsn :%lu is in segment file header when wal_buffer Init.",
            lastEndPlsn));
        return DSTORE_FAIL;
    }

    InitWalBufferCtrl(lastEndPlsn);
    m_insertCtl.endBytePos.store(WalPlsnToBytePos(lastEndPlsn), std::memory_order_release);
    m_maxContinuousPlsn.store(lastEndPlsn, std::memory_order_release);
    m_lastScannedPlsn = lastEndPlsn;
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("InitPlsn lastEndPlsn:%lu endBytePos:%lu.",
        lastEndPlsn, m_insertCtl.endBytePos.load(std::memory_order_acquire)));
    /* In DIO R/W, we need to init last wal block by read from disk, to ensure not overwrite wal in last block */
    if (walStream->IsDioReadWrite() && STORAGE_FUNC_FAIL(InitLastBlock(walStream, lastEndPlsn))) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus WalStreamBuffer::InitLastBlock(WalStream *walStream, uint64 lastEndPlsn)
{
    if (unlikely(lastEndPlsn % WAL_BLCKSZ) == 0) {
        return DSTORE_SUCC;
    }

    uint64 lastBlockStartPlsn = (lastEndPlsn / WAL_BLCKSZ) * WAL_BLCKSZ;
    WalFile *walFile = walStream->GetWalFileManager()->GetWalFileByPlsn(lastBlockStartPlsn);
    if (walFile == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("InitLastBlock failed for find WalFile for lastBlock:%lu failed", lastBlockStartPlsn));
        return DSTORE_FAIL;
    }
    uint8 *lastBlockBuf = GetBufferBlock(lastBlockStartPlsn);
    uint64 offset = (lastEndPlsn % walStream->GetWalFileSize()) / WAL_BLCKSZ * WAL_BLCKSZ;
    if (unlikely(offset > UINT32_MAX)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("InitLastBlock failed for invalid offset:%lu walFileSize:%lu", offset, walStream->GetWalFileSize()));
        return DSTORE_FAIL;
    }
    int64 readSize = 0;
    uint16 dataLen = lastEndPlsn % WAL_BLCKSZ;
    WalDioReadAdaptor dioReadAdaptor{m_memoryContext, WAL_BLCKSZ};
    if (walStream->IsDioReadWrite()) {
        if (STORAGE_FUNC_FAIL(dioReadAdaptor.Read(lastBlockBuf, WAL_BLCKSZ, walFile, offset, &readSize))) {
            ErrLog(DSTORE_WARNING, MODULE_WAL,
                ErrMsg("InitLastBlock failed for WalFile StartPlsn:%lu offset:%lu readLen:%d dio read failed",
                    walFile->GetStartPlsn(), offset, WAL_BLCKSZ));
        }
    } else {
        if (STORAGE_FUNC_FAIL(walFile->Read(lastBlockBuf, WAL_BLCKSZ, offset, &readSize))) {
            ErrLog(DSTORE_WARNING, MODULE_WAL,
                ErrMsg("InitLastBlock failed for WalFile StartPlsn:%lu offset:%lu readLen:%d read failed",
                    walFile->GetStartPlsn(), offset, WAL_BLCKSZ));
            return DSTORE_FAIL;
        }
    }
    if (readSize < dataLen) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("InitLastBlock failed for WalFile StartPlsn:%lu offset:%lu readLen:%d > actual readLen:%ld",
                walFile->GetStartPlsn(), offset, WAL_BLCKSZ, readSize));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus WalStreamBuffer::InitInsertLocks()
{
#ifdef DSTORE_USE_NUMA
    int numaNum = g_storageInstance->GetGuc()->numaNodeNum;
    StorageAssert(numaNum != 0);

    int32 retryCnt = 0;
    constexpr int32 retryMax = 1000;
    constexpr long sleepTime = 1000;
    uint32 lockGroupPtrSize = numaNum * sizeof(WalInsertLock *);
RETRY1:
    StorageExit0(retryCnt++ > retryMax, MODULE_WAL,
        ErrMsg("No memory WalStreamBuffer::InitInsertLocks, wait for next term."));
    WalInsertLock **insertLockGroupPtr = (WalInsertLock **)DstorePallocAligned(lockGroupPtrSize, DSTORE_CACHELINE_SIZE);
    if (STORAGE_VAR_NULL(insertLockGroupPtr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
               ErrMsg("Alloc insertLockGroupPtr fail(%u). retry %d times", lockGroupPtrSize, retryCnt));
        GaussUsleep(sleepTime);
        goto RETRY1;
    }

    size_t allocSize = sizeof(WalInsertLock) * GROUP_INSERT_LOCKS_COUNT;
    for (int i = 0; i < numaNum; i++) {
        retryCnt = 0;
    RETRY2:
        StorageExit0(retryCnt++ > retryMax, MODULE_WAL,
                     ErrMsg("No memory WalStreamBuffer::InitInsertLocks, wait for next term."));
        char *pInsertLock = (char *)numa_alloc_onnode(allocSize, i);
        if (STORAGE_VAR_NULL(pInsertLock)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL,
                   ErrMsg("WALShmemInit could not alloc memory on node %d. retry %d times.", i, retryCnt));
            GaussUsleep(sleepTime);
            goto RETRY2;
        }
        insertLockGroupPtr[i] = reinterpret_cast<WalInsertLock *>(pInsertLock);
    }

    for (int processorIndex = 0; processorIndex < numaNum; processorIndex++) {
        for (int i = 0; i < GROUP_INSERT_LOCKS_COUNT; i++) {
            GsAtomicInitU32(&insertLockGroupPtr[processorIndex][i].walGroupFirst, INVALID_THREAD_CORE_ID);
        }
    }
    m_insertCtl.walInsertLocks = insertLockGroupPtr;
    return DSTORE_SUCC;
#else
    return DSTORE_SUCC;
#endif
}

void WalStreamBuffer::InitWalBufferCtrl(uint64 lastEndPlsn)
{
    /* init bufferBlockArr */
    uint64 bufferAlign = (lastEndPlsn / m_bufferSize) * m_bufferSize;

    m_walBufferCtl.maxFlushedPlsn = bufferAlign;
}

RetStatus WalStreamBuffer::InitBufferCtlData(uint64 lastEndPlsn)
{
    m_alignedBufferSize = ((m_bufferSize - 1) / WAL_DIO_BLOCK_SIZE + 1) * WAL_DIO_BLOCK_SIZE;
    set_use_huge_page(false);
    int32 retryCnt = 0;
    constexpr int32 retryMax = 1000;
    constexpr long sleepTime = 1000;
RETRY:
    StorageExit0(retryCnt++ > retryMax, MODULE_WAL,
                 ErrMsg("No memory WalStreamBuffer::InitBufferCtlData, wait for next term."));
    m_walBufferCtl.blockPtrArr =
        static_cast<uint8 *>(DstorePallocAlignedHugeMemory(m_alignedBufferSize, WAL_DIO_BLOCK_SIZE, m_memoryContext));
    if (STORAGE_VAR_NULL(m_walBufferCtl.blockPtrArr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
               ErrMsg("alloc insertStateEntrys fail(%lu). retry %d times.", m_alignedBufferSize, retryCnt));
        GaussUsleep(sleepTime);
        goto RETRY;
    }
    set_use_huge_page(true);
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("InitBufferCtlData m_alignedBufferSize %lu, not use huge page", m_alignedBufferSize));
    errno_t rc = memset_s(m_walBufferCtl.blockPtrArr, m_alignedBufferSize, 0, m_alignedBufferSize);
    storage_securec_check(rc, "\0", "\0");
    InitWalBufferCtrl(lastEndPlsn);
    return DSTORE_SUCC;
}

void WalStreamBuffer::Clear() noexcept
{
    if (m_walBufferCtl.blockPtrArr != nullptr) {
        DstorePfreeAligned(m_walBufferCtl.blockPtrArr);
        m_walBufferCtl.blockPtrArr = nullptr;
    }
    DstorePfreeAligned(m_walBufferInsertState.insertStateEntrys);
    m_memoryContext = nullptr;
}

void WalStreamBuffer::ReserveInsertByteLocation(uint32 reserveSize, WalBufferInsertPos &insertPos)
{
    /* Wal group size need less than buffer size */
    if (unlikely(reserveSize > m_maxReserveSize)) {
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Wal size :%u is bigger than buffer size.", reserveSize));
    }
    insertPos.startBytePos = m_insertCtl.endBytePos.fetch_add(reserveSize, std::memory_order_acq_rel);
    insertPos.endBytePos = insertPos.startBytePos + reserveSize;
}

void WalStreamBuffer::ReserveInsertLocation(uint32 size, uint64 &startPlsn, uint64 &endPlsn)
{
    WalBufferInsertPos insertPos = {};
    ReserveInsertByteLocation(size, insertPos);

    startPlsn = WalBytePosToPlsn(insertPos.startBytePos, false);
    endPlsn = WalBytePosToPlsn(insertPos.endBytePos, true);
}

void WalStreamBuffer::SetInsertCtl(uint64 endPlsn)
{
    uint64 curEndBytePos;
    uint64 newEndBytePos = WalPlsnToBytePos(endPlsn);
    do {
        curEndBytePos = m_insertCtl.endBytePos.load(std::memory_order_acquire);
    } while (curEndBytePos < newEndBytePos &&
             !(m_insertCtl.endBytePos.compare_exchange_weak(curEndBytePos, newEndBytePos)));
}

uint8 *WalStreamBuffer::GetBufferBlock(uint64 plsn)
{
    uint32 idx = PlsnToBufferBlockIndex(plsn);
    return m_walBufferCtl.blockPtrArr + idx * static_cast<Size>(WAL_BLCKSZ) + plsn % WAL_BLCKSZ;
}

void WalStreamBuffer::MarkInsertFinish(uint64 startPlsn, uint64 endPlsn)
{
    uint32 retryTime = 0;
    const uint32 MAX_RETRY_TIMES = 1000;
    while (m_maxContinuousPlsn.load(std::memory_order_acquire) + INSERT_STATUS_ENTRY_CNT <= startPlsn) {
        std::this_thread::yield();
        if (++retryTime == MAX_RETRY_TIMES) {
            ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("MarkInsertFinish wait startPlsn %lu maxContinuousPlsn %lu ",
                startPlsn, m_maxContinuousPlsn.load()));
            GaussUsleep(1);
            retryTime = 0;
        }
    }

    if (startPlsn % m_walFileSize == WAL_FILE_HDR_SIZE) {
        /*
         * Because WalUtils::WalBytePosToPlsn performed special transform for endPlsn when endPlsn exactly
         * is end of walFile, the end of walFile to the header of next walFile is discontinuous.
         * In order to solve this problem,we manually fill the value to keep ring's continuous.
         */
        uint32 entryIndex = (startPlsn - WAL_FILE_HDR_SIZE) & INSERT_STATUS_ENTRY_LAST_INDEX;
        m_walBufferInsertState.insertStateEntrys[entryIndex].endPlsn.store(startPlsn, std::memory_order_release);
    }
    uint32 entryIndex = startPlsn & INSERT_STATUS_ENTRY_LAST_INDEX;
    m_walBufferInsertState.insertStateEntrys[entryIndex].endPlsn.store(endPlsn, std::memory_order_release);
}

void WalStreamBuffer::GetNextFlushData(uint64 &startPlsn, uint64 &endPlsn, uint8 *&data, bool &reachBufferTail)
{
    StorageAssert(m_walBufferInsertState.insertStateEntrys != nullptr);
    reachBufferTail = false;
    uint64 endBound;

    startPlsn = m_lastScannedPlsn;
    endPlsn = m_lastScannedPlsn;
    endBound = GetMaxContinuesPlsn();
    if (endBound == startPlsn) {
        endPlsn = startPlsn;
        data = nullptr;
        return;
    }
    uint64 currentBufferEndPlsn = (m_lastScannedPlsn / m_bufferSize + 1) * m_bufferSize;
    if (endBound >= currentBufferEndPlsn) {
        endBound = currentBufferEndPlsn;
        reachBufferTail = true;
    }

    /* Step 3: calculate out prameter */
    if (unlikely(m_lastScannedPlsn > endBound)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
               ErrMsg("GetNextFlushData m_lastScannedPlsn:%lu > endBound:%lu", m_lastScannedPlsn, endBound));
    }
    endPlsn = endBound;
    data = GetBufferBlock(m_lastScannedPlsn);
    m_lastScannedPlsn = endBound;
}

uint64 WalStreamBuffer::WalBytePosToPlsn(uint64 bytepos, bool isEnd) const
{
    if (isEnd) {
        return WalUtils::WalBytePosToPlsn<true>(m_walFileSize, bytepos);
    } else {
        return WalUtils::WalBytePosToPlsn<false>(m_walFileSize, bytepos);
    }
}

uint64 WalStreamBuffer::WalPlsnToBytePos(uint64 plsn) const
{
    return WalUtils::WalPlsnToBytePos(m_walFileSize, plsn);
}

uint64 WalStreamBuffer::GetBufferSize() const
{
    return m_bufferSize;
}

void WalStreamBuffer::GetFinalInsertLocation(uint64 &endPlsn)
{
    uint64 bytePos = m_insertCtl.endBytePos.load(std::memory_order_acquire);
    endPlsn = WalBytePosToPlsn(bytePos, true);
}

void *WalStreamBuffer::GetBufferPtr() const
{
    return m_walBufferCtl.blockPtrArr;
}

inline uint32 WalStreamBuffer::GetNextEntryIndex(uint32 entryIndex)
{
    return ((entryIndex) + 1) & INSERT_STATUS_ENTRY_LAST_INDEX;
}

inline uint32 WalStreamBuffer::PlsnToBufferBlockIndex(uint64 plsn) const
{
    return static_cast<uint32>((plsn / WAL_BLCKSZ) % m_walBufferCtl.blockCount);
}

uint64 WalStreamBuffer::GetMaxContinuesPlsn()
{
    uint64 maxContinuousPlsn = m_maxContinuousPlsn.load(std::memory_order_acquire);
    volatile WalBufferInsertStateEntry *startEntryPtr =
        &m_walBufferInsertState.insertStateEntrys[maxContinuousPlsn & INSERT_STATUS_ENTRY_LAST_INDEX];

    while (startEntryPtr->endPlsn.load(std::memory_order_acquire) > maxContinuousPlsn) {
        maxContinuousPlsn = startEntryPtr->endPlsn;
        startEntryPtr = &m_walBufferInsertState.insertStateEntrys[maxContinuousPlsn & INSERT_STATUS_ENTRY_LAST_INDEX];
    }
    m_maxContinuousPlsn.store(maxContinuousPlsn, std::memory_order_release);
    return maxContinuousPlsn;
}
}  // namespace DSTORE
