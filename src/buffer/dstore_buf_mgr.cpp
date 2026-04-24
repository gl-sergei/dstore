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
 * dstore_buf_mgr.cpp
 *      This file implements the functionality of buffer manager.
 *
 * IDENTIFICATION
 *      src/buffer/dstore_buf_mgr.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <ctime>
#include <thread>

#include "buffer/dstore_bg_page_writer_mgr.h"
#include "buffer/dstore_buf_refcount.h"
#include "config/dstore_vfs_config.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "framework/dstore_vfs_adapter.h"
#include "errorcode/dstore_buf_error_code.h"
#include "errorcode/dstore_framework_error_code.h"
#include "diagnose/dstore_lwlock_diagnose.h"
#include "diagnose/dstore_buf_mgr_diagnose.h"
#include "port/dstore_port.h"
#include "common/log/dstore_log.h"
#include "wal/dstore_wal.h"
#include "wal/dstore_wal_write_context.h"
#include "buffer/dstore_buf_perf_unit.h"
#include "transaction/dstore_transaction.h"
#include "page/dstore_index_page.h"
#include "framework/dstore_instr_time.h"
#include "buffer/dstore_buf_mgr.h"
#include "buffer/dstore_checkpointer.h"
#include "common/fault_injection/dstore_heap_fault_injection.h"
#include "fault_injection/fault_injection.h"
#include "common/error/dstore_error.h"

namespace DSTORE {

constexpr uint32 FREE_BUFFER_RETRY_TIMES = 32;
constexpr uint32 INVALIDATE_BUFFER_RETRY_TIMES = 65536;
constexpr uint32 RETRY_WARNING_THRESHOLD = 32;
constexpr uint32 RETRY_FLUSH_DEBUG_THRESHOLD = 20;
constexpr uint32 FLUSH_THREAD_NUM = 10;
constexpr uint32 INVALIDATE_THREAD_NUM = 10;
constexpr uint32 WAIT_COUNT_FOR_REPORT_WARNING = 1000;

char* BufMgr::GetBufferHitRatio()
{
    StringInfoData dumpHitRatioInfo;
    if (unlikely(!dumpHitRatioInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for BufferHitRatio dump info."));
        return nullptr;
    }

    uint64 allocCnt = m_bufPoolCnts.allocCnt.load(std::memory_order_seq_cst);
    uint64 loadFromDiskCnt = m_bufPoolCnts.loadFromDiskCnt.load(std::memory_order_acquire);
    uint64 flushCnt = m_bufPoolCnts.flushCnt.load(std::memory_order_acquire);
    uint64 mismatchCnt = m_bufPoolCnts.mismatchCnt.load(std::memory_order_acquire);
    uint64 matchCnt = m_bufPoolCnts.matchCnt.load(std::memory_order_acquire);

    uint64 crAllocCnt = m_bufPoolCnts.crAllocCnt.load(std::memory_order_seq_cst);
    uint64 crReuseSlotCnt = m_bufPoolCnts.crReuseSlotCnt.load(std::memory_order_acquire);
    uint64 crBuildCnt = m_bufPoolCnts.crBuildCnt.load(std::memory_order_acquire);
    uint64 crMismatchCnt = m_bufPoolCnts.crMismatchCnt.load(std::memory_order_acquire);
    uint64 crMatchCnt = m_bufPoolCnts.crMatchCnt.load(std::memory_order_acquire);

    uint64 baseBufferReqCnt = mismatchCnt + matchCnt;
    uint64 crBufferReqCnt = crMismatchCnt + crMatchCnt;

    dumpHitRatioInfo.AppendString("Buffer Manager Summary:\n");
    AppendCacheSummary(&dumpHitRatioInfo, "base buffer request", baseBufferReqCnt);
    AppendCacheSummary(&dumpHitRatioInfo, "alloc", allocCnt, baseBufferReqCnt);
    AppendCacheSummary(&dumpHitRatioInfo, "read disk", loadFromDiskCnt, baseBufferReqCnt);
    AppendCacheSummary(&dumpHitRatioInfo, "flush operation", flushCnt, baseBufferReqCnt);
    AppendCacheSummary(&dumpHitRatioInfo, "buffer mismatch", mismatchCnt, baseBufferReqCnt);
    AppendCacheSummary(&dumpHitRatioInfo, "buffer match", matchCnt, baseBufferReqCnt);

    dumpHitRatioInfo.AppendString("\n");
    AppendCacheSummary(&dumpHitRatioInfo, "cr buffer request", crBufferReqCnt);
    AppendCacheSummary(&dumpHitRatioInfo, "cr alloc", crAllocCnt, crBufferReqCnt);
    AppendCacheSummary(&dumpHitRatioInfo, "cr buffer mismatch", crMismatchCnt, crBufferReqCnt);
    AppendCacheSummary(&dumpHitRatioInfo, "cr buffer match", crMatchCnt, crBufferReqCnt);
    AppendCacheSummary(&dumpHitRatioInfo, "cr reuse", crReuseSlotCnt);
    AppendCacheSummary(&dumpHitRatioInfo, "cr build", crBuildCnt);

    dumpHitRatioInfo.AppendString("\n");
    AppendCacheSummary(&dumpHitRatioInfo, "LRU partitions", m_lru_partitions);
    dumpHitRatioInfo.AppendString("-----------------------------------------------------\n");

    StringInfo lruSummary = m_buffer_lru->DumpLruSummary();
    StorageAssert(lruSummary != nullptr);
    dumpHitRatioInfo.AppendString(lruSummary->get_bytes(lruSummary->len));
    DstorePfree(lruSummary->data);
    DstorePfreeExt(lruSummary);
    dumpHitRatioInfo.AppendString("-----------------------------------------------------\n");
    dumpHitRatioInfo.AppendString("\n");
    PrintMemChunkStatistics(dumpHitRatioInfo);

    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("%s", dumpHitRatioInfo.data));

    return dumpHitRatioInfo.data;
}

char* BufMgr::PrintBufferDescByTag(PdbId pdbId, FileId fileId, BlockNumber blockId)
{
    BufferTag bufTag{pdbId, {fileId, blockId}};

    m_bufferMemChunkList->LockBufferMemChunkList(LW_SHARED);
    BufferMemChunkWrapper<BufferMemChunk> *memChunkWrapper =
        m_bufferMemChunkList->GetMemChunkIterator<BufferMemChunk>();
    while (memChunkWrapper != nullptr) {
        memChunkWrapper->memChunk->LockMemChunk(LW_SHARED);
        for (Size i = 0; i < memChunkWrapper->memChunk->GetSize(); i++) {
            BufferDesc *bufferDesc = memChunkWrapper->memChunk->GetBufferDesc(i);
            bufferDesc->Pin();
            if ((bufferDesc->bufTag == bufTag) && !(bufferDesc->GetState(false) & Buffer::BUF_CR_PAGE)) {
                char *info = bufferDesc->PrintBufferDesc();
                bufferDesc->Unpin();
                memChunkWrapper->memChunk->UnlockMemChunk();
                m_bufferMemChunkList->UnlockBufferMemChunkList();
                return info;
            }
            bufferDesc->Unpin();
        }
        memChunkWrapper->memChunk->UnlockMemChunk();
        memChunkWrapper = memChunkWrapper->GetNext();
    }
    m_bufferMemChunkList->UnlockBufferMemChunkList();

    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("not find buffer bufTag:(%hhu, %hu, %u)",
        bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for BufferDesc dump info."));
        return nullptr;
    }
    dumpInfo.append("not find buftag (%hhu, %hu, %u)",
        bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId);
    return dumpInfo.data;
}

char* BufMgr::PrintLruInfo(BufferDesc *bufferDesc)
{
    const static char* lruListName[] = { "CANDIDATE", "LRU      ", "HOT      ", "PENDING  ", "UNEVICTABLE" };

    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for bufferDesc in lrulist dump info."));
        return nullptr;
    }

    uint64 state = bufferDesc->GetState(false);
    dumpInfo.append("Tag:{0x%01hhx, 0x%04hx, 0x%08x} Buffer:%p state: 0x%016lx IsCr:%s ListType:%s Usage:%hhu\n",
        bufferDesc->bufTag.pdbId, bufferDesc->bufTag.pageId.m_fileId, bufferDesc->bufTag.pageId.m_blockId,
        bufferDesc,
        state,
        state & Buffer::BUF_CR_PAGE ? "true " : "false",
        lruListName[bufferDesc->lruNode.m_type.load()],
        bufferDesc->lruNode.m_usage.load());
    bufferDesc->PrintBufferState(state, &dumpInfo);
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("%s", dumpInfo.data));

    return dumpInfo.data;
}

char *BufMgr::PrintBufferpoolStatistic()
{
    return GetBufferHitRatio();
}

char *BufMgr::ResetBufferpoolStatistic()
{
    m_bufPoolCnts.Initialize();
    StringInfoData str;
    if (unlikely(!str.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for Reset statistics info."));
        return nullptr;
    }
    str.append("Reset buffer pool statistics.");
    return str.data;
}

void BufMgr::PrintMemChunkStatistics(StringInfoData &str)
{
    m_bufferMemChunkList->LockBufferMemChunkList(LW_SHARED);
    BufferMemChunkWrapper<BufferMemChunk> **sortedMemChunkWrapperArray =
        m_bufferMemChunkList->SortByTemperature<BufferMemChunk>();
    if (STORAGE_VAR_NULL(sortedMemChunkWrapperArray)) {
        m_bufferMemChunkList->UnlockBufferMemChunkList();
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("sortedMemChunkWrapperArray is nullptr."));
        return;
    }
    uint64 totalHotPage = 0;
    uint64 totalIdlePage = 0;
    uint64 totalDirtyPage = 0;
    uint64 totalCrPage = 0;
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for dump info."));
        DstorePfreeExt(sortedMemChunkWrapperArray);
        m_bufferMemChunkList->UnlockBufferMemChunkList();
        return;
    }
    str.append("Memory chunks statistics:\n");
    str.append("Bufferpool size: %lu (blocks)\n", m_size);
    str.append("Memory chunk size: %lu (blocks)\n", m_bufferMemChunkList->GetNumOfBufInMemChunk());
    str.append("Number of chunks in total: %lu\n", m_bufferMemChunkList->GetSize());
    dumpInfo.append("The memory chunks are sorted in the order of coldest --> hottest:\n");
    for (Size i = 0; i < m_bufferMemChunkList->GetSize(); i++) {
        BufferMemChunk *memChunk = sortedMemChunkWrapperArray[i]->memChunk;
        dumpInfo.append("Memory chunk #%lu: hot pages = %u, idle pages = %u, dirty pages = %u\n",
                        memChunk->GetMemChunkId(), memChunk->GetHotPageCount(),
                        memChunk->GetIdlePageCount(), memChunk->GetDirtyPageCount());
        totalHotPage += memChunk->GetHotPageCount();
        totalIdlePage += memChunk->GetIdlePageCount();
        totalDirtyPage += memChunk->GetDirtyPageCount();
        totalCrPage += memChunk->GetCrPageCount();
    }
    str.append("Number of hot pages in total: %lu (blocks)\n", totalHotPage);
    str.append("Number of idle pages in total: %lu (blocks)\n", totalIdlePage);
    str.append("Number of dirty pages in total: %lu (blocks)\n", totalDirtyPage);
    str.append("Number of cr pages in total: %lu (blocks)\n", totalCrPage);
    str.AppendString(dumpInfo.data);
    str.append("(Note that the memory chunks statistics are estimates only)\n");
    DstorePfreeExt(dumpInfo.data);
    DstorePfreeExt(sortedMemChunkWrapperArray);
    m_bufferMemChunkList->UnlockBufferMemChunkList();
}

Size BufMgr::GetBufMgrSize() const
{
    return m_size;
}

Size BufMgr::GetLruPartition() const
{
    return m_lru_partitions;
}

double BufMgr::GetHotListRatio() const
{
    return BUFLRU_DEFAULT_HOT_RATIO;
}

BufLruList *BufMgr::GetBufLruList(Size queueIdx)
{
    StorageAssert(queueIdx < m_lru_partitions);
    return m_buffer_lru->GetLruListAt(queueIdx);
}

RetStatus BufMgr::GetLruSummary(LruInfo *lruInfo)
{
    LruInfo resultArr;
    for (Size i = 0; i < m_lru_partitions; i++) {
        LruCounters hotCounters = m_buffer_lru->GetLruListAt(i)->GetHotCounters();
        LruCounters lruCounters = m_buffer_lru->GetLruListAt(i)->GetLruCounters();
        LruCounters candidateCounters = m_buffer_lru->GetLruListAt(i)->GetCandidateCounters();

        /* Add the current count values of interest to the LRU summary. */
        resultArr.totalAddIntoHot += hotCounters.addIntoList;
        resultArr.totalRemoveFromHot += hotCounters.removeFromList;
        resultArr.totalAddIntoLru += lruCounters.addIntoList;
        resultArr.totalMoveWithinLru += lruCounters.moveWithinList;
        resultArr.totalRemoveFromLru += lruCounters.removeFromList;
        resultArr.totalAddIntoCandidate += candidateCounters.addIntoList;
        resultArr.totalRemoveFromCandidate += candidateCounters.removeFromList;
        resultArr.totalMissInCandidate += candidateCounters.missInList;
    }

    *lruInfo = resultArr;
    return DSTORE_SUCC;
}

uint64 BufMgr::GetBufferAllocCnt() const
{
    return m_bufPoolCnts.allocCnt;
}

HOTFUNCTION BufferDesc *BufMgr::LookupBuffer(const BufferTag &bufTag)
{
    LatencyStat::Timer timer(&BufPerfUnit::GetInstance().m_lookupBuffer);
    uint32 hashCode = m_buftable->GetHashCode(&bufTag);
    BufferDesc *bufferDesc = m_buftable->LookUp(&bufTag, hashCode);

    if (bufferDesc != INVALID_BUFFER_DESC) {
        StorageAssert(bufferDesc->GetBufferTag() == bufTag);
        m_buffer_lru->BufferAccessStat(bufferDesc);
#ifdef DSTORE_USE_ASSERT_CHECKING
        (void)++m_bufPoolCnts.matchCnt;
    } else {
        (void)++m_bufPoolCnts.mismatchCnt;
#endif
    }

    return bufferDesc;
}

BufferDesc *BufMgr::LookupBufferWithLock(const BufferTag &bufTag)
{
    LatencyStat::Timer timer(&BufPerfUnit::GetInstance().m_lookupBuffer);
    uint32 hashCode = m_buftable->GetHashCode(&bufTag);
    m_buftable->LockBufMapping(hashCode, LW_SHARED);
    BufferDesc *bufferDesc = m_buftable->LookUp(&bufTag, hashCode);
    m_buftable->UnlockBufMapping(hashCode);

    if (bufferDesc != INVALID_BUFFER_DESC) {
        StorageAssert(bufferDesc->GetBufferTag() == bufTag);
        m_buffer_lru->BufferAccessStat(bufferDesc);
#ifdef DSTORE_USE_ASSERT_CHECKING
        (void)++m_bufPoolCnts.matchCnt;
    } else {
        (void)++m_bufPoolCnts.mismatchCnt;
#endif
    }

    return bufferDesc;
}

BufferDesc *BufMgr::AllocBufferForBaseBuffer(const BufferTag &bufTag, bool& isValid, bool& otherInsertHash,
    BufferRing bufRing /* = nullptr */)
{
#ifdef DSTORE_USE_ASSERT_CHECKING
    (void) m_bufPoolCnts.allocCnt++;
#endif
    bool needRetry = true;
    BufferDesc* bufferDesc = INVALID_BUFFER_DESC;
    uint32 retryTimes = 0;
    for (;;) {
        AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
        bufferDesc = TryToReuseBufferForBasePage(bufTag, &isValid, &needRetry, &otherInsertHash, bufRing);
        if (bufferDesc != INVALID_BUFFER_DESC) {
            break;
        } else if (bufferDesc == INVALID_BUFFER_DESC && !needRetry) {
            return INVALID_BUFFER_DESC;
        }
        retryTimes++;
        if (retryTimes > RETRY_WARNING_THRESHOLD) {
            ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
                ErrMsg("after retry %u times, can not find free buffer for page bufTag:(%hhu, %hu, %u)",
                    retryTimes, bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
            retryTimes = 0;
        }
        /* clear the error */
        StorageClearError();
    }

    return bufferDesc;
}

static RetStatus TryOpenFile(VFSAdapter *vfs, BufferTag &bufTag)
{
    FileId fileId = bufTag.pageId.m_fileId;
    char fileName[MAXPGPATH] = {0};

    if (STORAGE_FUNC_FAIL(vfs->GetFileNameFromFileId(fileId, fileName))) {
        return DSTORE_FAIL;
    }

    RetStatus ret = DSTORE_SUCC;
    int reTryCnt = 0;
    do {
        ret = vfs->OpenFile(fileId, fileName,
            (USE_VFS_LOCAL_AIO ? DSTORE_FILE_ADIO_FLAG : DSTORE_FILE_OPEN_FLAG));
        ++reTryCnt;
    } while (reTryCnt <= MAX_OPEN_TIMES && STORAGE_FUNC_FAIL(ret));
    return ret;
}

/*
 * ReadBlock
 *
 * Return
 *  DSTORE_SUCC - read succeeded
 *  DSTORE_FAIL - read failed
 */
RetStatus BufMgr::ReadBlock(BufferDesc* bufferDesc)
{
    RetStatus rc = ReadBlock(bufferDesc->GetBufferTag(), bufferDesc->bufBlock);
    if (STORAGE_FUNC_FAIL(rc) || !IsAntiCacheOn()) {
        return rc;
    }

    /* AntiCache : storage fault check */
    Page* page = static_cast<Page *>(static_cast<void *>(bufferDesc->bufBlock));
    return AntiCacheHandleReadBlock(bufferDesc, page->GetGlsn(), page->GetPlsn());
}

RetStatus BufMgr::ReadBlock(BufferTag bufTag, BufBlock block)
{
    instr_time ioStart;
    instr_time ioTime;
    LatencyStat::Timer timer(&BufPerfUnit::GetInstance().m_readStat, false, PerfLevel::RELEASE, true);
    if (unlikely(m_isCount.load())) {
        m_readCount.fetch_add(1);
        timer.Start();
    }
#ifdef DSTORE_USE_ASSERT_CHECKING
    (void)++m_bufPoolCnts.loadFromDiskCnt;
#endif
    if (STORAGE_VAR_NULL(g_storageInstance->GetPdb(bufTag.pdbId))) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("storagePdb is nullptr."));
    }
    VFSAdapter *vfs = g_storageInstance->GetPdb(bufTag.pdbId)->GetVFS();
    StorageReleasePanic(vfs == nullptr, MODULE_BUFMGR, ErrMsg("vfs is nullptr, pdb %u", bufTag.pdbId));
    RetStatus ret = DSTORE_SUCC;
    int64_t startTime = 0;
    StorageStat *stat = g_storageInstance->GetStat();
    stat->ReportDataIOTimeRecord(&startTime, true);
READ:
    /* 1. read from vfs */
    PageId pageId = bufTag.pageId;
    if (unlikely(g_storageInstance->GetGuc()->enableTrackIOTiming)) {
        INSTR_TIME_SET_CURRENT(ioStart);
    }
    stat->m_reportWaitEvent(
        static_cast<uint32_t>(GsStatWaitEvent::WAIT_EVENT_BUFFER_READ_PAGE_SYNC));
    ret = vfs->ReadPageSync(pageId, block);
    stat->m_reportWaitEvent(static_cast<uint32_t>(OPTUTIL_GSSTAT_WAIT_EVENT_END));
    if (unlikely(g_storageInstance->GetGuc()->enableTrackIOTiming)) {
        INSTR_TIME_SET_CURRENT(ioTime);
        INSTR_TIME_SUBTRACT(ioTime, ioStart);
        stat->ReportBufferReadTime(ioTime);
    }
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrorCode err = StorageGetErrorCode();
        if (err == VFS_WARNING_FILE_NOT_OPENED) {
            ret = TryOpenFile(vfs, bufTag);
            if (STORAGE_FUNC_SUCC(ret)) {
                goto READ;
            }
        }
        return ret;
    }

    /* 2. check CRC */
    Page* page = static_cast<Page *>(static_cast<void *>(block));
    StorageReleasePanic(!page->CheckPageCrcMatch(), MODULE_BUFMGR, ErrMsg("read page bufTag:(%hhu, %hu, %u) "
        "check crc fail." PAGE_HEADER_FMT, bufTag.pdbId, pageId.m_fileId, pageId.m_blockId, PAGE_HEADER_VAL(page)));
#ifndef UT
    if (page->GetType() != PageType::INVALID_PAGE_TYPE && page->GetSelfPageId() != pageId) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("Reading invalid page from disk, bufTag:(%hhu, %hu, %u), "
            "page type:%d, page id:(%hu, %u) glsn(%lu) plsn(%lu) walId(%lu).",
            bufTag.pdbId, pageId.m_fileId, pageId.m_blockId, static_cast<int>(page->GetType()),
            page->GetSelfPageId().m_fileId, page->GetSelfPageId().m_blockId, page->GetGlsn(), page->GetPlsn(),
            page->GetWalId()));
    }
#endif
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("Buffer Status Observation: "
        "bufTag:(%hhu, %hu, %u) ReadBlock lsnInfo(walId %lu glsn %lu plsn %lu)",
        bufTag.pdbId, pageId.m_fileId, pageId.m_blockId, page->GetWalId(), page->GetGlsn(), page->GetPlsn()));
    stat->ReportDataIOTimeRecord(&startTime, false);
    return ret;
}

RetStatus BufMgr::GetBufDescPrintInfo(Size *length, char **errInfo, BufDescPrintInfo **bufferDescArr)
{
    BufferDesc *bufferDesc = nullptr;
    BufDescPrintInfo *resultArr = nullptr;
    Size totalSize = GetBufMgrSize();
    Size globalIndex = 0;
    StringInfoData dumpInfo;

    m_bufferMemChunkList->LockBufferMemChunkList(LW_SHARED);

    resultArr = (BufDescPrintInfo *)DstorePalloc(sizeof(BufDescPrintInfo) * totalSize);
    if (resultArr == nullptr) {
        *length = 0;    /* no output */
        m_bufferMemChunkList->UnlockBufferMemChunkList();
        if (unlikely(!dumpInfo.init())) {
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for result array and dump info."));
            *errInfo = nullptr;
            return DSTORE_FAIL;
        }
        dumpInfo.append("Allocating result array for buffer descriptors failed.");
        *errInfo = dumpInfo.data;
        return DSTORE_FAIL;
    }

    *length = totalSize;

    BufferMemChunkWrapper<BufferMemChunk> *currChunkWrapper =
        m_bufferMemChunkList->GetMemChunkIterator<BufferMemChunk>();
    while (currChunkWrapper != nullptr) {
        currChunkWrapper->memChunk->LockMemChunk(LW_SHARED);
        for (size_t index = 0; index < currChunkWrapper->memChunk->GetSize(); index++) {
            bufferDesc = currChunkWrapper->memChunk->GetBufferDesc(index);
            if (bufferDesc == INVALID_BUFFER_DESC) {
                *length = 0;    /* no output */
                DstorePfreeExt(resultArr);
                currChunkWrapper->memChunk->UnlockMemChunk();
                m_bufferMemChunkList->UnlockBufferMemChunkList();
                if (unlikely(!dumpInfo.init())) {
                    ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for error dump info "
                        "and Fetching buffer descriptor at index %lu failed.", globalIndex));
                    *errInfo = nullptr;
                    return DSTORE_FAIL;
                }
                dumpInfo.append("Fetching buffer descriptor at index %lu failed. \n", globalIndex);
                *errInfo = dumpInfo.data;
                return DSTORE_FAIL;
            }

            bufferDesc->Pin();

            StorageAssert(resultArr != nullptr);
            resultArr[globalIndex].Init(bufferDesc);

            bufferDesc->Unpin();
            globalIndex += 1;
        }
        currChunkWrapper->memChunk->UnlockMemChunk();
        currChunkWrapper = currChunkWrapper->GetNext();
    }

    m_bufferMemChunkList->UnlockBufferMemChunkList();

    *bufferDescArr = resultArr;
    return DSTORE_SUCC;
}

BufMgr::BufMgr(Size size, Size lruPartitions, Size maxSize)
    : m_size(size), m_maxSize(maxSize), m_buftable(nullptr), m_bufferMemChunkList(nullptr),
      m_buffer_lru(nullptr), m_lru_partitions(lruPartitions)
{
    m_antiCacheStat.antiCacheFlag = 0;
    LWLockInitialize(&m_sizeLock, LWLOCK_GROUP_BUF_MGR_SIZE);
    m_bufPoolCnts.Initialize();
}

BufMgr::~BufMgr()
{
    m_buftable = nullptr;
    m_bufferMemChunkList = nullptr;
    m_buffer_lru = nullptr;
}

/*
 * Initialize buffer pool
 *
 * This is called once during shared-memory initialization (either in the
 * postmaster, or in a standalone backend).
 */
RetStatus BufMgr::Init()
{
    AutoMemCxtSwitch autoSwitch{g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_BUFFER)};

    m_maxSize = (m_maxSize == 0) ? m_size : m_maxSize;

    /* Initialize buffer pool hash table. */
    m_buftable = DstoreNew(g_dstoreCurrentMemoryContext) BufTable(m_maxSize);
    if (STORAGE_VAR_NULL(m_buftable)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("alloc memory for buftable fail!"));
        return DSTORE_FAIL;
    }
    m_buftable->Initialize();

    /*
     * Initialize buffer memchunk list and LRU.
     * Note that the buffer pool size, the memchunk size or the number of LRU partitions may be adjusted downward
     * if the configured buffer pool size is not divisible by the memchunk size or the number of LRU partitions.
     */
    InitMemChunkAndLru();

    /* Shared-pin arena: singleton heap allocation used by BufferDesc pin/unpin
     * fast paths to avoid CAS contention on hot buffers. Safe to run after
     * memchunks; only Pin/Unpin paths read g_sharedPinArena and they tolerate
     * nullptr by falling back to the plain CAS loop. */
    Buffer::InitSharedPinArena();
    return DSTORE_SUCC;
}

void BufMgr::Destroy()
{
    /* check if it has buffer is not in lru list */
    (void)CheckForBufLruLeak(true);

    /* Call the destructor of each object in reverse order. */
    DestroyMemChunkAndLru();

    m_buftable->Destroy();
    delete m_buftable;

    Buffer::DestroySharedPinArena();
}

BufferDesc *BufMgr::Read(const PdbId &pdbId, const PageId &pageId, UNUSE_PARAM BufferPoolReadFlag flag,
                         BufferRing bufRing /* = nullptr */)
{
    if (pdbId == INVALID_PDB_ID || pdbId > PDB_MAX_ID) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Invalid pdbId: %u.", pdbId));
        return INVALID_BUFFER_DESC;
    }
    BufferTag bufTag = {pdbId, pageId};
    BufferDesc *bufferDesc = LookupBuffer(bufTag);
    bool isBufValid = false;
    bool otherInsertHash = false;
    /*
     * Step1: Try to reuse a buffer in the bufferpool.
     */
    if (bufferDesc == INVALID_BUFFER_DESC) {
        bufferDesc = AllocBufferForBaseBuffer(bufTag, isBufValid, otherInsertHash, bufRing);
#ifdef DSTORE_USE_ASSERT_CHECKING
        GS_MEMORY_BARRIER();
#endif
        StorageAssert(bufferDesc != INVALID_BUFFER_DESC);
    } else {
        isBufValid = static_cast<bool>(bufferDesc->GetState() & Buffer::BUF_VALID);
    }
    if (isBufValid) {
        thrd->m_bufferReadStat.bufferReadCount++;
        thrd->m_bufferReadStat.bufferReadHit++;
        return bufferDesc;
    }
    /*
     * Step2: if the buffer is not valid, load the page from storage layer into the buffer.
     */
    if (StartIo(bufferDesc, true)) {
        if (STORAGE_FUNC_FAIL(BufMgr::ReadBlock(bufferDesc))) {
            /* Failed to read a buffer from storage. */
            TerminateIo(bufferDesc, false, Buffer::BUF_IO_ERROR);
            bufferDesc->Unpin();
            return INVALID_BUFFER_DESC;
        }
        TerminateIo(bufferDesc, false, Buffer::BUF_VALID);
    }
    thrd->m_bufferReadStat.bufferReadCount++;
    return bufferDesc;
}

/*
 * read
 *
 * Read a page from the buffer table, or reuse a buffer and load the page from storage.
 *
 * Return
 *  a valid BufferDesc - read succeeded
 *  INVALID_BUFFER_DESC - read failed
 */
BufferDesc *BufMgr::Read(const PdbId &pdbId, const PageId &pageId, LWLockMode mode, UNUSE_PARAM BufferPoolReadFlag flag,
                         BufferRing bufRing /* = nullptr */)
{
    BufferDesc *bufferDesc = Read(pdbId, pageId, flag, bufRing);
    if (bufferDesc == INVALID_BUFFER_DESC) {
        return bufferDesc;
    }
    /*
     * the buffer we read must be a base buffer.
     */
    StorageAssert(!(bufferDesc->GetState(false) & Buffer::BUF_CR_PAGE));
    StorageAssert(bufferDesc->IsPinnedPrivately());
#ifdef DSTORE_USE_ASSERT_CHECKING
    if (bufferDesc->IsContentLocked(LW_SHARED) && (mode == LW_SHARED)) {
        const BufferTag bufTag = bufferDesc->GetBufferTag();
        ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
            ErrMsg("buffer bufTag:(%hhu, %hu, %u) is locked multiple times in share mode.",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
    }
#endif
    DstoreLWLockAcquireByMode(&bufferDesc->contentLwLock, mode)
    Page *page = bufferDesc->GetPage();
    PageType pageType = page->GetType();
    if (mode == LW_EXCLUSIVE) {
#ifdef DSTORE_USE_ASSERT_CHECKING
        page->SetChecksum();
#endif
        bufferDesc->UpdateLastModifyTimeToNow();
        bufferDesc->WaitIfIsWritingWal();
        bufferDesc->InvalidateCrPage();
    }
#ifndef UT
    if (pageType != PageType::INVALID_PAGE_TYPE && page->GetSelfPageId() != pageId) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("Reading invalid page, bufTag:(%hhu, %hu, %u)" PAGE_HEADER_FMT,
            pdbId, pageId.m_fileId, pageId.m_blockId, PAGE_HEADER_VAL(page)));
    }
#endif
    return bufferDesc;
}

RetStatus BufMgr::BatchCreateNewPage(const PdbId &pdbId, PageId *pageId, uint64 pageCount, BufferDesc **newBuffer,
                                     BufferRing bufRing /* = nullptr */)
{
    for (uint64 i = 0; i < pageCount; i++) {
        newBuffer[i] = BufMgr::Read(pdbId, pageId[i], LW_EXCLUSIVE, BufferPoolReadFlag(), bufRing);
    }

    return DSTORE_SUCC;
}

BufferDesc *BufMgr::ReadCr(BufferDesc *baseBufferDesc, Snapshot snapshot)
{
    if (STORAGE_VAR_NULL(baseBufferDesc)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("BufferDesc is nullptr."));
    }
    /* Acquire CR assign lock in SHARE mode */
    baseBufferDesc->AcquireCrAssignLwlock(LW_SHARED);
    /* Look up cr buffer */
    BufferDesc *crBufferDesc = FindCrBuffer(baseBufferDesc, snapshot);
    /* Release CR assign lock */
    baseBufferDesc->ReleaseCrAssignLwlock();

    /* Make sure CR buffer is valid */
    StorageAssert((crBufferDesc != INVALID_BUFFER_DESC) ? crBufferDesc->IsCrValid() : true);
    return crBufferDesc;
}

/*
 * If return INVALID_BUFFER_DESC, don't hold cr lock.
 */
BufferDesc *BufMgr::ReadOrAllocCr(BufferDesc *baseBufDesc, uint64 lastPageModifyTime,
                                  BufferRing bufRing /* = nullptr */)
{
    /* Step 1. Try to acquire CR assign lock in Exclusive mode. */
    if (!baseBufDesc->TryAcquireCrAssignLwlock(LW_EXCLUSIVE)) {
        return INVALID_BUFFER_DESC;
    }

    /* Step 2. Check if read/write interval exceed threshold. */
    uint64 now = static_cast<uint64>(time(nullptr));
    if (now < baseBufDesc->GetLastModifyTime() + TIMESTAMP_THRESHOLD_IN_CR) {
        baseBufDesc->ReleaseCrAssignLwlock();
        return INVALID_BUFFER_DESC;
    }

    /* Step 3. Check if the base page is modified by concurrent threads. */
    if (unlikely(baseBufDesc->GetLastModifyTime() != lastPageModifyTime)) {
        baseBufDesc->ReleaseCrAssignLwlock();
        return INVALID_BUFFER_DESC;
    }

    /* Step 4. Check if cr buffer is constructed by concurrent threads. */
    if (unlikely(baseBufDesc->IsCrUsable())) {
        baseBufDesc->ReleaseCrAssignLwlock();
        return INVALID_BUFFER_DESC;
    }

    /* Step 5. If we get here, we need alloc a cr buffer. */
    BufferDesc *crBufferDesc = AllocCrEntry(baseBufDesc, bufRing);
    if (crBufferDesc == INVALID_BUFFER_DESC) {
        /* This means origin cr buffer is pinned by other threads. */
        baseBufDesc->ReleaseCrAssignLwlock();
        return INVALID_BUFFER_DESC;
    }

    return crBufferDesc;
}

RetStatus BufMgr::ConsistentRead(ConsistentReadContext &crContext, BufferRing bufRing /* = nullptr */)
{
    return ConsistentReadInternal(crContext, false, bufRing);
}

RetStatus BufMgr::ConsistentReadInternal(ConsistentReadContext &crContext, bool needCheckPoFlag,
                                         BufferRing bufRing /* = nullptr */)
{
    /* Step 1: Read the base page by pageId. */
    BufferDesc *baseBufDesc = Read(crContext.pdbId, crContext.pageId, LW_SHARED, BufferPoolReadFlag(), bufRing);
    if (unlikely(baseBufDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFFER, ErrMsg("read base page(%hu, %u) failed!",
            crContext.pageId.m_fileId, crContext.pageId.m_blockId));
        return DSTORE_FAIL;
    }
    Page *basePage = baseBufDesc->GetPage();
    PageType pageType = basePage->GetType();
    if (unlikely(pageType != PageType::HEAP_PAGE_TYPE && pageType != PageType::INDEX_PAGE_TYPE)) {
        storage_set_error(BUFFER_INFO_CONSTRUCT_CR_NOT_DATA_PAGE);
        UnlockAndRelease(baseBufDesc);
        return DSTORE_FAIL;
    }

    /* The buffer always PO if not distirubted buffer pool */
    bool isPageOwner = needCheckPoFlag ? baseBufDesc->IsPageOwner() : true;
    bool isPoOrRa = (isPageOwner || baseBufDesc->HasReadAuth());
    /* Step 2: If there is a cached cr page that match with the snapshot, then return it directly. */
    if (isPoOrRa && pageType == PageType::INDEX_PAGE_TYPE) {
        crContext.crBufDesc = ReadCr(baseBufDesc, crContext.snapshot);
    }
    if (crContext.crBufDesc != INVALID_BUFFER_DESC) {
#ifdef DSTORE_USE_ASSERT_CHECKING
        ErrLog(DSTORE_DEBUG1, MODULE_BUFFER,
               ErrMsg("Can read matched cr page of base page(%d, %u) "
                      "according to snapshot.csn = %lu, page type is %hu",
                      baseBufDesc->GetPageId().m_fileId, baseBufDesc->GetPageId().m_blockId,
                      crContext.snapshot->GetCsn(), static_cast<uint16>(baseBufDesc->GetPage()->GetType())));
#endif
        UnlockAndRelease(baseBufDesc);
        return DSTORE_SUCC;
    }

    /* Step 3: Copy the base page to dest page. */
    errno_t rc = memcpy_s(static_cast<char*>(static_cast<void*>(crContext.destPage)),
                          BLCKSZ, basePage, BLCKSZ);
    StorageReleasePanic(rc != 0, MODULE_BUFFER, ErrMsg("memcpy base page to cr page failed!"));

    uint64 lastestPageWriteTime = baseBufDesc->GetLastModifyTime();
    UnlockContent(baseBufDesc);

    FAULT_INJECTION_WAIT(DstoreHeapFI::CONSTRUCT_CR_PAGE);

    /* Step 4: Construct a cr page that match with given snapshot. */
    CRContext crCtx{crContext.pdbId, INVALID_CSN, nullptr, nullptr, nullptr, false, false, crContext.snapshot,
        crContext.currentXid};
    RetStatus ret;
    if (crContext.destPage->GetType() == PageType::INDEX_PAGE_TYPE) {
        ret = (static_cast<BtrPage *>(crContext.destPage))->ConstructCR(thrd->GetActiveTransaction(), &crCtx,
            static_cast<BtreeUndoContext *>(crContext.dataPageExtraInfo), this);
    } else {
        ret = crContext.destPage->ConstructCR(thrd->GetActiveTransaction(), &crCtx);
    }

    crCtx.useLocalCr = isPoOrRa ? crCtx.useLocalCr : true;
    /* For the CR page of snapshot now, the snapshots obtained by different transactions are different. Therefore, the
     * snapshots cannot be cached locally. */
    crCtx.useLocalCr = crContext.snapshot->GetCsn() == MAX_COMMITSEQNO ? true : crCtx.useLocalCr;

    /* Step 5: If the page is the last visible version of the newest page, cache it in buf mgr for future read.
     * We have unlock the page content lock, this buffer may not have po or read auth flag, we still cache shared cr.
     * Therefore, we must verify the buffer is po or read auth before call ReadCr.
     */
    if (!crCtx.useLocalCr && pageType == PageType::INDEX_PAGE_TYPE) {
        BufferDesc *newCrBufDesc = ReadOrAllocCr(baseBufDesc, lastestPageWriteTime, bufRing);
        if (newCrBufDesc != INVALID_BUFFER_DESC) {
            /* We use cr buffer and release cr lock. */
            StorageAssert(newCrBufDesc->IsCrPage());
            StorageAssert(!newCrBufDesc->IsCrValid());
            rc = memcpy_s(newCrBufDesc->GetPage(), BLCKSZ,
                static_cast<char*>(static_cast<void*>(crContext.destPage)), BLCKSZ);
            StorageReleasePanic(rc != 0, MODULE_BUFFER, ErrMsg("memcpy cr page to cr buffer failed!"));
            FinishCrBuild(newCrBufDesc, crCtx.pageMaxCsn);
            Release(newCrBufDesc);
        }
    }

    Release(baseBufDesc);
    return ret;
}

/*
 * Release the pin on a buffer.
 */
void BufMgr::Release(BufferDesc* bufferDesc)
{
    if (STORAGE_VAR_NULL(bufferDesc)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("BufferDesc is nullptr."));
    }
    /*
     * The buffer's private reference count will be decreased by unpin. Whenever the private reference
     * count is decreased to 0, the shared reference count will be decreased by unpin.
     */
    bufferDesc->Unpin();
}

/*
 * MarkDirty
 *
 * Marks buffer contents as dirty (actual write happens later).
 *
 * Buffer must be pinned and exclusive-locked.  (If caller does not hold
 * exclusive lock, then somebody could be in process of writing the buffer,
 * leading to risk of bad data written to disk.)
 *
 * WARNING: This is a performance hotspot, be cautious when adding logs.
 */

RetStatus BufMgr::MarkDirty(BufferDesc *bufferDesc, bool needUpdateRecoveryPlsn)
{
    RetStatus rc = DSTORE_SUCC;

    StorageReleasePanic(!LWLockHeldByMeInMode(&(bufferDesc->contentLwLock), LW_EXCLUSIVE), MODULE_BUFMGR,
        ErrMsg("BufferDesc is not locked by me."));

    uint64 oldState = bufferDesc->LockHdr();
    StorageReleaseBufferCheckPanic(g_storageInstance->GetType() == StorageInstanceType::DISTRIBUTE_COMPUTE &&
        (oldState & Buffer::BUF_OWNED_BY_ME) == 0, MODULE_BUFMGR, bufferDesc->GetBufferTag(),
        "Page not owned when mark dirty");
    StorageAssert(bufferDesc->IsPinnedPrivately());

    /* 1. Set dirty page flag. */
    uint64 state = oldState | (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY);

    /* 2. Add it into dirty page queue if it's not already there. */
    StoragePdb *pdb = g_storageInstance->GetPdb(bufferDesc->GetPdbId());
    StorageReleasePanic(STORAGE_VAR_NULL(pdb), MODULE_BUFMGR, ErrMsg("Pdb is nullptr."));
    BgPageWriterMgr *bgPageWriterMgr = pdb->GetBgPageWriterMgr();
    if (bgPageWriterMgr != nullptr && needUpdateRecoveryPlsn) {
        /* now we just init one bgPageWriter in every pdb, so just pass the SlotId to GetBgPageWriter */
        int64 bgWriterSlotId = pdb->GetBgWriterSlotId(bufferDesc->GetPage()->GetWalId());
        if (likely(bgWriterSlotId >= DEFAULT_BGWRITER_SLOT_ID && bgWriterSlotId < DIRTY_PAGE_QUEUE_MAX_SIZE)) {
            rc = bgPageWriterMgr->PushDirtyPageToQueue(bufferDesc, needUpdateRecoveryPlsn, bgWriterSlotId);
            if (rc == DSTORE_SUCC) {
#ifdef DSTORE_USE_ASSERT_CHECKING
                ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("Success to push dirty page bufTag:%s"
                    " recoveryPlsn %lu into queue.", bufferDesc->GetBufferTag().ToString().CString(),
                    bufferDesc->recoveryPlsn[bgWriterSlotId].load(std::memory_order_acquire)));
#endif
            }
        } else if (g_storageInstance->IsInit() && !g_storageInstance->IsBootstrapping()) {
#ifndef UT
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
                   ErrMsg("MarkDirty failed to push dirty page bufTag:(%u, %hu, %u): "
                          "wal %lu lsn(%lu, %lu), because bgWriterSlotId is %ld.",
                          bufferDesc->GetBufferTag().pdbId, bufferDesc->GetBufferTag().pageId.m_fileId,
                          bufferDesc->GetBufferTag().pageId.m_blockId, bufferDesc->GetPage()->GetWalId(),
                          bufferDesc->GetPage()->GetGlsn(), bufferDesc->GetPage()->GetPlsn(), bgWriterSlotId));
#endif
        }
    } else if (needUpdateRecoveryPlsn && g_storageInstance->IsInit()  && (bgPageWriterMgr == nullptr)) {
        /* During Redo process when starting up, bgPageWriterMgr has not init when MarkDirty */
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("MarkDirty failed to push dirty page bufTag:(%u, %hu, %u): "
            "wal %lu lsn(%lu, %lu), because bgPageWriterMgr is nullptr pdbId %u.", bufferDesc->GetBufferTag().pdbId,
            bufferDesc->GetBufferTag().pageId.m_fileId, bufferDesc->GetBufferTag().pageId.m_blockId,
            bufferDesc->GetPage()->GetWalId(), bufferDesc->GetPage()->GetGlsn(), bufferDesc->GetPage()->GetPlsn(),
            pdb->GetPdbId()));
    }
    /* get file version */
    if (bufferDesc->fileVersion == INVALID_FILE_VERSION) {
        bufferDesc->SetFileVersion(WalUtils::GetFileVersion(bufferDesc->GetPdbId(), bufferDesc->GetPageId().m_fileId));
        StorageAssert(bufferDesc->fileVersion != INVALID_FILE_VERSION);
    }
    bufferDesc->UnlockHdr(state);

    return rc;
}

static RetStatus PrepareCheckPageBeforeStartIo(BufferDesc *bufferDesc)
{
    StorageAssert(bufferDesc->IsPinnedPrivately());

    /* Check whether Wal of page flush done */
    bufferDesc->WaitIfIsWritingWal();

    /* Wait for wal flush finish the page lsn. */
    Page *page = bufferDesc->GetPage();
    WalId walId = page->GetWalId();
    uint64 waitCnt = 0;
    StoragePdb *storagePdb = g_storageInstance->GetPdb(bufferDesc->GetPdbId());
    if (storagePdb == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("StoragePdb is nullptr."));
    }
    while (STORAGE_VAR_NULL(storagePdb->GetWalMgr()) ||
           STORAGE_VAR_NULL(storagePdb->GetWalMgr()->GetWalStreamManager()) ||
           STORAGE_VAR_NULL(storagePdb->GetVFS())) {
        if (waitCnt++ % WAIT_COUNT_FOR_REPORT_WARNING == 0) {
            ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
                ErrMsg("PrepareCheckPageBeforeStartIo wait for pdb %u init", bufferDesc->GetPdbId()));
        }
        const long micros = 1000; /* 1 ms */
        GaussUsleep(micros);
    }
    WalStreamManager *walStreamMgr = storagePdb->GetWalMgr()->GetWalStreamManager();
    if (storagePdb->GetPdbRoleMode() == PdbRoleMode::PDB_PRIMARY && walStreamMgr->IsSelfWritingWalStream(walId)) {
        walStreamMgr->GetWritingWalStream()->WaitTargetPlsnPersist(page->GetPlsn());
    }
    return DSTORE_SUCC;
}

/*
 * During redo drop tablespace datafile, after invalidating buffer operation, other concurrent redo tasks may still
 * generate new dirty pages in the buffer. As a result, the deleted files fail to be read during subsequent flush dirty
 * operation.
 *
 * This function compares the file version in the buffer with that of the datafile.
 * If the file version in the buffer is smaller, the file has been deleted and does not need to flush.
 */
RetStatus BufMgr::SkipWriteBlock(BufferDesc *bufferDesc)
{
    if (STORAGE_VAR_NULL(bufferDesc)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Failed to check if skip WriteBlock."));
        return DSTORE_FAIL;
    }

    PdbId pdbId = bufferDesc->GetPdbId();
    PageId pageId = bufferDesc->GetPageId();
    uint64 currentFileVersion = 0;
    uint64 bufDescFileVersion = bufferDesc->GetFileVersion();

    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Failed to get pdb, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Failed to get tablespaceMgr, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    RetStatus ret = tablespaceMgr->GetFileVersion(pageId.m_fileId, &currentFileVersion);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
               ErrMsg("Failed to get datafile version, fileId %hu, pdbId %u.", pageId.m_fileId, pdbId));
        return DSTORE_FAIL;
    }

    if (currentFileVersion <= bufDescFileVersion) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
               ErrMsg("The currentFileVersion %lu is smaller than or equal to bufDescFileVersion %lu, fileId %hu, "
                      "pdbId %u.",
                      currentFileVersion, bufDescFileVersion, pageId.m_fileId, pdbId));
        return DSTORE_FAIL;
    } else {
        ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
               ErrMsg("Skip write page bufTag:(%u, %hu, %u): wal %lu lsn(%lu, %lu), because "
                      "the datafile may have been deleted, currentFileVersion %lu, bufDescFileVersion %lu.",
                      pdbId, pageId.m_fileId, pageId.m_blockId, bufferDesc->GetPage()->GetWalId(),
                      bufferDesc->GetPage()->GetGlsn(), bufferDesc->GetPage()->GetPlsn(), currentFileVersion,
                      bufDescFileVersion));
        return DSTORE_SUCC;
    }
}

/*
 * WriteBlock
 *
 * Physically write out a shared buffer.
 * NOTE: this actually just passes the buffer contents to the kernel; the
 * real write to disk won't happen until the kernel feels like it.  This
 * is okay from our point of view since we can redo the changes from WAL.
 * However, we will need to force the changes to disk via fsync before
 * we can checkpoint WAL.
 *
 * The caller must hold a pin on the buffer and have share-locked the
 * buffer contents.  (Note: a share-lock does not prevent updates of
 * hint bits in the buffer, so the page could change while the write
 * is in progress, but we assume that that will not invalidate the data
 * written.)
 *
 * Return
 *  DSTORE_SUCC - flush succeeded
 *  DSTORE_FAIL - flush failed
 */
RetStatus BufMgr::WriteBlock(BufferDesc *bufferDesc)
{
    LatencyStat::Timer timer(&BufPerfUnit::GetInstance().m_writeStat, false, PerfLevel::RELEASE, true);
    if (unlikely(m_isCount.load())) {
        m_writeCount.fetch_add(1);
        timer.Start();
    }
    uint64 state;
    RetStatus ret = DSTORE_SUCC;
    BufferTag bufTag = bufferDesc->GetBufferTag();
    PageId pageId = bufferDesc->GetPageId();
    VFSAdapter *vfs = nullptr;

    /* Step 1: check page state before start IO. */
    if (STORAGE_FUNC_FAIL(PrepareCheckPageBeforeStartIo(bufferDesc))) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("WriteBlock fail when prepare check page before start io."));
        return DSTORE_FAIL;
    }

    /* Step 2: Get IO lock. */
    /*
     * Acquire the buffer's io_in_progress lock.  If start_io returns
     * false, then someone else flushed the buffer before we could,
     * so there is nothing for us to do.
     */
    if (!StartIo(bufferDesc, false)) {
        return DSTORE_SUCC;
    }

    /* Step 3: clean BUF_HINT_DIRTY flag */
    state = bufferDesc->LockHdr();
    state &= ~Buffer::BUF_HINT_DIRTY;
    bufferDesc->UnlockHdr(state);

    /* Step 4: Write page data. */
    /*
     * Now it's safe to write buffer to disk. Note that no one else should
     * have been able to write it while we were busy with log flushing because
     * we have the io_in_progress lock.
     */
    StoragePdb *pdb = g_storageInstance->GetPdb(bufferDesc->GetPdbId());
    if (pdb == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("storagePdb is nullptr."));
    }
    vfs = pdb->GetVFS();
    StorageReleasePanic(vfs == nullptr, MODULE_BUFMGR, ErrMsg("vfs is nullptr, pdb %u", bufferDesc->GetPdbId()));
    bufferDesc->GetPage()->SetChecksum();
    int64_t startTime = 0;
    StorageStat *stat = g_storageInstance->GetStat();
    stat->ReportDataIOTimeRecord(&startTime, true);
WRITE:
    stat->m_reportWaitEvent(
        static_cast<uint32_t>(GsStatWaitEvent::WAIT_EVENT_BUFFER_WRITE_PAGE_SYNC));
    if (STORAGE_FUNC_FAIL(vfs->WritePageSync(pageId, bufferDesc->GetPage()))) {
        stat->m_reportWaitEvent(static_cast<uint32_t>(OPTUTIL_GSSTAT_WAIT_EVENT_END));
        ErrorCode err = StorageGetErrorCode();
        if (err == VFS_WARNING_FILE_NOT_OPENED) {
            ret = TryOpenFile(vfs, bufTag);
            if (STORAGE_FUNC_SUCC(ret)) {
                goto WRITE;
            }
        }
        err = StorageGetErrorCode();
        if (err == VFS_WARNING_FILE_NOT_EXISTS && STORAGE_FUNC_SUCC(SkipWriteBlock(bufferDesc))) {
            ret = DSTORE_SUCC;
        } else {
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
                   ErrMsg("VFS write page bufTag:(%hhu, %hu, %u) fail, error:%lld", bufTag.pdbId,
                          bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, err));
            ret = DSTORE_FAIL;
        }
    } else {
        stat->m_reportWaitEvent(static_cast<uint32_t>(OPTUTIL_GSSTAT_WAIT_EVENT_END));
        if (IsEnablePageMissingDirtyCheck()) {
            /* AntiCache: update pageVersionOnDisk after flush success */
            bufferDesc->UpdatePageVersion(bufferDesc->GetPage());
        }
        ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR,
            ErrMsg("WriteBlock page bufTag:(%hhu, %hu, %u): wal %lu lsn(%lu, %lu).",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                bufferDesc->GetPage()->GetWalId(), bufferDesc->GetPage()->GetGlsn(), bufferDesc->GetPage()->GetPlsn()));
    }

    /* Step 5: Release IO lock. */
    /*
     * Mark the buffer as clean (unless BUF_JUST_DIRTIED has become set) and
     * end the io_in_progress state. This is all within terminate_io.
     */
    if (STORAGE_FUNC_SUCC(ret)) {
#ifdef DSTORE_USE_ASSERT_CHECKING
        (void)++m_bufPoolCnts.flushCnt;
#endif
        TerminateIo(bufferDesc, true, 0);
    } else {
        TerminateIo(bufferDesc, false, Buffer::BUF_IO_ERROR);
    }
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("Buffer Status Observation: "
        "bufTag:(%hhu, %hu, %u) WriteBlock lsnInfo(walId %lu glsn %lu plsn %lu) state(%lu) ret(%d)",
        bufferDesc->GetPdbId(), bufferDesc->GetPageId().m_fileId, bufferDesc->GetPageId().m_blockId,
        bufferDesc->GetPage()->GetWalId(), bufferDesc->GetPage()->GetGlsn(), bufferDesc->GetPage()->GetPlsn(),
        bufferDesc->GetState(), ret));
    stat->ReportDataIOTimeRecord(&startTime, false);
    return ret;
}

RetStatus BufMgr::Flush(BufferTag &bufTag, void* aioCtx)
{
    RetStatus rc = DSTORE_SUCC;
    BufferDesc *bufferDesc = LookupBufferWithLock(bufTag);
    if (bufferDesc == INVALID_BUFFER_DESC) {
        return rc;
    }
    (void)LockContent(bufferDesc, LW_SHARED);
    uint64 state = bufferDesc->GetState();
    if (!((state & Buffer::BUF_CONTENT_DIRTY) || (state & Buffer::BUF_HINT_DIRTY))) {
        UnlockContent(bufferDesc);
        bufferDesc->Unpin();
        return rc;
    }
    if (aioCtx) {
        BufferAioContext *bufferAioCtx = static_cast<BufferAioContext *>(static_cast<AsyncIoContext *>(aioCtx)->
            asyncContext);
        bufferAioCtx->bufferDesc = bufferDesc;
        rc = WriteBlockAsync(bufferDesc, aioCtx);
        ReleasePageLockForAsyncFlush(rc, bufferDesc, aioCtx);
    } else {
        rc = WriteBlock(bufferDesc);
        UnlockContent(bufferDesc);
    }
    bufferDesc->Unpin();
    return rc;
}

RetStatus BufMgr::WriteBlockAsync(BufferDesc *bufferDesc, void *aioCtx)
{
    uint64 state;
    RetStatus ret = DSTORE_SUCC;
    AsyncIoContext *aioContext = static_cast<AsyncIoContext *>(aioCtx);
    BufferAioContext *bufferAioCtx = static_cast<BufferAioContext *>(aioContext->asyncContext);
    BatchBufferAioContextMgr *batchCtxMgr = static_cast<BatchBufferAioContextMgr *>(bufferAioCtx->batchCtxMgr);
    if (STORAGE_FUNC_FAIL(PrepareCheckPageBeforeStartIo(bufferDesc))) {
        UnlockContent(bufferDesc);
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Async flush fail when prepare check page before start io."));
        return DSTORE_FAIL;
    }

    if (!StartIo(bufferDesc, false)) {
        UnlockContent(bufferDesc);
        return DSTORE_SUCC;
    }
    bufferAioCtx->hasStartAioFlag = true;
    bufferAioCtx->submittedTime = static_cast<uint64>(GetSystemTimeInMicrosecond());
    state = bufferDesc->LockHdr();
    state &= ~Buffer::BUF_HINT_DIRTY;
    bufferDesc->UnlockHdr(state);

    /*
     * The BUF_IO_IN_PROGRESS flag has already been set when starting IO.
     * The PO will wait for the I/O to complete before the transfer.
     * Therefore, it is safe to unlock the content after copying the page here.
     */
    Page *page = bufferAioCtx->pageCopy;
    errno_t rc = memcpy_s(page, BLCKSZ, bufferDesc->GetPage(), BLCKSZ);
    storage_securec_check(rc, "\0", "\0");
    UnlockContent(bufferDesc);

    BufferTag bufTag = bufferDesc->GetBufferTag();
    WalId pageWalId = page->GetWalId();
    uint64 pageGlsn = page->GetGlsn();
    uint64 pagePlsn = page->GetPlsn();
    if (g_storageInstance->GetPdb(bufferDesc->GetPdbId()) == nullptr ||
        g_storageInstance->GetPdb(bufferDesc->GetPdbId())->GetVFS() == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("StoragePdb or VFS is nullptr."));
    }
    VFSAdapter *vfs = g_storageInstance->GetPdb(bufferDesc->GetPdbId())->GetVFS();
    page->SetChecksum();
    /*
     * this IO lock and contentLWLock is just no longer recorded by this flush thread,
     * so that the thread can acquire them again and we dont wait the IO complete.
     * the locks are really released at CallbackForAioBatchFlushBuffers when async io finished.
     */
    LWLockDisown(bufferDesc->controller->GetIoInProgressLwLock());
    /*
     * extraly do one more shared pin for async callback thread that will occupy this bufferDesc,
     * then this flush thread unpin privately at outer.
     */
    bufferDesc->PinForAio();
    batchCtxMgr->AddOneInProgressPage();
    /* the bufferDesc maybe has released after call WriteBlockAsync so that should not read the page after it */
    int64_t startTime = 0;
    StorageStat *stat = g_storageInstance->GetStat();
    stat->ReportDataIOTimeRecord(&startTime, true);
ASYNC_WRITE:
    stat->m_reportWaitEvent(
        static_cast<uint32_t>(GsStatWaitEvent::WAIT_EVENT_BUFFER_WRITE_PAGE_ASYNC));
    if (STORAGE_FUNC_FAIL(vfs->WritePageAsync(bufTag.pageId, page, (const AsyncIoContext *)aioCtx))) {
        stat->m_reportWaitEvent(static_cast<uint32_t>(OPTUTIL_GSSTAT_WAIT_EVENT_END));
        ErrorCode err = StorageGetErrorCode();
        if (err == VFS_WARNING_FILE_NOT_OPENED && STORAGE_FUNC_SUCC(TryOpenFile(vfs, bufTag))) {
            goto ASYNC_WRITE;
        }

        err = StorageGetErrorCode();
        if (err == VFS_WARNING_FILE_NOT_EXISTS && STORAGE_FUNC_SUCC(SkipWriteBlock(bufferDesc))) {
            ret = DSTORE_SUCC;
        } else {
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
                   ErrMsg("VFS async write page bufTag:(%hhu, %hu, %u) fail, error:%lld", bufTag.pdbId,
                          bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, err));
            ret = DSTORE_FAIL;
        }
        batchCtxMgr->SubOneInProgressPage();
    }

    if (STORAGE_FUNC_SUCC(ret)) {
        stat->m_reportWaitEvent(static_cast<uint32_t>(OPTUTIL_GSSTAT_WAIT_EVENT_END));
#ifdef DSTORE_USE_ASSERT_CHECKING
        (void)++m_bufPoolCnts.flushCnt;
#endif
    } else {
        TerminateAsyncIo(bufferDesc, false, Buffer::BUF_IO_ERROR);
    }
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("Buffer Status Observation: "
        "bufTag:(%hhu, %hu, %u) WriteBlockAsync lsnInfo(walId %lu glsn %lu plsn %lu) state(%lu) ret(%d)",
        bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
        pageWalId, pageGlsn, pagePlsn,
        state, ret));
    stat->ReportDataIOTimeRecord(&startTime, true);
    return ret;
}

RetStatus BufMgr::FlushIfDirty(BufferDesc *bufferDesc)
{
    RetStatus rc = DSTORE_SUCC;
    uint64 bufferState = GsAtomicReadU64(&(bufferDesc->state));
    if ((bufferState & Buffer::BUF_CONTENT_DIRTY) || (bufferState & Buffer::BUF_HINT_DIRTY)) {
        bufferDesc->Pin();
        (void)LockContent(bufferDesc, LW_SHARED);
        rc = WriteBlock(bufferDesc);
        UnlockContent(bufferDesc);
        bufferDesc->Unpin();
    }
    return rc;
}

void BufMgr::StartFlushThread(FlushThreadContext *context)
{
    /* create and register thread */
    InitSignalMask();
    (void)g_storageInstance->CreateThreadAndRegister(INVALID_PDB_ID, context->isBootstrap, "FlushAll", true,
                                                     ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);

    BufferMemChunkWrapper<BufferMemChunk> *memChunkWrapper = nullptr;
    BufferDesc *bufferDesc = nullptr;
    RetStatus ret = DSTORE_SUCC;
    BatchBufferAioContextMgr *batchCtxMgr = nullptr;
    /*
     * func io_submit do not support concurrent, which called by Utils local disk aio with libaio
     * so that FlushAll use aio just in pagestore
     */
    if (USE_VFS_PAGESTORE_AIO) {
        batchCtxMgr = DstoreNew(g_dstoreCurrentMemoryContext) BatchBufferAioContextMgr();
        StorageReleasePanic(batchCtxMgr == nullptr, MODULE_BUFMGR, ErrMsg("alloc batchCtxMgr fail"));
        ret = batchCtxMgr->InitBatch(true, static_cast<BufMgrInterface *>(this));
        StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_BUFMGR, ErrMsg("init batchCtxMgr fail"));
    }
    for (;;) {
        thrd->RefreshWorkingVersionNum();
        context->memChunkQueueLock.Acquire();
        memChunkWrapper = context->nextMemChunk;
        if (memChunkWrapper != nullptr) {
            context->nextMemChunk = memChunkWrapper->GetNext();
        }
        context->memChunkQueueLock.Release();
        if (memChunkWrapper == nullptr) {
            break;
        }
        memChunkWrapper->memChunk->LockMemChunk(LW_SHARED);
        for (Size i = 0; i < memChunkWrapper->memChunk->GetSize(); i++) {
            bufferDesc = memChunkWrapper->memChunk->GetBufferDesc(i);
            if (context->pdbId != INVALID_PDB_ID && context->pdbId != bufferDesc->GetPdbId()) {
                /* INVALID_PDB_ID means scan all of the PDBs */
                continue;
            }
            if (context->onlyOwnedByMe) {
                uint64 state = bufferDesc->GetState();
                if ((state & Buffer::BUF_OWNED_BY_ME) == 0) {
                    continue;
                }
            }
            if (!USE_VFS_PAGESTORE_AIO) {
                ret = Flush(bufferDesc, context->actualFlushNum);
            } else {
                BufferTag bufTag = bufferDesc->GetBufferTag();
                ret = batchCtxMgr->AsyncFlushPage(bufTag);
            }
            StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_BUFMGR, ErrMsg("Flush bufTag:(%hhu, %hu, %u) failed.",
                bufferDesc->GetPdbId(), bufferDesc->GetPageId().m_fileId, bufferDesc->GetPageId().m_blockId));
        }
        memChunkWrapper->memChunk->UnlockMemChunk();
    }
    if (USE_VFS_PAGESTORE_AIO) {
        batchCtxMgr->FsyncBatch();
        (void)GsAtomicAddFetchU64(&(context->actualFlushNum), batchCtxMgr->GetFlushedPagesNum());
        batchCtxMgr->DestoryBatch();
        delete batchCtxMgr;
    }
    /* unregister thread */
    g_storageInstance->UnregisterThread(context->isBootstrap);
}

RetStatus BufMgr::Flush(BufferDesc *bufferDesc, gs_atomic_uint64 &actualFlushNum)
{
    RetStatus rc = DSTORE_SUCC;
    uint64 state = bufferDesc->LockHdr();
    BufferTag bufTag = bufferDesc->GetBufferTag();
    instr_time ioStart;
    instr_time ioTime;

    /*
     * Flush page just flush dirty page;
     */
    if ((state & Buffer::BUF_CONTENT_DIRTY) || (state & Buffer::BUF_HINT_DIRTY)) {
        bufferDesc->PinUnderHdrLocked();
        bufferDesc->UnlockHdr(state);
        (void)LockContent(bufferDesc, LW_SHARED);
        if (unlikely(g_storageInstance->GetGuc()->enableTrackIOTiming)) {
            INSTR_TIME_SET_CURRENT(ioStart);
        }
        rc = WriteBlock(bufferDesc);
        if (unlikely(g_storageInstance->GetGuc()->enableTrackIOTiming)) {
            INSTR_TIME_SET_CURRENT(ioTime);
            INSTR_TIME_SUBTRACT(ioTime, ioStart);
            g_storageInstance->GetStat()->ReportBufferWriteTime(ioTime);
        }
        StorageAssert(rc == DSTORE_SUCC);
        UnlockContent(bufferDesc);
        bufferDesc->Unpin();
        (void)GsAtomicAddFetchU64(&actualFlushNum, 1);
        ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("flushed page bufTag:(%hhu, %hu, %u) status:%lu",
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, state));
    } else {
        bufferDesc->UnlockHdr(state);
    }
    return rc;
}

RetStatus BufMgr::FlushBuffers(bool isBootstrap, bool onlyOwnedByMe, PdbId pdbId)
{
    FlushThreadContext context;
    context.isBootstrap = isBootstrap;
    context.onlyOwnedByMe = onlyOwnedByMe;
    GsAtomicInitU64(&context.actualFlushNum, 0);
    context.pdbId = pdbId;
    context.memChunkQueueLock.Init();
    context.rc = DSTORE_SUCC;

    std::thread *flushThreads[FLUSH_THREAD_NUM];

    m_bufferMemChunkList->LockBufferMemChunkList(LW_SHARED);
    context.nextMemChunk = m_bufferMemChunkList->GetMemChunkIterator<BufferMemChunk>();

    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("flush all start"));
    for (uint32 i = 0; i < FLUSH_THREAD_NUM; i++) {
        flushThreads[i] = new std::thread([&, this] {StartFlushThread(&context);});
    }
    for (uint32 i = 0; i < FLUSH_THREAD_NUM; i++) {
        flushThreads[i]->join();
        delete flushThreads[i];
    }

    m_bufferMemChunkList->UnlockBufferMemChunkList();

    uint64 flushCtn = GsAtomicReadU64(&context.actualFlushNum);
    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("flush all finish, pdb:%hhu, actual flush %lu", pdbId, flushCtn));
    return context.rc;
}

RetStatus BufMgr::FlushAll(bool isBootstrap, bool onlyOwnedByMe, PdbId pdbId)
{
#if !defined(UT) || defined(EMBEDDED)
    /* Step 1: Store MaxAppendedPlsn from all wal streams of specified PDB, if pdbId = INVALID_PDB_ID, then all
     * of the PDBs are specified.
     */
    PdbsPlsnRecords plsnRecords;
    plsnRecords.Init();
    g_storageInstance->StoreMaxAppendPlsnOfPdbs(pdbId, plsnRecords);

    /* Step 2: Flush all dirty pages of specified PDB */
    RetStatus ret = FlushBuffers(isBootstrap, onlyOwnedByMe, pdbId);
    if (STORAGE_FUNC_SUCC(ret)) {
    /* Step 3: Update control files based on the MaxAppendedPlsn that we stored before flushing dirty pages */
        ret = g_storageInstance->CreateCheckpointForPdbs(plsnRecords);
    }

    plsnRecords.Destroy();
#else
    RetStatus ret = FlushBuffers(isBootstrap, onlyOwnedByMe, pdbId);
#endif
    return ret;
}

RetStatus BufMgr::TryFlush(BufferDesc *bufferDesc)
{
    RetStatus ret = DSTORE_SUCC;
    uint64 state = bufferDesc->GetState();
    if (state & Buffer::BUF_CONTENT_DIRTY) {
        StorageAssert(~(state & Buffer::BUF_CR_PAGE));
        bufferDesc->Pin();
        if (bufferDesc->bufTag.IsInvalid()) {
            bufferDesc->Unpin();
            return ret;
        }
        DstoreLWLockAcquire(&bufferDesc->contentLwLock, LW_SHARED);
        ret = WriteBlock(bufferDesc);
        LWLockRelease(&bufferDesc->contentLwLock);
        bufferDesc->Unpin();
    }

    return ret;
}

/*
 * Mark a buffer invalid and return it to the candidate list.
 *
 * NOTE:
 * The caller should hold the buffer header lock to make sure no other threads can reuse this buffer
 * and check the page id of the buffer to make sure that the buffer does need to be invalidated.
 * This is used only in the context such as dropping a relation or shrinking the buffer pool.
 *
 * Return
 *  DSTORE_SUCC - invalidate succeeded
 *  DSTORE_FAIL - invalidate failed
 */
RetStatus BufMgr::Invalidate(BufferDesc* bufferDesc)
{
    /*
     * Make sure the buffer header is locked when we get pageId and the buffer is not CR buffer.
     * NOTE: we just invalidate base buffer, CR buffer will be invalidated when we invalidate associate base buffer.
     */
    uint64 bufferState = GsAtomicReadU64(&(bufferDesc->state));
    StorageAssert(bufferState & Buffer::BUF_LOCKED);
    StorageAssert(!(bufferState & Buffer::BUF_CR_PAGE));
    bufferDesc->UnlockHdr(bufferState);

    BufferTag bufTag = bufferDesc->bufTag;
    uint32 bufferHash = m_buftable->GetHashCode(&bufTag);
#ifndef UT
    uint32 retryTimes = 0;
#endif
    goto RETRY;

UNLOCK_AND_RETRY:
    bufferDesc->UnlockHdr(bufferState);
    m_buftable->UnlockBufMapping(bufferHash);

RETRY:
#ifndef UT
    if (unlikely((++retryTimes) > INVALIDATE_BUFFER_RETRY_TIMES)) {
        storage_set_error(BUFFER_WARNING_FAIL_TO_INVALIDATE_BASE_BUFFER);
        return DSTORE_FAIL;
    }
#endif

    /*
     * Acquire exclusive mapping lock in preparation for changing the buffer's association.
     */
    if (!m_buftable->TryLockBufMapping(bufferHash, LW_EXCLUSIVE)) {
        goto RETRY;
    }

    /* Flush this buffer if it's dirty. */
    if (STORAGE_FUNC_FAIL(FlushIfDirty(bufferDesc))) {
        m_buftable->UnlockBufMapping(bufferHash);
        storage_set_error(BUFFER_WARNING_FAIL_TO_INVALIDATE_BASE_BUFFER);
        return DSTORE_FAIL;
    }

    bufferState = bufferDesc->LockHdr();

    /*
     * Buffer is reused by other thread.
     */
    if (bufferDesc->bufTag != bufTag) {
        bufferDesc->UnlockHdr(bufferState);
        m_buftable->UnlockBufMapping(bufferHash);
        return DSTORE_SUCC;
    }

    /*
     * Retry if the buffer is pinned by others.
     */
    if (bufferDesc->GetRefcount() > 0) {
        goto UNLOCK_AND_RETRY;
    }

    /* If it's still dirty then retry to avoid deadlock. */
    if ((bufferState & Buffer::BUF_CONTENT_DIRTY) || (bufferState & Buffer::BUF_HINT_DIRTY)) {
        goto UNLOCK_AND_RETRY;
    }

    /* Acquire CR assign lock, and invalidate CR buffer */
    if (STORAGE_FUNC_FAIL(InvalidateCrBufferOfBaseBuffer(bufferDesc))) {
        goto UNLOCK_AND_RETRY;
    }

    BufferTag invalidBufTag = bufferDesc->bufTag;
    uint64 invalidBufState = bufferState;
    RemoveBufferFromLruAndHashTable(bufferDesc);

    /*
     * Done with mapping lock.
     */
    uint64 timestamp = static_cast<uint64>(GetSystemTimeInMicrosecond());
    m_buftable->UnlockBufMapping(bufferHash);
    EvictBaseBufferCallback(invalidBufTag, invalidBufState, timestamp);

    return DSTORE_SUCC;
}

RetStatus BufMgr::InvalidateBaseBuffer(BufferDesc *bufferDesc, BufferTag bufTag, bool needFlush)
{
    StorageAssert(bufferDesc->IsPinnedPrivately());
    uint64 state = GsAtomicReadU64(&(bufferDesc->state));

    uint32 bufferHash = m_buftable->GetHashCode(&(bufferDesc->bufTag));
    if (state & Buffer::BUF_TAG_VALID) {
        m_buftable->LockBufMapping(bufferHash, LW_EXCLUSIVE);
    }

    if (needFlush) {
        DstoreLWLockAcquire(&bufferDesc->contentLwLock, LW_SHARED);
        state = GsAtomicReadU64(&(bufferDesc->state));
        if (((state & Buffer::BUF_CONTENT_DIRTY) || (state & Buffer::BUF_HINT_DIRTY)) &&
            STORAGE_FUNC_FAIL(WriteBlock(bufferDesc))) {
            LWLockRelease(&bufferDesc->contentLwLock);
            if (state & Buffer::BUF_TAG_VALID) {
                m_buftable->UnlockBufMapping(bufferHash);
            }
            bufferDesc->Unpin();
            StorageAssert(StorageGetErrorCode() != STORAGE_OK);
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Invalidate by buftag=(%hhu, %hu, %u) flush failed.",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
            return DSTORE_FAIL;
        }
        LWLockRelease(&bufferDesc->contentLwLock);
    }

    state = bufferDesc->LockHdr();
    PrivateRefCountEntry *ref = thrd->GetBufferPrivateRefCount()->GetPrivateRefcount(bufferDesc);
    if (bufferDesc->GetRefcount() > 1 || (ref != nullptr && ref->refcount > 1)) {
        bufferDesc->UnlockHdr(state);
        if (state & Buffer::BUF_TAG_VALID) {
            m_buftable->UnlockBufMapping(bufferHash);
        }
        bufferDesc->Unpin();
        storage_set_error(BUFFER_WARNING_FAIL_TO_INVALIDATE_BASE_BUFFER);
        ErrLog(DSTORE_WARNING, MODULE_BUFMGR, ErrMsg("Invalidate by buftag=(%hhu, %hu, %u) retry due to refcount(%lu), "
            "private refcount(%d).", bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
            bufferDesc->GetRefcount(), ref->refcount));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("Buffer Status Observation: bufTag:(%hhu, %hu, %u)"
        " InvalidatePage lsnInfo(walId %lu glsn %lu plsn %lu) state(%lu)",
        bufferDesc->GetPdbId(), bufferDesc->GetPageId().m_fileId, bufferDesc->GetPageId().m_blockId,
        reinterpret_cast<Page *>(bufferDesc->bufBlock)->GetWalId(),
        reinterpret_cast<Page *>(bufferDesc->bufBlock)->GetGlsn(),
        reinterpret_cast<Page *>(bufferDesc->bufBlock)->GetPlsn(),
        state));
    bufferDesc->AcquireCrAssignLwlock(LW_EXCLUSIVE);
    if (bufferDesc->HasCrBuffer()) {
        BufferDesc *crBufferDesc = bufferDesc->GetCrBuffer();
        crBufferDesc->Pin();
        InvalidateCrBufferUsingGivenPdbIdOrFileId(crBufferDesc, crBufferDesc->GetPdbId());
    }
    bufferDesc->ReleaseCrAssignLwlock();

    bufferDesc->bufTag.SetInvalid();
    bufferDesc->fileVersion = INVALID_FILE_VERSION;
    uint64 newBufferState = state & Buffer::BUF_FLAG_RESET_MASK;
    if (state & Buffer::BUF_TAG_VALID) {
        m_buftable->Remove(&bufTag, bufferHash);
        m_buftable->UnlockBufMapping(bufferHash);
    }
    bufferDesc->UnlockHdr(newBufferState);
    m_buffer_lru->MoveToCandidateList(bufferDesc);
    bufferDesc->Unpin();

    return DSTORE_SUCC;
}

RetStatus BufMgr::InvalidateByBufTag(BufferTag bufTag, bool needFlush)
{
RETRY:
    BufferDesc *bufferDesc = LookupBufferWithLock(bufTag);
    if (bufferDesc == INVALID_BUFFER_DESC) {
        ErrLog(DSTORE_WARNING, MODULE_BUFMGR, ErrMsg("Invalidate by buftag=(%hhu, %hu, %u) not found.",
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
        return DSTORE_SUCC;
    }

    RetStatus ret = InvalidateBaseBuffer(bufferDesc, bufTag, needFlush);
    if (STORAGE_FUNC_FAIL(ret) && StorageGetErrorCode() == BUFFER_WARNING_FAIL_TO_INVALIDATE_BASE_BUFFER) {
        goto RETRY;
    }

    return ret;
}

char *BufMgr::GetClusterBufferInfo(UNUSE_PARAM PdbId pdbId, UNUSE_PARAM FileId fileId, UNUSE_PARAM BlockNumber blockId)
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        return nullptr;
    }
    dumpInfo.append("The function is applicable to cluster mode only");
    return dumpInfo.data;
}

RetStatus BufMgr::InvalidateCrBuffer(BufferDesc *crBufferDesc)
{
    RetStatus rc = DSTORE_FAIL;
    SpinDelayStatus delayStatus = InitSpinDelay(__FILE__, __LINE__);

    /* Remove CR buffer from the base buffer's slot. */
    for (uint32 retryTimes = 0; STORAGE_FUNC_FAIL(rc) && (retryTimes < RETRY_WARNING_THRESHOLD); retryTimes++) {
        crBufferDesc->Pin();
        if (MakeCrBufferFree(crBufferDesc)) {
            rc = DSTORE_SUCC;
        }
        crBufferDesc->Unpin();
        if (STORAGE_FUNC_FAIL(rc)) {
            PerformSpinDelay(&delayStatus, crBufferDesc->contentLwLock.spinsPerDelay);
        }
    }

    if (STORAGE_FUNC_FAIL(rc)) {
        storage_set_error(BUFFER_WARNING_FAIL_TO_INVALIDATE_CR_BUFFER);
        return rc;
    }

    /*
     * Move the CR buffer back to candidate list if it's not being invalidated for bufferpool shrink.
     */
    if (!crBufferDesc->lruNode.IsInInvalidationList() && !crBufferDesc->lruNode.IsInPendingState()) {
        m_buffer_lru->MoveToCandidateList(crBufferDesc);
    }

    /* Clear the CR buffer flag */
    uint64 state = crBufferDesc->LockHdr();
    state &= ~(Buffer::BUF_CR_PAGE);
    crBufferDesc->ResetAsBaseBuffer();
    crBufferDesc->UnlockHdr(state);
    return rc;
}

RetStatus BufMgr::InvalidateCrBufferOfBaseBuffer(BufferDesc *baseBufferDesc)
{
    StorageAssert(baseBufferDesc->IsHdrLocked());
    BufferTag bufTag = baseBufferDesc->GetBufferTag();

    baseBufferDesc->AcquireCrAssignLwlock(LW_EXCLUSIVE);
    if (baseBufferDesc->HasCrBuffer()) {
        BufferDesc *crBufferDesc = baseBufferDesc->GetCrBuffer();
        ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR,
            ErrMsg("Invalidate page bufTag:(%hhu, %hu, %u) base buffer (lru type = %hhu) and "
                "CR buffer (lru type = %hhu) together.\n",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                static_cast<uint8>(baseBufferDesc->lruNode.m_type.load()),
                static_cast<uint8>(crBufferDesc->lruNode.m_type.load())));
        if (STORAGE_FUNC_FAIL(InvalidateCrBuffer(crBufferDesc))) {
            baseBufferDesc->ReleaseCrAssignLwlock();
            storage_set_error(BUFFER_WARNING_FAIL_TO_INVALIDATE_CR_BUFFER);
            return DSTORE_FAIL;
        }
    }
    baseBufferDesc->ReleaseCrAssignLwlock();
    return DSTORE_SUCC;
}

void BufMgr::MoveBuffersToInvalidationList(PdbId pdbId)
{
    for (Size i = 0; i < m_lru_partitions; ++i) {
        UNUSE_PARAM LruGenericList *invalidationList = GetBufLruList(i)->GetInvalidationList();
        LruHotList *hotList = GetBufLruList(i)->GetHotList();
        hotList->LockList();
        ConcurrentDList::ForwardIterator iter = hotList->begin();
        while (iter != hotList->end()) {
            LruNode *lruNode = BufLruList::GetNode(*iter);
            BufferDesc *bufferDesc = lruNode->GetValue<BufferDesc>();
            bufferDesc->Pin();
            if (bufferDesc->bufTag.pdbId != pdbId) {
                bufferDesc->Unpin();
            } else {
                hotList->RemoveUnderListLock(lruNode);
                invalidationList->PushTail(lruNode);
                bufferDesc->Unpin();
            }
            ++iter;
        }
        LruList *lruList = GetBufLruList(i)->GetLruList();
        lruList->LockList();
        iter = lruList->begin();
        while (iter != lruList->end()) {
            LruNode *lruNode = BufLruList::GetNode(*iter);
            BufferDesc *bufferDesc = lruNode->GetValue<BufferDesc>();
            bufferDesc->Pin();
            if (bufferDesc->bufTag.pdbId != pdbId) {
                bufferDesc->Unpin();
            } else {
                lruList->Remove(lruNode);
                invalidationList->PushTail(lruNode);
                bufferDesc->Unpin();
            }
            ++iter;
        }
        lruList->UnlockList();
        hotList->UnlockList();
    }
}

void BufMgr::InvalidateBaseBufferUsingGivenPdbIdOrFileId(BufferDesc *baseBufferDesc, PdbId pdbId, FileId fileId)
{
    uint64 bufferState = GsAtomicReadU64(&(baseBufferDesc->state));
    baseBufferDesc->UnlockHdr(bufferState);
RETRY:
    baseBufferDesc->Pin();
    BufferTag bufTag = baseBufferDesc->bufTag;
    uint32 bufferHash = m_buftable->GetHashCode(&bufTag);

    if (bufferState & Buffer::BUF_TAG_VALID) {
        m_buftable->LockBufMapping(bufferHash, LW_EXCLUSIVE);
    }

    if (baseBufferDesc->bufTag.pdbId != pdbId ||
       (fileId != INVALID_VFS_FILE_ID && baseBufferDesc->bufTag.pageId.m_fileId != fileId)) {
        if (bufferState & Buffer::BUF_TAG_VALID) {
            m_buftable->UnlockBufMapping(bufferHash);
        }
        baseBufferDesc->Unpin();
        return;
    }
    bufferState = baseBufferDesc->LockHdr();
    PrivateRefCountEntry *ref = thrd->GetBufferPrivateRefCount()->GetPrivateRefcount(baseBufferDesc);
    if (baseBufferDesc->GetRefcount() > 1 || (ref != nullptr && ref->refcount > 1)) {
        baseBufferDesc->UnlockHdr(bufferState);
        if (bufferState & Buffer::BUF_TAG_VALID) {
            m_buftable->UnlockBufMapping(bufferHash);
        }
        baseBufferDesc->Unpin();
        goto RETRY;
    }
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("Buffer Status Observation: bufTag:(%hhu, %hu, %u)"
        " DropPage lsnInfo(walId %lu glsn %lu plsn %lu) state(%lu)",
        baseBufferDesc->GetPdbId(), baseBufferDesc->GetPageId().m_fileId, baseBufferDesc->GetPageId().m_blockId,
        reinterpret_cast<Page *>(baseBufferDesc->bufBlock)->GetWalId(),
        reinterpret_cast<Page *>(baseBufferDesc->bufBlock)->GetGlsn(),
        reinterpret_cast<Page *>(baseBufferDesc->bufBlock)->GetPlsn(), bufferState));
    baseBufferDesc->AcquireCrAssignLwlock(LW_EXCLUSIVE);
    if (baseBufferDesc->HasCrBuffer()) {
        BufferDesc *crBufferDesc = baseBufferDesc->GetCrBuffer();
        crBufferDesc->Pin();
        InvalidateCrBufferUsingGivenPdbIdOrFileId(crBufferDesc, pdbId, fileId);
    }
    baseBufferDesc->ReleaseCrAssignLwlock();

    baseBufferDesc->bufTag.SetInvalid();
    baseBufferDesc->fileVersion = INVALID_FILE_VERSION;
    uint64 newBufferState = bufferState & Buffer::BUF_FLAG_RESET_MASK;
    uint64 timestamp = static_cast<uint64>(GetSystemTimeInMicrosecond());
    if (bufferState & Buffer::BUF_TAG_VALID) {
        m_buftable->Remove(&bufTag, bufferHash);
        m_buftable->UnlockBufMapping(bufferHash);
    }
    baseBufferDesc->UnlockHdr(newBufferState);
    m_buffer_lru->MoveToCandidateList(baseBufferDesc);
    baseBufferDesc->Unpin();

    EvictBaseBufferCallback(bufTag, bufferState, timestamp);
}

void BufMgr::InvalidateCrBufferUsingGivenPdbIdOrFileId(BufferDesc *crBufferDesc, PdbId pdbId, FileId fileId)
{
    SpinDelayStatus delayStatus = InitSpinDelay(__FILE__, __LINE__);
    UNUSE_PARAM BufferDesc *baseBufferDesc = crBufferDesc->GetCrBaseBuffer();
    StorageAssert(baseBufferDesc->IsCrAssignLocked());

    while (true) {
        if (crBufferDesc->bufTag.pdbId != pdbId ||
            (fileId != INVALID_VFS_FILE_ID && crBufferDesc->bufTag.pageId.m_fileId != fileId)) {
            crBufferDesc->Unpin();
            return;
        }
        if (MakeCrBufferFree(crBufferDesc)) {
            break;
        }
        PerformSpinDelay(&delayStatus, crBufferDesc->contentLwLock.spinsPerDelay);
    }

    uint64 state = crBufferDesc->LockHdr();
    crBufferDesc->bufTag.SetInvalid();
    crBufferDesc->fileVersion = INVALID_FILE_VERSION;
    state &= ~(Buffer::BUF_CR_PAGE);
    crBufferDesc->ResetAsBaseBuffer();
    crBufferDesc->UnlockHdr(state);
    m_buffer_lru->MoveToCandidateList(crBufferDesc);
    crBufferDesc->Unpin();
}

void BufMgr::TryFlushBeforeInvalidateBuffer(BufferDesc *baseBufferDesc, PdbId pdbId, FileId fileId)
{
    if (unlikely(fileId == INVALID_VFS_FILE_ID)) {
        return;
    }
RETRY:
    baseBufferDesc->Pin();
    BufferTag bufTag = baseBufferDesc->bufTag;
    if (bufTag.pageId.m_fileId != fileId) {
        baseBufferDesc->Unpin();
        return;
    }

    DstoreLWLockAcquire(&baseBufferDesc->contentLwLock, LW_SHARED);
    uint64 bufferState = GsAtomicReadU64(&(baseBufferDesc->state));
    if (((bufferState & Buffer::BUF_CONTENT_DIRTY) || (bufferState & Buffer::BUF_HINT_DIRTY)) &&
        STORAGE_FUNC_FAIL(WriteBlock(baseBufferDesc))) {
        LWLockRelease(&baseBufferDesc->contentLwLock);
        baseBufferDesc->Unpin();
        StorageAssert(StorageGetErrorCode() != STORAGE_OK);
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Invalidate(%hhu, %hu) by buftag=(%hhu, %hu, %u) flush failed.",
            pdbId, fileId, bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
        goto RETRY;
    }

    LWLockRelease(&baseBufferDesc->contentLwLock);
    baseBufferDesc->Unpin();
}

void BufMgr::InvalidateOneBufferUsingGivenPdbIdOrFileId(BufferDesc *bufferDesc, PdbId pdbId, FileId fileId)
{
    uint64 bufferState = bufferDesc->LockHdr();
    uint64 retryTimes = 0;
    const uint64 retryTimesCnt = 1000;
    if ((bufferState & Buffer::BUF_CR_PAGE) == 0) {
        bufferDesc->UnlockHdr(bufferState);
        /* if only valid pdbId, already FlushAll before */
        /* if valid pdbId&valid fileId, need flush here */
        if (fileId != INVALID_VFS_FILE_ID &&
            ((bufferState & Buffer::BUF_CONTENT_DIRTY) || (bufferState & Buffer::BUF_HINT_DIRTY))) {
            TryFlushBeforeInvalidateBuffer(bufferDesc, pdbId, fileId);
        }
RETRY:
        bufferDesc->Pin();
        bufferState = GsAtomicReadU64(&(bufferDesc->state));
        BufferTag bufTag = bufferDesc->bufTag;
        uint32 bufferHash = m_buftable->GetHashCode(&bufTag);

        if (bufferState & Buffer::BUF_TAG_VALID) {
            m_buftable->LockBufMapping(bufferHash, LW_EXCLUSIVE);
        }

        if (bufferDesc->bufTag.pdbId != pdbId ||
            (fileId != INVALID_VFS_FILE_ID && bufferDesc->bufTag.pageId.m_fileId != fileId)) {
            if (bufferState & Buffer::BUF_TAG_VALID) {
                m_buftable->UnlockBufMapping(bufferHash);
            }
            bufferDesc->Unpin();
            return;
        }
        bufferState = bufferDesc->LockHdr();
        StorageAssert((bufferState & Buffer::BUF_CR_PAGE) == 0);
        PrivateRefCountEntry *ref = thrd->GetBufferPrivateRefCount()->GetPrivateRefcount(bufferDesc);
        if (bufferDesc->GetRefcount() > 1 || (ref != nullptr && ref->refcount > 1)) {
            bufferDesc->UnlockHdr(bufferState);
            if (bufferState & Buffer::BUF_TAG_VALID) {
                m_buftable->UnlockBufMapping(bufferHash);
            }
            bufferDesc->Unpin();
            if (retryTimes % retryTimesCnt == 0) {
                StringInfoData string;
                if (likely(string.init())) {
                    string.append("Failed to invalidate buffer buftag=(%hhu, %hu, %u) state %lu, contentLwLock %p.\n",
                                  bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, bufferState,
                                  &bufferDesc->contentLwLock);
                    ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("%s", string.data));
                } else {
                    ErrLog(DSTORE_WARNING, MODULE_BUFMGR, ErrMsg("Out of memory when log bufferDesc->contentLwLock"));
                }
                DstorePfreeExt(string.data);
                char *lwlockInfo = DSTORE::LWLockDiagnose::GetLWLockStatus();
                if (lwlockInfo != nullptr) {
                    ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
                           ErrMsg("Failed to invalidate buffer buftag=(%hhu, %hu, %u) "
                                  "Lw lock info:%s .",
                                  bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, lwlockInfo));
                    DstorePfree(lwlockInfo);
                }
                char *bucketInfo = DSTORE::BufMgrDiagnose::GetPdBucketLockInfo();
                if (bucketInfo != nullptr) {
                    ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
                           ErrMsg("Failed to invalidate buffer buftag=(%hhu, %hu, %u) "
                                  "Bucket lock info:%s .",
                                  bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, bucketInfo));
                    DstorePfree(bucketInfo);
                }
            }
            retryTimes++;
            goto RETRY;
        }
        ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("Buffer Status Observation: bufTag:(%hhu, %hu, %u)"
            " DropPage lsnInfo(walId %lu glsn %lu plsn %lu) state(%lu)",
            bufferDesc->GetPdbId(), bufferDesc->GetPageId().m_fileId, bufferDesc->GetPageId().m_blockId,
            reinterpret_cast<Page *>(bufferDesc->bufBlock)->GetWalId(),
            reinterpret_cast<Page *>(bufferDesc->bufBlock)->GetGlsn(),
            reinterpret_cast<Page *>(bufferDesc->bufBlock)->GetPlsn(), bufferState));
        bufferDesc->AcquireCrAssignLwlock(LW_EXCLUSIVE);
        if (bufferDesc->HasCrBuffer()) {
            BufferDesc *crBufferDesc = bufferDesc->GetCrBuffer();
            crBufferDesc->Pin();
            InvalidateCrBufferUsingGivenPdbIdOrFileId(crBufferDesc, pdbId, fileId);
        }
        bufferDesc->ReleaseCrAssignLwlock();

        bufferDesc->bufTag.SetInvalid();
        bufferDesc->pageVersionOnDisk = INVALID_PAGE_LSN;
        bufferDesc->fileVersion = INVALID_FILE_VERSION;
        uint64 newBufferState = bufferState & Buffer::BUF_FLAG_RESET_MASK;
        uint64 timestamp = static_cast<uint64>(GetSystemTimeInMicrosecond());
        if (bufferState & Buffer::BUF_TAG_VALID) {
            m_buftable->Remove(&bufTag, bufferHash);
            m_buftable->UnlockBufMapping(bufferHash);
        }
        bufferDesc->UnlockHdr(newBufferState);
        m_buffer_lru->MoveToCandidateList(bufferDesc);
        bufferDesc->Unpin();

        EvictBaseBufferCallback(bufTag, bufferState, timestamp);
    } else {
        bufferDesc->UnlockHdr(bufferState);
    }
}

void BufMgr::StartInvalidateThread(InvalidateThreadContext *context)
{
    InitSignalMask();
    RetStatus ret = g_storageInstance->CreateThreadAndRegister(INVALID_PDB_ID, false, "InvalidateBP", true,
        ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    if (STORAGE_FUNC_FAIL(ret)) {
        context->rc = DSTORE_FAIL;
        return;
    }
    BufferMemChunkWrapper<BufferMemChunk> *memChunkWrapper = nullptr;
    for (;;) {
        thrd->RefreshWorkingVersionNum();
        context->memChunkQueueLock.Acquire();
        memChunkWrapper = context->nextMemChunk;
        if (memChunkWrapper != nullptr) {
            context->nextMemChunk = memChunkWrapper->GetNext();
        }
        context->memChunkQueueLock.Release();
        if (memChunkWrapper == nullptr) {
            break;
        }
        BufferMemChunk *memChunk = memChunkWrapper->memChunk;
        memChunk->LockMemChunk(LW_SHARED);
        for (Size i = 0; i < memChunk->GetSize(); ++i) {
            BufferDesc *bufferDesc = memChunk->GetBufferDesc(i);
            if (bufferDesc->bufTag.pdbId != context->pdbId ||
                (context->fileId != INVALID_VFS_FILE_ID && bufferDesc->bufTag.pageId.m_fileId != context->fileId)) {
                continue;
            } else {
                InvalidateOneBufferUsingGivenPdbIdOrFileId(bufferDesc, context->pdbId, context->fileId);
            }
        }
        memChunk->UnlockMemChunk();
    }
    g_storageInstance->UnregisterThread();
}

RetStatus BufMgr::InvalidateUsingGivenPdbIdOrFileId(PdbId pdbId, FileId fileId)
{
    InvalidateThreadContext context;
    context.pdbId = pdbId;
    context.fileId = fileId;
    context.memChunkQueueLock.Init();
    context.rc = DSTORE_SUCC;
    std::thread *InvalidateThreads[INVALIDATE_THREAD_NUM];

    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_BUFFER, ErrMsg("pdb %u is nullptr", pdbId));
    WalStream *walStream = nullptr;
    if (pdb->GetPdbStatus() == PdbStatus::PDB_STATUS_OPENED_READ_WRITE) {
        walStream = pdb->GetWalMgr()->GetWalStreamManager()->GetWritingWalStream();
        StorageReleasePanic(STORAGE_VAR_NULL(walStream), MODULE_BUFFER,
            ErrMsg("pdb %u GetWritingWalStream is nullptr", pdbId));
    }
    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("Invalidate pdbId %hhu/fileId %hu start,  "
        "wal(%lu) maxAppendPlsn is %lu.", pdbId, fileId, walStream ? walStream->GetWalId() : 0,
        walStream ? walStream->GetMaxAppendedPlsn() : 0));

    m_bufferMemChunkList->LockBufferMemChunkList(LW_SHARED);
    context.nextMemChunk = m_bufferMemChunkList->GetMemChunkIterator<BufferMemChunk>();

    for (uint32 i = 0; i < INVALIDATE_THREAD_NUM; i++) {
        InvalidateThreads[i] = new std::thread(&BufMgr::StartInvalidateThread, this, &context);
    }
    for (uint32 i = 0; i < INVALIDATE_THREAD_NUM; i++) {
        InvalidateThreads[i]->join();
        delete InvalidateThreads[i];
        InvalidateThreads[i] = nullptr;
    }
    m_bufferMemChunkList->UnlockBufferMemChunkList();
    if (STORAGE_FUNC_FAIL(context.rc)) {
        storage_set_error(BUFFER_ERROR_INVALIDATE_FAILED);
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Invalidate thread fail, pdbId %hhu/fileId %hu.", pdbId, fileId));
        return DSTORE_FAIL;
    }
    EvictBaseBufferInstantly(pdbId, fileId);
    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("Invalidate pdbId %hhu/fileId %hu finish, "
        "wal(%lu) maxFlushedPlsn is %lu.", pdbId, fileId, walStream ? walStream->GetWalId() : 0,
        walStream ? walStream->GetMaxFlushedPlsn() : 0));
    return context.rc;
}

RetStatus BufMgr::InvalidateUsingGivenPdbId(PdbId pdbId)
{
    return InvalidateUsingGivenPdbIdOrFileId(pdbId);
}

RetStatus BufMgr::InvalidateUsingGivenFileId(PdbId pdbId, FileId fileId)
{
    return InvalidateUsingGivenPdbIdOrFileId(pdbId, fileId);
}

void BufMgr::RemoveBufferFromLruAndHashTable(BufferDesc *bufferDesc)
{
    StorageAssert(m_buftable->IsBufMappingLockedByMe(bufferDesc));
    StorageAssert(bufferDesc->IsHdrLocked());

    /*
     * Move the base buffer back to candidate list if it's not being invalidated for bufferpool shrink or
     * previously unevictable.
     * During bufferpool shrink, the buffer is popped from invalidation list before invalidated
     * and it doesn't belong to any list at the moment.
     */
    if (!bufferDesc->lruNode.IsInPendingState()) {
        m_buffer_lru->MoveToCandidateList(bufferDesc);
    }

    /*
     * 1. Clear out the buffer's pageId and flags.
     * Store the old flags into bufferFlags before we clear out the flags.
     */
    uint64 bufferState = GsAtomicReadU64(&(bufferDesc->state));
    uint64 bufferFlags = bufferState & Buffer::BUF_FLAG_MASK;
    BufferTag bufTag = bufferDesc->bufTag;
    uint32 bufferHash = m_buftable->GetHashCode(&bufTag);
    bufferDesc->bufTag.SetInvalid();
    bufferDesc->fileVersion = INVALID_FILE_VERSION;
    bufferState &= Buffer::BUF_FLAG_RESET_MASK;

    /*
     * 2. Remove the buffer from the lookup hashtable, if it was in there.
     */
    if (bufferFlags & Buffer::BUF_TAG_VALID) {
        m_buftable->Remove(&bufTag, bufferHash);
    }

    bufferDesc->UnlockHdr(bufferState);
}

RetStatus BufMgr::LockContent(BufferDesc* bufferDesc, LWLockMode mode)
{
    /*
     * We ignore the CR buffer here.
     * Because the buffer is pinned by thread locally, no one can change the BUF_CR_PAGE flag,
     * so we get the snapshot of buffer state, and check the BUF_CR_PAGE flag directly, don't care about BUF_LOCKED.
     */
    uint64 state = bufferDesc->GetState(false);
    if (state & Buffer::BUF_CR_PAGE) {
        return DSTORE_SUCC;
    }

    StorageAssert(bufferDesc->IsPinnedPrivately());
#ifdef DSTORE_USE_ASSERT_CHECKING
    if (bufferDesc->IsContentLocked(LW_SHARED) && (mode == LW_SHARED)) {
        BufferTag bufTag = bufferDesc->GetBufferTag();
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
            ErrMsg("buffer bufTag:(%hhu, %hu, %u) is locked multiple times in share mode.",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
    }
#endif
    if (mode == LW_EXCLUSIVE) {
        DstoreLWLockAcquire(&bufferDesc->contentLwLock, LW_EXCLUSIVE);
    } else {
        DstoreLWLockAcquire(&bufferDesc->contentLwLock, LW_SHARED);
    }
    if (mode == LW_EXCLUSIVE) {
#ifdef DSTORE_USE_ASSERT_CHECKING
        (bufferDesc->GetPage())->SetChecksum();
#endif
        bufferDesc->UpdateLastModifyTimeToNow();
        bufferDesc->WaitIfIsWritingWal();
        bufferDesc->InvalidateCrPage();
    }
    return DSTORE_SUCC;
}

bool BufMgr::TryLockContent(BufferDesc* bufferDesc, LWLockMode mode)
{
    /*
     * We ignore the CR buffer here.
     * Because the buffer is pinned by thread locally, no one can change the BUF_CR_PAGE flag,
     * so we get the snapshot of buffer state, and check the BUF_CR_PAGE flag directly, don't care about BUF_LOCKED.
     */
    uint64 state = bufferDesc->GetState(false);
    if (state & Buffer::BUF_CR_PAGE) {
        return false;
    }
    StorageAssert(bufferDesc->IsPinnedPrivately());
    if (bufferDesc->IsContentLocked(LW_SHARED) && (mode == LW_SHARED)) {
        BufferTag bufTag = bufferDesc->GetBufferTag();
        ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
            ErrMsg("buffer bufTag:(%hhu, %hu, %u) is locked multiple times in share mode.",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
    }
    bool isLockSuc = DstoreLWLockConditionalAcquire(&bufferDesc->contentLwLock, mode);
    if (isLockSuc && mode == LW_EXCLUSIVE) {
        bufferDesc->WaitIfIsWritingWal();
    }
    return isLockSuc;
}

void BufMgr::PinAndLock(BufferDesc *bufferDesc, LWLockMode mode)
{
    bufferDesc->Pin();
    (void)LockContent(bufferDesc, mode);
}

RetStatus BufMgr::InvalidateMemChunkForDebug(Size memChunkId)
{
    BufferMemChunk *memChunk = m_bufferMemChunkList->GetMemChunk(memChunkId);
    if (STORAGE_VAR_NULL(memChunk)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("MemChunk is nullptr."));
        return DSTORE_FAIL;
    }
    for (Size index = 0; index < memChunk->GetSize(); index++) {
        BufferDesc *bufferDesc = memChunk->GetBufferDesc(index);
        bufferDesc->Pin();
        m_buffer_lru->Remove(bufferDesc);
        bufferDesc->Unpin();
    }

    /* Step 1: Set the MemChunk is Removing. */
    m_bufferMemChunkList->SetRemovingMemChunkId(memChunk->GetMemChunkId());

    /* Step 2: Increase the size change sequence to trigger re-read NE buffer. */
    m_bufferMemChunkList->IncreaseSizeChangeSequence();

    /* Step 3: Invalidate NE buffer(and all other buffer) one by one. */
    for (Size index = 0; index < memChunk->GetSize(); index++) {
        BufferDesc *bufferDesc = memChunk->GetBufferDesc(index);
        uint64 state = bufferDesc->LockHdr();
        if ((state & Buffer::BUF_CR_PAGE) == 0) {
            (void)Invalidate(bufferDesc);
        } else {
        }
    }
    return DSTORE_SUCC;
}

void BufMgr::UnlockContent(BufferDesc *bufferDesc, UNUSE_PARAM BufferPoolUnlockContentFlag flag)
{
    /*
     * We ignore the CR buffer here.
     * Because the buffer is pinned by thread locally, no one can change the BUF_CR_PAGE flag,
     * so we get the snapshot of buffer state, and check the BUF_CR_PAGE flag directly, don't care about BUF_LOCKED.
     */
    StorageReleasePanic(STORAGE_VAR_NULL(bufferDesc), MODULE_BUFMGR, ErrMsg("bufferDesc is nullptr."));
    uint64 state = bufferDesc->GetState(false);
    if (state & Buffer::BUF_CR_PAGE) {
        return;
    }

    StorageAssert(bufferDesc->IsPinnedPrivately());
#ifdef DSTORE_USE_ASSERT_CHECKING
    state = bufferDesc->LockHdr();
    if (bufferDesc->IsContentLocked(LW_EXCLUSIVE) && !(state & Buffer::BUF_CONTENT_DIRTY) && flag.IsCheckCrc()) {
        Page *page = bufferDesc->GetPage();
        StorageAssert(page->CheckPageCrcMatch());
    }
    bufferDesc->UnlockHdr(state);
#endif
    LWLockRelease(&bufferDesc->contentLwLock);
}

void BufMgr::UnlockAndRelease(BufferDesc *bufferDesc, BufferPoolUnlockContentFlag flag)
{
    UnlockContent(bufferDesc, flag);
    Release(bufferDesc);
}

void BufMgr::FinishCrBuild(BufferDesc *crBufferDesc, CommitSeqNo pageMaxCsn)
{
    StorageAssert(crBufferDesc != INVALID_BUFFER_DESC);
    StorageAssert(crBufferDesc->IsCrPage());

    /* Step1: Get base buffer */
    BufferDesc *baseBufferDesc = crBufferDesc->GetCrBaseBuffer();
    StorageAssert(baseBufferDesc != INVALID_BUFFER_DESC);
    StorageAssert(baseBufferDesc->IsCrAssignLocked(LW_EXCLUSIVE));
    StorageAssert(pageMaxCsn != INVALID_CSN);

    /* Step2: Set CR usable */
    baseBufferDesc->crInfo.SetCrPageMaxCsn(pageMaxCsn);
    baseBufferDesc->SetCrUsable();

    /* Step3: Set BUF_VALID flag to indicate that the CR page is valid. */
    uint64 state = crBufferDesc->LockHdr();
    state |= Buffer::BUF_VALID;
    crBufferDesc->UnlockHdr(state);

    /* Step4: release CR assign lock */
    baseBufferDesc->ReleaseCrAssignLwlock();
#ifdef DSTORE_USE_ASSERT_CHECKING
    (void)++m_bufPoolCnts.crBuildCnt;
#endif
}

/*
 * DoWhenBufferpoolResize
 *
 * This is a method for elastic bufferpool.
 * @param   bufferPoolNewSize: 64 bit unsigned integer as the new bufferpool size (blocks).
 *          This number will be rounded up to the next memchunk level.
 * @param   outputMessage: The message to be displayed to the user when the operation finishes.
 * @return  Success or failure.
  */
RetStatus BufMgr::DoWhenBufferpoolResize(Size bufferPoolNewSize, StringInfoData &outputMessage)
{
    ErrLog(DSTORE_LOG, MODULE_BUFMGR,
        ErrMsg("Try to resize the bufferpool to %lu (blocks).", bufferPoolNewSize));

    if (!DstoreLWLockConditionalAcquire(&m_sizeLock, LW_EXCLUSIVE)) {
        outputMessage.append("Another operation is in progress. The bufferpool size is unchanged.");
        ErrLog(DSTORE_WARNING, MODULE_BUFMGR, ErrMsg("%s", outputMessage.data));
        storage_set_error(BUFFER_WARNING_FAIL_TO_RESIZE);
        return DSTORE_FAIL;
    }

    RetStatus rc = DSTORE_SUCC;
    bool isUpSize = (bufferPoolNewSize > m_size);

    /*
     * Step 1: Calculate the number of memchunks to add or drop for this resize.
     *
     * Align the new bufferpool size by rounding down to the multiples of
     * the least common multiple (LCM) of m_lru_partitions and memchunk size.
     */
    Size memChunkSize = m_bufferMemChunkList->GetNumOfBufInMemChunk();
    bufferPoolNewSize = DstoreRoundDown<Size>(bufferPoolNewSize, DstoreMax(memChunkSize, m_lru_partitions));

    Size bufferPoolDeltaSize = isUpSize ? (bufferPoolNewSize - m_size) : (m_size - bufferPoolNewSize);
    Size bufferPoolMinSize = m_bufferMemChunkList->GetMinBufferPoolSize();
    Size numMemChunksToChange = bufferPoolDeltaSize / memChunkSize;
    if (bufferPoolNewSize < bufferPoolMinSize) {
        outputMessage.append("Cannot resize the bufferpool below the minimal required size %lu (blocks).",
            bufferPoolMinSize);
        rc = DSTORE_FAIL;
        goto EXIT;
    }
    if (numMemChunksToChange == 0) {
        /*
         * There's nothing to resize after rounding the new bufferpool size.
         * For example, if you try to reduce bufferpool size by 1 byte, it will hit this place.
         */
        rc = DSTORE_FAIL;
        goto EXIT;
    }
    ErrLog(DSTORE_LOG, MODULE_BUFMGR,
        ErrMsg("Try to %s memchunks to the bufferpool. There are %lu existing memchunks.",
            (isUpSize ? "add" : "drop"), m_bufferMemChunkList->GetSize()));

    /*
     * Step 2: Add or drop memchunks.
     */
    if (isUpSize) {
        for (Size i = 0; i < numMemChunksToChange; i++) {
            rc = DoWhenBufferpoolResizeHelper(bufferPoolNewSize, 1, isUpSize, outputMessage);
            if (STORAGE_FUNC_FAIL(rc)) {
                ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
                    ErrMsg("Try to add the %lu memchunks to bufferpool failed. Error code 0x%llx.",
                        i, static_cast<unsigned long long>(StorageGetErrorCode())));
                break;
            }
        }
    } else {
        rc = DoWhenBufferpoolResizeHelper(bufferPoolNewSize, numMemChunksToChange, isUpSize, outputMessage);
    }

EXIT:
    if (rc == DSTORE_SUCC) {
        outputMessage.append("The bufferpool is resized from %lu (blocks) to %lu (blocks).",
            m_size, bufferPoolNewSize);
        ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("Succeeded on resizing the bufferpool. %s",
            outputMessage.data));

        /*
         * Update bufferpool size. We're already under m_sizeLock protection in exclusive mode.
         */
        m_size = bufferPoolNewSize;
    } else {
        outputMessage.append("The bufferpool size %lu (blocks) is unchanged.", m_size);
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Failed to resize the bufferpool. %s",
            outputMessage.data));
        storage_set_error(BUFFER_WARNING_FAIL_TO_RESIZE);
    }

    if (LWLockHeldByMe(&m_sizeLock)) {
        LWLockRelease(&m_sizeLock);
    }
    return rc;
}

RetStatus BufMgr::DoWhenBufferpoolResizeHelper(Size bufferPoolNewSize, Size numMemChunksToChange,
    bool isUpSize, StringInfoData &outputMessage)
{
    return DoWhenBufferpoolResizeTemplate<BufferMemChunk>(bufferPoolNewSize, numMemChunksToChange,
        isUpSize, outputMessage);
}

/*
 * Elastic bufferpool shrink.
 *
 * Try to remove a certain number of memchunks from the bufferpool to achieve a smaller size.
 * First, the memchunks are sorted by temperature, and then the attempt to invalidate a memchunk
 * will start from the coldest one. If more memchunks need to be invalidated, it will move onto
 * the next coldest one.
 * The hottest memchunk is protected and we will never attempt to invalidate it.
 */
RetStatus BufMgr::DoWhenBufferpoolDownSize(const Size numMemChunks)
{
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));

    RetStatus rc = DSTORE_SUCC;
    Size numMemChunksToRemove = numMemChunks;
    Size numMemChunksRemovable = m_bufferMemChunkList->GetSize() - m_bufferMemChunkList->GetMinEvictableSize();
    BufferMemChunkWrapper<BufferMemChunk> **completeList = static_cast<BufferMemChunkWrapper<BufferMemChunk> **>
        (DstorePalloc(sizeof(BufferMemChunkWrapper<BufferMemChunk> *) * numMemChunks));
    if (STORAGE_VAR_NULL(completeList)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("alloc memory for completeList fail %lu.", numMemChunks));
        return DSTORE_FAIL;
    }
    StorageAssert(numMemChunksToRemove <= numMemChunksRemovable);

    m_bufferMemChunkList->LockBufferMemChunkList(LW_SHARED);
    BufferMemChunkWrapper<BufferMemChunk> **sortedMemChunkWrapperArray =
        m_bufferMemChunkList->SortByTemperature<BufferMemChunk>();
    if (STORAGE_VAR_NULL(sortedMemChunkWrapperArray)) {
        m_bufferMemChunkList->UnlockBufferMemChunkList();
        DstorePfree(completeList);
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("alloc memory for sortedMemChunkWrapperArray fail."));
        return DSTORE_FAIL;
    }
    StorageAssert(sortedMemChunkWrapperArray != nullptr);
    for (Size i = 0; (i < numMemChunksRemovable) && (numMemChunksToRemove > 0); i++) {
        BufferMemChunk *memChunk = sortedMemChunkWrapperArray[i]->memChunk;
        if (STORAGE_FUNC_SUCC(DoWhenBufferpoolDownSizeHelper(memChunk))) {
            completeList[numMemChunks - numMemChunksToRemove] = sortedMemChunkWrapperArray[i];
            numMemChunksToRemove--;
        }
        /* If it fails to invalidate one memchunk, it will try the next coldest memchunk. */
    }

    /*
     * If it fails to invalidate enough number of memchunks, the memchunks that were invalidated previously
     * will be restored to LRU list.
     */
    if (numMemChunksToRemove != 0) {
        for (Size i = 0; i < numMemChunks - numMemChunksToRemove; i++) {
            BufferMemChunkWrapper<BufferMemChunk> *memChunkWrapper = completeList[i];
            m_buffer_lru->RestoreMemChunkToLru(memChunkWrapper->memChunk);
        }
        rc = DSTORE_FAIL;
        storage_set_error(BUFFER_WARNING_FAIL_TO_SHRINK);
    }
    DstorePfreeExt(sortedMemChunkWrapperArray);
    m_bufferMemChunkList->UnlockBufferMemChunkList();

    /* If it succeeds, proceed to deallocate the memchunks. */
    if (numMemChunksToRemove == 0) {
        m_bufferMemChunkList->LockBufferMemChunkList(LW_EXCLUSIVE);
        for (Size i = 0; i < numMemChunks; i++) {
            BufferMemChunkWrapper<BufferMemChunk> *memChunkWrapper = completeList[i];
            m_bufferMemChunkList->DeallocateOneMemChunk<BufferMemChunk>(memChunkWrapper);
        }
        m_bufferMemChunkList->UnlockBufferMemChunkList();
    }

    /* Sanity check. */
    for (Size i = 0; i < m_lru_partitions; i++) {
        StorageAssert(GetBufLruList(i)->GetInvalidationList()->Length() == 0);
    }

    DstorePfree(completeList);
    return rc;
}

/*
 * Invalidates one memchunk.
 * Similar to InvalidateMemChunkForDebug(). Issue: should we merge the 2 functions?
 */
RetStatus BufMgr::DoWhenBufferpoolDownSizeHelper(BufferMemChunk *memChunk)
{
    RetStatus rc = DSTORE_SUCC;
    std::thread **workerThreads = (std::thread **) DstorePalloc0(m_lru_partitions * sizeof(std::thread *));
    if (STORAGE_VAR_NULL(workerThreads)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("alloc memory for workerThreads fail %lu.", m_lru_partitions));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("Start invalidating memchunk #%lu, temperature = %u.",
        memChunk->GetMemChunkId(), memChunk->GetHotPageCount()));
    /* Move all buffers to invalidation list to make them unavailable to the public. */
    m_buffer_lru->RemoveMemChunkFromLru(memChunk);

    /* Step 1: Set the MemChunk is being removed. */
    m_bufferMemChunkList->SetRemovingMemChunkId(memChunk->GetMemChunkId());
    /* Step 2: Increase the size change sequence to trigger re-read NE buffer. */
    m_bufferMemChunkList->IncreaseSizeChangeSequence();

    /* Step 3: Spawn worker threads to cleanup invalidation list. */
    for (Size i = 0; i < m_lru_partitions; i++) {
        workerThreads[i] = new std::thread([&, this, i] {
            this->InvalidateMemChunkWorker(i, &rc);
        });
    }
    for (Size i = 0; i < m_lru_partitions; i++) {
        workerThreads[i]->join();
        delete workerThreads[i];
        workerThreads[i] = nullptr;
    }
    DstorePfree(workerThreads);
    workerThreads = nullptr;

    if STORAGE_FUNC_FAIL(rc) {
        /* Move all buffers to candidate/LRU list to make them available to the public again. */
        m_buffer_lru->RestoreMemChunkToLru(memChunk);
        storage_set_error(BUFFER_WARNING_FAIL_TO_INVALIDATE_MEMCHUNK);
    }
    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("%s on invalidating memchunk #%lu.",
        (STORAGE_FUNC_SUCC(rc)) ? ("Succeeded") : ("Failed"), memChunk->GetMemChunkId()));
    return rc;
}

/*
 * The worker thread to monitor the invalidation list in each LRU partition until they become empty.
 * For every iteration, the thread pops a buffer and checks if it's ready to be invalidated.
 * If the buffer is ready, then invalidate it (flush the page, remove the buffer bufTag from hash table, etc.)
 *      and do not return it to the list.
 * If the buffer is not ready, then push the buffer back to the invalidation list and try again later.
 *
 * The thread is seeded by lru partition index. It starts with monitoring the invalidation list
 * at the seeded lru partition. Once the list is finished, it will move on to monitor lists
 * of other lru partitions.
 */
void BufMgr::InvalidateMemChunkWorker(const Size seed, UNUSE_PARAM RetStatus *rc)
{
    StorageAssert(rc != nullptr);
    InitSignalMask();
    (void)g_storageInstance->CreateThreadAndRegister(INVALID_PDB_ID, false, "InvalidMemWrk", true,
                                                     ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("Invalidation worker starts for seed = %lu.\n", seed));

    uint32 retryTimes = 0;
    SpinDelayStatus delayStatus = InitSpinDelay(__FILE__, __LINE__);
#ifdef DSTORE_USE_ASSERT_CHECKING
    char bufferInfo[BUFFER_DESC_FORMAT_SIZE];
#endif
    for (;;) {
        bool isIdle = true;
        for (Size i = seed; i < seed + m_lru_partitions; i++) {
            thrd->RefreshWorkingVersionNum();
            Size lruIndex = i % m_lru_partitions;
            LruGenericList *invalidationList = GetBufLruList(lruIndex)->GetInvalidationList();
            LruNode *lruNode = invalidationList->PopHead();

            /* If this list is empty, move onto the next list. */
            if (lruNode == nullptr) {
                continue;
            }

            isIdle = false;
            BufferDesc *bufferDesc = lruNode->GetValue<BufferDesc>();
            if (STORAGE_FUNC_FAIL(InvalidateMemChunkWorkerHelper(bufferDesc))) {
                retryTimes++;
                invalidationList->PushTail(lruNode);

#ifdef DSTORE_USE_ASSERT_CHECKING
                uint64 bufferState = GsAtomicReadU64(&(bufferDesc->state));
                bufferDesc->PrintBufferDesc(bufferInfo, BUFFER_DESC_FORMAT_SIZE);
                ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR,
                       ErrMsg("Failed to invalidate buffer %s, refcount = %lu, isDirty = %lu, "
                              "isCrPage = %lu, lru partition = %lu, invalidation list size = %lu, retryTimes = %u.\n",
                              bufferInfo, (bufferState & Buffer::BUF_REFCOUNT_MASK),
                              (bufferState & Buffer::BUF_CONTENT_DIRTY) | (bufferState & Buffer::BUF_HINT_DIRTY),
                              (bufferState & Buffer::BUF_CR_PAGE), lruIndex, invalidationList->Length(), retryTimes));
#endif
                /* When it fails to invalidate a buffer, perform a delay to avoid busy waiting. */
                PerformSpinDelay(&delayStatus, bufferDesc->contentLwLock.spinsPerDelay);
            }

            /* When the thread is not idle, it only processes one buffer per iteration. */
            break;
        }
        if (isIdle) {
            ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
                   ErrMsg("Invalidation worker exits for seed = %lu, retryTimes = %u.\n", seed, retryTimes));
            break;
        }
        /* rc is shared by all worker threads. Any thread hitting the timeout will trigger the others to fail. */
        if (retryTimes > INVALIDATE_BUFFER_RETRY_TIMES || STORAGE_FUNC_FAIL(*rc)) {
            ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
                   ErrMsg("Invalidation worker timeout for seed = %lu, retryTimes = %u.\n", seed, retryTimes));
            *rc = DSTORE_FAIL;
            storage_set_error(BUFFER_WARNING_INVALIDATION_WORKER_TIMEOUT);
            break;
        }
    }
    g_storageInstance->UnregisterThread();
}

RetStatus BufMgr::InvalidateMemChunkWorkerHelper(BufferDesc *bufferDesc)
{
    RetStatus rc = DSTORE_SUCC;
    uint64 bufferState = bufferDesc->LockHdr();
    if (bufferDesc->GetRefcount() == 0) {
        if ((bufferState & Buffer::BUF_CR_PAGE) == 0) {
            /*
             * If this buffer is base buffer, invalidate it.
             * BufMgr::Invalidate() expects the buffer header is locked before passing in, and it always
             * returns the buffer header as unlocked no matter the invalidation has succeededed or not.
             */
            rc = Invalidate(bufferDesc);

            StorageReleasePanic(bufferDesc->IsHdrLocked(), MODULE_BUFMGR,
                ErrMsg("The buffer header should be unlocked."));
            StorageReleasePanic(m_buftable->IsBufMappingLockedByMe(bufferDesc), MODULE_BUFMGR,
                ErrMsg("The hash table should be unlocked."));
        } else {
            /*
             * BufMgr::InvalidateCrBuffer() expects the buffer header is unlocked.
             */
            bufferDesc->UnlockHdr(bufferState);

            /*
             * If this buffer is CR buffer,
             *      If the base buffer also needs to be invalidated, do nothing here but let the base buffer
             *          invalidation handle this buffer.
             *      If the base buffer doesn't need to be invalidated, detach the CR buffer from base buffer
             *          and the caller will reload the CR page when it's needed.
             */
            BufferDesc *baseBufferDesc = bufferDesc->GetCrBaseBuffer();
            StorageReleasePanic(baseBufferDesc == nullptr, MODULE_BUFMGR,
                ErrMsg("CR buffer must be associated with a base buffer."));

            if (!baseBufferDesc->lruNode.IsInInvalidationList()) {
                BufferTag bufTag = bufferDesc->GetBufferTag();

                ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
                    ErrMsg("Invalidate page bufTag:(%hhu, %hu, %u) CR buffer (lru type = %hhu), "
                        "but not base buffer (lru type = %hhu).\n",
                        bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                        static_cast<uint8>(bufferDesc->lruNode.m_type.load()),
                        static_cast<uint8>(baseBufferDesc->lruNode.m_type.load())));

                baseBufferDesc->AcquireCrAssignLwlock(LW_EXCLUSIVE);
                rc = InvalidateCrBuffer(bufferDesc);
                baseBufferDesc->ReleaseCrAssignLwlock();
            }
        }
    } else {
        bufferDesc->UnlockHdr(bufferState);
        rc = DSTORE_FAIL;
    }
    return rc;
}

RetStatus BufMgr::BBoxBlackListGetBuffer(char*** chunk_addr, uint64** chunk_size, uint64* numOfChunk)
{
    RetStatus rc = DSTORE_SUCC;
    uint64 count = 0;

    m_bufferMemChunkList->LockBufferMemChunkList(LW_SHARED);
    uint64 totalChunks = m_bufferMemChunkList->GetSize();

    *chunk_addr = new char* [totalChunks];
    *chunk_size = new uint64[totalChunks];
    if (*chunk_addr == nullptr || *chunk_size == nullptr) {
        m_bufferMemChunkList->UnlockBufferMemChunkList();
        return DSTORE_FAIL;
    }

    BufferMemChunkWrapper<BufferMemChunk>* memChunkWrapper =
        m_bufferMemChunkList->GetMemChunkIterator<BufferMemChunk>();

    while (memChunkWrapper != nullptr && count < totalChunks) {
        memChunkWrapper->memChunk->LockMemChunk(LW_SHARED);
        (*chunk_size)[count] = memChunkWrapper->memChunk->GetBufferBlockSize();
        (*chunk_addr)[count] = memChunkWrapper->memChunk->GetRawBufferBlock();
        memChunkWrapper->memChunk->UnlockMemChunk();

        count++;
        memChunkWrapper = memChunkWrapper->GetNext();
    }

    *numOfChunk = count;
    m_bufferMemChunkList->UnlockBufferMemChunkList();

    return rc;
}

void BufMgr::InitMemChunkAndLru()
{
    InitMemChunkAndLruTemplate<BufferMemChunk>();
}

void BufMgr::DestroyMemChunkAndLru()
{
    DestroyMemChunkAndLruTemplate<BufferMemChunk>();
}

void BufMgr::ResetBufferInitialPageInfo(__attribute__((unused)) BufferDesc *bufferDesc)
{}

void BufMgr::EvictBaseBufferCallback(__attribute__((unused)) const BufferTag &bufTag,
                                     __attribute__((unused)) uint64 bufState,
                                     __attribute__((unused)) uint64 timestamp)
{}

void BufMgr::EvictBaseBufferInstantly(__attribute__((unused)) PdbId pdbId, __attribute__((unused)) FileId fileId)
{}

BufferDesc *BufMgr::TryToReuseBufferForBasePage(const BufferTag &bufTag, bool *isValid, bool *needRetry,
    bool* otherInsertHash, BufferRing bufRing /* = nullptr */)
{
    uint64 bufState;
    StorageStat *stat = g_storageInstance->GetStat();
    /* STEP1: Get a free buffer from BufLruList or BufRing. */
    stat->m_reportWaitStatus(
        static_cast<uint32_t>(GsStatWaitState::STATE_BUFFER_GET_FREEBUF));
    BufferDesc *freeBuffer = GetFreeBuffer(&bufTag, needRetry, bufRing);
    stat->m_reportWaitStatus(static_cast<uint32_t>(GsStatWaitState::STATE_WAIT_UNDEFINED));
    if (freeBuffer == INVALID_BUFFER_DESC) {
        return freeBuffer;
    }

    /* STEP2: try to reuse free buffer */
    BufferDesc *usedBuffer;
    bool reuseSuccess = false;
    if (freeBuffer->IsCrPage()) {
        reuseSuccess = ReuseCrBufferForBasePage(bufTag, freeBuffer, &usedBuffer);
        /* For the cr buffer, if the make free operation succeeds, the cr buffer is added to the ring. */
        if (bufRing != nullptr && !bufRing->curWasInRing) {
            bufRing->AddFreeBufToRing(freeBuffer);
        }
    } else {
        reuseSuccess = ReuseBaseBufferForBasePage(bufTag, freeBuffer, &usedBuffer, bufRing);
        if (reuseSuccess && bufRing != nullptr) {
            /* reuse succeed, add the buffer to the buffer ring. */
            bufRing->AddFreeBufToRing(freeBuffer);
        }
    }

    /* STEP3: check if the buffer is valid now */
    if (reuseSuccess) {
        /* the buffer is surely invalid */
        *isValid = false;
    } else {
        if (usedBuffer != INVALID_BUFFER_DESC) {
            /* some thread has allocated a free buffer for this page,
             * we should check the state of the buffer to decide if it should start i/o to load page from storage
             */
            *otherInsertHash = true;
            bufState = usedBuffer->GetState();
            *isValid = (bufState & Buffer::BUF_VALID) != 0;
        }
    }

    *needRetry = (usedBuffer == INVALID_BUFFER_DESC);
    return usedBuffer;
}

BufferDesc *BufMgr::MakeBaseBufferFree(BufferDesc *baseCandidateBuffer, bool *needRetry,
                                       BufferRing bufRing /* = nullptr */)
{
    StorageAssert(!baseCandidateBuffer->IsCrPage());
    BufferDesc *freeBuffer = INVALID_BUFFER_DESC;
    baseCandidateBuffer->AcquireCrAssignLwlock(LW_SHARED);
    bool hasCrBuffer = baseCandidateBuffer->HasCrBuffer();
    baseCandidateBuffer->ReleaseCrAssignLwlock();
    /* If the candidate buffer has associate CR buffer, try to find a free one */
    if (hasCrBuffer) {
        freeBuffer = FindFreeCrBufferFromBaseBuffer(baseCandidateBuffer, needRetry);
        if (freeBuffer != INVALID_BUFFER_DESC) {
            /*
             * find a free CR buffer from base buffer, release the candidate base buffer,
             * and use the CR buffer just get as free buffer.
             */
            m_buffer_lru->PushBufferBackToLru(baseCandidateBuffer, false);
            /* release candidateBuffer */
            baseCandidateBuffer->Unpin();
            return freeBuffer;
        }
    } else {
        /* flush buffer if it's dirty */
        uint64 state = baseCandidateBuffer->GetState();
        if ((state & Buffer::BUF_CONTENT_DIRTY) && (STORAGE_FUNC_FAIL(TryFlush(baseCandidateBuffer)))) {
            /* flush fails, the buffer is dirty, delete it from the ring. */
            if (bufRing != nullptr) {
                bufRing->RemoveBufInRing(baseCandidateBuffer);
            }
            goto EXIT;
        }

        /* recheck if the base buffer has CR buffer */
        baseCandidateBuffer->AcquireCrAssignLwlock(LW_SHARED);
        hasCrBuffer = baseCandidateBuffer->HasCrBuffer();
        baseCandidateBuffer->ReleaseCrAssignLwlock();
        if (!hasCrBuffer) {
            *needRetry = false;
            return baseCandidateBuffer;
        }
    }

EXIT:
    BufferTag bufTag = baseCandidateBuffer->GetBufferTag();

    /*  give up free the base buffer. */
    ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
        ErrMsg("make base buffer bufTag:(%hhu, %hu, %u) free fail, try other buffers.",
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));

    m_buffer_lru->PushBufferBackToLru(baseCandidateBuffer, false);
    baseCandidateBuffer->Unpin();
    *needRetry = true;
    return INVALID_BUFFER_DESC;
}

bool BufMgr::MakeCrBufferFree(BufferDesc *crBufferDesc)
{
    StorageAssert(crBufferDesc != nullptr);
    StorageAssert(crBufferDesc->IsCrPage());
    BufferDesc *baseBufferDesc = crBufferDesc->GetCrBaseBuffer();
    StorageAssert(baseBufferDesc != INVALID_BUFFER_DESC);
    StorageAssert(baseBufferDesc->IsCrAssignLocked()); /* make sure we hold cr assign lock here */

    uint64 bufState = crBufferDesc->LockHdr();
    PrivateRefCountEntry *ref = thrd->GetBufferPrivateRefCount()->GetPrivateRefcount(crBufferDesc);
    if (crBufferDesc->GetRefcount() != 1 || /* the buffer is not pinned by other thread */
        ref == nullptr || ref->refcount != 1 /* the buffer is pinned only once by ourselves */) {
        /* unlock buffer header */
        crBufferDesc->UnlockHdr(bufState);
        /* make the buffer free fail */
        return false;
    }

    /* remove the candidate buffer from cr list in the base buffer */
    baseBufferDesc->SetCrUnusable();
    baseBufferDesc->crInfo.RemoveCrBuffer();

    /* clear buffer state */
    bufState &= Buffer::BUF_FLAG_RESET_MASK;
    bufState |= Buffer::BUF_CR_PAGE;

    /* unlock buffer header */
    crBufferDesc->UnlockHdr(bufState);
    /* make free success */
    return true;
}

BufferDesc *BufMgr::FindFreeCrBufferFromBaseBuffer(BufferDesc *baseBuffer, bool *needRetry)
{
    StorageAssert(!baseBuffer->IsCrPage());
    /* Try to acquire CR assign lock in the base buffer in Exclusive mode */
    if (!baseBuffer->TryAcquireCrAssignLwlock(LW_EXCLUSIVE)) {
        return INVALID_BUFFER_DESC;
    }
    uint64 state = baseBuffer->LockHdr();
    /* scan cr list and try to free one of cr buffer */
    BufferDesc *crBuffer = baseBuffer->GetCrBuffer();
    if (crBuffer != INVALID_BUFFER_DESC) {
        /* pin the free crBuffer */
        crBuffer->Pin();
        if (MakeCrBufferFree(crBuffer)) {
            baseBuffer->UnlockHdr(state);
            m_buffer_lru->Remove(crBuffer);
            /* release CR assign lock */
            baseBuffer->ReleaseCrAssignLwlock();
            *needRetry = false;
            return crBuffer;
        }
        crBuffer->Unpin();
    }
    baseBuffer->UnlockHdr(state);

    /* release CR assign lock */
    baseBuffer->ReleaseCrAssignLwlock();
    /* do not find free cr buffer from cr list */
    *needRetry = true;
    return INVALID_BUFFER_DESC;
}

bool BufMgr::ReuseCrBufferForBasePage(const BufferTag &bufTag, BufferDesc *freeBuffer, BufferDesc **usedBuffer)
{
    StorageAssert(usedBuffer != nullptr);
    StorageAssert(freeBuffer != INVALID_BUFFER_DESC);
    StorageAssert(freeBuffer->IsCrPage());


    /* acquire BufMapping lock */
    uint32 hashCode = m_buftable->GetHashCode(&bufTag);
    m_buftable->LockBufMapping(hashCode, LW_EXCLUSIVE);

    /* try to insert entry into hash table */
    BufferDesc* existBuffer = m_buftable->Insert(&bufTag, hashCode, freeBuffer);
    if (existBuffer != INVALID_BUFFER_DESC) {
        /* others have inserted the entry */

        /* pin the exist buffer */
        existBuffer->Pin();

        /* release BufMapping lock */
        m_buftable->UnlockBufMapping(hashCode);

        uint64 state = freeBuffer->LockHdr();
        state &= ~(Buffer::BUF_CR_PAGE);
        freeBuffer->ResetAsBaseBuffer();
        freeBuffer->UnlockHdr(state);

        /* put the free buffer back to BufLruList */
        PushBufferBack(freeBuffer);

        /* release freeBuffer */
        freeBuffer->Unpin();

        /* reuse fail, return exist buffer */
        *usedBuffer = existBuffer;
        return false;
    }

    uint64 bufState = freeBuffer->LockHdr();

    if (!IsBufferReusableByCurrentPdb(freeBuffer, bufTag.pdbId)) {
        m_buftable->Remove(&bufTag, hashCode);
        m_buftable->UnlockBufMapping(hashCode);

        bufState &= ~(Buffer::BUF_CR_PAGE);
        freeBuffer->ResetAsBaseBuffer();
        freeBuffer->UnlockHdr(bufState);

        m_buffer_lru->PushBufferBackToLru(freeBuffer, false);
        freeBuffer->Unpin();
        *usedBuffer = INVALID_BUFFER_DESC;
        return false;
    }

    /* make sure the buffer is free now */
    StorageAssert(!(bufState & Buffer::BUF_CONTENT_DIRTY));

    /* start to reuse buffer */
    freeBuffer->bufTag = bufTag;

    bufState &= Buffer::BUF_FLAG_RESET_MASK;
    bufState |= Buffer::BUF_TAG_VALID;
    /* reset CR info */
    freeBuffer->ResetAsBaseBuffer();
    freeBuffer->UnlockHdr(bufState);

    /* put the buffer to lru list */
    m_buffer_lru->PushBufferBackToLru(freeBuffer, true);

    /* release BufMapping lock */
    m_buftable->UnlockBufMapping(hashCode);

    /* reuse success, return freeBuffer */
    *usedBuffer = freeBuffer;
    return true;
}

bool BufMgr::IsBaseBufferReusable(const uint64 bufState, BufferDesc *candidateBuffer, const PrivateRefCountEntry *ref,
                                  PdbId newPdbId)
{
    if (candidateBuffer->GetRefcount() == 1 && ref != nullptr && ref->refcount == 1 &&
        !(bufState & Buffer::BUF_CONTENT_DIRTY) && (!candidateBuffer->HasCrBuffer())) {
        return IsBufferReusableByCurrentPdb(candidateBuffer, newPdbId);
    }

    return false;
}

bool BufMgr::IsBufferReusableByCurrentPdb(BufferDesc *candidateBuffer, PdbId newPdbId)
{
    StorageReleasePanic(candidateBuffer == nullptr, MODULE_BUFMGR, ErrMsg("CandidateBuffer is null!"));
    PdbId oldPdbId = candidateBuffer->GetPdbId();
    if (StoragePdb::IsValidPdbId(oldPdbId) && oldPdbId != newPdbId) {
        StoragePdb *pdb = g_storageInstance->GetPdb(oldPdbId);
        if (STORAGE_VAR_NULL(pdb)) {
            return true;
        }
        BgPageWriterMgr *bgPageWriterMgr = pdb->GetBgPageWriterMgr();
        if (STORAGE_VAR_NULL(bgPageWriterMgr)) {
            return true;
        }
        /* now we just init one bgPageWriter in every pdb, so just pass the SlotId to GetBgPageWriter */
        int64 bgWriterSlotId = pdb->GetOrgBgWriterSlotId();
        if (oldPdbId >= FIRST_USER_PDB_ID && bgWriterSlotId == INVALID_BGWRITER_SLOT_ID) {
            for (uint32 i = 0; i < DIRTY_PAGE_QUEUE_MAX_SIZE; i++) {
                if (candidateBuffer->IsInDirtyPageQueue(i)) {
                    return false;
                }
            }
        }
        if (bgWriterSlotId >= DEFAULT_BGWRITER_SLOT_ID && bgWriterSlotId < DIRTY_PAGE_QUEUE_MAX_SIZE &&
            candidateBuffer->IsInDirtyPageQueue(bgWriterSlotId)) {
            return false;
        }
    }
    /* If pdbid is invalid, this buffer also may be in dirtypage queue. */
    if (!StoragePdb::IsValidPdbId(oldPdbId)) {
        for (uint32 i = 0; i < DIRTY_PAGE_QUEUE_MAX_SIZE; i++) {
            if (candidateBuffer->IsInDirtyPageQueue(i)) {
                return false;
            }
        }
    }
    return true;
}

void BufMgr::PushBufferBack(BufferDesc *buffer)
{
    StorageReleasePanic(buffer == nullptr, MODULE_BUFMGR, ErrMsg("buffer is null!"));
    /* It is performance insensitive, so we just traverse slots here. */
    for (uint32 i = 0; i < DIRTY_PAGE_QUEUE_MAX_SIZE; ++i) {
        if (buffer->IsInDirtyPageQueue(i)) {
            m_buffer_lru->PushBufferBackToLru(buffer, false);
            return;
        }
    }
    m_buffer_lru->PushBufferBackToCandidate(buffer);
}

bool BufMgr::ReuseBaseBufferForBasePage(const BufferTag &bufTag, BufferDesc *candidateBuffer, BufferDesc **usedBuffer,
                                        BufferRing bufRing /* = nullptr */)
{
    BufferTag oldBufTag;
    uint32 oldHashCode = 0;
    uint32 newHashCode = m_buftable->GetHashCode(&bufTag);
    uint64 bufState = candidateBuffer->GetState();
    StorageAssert(candidateBuffer->IsPinnedPrivately());

    /*
     * Step 1: If the candidate buffer has cr buffer, then we should first reuse the cr buffer,
     * so here we return invalid buffer and retry
     * */
    candidateBuffer->AcquireCrAssignLwlock(LW_SHARED);
    if (candidateBuffer->HasCrBuffer()) {
        candidateBuffer->ReleaseCrAssignLwlock();
        m_buffer_lru->PushBufferBackToLru(candidateBuffer, false);
        candidateBuffer->Unpin();
        *usedBuffer = INVALID_BUFFER_DESC;
        return false;
    }

    /* Step 2: Lock partitions of hash table as needed. If the candidate buffer of old page is in hash table,
     * we need remove it from hash table later, so we must first lock the partition of hash table where the old page
     * is located. If the candidate buffer is not in hash table, we just need lock the partition of hash table
     * in order to insert page id entry into hash table.
     * */
    if (bufState & Buffer::BUF_TAG_VALID) {
        oldBufTag = candidateBuffer->bufTag;
        oldHashCode = m_buftable->GetHashCode(&oldBufTag);
        m_buftable->LockBufMapping(oldHashCode, newHashCode, LW_EXCLUSIVE);
    } else {
        /* Candidate buffer is not in hash table because buffer bufTag is not valid */
        m_buftable->LockBufMapping(newHashCode, LW_EXCLUSIVE);
    }


    bufState = candidateBuffer->LockHdr();

    /* Step 3: Insert buffer entry of page into hash table */
    BufferDesc *existBuffer = m_buftable->Insert(&bufTag, newHashCode, candidateBuffer);

    /* Other thread has inserted the BufferEntry, so release candidate buffer */
    if (unlikely(existBuffer != INVALID_BUFFER_DESC)) {
        m_buffer_lru->PushBufferBackToLru(candidateBuffer, false);
        candidateBuffer->UnlockHdr(bufState);
        candidateBuffer->Unpin();

        if ((bufState & Buffer::BUF_TAG_VALID) && !m_buftable->IsSameBufMapping(newHashCode, oldHashCode)) {
            m_buftable->UnlockBufMapping(oldHashCode);
        }
        candidateBuffer->ReleaseCrAssignLwlock();
        existBuffer->Pin();
        m_buftable->UnlockBufMapping(newHashCode);
        *usedBuffer = existBuffer;
        return false;
    } else {
        /* Step 4: We already create the BufferEntry successfully, and check if we can reuse it right now. */
        BufferTag evictedBuftag = INVALID_BUFFER_TAG;
        uint64 evictedBufState = 0;
        uint64 timestamp = INVALID_TIMESTAMP;

        uint64 oldBufState = bufState & Buffer::BUF_FLAG_MASK;
        PrivateRefCountEntry *ref = thrd->GetBufferPrivateRefCount()->GetPrivateRefcount(candidateBuffer);

        /* If buffer is not dirty and only pinned by me, we can reuse the candidate buffer */
        if (IsBaseBufferReusable(oldBufState, candidateBuffer, ref, bufTag.pdbId)) {
            ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("Buffer Status Observation: bufTag:(%hhu, %hu, %u)"
                " EvictPage lsnInfo(walId %lu glsn %lu plsn %lu) state(%lu)",
                candidateBuffer->GetPdbId(), candidateBuffer->GetPageId().m_fileId,
                candidateBuffer->GetPageId().m_blockId,
                reinterpret_cast<Page *>(candidateBuffer->bufBlock)->GetWalId(),
                reinterpret_cast<Page *>(candidateBuffer->bufBlock)->GetGlsn(),
                reinterpret_cast<Page *>(candidateBuffer->bufBlock)->GetPlsn(), bufState));
            if (oldBufState & Buffer::BUF_OWNED_BY_ME) {
                /* AntiCache: miss dirty check and insert anticache before buffer evicted */
                AntiCacheHandleBufferEvicted(candidateBuffer);
            }
            evictedBuftag = candidateBuffer->bufTag;
            evictedBufState = bufState;
            candidateBuffer->bufTag = bufTag;
            candidateBuffer->pageVersionOnDisk = INVALID_PAGE_LSN;
            candidateBuffer->fileVersion = INVALID_FILE_VERSION;
            bufState &= Buffer::BUF_FLAG_RESET_MASK;
            bufState |= Buffer::BUF_TAG_VALID;
            candidateBuffer->ResetAsBaseBuffer();

            timestamp = static_cast<uint64>(GetSystemTimeInMicrosecond());
            if (oldBufState & Buffer::BUF_TAG_VALID) {
                m_buftable->Remove(&oldBufTag, oldHashCode);
            }

            if ((oldBufState & Buffer::BUF_TAG_VALID) &&
                (!m_buftable->IsSameBufMapping(oldHashCode, newHashCode))) {
                m_buftable->UnlockBufMapping(oldHashCode);
            }
            m_buftable->UnlockBufMapping(newHashCode);
            candidateBuffer->UnlockHdr(bufState);

            m_buffer_lru->PushBufferBackToLru(candidateBuffer, true);

            candidateBuffer->ReleaseCrAssignLwlock();
            ResetBufferInitialPageInfo(candidateBuffer);
            *usedBuffer = candidateBuffer;
            EvictBaseBufferCallback(evictedBuftag, evictedBufState, timestamp);
            return true;
        } else {
            /* Step 5: The buffer can not be reused now, release anything what we hold, and prepare try again. */

            /* reuse fails, the buffer is dirty again, delete it from the ring. */
            if (bufRing != nullptr && (bufState & Buffer::BUF_CONTENT_DIRTY)) {
                bufRing->RemoveBufInRing(candidateBuffer);
            }

            m_buftable->Remove(&bufTag, newHashCode);
            if ((oldBufState & Buffer::BUF_TAG_VALID) &&
                (!m_buftable->IsSameBufMapping(oldHashCode, newHashCode))) {
                m_buftable->UnlockBufMapping(oldHashCode);
            }
            m_buffer_lru->PushBufferBackToLru(candidateBuffer, false);
            m_buftable->UnlockBufMapping(newHashCode);
            candidateBuffer->UnlockHdr(bufState);
            candidateBuffer->ReleaseCrAssignLwlock();
            candidateBuffer->Unpin();
            *usedBuffer = INVALID_BUFFER_DESC;
        }
    }
    return false;
}

BufferDesc *BufMgr::TryToReuseBufferForCrPage(BufferDesc *baseBufferDesc, bool *needRetry,
                                              BufferRing bufRing /* = nullptr */)
{
    BufferDesc *freeBufferDesc = INVALID_BUFFER_DESC;
    if (STORAGE_VAR_NULL(baseBufferDesc)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("BaseBufferDesc is nullptr."));
    }
    StorageStat *stat = g_storageInstance->GetStat();
    /* STEP1: Get a free buffer from the BufLruList. */
    stat->m_reportWaitStatus(
        static_cast<uint32_t>(GsStatWaitState::STATE_BUFFER_GET_FREEBUF));
    freeBufferDesc = GetFreeBuffer(&baseBufferDesc->bufTag, needRetry, bufRing);
    stat->m_reportWaitStatus(static_cast<uint32_t>(GsStatWaitState::STATE_WAIT_UNDEFINED));
    if (freeBufferDesc == INVALID_BUFFER_DESC) {
        return freeBufferDesc;
    }

    if (freeBufferDesc->IsCrPage()) {
        if (!ReuseCrBufferForCrPage(baseBufferDesc, freeBufferDesc)) {
            freeBufferDesc = INVALID_BUFFER_DESC;
        }
    } else if (!ReuseBaseBufferForCrPage(baseBufferDesc, freeBufferDesc, bufRing)) {
        freeBufferDesc = INVALID_BUFFER_DESC;
    }

    *needRetry = (freeBufferDesc == INVALID_BUFFER_DESC);
    if (!*needRetry) {
        /* reuse success, add to the buffer ring. */
        if (bufRing != nullptr && !bufRing->curWasInRing) {
            bufRing->AddFreeBufToRing(freeBufferDesc);
        }
    }
    return freeBufferDesc;
}

BufferDesc *BufMgr::AllocCrEntry(BufferDesc *baseBufferDesc, BufferRing bufRing /* = nullptr */)
{
    StorageAssert(baseBufferDesc->IsCrAssignLocked(LW_EXCLUSIVE));
    BufferTag bufTag = baseBufferDesc->GetBufferTag();

#ifdef DSTORE_USE_ASSERT_CHECKING
    (void)++m_bufPoolCnts.crAllocCnt;
#endif
    /* find a free slot */
    uint32 retryTimes = 0;
    if (!baseBufferDesc->HasCrBuffer()) {
        for (;;) {
            bool needRetry = true;
            BufferDesc *crBufferDesc = TryToReuseBufferForCrPage(baseBufferDesc, &needRetry, bufRing);
            if (crBufferDesc != INVALID_BUFFER_DESC) {
                return crBufferDesc;
            } else if (crBufferDesc == INVALID_BUFFER_DESC && !needRetry) {
                return INVALID_BUFFER_DESC;
            }
            retryTimes++;
            if (retryTimes > RETRY_WARNING_THRESHOLD) {
                ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
                    ErrMsg("after retry %u times, failed to construct CR buffer for base buffer bufTag:(%hhu, %hu, %u)",
                        retryTimes, bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
                return INVALID_BUFFER_DESC;
            }
        }
    } else {
        uint64 state = baseBufferDesc->LockHdr();
        BufferDesc *crBufferDesc = baseBufferDesc->GetCrBuffer();
        crBufferDesc->Pin();
        /* If only pinned by me, make it free for reuse */
        if (MakeCrBufferFree(crBufferDesc)) {
            baseBufferDesc->UnlockHdr(state);
            /* First remove from lru and will be add into lru in ReuseCrBufferForCrPage */
            m_buffer_lru->Remove(crBufferDesc);
            if (!ReuseCrBufferForCrPage(baseBufferDesc, crBufferDesc)) {
                return INVALID_BUFFER_DESC;
            }
#ifdef DSTORE_USE_ASSERT_CHECKING
            (void)m_bufPoolCnts.crReuseSlotCnt++;
#endif
            return crBufferDesc;
        }
        crBufferDesc->Unpin();
        baseBufferDesc->UnlockHdr(state);
    }
    return INVALID_BUFFER_DESC;
}

BufferDesc *BufMgr::GetFreeBuffer(const BufferTag *bufTag, bool *needRetry, BufferRing bufRing /* = nullptr */)
{
    uint64 bufState;

    /* STEP1: Get a candidate buffer from the BufferRing. */
    BufferDesc *candidateBuffer = bufRing != nullptr ? bufRing->GetFreeBufFromRing() : INVALID_BUFFER_DESC;

    /* STEP2: When the buffer obtained from the buffer ring is null,
       the buffer is obtained from the BufLruList.
    */
    if (candidateBuffer == INVALID_BUFFER_DESC) {
        candidateBuffer = m_buffer_lru->GetCandidateBuffer(bufTag, &bufState);
    } else {
        m_buffer_lru->Remove(candidateBuffer);
    }
    if (candidateBuffer == INVALID_BUFFER_DESC) {
        *needRetry = true;
        return candidateBuffer;
    }

    StorageAssert(candidateBuffer->lruNode.IsInPendingState());

    /* STEP2: try to make candidate buffer free */
    if (candidateBuffer->IsCrPage()) { /* candidate buffer cache the CR page */
        /* find corresponding base buffer */
        BufferDesc *baseBuffer = candidateBuffer->GetCrBaseBuffer();

        /* acquire CR assign lock in the base buffer in Exclusive mode */
        /* NOTE: we use try_acquire_xxx_lwlock method to avoid deadlock */
        if (!baseBuffer->TryAcquireCrAssignLwlock(LW_EXCLUSIVE)) {
            /* put the buffer back to BufLruList */
            m_buffer_lru->PushBufferBackToLru(candidateBuffer, false);
            /* release candidate buffer */
            candidateBuffer->Unpin();
            *needRetry = true;
            return INVALID_BUFFER_DESC;
        }

        /* recheck if the CR buffer is in the CR list of the base buffer now
         * because we pin the CR buffer before acquiring the CR assign lock in the base buffer,
         * so the base buffer of the CR buffer can not be changed.
         */
        StorageAssert(baseBuffer->GetCrBuffer() == candidateBuffer);

        /* try to make CR candidate buffer free */
        if (!MakeCrBufferFree(candidateBuffer)) {
            /* put the buffer back to BufLruList */
            m_buffer_lru->PushBufferBackToLru(candidateBuffer, false);
            /* release candidate buffer */
            candidateBuffer->Unpin();
            /* release CR assign lock */
            baseBuffer->ReleaseCrAssignLwlock();
            *needRetry = true;
            return INVALID_BUFFER_DESC;
        }
        StorageAssert(candidateBuffer->lruNode.IsInPendingState());
        /* release CR assign lock, and now the cr_buffer is completely free */
        baseBuffer->ReleaseCrAssignLwlock();
        *needRetry = false;
        return candidateBuffer;
    } else { /* candidate buffer cache the base page */
        BufferDesc *bufferDesc = MakeBaseBufferFree(candidateBuffer, needRetry, bufRing);
        StorageAssert((bufferDesc != INVALID_BUFFER_DESC) ? bufferDesc->lruNode.IsInPendingState() : true);
        return bufferDesc;
    }
}

BufferDesc *BufMgr::FindCrBuffer(BufferDesc *baseBufferDesc, Snapshot snapshot)
{
    StorageAssert(baseBufferDesc->IsCrAssignLocked());
    BufferDesc *crBuffer = INVALID_BUFFER_DESC;
    /* If base page is modified, cr is invalid. */
    Page* basePage = baseBufferDesc->GetPage();
    if (baseBufferDesc->IsCrUsable() && baseBufferDesc->crInfo.IsCrMatched(snapshot->snapshotCsn)) {
        crBuffer = baseBufferDesc->GetCrBuffer();
        if (STORAGE_VAR_NULL(crBuffer)) {
            ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("CrBuffer is nullptr."));
        }
        crBuffer->Pin();
        if (likely(g_storageInstance->GetType() == StorageInstanceType::DISTRIBUTE_COMPUTE)) {
            StorageReleaseBufferCheckPanic(((baseBufferDesc->GetState(false) & Buffer::BUF_OWNED_BY_ME) == 0) &&
                ((baseBufferDesc->GetState(false) & Buffer::BUF_READ_AUTHORITY) == 0), MODULE_BUFMGR,
                baseBufferDesc->GetBufferTag(), "Page not owned or not read auth when find cr");
            Page* crPage = crBuffer->GetPage();
            if (unlikely((crPage->GetGlsn() != basePage->GetGlsn()) || (crPage->GetPlsn() != basePage->GetPlsn()))) {
                BufferTag bufTag = baseBufferDesc->GetBufferTag();
                ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("CR page not matched with base page, bufTag:(%hhu, %hu, %u) "
                    "BASE page's header: " PAGE_HEADER_FMT,
                    bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                    PAGE_HEADER_VAL(basePage)));
                ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("CR page not matched with base page, bufTag:(%hhu, %hu, %u) "
                    "CR page's header: " PAGE_HEADER_FMT,
                    bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                    PAGE_HEADER_VAL(crPage)));
                char *clusterBufferInfo = g_storageInstance->GetBufferMgr()->GetClusterBufferInfo(bufTag.pdbId,
                    bufTag.pageId.m_fileId, bufTag.pageId.m_blockId);
                if (likely(clusterBufferInfo != nullptr)) {
                    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("%s.", clusterBufferInfo));
                    DstorePfreeExt(clusterBufferInfo);
                }
                crBuffer->Unpin();
                baseBufferDesc->SetCrUnusable();
                return INVALID_BUFFER_DESC;
            }
            StorageReleaseBufferCheckPanic((crPage->GetGlsn() != basePage->GetGlsn()),
                MODULE_BUFMGR, baseBufferDesc->GetBufferTag(), "Page glsn not equal when find cr");
            StorageReleaseBufferCheckPanic((crPage->GetPlsn() != basePage->GetPlsn()),
                MODULE_BUFMGR, baseBufferDesc->GetBufferTag(), "Page plsn not equal when find cr");
        }
#ifdef DSTORE_USE_ASSERT_CHECKING
        ++m_bufPoolCnts.crMatchCnt;
#endif
    } else {
#ifdef DSTORE_USE_ASSERT_CHECKING
        ++m_bufPoolCnts.crMismatchCnt;
#endif
    }
    return crBuffer;
}

bool BufMgr::ReuseCrBufferForCrPage(BufferDesc *baseBufferDesc, BufferDesc *freeBufferDesc)
{
    /* set buffer state */
    if (STORAGE_VAR_NULL(baseBufferDesc)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("BaseBufferDesc is nullptr."));
    }
    if (STORAGE_VAR_NULL(freeBufferDesc)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("FreeBufferDesc is nullptr."));
    }
    uint64 bufState = freeBufferDesc->LockHdr();

    if (!IsBufferReusableByCurrentPdb(freeBufferDesc, baseBufferDesc->GetPdbId())) {
        bufState &= ~(Buffer::BUF_CR_PAGE);
        freeBufferDesc->ResetAsBaseBuffer();
        freeBufferDesc->UnlockHdr(bufState);
        m_buffer_lru->PushBufferBackToLru(freeBufferDesc, false);
        freeBufferDesc->Unpin();
        return false;
    }

    bufState |= Buffer::BUF_CR_PAGE;
    freeBufferDesc->UnlockHdr(bufState);

    /* set buffer pageId */
    freeBufferDesc->bufTag = baseBufferDesc->bufTag;

    freeBufferDesc->ResetAsCrBuffer();
    /* set base buffer pointer in cr buffer */
    freeBufferDesc->SetBaseBuffer(baseBufferDesc);
    /* put the freeBufferDesc into the CR array */
    baseBufferDesc->SetCrBuffer(freeBufferDesc);

    /* put free buffer back to BufLruList */
    m_buffer_lru->PushBufferBackToLru(freeBufferDesc, true);

    return true;
}

bool BufMgr::ReuseBaseBufferForCrPage(BufferDesc *baseBufferDesc, BufferDesc *candidateBufferDesc,
                                      BufferRing bufRing /* = nullptr */)
{
    uint32 hashCode = 0;
    StorageAssert(candidateBufferDesc != nullptr);
    uint64 bufState = candidateBufferDesc->GetState();
    uint64 oldBufState = bufState & Buffer::BUF_FLAG_MASK;

    /* STEP1: acquire CR assign lock in the base buffer in shared mode */
    candidateBufferDesc->AcquireCrAssignLwlock(LW_SHARED);
    if (candidateBufferDesc->HasCrBuffer()) {
        candidateBufferDesc->ReleaseCrAssignLwlock();
        /* put the candidate buffer back to BufLruList */
        m_buffer_lru->PushBufferBackToLru(candidateBufferDesc, false);
        /* release candidate buffer */
        candidateBufferDesc->Unpin();
        return false;
    }

    /* STEP2: Acquire BufMapping Lock */
    if (oldBufState & Buffer::BUF_TAG_VALID) {
        hashCode = m_buftable->GetHashCode(&candidateBufferDesc->bufTag);
        m_buftable->LockBufMapping(hashCode, LW_EXCLUSIVE);
    }


    bufState = candidateBufferDesc->LockHdr();

    PrivateRefCountEntry *ref = thrd->GetBufferPrivateRefCount()->GetPrivateRefcount(candidateBufferDesc);
    /* STEP3: Check if the buffer is free now */
    if (IsBaseBufferReusable(bufState, candidateBufferDesc, ref, baseBufferDesc->GetPdbId())) {
        /* remove hash entry */
        if (bufState & Buffer::BUF_TAG_VALID) {
            m_buftable->Remove(&candidateBufferDesc->bufTag, hashCode);
        }
        if (bufState & Buffer::BUF_OWNED_BY_ME) {
            /* AntiCache: miss dirty check and insert anticache before buffer evicted */
            AntiCacheHandleBufferEvicted(candidateBufferDesc);
        }
        BufferTag evictedBuftag = candidateBufferDesc->bufTag;
        uint64 evictedBufState = bufState;
        uint64 timestamp = INVALID_TIMESTAMP;

        /* set buffer pageId */
        StorageAssert(baseBufferDesc != nullptr);
        candidateBufferDesc->bufTag = baseBufferDesc->bufTag;
        candidateBufferDesc->pageVersionOnDisk = INVALID_PAGE_LSN;
        candidateBufferDesc->fileVersion = INVALID_FILE_VERSION;
        /* set buffer state */
        bufState &= Buffer::BUF_FLAG_RESET_MASK;
        bufState |= Buffer::BUF_CR_PAGE;
        /* Reset as CR buffer */
        candidateBufferDesc->ResetAsCrBuffer();
        candidateBufferDesc->SetBaseBuffer(baseBufferDesc);
        candidateBufferDesc->UnlockHdr(bufState);

        /* release BufMapping lock */
        if (oldBufState & Buffer::BUF_TAG_VALID) {
            timestamp = static_cast<uint64>(GetSystemTimeInMicrosecond());
            m_buftable->UnlockBufMapping(hashCode);
        }

        /* put the freeBuffer into the CR array */
        baseBufferDesc->SetCrBuffer(candidateBufferDesc);

        /* put the candidate buffer back to BufLruList */
        m_buffer_lru->PushBufferBackToLru(candidateBufferDesc, true);

        candidateBufferDesc->ReleaseCrAssignLwlock();
        ResetBufferInitialPageInfo(candidateBufferDesc);
        EvictBaseBufferCallback(evictedBuftag, evictedBufState, timestamp);
        return true;
    }
    /* reuse fails, the buffer is dirty again, delete it from the ring. */
    if (bufRing != nullptr && (bufState & Buffer::BUF_CONTENT_DIRTY)) {
        bufRing->RemoveBufInRing(candidateBufferDesc);
    }

    candidateBufferDesc->UnlockHdr(bufState);

    /* put the candidate buffer back to BufLruList */
    m_buffer_lru->PushBufferBackToLru(candidateBufferDesc, false);

    /* release candidate buffer */
    candidateBufferDesc->Unpin();

    /* release BufMapping lock */
    if (bufState & Buffer::BUF_TAG_VALID) {
        m_buftable->UnlockBufMapping(hashCode);
    }
    candidateBufferDesc->ReleaseCrAssignLwlock();
    return false;
}

RetStatus BufMgr::GetPageDirectoryInfo(Size *length, char **errInfo, PageDirectoryInfo **pageDirectoryArr)
{
    *length = 0;
    *pageDirectoryArr = nullptr;
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("The function is applicable to cluster mode only and "
            "cannot allocate memory for dump info."));
        *errInfo = nullptr;
        return DSTORE_FAIL;
    }
    dumpInfo.append("The function is applicable to cluster mode only");
    *errInfo = dumpInfo.data;
    return DSTORE_FAIL;
}

RetStatus BufMgr::GetPDBucketInfo(Size *length, char ***chashBucketInfo,
    UNUSE_PARAM uint32 startBucket, UNUSE_PARAM uint32 endBucket)
{
    *length = 0;
    *chashBucketInfo = nullptr;
    return DSTORE_FAIL;
}

uint8 BufMgr::GetBufDescResponseType(UNUSE_PARAM BufferDesc *bufferDesc)
{
    return 0;
}

Size BufMgr::CheckForBufLruLeak(UNUSE_PARAM bool isCleanup)
{
    AutoMemCxtSwitch autoSwitch{g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_BUFFER)};
    uint32 leakCount = 0;
#ifdef DSTORE_USE_ASSERT_CHECKING
    StringInfoData leakWarningMsg;
    if (unlikely(!leakWarningMsg.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for leakWarningMsg dump info."));
        return 0;
    }
    /* When this function is called during cleanup, thrd variable is already deallocated and will cause SEGV
     * if we try to acquire LWLock. Also, we assume there's no other threads accessing the memchunk list
     * during cleanup and we won't use the LWLock protection. */
    if (!isCleanup) {
        m_bufferMemChunkList->LockBufferMemChunkList(LW_SHARED);
    }
    BufferMemChunkWrapper<BufferMemChunk> *memChunkWrapper =
        m_bufferMemChunkList->GetMemChunkIterator<BufferMemChunk>();
    while (memChunkWrapper != nullptr) {
        if (!isCleanup) {
            memChunkWrapper->memChunk->LockMemChunk(LW_SHARED);
        }
        for (Size i = 0; i < memChunkWrapper->memChunk->GetSize(); i++) {
            BufferDesc *bufferDesc = memChunkWrapper->memChunk->GetBufferDesc(i);
            if (bufferDesc->lruNode.IsInPendingState()) {
                BufferTag bufTag = bufferDesc->GetBufferTag();
                uint64 state = bufferDesc->GetState(false);
                if (state & Buffer::BUF_TAG_VALID) {
                    leakWarningMsg.append("bufTag:(%hhu, %hu, %u) is in lru pending state\n",
                        bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId);
                } else {
                    leakWarningMsg.append("Invalid bufTag:(%hhu, %hu, %u) is in lru pending state\n",
                        bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId);
                }
                leakCount++;
            }
        }
        if (!isCleanup) {
            memChunkWrapper->memChunk->UnlockMemChunk();
        }
        memChunkWrapper = memChunkWrapper->GetNext();
    }
    if (!isCleanup) {
        m_bufferMemChunkList->UnlockBufferMemChunkList();
    }

    if (leakCount > 0) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("%s", leakWarningMsg.data));
    }

    DstorePfreeExt(leakWarningMsg.data);
#endif
    return leakCount;
}

void BufferTagArray::Initialize()
{
    for (int i = 0; i < MAX_ARRAY_SIZE; i++) {
        bufTags[i] = INVALID_BUFFER_TAG;
    }

    spinLock.Init();
}

void BufferTagArray::Add(const BufferTag &bufTag)
{
    bool isFull = true;
    spinLock.Acquire();

    for (int i = 0; i < MAX_ARRAY_SIZE; i++) {
        if (bufTags[i] == INVALID_BUFFER_TAG) {
            bufTags[i] = bufTag;
            isFull = false;
            break;
        }
    }

    spinLock.Release();

    StorageAssert(!isFull);
    if (unlikely(isFull)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFFER, ErrMsg("Cannot find a slot for adding into BufferTagArray."));
    }
}

void BufferTagArray::Remove(const BufferTag &bufTag)
{
    bool isFound = false;
    spinLock.Acquire();

    for (int i = 0; i < MAX_ARRAY_SIZE; i++) {
        if (bufTags[i] == bufTag) {
            bufTags[i] = INVALID_BUFFER_TAG;
            isFound = true;
            break;
        }
    }

    spinLock.Release();

    /* If buffer bufTag is not found, the thread must be using buffer in a wrong way. */
    StorageAssert(isFound);
    if (unlikely(!isFound)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFFER, ErrMsg("Cannot find the bufTag from BufferTagArray."));
    }
}

bool BufferTagArray::IsExist(const BufferTag &bufTag)
{
    bool ret = false;
    spinLock.Acquire();

    for (int i = 0; i < MAX_ARRAY_SIZE; i++) {
        if (bufTags[i] == bufTag) {
            ret = true;
            break;
        }
    }

    spinLock.Release();
    return ret;
}

void WaitIo(BufferDesc *bufferDesc)
{
    /*
     * Changed to wait until there's no IO - Inoue 01/13/2000
     *
     * Note this is *necessary* because an error abort in the process doing
     * I/O could release the ioInProgressLwlock prematurely. See
     * AbortBufferIO.
     */
    for (;;) {
        uint64 bufState;

        /*
         * It may not be necessary to acquire the spinlock to check the flag
         * here, but since this test is essential for correctness, we'd better
         * play it safe.
         */
        bufState = bufferDesc->LockHdr();
        bufferDesc->UnlockHdr(bufState);

        if (!(bufState & Buffer::BUF_IO_IN_PROGRESS)) {
            break;
        }
        DstoreLWLockAcquire(bufferDesc->controller->GetIoInProgressLwLock(), LW_SHARED);
        LWLockRelease(bufferDesc->controller->GetIoInProgressLwLock());
    }
}

bool StartIo(BufferDesc* bufferDesc, bool forInput)
{
    if (unlikely(bufferDesc == nullptr)) {
        return false;
    }

    uint64 bufState;

    for (;;) {
        /*
         * Grab the io_in_progress lock so that other processes can wait for
         * me to finish the I/O.
         */
        DstoreLWLockAcquire(bufferDesc->controller->GetIoInProgressLwLock(), LW_EXCLUSIVE);

        bufState = bufferDesc->LockHdr();
        if (!(bufState & Buffer::BUF_IO_IN_PROGRESS)) {
            break;
        }

        /*
         * The only way BUF_IO_IN_PROGRESS could be set when the io_in_progress
         * lock isn't held is if the process doing the I/O is recovering from
         * an error (see AbortBufferIO).  If that's the case, we must wait for
         * him to get unwedged.
         */
        bufferDesc->UnlockHdr(bufState);
        LWLockRelease(bufferDesc->controller->GetIoInProgressLwLock());
        WaitIo(bufferDesc);
    }

    /* Once we get here, there is definitely no I/O active on this buffer_desc */
    if (forInput ? (bufState & Buffer::BUF_VALID) : !(bufState & Buffer::BUF_CONTENT_DIRTY)) {
        /* someone else already did the I/O */
        bufferDesc->UnlockHdr(bufState);
        LWLockRelease(bufferDesc->controller->GetIoInProgressLwLock());
        return false;
    }

    bufState |= Buffer::BUF_IO_IN_PROGRESS;
    bufferDesc->UnlockHdr(bufState);

    return true;
}

static void TerminateIoCommon(BufferDesc* bufferDesc, bool clearDirty, uint64 setFlagBits)
{
    uint64 bufState = bufferDesc->LockHdr();

    StorageAssert(bufState & Buffer::BUF_IO_IN_PROGRESS);

    bufState &= ~(Buffer::BUF_IO_IN_PROGRESS | Buffer::BUF_IO_ERROR);

    if (clearDirty) {
        // clean the BUF_DIRTY if the page not re-dirty when we are writing.
        if (!(bufState & Buffer::BUF_HINT_DIRTY)) {
            bufState &= ~(Buffer::BUF_CONTENT_DIRTY);
            StorageReleasePanic(bufState & Buffer::BUF_IS_WRITING_WAL, MODULE_BUFMGR,
                ErrMsg("Terminate Io has writing wal flag bufTag:(%hhu, %hu, %u) lsnInfo(walId %lu glsn %lu plsn %lu) "
                "state(%lu), recoveryPlsn(%lu)", bufferDesc->GetPdbId(), bufferDesc->GetPageId().m_fileId,
                bufferDesc->GetPageId().m_blockId, bufferDesc->GetPage()->GetWalId(), bufferDesc->GetPage()->GetGlsn(),
                bufferDesc->GetPage()->GetPlsn(), bufState,
                bufferDesc->recoveryPlsn[DEFAULT_BGWRITER_SLOT_ID].load(std::memory_order_acquire)));
        }
    }

    bufState |= setFlagBits;
    bufferDesc->UnlockHdr(bufState);
}

void TerminateIo(BufferDesc* bufferDesc, bool clearDirty, uint64 setFlagBits)
{
    TerminateIoCommon(bufferDesc, clearDirty, setFlagBits);

    StorageAssert(LWLockHeldByMe(bufferDesc->controller->GetIoInProgressLwLock()));
    LWLockRelease(bufferDesc->controller->GetIoInProgressLwLock());
}

/*
 * TerminateAsyncIo - need release IO lock at other thread.
 * In general, other thread acquire the IO lock, now need release the lock at this thread.
 * because the new thread hardly record the IO lock in its thread context, we need LWLockOwn firstly
 * and we can release it safely.
 */
void TerminateAsyncIo(BufferDesc* bufferDesc, bool clearDirty, uint64 setFlagBit)
{
    TerminateIoCommon(bufferDesc, clearDirty, setFlagBit);
    if (likely(!LWLockHeldByMe(bufferDesc->controller->GetIoInProgressLwLock()))) {
        DstoreLWLockOwn(bufferDesc->controller->GetIoInProgressLwLock(), LW_EXCLUSIVE);
    } else {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR,
            ErrMsg("the bufTag:(%hhu, %hu, %u) should not acquire the IOLWlock.",
            bufferDesc->GetPdbId(), bufferDesc->GetPageId().m_fileId,
            bufferDesc->GetPageId().m_blockId));
    }
    LWLockRelease(bufferDesc->controller->GetIoInProgressLwLock());
}

void AbortIo(BufferDesc* bufferDesc, bool forInput)
{
    uint64 bufState;

    DstoreLWLockAcquire(bufferDesc->controller->GetIoInProgressLwLock(), LW_EXCLUSIVE);

    bufState = bufferDesc->LockHdr();

    StorageAssert(bufState & Buffer::BUF_IO_IN_PROGRESS);

    if (forInput) {
        /* When reading we expect the buffer_desc to be invalid but not dirty */
        StorageAssert(!(bufState & Buffer::BUF_CONTENT_DIRTY));
        StorageAssert(!(bufState & Buffer::BUF_VALID));
        bufferDesc->UnlockHdr(bufState);
    } else {
        /* When writing we expect the buffer_desc to be valid and dirty */
        StorageAssert(bufState & Buffer::BUF_CONTENT_DIRTY);
        bufferDesc->UnlockHdr(bufState);
    }

    TerminateIo(bufferDesc, false, Buffer::BUF_IO_ERROR);
    StorageAssert(!LWLockHeldByMe(bufferDesc->controller->GetIoInProgressLwLock()));
}

#ifdef DSTORE_USE_ASSERT_CHECKING
void BufMgr::AssertHasHoldBufLock(__attribute__((unused)) const PdbId pdbId,
                                  __attribute__((unused)) const PageId pageId,
                                  __attribute__((unused)) LWLockMode lockMode)
{
#ifndef UT
    BufferTag bufTag(pdbId, pageId);
    BufferDesc *bufferDesc = LookupBuffer(bufTag);   /* Need unpin buffer */
    if (STORAGE_VAR_NULL(bufferDesc)) {
        /* lookup with lock again. */
        bufferDesc = LookupBufferWithLock(bufTag);
    }
    StorageAssert(bufferDesc);
    StorageAssert(bufferDesc->IsPinnedPrivately());
    StorageAssert(bufferDesc->IsContentLocked(lockMode));
    Release(bufferDesc);
    StorageAssert(bufferDesc->IsPinnedPrivately());
#endif
}
#endif

static void FlushDirtyPageUnderLockIfRetry(Page *page, const BufferTag &bufTag, ErrorCode errorCode)
{
    if (likely(errorCode == 0)) {
        return;
    }
    ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
           ErrMsg("Async Flush Pageid %hu-%u failed,  ret = %lld", bufTag.pageId.m_fileId,
                  bufTag.pageId.m_blockId, errorCode));
    StoragePdb *storagePdb = g_storageInstance->GetPdb(bufTag.pdbId);
    StorageReleasePanic(storagePdb == nullptr, MODULE_BUFMGR, ErrMsg("pdb %u is nullptr", bufTag.pdbId));
    if (!storagePdb->IsReadFileOk()) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("StoragePdb is nullptr."));
    }
    VFSAdapter *vfs = storagePdb->GetVFS();
    StorageReleasePanic(vfs == nullptr, MODULE_BUFMGR, ErrMsg("vfs is nullptr, pdb %u", bufTag.pdbId));
    if (STORAGE_FUNC_FAIL(vfs->WritePageSync(bufTag.pageId, page))) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg(
            "Retry Sync Flush Pageid %hu-%u failed, error:%lld",
            bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, StorageGetErrorCode()));
    }
}

RetStatus BatchBufferAioContextMgr::InitBatch(bool needCountTotal, BufMgrInterface *bufMgr, uint32 size)
{
    m_needCountTotal = needCountTotal;
    m_bufMgr = bufMgr;
    m_size = size;

    m_curFreeMinIndex = 0;
    std::atomic_init(&m_inProgressPages, 0);
    std::atomic_init(&m_totalFlushedCount, 0U);

    /*
     * BatchBufferAioContextMgr and bufferAioCtxArr release and malloc at just one same thread,
     * although it need shared with other threads. we use thread level context is enough
     * because multi-thread access shared variables are atomic, others variables accessed by
     * just one dispatch aio task thread self sequentially.
     */
    StorageAssert(g_dstoreCurrentMemoryContext != nullptr);
    m_memoryContext = DstoreAllocSetContextCreate(g_dstoreCurrentMemoryContext,
        "BatchBufferAioContextMgr memcontext", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE, MemoryContextType::THREAD_CONTEXT);
    if (STORAGE_VAR_NULL(m_memoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFFER, ErrMsg("alloc BatchBufferAioContextMgr memcontext failed."));
        return DSTORE_FAIL;
    }

    AutoMemCxtSwitch autoSwitch(m_memoryContext);
    while (m_size != 0) {
        m_bufferAioCtxArr = static_cast<BufferAioContext *>(DstorePalloc(m_size * sizeof(BufferAioContext)));
        if (m_bufferAioCtxArr != nullptr) {
            break;
        }
        m_size = m_size / 10;  /* 10: if size is too large, decrease the size and retry. */
    }

    if (STORAGE_VAR_NULL(m_bufferAioCtxArr)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFFER, ErrMsg("alloc m_bufferAioCtxArr failed."));
        DstoreMemoryContextDelete(m_memoryContext);
        return DSTORE_FAIL;
    }

    for (uint32 i = 0; i < m_size; i++) {
        m_bufferAioCtxArr[i].batchCtxMgr = static_cast<void *>(this);
        m_bufferAioCtxArr[i].hasStartAioFlag = false;
        m_bufferAioCtxArr[i].bufferDesc = nullptr;
        m_bufferAioCtxArr[i].pageCopy =
            reinterpret_cast<Page *>(TYPEALIGN(AIO_MAX_REQNUM, m_bufferAioCtxArr[i].pageData));
        m_bufferAioCtxArr[i].submittedTime = 0;
    }
    return DSTORE_SUCC;
}

constexpr uint32 AIO_WAIT_LOG_INTEVAL_SEC_TIME = 10000U;
constexpr uint32 AIO_FSYNC_WAIT_MILLI_SEC = 100L;
void BatchBufferAioContextMgr::FsyncBatch()
{
    LatencyStat::Timer timer(&BufPerfUnit::GetInstance().m_aioFsync, true);
    int32 uncompleteAioNum = -1;
    uint32 loopCnt = 0;
    while ((uncompleteAioNum = m_inProgressPages.load(std::memory_order_acquire)) != 0) {
        GaussUsleep(AIO_FSYNC_WAIT_MILLI_SEC);
        loopCnt++;
        if ((loopCnt % AIO_WAIT_LOG_INTEVAL_SEC_TIME) == 0) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("Fsync batch aios has wait about %u secends",
                static_cast<uint32>(loopCnt / AIO_WAIT_LOG_INTEVAL_SEC_TIME)));
        }
    }
    ErrLog(DSTORE_DEBUG1, MODULE_FRAMEWORK, ErrMsg("Fsync batch aios succeed."));
}

void BatchBufferAioContextMgr::CallbackForAioBatchFlushBuffers(ErrorCode errorCode,
    int64_t successSize, void *asyncContext)
{
    BufferAioContext *bufferAioContext = static_cast<BufferAioContext *>(asyncContext);
    BatchBufferAioContextMgr *batchCtxMgr =
        static_cast<BatchBufferAioContextMgr *>(bufferAioContext->batchCtxMgr);
    BufferDesc* bufferDesc = bufferAioContext->bufferDesc;
    Page *pageCopy = reinterpret_cast<Page *>(bufferAioContext->pageCopy);
    /* thrd memory allocated here will be destroyed in comm thread pool exit callback */
    StorageReleasePanic(STORAGE_VAR_NULL(bufferDesc) || bufferDesc->bufTag.pdbId == INVALID_PDB_ID, MODULE_BUFMGR,
                        ErrMsg("Invalid buffer tag"));
    RetStatus ret = ThreadContext::DstoreThreadTryInitialize("Communication", bufferDesc->bufTag.pdbId);
    StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_BUFMGR, ErrMsg("Init thread failed"));

    /* some permanent aio context do not need count to avoid overflow */
    if (batchCtxMgr->IsCountFlushedPages()) {
        batchCtxMgr->AddOneFlushedPage();
    }

    StorageAssert(bufferDesc);
    StorageAssert((bufferDesc->state & Buffer::BUF_CONTENT_DIRTY));
    StorageReleasePanic((successSize != BLCKSZ), MODULE_BUFMGR,
        ErrMsg("CallbackForAioFlushBuffer pdbId %hhu page (%hu, %u) only flushed partial size %ld",
        bufferDesc->GetPdbId(), bufferDesc->GetPageId().m_fileId,
        bufferDesc->GetPageId().m_blockId, successSize));
    FlushDirtyPageUnderLockIfRetry(pageCopy, bufferDesc->GetBufferTag(), errorCode);
    BufMgr *bufMgr = dynamic_cast<BufMgr *>(g_storageInstance->GetBufferMgr());
    if (bufMgr->IsEnablePageMissingDirtyCheck()) {
        /* AntiCache: update pageVersionOnDisk after flush success */
        bufferDesc->UpdatePageVersion(pageCopy);
    }
    TerminateAsyncIo(bufferDesc, true, 0);
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR,
        ErrMsg("pdbId %hhu page (%hu, %u) has aio flush succeed(%lu, %lu)", bufferDesc->GetPdbId(),
        bufferDesc->GetPageId().m_fileId, bufferDesc->GetPageId().m_blockId,
        bufferDesc->pageVersionOnDisk.glsn, bufferDesc->pageVersionOnDisk.plsn));
    bufferDesc->UnpinForAio();
    batchCtxMgr->SubOneInProgressPage();
    bufferAioContext->submittedTime = 0;
}

RetStatus BatchBufferAioContextMgr::AsyncFlushPage(BufferTag &bufTag)
{
    RetStatus ret = DSTORE_SUCC;
    BufferAioContext *bufferAioCtx = nullptr;
    AsyncIoContext aioContext;

    if (m_curFreeMinIndex >= m_size) {
        FsyncBatch();
        ReuseBatch();
    }
    bufferAioCtx = &(m_bufferAioCtxArr[m_curFreeMinIndex]);
    bufferAioCtx->hasStartAioFlag = false;
    aioContext.callback = CallbackForAioBatchFlushBuffers;
    aioContext.asyncContext = static_cast<void *>(bufferAioCtx);
    StorageAssert(bufferAioCtx->batchCtxMgr == static_cast<void*>(this));

    if (STORAGE_FUNC_FAIL(m_bufMgr->Flush(bufTag, &aioContext))) {
        ret = DSTORE_FAIL;
    } else if (bufferAioCtx->hasStartAioFlag) {
        /*
        * Only succeed dispatching async IO to vfs can we occupy one free aio context.
        * otherwise, we continue to reuse this position context for next buffer.
        */
        m_curFreeMinIndex++;
    }

    return ret;
}
BufferRing CreateBufferRing(BufferAccessType strategyType)
{
    constexpr int minRingSize = 4;
    constexpr int kb = 1024;
    int ringSize = 0;
    switch (strategyType) {
        case BufferAccessType::BAS_NORMAL:
            /* Just give 'em a "default" object if NORMAL is asked for by someone. */
            return nullptr;
        case BufferAccessType::BAS_BULKREAD:
            ringSize = static_cast<int64>(g_storageInstance->GetGuc()->bulkReadRingSize) * kb / BLCKSZ;
            break;
        case BufferAccessType::BAS_BULKWRITE:
            ringSize = static_cast<int64>(g_storageInstance->GetGuc()->bulkWriteRingSize) * kb / BLCKSZ;
            break;
        case BufferAccessType::BAS_VACUUM:
        case BufferAccessType::BAS_REPAIR:
            ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
                   ErrMsg("The current buffer access type is not supported in dstore: %hhu.",
                          static_cast<uint8>(strategyType)));
            return nullptr;
        default:
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
                   ErrMsg("The buffer access type is not recognized: %hhu.", static_cast<uint8>(strategyType)));
            return nullptr;
    }

    /* Ring size must bigger than minRingSize. */
    ringSize = DstoreMax(ringSize, minRingSize);

    /* Make sure ring is not an undue fraction of shared buffers */
    int bufferRatio = minRingSize;
    constexpr int otherRatio = 8;
    if (strategyType != BAS_BULKWRITE && strategyType != BAS_BULKREAD) {
        bufferRatio = otherRatio;
    }
    ringSize = DstoreMin(g_storageInstance->GetGuc()->buffer / bufferRatio, ringSize);

    BufferRing strategyObj = static_cast<BufferRing>(
        DstorePalloc0(offsetof(BufferRingData, bufferDescArray) + ringSize * sizeof(BufferDesc *)));
    if (unlikely(strategyObj == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Alloc memory for strategyObj failed %u.", ringSize));
        return nullptr;
    }
    strategyObj->Init(strategyType, ringSize);
    return strategyObj;
}
void DestroyBufferRing(BufferRing *strategyObj)
{
    DstorePfreeExt(*strategyObj);
}

BufferDesc *BufferRingData::GetFreeBufFromRing()
{
    const int RING_MAX_RETRY_TIMES = 100;
    const float RING_MAX_RETRY_PCT = 0.1;
    uint16 retryTimes = 0;
RETRY:
    if (++curPos >= ringSize) {
        curPos = 0;
    }
    retryTimes++;

    BufferDesc *freeBufDesc = bufferDescArray[curPos];
    if (freeBufDesc == INVALID_BUFFER_DESC) {
        curWasInRing = false;
        return INVALID_BUFFER_DESC;
    }
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR,
           ErrMsg("try get pdbId %hhu page (%hu, %u) from ring buffer.", freeBufDesc->GetPdbId(),
                  freeBufDesc->GetPageId().m_fileId, freeBufDesc->GetPageId().m_blockId));

    if (freeBufDesc->FastLockHdrIfReusable(INVALID_BUFFER_TAG, false)) {
        freeBufDesc->PinUnderHdrLocked();
        uint64 bufState = GsAtomicReadU64(&freeBufDesc->state);
        StorageAssert((bufState & Buffer::BUF_LOCKED) != 0);
        freeBufDesc->UnlockHdr(bufState);
        curWasInRing = true;
        ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR,
               ErrMsg("get pdbId %hhu page (%hu, %u) from ring buffer succeeded.", freeBufDesc->GetPdbId(),
                      freeBufDesc->GetPageId().m_fileId, freeBufDesc->GetPageId().m_blockId));

        return freeBufDesc;
    } else if (likely(retryTimes < Min(RING_MAX_RETRY_TIMES, ringSize * RING_MAX_RETRY_PCT))) {
        goto RETRY;
    }
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR,
           ErrMsg("get pdbId %hhu page (%hu, %u) from ring buffer failed.", freeBufDesc->GetPdbId(),
                  freeBufDesc->GetPageId().m_fileId, freeBufDesc->GetPageId().m_blockId));
    curWasInRing = false;
    return INVALID_BUFFER_DESC;
}
void BufferRingData::AddFreeBufToRing(BufferDesc *bufDesc)
{
    StorageAssert(bufDesc != INVALID_BUFFER_DESC);
    if (bufferDescArray[curPos] != bufDesc) {
        StorageAssert(!curWasInRing);
        bufferDescArray[curPos] = bufDesc;
        ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR,
               ErrMsg("add pdbId %hhu page (%hu, %u) from ring buffer.", bufDesc->GetPdbId(),
                      bufDesc->GetPageId().m_fileId, bufDesc->GetPageId().m_blockId));
    }
}
void BufferRingData::RemoveBufInRing(BufferDesc *bufDesc)
{
    StorageAssert(bufDesc != INVALID_BUFFER_DESC);

    /* BAS_BULKREAD is used only in bulkread mode */
    if (accessType != BufferAccessType::BAS_BULKREAD) {
        return;
    }

    /* The normal buffer-replacement strategy should not be broken */
    /* If the buffer fails to be reused because it is dirty again, the buffer needs to be deleted from the ring. */
    bool isCurBuf = bufferDescArray[curPos] == bufDesc;
    if (!curWasInRing || !isCurBuf) {
        return;
    }
    /*
     * If the buffer in the ring fails to be made free due to dirty, the buffer is deleted from the ring.
     * Note: If all the buffers in the ring are dirty, avoid a dead cycle is necessary.
     */
    bufferDescArray[curPos] = INVALID_BUFFER_DESC;
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR,
           ErrMsg("remove pdbId %hhu page (%hu, %u) from ring buffer.", bufDesc->GetPdbId(),
                  bufDesc->GetPageId().m_fileId, bufDesc->GetPageId().m_blockId));
}

char *BufMgr::GetPdBucketLockInfo()
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        return nullptr;
    }
    return dumpInfo.data;
}

void BufMgr::ReportBgwriter(PdbId pdbId, const int64 slotId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (pdb == nullptr || !pdb->IsInit()) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
            ErrMsg("%s pdb object for pdbId %u.", pdb == nullptr ? "Null" : "Not null", pdbId));
        return;
    }
    CheckpointMgr *checkpointMgr = pdb->GetCheckpointMgr();
    if (!checkpointMgr || !checkpointMgr->IsInitialized()) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
            ErrMsg("%s checkpointMgr object for pdbId %u.", checkpointMgr == nullptr ? "Null" : "Not null", pdbId));
        return;
    }
    uint32 totalCheckpointDone = 0;
    dlist_iter iter;
    checkpointMgr->m_queueSpinlock.Acquire();
    dlist_head *checkPointNodeList = checkpointMgr->GetCheckpointInfoList();
    dlist_foreach(iter, checkPointNodeList) {
        WalCheckpointInfoData *checkpointInfo = dlist_container(WalCheckpointInfoData, node, iter.cur);
        totalCheckpointDone += checkpointInfo->checkpointStreamRequest.GetCheckpointDoneCnt();
    }
    checkpointMgr->m_queueSpinlock.Release();

    BgPageWriterEntry *entry = pdb->GetBgPageWriterMgr()->GetBgPageWriterEntry(slotId);
    GsStatMsgBgWriter *statBgwriter = pdb->GetGsStatMsgBgWriter();
    statBgwriter->m_timedCheckpoints = static_cast<long>(totalCheckpointDone);
    statBgwriter->m_requestedCheckpoints = checkpointMgr->GetRequestedCheckpointsCnt();
    statBgwriter->m_bufAlloc = static_cast<long>(GetBufferAllocCnt());
    if (entry != nullptr && entry->dirtyPageQueue != nullptr) {
        statBgwriter->m_bufWrittenCheckpoints = static_cast<long>(entry->dirtyPageQueue->GetTotalRemoveCnt());
    }

    g_storageInstance->GetStat()->ReportSendBgwriter(statBgwriter);
}

} /* namespace DSTORE */
