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
 * dstore_wal_recovery.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/wal/dstore_wal_recovery.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "wal/dstore_wal_recovery.h"

#include "framework/dstore_pdb.h"
#include "heap/dstore_heap_wal_struct.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "undo/dstore_undo_wal.h"
#include "index/dstore_btree_wal.h"
#include "port/dstore_port.h"
#include "index/dstore_btree_recycle_wal.h"
#include "wal/dstore_wal_reader.h"
#include "wal/dstore_wal_parallel_redo_worker.h"
#include "wal/dstore_wal_perf_statistic.h"
#include "wal/dstore_wal_perf_unit.h"
#include "wal/dstore_wal_utils.h"
#include "wal/dstore_wal_redo_manager.h"
#include "errorcode/dstore_buf_error_code.h"
#include "lock/dstore_lock_interface.h"
#include "systable/dstore_systable_wal.h"

namespace DSTORE {

constexpr uint32 REDO_WORKER_QUE_CAPACITY = 16384;
constexpr uint32 DISPATCH_WORKER_QUE_CAPACITY = 16384;
constexpr uint32 SLEEP_MILLISECONDS = 100;
constexpr uint32 WAIT_DISPATCH_QUEUE_MICROSEC = 10;
constexpr uint64 WAL_READER_MAX_ALLOCATED_SIZE = 1024 * 1024 * 1024UL;
constexpr uint32 WAIT_WAL_REPLAYABLE_SLEEP_MILLISECONDS = 1000;
constexpr uint32 WAIT_COUNT_FOR_REPORT_WARNING = 1000;
constexpr uint32 WAIT_COUNT_FOR_REPORT_REDO_PLSN = 500000;
constexpr uint32 DDL_REDO_WORKER_NUM = 1;
WalRedoStatisticInfo WalRecovery::m_redoStatisticInfo;
WalRedoTypeStatisticInfo WalRecovery::m_walTypeRedoStatisticInfo;
std::atomic<bool> WalRecovery::m_statWalTypeInfo(false);

template uint64 WalRecovery::BuildDirtyPageSetAndPageWalRecordListReadWal<true>(WalRecordReader *walRecordReader);
template uint64 WalRecovery::BuildDirtyPageSetAndPageWalRecordListReadWal<false>(WalRecordReader *walRecordReader);
static bool IsRedoableWalRecord(WalType walType)
{
    if (walType == WAL_CHECKPOINT_SHUTDOWN || walType == WAL_CHECKPOINT_ONLINE || walType == WAL_TYPE_BUTTOM) {
        return false;
    }
    return true;
}

static bool IsWalRecordWithMultiPagesInfo(WalType walType)
{
    return (walType == WAL_TBS_INIT_MULTIPLE_DATA_PAGES || walType == WAL_TBS_INIT_BITMAP_PAGES);
}

static bool IsWalRecordWithoutPageInfo(WalType walType)
{
#ifdef UT
    return ((walType >= WAL_TBS_CREATE_TABLESPACE && walType <= WAL_TBS_ALTER_TABLESPACE) ||
        walType == WAL_EMPTY_DDL_REDO || walType == WAL_SYSTABLE_WRITE_BUILTIN_RELMAP);
#else
    return ((walType >= WAL_TBS_CREATE_TABLESPACE && walType <= WAL_TBS_ALTER_TABLESPACE) ||
        walType == WAL_SYSTABLE_WRITE_BUILTIN_RELMAP);
#endif
}

static bool IsWalRecordWithoutFileInfo(WalType walType)
{
#ifdef UT
    return (walType == WAL_TBS_CREATE_TABLESPACE || walType == WAL_TBS_DROP_TABLESPACE ||
            walType == WAL_TBS_ALTER_TABLESPACE || walType == WAL_TBS_ADD_FILE_TO_TABLESPACE ||
            walType == WAL_EMPTY_DDL_REDO || walType == WAL_SYSTABLE_WRITE_BUILTIN_RELMAP);
#else
    return (walType == WAL_TBS_CREATE_TABLESPACE || walType == WAL_TBS_DROP_TABLESPACE ||
            walType == WAL_TBS_ADD_FILE_TO_TABLESPACE || walType == WAL_TBS_ALTER_TABLESPACE ||
            walType == WAL_SYSTABLE_WRITE_BUILTIN_RELMAP);
#endif
}

WalRecovery::WalRecovery(DstoreMemoryContext memoryContext, WalStream *walStream, uint64 walFileSize, WalId walId)
    : m_memoryContext(memoryContext),
      m_recordReaderMemContext(nullptr),
      m_recoveryStartPlsn(INVALID_PLSN),
      m_recoveryEndPlsn(INVALID_PLSN),
      m_diskRecoveryStartPlsn(INVALID_PLSN),
      m_walStream(walStream),
      m_redoReadBuffer(nullptr),
      m_getDirtyPageReadBuffer(nullptr),
      m_walFileSize(walFileSize),
      m_walId(walId),
      m_redoWorkerNum(0),
      m_enableParallelRedo(false),
      m_redoWorkersMaxDispatchedPlsn(nullptr),
      m_dispatchWorkerThd(nullptr),
      m_dispatchQueue(nullptr),
      m_stage(WalRecoveryStage::RECOVERY_NO_START),
      m_pageWalRecordInfoListHtab(),
      m_dirtyPageEntryArray(nullptr),
      m_dirtyPageEntryArraySize(0),
      m_isDirtyPageSetBuilt(false),
      m_lastGroupEndPlsn(0),
      m_redoMode(RedoMode::RECOVERY_REDO),
      m_pdbId(INVALID_PDB_ID),
      m_curRedoFinishedPlsn (0),
      m_barrierCsn(INVALID_CSN),
      m_curSyncMode(PdbSyncMode::INVALID_SYNC_MODE),
      m_term(0),
      m_useAio(USE_VFS_AIO)
{
    for (uint32 i = 0; i < MAX_REDO_WORKER_NUM; i++) {
        m_parallelRedoWorkers[i] = nullptr;
    }
    for (uint32 i = 0; i < PAGE_WAL_RECORD_HTAB_PUTTER_NUM; i++) {
        m_pageWalRecordDispatchQueues[i] = nullptr;
        m_buildDirtyPageSetWorkerThd[i] = nullptr;
    }
    for (FileId i = 0; i < MAX_DATA_FILE_ID; i++) {
        m_droppedFileArray[i] = false;
    }
}

WalRecovery::~WalRecovery()
{
    Destroy();
    m_walStream = nullptr;
    m_dirtyPageEntryArray = nullptr;
    m_memoryContext = nullptr;
}

void WalRecovery::Destroy() noexcept
{
    if (m_dirtyPageEntryArray) {
        DstorePfree(m_dirtyPageEntryArray);
        m_dirtyPageEntryArray = nullptr;
    }

    if (m_dispatchWorkerThd != nullptr) {
        if (m_dispatchWorkerThd->joinable()) {
            ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[Replica] pdb %u, wal %lu, try wait m_dispatchWorkerThd.",
                m_pdbId, m_walId));
            m_dispatchWorkerThd->join();
            ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[Replica] pdb %u, wal %lu, wait m_dispatchWorkerThd.",
                m_pdbId, m_walId));
        }
        delete m_dispatchWorkerThd;
        m_dispatchWorkerThd = nullptr;
    }
    if (m_dispatchQueue != nullptr) {
        m_dispatchQueue->Destroy();
        DstorePfreeExt(m_dispatchQueue);
    }
    DstorePfreeExt(m_redoWorkersMaxDispatchedPlsn);
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[Replica] pdb %u, wal %lu, destroy wal recovery.",
        m_pdbId, m_walId));
}

RetStatus WalRecovery::Init(RedoMode redoMode, PdbId pdbId, uint64 term)
{
    /* Step 1: Initilize worker threads for redo */
    m_redoWorkerNum = g_storageInstance->GetGuc()->recoveryWorkerNum;
    if (m_redoWorkerNum > MAX_REDO_WORKER_NUM) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Set too many redo workers."));
        return DSTORE_FAIL;
    }

    m_enableParallelRedo = m_redoWorkerNum > 1;

    /* Step 2: Initilize page's wal record list hash table structure */
    LWLockInitialize(&m_getPageWalRecordsLock, LWLOCK_GROUP_GET_PAGE_WAL_RECORDS);
    SetWalRecoveryStage(WalRecoveryStage::RECOVERY_STARTING);
    m_redoMode = redoMode;
    m_pdbId = pdbId;
    m_term = term;

    /* Step 3: Get the recovery lsn of checkpoint */
    ControlWalStreamPageItemData *walStreamInfo = nullptr;
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("WalRecovery get pdb failed, pdbId(%u).", m_pdbId));
        return DSTORE_FAIL;
    }
    ControlFile *controlFile = pdb->GetControlFile();
    StorageReleasePanic(controlFile == nullptr, MODULE_WAL, ErrMsg("WalRecovery get control file failed."));
    RetStatus retStatus = controlFile->GetWalStreamInfo(m_walId, &walStreamInfo);
    if (STORAGE_FUNC_FAIL(retStatus)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
               ErrMsg("[PDB:%u WAL:%lu]WalRecovery control file get wal stream info fail.", m_pdbId, m_walId));
        return DSTORE_FAIL;
    }
    InitRecoveryPlsn(walStreamInfo->walId, &walStreamInfo->lastWalCheckpoint);
    controlFile->FreeWalStreamsInfo(walStreamInfo);

    InitRedoStatisticInfo(m_recoveryStartPlsn, m_redoWorkerNum);
    InitStatisticInfo();
    return DSTORE_SUCC;
}

void WalRecovery::BatchPutToDispatchQueue(RedoWalRecordEntry *recordEntry, uint32 *recordEntryNum,
    uint32 entryNumThreshold)
{
    if (*recordEntryNum > MAX_REDO_QUE_BATCH_NUM) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery BatchPutToDispatchQueue get invalid batch num %u.",
            m_pdbId, m_walId, *recordEntryNum));
    }
    if (*recordEntryNum >= entryNumThreshold) {
        ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg("BatchPutToDispatchQueue get batch num %u, dispach plsn %lu",
            *recordEntryNum, recordEntry[*recordEntryNum - 1].recordEndPlsn));
        m_dispatchQueue->PutN<RedoWalRecordEntry *>(recordEntry, *recordEntryNum);
        *recordEntryNum = 0;
    }
}

inline void WalRecovery::FillBatchRedoEntry(RedoWalRecordEntry &entry,
    const WalRecordRedoContext &redoCtx, const WalRecord *walRecord) const
{
    entry.recordEndPlsn = redoCtx.recordEndPlsn;
    entry.ctx = redoCtx;
    entry.walRecordNeedFree = false;
    entry.walRecordInfo.walRecord = walRecord;
}

void WalRecovery::AddPlsnSyncer(RedoWalRecordEntry &recordEntry, uint32 &recordEntryNum, uint64 recordEndPlsn)
{
    recordEntry.recordEndPlsn = recordEndPlsn;
    recordEntry.ctx = {INVALID_XID, INVALID_WAL_ID, INVALID_PDB_ID, INVALID_PLSN};
    recordEntry.walRecordNeedFree = false;
    recordEntry.walRecordInfo.walRecord = nullptr;
    recordEntryNum++;
    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
        ErrMsg("AddPlsnSyncer plsn %lu, entry num %u", recordEndPlsn, recordEntryNum));
}

RetStatus WalRecovery::Redo(uint64 *lastGroupEndPlsn)
{
    RetStatus retStatus;
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery redo start recoveryStartPlsn:%lu diskRecoveryStartPlsn:%lu",
            m_pdbId, m_walId, m_recoveryStartPlsn, m_diskRecoveryStartPlsn));
    /* allocate wal reader, which reads atomic wal groups from read buffer */
    WalRecordReader *walRecordReader = RedoAllocateWalReader(m_recoveryStartPlsn);
    if (STORAGE_VAR_NULL(walRecordReader) || STORAGE_FUNC_FAIL(walRecordReader->Init())) {
        delete walRecordReader;
        return DSTORE_FAIL;
    }

    BuffForDecompress buffForDecompress;
    buffForDecompress.bufferSize = INIT_TEMP_BUFF_SIZE;
    buffForDecompress.buffer = static_cast<char *>(DstorePalloc0(INIT_TEMP_BUFF_SIZE));
    if (buffForDecompress.buffer == nullptr) {
        delete walRecordReader;
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery new buffForDecompress error!",
            m_pdbId, m_walId));
        return DSTORE_FAIL;
    }

    /* start wal loader thread: load flushed wal blocks into wal read buffers */
    if (m_enableParallelRedo && STORAGE_FUNC_FAIL(RedoLoadWalToBuffer(m_recoveryStartPlsn))) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery RedoLoadWalToBuffer fail.",
            m_pdbId, m_walId));
        DstorePfreeExt(buffForDecompress.buffer);
        delete walRecordReader;
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(RedoLoopReadAndRedoWalRecord(
        walRecordReader, m_recoveryStartPlsn, &buffForDecompress, lastGroupEndPlsn))) {
        DstorePfreeExt(buffForDecompress.buffer);
        delete walRecordReader;
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        retStatus = DSTORE_FAIL;
    }
    retStatus = DSTORE_SUCC;
    delete walRecordReader;
    DstorePfreeExt(buffForDecompress.buffer);
    return retStatus;
}

RetStatus WalRecovery::RedoLoadWalToBuffer(uint64 loadStartPlsn)
{
    uint64 walRedoReadBufferSize = static_cast<uint64>(g_storageInstance->GetGuc()->walRedoBufferSize);
    WalFileLoadToBufferConf walFileLoadToBufferConf = {loadStartPlsn,
        static_cast<uint32>(walRedoReadBufferSize / WAL_READ_BUFFER_BLOCK_SIZE), WAL_READ_BUFFER_BLOCK_SIZE,
        walRedoReadBufferSize};
    StorageReleasePanic(m_redoReadBuffer == nullptr, MODULE_WAL,
                        ErrMsg("RedoLoadWalToBuffer get invalid redoReadBuffer"));
    return m_redoReadBuffer->StartLoadToBuffer(walFileLoadToBufferConf);
}

WalRecordReader *WalRecovery::RedoAllocateWalReader(uint64 readStartPlsn)
{
    WalRecordReader *walRecordReader;
    WalReadSource readSource =
    m_enableParallelRedo ? WalReadSource::WAL_READ_FROM_BUFFER : WalReadSource::WAL_READ_FROM_DISK;
    WalReaderConf conf = {m_walId, readStartPlsn, m_walStream, m_redoReadBuffer, m_walFileSize, readSource};
    walRecordReader = DstoreNew(m_recordReaderMemContext) WalRecordReader(m_recordReaderMemContext, conf);
    return walRecordReader;
}

bool WalRecovery::ReadWalPlsnToMaxFlushed()
{
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]Don't need to judge max flushed plsn.", m_pdbId, m_walId));
    return true;
}

RetStatus WalRecovery::RedoLoopReadAndRedoWalRecord(WalRecordReader *walRecordReader,
    uint64 redoStartPlsn, BuffForDecompress *buffForDecompress, uint64 *lastGroupEndPlsn)
{
    const WalRecordAtomicGroup *walGroup = nullptr;
    const WalRecord *walRecord = nullptr;
    uint64 readerAllocatedSize = 0;
    uint64 recordEndPlsn = redoStartPlsn;
    BatchPutParaForWalReplay parallelEntrys;
    parallelEntrys.entryNum = 0;
    uint64 lastUpdateFlushedPlsn = 0;
    SetWalRecoveryStage(WalRecoveryStage::RECOVERY_REDO_STARTED);
    RetStatus retStatus = DSTORE_FAIL;
    FAULT_INJECTION_WAIT(DstorePdbReplicaFI::BEFORE_WAL_REPLAY_POINT);
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery RedoLoopReadAndRedoWalRecord start.", m_pdbId, m_walId));

    /* each iteration processes an atomic wal group */
    do {
        retStatus = walRecordReader->ReadNext(&walGroup);
        if (STORAGE_FUNC_FAIL(retStatus)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery read next group fail, currPlsn %lu.",
                m_pdbId, m_walId, walRecordReader->GetCurGroupStartPlsn()));
            goto END;
        }

        /*
         * for standy redo, exit only when redo thread is stopped. try again if walGroup is null.
         * for other case, the "normal" exit point of wal replay -- wal reader sees the end of wal records.
         */
        if (walGroup == nullptr) {
            ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg("walGroup is null"));
            /* dispatch plsn syncer for standby redo */
            if (m_redoMode == RedoMode::PDB_STANDBY_REDO && m_enableParallelRedo && recordEndPlsn > redoStartPlsn) {
                AddPlsnSyncer(parallelEntrys.redoWalRecordEntry[parallelEntrys.entryNum], parallelEntrys.entryNum,
                    recordEndPlsn);
                BatchPutToDispatchQueue(&parallelEntrys.redoWalRecordEntry[0], &parallelEntrys.entryNum);
            }
            if (m_redoMode == RedoMode::PDB_STANDBY_REDO && !m_walStream->IsHasStopWalReceiving()) {
                GaussUsleep(SLEEP_MILLISECONDS);
                continue;
            } else if (m_redoMode == RedoMode::PDB_STANDBY_REDO && !ReadWalPlsnToMaxFlushed()) {
                GaussUsleep(SLEEP_MILLISECONDS);
                continue;
            } else {
                break;
            }
        }

        if (m_enableParallelRedo && walRecordReader->NeedFreeCurGroup(walGroup)) {
            readerAllocatedSize += walGroup->groupLen;
            if (unlikely(readerAllocatedSize > WAL_READER_MAX_ALLOCATED_SIZE)) {
                ErrLog(DSTORE_ERROR, MODULE_WAL,
                       ErrMsg("wal record reader allocate %lu bytes memory, not free.", readerAllocatedSize));
            }
        }
        if (m_enableParallelRedo && parallelEntrys.entryNum + walGroup->recordNum >= MAX_REDO_QUE_BATCH_NUM) {
            AddPlsnSyncer(parallelEntrys.redoWalRecordEntry[parallelEntrys.entryNum], parallelEntrys.entryNum,
                recordEndPlsn);
            BatchPutToDispatchQueue(&parallelEntrys.redoWalRecordEntry[0], &parallelEntrys.entryNum,
                MAX_REDO_QUE_BATCH_NUM);
        }
        /* redo each wal record */
        for (uint16 i = 0; i < walGroup->recordNum; i++) {
            walRecord = walRecordReader->GetNextWalRecord();
            StorageReleasePanic(walRecord == nullptr, MODULE_WAL, ErrMsg("Invalid wal record."));
            recordEndPlsn = walRecordReader->GetCurRecordEndPlsn();
            WalRecordRedoContext redoCtx = {walGroup->xid, m_walId, m_pdbId, recordEndPlsn};
            if (m_enableParallelRedo) {
                FillBatchRedoEntry(parallelEntrys.redoWalRecordEntry[parallelEntrys.entryNum], redoCtx, walRecord);
                parallelEntrys.entryNum++;
                /* put each wal record to dispatch queue for dispatcher thread to take */
                BatchPutToDispatchQueue(&parallelEntrys.redoWalRecordEntry[0], &parallelEntrys.entryNum,
                    MAX_REDO_QUE_BATCH_NUM);
            } else {
                if (STORAGE_FUNC_FAIL(RedoSingle(&redoCtx, walRecord, 0, buffForDecompress))) {
                    goto END;
                }
                m_curRedoFinishedPlsn = recordEndPlsn;
            }
        }
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
            ErrMsg("WalRecovery Redo put to dispatch queue up to plsn: %lu", recordEndPlsn));

        if (recordEndPlsn - lastUpdateFlushedPlsn >= WAL_READ_BUFFER_BLOCK_SIZE) {
            lastUpdateFlushedPlsn = recordEndPlsn;
        }
    } while (true);
    *lastGroupEndPlsn = recordEndPlsn;
    retStatus = DSTORE_SUCC;

END:
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery Redo dispatch finish, last group end plsn: %lu",
        m_pdbId, m_walId, *lastGroupEndPlsn));
    /* put INVALID_PLSN to stop dispatcher */
    if (m_enableParallelRedo) {
        AddPlsnSyncer(parallelEntrys.redoWalRecordEntry[parallelEntrys.entryNum], parallelEntrys.entryNum,
            recordEndPlsn);
        BatchPutToDispatchQueue(&parallelEntrys.redoWalRecordEntry[0], &parallelEntrys.entryNum,
                                MAX_REDO_QUE_BATCH_NUM);
        parallelEntrys.redoWalRecordEntry[parallelEntrys.entryNum++].recordEndPlsn = INVALID_PLSN;
        m_dispatchQueue->PutN<RedoWalRecordEntry *>(&parallelEntrys.redoWalRecordEntry[0], parallelEntrys.entryNum);
    }
    return retStatus;
}

bool WalRecovery::IsLsnError(const WalRecordLsnInfo &pageLsn, const WalRecordLsnInfo &recordPreLsn)
{
    if (recordPreLsn.glsn == pageLsn.glsn && recordPreLsn.walId != pageLsn.walId) {
        return false;
    }
    
    return (pageLsn > recordPreLsn);
}

WalRecordReplayType WalRecovery::GetWalRecordReplayType(const BufferTag bufTag, Page *page,
                                                        const WalRecordLsnInfo &pagePreRecordLsnInfo,
                                                        const WalRecordLsnInfo &recordLsnInfo, bool reportWarning)
{
    WalRecordReplayType replayType;
    if (recordLsnInfo.glsn > page->GetGlsn()) {
        if (recordLsnInfo.glsn == page->GetGlsn() + 1) {
            if (unlikely(pagePreRecordLsnInfo.glsn != page->GetGlsn() ||
                pagePreRecordLsnInfo.endPlsn != page->GetPlsn())) {
                /**
                 * at least a previous wal record with
                 * (pagePreRecordLsnInfo.glsn, pagePreRecordLsnInfo.endPlsn)
                 * has not been replayed yet.
                 */
                replayType = WalRecordReplayType::NOT_REPLAYABLE;
            } else {
                replayType = WalRecordReplayType::REPLAYABLE;
            }
        } else {
            /**
             * glsn is more than 1 larger than the page's glsn. Some other record(s)
             * has to be applied before this.
             */
            replayType = WalRecordReplayType::NOT_REPLAYABLE;
        }
    } else if (recordLsnInfo.glsn == page->GetGlsn()) {
        if (recordLsnInfo.endPlsn > page->GetPlsn()) {
            replayType = WalRecordReplayType::REPLAYABLE;
        } else {
            /* a wal record with the same or larger plsn is already replayed */
            replayType = WalRecordReplayType::NO_NEED_TO_REPLAY;
        }
    } else {
        replayType = WalRecordReplayType::NO_NEED_TO_REPLAY;
    }

    if (replayType == WalRecordReplayType::REPLAYABLE) {
        if (unlikely(pagePreRecordLsnInfo.glsn != page->GetGlsn() ||
            pagePreRecordLsnInfo.endPlsn != page->GetPlsn() ||
            pagePreRecordLsnInfo.walId != page->GetWalId())) {
            /* page lsn and wal info is inconsistent with last repalyed wal record */
            replayType = WalRecordReplayType::NOT_REPLAYABLE;
        }
    }
    if (replayType == WalRecordReplayType::NO_NEED_TO_REPLAY) {
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]Skip the wal whose pre LsnInfo is (%lu,%lu,%lu), page (%d, %u) LsnInfo is "
                   "(%lu,%lu,%lu) record LsnInfo (%lu,%lu,%lu)",
                   bufTag.pdbId, recordLsnInfo.walId, pagePreRecordLsnInfo.walId, pagePreRecordLsnInfo.endPlsn,
                   pagePreRecordLsnInfo.glsn, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, page->GetWalId(),
                   page->GetPlsn(), page->GetGlsn(), recordLsnInfo.walId, recordLsnInfo.endPlsn, recordLsnInfo.glsn));
    } else if (replayType == WalRecordReplayType::REPLAYABLE) {
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]The wal whose pre LsnInfo is (%lu,%lu,%lu), page (%d, %u) LsnInfo is "
                   "(%lu,%lu,%lu), record LsnInfo (%lu,%lu,%lu) replayType = REPLAYABLE",
                   bufTag.pdbId, recordLsnInfo.walId, pagePreRecordLsnInfo.walId, pagePreRecordLsnInfo.endPlsn,
                   pagePreRecordLsnInfo.glsn, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, page->GetWalId(),
                   page->GetPlsn(), page->GetGlsn(), recordLsnInfo.walId, recordLsnInfo.endPlsn, recordLsnInfo.glsn));
    }
    if (replayType == WalRecordReplayType::NOT_REPLAYABLE) {
        WalRecordLsnInfo lsnInfoInpage = {page->GetWalId(), page->GetPlsn(), page->GetGlsn()};
        ErrLog(reportWarning ? DSTORE_LOG : DSTORE_DEBUG1, MODULE_WAL,
               ErrMsg("[PDB:%u WAL:%lu]unhandleable whose pre LsnInfo is (%lu,%lu,%lu), page (%d, %u) LsnInfo is "
                      "(%lu,%lu,%lu) record LsnInfo (%lu,%lu,%lu), is some error in page: %s",
                      bufTag.pdbId, recordLsnInfo.walId, pagePreRecordLsnInfo.walId, pagePreRecordLsnInfo.endPlsn,
                      pagePreRecordLsnInfo.glsn, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, lsnInfoInpage.walId,
                      lsnInfoInpage.endPlsn, lsnInfoInpage.glsn, recordLsnInfo.walId, recordLsnInfo.endPlsn,
                      recordLsnInfo.glsn, WalRecovery::IsLsnError(lsnInfoInpage, pagePreRecordLsnInfo) ? "Yes" : "No"));
    }
    return replayType;
}

WalRecordReplayType WalRecovery::WalRecordCheckFileVersion(const PdbId pdbId, const FileId fileId,
                                                           uint64 recordVersionInfo, uint64 *versionInFile)
{
    WalRecordReplayType replayType;
    *versionInFile = WalUtils::GetFileVersion(pdbId, fileId);
    if (*versionInFile > recordVersionInfo) {
        replayType = WalRecordReplayType::NO_NEED_TO_REPLAY;
    } else if (*versionInFile < recordVersionInfo) {
        replayType = WalRecordReplayType::NOT_REPLAYABLE;
    } else {
        replayType = WalRecordReplayType::REPLAYABLE;
    }
    return replayType;
}

WalRecordReplayType WalRecovery::WalRecordCheckTbsVersion(const PdbId pdbId, const TablespaceId tbsId,
                                                          uint64 recordVersionInfo, uint64 *versionInFile)
{
    WalRecordReplayType replayType;
    *versionInFile = WalUtils::GetTbsVersion(pdbId, tbsId);
    if (*versionInFile > recordVersionInfo) {
        replayType = WalRecordReplayType::NO_NEED_TO_REPLAY;
    } else if (*versionInFile < recordVersionInfo) {
        replayType = WalRecordReplayType::NOT_REPLAYABLE;
    } else {
        LockInterface::TablespaceLockContext context;
        context.pdbId = pdbId;
        context.tablespaceId = tbsId;
        context.dontWait = true;
        context.mode = DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK;
        if (LockInterface::LockTablespace(&context) != DSTORE_SUCC) {
            replayType = WalRecordReplayType::NOT_REPLAYABLE;
        } else {
            replayType = WalRecordReplayType::REPLAYABLE;
        }
    }
    return replayType;
}

void WalRecovery::AddRedoStatisticInfo(double time, WalType walType, uint64 endPlsn,
    uint32 workerId, const WalRecord *walRecord)
{
    m_redoStatisticInfo.walTypeRedoCnt[walType][workerId]++;
    m_redoStatisticInfo.walRedoCurPlsn[workerId] = DstoreMax(m_redoStatisticInfo.walRedoCurPlsn[workerId], endPlsn);
    m_redoStatisticInfo.walTypeRedoSize[walType][workerId] += walRecord->m_size;
    m_redoStatisticInfo.walTypeRedoTime[walType][workerId] += time;
    m_redoStatisticInfo.walRedoTime[workerId] += time;
}

bool WalRecovery::IsSupportCompress(WalType recordType)
{
    switch (recordType) {
#ifdef UT
        case WAL_EMPTY_REDO:
        case WAL_EMPTY_DDL_REDO:
#endif
        case WAL_TBS_INIT_ONE_DATA_PAGE:
        case WAL_TBS_INIT_ONE_BITMAP_PAGE:
        case WAL_TBS_INIT_MULTIPLE_DATA_PAGES:
        case WAL_TBS_INIT_BITMAP_PAGES:
        case WAL_TBS_CREATE_TABLESPACE:
        case WAL_TBS_CREATE_DATA_FILE:
        case WAL_TBS_ADD_FILE_TO_TABLESPACE:
        case WAL_TBS_DROP_TABLESPACE:
        case WAL_TBS_DROP_DATA_FILE:
        case WAL_TBS_ALTER_TABLESPACE:
        case WAL_NEXT_CSN:
        case WAL_BARRIER_CSN:
        case WAL_SYSTABLE_WRITE_BUILTIN_RELMAP: {
            return false;
        }
        default:
            return true;
    }
}

void WalRecovery::ReAllocBuffForDecompress(uint32 newBuffSize, uint32 oldBufSize, void **buffer)
{
    char *newBuf = static_cast<char *>(DstorePalloc0(newBuffSize));
    if (newBuf == nullptr) {
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalRecovery new buffForDecompress error!"));
    }
    errno_t rc = memcpy_s(newBuf, newBuffSize, static_cast<char *>(*buffer), oldBufSize);
    storage_securec_check(rc, "\0", "\0");
    DstorePfreeExt(*buffer);
    *buffer = newBuf;
}

WalRecordForPage *WalRecovery::DecompressProc(const WalRecord *walRecord, BuffForDecompress *buffForDecompress)
{
    WalRecordForPage *recordForPage = static_cast<WalRecordForPage *>(buffForDecompress->buffer);
    StorageReleasePanic(recordForPage == nullptr, MODULE_WAL, ErrMsg("DecompressProc get invalid decompress buffer"));
    uint16 compressLen = 0;
    uint16 headerSize = sizeof(WalRecordForPage);
    const WalRecordCompressAndDecompressItem *walRecordItem = nullptr;
    if (walRecord->GetType() >= WAL_UNDO_INIT_MAP_SEGMENT && walRecord->GetType() <= WAL_TXN_ABORT) {
        walRecordItem = WalRecordUndo::GetWalRecordItem(walRecord->GetType());
    }
    if (walRecordItem != nullptr) {
        compressLen = walRecordItem->decompress(static_cast<WalRecord *>(recordForPage), walRecord);
        headerSize = walRecordItem->headerSize;
    } else {
        compressLen = recordForPage->Decompress(walRecord);
    }

    if (unlikely((walRecord->GetSize() < compressLen) ||
                    (static_cast<uint32>(recordForPage->GetSize() - headerSize) !=
                        static_cast<uint32>(walRecord->GetSize() - compressLen)))) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
               ErrMsg("RedoWalRecord size err! walType:%hu, compressed total len:%hu,"
                      " decompressed len:%hu serialize size:%hhu.",
                      static_cast<uint16>(walRecord->m_type), walRecord->GetSize(), recordForPage->GetSize(),
                      compressLen));
        return nullptr;
    }
    uint32 leftNeedCopyLen = static_cast<uint32>(walRecord->GetSize() - compressLen);
    if (leftNeedCopyLen == 0) {
        return recordForPage;
    }

    if (unlikely(leftNeedCopyLen > (buffForDecompress->bufferSize - headerSize))) {
        ReAllocBuffForDecompress(leftNeedCopyLen + headerSize, headerSize, &(buffForDecompress->buffer));
        buffForDecompress->bufferSize = leftNeedCopyLen + headerSize;
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("RedoWalRecord new buffForDecompress SIZE %u!", leftNeedCopyLen));
    }
    errno_t rc = memcpy_s(static_cast<char *>(buffForDecompress->buffer) + headerSize,
        buffForDecompress->bufferSize - headerSize,
        static_cast<const char *>(static_cast<const void *>(walRecord)) + compressLen,
        leftNeedCopyLen);
    storage_securec_check(rc, "\0", "\0");

    return static_cast<WalRecordForPage *>(buffForDecompress->buffer);
}

RetStatus WalRecovery::OnDemandRedoWalRecord(WalRecordRedoContext *redoCtx, WalRecord *walRecord,
                                             BufferDesc *bufferDesc)
{
    WalType walType = walRecord->m_type;
    PageId pageId = bufferDesc->GetPageId();
    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
           ErrMsg("OnDemandRedoWalRecord Wal record type:%s, pageId(%hu, %u) walId:%lu endPlsn:%lu",
                  g_walTypeForPrint[static_cast<uint16>(walType)], pageId.m_fileId, pageId.m_blockId, redoCtx->walId,
                  redoCtx->recordEndPlsn));
    RetStatus ret = DSTORE_SUCC;
    ret = RedoWalRecordByType(redoCtx, walRecord, bufferDesc, walType);
    DstorePfree(walRecord);

    return ret;
}

RetStatus WalRecovery::RedoWalRecordByType(WalRecordRedoContext *redoCtx, const WalRecord *walRecord,
                                           BufferDesc *bufferDesc, WalType walType)
{
    PageId pageId = bufferDesc->GetPageId();
    const WalRecordForPage *walRecordForPage = static_cast<const WalRecordForPage *>(walRecord);
    StorageReleasePanic(walRecordForPage == nullptr, MODULE_WAL, ErrMsg("RedoWalRecordByType get wal record failed."));
    if (unlikely(redoCtx == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalRecovery redo context is nullptr."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
        ErrMsg("Wal record type:%hu, pageId(%hu, %u) walId:%lu endPlsn:%lu endGlsn:%lu", static_cast<uint16>(walType),
               pageId.m_fileId, pageId.m_blockId, redoCtx->walId, redoCtx->recordEndPlsn,
               walRecordForPage->m_pagePreWalId != redoCtx->walId ? walRecordForPage->m_pagePreGlsn + 1
                                                                  : walRecordForPage->m_pagePreGlsn));
    if (unlikely(walType >= WAL_TYPE_BUTTOM)) {
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Invalid Wal type:%hu.", static_cast<uint16>(walType)));
        return DSTORE_FAIL;
    }
    if (walType <= WAL_HEAP_FORCE_UPDATE_TUPLE_DATA_NO_TRX) {
        WalRecordHeap::RedoHeapRecord(redoCtx, static_cast<const WalRecordHeap *>(walRecord), bufferDesc);
    } else if (walType >= WAL_BTREE_BUILD && walType <= WAL_BTREE_ERASE_INS_FOR_DEL_FLAG) {
        WalRecordIndex::RedoIndexRecord(redoCtx, static_cast<const WalRecordIndex *>(walRecord), bufferDesc);
    } else if (walType >= WAL_BTREE_RECYCLE_PARTITION_INIT_PAGE &&
               walType <= WAL_BTREE_RECYCLE_ROOT_META_SET_PARTITION_META) {
        WalRecordBtrRecycle::RedoBtrRecycleRecord(redoCtx, static_cast<const WalRecordBtrRecycle *>(walRecord),
                                                  bufferDesc);
    } else if (walType >= WAL_TBS_INIT_BITMAP_META_PAGE && walType <= WAL_TBS_UPDATE_FIRST_FREE_BITMAP_PAGE) {
        WalRecordTbs::RedoTbsRecord(redoCtx, static_cast<const WalRecordTbs *>(walRecord), bufferDesc);
    } else if (walType >= WAL_UNDO_INIT_MAP_SEGMENT && walType <= WAL_TXN_ABORT) {
        WalRecordUndo::RedoUndoRecord(redoCtx, static_cast<const WalRecordUndo *>(walRecord), bufferDesc);
    } else if (walType >= WAL_SYSTABLE_WRITE_BUILTIN_RELMAP && walType <= WAL_SYSTABLE_WRITE_BUILTIN_RELMAP) {
        WalRecordSystable::RedoSystableRecord(static_cast<const WalRecordSystable *>(walRecord), redoCtx->pdbId);
    } else {
        ErrLog(DSTORE_WARNING,
               MODULE_WAL, ErrMsg("Wal type:%hu, not support redo now.", static_cast<uint16>(walType)));
    }

    return DSTORE_SUCC;
}

/* read target buffer, wait until get avialable buffer, return null if file version is bigger */
BufferDesc *WalRecovery::RecoveryReadBuffer(const ParaForReadBuffer para)
{
    BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    bufferDesc = bufMgr->RecoveryRead(m_pdbId, para.pageId);
    /* if get buffer failed, maybe the page is not extended yet, wait and retry */
    uint64 loopCount = 0;
    const uint64 shortSleepTime = 10; // 10us
    const uint64 longSleepTime = 10000; // 10ms
    const uint64 longSleepThreshold = 1000;
    while (bufferDesc == INVALID_BUFFER_DESC) {
        if (WalUtils::GetFileVersion(m_pdbId, para.pageId.m_fileId) <= para.filePreVersion) {
            if (++loopCount % WAIT_COUNT_FOR_REPORT_WARNING == 0) {
                ErrLog(DSTORE_ERROR, MODULE_WAL,
                       ErrMsg("RecoveryReadBuffer get invalid bufferDesc, pageId(%hu, %u), lsn: %lu, %lu",
                              para.pageId.m_fileId, para.pageId.m_blockId, para.walId, para.plsn));
            }
            GaussUsleep(loopCount >= longSleepThreshold ? longSleepTime : shortSleepTime);
            bufferDesc = bufMgr->RecoveryRead(m_pdbId, para.pageId);
        } else {
            break;
        }
    }
    return bufferDesc;
}

FileId WalRecovery::GetFileIdFromTbsRecord(const WalRecord * walrecordTbsLogical)
{
    FileId fileId = INVALID_DATA_FILE_ID;
    switch (walrecordTbsLogical->GetType()) {
        case WAL_TBS_CREATE_DATA_FILE: {
            const WalRecordTbsCreateDataFile *walRecordTbsCreateDataFile =
                static_cast<const WalRecordTbsCreateDataFile *>(walrecordTbsLogical);
            fileId = walRecordTbsCreateDataFile->GetFileId();
            break;
        }
        case WAL_TBS_DROP_DATA_FILE: {
            const WalRecordTbsDropDataFile *walRecordTbsDropDataFile =
                static_cast<const WalRecordTbsDropDataFile *>(walrecordTbsLogical);
            fileId = walRecordTbsDropDataFile->GetFileId();
            break;
        }
        case WAL_TBS_ADD_FILE_TO_TABLESPACE: {
            const WalRecordTbsAddFileToTbs *walRecordTbsAddFileToTbs =
                static_cast<const WalRecordTbsAddFileToTbs *>(walrecordTbsLogical);
            fileId = walRecordTbsAddFileToTbs->GetFileId();
            break;
        }
        default: {
            break;
        }
    }
    return fileId;
}

/* already handle buffer, check it */
WalRecordReplayType WalRecovery::WaitAvailableBufferForRedo(RedoWalRecordBuffPara *buffPara,
                                                            WalRecordRedoContext *redoCtx,
                                                            const WalRecordForPage *recordForPage)
{
    BufferDesc *bufferDesc = *(buffPara->bufferDesc);
    StorageReleasePanic((bufferDesc == nullptr || recordForPage == nullptr), MODULE_WAL,
                        ErrMsg("WaitAvailableBufferForRedo get invalid buffer"));
    PageId pageId = bufferDesc->GetPageId();
    uint64 loopCount = 0;
    WalRecordLsnInfo preRecordLsnInfo = {recordForPage->m_pagePreWalId, recordForPage->m_pagePrePlsn,
                                         recordForPage->m_pagePreGlsn};
    uint64 recordGlsn = recordForPage->m_pagePreWalId != redoCtx->walId ? recordForPage->m_pagePreGlsn + 1
                                                                        : recordForPage->m_pagePreGlsn;
    WalRecordLsnInfo recordLsnInfo = {redoCtx->walId, redoCtx->recordEndPlsn, recordGlsn};
    WalRecordReplayType replayType;
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    do {
        loopCount++;
        if (bufferDesc == INVALID_BUFFER_DESC) {
            /* step 1 read buffer again */
            ParaForReadBuffer paraForReadBuffer{pageId, recordForPage->m_filePreVersion, redoCtx->walId,
                                                redoCtx->recordEndPlsn};
            bufferDesc = RecoveryReadBuffer(paraForReadBuffer);
            *(buffPara->bufferDesc) = bufferDesc;
            if (bufferDesc == INVALID_BUFFER_DESC) {
                return WalRecordReplayType::NO_NEED_TO_REPLAY;
            }
        }

        /* step 2 check file version */
        uint64 versionInFile;
        replayType = WalRecovery::WalRecordCheckFileVersion(m_pdbId, recordForPage->m_pageId.m_fileId,
                                                            recordForPage->m_filePreVersion, &versionInFile);
        if (replayType == WalRecordReplayType::NOT_REPLAYABLE) {
            if (loopCount % WAIT_COUNT_FOR_REPORT_WARNING == 0 && bufferDesc != INVALID_BUFFER_DESC) {
                ErrLog(DSTORE_LOG, MODULE_WAL,
                       ErrMsg("[PDB:%u WAL:%lu] Worker %u wait available buffer for replayable, pageId (%u, %u), "
                              "filePreVersion:%lu, versionInFile:%lu, lsn: %lu, type: %s.",
                              m_pdbId, redoCtx->walId, buffPara->workerId, bufferDesc->GetPageId().m_fileId,
                              bufferDesc->GetPageId().m_blockId, recordForPage->m_filePreVersion, versionInFile,
                              redoCtx->recordEndPlsn, g_walTypeForPrint[static_cast<uint16>(recordForPage->m_type)]));
            }
            bufMgr->UnlockAndRelease(bufferDesc);
            bufferDesc = INVALID_BUFFER_DESC;
            continue;
        } else if (replayType == WalRecordReplayType::NO_NEED_TO_REPLAY) {
            break;
        }

        /* step 3 check page lsn */
        Page *page = static_cast<Page *>(bufferDesc->GetPage());
        bool reportWarning = (loopCount % WAIT_COUNT_FOR_REPORT_WARNING == 0);
        replayType =
            GetWalRecordReplayType(bufferDesc->GetBufferTag(), page, preRecordLsnInfo, recordLsnInfo, reportWarning);
        if (recordForPage->m_type == WAL_TBS_INIT_ONE_BITMAP_PAGE ||
            recordForPage->m_type == WAL_TBS_INIT_ONE_DATA_PAGE) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                   ErrMsg("[PDB:%u WAL:%lu] Worker %u GetWalRecordReplayType for wal type:%s, whose pre LsnInfo is "
                          "(%lu,%lu,%lu), page (%d, %u) LsnInfo is (%lu,%lu,%lu), record LsnInfo (%lu,%lu,%lu) "
                          "replayType = %u", m_pdbId, redoCtx->walId, buffPara->workerId,
                          g_walTypeForPrint[static_cast<uint16>(recordForPage->m_type)], preRecordLsnInfo.walId,
                          preRecordLsnInfo.endPlsn, preRecordLsnInfo.glsn, pageId.m_fileId, pageId.m_blockId,
                          page->GetWalId(), page->GetPlsn(), page->GetGlsn(), recordLsnInfo.walId,
                          recordLsnInfo.endPlsn, recordLsnInfo.glsn, static_cast<uint16>(replayType)));
        }
        if (replayType == WalRecordReplayType::NOT_REPLAYABLE) {
            bufMgr->UnlockAndRelease(bufferDesc);
            bufferDesc = INVALID_BUFFER_DESC;
            GaussUsleep(WAIT_WAL_REPLAYABLE_SLEEP_MILLISECONDS);
        } else {
            return replayType;
        }
    } while (true);
    return replayType;
}

RetStatus WalRecovery::RedoWalRecord(WalRecordRedoContext *redoCtx, const WalRecord *walRecord,
    RedoWalRecordBuffPara *buffPara)
{
    LatencyStat::Timer timer(&WalPerfUnit::GetInstance().m_walRedoSingleWalRecord);
    WalType walType = walRecord->m_type;
    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
           ErrMsg("Wal record type:%hu, endPlsn:%lu", static_cast<uint16>(walType), redoCtx->recordEndPlsn));
#ifdef UT
    if (walType == WAL_EMPTY_REDO) {
        return DSTORE_SUCC;
    }
#endif
    if (unlikely(walType >= WAL_TYPE_BUTTOM)) {
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Invalid Wal type:%hu.", static_cast<uint16>(walType)));
        return DSTORE_FAIL;
    }

    std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> start;
    if (unlikely((static_cast<uint64>(g_traceSwitch) & WAL_TRACE_SWITCH) ||
                 m_statWalTypeInfo.load(std::memory_order_acquire))) {
        start = std::chrono::high_resolution_clock::now();
    }

    const WalRecordForPage *recordForPage = static_cast<const WalRecordForPage *>(walRecord);
    if (IsSupportCompress(walRecord->GetType())) {
        recordForPage = DecompressProc(walRecord, buffPara->buffForDecompress);
    }
    if (STORAGE_VAR_NULL(recordForPage)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("RedoWalRecord failed to record for page."));
        return DSTORE_FAIL;
    }
    WalRecordReplayType replayType = WaitAvailableBufferForRedo(buffPara, redoCtx, recordForPage);
    if (replayType == WalRecordReplayType::NO_NEED_TO_REPLAY) {
        /**
         * When the read authorization is requested to obtain a page, we don't record whether the page is a dirty page.
         * Therefore, during fault recovery, the page after RecoveryRead may be a dirty page,
         * but we cannot distinguish it. so we simply set the page as a dirty page to guarantee
         * it will be flushed back even if the wal log does not need to be replayed.
         */
        buffPara->needMarkDirty = true;
        if (walType == WAL_TBS_EXTEND_FILE) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                   ErrMsg("[PDB:%u WAL:%lu] extend file no need to replay, fileId %u, blockcount %lu, endplsn = %lu",
                          m_pdbId, m_walId, static_cast<const WalRecordTbsExtendFile *>(walRecord)->fileId,
                          static_cast<const WalRecordTbsExtendFile *>(walRecord)->totalBlockCount,
                          redoCtx->recordEndPlsn));
        }
        return DSTORE_SUCC;
    }

    BufferDesc *bufferDesc = *(buffPara->bufferDesc);
    if (RedoWalRecordByType(redoCtx, recordForPage, bufferDesc, walType) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }
    if (m_redoMode == RedoMode::PDB_STANDBY_REDO) {
        UpdateTrxNeedRollbackBarrier(redoCtx, recordForPage);
    }
    buffPara->needMarkDirty = true;
    BufferTag bufTag = bufferDesc->GetBufferTag();
    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
           ErrMsg("End of redo BufTag:%s, glsn = %lu. plsn = %lu.", bufTag.ToString().CString(),
                  bufferDesc->GetPage()->GetGlsn(), redoCtx->recordEndPlsn));

    if (unlikely(static_cast<uint64>(g_traceSwitch) & WAL_TRACE_SWITCH)) {
        std::chrono::duration<double> spendSecs = std::chrono::high_resolution_clock::now() - start;
        AddRedoStatisticInfo(spendSecs.count(), walType, redoCtx->recordEndPlsn, buffPara->workerId, walRecord);
    }
    if (unlikely(m_statWalTypeInfo.load(std::memory_order_acquire))) {
        std::chrono::duration<double> spendSecs = std::chrono::high_resolution_clock::now() - start;
        m_walTypeRedoStatisticInfo.walTypeRedoTime[walType] += spendSecs.count();
        m_walTypeRedoStatisticInfo.walTypeRedoCnt[walType]++;
    }
    return DSTORE_SUCC;
}

void WalRecovery::UpdateTrxNeedRollbackBarrier(const WalRecordRedoContext *redoCtx, const WalRecordForPage *walRecord)
{
    AutoMemCxtSwitch autoMemCxtSwitch(m_memoryContext);
    PdbId pdbId = m_pdbId == 0 ? redoCtx->pdbId : m_pdbId;
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (pdb == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("pdb or walMgr is nullptr."));
        return;
    }
    if (walRecord->m_type == WAL_UNDO_ALLOCATE_TXN_SLOT) {
        TransactionsNeedRollbackListNode *node =
            static_cast<TransactionsNeedRollbackListNode *>(DstorePalloc0(sizeof(TransactionsNeedRollbackListNode)));
        if (unlikely(node == nullptr)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Malloc for barrier TransactionsNeedRollbackListNode fail."));
            return;
        }
        node->info.csn = MAX_COMMITSEQNO;
        node->info.xid = redoCtx->xid;
        pdb->PushTransactionIntoBarrierRollbackList(node);
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
            ErrMsg("UpdateTrxNeedRollbackBarrier add xid xlog to queue, xid(%d, %lu).",
            static_cast<int32>(redoCtx->xid.m_zoneId), redoCtx->xid.m_logicSlotId));
    } else if (walRecord->m_type == WAL_TXN_COMMIT) {
        const WalRecordTransactionCommit *walRecordTransactionCommit =
            static_cast<const WalRecordTransactionCommit *>(walRecord);
        if (walRecordTransactionCommit->GetTrxSlotStatus() == TXN_STATUS_COMMITTED) {
            pdb->UpdateCsnInBarrierRollbackListByXid(redoCtx->xid, walRecordTransactionCommit->GetCsn());
            ErrLog(DSTORE_DEBUG1, MODULE_WAL,
                ErrMsg("UpdateTrxNeedRollbackBarrier update csn(%lu) which xid(%d, %lu) in commit.",
                walRecordTransactionCommit->GetCsn(), static_cast<int32>(redoCtx->xid.m_zoneId),
                redoCtx->xid.m_logicSlotId));
        }
    } else if (walRecord->m_type == WAL_TXN_ABORT) {
        const WalRecordTransactionAbort *walRecordTransactionAbort =
            static_cast<const WalRecordTransactionAbort *>(walRecord);
        pdb->UpdateCsnInBarrierRollbackListByXid(redoCtx->xid, walRecordTransactionAbort->GetCsn());
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
            ErrMsg("UpdateTrxNeedRollbackBarrier update csn(%lu) which xid(%d, %lu) in commit abort.",
            walRecordTransactionAbort->GetCsn(), static_cast<int32>(redoCtx->xid.m_zoneId),
            redoCtx->xid.m_logicSlotId));
    }
}

void WalRecovery::SetBarrier(WalBarrier barrier)
{
    m_barrierCsn.store(barrier.barrierCsn, std::memory_order_release);
    m_curBarrierEndPlsn.store(barrier.barrierEndPlsn, std::memory_order_release);
    m_curSyncMode.store(barrier.barrierSyncMode, std::memory_order_release);
}

WalRecordReplayType WalRecovery::CheckOrWaitWalRecordVersionMatch(const PdbId pdbId, const WalRecord *walRecord,
                                                                  const uint32 workerId, bool withLoop,
                                                                  WalRecordRedoContext *redoCtx)
{
    WalType walType = walRecord->GetType();
    uint64 loopCount = 0;
    WalRecordReplayType walRecordReplayType;
    if (IsWalRecordWithoutFileInfo(walType)) {
        if (walType == WAL_SYSTABLE_WRITE_BUILTIN_RELMAP) {
            walRecordReplayType = WalRecordReplayType::REPLAYABLE;
        } else {
            /* wait for tablespace version */
            const WalRecordTbsLogical *walrecordTbsLogical = static_cast<const WalRecordTbsLogical *>(walRecord);
            TablespaceId tablespaceId = walrecordTbsLogical->GetTablespaceId();
            uint64 preReuseVersion = walrecordTbsLogical->GetPreReuseVersion();
            uint64 versionInFile;
            walRecordReplayType = WalRecordCheckTbsVersion(pdbId, tablespaceId, preReuseVersion, &versionInFile);
            while (withLoop && walRecordReplayType == WalRecordReplayType::NOT_REPLAYABLE) {
                GaussUsleep(SLEEP_MILLISECONDS);
                walRecordReplayType = WalRecordCheckTbsVersion(pdbId, tablespaceId, preReuseVersion, &versionInFile);
                if (++loopCount % WAIT_COUNT_FOR_REPORT_WARNING == 0) {
                    ErrLog(DSTORE_LOG, MODULE_WAL,
                           ErrMsg("[PDB:%u WAL:%lu] Worker %u wait tbs record replayable, tbsId: %u, preReuseVersion:%lu, "
                                  "versionInFile:%lu, lsn: %lu, type: %s.",
                                  pdbId, redoCtx->walId, workerId, tablespaceId, preReuseVersion, versionInFile,
                                  redoCtx->recordEndPlsn, g_walTypeForPrint[static_cast<uint16>(walType)]));
                }
            }
        }
    } else {
        /* wait for file version */
        FileId fileId;
        uint64 filePreVersion;
        switch (walType) {
            case WAL_TBS_CREATE_DATA_FILE: {
                const WalRecordTbsCreateDataFile *walRecordTbsCreateDataFile =
                    static_cast<const WalRecordTbsCreateDataFile *>(walRecord);
                fileId = walRecordTbsCreateDataFile->GetFileId();
                filePreVersion = walRecordTbsCreateDataFile->GetPreReuseVersion();
                break;
            }
            case WAL_TBS_DROP_DATA_FILE: {
                const WalRecordTbsDropDataFile *walRecordTbsDropDataFile =
                    static_cast<const WalRecordTbsDropDataFile *>(walRecord);
                fileId = walRecordTbsDropDataFile->GetFileId();
                filePreVersion = walRecordTbsDropDataFile->GetPreReuseVersion();
                break;
            }
            case WAL_TBS_INIT_MULTIPLE_DATA_PAGES: {
                const WalRecordTbsInitDataPages *walRecordTbsInitDataPages =
                    static_cast<const WalRecordTbsInitDataPages *>(walRecord);
                fileId = walRecordTbsInitDataPages->GetPageId().m_fileId;
                filePreVersion = walRecordTbsInitDataPages->GetFilePreVersion();
                break;
            }
            case WAL_TBS_INIT_BITMAP_PAGES: {
                const WalRecordTbsInitBitmapPages *walRecordTbsInitBitmapPages =
                    static_cast<const WalRecordTbsInitBitmapPages *>(walRecord);
                fileId = walRecordTbsInitBitmapPages->GetPageId().m_fileId;
                filePreVersion = walRecordTbsInitBitmapPages->GetFilePreVersion();
                break;
            }
            default: {
                const WalRecordForPageOnDisk *recordForPageOnDisk =
                    static_cast<const WalRecordForPageOnDisk *>(walRecord);
                fileId = recordForPageOnDisk->GetCompressedPageId().m_fileId;
                filePreVersion = recordForPageOnDisk->GetCompressedFilePreVersion();
                break;
            }
        }
        uint64 versionInFile;
        walRecordReplayType = WalRecovery::WalRecordCheckFileVersion(pdbId, fileId, filePreVersion, &versionInFile);
        if (walRecordReplayType == WalRecordReplayType::NO_NEED_TO_REPLAY) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                   ErrMsg("[PDB:%u WAL:%lu] Worker %u skip redo, fileId %u, "
                          "filePreVersion:%lu, versionInFile:%lu, lsn: %lu, type: %s.",
                          pdbId, redoCtx->walId, workerId, fileId, filePreVersion, versionInFile,
                          redoCtx->recordEndPlsn, g_walTypeForPrint[static_cast<uint16>(walType)]));
        }
        while (withLoop && walRecordReplayType == WalRecordReplayType::NOT_REPLAYABLE) {
            GaussUsleep(SLEEP_MILLISECONDS);
            walRecordReplayType = WalRecovery::WalRecordCheckFileVersion(pdbId, fileId, filePreVersion, &versionInFile);
            if (++loopCount % WAIT_COUNT_FOR_REPORT_WARNING == 0) {
                ErrLog(DSTORE_LOG, MODULE_WAL,
                       ErrMsg("[PDB:%u WAL:%lu] Worker %u wait WalRecordForPage replayable, fileId %u, "
                              "filePreVersion:%lu, versionInFile:%lu, lsn: %lu, type: %s.",
                              pdbId, redoCtx->walId, workerId, fileId, filePreVersion, versionInFile,
                              redoCtx->recordEndPlsn, g_walTypeForPrint[static_cast<uint16>(walType)]));
            }
        }
    }
    return walRecordReplayType;
}

RetStatus WalRecovery::RedoSingleForTbs(WalRecordRedoContext *redoCtx, const WalRecord *walRecord, uint32 workerId)
{
    WalType walType = walRecord->m_type;
    if (g_storageInstance->IsInBackupRestore(m_pdbId) && IsWalRecordWithoutPageInfo(walType)) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("[DDLREDO] walId: %lu, pdbId: %u, lsn: %lu, DDL type: %s, tablespaceId: %u, fileId: %u, "
                      "replayType: REPLAYABLE, in BackupRestore",
                      redoCtx->walId, redoCtx->pdbId, redoCtx->recordEndPlsn,
                      g_walTypeForPrint[static_cast<uint16>(walType)],
                      static_cast<const WalRecordTbsLogical *>(walRecord)->GetTablespaceId(),
                      WalRecovery::GetFileIdFromTbsRecord((walRecord))));
        WalRecordTbs::RedoTbsRecord(redoCtx, static_cast<const WalRecordTbs *>(walRecord), nullptr);
        if (walRecord->m_type == WAL_TBS_DROP_DATA_FILE) {
            MarkDroppedFile(GetFileIdFromTbsRecord(walRecord));
        }
        return DSTORE_SUCC;
    }

    if (CheckOrWaitWalRecordVersionMatch(m_pdbId, walRecord, workerId, true, redoCtx) ==
        WalRecordReplayType::NO_NEED_TO_REPLAY) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("[DDLREDO] walId: %lu, pdbId: %u, lsn: %lu, DDL type: %s, tablespaceId: %u, fileId: %u, "
                      "replayType: NO_NEED_TO_REPLAY",
                      redoCtx->walId, redoCtx->pdbId, redoCtx->recordEndPlsn,
                      g_walTypeForPrint[static_cast<uint16>(walType)],
                      static_cast<const WalRecordTbsLogical *>(walRecord)->GetTablespaceId(),
                      WalRecovery::GetFileIdFromTbsRecord((walRecord))));
        if (walRecord->m_type == WAL_TBS_DROP_DATA_FILE) {
            MarkDroppedFile(GetFileIdFromTbsRecord(walRecord));
        }
        return DSTORE_SUCC;
    }

    ErrLog(
        DSTORE_LOG, MODULE_WAL,
        ErrMsg("[DDLREDO] walId: %lu, pdbId: %u, lsn: %lu, DDL type: %s, tablespaceId: %u, fileId: %u, replayType: "
               "REPLAYABLE",
               redoCtx->walId, redoCtx->pdbId, redoCtx->recordEndPlsn, g_walTypeForPrint[static_cast<uint16>(walType)],
               static_cast<const WalRecordTbsLogical *>(walRecord)->GetTablespaceId(),
               WalRecovery::GetFileIdFromTbsRecord((walRecord))));
    std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> start;
    if (unlikely((static_cast<uint64>(g_traceSwitch) & WAL_TRACE_SWITCH) ||
                 m_statWalTypeInfo.load(std::memory_order_acquire))) {
        start = std::chrono::high_resolution_clock::now();
    }
    WalRecordTbs::RedoTbsRecord(redoCtx, static_cast<const WalRecordTbs *>(walRecord), nullptr);
    if (unlikely(m_statWalTypeInfo.load(std::memory_order_acquire))) {
        std::chrono::duration<double> spendSecs = std::chrono::high_resolution_clock::now() - start;
        m_walTypeRedoStatisticInfo.walTypeRedoTime[walRecord->m_type] += spendSecs.count();
        m_walTypeRedoStatisticInfo.walTypeRedoCnt[walRecord->m_type]++;
    }

    if (walRecord->m_type == WAL_TBS_DROP_DATA_FILE) {
        MarkDroppedFile(GetFileIdFromTbsRecord(walRecord));
    }

    if (unlikely(static_cast<uint64>(g_traceSwitch) & WAL_TRACE_SWITCH)) {
        std::chrono::duration<double> spendSecs = std::chrono::high_resolution_clock::now() - start;
        AddRedoStatisticInfo(spendSecs.count(), walType, redoCtx->recordEndPlsn, workerId, walRecord);
    }
    if (IsWalRecordWithoutFileInfo(walType)) {
        const WalRecordTbsLogical *tbsRecord = static_cast<const WalRecordTbsLogical *>(walRecord);
        LockInterface::TablespaceLockContext context;
        context.pdbId = m_pdbId;
        context.tablespaceId = tbsRecord->GetTablespaceId();
        context.dontWait = true;
        context.mode = DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK;
        LockInterface::UnlockTablespace(&context);
    }
    return DSTORE_SUCC;
}

RetStatus WalRecovery::RedoSingle(WalRecordRedoContext *redoCtx,
    const WalRecord *walRecord, uint32 workerId, BuffForDecompress *buffForDecompress)
{
    WalType walType = walRecord->m_type;
    if (!IsRedoableWalRecord(walType)) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
               ErrMsg("WalType:%hu not support redo and jump over.", static_cast<uint16>(walType)));
        return DSTORE_SUCC;
    }

    if (IsWalRecordWithMultiPagesInfo(walType) || IsWalRecordWithoutPageInfo(walType)) {
        if (walType == WAL_SYSTABLE_WRITE_BUILTIN_RELMAP) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("[DDLREDO] walId: %lu, pdbId: %u, lsn: %lu, DDL type: %s.",
                    redoCtx->walId, redoCtx->pdbId, redoCtx->recordEndPlsn,
                    g_walTypeForPrint[static_cast<uint16>(walRecord->m_type)]));
            WalRecordSystable::RedoSystableRecord(static_cast<const WalRecordSystable *>(walRecord), redoCtx->pdbId);
        } else {
            RedoSingleForTbs(redoCtx, walRecord, workerId);
        }
        return DSTORE_SUCC;
    }

    if (CheckOrWaitWalRecordVersionMatch(m_pdbId, walRecord, workerId, true, redoCtx) ==
        WalRecordReplayType::NO_NEED_TO_REPLAY) {
        if (walType == WAL_TBS_EXTEND_FILE) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                   ErrMsg("[PDB:%u WAL:%lu] extend file no need to replay because of file version, fileId %u, "
                          "blockcount %lu, endplsn = %lu",
                          m_pdbId, m_walId, static_cast<const WalRecordTbsExtendFile *>(walRecord)->fileId,
                          static_cast<const WalRecordTbsExtendFile *>(walRecord)->totalBlockCount,
                          redoCtx->recordEndPlsn));
        }
        return DSTORE_SUCC;
    }

    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    if (unlikely(pdb == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalRecovery get pdb %u failed.", m_pdbId));
        return DSTORE_FAIL;
    }
    if (pdb->GetPdbRoleMode() == PdbRoleMode::PDB_STANDBY && walRecord->GetType() == WAL_BARRIER_CSN) {
        ProcessBarrierRecord(static_cast<const WalBarrierCsn*>(walRecord), redoCtx->recordEndPlsn);
        return DSTORE_SUCC;
    }
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    const WalRecordForPageOnDisk *recordForPageOnDisk = static_cast<const WalRecordForPageOnDisk*>(walRecord);
    PageId pageId = recordForPageOnDisk->GetCompressedPageId();
    ParaForReadBuffer paraForReadBuffer{pageId, recordForPageOnDisk->GetCompressedFilePreVersion(), redoCtx->walId,
                                        redoCtx->recordEndPlsn};
    BufferDesc *bufferDesc = RecoveryReadBuffer(paraForReadBuffer);
    if (bufferDesc == INVALID_BUFFER_DESC) {
        /* INVALID_BUFFER_DESC means file version is changed, no need to replay */
        if (walType == WAL_TBS_EXTEND_FILE) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                   ErrMsg("[PDB:%u WAL:%lu] extend file no need to replay because of file version, fileId %u, "
                          "blockcount %lu, endplsn = %lu",
                          m_pdbId, m_walId, static_cast<const WalRecordTbsExtendFile *>(walRecord)->fileId,
                          static_cast<const WalRecordTbsExtendFile *>(walRecord)->totalBlockCount,
                          redoCtx->recordEndPlsn));
        }
        return DSTORE_SUCC;
    }

    RedoWalRecordBuffPara buffPara = {&bufferDesc, 0, true, false, buffForDecompress};
    RetStatus ret = RedoWalRecord(redoCtx, walRecord, &buffPara);
    if (buffPara.needMarkDirty) {
        (void)bufMgr->MarkDirty(bufferDesc);
    }
    bufMgr->UnlockAndRelease(bufferDesc);
    return ret;
}

RetStatus WalRecovery::RedoBatchForPage(WalBatchRecoveryPara *para, uint32 workerId,
                                        BuffForDecompress *buffForDecompress)
{
    RetStatus ret = DSTORE_SUCC;
    WalRecordReplayType walRecordReplayType = WalRecovery::CheckOrWaitWalRecordVersionMatch(
        m_pdbId, para->recordEntry[0].GetWalRecord(), workerId, true, &para->recordEntry->ctx);
    if (walRecordReplayType == WalRecordReplayType::NO_NEED_TO_REPLAY) {
        para->redoPos += para->batchNum;
        return ret;
    }

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    ParaForReadBuffer paraForReadBuffer{para->pageId, para->prevFileVersion, para->recordEntry->ctx.walId,
                                        para->recordEntry->ctx.recordEndPlsn};
    BufferDesc *bufferDesc = RecoveryReadBuffer(paraForReadBuffer);
    if (bufferDesc == INVALID_BUFFER_DESC) {
        para->redoPos += para->batchNum;
        return ret;
    }

    RedoWalRecordBuffPara buffPara = {&bufferDesc, workerId, false, false, buffForDecompress};
    for (uint32 i = 0; i < para->batchNum; i++) {
        RedoWalRecordEntry *recordEntry = &para->recordEntry[i];
        ret = RedoWalRecord(&recordEntry->ctx, recordEntry->GetWalRecord(), &buffPara);
        if (STORAGE_FUNC_FAIL(ret)) {
            break;
        }
        para->redoPos++;
    }
    if (bufferDesc == INVALID_BUFFER_DESC) {
        return ret;
    }

    if (buffPara.needMarkDirty) {
        (void)bufMgr->MarkDirty(bufferDesc);
    }
    bufMgr->UnlockAndRelease(bufferDesc);
    return ret;
}

void WalRecovery::ProcessBarrierRecord(const WalBarrierCsn *record, const uint64 recordEndPlsn, bool needReport)
{
    CommitSeqNo barrierCsn = m_barrierCsn.load();
    if (barrierCsn < record->GetBarrierCsn()) {
        m_barrierCsn.compare_exchange_strong(barrierCsn, record->GetBarrierCsn());
        m_curBarrierEndPlsn.store(recordEndPlsn, std::memory_order_release);
        uint32 clusterCount = record->GetPdbCount();
        uint32 clusterId = static_cast<uint32>(g_storageInstance->GetGuc()->clusterId);
        for (uint32 i = 0; i < clusterCount; i++) {
            if (record->syncModeArray[i].standbyClusterId == clusterId) {
                m_curSyncMode.store(record->syncModeArray[i].syncMode, std::memory_order_release);
                break;
            }
        }
        ErrLevel errLevel = needReport ? DSTORE_LOG : DSTORE_DEBUG1;
        ErrLog(errLevel, MODULE_WAL,
               ErrMsg("[PDB:%u WAL:%lu] ProcessBarrierRecord update barrier csn(%lu), endplsn(%lu), syncmode(%d).",
                      m_pdbId, m_walId, barrierCsn, recordEndPlsn,
                      static_cast<int32>(m_curSyncMode.load(std::memory_order_acquire))));
    }
}

bool WalRecovery::IsAllNullRecord(RedoWalRecordEntry *recordEntry, uint32 getNum)
{
    if (recordEntry == nullptr) {
        return true;
    }
    for (uint32 i = 0; i < getNum; i++) {
        if (recordEntry[i].GetWalRecord() != nullptr) {
            return false;
        }
    }
    return true;
}

bool WalRecovery::IsDispatchFinish()
{
    if (m_dispatchQueue == nullptr) {
        return true;
    }

    RedoWalRecordEntry *recordEntry = nullptr;
    uint32 getNum = 0;
    RetStatus ret = m_dispatchQueue->GetAll<RedoWalRecordEntry*>(&recordEntry, &getNum);
    if (ret == DSTORE_FAIL || getNum == 0 || IsAllNullRecord(recordEntry, getNum)) {
        return true;
    }
    return false;
}

bool WalRecovery::IsAllRedoQueueEmpty()
{
    bool res = true;
    for (uint32 i = 0; i < m_redoWorkerNum; i++) {
        if (m_parallelRedoWorkers[i] == nullptr) {
            continue;
        }
        res = res && m_parallelRedoWorkers[i]->IsRedoQueueEmptyOrOnlyNullRecord();
    }
    return res;
}

void WalRecovery::MarkDroppedFile(FileId fileId)
{
    m_droppedFileArray[fileId] = true;
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("[DDLREDO] walId: %lu, pdbId: %u, mark fileId: %u dropped", m_walId, m_pdbId, fileId));
}

RetStatus WalRecovery::RedoBatchForDdl(WalBatchRecoveryPara *para, bool printDdlLog)
{
    RetStatus ret = DSTORE_SUCC;
    for (uint32 i = para->redoPos; i < para->batchNum; i++) {
        RedoWalRecordEntry *recordEntry = &para->recordEntry[i];
        const WalRecord *record = recordEntry->GetWalRecord();
        if (!IsWalRecordWithoutPageInfo(record->m_type)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL,
                   ErrMsg("RedoBatch get invalid logical Wal record type:%hu.", static_cast<uint16>(record->m_type)));
            ret = DSTORE_FAIL;
            break;
        }
        if (g_storageInstance->IsInBackupRestore(m_pdbId)) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                   ErrMsg("[DDLREDO] walId: %lu, pdbId: %u, lsn: %lu, DDL type: %s, tablespaceId: %u, fileId: %u, "
                          "replayType: REPLAYABLE, in BackupRestore",
                          recordEntry->ctx.walId, recordEntry->ctx.pdbId, recordEntry->recordEndPlsn,
                          g_walTypeForPrint[static_cast<uint16>(record->m_type)],
                          static_cast<const WalRecordTbsLogical *>(record)->GetTablespaceId(),
                          WalRecovery::GetFileIdFromTbsRecord((record))));
            if (record->m_type == WAL_SYSTABLE_WRITE_BUILTIN_RELMAP) {
                WalRecordSystable::RedoSystableRecord(static_cast<const WalRecordSystable *>(record),
                    recordEntry->ctx.pdbId);
            } else {
                WalRecordTbs::RedoTbsRecord(&recordEntry->ctx, record, nullptr);
            }
            if (record->m_type == WAL_TBS_DROP_DATA_FILE) {
                MarkDroppedFile(GetFileIdFromTbsRecord(record));
            }
            para->redoPos++;
            para->waitStartPlsn = recordEntry->recordEndPlsn;
            continue;
        }
        WalRecordReplayType walRecordReplayType =
            CheckOrWaitWalRecordVersionMatch(m_pdbId, record, 0,
                m_redoMode == RedoMode::PDB_STANDBY_REDO ? true : false, &recordEntry->ctx);
        if (printDdlLog || walRecordReplayType != WalRecordReplayType::NOT_REPLAYABLE) {
            if (record->m_type == WAL_SYSTABLE_WRITE_BUILTIN_RELMAP) {
                ErrLog(DSTORE_LOG, MODULE_WAL,
                    ErrMsg("[DDLREDO] walId: %lu, pdbId: %u, lsn: %lu, DDL type: %s, replayType: %hu",
                        recordEntry->ctx.walId, recordEntry->ctx.pdbId, recordEntry->recordEndPlsn,
                        g_walTypeForPrint[static_cast<uint16>(record->m_type)],
                        static_cast<uint16>(walRecordReplayType)));
            } else {
                ErrLog(DSTORE_LOG, MODULE_WAL,
                    ErrMsg("[DDLREDO] walId: %lu, pdbId: %u, lsn: %lu, DDL type: %s, tablespaceId: %u, fileId: %u, "
                        "replayType: %hu",
                        recordEntry->ctx.walId, recordEntry->ctx.pdbId, recordEntry->recordEndPlsn,
                        g_walTypeForPrint[static_cast<uint16>(record->m_type)],
                        static_cast<const WalRecordTbsLogical *>(record)->GetTablespaceId(),
                        WalRecovery::GetFileIdFromTbsRecord((record)), static_cast<uint16>(walRecordReplayType)));
            }
        }
        if (walRecordReplayType == WalRecordReplayType::REPLAYABLE) {
            std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> start;
            if (unlikely(m_statWalTypeInfo.load(std::memory_order_acquire))) {
                start = std::chrono::high_resolution_clock::now();
            }
            if (record->m_type == WAL_SYSTABLE_WRITE_BUILTIN_RELMAP) {
                WalRecordSystable::RedoSystableRecord(static_cast<const WalRecordSystable *>(record),
                    recordEntry->ctx.pdbId);
            } else {
                WalRecordTbs::RedoTbsRecord(&recordEntry->ctx, record, nullptr);
            }
            if (unlikely(m_statWalTypeInfo.load(std::memory_order_acquire))) {
                std::chrono::duration<double> spendSecs = std::chrono::high_resolution_clock::now() - start;
                m_walTypeRedoStatisticInfo.walTypeRedoTime[record->m_type] += spendSecs.count();
                m_walTypeRedoStatisticInfo.walTypeRedoCnt[record->m_type]++;
            }
            if (record->m_type == WAL_TBS_DROP_DATA_FILE) {
                MarkDroppedFile(GetFileIdFromTbsRecord(record));
            }
            para->redoPos++;
            para->waitStartPlsn = recordEntry->recordEndPlsn;
            if (record->m_type != WAL_SYSTABLE_WRITE_BUILTIN_RELMAP && IsWalRecordWithoutFileInfo(record->m_type)) {
                const WalRecordTbsLogical *tbsRecord = static_cast<const WalRecordTbsLogical *>(record);
                LockInterface::TablespaceLockContext context;
                context.pdbId = m_pdbId;
                context.tablespaceId = tbsRecord->GetTablespaceId();
                context.dontWait = true;
                context.mode = DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK;
                LockInterface::UnlockTablespace(&context);
            }
        } else if (walRecordReplayType == WalRecordReplayType::NO_NEED_TO_REPLAY) {
            if (record->m_type == WAL_TBS_DROP_DATA_FILE) {
                MarkDroppedFile(GetFileIdFromTbsRecord(record));
            }
            para->redoPos++;
            para->waitStartPlsn = recordEntry->recordEndPlsn;
        } else if (walRecordReplayType == WalRecordReplayType::NOT_REPLAYABLE) {
            break;
        }
    }
    return ret;
}

RetStatus WalRecovery::RedoBatch(WalBatchRecoveryPara *para, uint32 workerId, BuffForDecompress *buffForDecompress,
                                 bool printDdlLog)
{
    if (para->pageId.IsValid()) {
        return RedoBatchForPage(para, workerId, buffForDecompress);
    } else {
        return RedoBatchForDdl(para, printDdlLog);
    }
}

inline void WalRecovery::SplitMultipleDataPagesWalRecordIntoDirtyPageSet(const WalRecord *walRecord,
                                                                         WalRecordInfoListNode *walRecordInfoListNode)
{
    const WalRecordTbsInitDataPages *tbsRecord =
        static_cast<const WalRecordTbsInitDataPages *>(static_cast<const void *>(walRecord));

    FileId fileId = tbsRecord->firstDataPageId.m_fileId;
    BlockNumber firstBlockId = tbsRecord->firstDataPageId.m_blockId;
    for (uint16 i = 0; i < tbsRecord->dataPageCount; ++i) {
        PageId curDataPageId = {fileId, firstBlockId + i};
        uint64 entryGlsn = tbsRecord->preWalPointer[i].walId != m_walId ? tbsRecord->preWalPointer[i].glsn + 1
                                                                        : tbsRecord->preWalPointer[i].glsn;
        walRecordInfoListNode->glsn = entryGlsn;
        walRecordInfoListNode->fileVersion = tbsRecord->filePreVersion;
        BatchPutDirtyPageEntry(curDataPageId, walRecordInfoListNode);
    }
}

inline void WalRecovery::SplitInitBitmapPagesWalRecordIntoDirtyPageSet(const WalRecord *walRecord,
                                                                       WalRecordInfoListNode *walRecordInfoListNode)
{
    const WalRecordTbsInitBitmapPages *tbsRecord =
        static_cast<const WalRecordTbsInitBitmapPages *>(static_cast<const void *>(walRecord));
    PageId curPageId = tbsRecord->firstPageId;
    for (uint16 i = 0; i < tbsRecord->pageCount; i++) {
        uint64 entryGlsn = tbsRecord->preWalPointer[i].walId != m_walId ? tbsRecord->preWalPointer[i].glsn + 1
                                                                        : tbsRecord->preWalPointer[i].glsn;
        walRecordInfoListNode->glsn = entryGlsn;
        walRecordInfoListNode->fileVersion = tbsRecord->filePreVersion;
        BatchPutDirtyPageEntry(curPageId, walRecordInfoListNode);
        curPageId.m_blockId++;
    }
}

const WalRecordForPage* WalRecovery::DecompressForBuildDirtyPageSet(WalRecordForPage *decompressedRecordForPage,
    const WalRecord *walRecord) const
{
    if (IsSupportCompress(walRecord->GetType())) {
        (void)(decompressedRecordForPage->Decompress(walRecord));
        /* dirtyPageSet just need field of WalRecordForPage */
        return static_cast<const WalRecordForPage *>(decompressedRecordForPage);
    } else {
        return static_cast<const WalRecordForPage *>(walRecord);
    }
}

RetStatus WalRecovery::FlushAllDirtyPages()
{
    if (m_dirtyPageEntryArraySize == 0) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery flush all dirty pages success for no dirty pages.", m_pdbId, m_walId));
        SetWalRecoveryStage(WalRecoveryStage::RECOVERY_DIRTY_PAGE_FLUSHED);
        return DSTORE_SUCC;
    }

    if (m_pdbId == INVALID_PDB_ID) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery flush all dirty pages failed for invalid pdbId", m_pdbId, m_walId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery try flush %ld dirty pages",
        m_pdbId, m_walId, m_dirtyPageEntryArraySize));
    if (m_dirtyPageEntryArraySize < 0 || m_dirtyPageEntryArray == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery flush all dirty pages failed for invalid entryArray",
            m_pdbId, m_walId));
        return DSTORE_FAIL;
    }

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    if (bufMgr == nullptr) {
        ErrLog(DSTORE_WARNING,
            MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery flush all dirty pages failed for invalid bufMgr.",
            m_pdbId, m_walId));
        return DSTORE_FAIL;
    }

    RetStatus ret = DSTORE_SUCC;
    BatchBufferAioContextMgr *batchCtxMgr = nullptr;
    if (m_useAio) {
        batchCtxMgr = DstoreNew(m_memoryContext) BatchBufferAioContextMgr();
        StorageReleasePanic(batchCtxMgr == nullptr, MODULE_WAL, ErrMsg("alloc batchCtxMgr fail"));
        ret = batchCtxMgr->InitBatch(false, bufMgr);
        StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_WAL, ErrMsg("init batchCtxMgr fail"));
    }

    for (int index = 0; index < m_dirtyPageEntryArraySize; index++) {
        PageId pageId = m_dirtyPageEntryArray[index].pageId;
        BufferTag bufTag{m_pdbId, pageId};
        BufferDesc *tmpBufferDesc = bufMgr->RecoveryRead(m_pdbId, pageId);
        if (tmpBufferDesc == INVALID_BUFFER_DESC) {
            if (m_droppedFileArray[pageId.m_fileId]) {
                ErrLog(DSTORE_WARNING, MODULE_WAL,
                       ErrMsg("[PDB:%u WAL:%lu]WalRecovery skip flush dirty page %u, %u, the file has been deleted.",
                              m_pdbId, m_walId, pageId.m_fileId, pageId.m_blockId));
            } else {
                ErrLog(DSTORE_WARNING, MODULE_WAL,
                       ErrMsg("[PDB:%u WAL:%lu]WalRecovery skip flush dirty page %u, %u, the file may be deleted.",
                              m_pdbId, m_walId, pageId.m_fileId, pageId.m_blockId));
            }
            continue;
        }
        bufMgr->UnlockAndRelease(tmpBufferDesc);
        if (m_useAio) {
            ret = batchCtxMgr->AsyncFlushPage(bufTag);
        } else {
            ret = bufMgr->Flush(bufTag);
        }
        StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_WAL, ErrMsg("Flush failed."));
    }

    if (m_useAio) {
        batchCtxMgr->FsyncBatch();
        batchCtxMgr->DestoryBatch();
        delete batchCtxMgr;
    }

    SetWalRecoveryStage(WalRecoveryStage::RECOVERY_DIRTY_PAGE_FLUSHED);
    return DSTORE_SUCC;
}

inline void WalRecovery::BatchPutWalRecordInfo(WalRecordInfo *walRecordInfos, uint32 &batchPutNum,
    const WalRecordInfo &walRecordInfo, bool forcePutAll)
{
    walRecordInfos[batchPutNum] = walRecordInfo;
    batchPutNum++;
    if (batchPutNum > MAX_PRE_READ_QUE_BATCH_NUM) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery BatchPutWalRecordInfo get invalid batch num %u.",
            m_pdbId, m_walId, batchPutNum));
    }
    if (batchPutNum == MAX_PRE_READ_QUE_BATCH_NUM || forcePutAll) {
        StorageReleasePanic(
            m_dispatchQueue == nullptr, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]BatchPutWalRecordInfo get invalid m_dispatchQueue.", m_pdbId, m_walId));
        m_dispatchQueue->PutN<WalRecordInfo *>(walRecordInfos, batchPutNum);
        batchPutNum = 0;
    }
}

void WalRecovery::FinishDirtyPageSetBuild()
{
    if (m_dispatchWorkerThd != nullptr) {
        m_dispatchWorkerThd->join();
        delete m_dispatchWorkerThd;
        m_dispatchWorkerThd = nullptr;
    }

    m_dirtyPageEntryArray = m_pageWalRecordInfoListHtab.BuildWalDirtyPageEntryArray(m_dirtyPageEntryArraySize);
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery DispatchDirtyPageWorker queue max usage %u pageNum %ld",
        m_pdbId, m_walId, m_dispatchQueue->GetMaxUsage(), m_dirtyPageEntryArraySize));

    m_dispatchQueue->Destroy();
    DstorePfreeExt(m_dispatchQueue);
    m_getDirtyPageReadBuffer->StopLoadWorker();
    m_isDirtyPageSetBuilt.store(true, std::memory_order_release);
}

RetStatus WalRecovery::BuildDirtyPageSetAndPageWalRecordListHtab()
{
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery BuildDirtyPageSet start", m_pdbId, m_walId));
    SetWalRecoveryStage(WalRecoveryStage::RECOVERY_GET_DIRTY_PAGE_SET);
    AutoMemCxtSwitch autoMemCxtSwitch(m_memoryContext);
    std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> startBuildSet =
            std::chrono::high_resolution_clock::now();
    LatencyStat::Timer timer(&WalPerfUnit::GetInstance().m_buildDirtyPageSet);
    StartDirtyPageSetBuildWorker();
    uint64 walOnDemandRedoReadBufferSize = static_cast<uint64>(g_storageInstance->GetGuc()->walReadBufferSize);
    WalFileLoadToBufferConf walFileLoadToBufferConf = {
        m_recoveryStartPlsn, static_cast<uint32>(walOnDemandRedoReadBufferSize / WAL_READ_BUFFER_BLOCK_SIZE),
        WAL_READ_BUFFER_BLOCK_SIZE, walOnDemandRedoReadBufferSize};
    /* m_getDirtyPageReadBuffer should be initialized when start DirtyPageSetBuildWorker */
    StorageExit1(STORAGE_VAR_NULL(m_getDirtyPageReadBuffer), MODULE_WAL,
                 ErrMsg("[PDB:%u WAL:%lu]WalRecovery get invalid m_getDirtyPageReadBuffer.", m_pdbId, m_walId));
    StorageExit1(STORAGE_FUNC_FAIL(m_getDirtyPageReadBuffer->StartLoadToBuffer(walFileLoadToBufferConf)), MODULE_WAL,
                 ErrMsg("[PDB:%u WAL:%lu]WalRecovery StartLoadToBuffer fail.", m_pdbId, m_walId));

    RetStatus retStatus;
    WalRecordReader *walRecordReader = nullptr;
    WalReaderConf conf = {m_walId, m_recoveryStartPlsn, nullptr, m_getDirtyPageReadBuffer, m_walFileSize,
                          WalReadSource::WAL_READ_FROM_BUFFER};
    DstoreMemoryContext recorderMemCtx = DstoreAllocSetContextCreate(
        m_memoryContext, "WalRecordMemContext", ALLOCSET_DEFAULT_SIZES, MemoryContextType::THREAD_CONTEXT);
    if (STORAGE_VAR_NULL(recorderMemCtx)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery Alloc WalRecordReaderMemContext fail.", m_pdbId, m_walId));
    }
    retStatus = WalRecordReader::AllocateWalReader(conf, &walRecordReader, recorderMemCtx);
    if (STORAGE_FUNC_FAIL(retStatus)) {
        delete walRecordReader;
        DstoreMemoryContextDelete(recorderMemCtx);
        return DSTORE_FAIL;
    }
    uint64 lastWalGroupEndPlsn = m_recoveryEndPlsn != INVALID_PLSN ?
        BuildDirtyPageSetAndPageWalRecordListReadWal<true>(walRecordReader) :
        BuildDirtyPageSetAndPageWalRecordListReadWal<false>(walRecordReader);
    delete walRecordReader;
    FinishDirtyPageSetBuild();
    DstoreMemoryContextDelete(recorderMemCtx);

    if (m_recoveryEndPlsn == INVALID_PLSN) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery term %lu BuildDirtyPageSet success, "
                "Set Lsn range [%lu, ), result is [%lu %lu]",
                m_pdbId, m_walId, m_term,
                m_recoveryStartPlsn, m_recoveryStartPlsn, lastWalGroupEndPlsn));
    } else {
        ErrLog(DSTORE_LOG, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery term %lu BuildDirtyPageSet success, "
                "Set Lsn range [%lu, %lu], result is [%lu %lu]",
                m_pdbId, m_walId, m_term,
                m_recoveryStartPlsn, m_recoveryEndPlsn, m_recoveryStartPlsn, lastWalGroupEndPlsn));
    }
    SetWalRecoveryStage(WalRecoveryStage::RECOVERY_GET_DIRTY_PAGE_SET_DONE);
    m_recoveryEndPlsn = lastWalGroupEndPlsn;
    std::chrono::duration<double> spendSecs = std::chrono::high_resolution_clock::now() - startBuildSet;

    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery BuildDirtyPageSet start from plsn:%lu build done, spend time %f seconds.",
        m_pdbId, m_walId, m_recoveryStartPlsn, spendSecs.count()));
    return DSTORE_SUCC;
}

template<bool checkEndPlsn>
uint64 WalRecovery::BuildDirtyPageSetAndPageWalRecordListReadWal(WalRecordReader *walRecordReader)
{
    const WalRecordAtomicGroup *walGroup = nullptr;
    const WalRecord *walRecord = nullptr;
    uint64 curGroupOffset = 0;
    WalRecordInfo walRecordInfos[MAX_PRE_READ_QUE_BATCH_NUM];
    uint32 batchPutNum = 0;
    uint64 lastWalGroupEndPlsn = 0;
    RetStatus retStatus;
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery BuildDirtyPageSetAndPageWalRecordListReadWal start", m_pdbId, m_walId));
    do {
        retStatus = walRecordReader->ReadNext(&walGroup);
        if (unlikely(retStatus != DSTORE_SUCC)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery read next group fail, currPlsn %lu.",
                m_pdbId, m_walId, walRecordReader->GetCurGroupStartPlsn()));
        }
        if (unlikely(walGroup == nullptr)) {
            break;
        }
        if (checkEndPlsn) {
            lastWalGroupEndPlsn = walRecordReader->GetCurGroupEndPlsn();
        }

        curGroupOffset = sizeof(WalRecordAtomicGroup);
        for (uint16 i = 0; i < walGroup->recordNum; i++) {
            walRecord = walRecordReader->GetNextWalRecord();
            StorageReleasePanic(walRecord == nullptr, MODULE_WAL,
                                ErrMsg("[PDB:%u WAL:%lu]WalRecovery get invalid walRecord.", m_pdbId, m_walId));
            WalRecordInfo walRecordInfo = {
                WalUtils::GetRecordPlsn<false>(walRecordReader->GetCurGroupStartPlsn(), curGroupOffset, m_walFileSize),
                {walGroup->xid, m_walId, m_pdbId, walRecordReader->GetCurRecordEndPlsn()},
                walRecord};
            BatchPutWalRecordInfo(walRecordInfos, batchPutNum, walRecordInfo, false);
            curGroupOffset += walRecord->m_size;
        }
        if (checkEndPlsn && lastWalGroupEndPlsn >= m_recoveryEndPlsn) {
            break;
        }
    } while (true);
    BatchPutWalRecordInfo(walRecordInfos, batchPutNum,
        {INVALID_PLSN, {INVALID_XID, INVALID_WAL_ID, INVALID_PDB_ID, INVALID_PLSN}, nullptr}, true);
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery BuildDirtyPageSetAndPageWalRecordListReadWal finish", m_pdbId, m_walId));
    return checkEndPlsn ? lastWalGroupEndPlsn : walRecordReader->GetCurGroupEndPlsn();
}

void WalRecovery::SplitAndDispatchInitMultipleDataPageWalRecord(
    WalRecordRedoContext *context, const WalRecord *record)
{
    const WalRecordTbsInitDataPages *tbsRecord =
        static_cast<const WalRecordTbsInitDataPages *>(static_cast<const void *>(record));
    if (tbsRecord->dataPageType != PageType::HEAP_PAGE_TYPE &&
        tbsRecord->dataPageType != PageType::INDEX_PAGE_TYPE) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("Invalid data page type"));
    }
    FileId fileId = tbsRecord->firstDataPageId.m_fileId;
    BlockNumber firstBlockId = tbsRecord->firstDataPageId.m_blockId;
    PageId fsmPageId = tbsRecord->firstFsmIndex.page;
    uint16 firstSlotId = tbsRecord->firstFsmIndex.slot;
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("[PDB:%u WAL:%lu]WalRecovery SplitAndDispatchInitMultipleDataPageWalRecord recordEndPlsn:%lu, "
                  "fileId:%u, blockId:%u-%u", m_pdbId, m_walId, context->recordEndPlsn, fileId, firstBlockId,
                  firstBlockId + tbsRecord->dataPageCount - 1));

    for (uint16 i = 0; i < tbsRecord->dataPageCount; ++i) {
        PageId curDataPageId = {fileId, firstBlockId + i};
        uint16 curSlotId = firstSlotId + i;
        FsmIndex curFsmIndex = {fsmPageId, curSlotId};
        uint32 redoWorkerId = GetDispatchPageRedoWorkerId(curDataPageId);
        BatchPutParaForWalReplay &batchPutPara = m_parallelBatchPutEntrys[redoWorkerId];
        RedoWalRecordEntry *redoWalRecordEntry = &batchPutPara.redoWalRecordEntry[batchPutPara.entryNum];
        redoWalRecordEntry->recordEndPlsn = context->recordEndPlsn;
        redoWalRecordEntry->walRecordNeedFree = true;
        redoWalRecordEntry->ctx = *context;
        redoWalRecordEntry->walRecordInfo.freeRedoEntry =
            m_parallelRedoWorkers[redoWorkerId]->AllocRedoWalRecord(sizeof(WalRecordTbsInitOneDataPage));

        WalRecordTbsInitOneDataPage *tbsInitOneDataPage =
            STATIC_CAST_PTR_TYPE(redoWalRecordEntry->GetWalRecordInBufFreeEntry(), WalRecordTbsInitOneDataPage *);
        tbsInitOneDataPage->SetData(curDataPageId, tbsRecord->dataPageType, curFsmIndex,
                                    tbsRecord->preWalPointer[i], tbsRecord->filePreVersion);
        batchPutPara.entryNum++;
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
               ErrMsg("[PDB:%u WAL:%lu]WalRecovery SplitAndDispatchInitMultipleDataPageWalRecord recordEndPlsn:%lu, "
                      "pageId:(%u, %u), dispatch worker:%u, fileVersion:%lu, preWalId:%lu, prePlsn:%lu, preGlsn:%lu",
                      m_pdbId, m_walId, context->recordEndPlsn, curDataPageId.m_fileId, curDataPageId.m_blockId,
                      redoWorkerId, tbsRecord->filePreVersion, tbsRecord->preWalPointer[i].walId,
                      tbsRecord->preWalPointer[i].endPlsn, tbsRecord->preWalPointer[i].glsn));
        if (batchPutPara.entryNum >= MAX_REDO_QUE_BATCH_NUM) {
            m_parallelRedoWorkers[redoWorkerId]->AppendWalRecord(
                &batchPutPara.redoWalRecordEntry[0], batchPutPara.entryNum, __FUNCTION__, __LINE__);
            batchPutPara.entryNum = 0;
        }

        GsAtomicWriteU64(&m_redoWorkersMaxDispatchedPlsn[redoWorkerId], context->recordEndPlsn);
    }
}

void WalRecovery::SplitAndDispatchInitBitmapPagesWalRecord(
    WalRecordRedoContext *context, const WalRecord *record)
{
    const WalRecordTbsInitBitmapPages *tbsRecord =
        static_cast<const WalRecordTbsInitBitmapPages *>(static_cast<const void *>(record));
    PageId curPageId = tbsRecord->firstPageId;
    PageId curDataPageId = tbsRecord->firstDataPageId;
    if (tbsRecord->pageType != PageType::TBS_BITMAP_PAGE_TYPE) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("Invalid page type"));
    }
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("[PDB:%u WAL:%lu]WalRecovery SplitAndDispatchInitBitmapPagesWalRecord recordEndPlsn:%lu, "
                  "fileId:%u, blockId:%u-%u", m_pdbId, m_walId, context->recordEndPlsn, curPageId.m_fileId,
                  curPageId.m_blockId, curPageId.m_blockId + tbsRecord->pageCount - 1));

    for (uint16 i = 0; i < tbsRecord->pageCount; i++) {
        uint32 redoWorkerId = GetDispatchPageRedoWorkerId(curPageId);
        BatchPutParaForWalReplay &batchPutPara = m_parallelBatchPutEntrys[redoWorkerId];
        RedoWalRecordEntry *redoWalRecordEntry =
            &batchPutPara.
            redoWalRecordEntry[batchPutPara.entryNum];
        redoWalRecordEntry->recordEndPlsn = context->recordEndPlsn;
        redoWalRecordEntry->walRecordNeedFree = true;
        redoWalRecordEntry->ctx = *context;
        redoWalRecordEntry->walRecordInfo.freeRedoEntry =
            m_parallelRedoWorkers[redoWorkerId]->AllocRedoWalRecord(sizeof(WalRecordTbsInitOneBitmapPage));
        WalRecordTbsInitOneBitmapPage *tbsInitOneBitmapPage =
            STATIC_CAST_PTR_TYPE(redoWalRecordEntry->GetWalRecordInBufFreeEntry(), WalRecordTbsInitOneBitmapPage *);
        tbsInitOneBitmapPage->SetData(curPageId, tbsRecord->pageType, curDataPageId,
                                      tbsRecord->preWalPointer[i], tbsRecord->filePreVersion);

        batchPutPara.entryNum++;
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
               ErrMsg("[PDB:%u WAL:%lu]WalRecovery SplitAndDispatchInitBitmapPagesWalRecord recordEndPlsn:%lu, "
                      "pageId:(%u, %u), dispatch worker:%u, fileVersion:%lu, preWalId:%lu, prePlsn:%lu, preGlsn:%lu",
                      m_pdbId, m_walId, context->recordEndPlsn, curPageId.m_fileId, curPageId.m_blockId, redoWorkerId,
                      tbsRecord->filePreVersion, tbsRecord->preWalPointer[i].walId,
                      tbsRecord->preWalPointer[i].endPlsn, tbsRecord->preWalPointer[i].glsn));
        if (batchPutPara.entryNum >= MAX_REDO_QUE_BATCH_NUM) {
            m_parallelRedoWorkers[redoWorkerId]->AppendWalRecord(
                &batchPutPara.redoWalRecordEntry[0], batchPutPara.entryNum, __FUNCTION__, __LINE__);
            batchPutPara.entryNum = 0;
        }

        GsAtomicWriteU64(&m_redoWorkersMaxDispatchedPlsn[redoWorkerId], context->recordEndPlsn);
        curPageId.m_blockId++;
        curDataPageId.m_blockId += DF_BITMAP_BIT_CNT * static_cast<uint16>(tbsRecord->extentSize);
    }
}

inline void WalRecovery::DispatchPlsnSyncerToAllWorkers(RedoWalRecordEntry &redoWalRecordEntry)
{
    ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg("DispatchPlsnSyncerToAllWorkers %lu", redoWalRecordEntry.recordEndPlsn));
    for (uint32 redoWorkerId = 0; redoWorkerId < m_redoWorkerNum; redoWorkerId++) {
        BatchPutParaForWalReplay &batchPutPara = m_parallelBatchPutEntrys[redoWorkerId];
        batchPutPara.redoWalRecordEntry[batchPutPara.entryNum] = redoWalRecordEntry;
        batchPutPara.entryNum++;
        if (batchPutPara.entryNum >= MAX_REDO_QUE_BATCH_NUM) {
            m_parallelRedoWorkers[redoWorkerId]->AppendWalRecord(&batchPutPara.redoWalRecordEntry[0],
                                                                 batchPutPara.entryNum, __FUNCTION__, __LINE__);
            batchPutPara.entryNum = 0;
        }
        GsAtomicWriteU64(&m_redoWorkersMaxDispatchedPlsn[redoWorkerId], redoWalRecordEntry.recordEndPlsn);
    }
}

inline void WalRecovery::DispatchBarrierToPageRedoWorkers(RedoWalRecordEntry &redoWalRecordEntry)
{
    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
        ErrMsg("DispatchBarrierToPageRedoWorkers %lu", redoWalRecordEntry.recordEndPlsn));
    for (uint32 redoWorkerId = 0; redoWorkerId < m_redoWorkerNum; redoWorkerId++) {
        BatchPutParaForWalReplay &batchPutPara = m_parallelBatchPutEntrys[redoWorkerId];
        batchPutPara.redoWalRecordEntry[batchPutPara.entryNum] = redoWalRecordEntry;
        batchPutPara.entryNum++;
        if (batchPutPara.entryNum >= MAX_REDO_QUE_BATCH_NUM) {
            m_parallelRedoWorkers[redoWorkerId]->AppendWalRecord(&batchPutPara.redoWalRecordEntry[0],
                                                                 batchPutPara.entryNum, __FUNCTION__, __LINE__);
            batchPutPara.entryNum = 0;
        }
        GsAtomicWriteU64(&m_redoWorkersMaxDispatchedPlsn[redoWorkerId], redoWalRecordEntry.recordEndPlsn);
    }
}

RetStatus WalRecovery::DispatchWalRecord(RedoWalRecordEntry &redoWalRecordEntry)
{
    LatencyStat::Timer timer(&WalPerfUnit::GetInstance().m_walRedoDispatchWalRecord);
    const WalRecord *record = redoWalRecordEntry.GetWalRecord();
    uint64 endPlsn = redoWalRecordEntry.recordEndPlsn;
    if (!IsRedoableWalRecord(record->m_type)) {
        ErrLog(DSTORE_WARNING,
            MODULE_WAL, ErrMsg("WalType:%hu not support redo and not dispatch.",
                static_cast<uint16>(record->m_type)));
        return DSTORE_SUCC;
    }

    /* For WalRecord with multipages info, split it into multiple WalRecords and dispatch to different workers */
    if (record->m_type == WAL_TBS_INIT_MULTIPLE_DATA_PAGES) {
        SplitAndDispatchInitMultipleDataPageWalRecord(&redoWalRecordEntry.ctx, record);
    } else if (record->m_type == WAL_TBS_INIT_BITMAP_PAGES) {
        SplitAndDispatchInitBitmapPagesWalRecord(&redoWalRecordEntry.ctx, record);
    /* For WalRecord without page info, dispath to DDL redo worker */
    } else if (IsWalRecordWithoutPageInfo(record->m_type)) {
        TablespaceId tablespaceId = static_cast<const WalRecordTbsLogical *>(record)->GetTablespaceId();
        uint32 redoWorkerId = GetDispatchDDLRedoWorkerId(tablespaceId);
        BatchPutParaForWalReplay &batchPutPara = m_parallelBatchPutEntrys[redoWorkerId];
        batchPutPara.redoWalRecordEntry[batchPutPara.entryNum] = redoWalRecordEntry;
        batchPutPara.entryNum++;
        m_parallelRedoWorkers[redoWorkerId]->AppendWalRecord(
            &batchPutPara.redoWalRecordEntry[0], batchPutPara.entryNum, __FUNCTION__, __LINE__);
        batchPutPara.entryNum = 0;
        GsAtomicWriteU64(&m_redoWorkersMaxDispatchedPlsn[redoWorkerId], endPlsn);
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("DispatchWalRecord plsn %lu to worker %u, type %s", endPlsn, redoWorkerId,
                      g_walTypeForPrint[static_cast<uint16>(record->m_type)]));
        /* For WalRecord with only one page info, dispatch to page redo worker */
    } else {
        PageId pageId =
            static_cast<const WalRecordForPageOnDisk *>(static_cast<const void *>(record))->
            GetCompressedPageId();
        uint32 redoWorkerId = GetDispatchPageRedoWorkerId(pageId);
        BatchPutParaForWalReplay &batchPutPara = m_parallelBatchPutEntrys[redoWorkerId];
        batchPutPara.redoWalRecordEntry[batchPutPara.entryNum] = redoWalRecordEntry;
        batchPutPara.entryNum++;
        uint32 batchNum = MAX_REDO_QUE_BATCH_NUM;
        if (batchPutPara.entryNum >= batchNum) {
            m_parallelRedoWorkers[redoWorkerId]->AppendWalRecord(
                &batchPutPara.redoWalRecordEntry[0], batchPutPara.entryNum, __FUNCTION__, __LINE__);
            batchPutPara.entryNum = 0;
        }
        GsAtomicWriteU64(&m_redoWorkersMaxDispatchedPlsn[redoWorkerId], endPlsn);
        ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg("DispatchWalRecord plsn %lu to worker %u", endPlsn, redoWorkerId));
    }
    return DSTORE_SUCC;
}

bool WalRecovery::IsDirtyPageSetBuilt() const
{
    return m_isDirtyPageSetBuilt.load(std::memory_order_acquire);
}

void WalRecovery::WaitDirtyPageSetBuilt() const
{
    constexpr uint64 reportStep = 100 * 1000;
    uint64 counter = 0; /*  */
    while (!IsDirtyPageSetBuilt()) {
        if (unlikely(counter++ % reportStep == 0)) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                   ErrMsg("PDB:%u WAL:%lu term:%lu WaitDirtyPageSet retry %lu times", GetPdbId(),
                          m_walStream->GetWalId(), m_term, counter));
        }
        GaussUsleep(SLEEP_MILLISECONDS);
    }
}

WalDirtyPageEntry *WalRecovery::GetDirtyPageEntryArrayCopy(long &retArraySize) const
{
    if (m_dirtyPageEntryArraySize == 0 || m_dirtyPageEntryArray == nullptr) {
        retArraySize = 0;
        return nullptr;
    }

    DstoreMemoryContext oldContext =
        DstoreMemoryContextSwitchTo(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    Size memorySize = static_cast<unsigned long>(m_dirtyPageEntryArraySize) * sizeof(WalDirtyPageEntry);
    WalDirtyPageEntry *dirtyPageEntryArray = nullptr;
    int32 retryCnt = 0;
    constexpr int32 retryMax = 1000;
RETRY:
    StorageExit0(retryCnt++ > retryMax, MODULE_WAL, ErrMsg("No memory for dirtyPageEntryArray."));
    if (memorySize <= MaxAllocSize) {
        dirtyPageEntryArray = static_cast<WalDirtyPageEntry *>(DstorePalloc(memorySize));
    } else {
        dirtyPageEntryArray = static_cast<WalDirtyPageEntry *>(
            DstoreMemoryContextAllocHugeSize(g_dstoreCurrentMemoryContext, memorySize));
    }
    if (dirtyPageEntryArray == nullptr) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_WAL,
               ErrMsg("[PDB:%u WAL:%lu]WalRecovery palloc space of dirtyPageEntryArray fail.", m_pdbId, m_walId));
        GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        goto RETRY;
    }
    DstoreMemcpySafelyForHugeSize(dirtyPageEntryArray, memorySize, m_dirtyPageEntryArray, memorySize);

    (void)DstoreMemoryContextSwitchTo(oldContext);
    retArraySize = m_dirtyPageEntryArraySize;
    return dirtyPageEntryArray;
}

const PageWalRecordInfoListHtab *WalRecovery::GetWalRecordInfoListHtab() const
{
    return &m_pageWalRecordInfoListHtab;
}

void WalRecovery::InitRedoStatisticInfo(uint64 recoveryStartPlsn, uint32 redoWorkerNum)
{
    if (unlikely(static_cast<uint64>(g_traceSwitch) & WAL_TRACE_SWITCH)) {
        uint64 totalNeed = m_redoStatisticInfo.walRedoTotalNeed;
        errno_t rc = memset_s(&m_redoStatisticInfo, sizeof(WalRedoStatisticInfo), 0, sizeof(WalRedoStatisticInfo));
        storage_securec_check(rc, "\0", "\0");
        m_redoStatisticInfo.walRedoStartPlsn = recoveryStartPlsn;
        m_redoStatisticInfo.walRedoTotalNeed = totalNeed;
        m_redoStatisticInfo.workerNum = redoWorkerNum;
    }
}

void WalRecovery::InitStatisticInfo()
{
    errno_t rc =
        memset_s(&m_walTypeRedoStatisticInfo, sizeof(WalRedoStatisticInfo), 0, sizeof(m_walTypeRedoStatisticInfo));
    storage_securec_check(rc, "\0", "\0");
}

void WalRecovery::InitRedoStatisticInfoTotalNeedPlsn(uint64 lastGroupPlsn)
{
    if (unlikely(static_cast<uint64>(g_traceSwitch) & WAL_TRACE_SWITCH)) {
        m_redoStatisticInfo.walRedoTotalNeed = lastGroupPlsn;
    }
}

void WalRecovery::InitRecoveryPlsn(WalId walId, WalCheckPoint *lastWalCheckpoint)
{
    uint64 recoveryPlsn = INVALID_PLSN;
    uint64 diskRecoveryPlsn = lastWalCheckpoint->diskRecoveryPlsn;
    uint64 memRecoveryPlsn = lastWalCheckpoint->memoryCheckpoint.memRecoveryPlsn;
    m_diskRecoveryStartPlsn = diskRecoveryPlsn;

    if (diskRecoveryPlsn >= memRecoveryPlsn) {
        recoveryPlsn = diskRecoveryPlsn;
        goto EXIT;
    }
    recoveryPlsn = lastWalCheckpoint->diskRecoveryPlsn;
EXIT:
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery checkpoint diskRecoveryPlsn:%lu memRecoveryPlsn:%lu recoveryPlsn:%lu.",
        m_pdbId, walId, diskRecoveryPlsn, memRecoveryPlsn, recoveryPlsn));
    m_recoveryStartPlsn = recoveryPlsn;
}

void WalRecovery::PrepareBgThreads()
{
    /* prepare redo read buffer. the actual thread will be spawn at StartLoadToBuffer() */
    m_dispatchQueue = static_cast<BlockSpscQueue *>(DstorePalloc0(sizeof(BlockSpscQueue)));
    if (m_dispatchQueue == nullptr || STORAGE_FUNC_FAIL(
        m_dispatchQueue->Init(DISPATCH_WORKER_QUE_CAPACITY, sizeof(RedoWalRecordEntry), nullptr))) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery DispatchQueue Init error!", m_pdbId, m_walId));
    }
    m_redoReadBuffer = DstoreNew(m_memoryContext) WalReadBuffer(m_memoryContext, m_walStream, m_redoMode);
    if (STORAGE_VAR_NULL(m_redoReadBuffer)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery WalReadBuffer allocation error!", m_pdbId, m_walId));
    }

    /* prepare redo worker threads */
    m_redoWorkersMaxDispatchedPlsn = static_cast<uint64 *>(DstoreMemoryContextAllocZero(m_memoryContext,
        m_redoWorkerNum * sizeof(uint64)));
    if (m_redoWorkersMaxDispatchedPlsn == nullptr) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery m_redoWorkersMaxDispatchedPlsn alloc error!", m_pdbId, m_walId));
    }
    ParallelRedoWorkerInitParam parallelRedoWorkerInitParam = {0, m_pdbId, m_walId, 0, REDO_WORKER_QUE_CAPACITY,
        nullptr, m_memoryContext, this};

    for (uint32 i = 0; i < DDL_REDO_WORKER_NUM; i++) {
        parallelRedoWorkerInitParam.workerId = static_cast<uint16>(i);
        m_parallelRedoWorkers[i] = DstoreNew(m_memoryContext) ParallelDDLRedoWorker(parallelRedoWorkerInitParam);
        if (STORAGE_VAR_NULL(m_parallelRedoWorkers[i])) {
            ErrLog(DSTORE_PANIC, MODULE_WAL,
                ErrMsg("[PDB:%u WAL:%lu]WalRecovery ParallelDDLRedoWorker %u new fail.", m_pdbId, m_walId, i));
        }
        if (STORAGE_FUNC_FAIL(m_parallelRedoWorkers[i]->Init())) {
            ErrLog(DSTORE_PANIC, MODULE_WAL,
                ErrMsg("[PDB:%u WAL:%lu]WalRecovery ParallelDDLRedoWorker %u init fail.", m_pdbId, m_walId, i));
        }
        static_cast<ParallelDDLRedoWorker*>(m_parallelRedoWorkers[i])->Run("ParallelDDLRedoWorker");
    }

    for (uint32 i = DDL_REDO_WORKER_NUM; i < m_redoWorkerNum; i++) {
        parallelRedoWorkerInitParam.workerId = static_cast<uint16>(i);
        m_parallelRedoWorkers[i] = DstoreNew(m_memoryContext) ParallelPageRedoWorker(parallelRedoWorkerInitParam);
        if (STORAGE_VAR_NULL(m_parallelRedoWorkers[i])) {
            ErrLog(DSTORE_PANIC, MODULE_WAL,
                ErrMsg("[PDB:%u WAL:%lu]WalRecovery ParallelPageRedoWorker %u new fail.", m_pdbId, m_walId, i));
        }
        if (STORAGE_FUNC_FAIL(m_parallelRedoWorkers[i]->Init())) {
            ErrLog(DSTORE_PANIC, MODULE_WAL,
                ErrMsg("[PDB:%u WAL:%lu]WalRecovery ParallelPageRedoWorker %u init fail.", m_pdbId, m_walId, i));
        }
        static_cast<ParallelPageRedoWorker*>(m_parallelRedoWorkers[i])->Run("ParallelPageRedoWorker");
    }

    /* prepare dispatch worker thread */
    m_parallelBatchPutEntrys = static_cast<BatchPutParaForWalReplay *>(
        DstorePalloc0(sizeof(BatchPutParaForWalReplay) * m_redoWorkerNum));
    if (STORAGE_VAR_NULL(m_parallelBatchPutEntrys)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery Dispatch palloc batchPutPara fail!", m_pdbId, m_walId));
    }
    m_dispatchWorkerThd = new std::thread(&WalRecovery::DispatchWorkerMain, this);
}

void WalRecovery::CleanUpBgThreads()
{
    /* clean up redo workers after DispatchWorkerMain is completed */
    while (!(m_dispatchFinished.load(std::memory_order_acquire))) {
        GaussUsleep(WAIT_DISPATCH_QUEUE_MICROSEC);
    }

    if (m_dispatchWorkerThd != nullptr) {
        /* clean up dispatch worker */
        m_dispatchWorkerThd->join();
        delete m_dispatchWorkerThd;
        m_dispatchWorkerThd = nullptr;
        ErrLog(DSTORE_LOG, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery dispatchWorker thread stop success.", m_pdbId, m_walId));
    }

    for (uint32 i = 0; i < m_redoWorkerNum; i++) {
        m_parallelRedoWorkers[i]->WaitRedoFinish();
    }

    m_curRedoFinishedPlsn = GetWorkersRedoFinishedPlsn();
    m_walStream->SetStandbyRedoFinishPlsn(m_curRedoFinishedPlsn);
    if (GetWorkersRedoFinishedPlsn() < m_lastGroupEndPlsn) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery not finish all redo work, redoFinishPlsn:%lu, lastGroupEndPlsn:%lu.",
            m_pdbId, m_walId, GetWorkersRedoFinishedPlsn(), m_lastGroupEndPlsn));
    }

    for (uint32 i = 0; i < m_redoWorkerNum; i++) {
        delete m_parallelRedoWorkers[i];
        m_parallelRedoWorkers[i] = nullptr;
    }

    DstorePfreeExt(m_parallelBatchPutEntrys);
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery parallelRedo threads stop success", m_pdbId, m_walId));

    /* worker threads assume a valid read buffer, so it should be free'd last */
    StorageReleasePanic(m_redoReadBuffer == nullptr, MODULE_WAL, ErrMsg("CleanUpBgThreads get invalid redoReadBuffer"));
    m_redoReadBuffer->FreeReadBuffer();
    delete m_redoReadBuffer;
    m_redoReadBuffer = nullptr;
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery m_loadWalToBuffer thread stop success", m_pdbId, m_walId));
}

RetStatus WalRecovery::Recovery()
{
    AutoMemCxtSwitch autoMemCxtSwitch(m_memoryContext);
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("Pdb %hhu WalRecovery start from plsn:%lu walId %lu.", m_pdbId, m_recoveryStartPlsn, m_walId));
    std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> startRedo =
        std::chrono::high_resolution_clock::now();
    m_startRedoTime = startRedo;

    /* prepare memory context for record reader */
    m_recordReaderMemContext = DstoreAllocSetContextCreate(m_memoryContext, "WalRecordMemContext",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::THREAD_CONTEXT);
    if (m_recordReaderMemContext == nullptr) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Alloc WalRecordReaderMemContext fail."));
        return DSTORE_FAIL;
    }
    /* Step 2: prepare background threads. later parallel Step 2 - 4 with more than one worker threads */
    if (m_enableParallelRedo) {
        PrepareBgThreads();
    }

    /* Step 3: Perform redo */
    uint64 lastGroupEndPlsn = m_recoveryStartPlsn;
    if (STORAGE_FUNC_FAIL(Redo(&lastGroupEndPlsn))) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Recovery redo fail."));
    }
    InitRedoStatisticInfoTotalNeedPlsn(lastGroupEndPlsn);
    m_lastGroupEndPlsn = lastGroupEndPlsn;

    /* Step : clean up threads */
    if (m_enableParallelRedo) {
        CleanUpBgThreads();
    }

    /* clean up wal recorder memory context */
    DstoreMemoryContextDelete(m_recordReaderMemContext);
    m_recordReaderMemContext = nullptr;

    SetWalRecoveryStage(WalRecoveryStage::RECOVERY_REDO_DONE);
    std::chrono::duration<double> spendSecs = std::chrono::high_resolution_clock::now() - startRedo;

    /* if start and end plsn are the same, no WAL replay and no need to dump a message out. */
    constexpr double byteToMbFactor = 1024 * 1024;
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("WalRecovery pdbId %hhu walId:%lu workerNum:%u redo startPlsn:%lu endPlsn:%lu spend time %f seconds"
            " avgSpeed %f MB/S.",
            m_pdbId, m_walId, m_redoWorkerNum, m_recoveryStartPlsn, lastGroupEndPlsn, spendSecs.count(),
            (static_cast<double>(lastGroupEndPlsn - m_recoveryStartPlsn) / byteToMbFactor) / spendSecs.count()));
    m_redoStatisticInfo.totalRecoveryTime = spendSecs.count();

    DstoreLWLockAcquire(&m_getPageWalRecordsLock, LW_EXCLUSIVE);
    m_pageWalRecordInfoListHtab.Destroy();
    if (m_getDirtyPageReadBuffer != nullptr) {
        m_getDirtyPageReadBuffer->FreeReadBuffer();
        delete m_getDirtyPageReadBuffer;
        m_getDirtyPageReadBuffer = nullptr;
    }
    LWLockRelease(&m_getPageWalRecordsLock);
    return DSTORE_SUCC;
}

RetStatus WalRecovery::GetAllRedoStatisticInfo(WalRedoStatisticInfo *redoStatisticInfo)
{
    errno_t rc = memcpy_s(redoStatisticInfo, sizeof(WalRedoStatisticInfo), &m_redoStatisticInfo,
        sizeof(WalRedoStatisticInfo));
    storage_securec_check(rc, "", "");
    return DSTORE_SUCC;
}

uint64 WalRecovery::GetWorkersRedoFinishedPlsn(bool reportLog) const
{
    uint64 workersMinRedonePlsn = UINT64_MAX;
    uint64 workersMaxRedonePlsn = 0;
    uint64 workerRedonePlsn = 0;
    for (uint32 i = 0; i < m_redoWorkerNum; i++) {
        if (m_parallelRedoWorkers[i] == nullptr) {
            break;
        }
        workerRedonePlsn = m_parallelRedoWorkers[i]->GetMaxRedoFinishedPlsn();
        if (reportLog) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("[PDB:%u WAL:%lu]WalRecovery GetWorkersRedoFinishedPlsn worker %u maxRedoPlsn %lu.",
                m_pdbId, m_walId, i, workerRedonePlsn));
        }
        uint64 dispatchplsn = GsAtomicReadU64(&m_redoWorkersMaxDispatchedPlsn[i]);
        if (dispatchplsn > workerRedonePlsn) {
            workersMinRedonePlsn = DstoreMin(workersMinRedonePlsn, workerRedonePlsn);
        }
        workersMaxRedonePlsn = DstoreMax(workersMaxRedonePlsn, workerRedonePlsn);
    }
    uint64 redoPlsn = workersMinRedonePlsn != UINT64_MAX ? workersMinRedonePlsn : workersMaxRedonePlsn;
    if (redoPlsn != 0) {
        return redoPlsn;
    }

    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    /* WalRecovery may not be inited when user queries pdb_wal_info */
    if (pdb && pdb->GetPdbRoleMode() == PdbRoleMode::PDB_STANDBY && pdb->GetControlFile()) {
        ControlWalStreamPageItemData *walStreamInfo = nullptr;
        RetStatus ret = pdb->GetControlFile()->GetWalStreamInfo(m_walId, &walStreamInfo);
        uint64 diskRecoveryPlsn = INVALID_PLSN;
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Failed to get diskRecoveryPlsn, pdbName: %s, walId: %lu.",
                pdb->GetPdbName(), m_walId));
        } else {
            diskRecoveryPlsn = walStreamInfo->lastWalCheckpoint.diskRecoveryPlsn;
        }
        redoPlsn = DstoreMax(redoPlsn, diskRecoveryPlsn);
    }
    return redoPlsn;
}

uint64 WalRecovery::GetWorkersRecoveryPlsn(bool reportLog) const
{
    uint64 workersMinRecoveryPlsn = UINT64_MAX;
    uint64 workerRecoveryPlsn = 0;
    for (uint32 i = 0; i < m_redoWorkerNum; i++) {
        if (m_parallelRedoWorkers[i] == nullptr) {
            break;
        }
        workerRecoveryPlsn = m_parallelRedoWorkers[i]->GetMaxRecoveryPlsn();
        if (reportLog) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("[PDB: %hhu, WAL: %lu]WalRecovery GetWorkersRecoveryPlsn worker %u maxRecoveryPlsn %lu.",
                m_pdbId, m_walId, i, workerRecoveryPlsn));
        }

        workersMinRecoveryPlsn = DstoreMin(workersMinRecoveryPlsn, workerRecoveryPlsn);
    }
    return workersMinRecoveryPlsn == UINT64_MAX ? 0 : workersMinRecoveryPlsn;
}


inline uint32 WalRecovery::GetDispatchPageRedoWorkerId(const PageId pageId) const
{
    return tag_hash(&pageId, sizeof(PageId)) % (m_redoWorkerNum - DDL_REDO_WORKER_NUM) + DDL_REDO_WORKER_NUM;
}

inline uint32 WalRecovery::GetDispatchDDLRedoWorkerId(const TablespaceId tablespaceId) const
{
    return tag_hash(&tablespaceId, sizeof(tablespaceId)) % DDL_REDO_WORKER_NUM;
}

void WalRecovery::TryDispatchWhenNoRecordsComing()
{
    for (uint32 i = 0; i < m_redoWorkerNum; i++) {
        BatchPutParaForWalReplay &batchPutPara = m_parallelBatchPutEntrys[i];
        if (batchPutPara.entryNum > 0 && m_parallelRedoWorkers[i]->GetFreeSpace() >= batchPutPara.entryNum) {
            m_parallelRedoWorkers[i]->AppendWalRecord(&batchPutPara.redoWalRecordEntry[0],
                                                      batchPutPara.entryNum, __FUNCTION__, __LINE__);
            batchPutPara.entryNum = 0;
        }
    }
}

void WalRecovery::DispatchWorkerMain()
{
    DstoreSetMemoryOutOfControl();
    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister(INVALID_PDB_ID, false, "WalRedodispWrk", true,
                                                     ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    AutoMemCxtSwitch autoMemCxtSwitch(m_memoryContext);
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]DispatchWorkerMain start.", m_pdbId, m_walId));
    WalUtils::HandleWalThreadCpuBind("DispatchWorker");
    RedoWalRecordEntry *recordEntry;
    uint32 getNum = 0;
    uint64 lastRecyclePlsn = m_recoveryStartPlsn;
    uint64 waitCount = 0;
    m_dispatchFinished.store(false, std::memory_order_release);
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    if (unlikely(pdb == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalRecovery get pdb %u failed.", m_pdbId));
    }
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]DispatchWorkerMain start dispatch.", m_pdbId, m_walId));
    while (true) {
        /* take wal records from dispatch queue */
        if (m_dispatchQueue->GetN<RedoWalRecordEntry*>(MAX_REDO_QUE_BATCH_NUM, &recordEntry, &getNum) != DSTORE_SUCC) {
            StorageReleasePanic(m_redoReadBuffer == nullptr, MODULE_WAL,
                                ErrMsg("DispatchWorkerMain get invalid redoReadBuffer"));
            bool printPlsn = (++waitCount % WAIT_COUNT_FOR_REPORT_REDO_PLSN == 0);
            m_redoReadBuffer->RecycleReadBuffer(GetWorkersRedoFinishedPlsn(printPlsn));
            TryDispatchWhenNoRecordsComing();
            GaussUsleep(1);
            continue;
        }
        StorageAssert(getNum >= 1);
        waitCount = 0;
        for (uint32 i = 0; i < getNum; i++) {
            if (recordEntry[i].recordEndPlsn == INVALID_PLSN) {
                goto DISPATCH_END;
            }

            /* dispatch plsn syncer to all workers */
            if (recordEntry[i].GetWalRecord() == nullptr) {
                DispatchPlsnSyncerToAllWorkers(recordEntry[i]);
                continue;
            }

            if (recordEntry[i].GetWalRecord()->GetType() == WAL_BARRIER_CSN) {
                if (pdb->GetPdbRoleMode() == PdbRoleMode::PDB_STANDBY) {
                    DispatchBarrierToPageRedoWorkers(recordEntry[i]);
                }
                continue;
            }

            /* dispatch to redoQueue of parallel redo workers */
            if (STORAGE_FUNC_FAIL(DispatchWalRecord(recordEntry[i]))) {
                ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Dispatch recordEntry fail!"));
                break;
            }
        }

        /* if redo workers finished with some buffer block(s), notify readBuffer to recycle them */
        if (recordEntry[getNum - 1].recordEndPlsn - lastRecyclePlsn >= WAL_READ_BUFFER_BLOCK_SIZE) {
            uint64 recyclePlsn = GetWorkersRedoFinishedPlsn();
            m_redoReadBuffer->RecycleReadBuffer(recyclePlsn);
            lastRecyclePlsn = recyclePlsn;
        }
        if (unlikely(m_dispatchQueue->PopN(getNum) != DSTORE_SUCC)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Dispatch worker m_dispatchQueue popN error!"));
            break;
        }
    }

    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery dispatch worker exit while loop.",
        m_pdbId, m_walId));

DISPATCH_END:
    /* Redo workers should only be shut down after the wal records have been appended */
    for (uint32 i = 0; i < m_redoWorkerNum; i++) {
        if (m_parallelBatchPutEntrys[i].entryNum > 0) {
            m_parallelRedoWorkers[i]->AppendWalRecord(&m_parallelBatchPutEntrys[i].redoWalRecordEntry[0],
                m_parallelBatchPutEntrys[i].entryNum, __FUNCTION__, __LINE__);
        }
    }
    m_dispatchFinished.store(true, std::memory_order_release);

    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery dispatch worker queue max usage %u",
        m_pdbId, m_walId, m_dispatchQueue->GetMaxUsage()));
    WalUtils::HandleWalThreadCpuUnbind("DispatchWorker");
    g_storageInstance->UnregisterThread();
    DstoreSetMemoryInControl();
}

void WalRecovery::DispatchDirtyPageWorkerMain()
{
    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister();
    WalUtils::HandleWalThreadCpuBind("DispatchDirtyPageWorker");
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]DispatchDirtyPageWorkerMain start", m_pdbId, m_walId));
    WalRecordInfo *recordEntry;
    uint32 getNum = 0;
    WalRecordInfoListNode walRecordInfoListNode = {};
    WalRecordForPage decompressedRecordForPage{};
    m_batchPutParaForDirtyPage =
        static_cast<BatchPutParaForDirtyPageSetQue *>(DstorePalloc0(sizeof(BatchPutParaForDirtyPageSetQue)));
    if (STORAGE_VAR_NULL(m_batchPutParaForDirtyPage)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Dispatch palloc batchPutPara fail!"));
        goto DISPATCH_END;
    }
    while (true) {
        if (m_dispatchQueue->GetAll<WalRecordInfo *>(&recordEntry, &getNum) != DSTORE_SUCC) {
            GaussUsleep(WAIT_DISPATCH_QUEUE_MICROSEC);
            continue;
        }
        for (uint32 i = 0; i < getNum; i++) {
            if (recordEntry[i].ctx.recordEndPlsn == INVALID_PLSN) {
                goto DISPATCH_END;
            }
            walRecordInfoListNode = {recordEntry[i].startPlsn, recordEntry[i].walRecord->m_type,
                recordEntry[i].ctx, INVALID_WAL_GLSN, recordEntry[i].walRecord->m_size, EXCLUDED_FILE_VERSION};
            switch (recordEntry[i].walRecord->m_type) {
                case WAL_TBS_INIT_MULTIPLE_DATA_PAGES: {
                    SplitMultipleDataPagesWalRecordIntoDirtyPageSet(recordEntry[i].walRecord, &walRecordInfoListNode);
                    break;
                }
                case WAL_TBS_INIT_BITMAP_PAGES: {
                    SplitInitBitmapPagesWalRecordIntoDirtyPageSet(recordEntry[i].walRecord, &walRecordInfoListNode);
                    break;
                }
                case WAL_TBS_CREATE_TABLESPACE:
                case WAL_TBS_CREATE_DATA_FILE:
                case WAL_TBS_ADD_FILE_TO_TABLESPACE:
                case WAL_TBS_DROP_TABLESPACE:
                case WAL_TBS_DROP_DATA_FILE:
                case WAL_BARRIER_CSN:
                case WAL_TBS_ALTER_TABLESPACE:
                case WAL_SYSTABLE_WRITE_BUILTIN_RELMAP: {
                    /* no need to put ddl record into dirty page set */
                    break;
                }
                default: {
                    const WalRecordForPage *recordForPage =
                        DecompressForBuildDirtyPageSet(&decompressedRecordForPage, recordEntry[i].walRecord);
                    PageId entryPageId = recordForPage->m_pageId;
                    uint64 entryGlsn = recordForPage->m_pagePreWalId != m_walId ? recordForPage->m_pagePreGlsn + 1
                                                                                : recordForPage->m_pagePreGlsn;
                    walRecordInfoListNode.glsn = entryGlsn;
                    walRecordInfoListNode.fileVersion = recordForPage->m_filePreVersion;
                    BatchPutDirtyPageEntry(entryPageId, &walRecordInfoListNode);
                    break;
                }
            }
            m_getDirtyPageReadBuffer->RecycleReadBuffer(recordEntry[i].ctx.recordEndPlsn);
        }
        if (STORAGE_FUNC_FAIL(m_dispatchQueue->PopN(getNum))) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Dispatch worker m_dispatchQueue popN error!"));
            break;
        }
    }

DISPATCH_END:
    for (uint32 i = 0; i < PAGE_WAL_RECORD_HTAB_PUTTER_NUM; i++) {
        if (m_batchPutParaForDirtyPage != nullptr) {
            if (m_batchPutParaForDirtyPage->pageNum[i] == MAX_PRE_READ_QUE_BATCH_NUM) {
                m_pageWalRecordDispatchQueues[i]->PutN<DirtyPageSetQueEntry *>(
                    &m_batchPutParaForDirtyPage->dirtyPageSetQueEntries[i][0], MAX_PRE_READ_QUE_BATCH_NUM);
                m_batchPutParaForDirtyPage->pageNum[i] = 0;
            }
            m_batchPutParaForDirtyPage->
                dirtyPageSetQueEntries[i][m_batchPutParaForDirtyPage->pageNum[i]++].pageId = INVALID_PAGE_ID;
            m_pageWalRecordDispatchQueues[i]->PutN<DirtyPageSetQueEntry *>(
                &m_batchPutParaForDirtyPage->dirtyPageSetQueEntries[i][0],
                m_batchPutParaForDirtyPage->pageNum[i]);
        }
        m_buildDirtyPageSetWorkerThd[i]->join();
        delete m_buildDirtyPageSetWorkerThd[i];
        m_buildDirtyPageSetWorkerThd[i] = nullptr;
        m_pageWalRecordDispatchQueues[i]->Destroy();
        DstorePfreeExt(m_pageWalRecordDispatchQueues[i]);
    }
    DstorePfreeExt(m_batchPutParaForDirtyPage);
    WalUtils::HandleWalThreadCpuUnbind("DispatchWorker");
    g_storageInstance->UnregisterThread();
}

inline void WalRecovery::BatchPutDirtyPageEntry(const PageId &pageId, const WalRecordInfoListNode *listNode)
{
    uint32 hashcode = get_hash_value(m_pageWalRecordInfoListHtab.m_pageWalRecordInfoListHtab, &pageId);
    uint32 workerId = hashcode % PAGE_WAL_RECORD_HTAB_PUTTER_NUM;
    m_batchPutParaForDirtyPage->
        dirtyPageSetQueEntries[workerId][m_batchPutParaForDirtyPage->pageNum[workerId]].pageId = pageId;
    m_batchPutParaForDirtyPage->
        dirtyPageSetQueEntries[workerId][m_batchPutParaForDirtyPage->pageNum[workerId]++].listNode = *listNode;
    if (m_batchPutParaForDirtyPage->pageNum[workerId] > MAX_PRE_READ_QUE_BATCH_NUM) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery BatchPutDirtyPageEntry get invalid batch num %u.",
            m_pdbId, m_walId, m_batchPutParaForDirtyPage->pageNum[workerId]));
    }
    if (m_batchPutParaForDirtyPage->pageNum[workerId] == MAX_PRE_READ_QUE_BATCH_NUM) {
        m_pageWalRecordDispatchQueues[workerId]->PutN<DirtyPageSetQueEntry *>(
            &m_batchPutParaForDirtyPage->dirtyPageSetQueEntries[workerId][0], MAX_PRE_READ_QUE_BATCH_NUM);
        m_batchPutParaForDirtyPage->pageNum[workerId] = 0;
    }
}

void WalRecovery::GetPageWalRecordInfoList(PageId pageId, PageWalRecordInfoListEntry **entry)
{
    if (entry == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("GetPageWalRecordInfoList invalid param"));
        return;
    }
    StorageAssert(LWLockHeldByMe(&m_getPageWalRecordsLock));
    *entry = m_pageWalRecordInfoListHtab.Get(pageId);
}

WalRecord *WalRecovery::GetWalRecordForPageByPlsn(uint64 startPlsn, uint16 walRecordSize, const PageId pageId,
                                                  bool *needFree)
{
    StorageAssert(LWLockHeldByMe(&m_getPageWalRecordsLock));
    if (m_getDirtyPageReadBuffer == nullptr) {
        return nullptr;
    }
    return m_getDirtyPageReadBuffer->GetWalRecordForPageByPlsn(startPlsn, walRecordSize, pageId, needFree);
}

void WalRecovery::WalRecordInfoListHtabBuildWorkerMain(uint32 workerId)
{
    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister(g_defaultPdbId);
    WalUtils::HandleWalThreadCpuBind("DispatchDirtyPageWorker");
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecordInfoListHtabBuildWorkerMain %u start", m_pdbId, m_walId, workerId));
    DirtyPageSetQueEntry *entrys;
    uint32 getNum = 0;
    BlockSpscQueue *queue = m_pageWalRecordDispatchQueues[workerId];
    while (true) {
        if (queue->GetAll<DirtyPageSetQueEntry*>(&entrys, &getNum) != DSTORE_SUCC) {
            GaussUsleep(WAIT_DISPATCH_QUEUE_MICROSEC);
            continue;
        }

        for (uint32 i = 0; i < getNum; i++) {
            if (unlikely(entrys[i].pageId == INVALID_PAGE_ID)) {
                goto BUILD_END;
            }
            m_pageWalRecordInfoListHtab.Add(entrys[i].pageId, &entrys[i].listNode);
        }

        if (unlikely(queue->PopN(getNum) != DSTORE_SUCC)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Dispatch worker m_dispatchQueue popN error!"));
            break;
        }
    }
BUILD_END:
    WalUtils::HandleWalThreadCpuUnbind("DispatchDirtyPageWorker");
    g_storageInstance->UnregisterThread();
}

void WalRecovery::StartDirtyPageSetBuildWorker()
{
    m_pageWalRecordInfoListHtab.Init(m_memoryContext);
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery BuildDirtyPageSet DispatchQueue Init start", m_pdbId, m_walId));
    for (int i = 0; i < PAGE_WAL_RECORD_HTAB_PUTTER_NUM; i++) {
        m_pageWalRecordDispatchQueues[i] = static_cast<BlockSpscQueue *>(DstorePalloc0(sizeof(BlockSpscQueue)));
        if (m_pageWalRecordDispatchQueues[i] == nullptr ||
            STORAGE_FUNC_FAIL(m_pageWalRecordDispatchQueues[i]->Init(DISPATCH_WORKER_QUE_CAPACITY,
                                                                     sizeof(DirtyPageSetQueEntry), nullptr))) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("DispatchQueue Init error!"));
        }
        m_buildDirtyPageSetWorkerThd[i] = new std::thread(&WalRecovery::WalRecordInfoListHtabBuildWorkerMain, this, i);
    }
    m_dispatchQueue = static_cast<BlockSpscQueue *>(DstorePalloc0(sizeof(BlockSpscQueue)));
    if (m_dispatchQueue == nullptr ||
        STORAGE_FUNC_FAIL(m_dispatchQueue->Init(DISPATCH_WORKER_QUE_CAPACITY, sizeof(WalRecordInfo), nullptr))) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("DispatchQueue Init error!"));
    }
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery BuildDirtyPageSet GetDirtyPageReadBuffer Init start", m_pdbId, m_walId));
    m_getDirtyPageReadBuffer = DstoreNew(m_memoryContext) WalReadBuffer(m_memoryContext, m_walStream, m_redoMode);
    if (STORAGE_VAR_NULL(m_getDirtyPageReadBuffer)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("GetDirtyPageReadBuffer allocation error!"));
    }
    m_dispatchWorkerThd = new std::thread(&WalRecovery::DispatchDirtyPageWorkerMain, this);
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("[PDB:%u WAL:%lu]WalRecovery StartDirtyPageSetBuildWorker finish", m_pdbId, m_walId));
}

void WalRecovery::StopRedoReadBufferLoadWorker()
{
    if (m_redoReadBuffer != nullptr) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
            ErrMsg("StopRedoReadBufferLoadWorker() called."));
        m_redoReadBuffer->StopLoadWorker();
    } else {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("StopRedoReadBufferLoadWorker sees m_redoReadBuffer == nullptr"));
    }
}

uint64 WalRecovery::GetMaxParseredPlsn() const
{
    if (!m_enableParallelRedo) {
        return m_curRedoFinishedPlsn ;
    }
    if (m_dispatchWorkerThd == nullptr) {
        return INVALID_PLSN;
    }
    uint64 maxDispatchedPlsn = INVALID_PLSN;
    for (uint32 i = 0; i < m_redoWorkerNum; i++) {
        uint64 dispatchplsn = GsAtomicReadU64(&m_redoWorkersMaxDispatchedPlsn[i]);
        maxDispatchedPlsn = maxDispatchedPlsn > dispatchplsn ? maxDispatchedPlsn : dispatchplsn;
    }
    return maxDispatchedPlsn;
}

uint64 WalRecovery::GetCurrentRedoDonePlsn() const
{
    if (!m_enableParallelRedo || GetWalRecoveryStage() >= WalRecoveryStage::RECOVERY_REDO_DONE) {
        return m_curRedoFinishedPlsn;
    }
    return GetWorkersRedoFinishedPlsn();
}

RetStatus WalRecovery::BuildDirtyPageSet(uint64 startPlsn, uint64 endPlsn)
{
    AutoMemCxtSwitch autoMemCxtSwitch(m_memoryContext);
    std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> startBuildSet =
            std::chrono::high_resolution_clock::now();
    m_recoveryStartPlsn = startPlsn;
    m_recoveryEndPlsn = endPlsn;
    m_diskRecoveryStartPlsn = m_recoveryStartPlsn;
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery BuildDirtyPageSet for wal[%lu %lu] start.",
        m_pdbId, m_walId, m_recoveryStartPlsn, m_recoveryEndPlsn));

    RetStatus result = BuildDirtyPageSetAndPageWalRecordListHtab();
    std::chrono::duration<double> spendSecs = std::chrono::high_resolution_clock::now() - startBuildSet;
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery BuildDirtyPageSet for wal[%lu %lu] finish and spend time %f seconds.",
            m_pdbId, m_walId, m_recoveryStartPlsn, m_recoveryEndPlsn, spendSecs.count()));
    return result;
}

RetStatus WalRecovery::FlushDirtyPages()
{
    AutoMemCxtSwitch autoMemCxtSwitch(m_memoryContext);

    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery FlushDirtyPages between [%lu, %lu] start.",
        m_pdbId, m_walId, m_recoveryStartPlsn, m_recoveryEndPlsn));
    /* Step 1: wait dirty page set build finished wal */
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery FlushDirtyPages WaitBuildDirtyPageSet start.",
        m_pdbId, m_walId));
    std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> t1 =
            std::chrono::high_resolution_clock::now();
    while (!IsDirtyPageSetBuilt()) {
        GaussUsleep(SLEEP_MILLISECONDS);
    }
    std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> t2 =
        std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> spendSecs = t2 - t1;
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery FlushDirtyPages WaitBuildDirtyPageSet finish and spend time %f seconds. "
        "flush dirty page start", m_pdbId, m_walId, (spendSecs).count()));

    /* Step 2: flush all dirty pages in dirty page set */
    if (STORAGE_FUNC_FAIL(FlushAllDirtyPages())) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]WalRecovery FlushDirtyPages in dirty page set failed.", m_pdbId, m_walId));
        return DSTORE_FAIL;
    }
    std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> t3 =
        std::chrono::high_resolution_clock::now();
    spendSecs = t3 - t2;
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery FlushDirtyPages finish and spend time %f seconds.",
        m_pdbId, m_walId, spendSecs.count()));
    return DSTORE_SUCC;
}

const char *WalRecovery::WalRecoveryStageToStr(WalRecoveryStage stage)
{
    switch (stage) {
        case WalRecoveryStage::RECOVERY_NO_START:
            return "RECOVERY_NO_START";
        case WalRecoveryStage::RECOVERY_STARTING:
            return "RECOVERY_STARTING";
        case WalRecoveryStage::RECOVERY_GET_DIRTY_PAGE_SET:
            return "RECOVERY_GET_DIRTY_PAGE_SET";
        case WalRecoveryStage::RECOVERY_GET_DIRTY_PAGE_SET_DONE:
            return "RECOVERY_GET_DIRTY_PAGE_SET_DONE";
        case WalRecoveryStage::RECOVERY_REDO_STARTED:
            return "RECOVERY_REDO_STARTED";
        case WalRecoveryStage::RECOVERY_REDO_STOPPING:
            return "RECOVERY_REDO_STOPPING";
        case WalRecoveryStage::RECOVERY_REDO_DONE:
            return "RECOVERY_REDO_DONE";
        case WalRecoveryStage::RECOVERY_DIRTY_PAGE_FLUSHED:
            return "RECOVERY_DIRTY_PAGE_FLUSHED";
        default:
            return "INVALID";
    }
}

const char *WalRecovery::RedoModeToStr(RedoMode mode)
{
    switch (mode) {
        case RedoMode::RECOVERY_REDO:
            return "RECOVERY_REDO";
        case RedoMode::PDB_STANDBY_REDO:
            return "PDB_STANDBY_REDO";
        default:
            return "INVALID";
    }
}

PageWalRecordInfoListHtab::PageWalRecordInfoListHtab()
    : m_pageWalRecordInfoListHtab(nullptr),
    m_inited(false),
    m_memoryContext(nullptr)
{
}

PageWalRecordInfoListHtab::~PageWalRecordInfoListHtab()
{
    Destroy();
    m_pageWalRecordInfoListHtab = nullptr;
    m_memoryContext = nullptr;
}

void PageWalRecordInfoListHtab::Init(DstoreMemoryContext memoryContext)
{
    DstoreMemoryContext hashMemCtx = DstoreAllocSetContextCreate(
        memoryContext,
        "PageWalRecordInfoListHtab MemCtx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    if (hashMemCtx == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Create PageWalRecordInfoListHtab MemCtx failed."));
    }
    m_memoryContext = hashMemCtx;
    constexpr uint32 expectPageNum = 500000;
    HASHCTL info;
    info.keysize = sizeof(PageId);
    info.entrysize = sizeof(PageWalRecordInfoListEntry);
    info.dsize = hash_select_dirsize(expectPageNum);
    info.num_partitions = PAGE_WAL_RECORD_HTAB_PUTTER_NUM;
    info.hash = tag_hash;
    info.hcxt = hashMemCtx;

    m_pageWalRecordInfoListHtab = hash_create("PageWalRecordInfoListHtab", expectPageNum, &info,
                                              HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_DIRSIZE |
                                              HASH_SHRCTX | HASH_PARTITION);
    if (unlikely(m_pageWalRecordInfoListHtab == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("Cannot create PageWalRecordInfoListHtab when WalRecovery Init."));
    }

    m_inited = true;
}

void PageWalRecordInfoListHtab::Add(PageId pageId, const WalRecordInfoListNode *walRecordInfoListNode)
{
    bool isFound;
    PageWalRecordInfoListEntry *findEntry = static_cast<PageWalRecordInfoListEntry *>(
        hash_search(m_pageWalRecordInfoListHtab, &pageId, HASH_ENTER, &isFound));
    if (unlikely(findEntry == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Cannot get valid PageWalRecordInfoListEntry."));
    }
    if (unlikely(!isFound)) {
        constexpr uint16 expectArraySize = 64;
        findEntry->pageId = pageId;
        findEntry->walRecordInfoList.Init(expectArraySize, nullptr);
    }
    findEntry->walRecordInfoList.Append(*walRecordInfoListNode);
}

PageWalRecordInfoListEntry *PageWalRecordInfoListHtab::Get(PageId pageId) const
{
    if (!m_inited) {
        return nullptr;
    }
    bool isFound;
    return static_cast<PageWalRecordInfoListEntry *>(
        hash_search(m_pageWalRecordInfoListHtab, &pageId, HASH_FIND, &isFound));
}

void PageWalRecordInfoListHtab::Destroy()
{
    if (m_pageWalRecordInfoListHtab != nullptr) {
        HASH_SEQ_STATUS hashStatus;
        PageWalRecordInfoListEntry *entry = nullptr;
        hash_seq_init(&hashStatus, m_pageWalRecordInfoListHtab);
        while ((entry = static_cast<PageWalRecordInfoListEntry *>(hash_seq_search(&hashStatus))) != nullptr) {
            entry->walRecordInfoList.Destroy();
        }
        hash_destroy(m_pageWalRecordInfoListHtab);
        m_pageWalRecordInfoListHtab = nullptr;
    }
    m_memoryContext = nullptr;
    m_inited = false;
}
WalDirtyPageEntry *PageWalRecordInfoListHtab::BuildWalDirtyPageEntryArray(long &retArraySize)
{
    if (unlikely(!m_inited.load())) {
        retArraySize = 0;
        return nullptr;
    }

    HASH_SEQ_STATUS hashStatus;
    PageWalRecordInfoListEntry *entry = nullptr;

    long entrySize = hash_get_num_entries(m_pageWalRecordInfoListHtab);
    uint32 arraySize = static_cast<unsigned long>(entrySize) * sizeof(WalDirtyPageEntry);
    WalDirtyPageEntry *dirtyPageEntryArray = nullptr;
    if (arraySize <= MaxAllocSize) {
        dirtyPageEntryArray = static_cast<WalDirtyPageEntry *>(DstorePalloc(arraySize));
    } else {
        dirtyPageEntryArray =
            static_cast<WalDirtyPageEntry *>(DstoreMemoryContextAllocHugeSize(g_dstoreCurrentMemoryContext, arraySize));
    }
    if (dirtyPageEntryArray == nullptr) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("palloc space of dirtyPageEntryArray fail."));
        return nullptr;
    }
    hash_seq_init(&hashStatus, m_pageWalRecordInfoListHtab);
    long idx = 0;
    while ((entry = static_cast<PageWalRecordInfoListEntry *>(hash_seq_search(&hashStatus))) != nullptr) {
        WalRecordInfoListNode *listNode = (entry->walRecordInfoList)[entry->walRecordInfoList.Length() - 1];
        StorageReleasePanic(listNode == nullptr, MODULE_WAL,
                            ErrMsg("BuildWalDirtyPageEntryArray get invalid listnode."));
        dirtyPageEntryArray[idx++] = {entry->pageId, listNode->redoContext.walId, listNode->redoContext.recordEndPlsn,
                                      listNode->glsn};
    }
    retArraySize = entrySize;
    return dirtyPageEntryArray;
}

BigArray::BigArray(Size chunkSizeArg, DstoreMemoryContext context)
    : memContext(context), chunkSize(chunkSizeArg), chunkSizeBits(0), size(0),
      head{ { nullptr } }, tailNode(nullptr)
{
}

BigArray::~BigArray()
{
    Destroy();
}

void BigArray::Init(Size chunkSizeArg, DstoreMemoryContext context)
{
    memContext = context;
    chunkSize = chunkSizeArg;
    chunkSizeBits = 0;
    size = 0;
    head = { nullptr };
    tailNode = nullptr;
    while (!(chunkSize & (1 << chunkSizeBits))) {
        chunkSizeBits++;
    }
}

void BigArray::Destroy()
{
    slist_mutable_iter iter;
    slist_foreach_modify(iter, &head) {
        char* tmp = STATIC_CAST_PTR_TYPE(iter.cur, char *);
        Node *data = STATIC_CAST_PTR_TYPE(tmp - OFFSETOF(Node, listNode), Node *);
        SListDeleteCurrent(&iter);
        if (memContext != nullptr) {
            DstorePfreeExt(data);
        } else {
            free(data);
        }
    }
}

RetStatus BigArray::AllocNode(Node **node)
{
    if (*node != nullptr) {
        return DSTORE_SUCC;
    }
    if (memContext != nullptr) {
        *node = static_cast<Node *>(
            DstoreMemoryContextAlloc(memContext, chunkSize * sizeof(WalRecordInfoListNode) + sizeof(Node)));
    } else {
        *node = static_cast<Node *>(malloc(chunkSize * sizeof(WalRecordInfoListNode) + sizeof(Node)));
    }

    if (STORAGE_VAR_NULL(*node)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Fail to alloc mem for BigArray array"));
        return DSTORE_FAIL;
    }
    (*node)->listNode.next = nullptr;
    return DSTORE_SUCC;
}

void BigArray::Append(const WalRecordInfoListNode& data)
{
    if (STORAGE_VAR_NULL(tailNode)) {
        Node *node = nullptr;
        while (STORAGE_FUNC_FAIL(AllocNode(&node))) {
            GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        }
        if (STORAGE_VAR_NULL(node)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Fail to alloc node for BigArray array"));
            return;
        }
        SListInsertAfter(&head.head, &node->listNode);
        tailNode = node;
    } else if (unlikely((size & (chunkSize - 1)) == 0)) {
        Node *node = nullptr;
        while (STORAGE_FUNC_FAIL(AllocNode(&node))) {
            GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        }
        SListInsertAfter(&tailNode->listNode, &node->listNode);
        tailNode = node;
    }

    tailNode->array[size & (chunkSize - 1)] = data;
    size++;
}

WalRecordInfoListNode *BigArray::operator[](Size index) const
{
    if (index >= size) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("BigArray array out of bounds index %lu size %lu", index, size));
        return nullptr;
    }
    slist_node* cur = head.head.next;
    Size times = index >> chunkSizeBits;
    while (times != 0) {
        cur = cur->next;
        times--;
    }
    char* tmp = STATIC_CAST_PTR_TYPE(cur, char *);
    Node *node = STATIC_CAST_PTR_TYPE((tmp - OFFSETOF(Node, listNode)), Node *);
    return &(node->array[index & (chunkSize - 1)]);
}

Size BigArray::Length() const
{
    return size;
}

BigArray::Iter::Iter()
    : size(0), curIndex(0), curNode(nullptr),
      chunkSize(0), chunkSizeBits(0), reachEnd(false)
{
}

BigArray::Iter::Iter(BigArray& array, Size index)
    : size(array.size), curIndex(index), curNode(nullptr),
      chunkSize(array.chunkSize), chunkSizeBits(array.chunkSizeBits),
      reachEnd(false)
{
    slist_node* cur = array.head.head.next;
    Size times = index >> chunkSizeBits;
    while (times--) {
        cur = cur->next;
    }
    char* tmp = STATIC_CAST_PTR_TYPE(cur, char *);
    curNode = STATIC_CAST_PTR_TYPE((tmp - OFFSETOF(Node, listNode)), Node *);
}

BigArray::Iter::Iter(Iter& iter, Size index)
    : size(iter.size), curIndex(index), curNode(nullptr),
      chunkSize(iter.chunkSize), chunkSizeBits(iter.chunkSizeBits),
      reachEnd(false)
{
    slist_node* cur = &iter.curNode->listNode;
    Size times = (index - iter.curIndex) >> chunkSizeBits;
    while (times--) {
        cur = cur->next;
    }
    char* tmp = STATIC_CAST_PTR_TYPE(cur, char *);
    curNode = STATIC_CAST_PTR_TYPE((tmp - OFFSETOF(Node, listNode)), Node *);
}

void BigArray::Iter::Init(BigArray& array, Size index)
{
    size = array.size;
    curIndex = index;
    chunkSize = array.chunkSize;
    chunkSizeBits = array.chunkSizeBits;
    reachEnd = false;
    slist_node* cur = array.head.head.next;
    Size times = index >> chunkSizeBits;
    while (times--) {
        cur = cur->next;
    }
    char* tmp = STATIC_CAST_PTR_TYPE(cur, char *);
    curNode = STATIC_CAST_PTR_TYPE((tmp - OFFSETOF(Node, listNode)), Node *);
}

WalRecordInfoListNode* BigArray::Iter::operator*() const
{
    return &(curNode->array[curIndex & (chunkSize - 1)]);
}

void BigArray::Iter::operator++(int)
{
    if (curIndex == size - 1) {
        reachEnd = true;
    }
    curIndex++;
    if (!(curIndex & (chunkSize - 1))) {
        slist_node* next = curNode->listNode.next;
        char* tmp = STATIC_CAST_PTR_TYPE(next, char *);
        curNode = STATIC_CAST_PTR_TYPE((tmp - OFFSETOF(Node, listNode)), Node *);
    }
}

Size BigArray::Iter::Length() const
{
    return size;
}

bool BigArray::Iter::IsEnd() const
{
    return reachEnd;
}
} /* The end of namespace DSTORE */
