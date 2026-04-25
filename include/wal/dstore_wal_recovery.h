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
 * dstore_wal_recovery.h
 *
 *
 *
 * IDENTIFICATION
 *        storage/include/wal/dstore_wal_recovery.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_WAL_RECOVERY_H
#define DSTORE_WAL_RECOVERY_H

#include "common/dstore_datatype.h"
#include "common/algorithm/dstore_dynahash.h"
#include "page/dstore_page_struct.h"
#include "dstore_wal_file_manager.h"
#include "framework/dstore_instance.h"
#include "dstore_wal_parallel_redo_worker.h"
#include "dstore_wal_reader.h"
namespace DSTORE {

constexpr uint32 MAX_REDO_WORKER_NUM = 500;
constexpr uint32 MAX_REDO_QUE_BATCH_NUM = 100;
constexpr uint32 INIT_TEMP_BUFF_SIZE = 10240;
constexpr uint32 MAX_PRE_READ_QUE_BATCH_NUM = 100;

constexpr uint16 PAGE_WAL_RECORD_HTAB_PUTTER_NUM = 8;

enum class RedoMode {
    RECOVERY_REDO,
    PDB_STANDBY_REDO
};

struct OnDemandRedoWalRecordEntry {
    WalRecordRedoContext redoContext;
    WalRecord walRecord[];
};

struct WalRecordInfoListNode {
    uint64 startPlsn;
    WalType walType;
    WalRecordRedoContext redoContext;
    uint64 glsn;
    uint16 walRecordSize;
    uint64 fileVersion;
};

struct DirtyPageSetQueEntry {
    PageId pageId;
    WalRecordInfoListNode listNode;
};

struct WalDirtyPageEntry {
    PageId pageId;
    WalId walId;
    uint64 plsn;
    uint64 glsn;

    bool operator == (const WalDirtyPageEntry &entry) const
    {
        if (pageId == entry.pageId && walId == entry.walId &&
            plsn == entry.plsn && glsn == entry.glsn) {
            return true;
        }
        return false;
    }

    bool operator > (const WalDirtyPageEntry &entry) const
    {
        if (glsn > entry.glsn) {
            return true;
        } else if (glsn == entry.glsn && plsn > entry.plsn) {
            StorageAssert(walId == entry.walId);
            return true;
        }
        return false;
    }
};

struct BatchPutParaForDirtyPageSetQue {
    DirtyPageSetQueEntry dirtyPageSetQueEntries[PAGE_WAL_RECORD_HTAB_PUTTER_NUM][MAX_PRE_READ_QUE_BATCH_NUM];
    uint32 pageNum[PAGE_WAL_RECORD_HTAB_PUTTER_NUM];
};

struct WalRecordInfo {
    uint64 startPlsn;
    WalRecordRedoContext ctx;
    const WalRecord *walRecord;
};

class BigArray {
public:
    BigArray(Size chunkSizeArg, DstoreMemoryContext context);
    DISALLOW_COPY_AND_MOVE(BigArray);
    ~BigArray();
    void Init(Size chunkSizeArg, DstoreMemoryContext context);
    void Destroy();
    void Append(const WalRecordInfoListNode& data);
    Size Length() const;

    WalRecordInfoListNode *operator[](Size index) const;

    struct Node {
        slist_node listNode;
        WalRecordInfoListNode array[];
    };
    RetStatus AllocNode(Node **node);
    struct Iter {
        Iter();
        Iter(BigArray& array, Size index);
        /* index must greater than iter.curIndex. */
        Iter(Iter& iter, Size index);
        void Init(BigArray& array, Size index);

        ~Iter()
        {
            curNode = nullptr;
            reachEnd = false;
        }

        Size Length() const;
        bool IsEnd() const;

        WalRecordInfoListNode* operator*() const;
        void operator++(int);

        Size size;
        Size curIndex;
        Node* curNode;
        Size chunkSize;
        Size chunkSizeBits;
        bool reachEnd;
    };

private:
    DstoreMemoryContext memContext;
    Size chunkSize; /* Must be a power of 2 */
    Size chunkSizeBits;
    Size size;
    slist_head head;
    Node* tailNode;
};

struct PageWalRecordInfoListEntry {
    PageId pageId;
    BigArray walRecordInfoList;
};

class PageWalRecordInfoListHtab {
public:
    explicit PageWalRecordInfoListHtab();
    ~PageWalRecordInfoListHtab();

    void Init(DstoreMemoryContext memoryContext);
    void Add(PageId pageId, const WalRecordInfoListNode *walRecordInfoListNode);
    PageWalRecordInfoListEntry *Get(PageId pageId) const;
    WalDirtyPageEntry *BuildWalDirtyPageEntryArray(long &retArraySize);
    void Destroy();

    HTAB *m_pageWalRecordInfoListHtab;

private:
    std::atomic_bool m_inited;
    DstoreMemoryContext m_memoryContext;
};

enum class WalRecoveryStage {
    RECOVERY_NO_START,
    RECOVERY_STARTING,
    RECOVERY_GET_DIRTY_PAGE_SET,
    RECOVERY_GET_DIRTY_PAGE_SET_DONE,
    RECOVERY_REDO_STARTED,
    /* wal replay runs under RECOVERY_REDO_STARTED */
    RECOVERY_REDO_STOPPING,
    RECOVERY_REDO_DONE,
    RECOVERY_DIRTY_PAGE_FLUSHED,
};

struct WalRedoStatisticInfo {
    uint32 workerNum;
    uint64 walRedoStartPlsn;
    uint64 walRedoTotalNeed;
    double totalRecoveryTime;
    uint64 walRedoCurPlsn[MAX_REDO_WORKER_NUM];

    double walRedoTime[MAX_REDO_WORKER_NUM];
    double walTypeRedoTime[WAL_TYPE_BUTTOM][MAX_REDO_WORKER_NUM];
    uint32 walTypeRedoCnt[WAL_TYPE_BUTTOM][MAX_REDO_WORKER_NUM];
    uint64 walTypeRedoSize[WAL_TYPE_BUTTOM][MAX_REDO_WORKER_NUM];
};

struct WalRedoTypeStatisticInfo {
    double walTypeRedoTime[WAL_TYPE_BUTTOM];
    uint32 walTypeRedoCnt[WAL_TYPE_BUTTOM];
};

struct RedoWalRecordBuffPara {
    BufferDesc **bufferDesc;
    uint32 workerId;
    bool redoSingleFlag;
    bool needMarkDirty;
    BuffForDecompress *buffForDecompress;
};

struct BatchPutParaForWalReplay {
    RedoWalRecordEntry redoWalRecordEntry[MAX_REDO_QUE_BATCH_NUM];
    uint32 entryNum;
};

enum class WalRecordReplayType {
    NO_NEED_TO_REPLAY, /* This WalRecord's content is already on the page and don't need replay */
    REPLAYABLE, /* This WalRecord can replay on this page */
    NOT_REPLAYABLE, /* This WalRecord can not replay on this page because other WalRecord need play first */
};

struct ParaForReadBuffer {
    PageId pageId;
    uint64 filePreVersion;
    WalId walId;
    uint64 plsn;
};

class WalStream;
class WalReadBuffer;

class WalRecovery : public BaseObject {
public:
    WalRecovery(DstoreMemoryContext memoryContext, WalStream *walStream, uint64 walFileSize, WalId walId);
    ~WalRecovery();
    RetStatus Init(RedoMode redoMode, PdbId pdbId, uint64 term = 0);
    RetStatus Recovery();
    void Destroy() noexcept;

    RetStatus RedoSingle(WalRecordRedoContext *redoCtx, const WalRecord *walRecord,
        uint32 workerId, BuffForDecompress *buffForDecompress);
    RetStatus RedoSingleForTbs(WalRecordRedoContext *redoCtx, const WalRecord *walRecord, uint32 workerId);
    RetStatus RedoBatch(WalBatchRecoveryPara *para, uint32 workerId, BuffForDecompress *buffForDecompress,
                        bool printDdlLog = true);
    RetStatus RedoBatchForPage(WalBatchRecoveryPara *para, uint32 workerId, BuffForDecompress *buffForDecompress);
    RetStatus RedoBatchForDdl(WalBatchRecoveryPara *para, bool printDdlLog);
    void ProcessBarrierRecord(const WalBarrierCsn *record, const uint64 recordEndPlsn, bool needReport = false);
    bool IsAllNullRecord(RedoWalRecordEntry *recordEntry, uint32 getNum);
    bool IsDispatchFinish();
    bool IsAllRedoQueueEmpty();
    static bool IsLsnError(const WalRecordLsnInfo &pageLsn, const WalRecordLsnInfo &recordPreLsn);
    static WalRecordReplayType GetWalRecordReplayType(const BufferTag bufTag, Page *page,
                                                      const WalRecordLsnInfo &pagePreRecordLsnInfo,
                                                      const WalRecordLsnInfo &recordLsnInfo, bool reportWarning = true);
    static WalRecordReplayType WalRecordCheckFileVersion(const PdbId pdbId, const FileId fileId,
                                                         uint64 recordVersionInfo, uint64 *versionInFile);
    static WalRecordReplayType WalRecordCheckTbsVersion(const PdbId pdbId, const TablespaceId tbsId,
                                                        uint64 recordVersionInfo, uint64 *versionInFile);
    bool IsDirtyPageSetBuilt() const;
    void WaitDirtyPageSetBuilt() const;
    RetStatus BuildDirtyPageSetAndPageWalRecordListHtab();
    static void InitRedoStatisticInfoTotalNeedPlsn(uint64 lastGroupPlsn);
    static RetStatus GetAllRedoStatisticInfo(WalRedoStatisticInfo *redoStatisticInfo);
    static bool IsSupportCompress(WalType recordType);
    static WalRecordForPage *DecompressProc(const WalRecord *walRecord, BuffForDecompress *buffForDecompress);
    static RetStatus RedoWalRecordByType(WalRecordRedoContext *redoCtx, const WalRecord *walRecord,
                                         BufferDesc *bufferDesc, WalType walType);
    static RetStatus OnDemandRedoWalRecord(WalRecordRedoContext *redoCtx, WalRecord *walRecord,
                                           BufferDesc *bufferDesc);
    // this pointer will be free by PD recovery
    WalDirtyPageEntry *GetDirtyPageEntryArrayCopy(long &retArraySize) const;
    const PageWalRecordInfoListHtab *GetWalRecordInfoListHtab() const;
    void TryDispatchWhenNoRecordsComing();
    void UpdateTrxNeedRollbackBarrier(const WalRecordRedoContext *redoCtx, const WalRecordForPage *walRecord);
    void SetBarrier(WalBarrier barrier);

    uint64 GetLastGroupEndPlsn() const
    {
        return m_lastGroupEndPlsn;
    }

    WalRecoveryStage GetWalRecoveryStage() const
    {
        GS_READ_BARRIER();
        return m_stage.load();
    }

    void SetWalRecoveryStage(WalRecoveryStage stage)
    {
        m_stage.store(stage, std::memory_order_release);
        GS_WRITE_BARRIER();
    }

    uint64 GetTerm() const
    {
        return m_term;
    }

    uint64 GetRecoveryStartPlsn() const
    {
        return m_recoveryStartPlsn;
    }

    uint64 GetRecoveryEndPlsn() const
    {
        return m_recoveryEndPlsn;
    }

    uint64 GetDiskRecoveryStartPlsn() const
    {
        return m_diskRecoveryStartPlsn;
    }

    long GetDirtyPageArraySize() const
    {
        return m_dirtyPageEntryArraySize;
    }

    CommitSeqNo GetBarrierCsn() const
    {
        return m_barrierCsn.load();
    }

    PdbSyncMode GetBarrierSyncMode() const
    {
        return m_curSyncMode.load();
    }

    uint64 GetBarrierEndPlsn() const
    {
        return m_curBarrierEndPlsn.load();
    }

    bool SwapWalRecoveryStage(WalRecoveryStage &expected, WalRecoveryStage newStage)
    {
        return m_stage.compare_exchange_strong(expected, newStage);
    }

    void GetPageWalRecordInfoList(PageId pageId, PageWalRecordInfoListEntry **entry);

    WalRecord *GetWalRecordForPageByPlsn(uint64 startPlsn, uint16 walRecordSize, const PageId pageId, bool *needFree);

    inline bool TryAcquirePageWalRecordsLwlock(LWLockMode mode)
    {
        return DstoreLWLockConditionalAcquire(&m_getPageWalRecordsLock, mode);
    }

    inline void ReleasePageWalRecordsLwlock()
    {
        StorageAssert(LWLockHeldByMe(&m_getPageWalRecordsLock));
        LWLockRelease(&m_getPageWalRecordsLock);
    }

    void StopRedoReadBufferLoadWorker();

    inline RedoMode GetRedoMode() const
    {
        return m_redoMode;
    }
    inline PdbId GetPdbId() const
    {
        return m_pdbId;
    }

    RetStatus FlushAllDirtyPages();

#ifdef UT
    inline void InitParallelRedoWorker(ParallelRedoWorker *redoWorker, uint32 workerId)
    {
        m_parallelRedoWorkers[workerId] = redoWorker;
    }
    inline bool IsRedoWorkerAlive(uint32 workerId) const
    {
        return (m_parallelRedoWorkers[workerId] != nullptr);
    }
    inline void WaitAllWorkerRedoFinish()
    {
        for (uint32 i = 0; i < m_redoWorkerNum; i++) {
            m_parallelRedoWorkers[i]->WaitRedoFinish();
        }
    }
#endif
    inline bool IsLoadWorkerStarted() const
    {
        if (m_redoReadBuffer == nullptr) {
            return false;
        }

        return m_redoReadBuffer->IsLoadWorkerStarted();
    }

    uint64 GetWorkersRedoFinishedPlsn(bool reportLog = false) const;

    uint64 GetWorkersRecoveryPlsn(bool reportLog = false) const;

    uint64 GetCurrentRedoDonePlsn() const;

    /*
     * Try Build dirty page sets for Wal between startPlsn and endPlsn
     * @param startPlsn is wal recovery start position
     * @param endPlsn is walRecovery end position, and last WalRecord allow to exceed endPlsn
     *
     * @return DSTORE_SUCC if success
     */
    RetStatus BuildDirtyPageSet(uint64 startPlsn, uint64 endPlsn = INVALID_PLSN);

    /*
     * Try flush all dirty pages that Wal between startPlsn and endPlsn
     * @param startPlsn is wal recovery start position
     * @param endPlsn is walRecovery end position, and last WalRecord allow to exceed endPlsn
     */
    RetStatus FlushDirtyPages();

    /*
     * Convert WalRecoveryStage to string
     */
    static const char *WalRecoveryStageToStr(WalRecoveryStage stage);

    /*
     * Convert RedoMode to string
     */
    static const char *RedoModeToStr(RedoMode mode);
    static void StartStatRedo()
    {
        m_statWalTypeInfo.store(true, std::memory_order_release);
    }
    static void StopStatRedo()
    {
        m_statWalTypeInfo.store(false, std::memory_order_release);
    }
    static void ClearRedoStat()
    {
        for (uint16 type = 0; type < static_cast<uint16>(WAL_TYPE_BUTTOM); type++) {
            m_walTypeRedoStatisticInfo.walTypeRedoTime[type] = 0;
            m_walTypeRedoStatisticInfo.walTypeRedoCnt[type] = 0;
        }
    }
    static uint32 GetWalTypeRedoCount(uint16 type)
    {
        return m_walTypeRedoStatisticInfo.walTypeRedoCnt[type];
    }
    static double GetWalTypeRedoAvgTime(uint16 type)
    {
        return m_walTypeRedoStatisticInfo.walTypeRedoCnt[type] == 0
                   ? 0
                   : static_cast<double>(m_walTypeRedoStatisticInfo.walTypeRedoTime[type] /
                                         m_walTypeRedoStatisticInfo.walTypeRedoCnt[type]);
    }
    std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> GetStartRedoTime() const
    {
        return m_startRedoTime;
    }
    uint64 GetWalFileSize() const
    {
        return m_walFileSize;
    }
#ifndef UT
private:
#endif
    void InitRecoveryPlsn(WalId walId, WalCheckPoint *lastWalCheckpoint);
    static void InitRedoStatisticInfo(uint64 recoveryStartPlsn, uint32 redoWorkerNum);
    static void AddRedoStatisticInfo(double time, WalType walType,
        uint64 endPlsn, uint32 workerId, const WalRecord *walRecord);

    static void ReAllocBuffForDecompress(uint32 newBuffSize, uint32 oldBufSize, void **buffer);

    RetStatus RedoWalRecord(WalRecordRedoContext *redoCtx, const WalRecord *walRecord, RedoWalRecordBuffPara *buffPara);
    inline void FillBatchRedoEntry(RedoWalRecordEntry &entry,
        const WalRecordRedoContext &redoCtx, const WalRecord *walRecord) const;
    void AddPlsnSyncer(RedoWalRecordEntry &recordEntry, uint32 &recordEntryNum, uint64 recordEndPlsn);
    RetStatus Redo(uint64 *lastGroupEndPlsn);
    RetStatus RedoLoadWalToBuffer(uint64 loadStartPlsn);
    WalRecordReader *RedoAllocateWalReader(uint64 readStartPlsn);
    bool ReadWalPlsnToMaxFlushed();
    RetStatus RedoLoopReadAndRedoWalRecord(WalRecordReader *walRecordReader,
        uint64 redoStartPlsn, BuffForDecompress *buffForDecompress, uint64 *lastGroupEndPlsn);
    void PrepareBgThreads();
    void CleanUpBgThreads();
    void StartDirtyPageSetBuildWorker();
    inline void SplitMultipleDataPagesWalRecordIntoDirtyPageSet(const WalRecord *walRecord,
                                                                WalRecordInfoListNode *walRecordInfoListNode);
    inline void SplitInitBitmapPagesWalRecordIntoDirtyPageSet(const WalRecord *walRecord,
                                                              WalRecordInfoListNode *walRecordInfoListNode);
    inline void BatchPutDirtyPageEntry(const PageId &pageId, const WalRecordInfoListNode *listNode);
    void SplitAndDispatchInitMultipleDataPageWalRecord(
        WalRecordRedoContext *context, const WalRecord *record);
    void SplitAndDispatchInitBitmapPagesWalRecord(
        WalRecordRedoContext *context, const WalRecord *record);
    inline void DispatchPlsnSyncerToAllWorkers(RedoWalRecordEntry &redoWalRecordEntry);
    inline void DispatchBarrierToPageRedoWorkers(RedoWalRecordEntry &redoWalRecordEntry);
    RetStatus DispatchWalRecord(RedoWalRecordEntry &redoWalRecordEntry);
    inline uint32 GetDispatchDDLRedoWorkerId(const TablespaceId tablespaceId) const;
    inline uint32 GetDispatchPageRedoWorkerId(const PageId pageId) const;
    void DispatchWorkerMain();
    void DispatchDirtyPageWorkerMain();
    void BatchPutToDispatchQueue(RedoWalRecordEntry *recordEntry, uint32 *recordEntryNum, uint32 entryNumThreshold = 1);
    void WalRecordInfoListHtabBuildWorkerMain(uint32 workerId);

    const WalRecordForPage* DecompressForBuildDirtyPageSet(WalRecordForPage *decompressedRecordForPage,
        const WalRecord *walRecord) const;
    template<bool checkEndPlsn>
    uint64 BuildDirtyPageSetAndPageWalRecordListReadWal(WalRecordReader *walRecordReader);

    inline void BatchPutWalRecordInfo(WalRecordInfo *walRecordInfos, uint32 &batchPutNum,
        const WalRecordInfo &walRecordInfo, bool forcePutAll);
    void FinishDirtyPageSetBuild();

    void InitStatisticInfo();

    uint64 GetMaxParseredPlsn() const;
    WalRecordReplayType CheckOrWaitWalRecordVersionMatch(const PdbId pdbId, const WalRecord *walRecord,
                                                         const uint32 workerId, bool withLoop,
                                                         WalRecordRedoContext *redoCtx);
    WalRecordReplayType WaitAvailableBufferForRedo(RedoWalRecordBuffPara *buffPara, WalRecordRedoContext *redoCtx,
                                                   const WalRecordForPage *recordForPage);
    BufferDesc *RecoveryReadBuffer(const ParaForReadBuffer para);
    static FileId GetFileIdFromTbsRecord(const WalRecord *walrecordTbsLogical);
    void MarkDroppedFile(FileId fileId);

    DstoreMemoryContext m_memoryContext;
    DstoreMemoryContext m_recordReaderMemContext;
    uint64 m_recoveryStartPlsn;
    uint64 m_recoveryEndPlsn;
    uint64 m_diskRecoveryStartPlsn;
    WalStream *m_walStream;
    WalReadBuffer *m_redoReadBuffer;
    WalReadBuffer *m_getDirtyPageReadBuffer;
    uint64 m_walFileSize;
    WalId m_walId;
    uint32 m_redoWorkerNum;
    bool m_enableParallelRedo;
    ParallelRedoWorker *m_parallelRedoWorkers[MAX_REDO_WORKER_NUM];
    uint64 *m_redoWorkersMaxDispatchedPlsn;
    std::thread *m_dispatchWorkerThd;
    BlockSpscQueue *m_dispatchQueue;
    BatchPutParaForWalReplay *m_parallelBatchPutEntrys = nullptr;

    std::atomic<WalRecoveryStage> m_stage;

    LWLock m_getPageWalRecordsLock;
    PageWalRecordInfoListHtab m_pageWalRecordInfoListHtab;
    std::thread *m_buildDirtyPageSetWorkerThd[PAGE_WAL_RECORD_HTAB_PUTTER_NUM];
    BlockSpscQueue *m_pageWalRecordDispatchQueues[PAGE_WAL_RECORD_HTAB_PUTTER_NUM];
    BatchPutParaForDirtyPageSetQue *m_batchPutParaForDirtyPage = nullptr;

    WalDirtyPageEntry *m_dirtyPageEntryArray;
    long m_dirtyPageEntryArraySize;
    std::atomic<bool> m_isDirtyPageSetBuilt;
    bool m_droppedFileArray[MAX_DATA_FILE_ID];

    uint64 m_lastGroupEndPlsn;
    static WalRedoStatisticInfo m_redoStatisticInfo;

    RedoMode m_redoMode;
    PdbId m_pdbId;
    uint64 m_curRedoFinishedPlsn  = INVALID_PLSN;
    std::atomic<bool> m_dispatchFinished;
    std::atomic<CommitSeqNo> m_barrierCsn;
    std::atomic<PdbSyncMode> m_curSyncMode;
    std::atomic<uint64> m_curBarrierEndPlsn;
    uint64 m_term;
    bool m_useAio;
    static std::atomic<bool> m_statWalTypeInfo;
    static WalRedoTypeStatisticInfo m_walTypeRedoStatisticInfo;
    std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>> m_startRedoTime;
};

}; /* The end of namespace DSTORE */
#endif
