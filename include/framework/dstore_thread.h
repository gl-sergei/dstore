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
 */

#ifndef DSTORE_THREAD_H
#define DSTORE_THREAD_H

#include <mutex>
#include "framework/dstore_thread_interface.h"
#include "common/concurrent/dstore_futex.h"
#include "lock/dstore_lwlock.h"
#include "common/memory/dstore_mctx.h"
#include "transaction/dstore_transaction_list.h"
#include "framework/dstore_instance_interface.h"
#include "transaction/dstore_tac_struct.h"
#include "framework/dstore_session.h"

namespace DSTORE {
#define START_BOOTSTRAP_OBJECT_ID 10000
/*
 * Same as PGPROC
 * */
struct StringInfoData;
struct WalPerfCounter;
class ThreadContext;
struct ThreadPerfCounter {
    WalPerfCounter *walPerfCounter = nullptr;

    RetStatus Init();
};

constexpr uint64 INVALID_VIRTUAL_TRANSACTION_COUNTER_ID = 0;

struct ThreadBufferReadStat {
    uint32_t bufferReadFromRemote;
    uint32_t bufferReadHit;
    uint32_t bufferReadCount;
    bool isReporting;
    void resetBufferReadStat()
    {
        bufferReadFromRemote = 0;
        bufferReadHit = 0;
        bufferReadCount = 0;
        isReporting = false;
    }
};

static constexpr int THREAD_NAME_MAX_LENGTH = 16;

struct ThreadCore {
    struct ThreadCore *next;
    struct ThreadCore *prev;
    ThreadId pid; /* Backend's process ID; 0 if prepared xact */
    char *threadName;
    ThreadContext *threadContext;   /* Anaphora pointer, due to find thread context by thread id. */
    uint32 selfIdx;
    int numaId;
    DstoreFutex futex;
    LWLockWaiter lockWaiter;
    struct BufferTagArray *bufTagArray;
    bool immediateAbort;
    volatile uint32_t *interruptHoldoffCount;
    TimestampTz startTime;
    long lwpid; /* thread's lwpid */

    /* Support for group wal insert. */
    struct WalBatchInsertStatus *walInsertStatus;
    struct ThreadPerfCounter *threadPerfCounter;
    class LockThreadContext *regularLockCtx;
    RetStatus Init(uint32 idx = INVALID_THREAD_CORE_ID, int numaNodeId = 0);
    void SetThreadName(const char *name);
    void SetStartTime(TimestampTz time);
    void Wakeup();
};

union ThreadCorePad {
    ThreadCore m_core;
    char m_pad[DSTORE_CACHELINE_SIZE];
};

/**
 * Same as PGXACT, we separate the xact and status is
 * because xact should be as tidy as possible to avoid multiple cacheline read
 */
struct ThreadXact {
    gs_atomic_uint64 csnMin;
    /*
     * This is the csn from the first statement of this transaction.
     * If TAC is enabled, vaccum must not remove tuples whose
     * tuple.csn >= firstStatementCsnMin.
     */
    gs_atomic_uint64 firstStatementCsnMin;
    /*
     * This is the expiry time of the first statement csn of this transaction.
     * The timer will start when the commit happens internally, and will be resetted at
     * the next query.
     * The purpose for this timestamp is when we are considering the case where the
     * server received the commit message from the client and executed the commit
     * internally, but the client failed to receive the commit response (ReadyForQuery)
     * from the server, therefore client will ask for the transaction state of the
     * current transaction. And since the transaction is committed, it is possible that the
     * tuple has been recycled therefore makes the transaction unreplayable.
     */
    gs_atomic_uint64 firstStatementCsnMinExpiryTimestamp;
    /* This is the current active transaction xid of the thread,
     * it will be set to INVALID_XID when the transaction start and end,
     * and be set to assigned xid when AllocTransactionSlot.
     * It's used by ccindex to maintain a hash table of concurrent active transactions.
     * Because the Xid has not defined yet, we here use m_placeHolder instead of.
     */
    gs_atomic_uint64 currentActiveXid;
    std::atomic<CommitSeqNo> toBeCommitedCsn;
    volatile PdbId pdbId;
    void Init();
};

union ThreadXactPad {
    ThreadXact m_xact;
    char m_pad[DSTORE_CACHELINE_SIZE];
};

struct ThreadCorePayload {
    struct ThreadCore *core;
    struct ThreadXact *xact;
    LWLock *xactLock;
    uint32 selfIdx; /* self index in the status array */
};

/*
 * About this structure  Q&A
 *
 * Q:why we need separate the core structure into threadcore/thread xact
 * A:to gain better performance, we should keep xact fit into cacheline.
 *
 * Q:why we need 2 locks?
 * A:to gain better scalability
 * core_lock : thread add/remove, calculate global xact info
 * xactLock : thread change own xact info, calculate global xact info
 *
 * Q:why we need another sort index array,
 * A:to gain better performance
 * when we calculate global xact info, we should scan the xact array, but
 * the inuse slot distribute pretty random in the whole array, we maintain a
 * sorted array when thread add/remove to utilize locality, when scan the array,
 * there is an increased likelihood of finding the next xact in the cacheline
 * with help of the cache prefetch.
 */
class ThreadCoreMgr {
public:
    ThreadCoreMgr()
        : m_coreArray(nullptr),
          m_xactArray(nullptr),
          m_lockArray(nullptr),
          m_nameArray(nullptr),
          m_sortCoreIdxArr(nullptr),
          m_sortCoreIdxArrBound(0),
          m_coreArrayNum(0),
          m_numaNodeNum(0),
          m_freeCoreList(nullptr),
          m_usedCoreNum(0),
          m_spinLock(nullptr)
    {}
    RetStatus Init(uint32 maxThreadNum, uint32 numaNodeNum = 1);
    void Destroy();

    /*
     * allocate a core/xact slot from the whole array, and bind to the thread. but not add the sort array
     * as current we have not initial the thread core yet, lwlock can not be used.
     */
    RetStatus ThreadBindCoreSlot(PdbId pdbId, ThreadContext *threadContext, ThreadCorePayload &threadCore,
                                 volatile uint32_t *InterruptHoldoffCount);

    /* unbind a thread from the core slot, reset some area and return to free slot */
    void ThreadUnbindCoreSlot(ThreadCorePayload &threadCore);

    /* add self to the sorted array, then make thread visible to master and other thread */
    void InsertSortArray(const ThreadCorePayload &threadCore);

    /* remove self to the sorted array, then make thread invisible to master and other thread */
    void RemoveSortArray(ThreadCorePayload &threadCore);

    ThreadCore *GetSpecifiedCore(uint32 idx);

    void WakeupAllThreads();

    RetStatus GetThreadsInfo(ThreadStatsInfo **threadInfoArr, size_t *length);

    class ThreadIterator {
    public:
        explicit ThreadIterator(ThreadCoreMgr *mgr) : m_mgr(mgr), m_current(-1)
        {
            DstoreLWLockAcquire(&(m_mgr->m_coreLock.lock), LW_SHARED);
        }
        ~ThreadIterator()
        {
            LWLockRelease(&(m_mgr->m_coreLock.lock));
            m_mgr = nullptr;
        }
        DISALLOW_COPY_AND_MOVE(ThreadIterator);

        ThreadCore *GetNextThreadCore()
        {
            m_current++;
            if (m_current < m_mgr->m_sortCoreIdxArrBound) {
                StorageAssert(m_current < m_mgr->m_coreArrayNum);
                return &m_mgr->m_coreArray[m_mgr->m_sortCoreIdxArr[m_current]]->m_core;
            } else {
                return nullptr;
            }
        }

        ThreadXact *GetNextThreadXactInPdb(PdbId pdbId, ThreadId *nextThreadId, ThreadContext **thrdContext)
        {
            ThreadXact *xact = nullptr;
            while (xact == nullptr && ++m_current < m_mgr->m_sortCoreIdxArrBound) {
                StorageAssert(m_current < m_mgr->m_coreArrayNum);
                if (m_mgr->m_xactArray[m_mgr->m_sortCoreIdxArr[m_current]]->m_xact.pdbId != pdbId) {
                    continue;
                }
                *nextThreadId = m_mgr->m_coreArray[m_mgr->m_sortCoreIdxArr[m_current]]->m_core.pid;
                *thrdContext = m_mgr->m_coreArray[m_mgr->m_sortCoreIdxArr[m_current]]->m_core.threadContext;
                xact = &m_mgr->m_xactArray[m_mgr->m_sortCoreIdxArr[m_current]]->m_xact;
            }
            return xact;
        }

        ThreadCore *GetNextThreadCoreInAll()
        {
            m_current++;
            if (m_current < m_mgr->m_coreArrayNum) {
                return &m_mgr->m_coreArray[m_current]->m_core;
            } else {
                return nullptr;
            }
        }

    private:
        ThreadCoreMgr *m_mgr;
        uint32 m_current;
    };

    CommitSeqNo GetLocalCsnMin();

    CommitSeqNo GetLocalMinCommitedCsn();

private:
    void FindFreeCoreByNumaId(int numaId, ThreadCore **current);
    ThreadCore *GetFreeCore();
    void AddFreeCore(ThreadCore *core);
    void ResetCoreState(ThreadCore *core);
    RetStatus CheckDuplicateThreadId(const ThreadId threadId);

    ThreadCorePad **m_coreArray;
    ThreadXactPad **m_xactArray; /* its a xact array */
    LWLockPadded m_xactLock;     /* protect xact array specifically. */
    LWLockPadded m_coreLock;     /* protect above structure */

    class LockThreadContext **m_lockArray;
    char *m_nameArray;
    uint32 *m_sortCoreIdxArr;
    uint32 m_sortCoreIdxArrBound;
    uint32 m_coreArrayNum;
    uint32 m_numaNodeNum;
    ThreadCore **m_freeCoreList; /* free core list */
    std::atomic<uint32> m_usedCoreNum;        /* indicates how many core resources are used */
    DstoreSpinLock *m_spinLock;
};

union Xid;
struct BufferTag;
constexpr Size PRIVATEBUFF_ARRAY_ENTRIES = 8;
struct PrivateBufferCollector {
    struct HTAB *privateBufTable; /* Used for parallel reading of a single buffer page. */
    struct PrivateBufferEntry *privateBufArray;
    uint64 privateBufTotalNum;
    uint8 privateBufMaxValidPos;
};

class ThreadContext : public ThreadContextInterface, public BaseObject {
    /* variables used only by the current thread */
public:
    ThreadContext();
    ~ThreadContext() final = default;

    RetStatus InitializeBasic() final;
    RetStatus InitStorageContext(PdbId pdbId) final;
    RetStatus InitTransactionRuntime(PdbId pdbId, TrxCallback callbck, void *arg, bool allocateZoneId = true) final;
    void DestroyTransactionRuntime() final;
    RetStatus CreateAutonomousTrx(bool isSpecialAyncCommitAutoTrx = false);
    void DestroyAutonomousTrx();
    bool ContainsTransaction(Xid xid) const;
    bool NonActiveTransactionHoldConflict2PLock(const struct LockTag &tag, LockMode mode, LockMgrType mgrType) const;
    bool AllTransactionsHold2PLockMoreThan(const struct LockTag &tag, LockMode mode, LockMgrType mgrType,
                                           uint32 cnt) const;
    RetStatus ActuallyAcquireLazyLocksOnCurrentThread();
    void ReleaseAllTransactionLocks();
    DstoreMemoryContext GetTransactionMemoryContext();
    DstoreMemoryContext GetQueryMemoryContext();
    DstoreMemoryContext GetTopTransactionMemoryContext();
    StorageSession *GetSession();
    class BufMgrInterface *GetTmpLocalBufMgr();
    DstoreMemoryContext GetSessionMemoryCtx();
    DstoreMemoryContext GetGlobalSmgrMemoryCtx();
    struct BufPrivateRefCount *GetBufferPrivateRefCount();
    RetStatus InitPrivateBufferCollector();
    void DestroyPrivateBufferCollector();
    void Destroy() final;
    CommitSeqNo GetSnapShotCsn() final;
    void SetSnapshotCsnForFlashback(CommitSeqNo csn) final;
    void SetFlashbackCsnMin(CommitSeqNo flashbackCsnMin) final;
    CommitSeqNo GetNextCsn() final;
    void AppendTransactionDebugInfo(StringInfoData *buf);
    void AppendErrorDebugInfo(StringInfoData *buf);

    void ResetErrorMemory() final;
    void ResetQueryMemory() final;
    void ResetSmgrMemory() final;

    void SetLocalCsn(CommitSeqNo csn);
    CommitSeqNo GetLocalCsn();
    void SetLocalXid(uint64 xid);

    void SetSnapshotCsnForTac(CommitSeqNo csn);
    void CleanupTacSnapshot(bool resetXactFirstStmtCsnMin);
    void StartTacFirstStatementCsnMinExpiryTimer();
    Xid GetCurrentXid();
    void SetThreadMemLevel(ThreadMemoryLevel level);

    inline void AttachSessionToThread(StorageSession *session) final
    {
        m_session = session;
    }

    void DetachSessionFromThread() final;

    inline void SetXactPdbId(PdbId pdbId)
    {
        if (threadCore.xact != nullptr) {
            threadCore.xact->pdbId = pdbId;
        }
    }

    inline PdbId GetXactPdbId()
    {
        return (threadCore.xact != nullptr) ? threadCore.xact->pdbId : INVALID_PDB_ID;
    }

    inline ThreadMemoryLevel GetLevel()
    {
        return m_threadmemlevel;
    }

    inline int GetNumaId() final
    {
        return m_numaId;
    }

    inline void SetNumaId(int numaId) final
    {
        m_numaId = numaId;
    }

    inline ThreadCore *GetCore()
    {
        return threadCore.core;
    }

    inline ThreadXact *GetXact()
    {
        return threadCore.xact;
    }

    inline class LockThreadContext *GetLockCtx()
    {
        return threadCore.core->regularLockCtx;
    }

    inline ThreadId GetThreadId() const override
    {
        return threadCore.core->pid;
    }

    void ReleaseTableSpaceCache();

    /*
     * etc lock operation.
     */
    void Sleep(const struct timespec *timeout = nullptr) final;

    void Wakeup() final;

    inline void SetWaitLwlock(bool wait)
    {
        GS_WRITE_BARRIER();
        threadCore.core->lockWaiter.waiting = wait;
    }

    inline bool IsWaitLwlock() const
    {
        bool isWaiting = threadCore.core->lockWaiter.waiting;
        GS_READ_BARRIER(); /* Make sure all the modifications before the flag are visible. */
        return isWaiting;
    }

    inline class Transaction *GetActiveTransaction()
    {
        return m_transactionList.GetActiveTransaction();
    }

    inline class Transaction *GetTopTransaction()
    {
        return m_transactionList.GetTopTransaction();
    }

    DstoreMemoryContext GetUndoMemoryCtx()
    {
        return m_undoMemoryContext;
    }

    inline char* GetUndoContext()
    {
        return m_undoContext;
    }

    inline void SetUniqueQueryId(uint64 queryId) final
    {
        m_uniqueQueryId = queryId;
    }

    inline uint64 GetUniqueQueryId() const
    {
        return m_uniqueQueryId;
    }

    inline uint64 GetAndIncrementAndSetTransactionVirtualCounter()
    {
        // maintain a per thread transaction counter value
        return ++m_ThreadVirtualTransactionCounter;
    }
    inline uint64 GetTransactionVirtualCounter() final
    {
        return m_ThreadVirtualTransactionCounter;
    }

    inline bool NeedCommBuffer() const
    {
        return m_needCommBuffer;
    }

    inline void SetNeedCommBuffer(bool needCommBuffer)
    {
        m_needCommBuffer = needCommBuffer;
    }

    inline void SetIsObjSpaceThrd(bool flag)
    {
        m_isObjSpaceThrd = flag;
    }

    inline uint64_t GetPdbTerm()
    {
        return GsAtomicReadU64(const_cast<uint64_t *>(&m_pdbTerm));
    }

    inline void SetPdbTerm(uint64_t pdbTerm)
    {
        GsAtomicWriteU64(&m_pdbTerm, pdbTerm);
    }

    void SetInterruptPending() final
    {
        threadCore.core->immediateAbort = true;
        Wakeup();
    }
    void ClearInterruptPending() final
    {
        threadCore.core->immediateAbort = false;
    }
    inline bool IsInterruptPending()
    {
        return threadCore.core->immediateAbort;
    }
    inline void HoldInterrupts()
    {
        if (unlikely(threadCore.core->interruptHoldoffCount == NULL)) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("InterruptHoldoffCount is leaking."));
        }
        (*threadCore.core->interruptHoldoffCount)++;
    }
    inline void ResumeInterrupts()
    {
        if (unlikely(threadCore.core->interruptHoldoffCount == NULL) ||
            unlikely(*(threadCore.core->interruptHoldoffCount) <= 0)) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("InterruptHoldoffCount is leaking."));
        }
        (*threadCore.core->interruptHoldoffCount)--;
    }
    inline bool AreInterruptsHeld()
    {
        if (unlikely(threadCore.core->interruptHoldoffCount == NULL)) {
            return false;
        }
        return *(threadCore.core->interruptHoldoffCount) > 0;
    }
    inline void SetCallbackInvoke(bool flag) final
    {
        m_isCallbackInvoke = flag;
    }
    inline bool IsCallbackInvoke() final
    {
        return m_isCallbackInvoke;
    }
    inline void SetCallbackErrExist(bool flag) final
    {
        m_isCallbackErrExist = flag;
    }
    inline bool IsCallbackErrExist() final
    {
        return m_isCallbackErrExist;
    }
    RetStatus CheckforInterrupts();
    void RefreshWorkingVersionNum() final;

    ErrorCode GetErrorCode() final;
    const char *GetErrorMessage() final;
    NodeId GetErrorNodeId() final;

    void *GetPrivateBuffer(BufferTag bufTag);
    void SetPrivateBuffer(BufferTag bufTag, void *addr);
    void DelPrivateBuffer(BufferTag bufTag);
    void BuildPrivateBufferTbl();
    static RetStatus DstoreThreadTryInitialize(const char *threadName, PdbId pdbId = g_defaultPdbId);
    static void DstoreThreadTryDestory();
    static void DstoreThreadAddVisibleThread();

    ThreadCorePayload threadCore;
    struct AtomicWalWriterContext *m_walWriterContext;
    struct LWlockContext lwlockContext;
    struct BootStrapContext *bootstrapContext;
    struct BufPrivateRefCount *bufferPrivateRefCount;
    class StorageMemoryMgr *m_memoryMgr = nullptr;
    struct ThreadLogContext *logContext;
    uint64 m_uniqueQueryId;
    int m_numaId;
    bool m_needCommBuffer;
    uint64 m_ThreadVirtualTransactionCounter;
    Oid m_inplaceUpgradeNextOid;
    struct Error *error;
    void *receivedRequest;
    /* whether tac replay is disabled due to STP commit/rollback or autonm execution */
    bool m_tacReplayDisabled;
    struct TacSnapshotPayload m_tacSnapshotReceived;
    struct TacSnapshotPayload m_tacSnapshotToSend;
    StorageSession *m_session;
    struct ThreadBufferReadStat m_bufferReadStat;
    ThreadMemoryLevel m_threadmemlevel;
    volatile uint32_t m_working_version_num;
    bool m_isCallbackInvoke;
    bool m_isCallbackErrExist;
    bool m_isObjSpaceThrd;
    gs_atomic_uint64 m_pdbTerm = 0;

private:
    TransactionList m_transactionList;
    struct PrivateBufferCollector m_privateBufCollector;
    DstoreMemoryContext m_undoMemoryContext; /* memory context for insert undo and fetch undo. */
    char* m_undoContext; /* Used for fetch undo record. */
} ALIGNED(DSTORE_CACHELINE_SIZE);

extern thread_local ThreadContext *thrd;
}  // namespace DSTORE

#endif
