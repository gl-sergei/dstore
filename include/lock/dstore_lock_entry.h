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

#ifndef DSTORE_LOCK_ENTRY_H
#define DSTORE_LOCK_ENTRY_H

#include "securec.h"
#include "lock/dstore_lwlock.h"
#include "lock/dstore_lock_datatype.h"
#include "errorcode/dstore_lock_error_code.h"
#include "fault_injection/fault_injection.h"

namespace DSTORE {

/* The maximum level of LockRequestSkipList. */
const int32 MAX_SKIPLIST_LEVEL = 8;

/*
 * Intrusive list node linker for lock requests.
 * One LockRquestLinker is corresponding to one lock request.
 * Each LockRequestLinker contains an array of dlist_node. Each dlist_node points to another dlist_node
 * to its right.
 */
struct LockRequestLinker {
public:
    explicit LockRequestLinker(void *linkOwner)
        : dlistNodes(defaultNodesSpace), ownerLockRequest(linkOwner)
    {
        for (uint32 i = 0; i < MAX_SKIPLIST_LEVEL; i++) {
            DListNodeInit(&(defaultNodesSpace[i]));
        }
    }

    void DestroyLinker()
    {
        dlistNodes = nullptr;
    }

    inline void *GetLinkerOwner()
    {
        return ownerLockRequest;
    }

    static inline LockRequestLinker *GetLinkerFromDlistNode(dlist_node *node)
    {
        return GetLinkerFromLevelNode(node, 0);
    }

    static inline LockRequestLinker *GetLinkerFromLevelNode(dlist_node *node, int32 level)
    {
        /* Cannot use dlist_container here because defaultNodesSpace[level] has a
         * runtime index, which is not a constant expression for offsetof.
         * Instead, compute the offset manually: base of defaultNodesSpace + level * sizeof(dlist_node). */
        size_t offset = offsetof(LockRequestLinker, defaultNodesSpace) + static_cast<size_t>(level) * sizeof(dlist_node);
        return reinterpret_cast<LockRequestLinker *>(reinterpret_cast<char *>(node) - offset);
    }

    inline dlist_node *GetDlistNode()
    {
        return GetDlistNodeAtLevel(0);
    }

    inline dlist_node *GetDlistNodeAtLevel(int32 level)
    {
        return &(dlistNodes[level]);
    }

    /* An array to store all dlist_node's corresponded owner lock request. */
    dlist_node defaultNodesSpace[MAX_SKIPLIST_LEVEL];
    /* A copy of pointer to defaultNodesSpace. */
    dlist_node *dlistNodes;
    /* Record the lock request that this LockRequestLinker represents. */
    void *ownerLockRequest;
};

enum class LockEnqueueMethod : uint8 {
    HOLD_OR_WAIT = 0,          /* The lock request could be added into granted queue or wait queue. */
    HOLD_BUT_DONT_WAIT = 1,    /* The lock request won't be added into wait queue. */
    DONT_HOLD_ONLY_WAIT = 2,   /* The lock request won't be added into granted queue. */
    FORCE_HOLD = 3,            /* The lock request must be added into granted queue,
                                  regardless of conflict calculations. */
    INVALID_METHOD = 4,
};

/*
 * Per-lock-request interface:
 *
 * This interface defines the virtual functions that need to be implemented,
 * so that LockEntry can use generic algorithms without having to consider
 * the different behaviors in each lock manager.
 */
class LockRequestInterface : public BaseObject {
public:
    explicit LockRequestInterface(LockMode lockMode, LockEnqueueMethod method = LockEnqueueMethod::HOLD_OR_WAIT)
        : m_linker(this),
          m_lockMode(lockMode),
          m_enqueueMethod(method)
    {}

    virtual ~LockRequestInterface() = default;

    virtual bool IsSameRequester(const LockRequestInterface *requester) const = 0;
    virtual bool IsValid(LockRecoveryMode recoveryMode) const = 0;
    virtual void RecordErrInfo(LockErrorInfo *info) const = 0;
    virtual void WakeUp() = 0;
    virtual int32 Compare(const LockRequestInterface *requester) const = 0;
    virtual uint32 GetSeed() const = 0;
    virtual LockRequestInterface *Duplicate(DstoreMemoryContext ctx) const = 0;
    virtual LockRequestInterface *Duplicate(void *memory) const = 0;
    virtual RetStatus DumpLockRequest(StringInfo stringInfo) const = 0;

    static inline LockRequestInterface *GetLockRequestFromNode(dlist_node* node)
    {
        return static_cast<LockRequestInterface *>(LockRequestLinker::GetLinkerFromDlistNode(node)->GetLinkerOwner());
    }

    static inline LockRequestInterface *GetLockRequestFromLevelNode(dlist_node* node, int32 level)
    {
        return static_cast<LockRequestInterface *>
            (LockRequestLinker::GetLinkerFromLevelNode(node, level)->GetLinkerOwner());
    }

    inline dlist_node *GetDlistNode()
    {
        return m_linker.GetDlistNode();
    }

    inline dlist_node *GetDlistNodeAtLevel(int32 level)
    {
        return m_linker.GetDlistNodeAtLevel(level);
    }

    inline LockMode GetLockMode() const
    {
        return m_lockMode;
    }

    inline bool IsOnlyWait() const
    {
        return m_enqueueMethod == LockEnqueueMethod::DONT_HOLD_ONLY_WAIT;
    }

    inline bool IsForceHold() const
    {
        return m_enqueueMethod == LockEnqueueMethod::FORCE_HOLD;
    }

    inline bool IsDontWait() const
    {
        return m_enqueueMethod == LockEnqueueMethod::HOLD_BUT_DONT_WAIT;
    }

    LockRequestLinker   m_linker;
    LockMode            m_lockMode;
    LockEnqueueMethod   m_enqueueMethod;

protected:
    DISALLOW_COPY_AND_MOVE(LockRequestInterface);
};

/*
 * Per-lock-request lock information:
 *
 * threadId -- thread id of this lock request.
 */
class LockRequest : public LockRequestInterface {
public:
    LockRequest(LockMode lockMode, ThreadId id, uint32 threadCoreIndex,
                LockEnqueueMethod method = LockEnqueueMethod::HOLD_OR_WAIT)
        : LockRequestInterface(lockMode, method),
          threadId(id),
          threadCoreIdx(threadCoreIndex)
    {}
    LockRequest(LockMode lockMode, ThreadContext *thread,
                LockEnqueueMethod method = LockEnqueueMethod::HOLD_OR_WAIT)
        : LockRequest(lockMode, thread->GetThreadId(), thread->GetCore()->selfIdx, method)
    {}
    ~LockRequest() override = default;

    bool IsSameRequester(const LockRequestInterface *requester) const override
    {
        const LockRequest *req = dynamic_cast<const LockRequest *>(requester);
        /*
         * Two requesters are the "same" if they come from the same thread.
         * i.e. threadId and threadCoreIdx are all same.
         */
        StorageAssert(threadCoreIdx == req->threadCoreIdx || threadId != req->threadId);
        return threadId == req->threadId;
    }

    bool IsValid(UNUSE_PARAM LockRecoveryMode recoveryMode) const override
    {
        return true;
    }

    void RecordErrInfo(LockErrorInfo *info) const override
    {
        if (info != nullptr) {
            info->threadId = threadId;
            info->lockMode = GetLockMode();
        }
    }

    void WakeUp() override;

    int32 Compare(const LockRequestInterface *requester) const override;

    uint32 GetSeed() const override
    {
        return threadCoreIdx;
    }

    LockRequestInterface *Duplicate(DstoreMemoryContext ctx) const override
    {
        FAULT_INJECTION_RETURN(DstoreLockMgrFI::LOCK_REQUEST_OOM_POINT, nullptr);
        LockRequest* request = DstoreNew(ctx) LockRequest(m_lockMode, threadId,
                                                         threadCoreIdx, m_enqueueMethod);
        if (STORAGE_VAR_NULL(request)) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                   ErrMsg("Failed to allocate memory for duplicate."));
            return nullptr;
        }
        return dynamic_cast<LockRequestInterface *>(request);
    }

    LockRequestInterface *Duplicate(void *memory) const override
    {
        FAULT_INJECTION_RETURN(DstoreLockMgrFI::LOCK_REQUEST_OOM_POINT, nullptr);
        LockRequest* request = DstoreNew(memory) LockRequest(m_lockMode, threadId,
                                                            threadCoreIdx, m_enqueueMethod);
        if (STORAGE_VAR_NULL(request)) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                   ErrMsg("Failed to allocate memory for duplicate."));
            return nullptr;
        }
        return dynamic_cast<LockRequestInterface *>(request);
    }

    RetStatus DumpLockRequest(StringInfo stringInfo) const override;

    ThreadId          threadId;
    uint32            threadCoreIdx;

protected:
    DISALLOW_COPY_AND_MOVE(LockRequest);
};

/**
 * Skip list can achieve O(log(N)) insertion and deletion time complexity,
 * which is similar to balanced tree but simpler.
 *
 * Skip lists are built in levels. The bottom level is an ordinary ordered linked list.
 * Each higher level acts as an "index" for the lower level linked list.
 *
 * | Level 2 |->| 2 A |------------------------------------->| 2 F |--------------------| NULL |
 * | Level 1 |->| 1 A |->| 1 B |---------->| 1 D |---------->| 1 F |---------->| 1 H |--| NULL |
 * | Level 0 |->| 0 A |->| 0 B |->| 0 C |->| 0 D |->| 0 E |->| 0 F |->| 0 G |->| 0 H |->| NULL |
 * |   Data  |  |  A  |  |  B  |  |  C  |  |  D  |  |  E  |  |  F  |  |  G  |  |  H  |
 */
class LockRequestSkipList {
public:
    LockRequestSkipList();
    ~LockRequestSkipList() = default;

    RetStatus InsertLockRequest(LockRequestInterface *request);
    LockRequestInterface *RemoveLockRequest(const LockRequestInterface *request);
    LockRequestInterface *SearchLockRequest(const LockRequestInterface *request);

    class ListIterator {
    public:
        explicit ListIterator(LockRequestSkipList *skipList, int32 level);
        ~ListIterator() noexcept;
        DISALLOW_COPY_AND_MOVE(ListIterator);

        LockRequestInterface *GetNextRequest();
    private:
        int32                 m_level;
        LockRequestSkipList  *m_skipList;
        LockRequestInterface *m_nextRequest;
    };

    inline bool IsSkipListEmpty()
    {
        return ((m_currHighestLevel == 0) && IsLevelEmpty(0));
    }

    inline int32 GetCurrHighestLevel() const
    {
        return m_currHighestLevel;
    }

    uint32 GetLevelRequestCnt(int32 level);

    RetStatus DumpSkipList(StringInfo str);

private:
    void RecordTargetRequestLevel(int32 *targetRequestLevel, const int32 level) const;
    LockRequestInterface *SearchLockRequestPosition(const LockRequestInterface *targetRequest,
        LockRequestInterface **stack, int32 *targetRequestLevel);
    int32 GetInsertLevel(LockRequestInterface *request);

    inline dlist_head *GetLevelHead(int32 level)
    {
        return &(m_indexHeads[level]);
    }

    inline dlist_node *GetLevelHeadNode(int32 level)
    {
        return DListHeadNode(GetLevelHead(level));
    }

    inline LockRequestInterface *GetLevelFirst(int32 level)
    {
        if (DListIsEmpty(GetLevelHead(level))) {
            return nullptr;
        }
        return LockRequestInterface::GetLockRequestFromLevelNode(GetLevelHeadNode(level), level);
    }

    inline LockRequestInterface *GetNextLockRequestAtLevel(int32 level, LockRequestInterface *request)
    {
        if (request == nullptr) {
            return GetLevelFirst(level);
        }

        dlist_node *node = request->GetDlistNodeAtLevel(level);
        if (DListHasNext(GetLevelHead(level), node)) {
            return LockRequestInterface::GetLockRequestFromLevelNode(DListNextNode(GetLevelHead(level), node), level);
        } else {
            return nullptr;
        }
    }

    inline void InsertRequestToLevelHead(int32 level, LockRequestInterface *request)
    {
        DListPushHead(GetLevelHead(level), request->GetDlistNodeAtLevel(level));
    }

    inline void InsertRequestNextTo(int32 level, LockRequestInterface *leftNeighbor, LockRequestInterface *request)
    {
        DListInsertAfter(leftNeighbor->GetDlistNodeAtLevel(level), request->GetDlistNodeAtLevel(level));
    }

    inline void RemoveRequestFromLevel(int32 level, LockRequestInterface *request)
    {
        DListDelete(request->GetDlistNodeAtLevel(level));
    }

    inline bool IsLevelEmpty(int32 level)
    {
        return DListIsEmpty(GetLevelHead(level));
    }

    dlist_head m_indexHeads[MAX_SKIPLIST_LEVEL];
    int32 m_currHighestLevel;
};

/*
 * Store those lock requests that are recently removed from granted list.
 * Every time we want to allocate a new lock request, use those in free list first.
 * This can reduce memory allocation & clean times.
 */
class LockRequestFreeList : public BaseObject {
public:
    LockRequestFreeList(uint32 maxLength, DstoreMemoryContext ctx);
    ~LockRequestFreeList() = default;

    void FreeAllMemory();

    LockRequestInterface *PopACopyOf(const LockRequestInterface *request);
    void Push(LockRequestInterface *request);

    uint32 GetCurLength();
    uint32 GetMaxLength();

private:
    dlist_head          m_freeList;
    uint32              m_freeListLength;
    uint32              m_maxLength;
    DstoreMemoryContext m_ctx;
};

class LockRequestFreeLists : public BaseObject {
public:
    LockRequestFreeLists();
    ~LockRequestFreeLists();

    RetStatus BuildFreeLists(uint32 partitionNum, uint32 maxLength, DstoreMemoryContext ctx);
    void DestroyFreeLists();

    inline LockRequestFreeList *GetFreeList(uint32 partitionIndex)
    {
        StorageAssert(partitionIndex < m_partitionNum);
        return m_partitionFreeLists[partitionIndex];
    }
private:
    uint32                m_partitionNum;
    LockRequestFreeList **m_partitionFreeLists;
    uint32                m_maxLength;
    DstoreMemoryContext   m_ctx;
};

/*
 * Checks if the requested mode conflicts with the granted mode on an arbitrary
 * lock. Used in DeadlockDetector::FindWaitForGraphCycle().
 */
bool HasConflictWithMode(const LockMode requestedMode, const LockMode mode);
bool HasConflictWithMask(const LockMode requestedMode, const LockMask mask);
bool HasConflictWithMask(const LockMask requestedMask, const LockMask mask);

/*
 * m_lockTag -- uniquely identifies the object being locked.
 * m_grantMask -- bitmask for all lock types currently granted on this object.
 * m_waitMask -- bitmask for all lock types currently awaited on this object.
 * m_grantedCnt -- count of each lock type currently granted on the lock.
 * m_waitingCnt -- count of each lock type currently waiting on the lock.
 * m_grantedTotal -- total granted requests of all types.
 * m_waitingTotal -- total waiting requests of all types.
 *
 * Note: these counts count 1 for each backend. Internally to a backend,
 * there may be multiple grabs on a particular lock, but this is not reflected
 * into shared memory.
 */
class LockEntryCore {
public:
    LockTag m_lockTag;
    LockMask m_grantMask;
    LockMask m_waitMask;
    uint32 m_grantedTotal;
    uint32 m_waitingTotal;
    uint32 m_grantedCnt[DSTORE_LOCK_MODE_MAX];
    uint32 m_waitingCnt[DSTORE_LOCK_MODE_MAX];

    void Copy(const LockEntryCore &core)
    {
        *this = core;
    }

    LockEntryCore()
        : m_lockTag(),
          m_grantMask(static_cast<LockMask>(0)),
          m_waitMask(static_cast<LockMask>(0)),
          m_grantedTotal(0),
          m_waitingTotal(0),
          m_grantedCnt{0},
          m_waitingCnt{0}
    {}
};

/*
 * Per-locked-object lock information:
 *
 * lockEntryCore -- core struct contains lockTag, grantMask, waitMask, granted, waiting, grantedTotal,
 * waitingTotal.
 * grantedQueue -- queue of LockRequest hold this lock.
 * waitingQueue -- queue of LockRequest waiting for this lock.
 *
 * The granted and waiting queue are not thread protected here, but lock manger would have
 * protective latches to ensure no read-write or write-write collision on the same lock entry.
 *
 * Note: Although LockEntry can accept any derived class from LockRequestInterface as a parameter,
 *       mixing different derived classes in one LockEntry should be prohibited.
 */
struct LockEntry {
public:
    LockEntryCore       lockEntryCore;
    LockRequestSkipList grantedQueue;
    dlist_head          waitingQueue;

    void Initialize(const LockTag *lockTag);

    RetStatus EnqueueLockRequest(const LockRequestInterface *request, LockRequestFreeList *freeList,
                                 LockErrorInfo *info);
    bool DequeueLockRequest(const LockRequestInterface *requester, LockRequestFreeList *freeList);

    void AdvanceWaitingQueue(LockRequestFreeList *freeList);

    void RemoveAllRequests(LockRequestFreeList *freeList);

    inline bool IsNoHolderAndNoWaiter() const
    {
        return ((lockEntryCore.m_grantedTotal == 0) && (lockEntryCore.m_waitingTotal == 0));
    }

    bool IsHeldByRequester(const LockRequestInterface *requester);

    inline const LockTag *GetLockTag() const
    {
        return &(lockEntryCore.m_lockTag);
    }

    RetStatus DumpLockEntry(StringInfo str);

private:
    bool HasConflictWithAnyHolder(const LockRequestInterface *requester, LockRequestFreeList *freeList,
                                  LockErrorInfo *info);

    RetStatus TryAddToWaiterQueue(LockRequestInterface *requester, LockRequestFreeList *freeList,
                                  LockErrorInfo *info);
    void RemoveFromWaiterQueue(LockRequestInterface *requester);

    void AddToHolderQueue(LockRequestInterface *requester, LockRequestFreeList *freeList);
    RetStatus RemoveFromHolderQueue(const LockRequestInterface *requester, LockRequestFreeList *freeList);
};
} /* namespace DSTORE */
#endif
