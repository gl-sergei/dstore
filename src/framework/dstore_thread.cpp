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
 * dstore_thread.cpp
 *
 * IDENTIFICATION
 *        src/framework/dstore_thread.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <sys/syscall.h>
#include <unistd.h>
#include "framework/dstore_thread.h"
#include "framework/dstore_session.h"
#include "securec.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_mgr.h"
#include "transaction/dstore_transaction_list.h"
#include "framework/dstore_instance.h"
#include "buffer/dstore_buf_refcount.h"
#include "common/instrument/trace/dstore_framework_trace.h"
#include "wal/dstore_wal_write_context.h"
#include "wal/dstore_wal_logstream.h"
#include "wal/dstore_wal_perf_statistic.h"
#include "lock/dstore_lock_thrd_local.h"
#include "lock/dstore_table_lock_mgr.h"
#include "common/log/dstore_log.h"
#include "common/algorithm/dstore_string_info.h"
#include "port/dstore_port.h"
#include "buffer/dstore_buf_mgr_temporary.h"
#include "buffer/dstore_buf.h"
#include "framework/dstore_framework_perf_unit.h"
#include "common/concurrent/dstore_futex.h"

#ifdef DSTORE_USE_NUMA
#include <utmpx.h>
#include "numa.h"
#endif

#define GET_TID() syscall(__NR_gettid)

extern "C" void CloseLogger(void);
namespace DSTORE {

constexpr int THRD_MAX_PRIVATE_BUF_SIZE{7};

thread_local ThreadContext *thrd = nullptr;

static_assert(INVALID_CSN == 0, "Csn in proc_array must be memset_s to 0(INVALID_CSN) when inited.");

RetStatus ThreadPerfCounter::Init()
{
    walPerfCounter = (WalPerfCounter *)DstorePalloc(sizeof(WalPerfCounter));
    if (STORAGE_VAR_NULL(walPerfCounter)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to alloc memory for walPerfCounter."));
        return DSTORE_FAIL;
    }
    walPerfCounter->Init();
    return DSTORE_SUCC;
}

ThreadContext::ThreadContext()
    : threadCore{nullptr, nullptr, nullptr, 0},
      m_walWriterContext(nullptr),
      lwlockContext(),
      bootstrapContext(nullptr),
      bufferPrivateRefCount(nullptr),
      m_memoryMgr(nullptr),
      logContext(nullptr),
      m_uniqueQueryId(0),
      m_numaId(0),
      m_needCommBuffer(true),
      m_ThreadVirtualTransactionCounter(0),
      m_inplaceUpgradeNextOid(START_BOOTSTRAP_OBJECT_ID),
      error(nullptr),
      receivedRequest(nullptr),
      m_tacReplayDisabled{false},
      m_tacSnapshotReceived{INVALID_CSN},
      m_tacSnapshotToSend{INVALID_CSN},
      m_session(nullptr),
      m_bufferReadStat{0, 0, 0, false},
      m_threadmemlevel(ThreadMemoryLevel::THREADMEM_WORKER_INVALID),
      m_working_version_num(0),
      m_isCallbackInvoke(false),
      m_isCallbackErrExist(false),
      m_isObjSpaceThrd(false),
      m_pdbTerm(0),
      m_transactionList(),
      m_privateBufCollector{nullptr, nullptr, 0, 0},
      m_undoMemoryContext(nullptr),
      m_undoContext(nullptr)
{}

RetStatus ThreadContext::InitPrivateBufferCollector()
{
    m_privateBufCollector.privateBufArray = static_cast<PrivateBufferEntry *>(
        DstorePallocAligned(PRIVATEBUFF_ARRAY_ENTRIES * sizeof(PrivateBufferEntry), DSTORE_CACHELINE_SIZE));
    if (STORAGE_VAR_NULL(m_privateBufCollector.privateBufArray)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to alloc memory for private buffer collector."));
        return DSTORE_FAIL;
    }
    m_privateBufCollector.privateBufMaxValidPos = 0;
    m_privateBufCollector.privateBufTotalNum = 0;
    m_privateBufCollector.privateBufTable = nullptr;
    return DSTORE_SUCC;
}

RetStatus ThreadContext::InitializeBasic()
{
#if !defined(DSTORE_TEST_TOOL) && !defined(UT)
    m_working_version_num = g_storageInstance->GetWorkingVersionNum();
#endif
    m_numaId = 1;

    m_memoryMgr = StorageMemoryMgr::create_memory_mgr(STORAGE_THREAD_MEMORY);
    if (STORAGE_VAR_NULL(m_memoryMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to alloc memory for thread mmgr."));
        return DSTORE_FAIL;
    } else {
        error = DstoreNew(m_memoryMgr->GetRoot()) Error();
        if (STORAGE_VAR_NULL(error)) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to alloc memory for error."));
            return DSTORE_FAIL;
        }
        OpenLoggerThread();
        AutoMemCxtSwitch autoSwitch{m_memoryMgr->GetRoot()};
        /* LWlockContext init */
        lwlockContext.num_held_lwlocks = 0;
        lwlockContext.held_lwlocks =
            (LWLockHandle *)DstorePallocAligned(sizeof(LWLockHandle) * MAX_SIMUL_LWLOCKS, DSTORE_CACHELINE_SIZE);
        if (STORAGE_VAR_NULL(lwlockContext.held_lwlocks)) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON,
                   ErrMsg("Dstore lwlockContext init failed for out of memory, when allocate size:%u.",
                          (uint32)(sizeof(LWLockHandle) * MAX_SIMUL_LWLOCKS)));
            return DSTORE_FAIL;
        }
        errno_t rc = memset_s(lwlockContext.held_lwlocks, sizeof(LWLockHandle) * MAX_SIMUL_LWLOCKS, 0,
            sizeof(LWLockHandle) * MAX_SIMUL_LWLOCKS);
        storage_securec_check(rc, "\0", "\0");

        /* BufPrivateRefCount init */
        char *alignedMem = (char *)DstorePallocAligned(sizeof(BufPrivateRefCount), DSTORE_CACHELINE_SIZE);
        if (STORAGE_VAR_NULL(alignedMem)) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to create thread context for alloc alignedMem."));
            return DSTORE_FAIL;
        }
        bufferPrivateRefCount = DstoreNew(alignedMem) BufPrivateRefCount();
        if (STORAGE_VAR_NULL(bufferPrivateRefCount)) {
            DstorePfreeAligned(alignedMem);
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Dstore BufPrivateRefCount init failed for out of memory."));
            return DSTORE_FAIL;
        }
        bufferPrivateRefCount->Initialize();

        if (STORAGE_FUNC_FAIL(InitPrivateBufferCollector())) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Dstore PrivateBuffer init failed for out of memory."));
            return DSTORE_FAIL;
        }

        m_uniqueQueryId = 0;
        m_ThreadVirtualTransactionCounter = INVALID_VIRTUAL_TRANSACTION_COUNTER_ID;
        receivedRequest = nullptr;
    }
    return DSTORE_SUCC;
}

RetStatus ThreadContext::InitStorageContext(PdbId pdbId)
{
    storage_trace_entry(TRACE_ID_ThreadContext__initialize);
    receivedRequest = nullptr;
    /* The walContext can be initialized only after the g_storageInstance initializes the WalManager. */

    if (m_walWriterContext == nullptr && pdbId != INVALID_PDB_ID) {
        StorageAssert(g_storageInstance->GetPdb(pdbId) != nullptr);
        m_walWriterContext = DstoreNew(m_memoryMgr->GetRoot())
            AtomicWalWriterContext(m_memoryMgr->GetRoot(), pdbId, g_storageInstance->GetPdb(pdbId)->GetWalMgr());
        if (STORAGE_VAR_NULL(m_walWriterContext)) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Dstore WalWriterContext init failed for out of memory."));
            return DSTORE_FAIL;
        }
    }

    if (m_walWriterContext != nullptr) {
        StorageAssert(m_walWriterContext->GetPdbId() == pdbId);
        if (STORAGE_FUNC_FAIL(m_walWriterContext->Init())) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Dstore WalWriterContext init failed."));
            return DSTORE_FAIL;
        }
    }

    if (m_undoMemoryContext == nullptr) {
        m_undoMemoryContext =
            DstoreAllocSetContextCreate(thrd->m_memoryMgr->GetRoot(), "UndoCtx", ALLOCSET_DEFAULT_SIZES);
        if (STORAGE_VAR_NULL(m_undoMemoryContext)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("[%s] Failed to create InitStorageContext.", __FUNCTION__));
            return DSTORE_FAIL;
        }
    }

    if (m_undoContext == nullptr && pdbId != INVALID_PDB_ID) {
        m_undoContext = static_cast<char *>(DstoreMemoryContextAllocZero(m_undoMemoryContext, BLCKSZ));
        if (STORAGE_VAR_NULL(m_undoContext)) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Dstore create undo context failed for out of memory."));
            return DSTORE_FAIL;
        }
    }

    storage_trace_exit(TRACE_ID_ThreadContext__initialize);
    return DSTORE_SUCC;
}

RetStatus ThreadContext::InitTransactionRuntime(PdbId pdbId, TrxCallback callbck, void *arg, bool allocateZoneId)
{
    if (STORAGE_FUNC_FAIL(m_transactionList.InitRuntime(allocateZoneId, pdbId))) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("[InitTransactionRuntime (threadId = %lu)] Failed to execute TransactionList::InitRuntime.",
                      thrd->GetThreadId()));
        return DSTORE_FAIL;
    }
    thrd->SetPdbTerm(g_storageInstance->GetPdbTerm(pdbId));
    if (callbck != nullptr) {
        GetActiveTransaction()->RegisterCallback(callbck, arg);
    }
    return DSTORE_SUCC;
}

void ThreadContext::DestroyTransactionRuntime()
{
#ifndef UT
    if (thrd && unlikely(thrd->GetXactPdbId() != m_transactionList.GetPdbId())) {
            ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
                ErrMsg("pdbId must be same when Init&DestroyTransactionRuntime.%u vs %u.",
                thrd->GetXactPdbId(), m_transactionList.GetPdbId()));
    }
#endif
    m_transactionList.Destroy();
}

RetStatus ThreadContext::CreateAutonomousTrx(bool isSpecialAyncCommitAutoTrx)
{
    return m_transactionList.CreateAutonomousTransaction(isSpecialAyncCommitAutoTrx);
}

void ThreadContext::DestroyAutonomousTrx()
{
    m_transactionList.DestroyAutonomousTransaction();
}

bool ThreadContext::ContainsTransaction(Xid xid) const
{
    return m_transactionList.ContainsTransaction(xid);
}

bool ThreadContext::NonActiveTransactionHoldConflict2PLock(const LockTag &tag, LockMode mode, LockMgrType mgrType) const
{
    return m_transactionList.NonActiveTransactionHoldConflict2PLock(tag, mode, mgrType);
}

bool ThreadContext::AllTransactionsHold2PLockMoreThan(const struct LockTag &tag, LockMode mode, LockMgrType mgrType,
                                                      uint32 cnt) const
{
    return m_transactionList.AllTransactionsHold2PLockMoreThan(tag, mode, mgrType, cnt);
}

RetStatus ThreadContext::ActuallyAcquireLazyLocksOnCurrentThread()
{
    return m_transactionList.ActuallyAcquireLazyLocksOnCurrentThread();
}

void ThreadContext::ReleaseAllTransactionLocks()
{
    m_transactionList.ReleaseLocksInAllTransactionRes();
}

DstoreMemoryContext ThreadContext::GetTransactionMemoryContext()
{
    /* Get transaction-level memory context */
    return GetActiveTransaction()->GetMemoryContext();
}

DstoreMemoryContext ThreadContext::GetTopTransactionMemoryContext()
{
    /* Get top transaction memory context */
    return GetTopTransaction()->GetMemoryContext();
}

DstoreMemoryContext ThreadContext::GetQueryMemoryContext()
{
    return m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY);
}

StorageSession *ThreadContext::GetSession()
{
    return m_session;
}

BufMgrInterface *ThreadContext::GetTmpLocalBufMgr()
{
    StorageAssert(m_session != nullptr);
    return m_session->tmpLocalBufMgr;
}

void ThreadContext::DetachSessionFromThread()
{
    m_session = nullptr;
}

DstoreMemoryContext ThreadContext::GetSessionMemoryCtx()
{
    if (m_session) {
        return m_session->GetRoot();
    } else {
        return g_storageInstance->GetMemoryMgr()->GetRoot();
    }
}

DstoreMemoryContext ThreadContext::GetGlobalSmgrMemoryCtx()
{
    return g_storageInstance->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE);
}

BufPrivateRefCount *ThreadContext::GetBufferPrivateRefCount()
{
    Transaction* trx = GetActiveTransaction();
    if (STORAGE_VAR_NULL(trx)) {
        return bufferPrivateRefCount;
    }
    return trx->GetBufferPrivateRefCount();
}

void ThreadContext::AppendTransactionDebugInfo(StringInfo buf)
{
    Transaction *transaction = GetActiveTransaction();
    if (transaction != nullptr && !transaction->ReadOnly()) {
        buf->append(" Xid(%d, %lu) Cid(%u)", static_cast<int32>(transaction->GetCurrentXid().m_zoneId),
                    transaction->GetCurrentXid().m_logicSlotId, transaction->GetCurCid());
    }
}

void ThreadContext::AppendErrorDebugInfo(StringInfo buf)
{
    if (error->GetErrorCode() != STORAGE_OK) {
        buf->append(" ErrorCode(%lld) ErrorMsg(%s)", error->GetErrorCode(), error->GetMessage());
    }
}

void ThreadContext::Sleep(const struct timespec *timeout)
{
    threadCore.core->futex.DstoreFutexWait(false, timeout);
}

void ThreadContext::Wakeup()
{
    threadCore.core->futex.DstoreFutexPost();
}

ErrorCode ThreadContext::GetErrorCode()
{
    return error->GetErrorCode();
}

const char *ThreadContext::GetErrorMessage()
{
    return error->GetMessage();
}

NodeId ThreadContext::GetErrorNodeId()
{
    return error->GetErrorNodeId();
}

void ThreadContext::ResetErrorMemory()
{
    m_memoryMgr->reset_group_context(MEMORY_CONTEXT_ERROR);
}

void ThreadContext::ResetQueryMemory()
{
    m_memoryMgr->reset_group_context(MEMORY_CONTEXT_QUERY);
    m_uniqueQueryId = 0;
}

void ThreadContext::ResetSmgrMemory()
{
    if (m_memoryMgr != nullptr) {
        m_memoryMgr->reset_group_context(MEMORY_CONTEXT_SMGR);
    }
}

void ThreadContext::DestroyPrivateBufferCollector()
{
    DstorePfreeAligned(m_privateBufCollector.privateBufArray);
    if (m_privateBufCollector.privateBufTable != nullptr) {
        hash_destroy(m_privateBufCollector.privateBufTable);
    }
}

void ThreadContext::Destroy()
{
    if (STORAGE_VAR_NULL(m_memoryMgr)) {
        return;
    }
    storage_trace_entry(TRACE_ID_ThreadContext__destroy);

    m_transactionList.Destroy();

    if (m_walWriterContext != nullptr) {
        delete m_walWriterContext;
        m_walWriterContext = nullptr;
    }

    if (m_undoMemoryContext) {
        DstoreMemoryContextDelete(m_undoMemoryContext);
        m_undoMemoryContext = nullptr;
    }
    m_undoContext = nullptr;
    if (bufferPrivateRefCount != nullptr) {
        AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetRoot());
        bufferPrivateRefCount->Destroy();
        delete bufferPrivateRefCount;
        bufferPrivateRefCount = nullptr;
    }

    DstorePfreeAligned(lwlockContext.held_lwlocks);
    DestroyPrivateBufferCollector();
    lwlockContext.num_held_lwlocks = 0;
    lwlockContext.held_lwlocks = nullptr;
    CloseLoggerThread();
    if (!STORAGE_VAR_NULL(error)) {
        delete error;
    }
    StorageMemoryMgr::destroy(m_memoryMgr);
    m_memoryMgr = nullptr;
    storage_trace_exit(TRACE_ID_ThreadContext__destroy);
}

void ThreadContext::SetLocalCsn(CommitSeqNo csn)
{
    StorageAssert(threadCore.xactLock != NULL);
    gs_atomic_uint64 *csnMin = &threadCore.xact->csnMin;
    CommitSeqNo curCsnMin = GsAtomicReadU64(csnMin);
    if ((csn == INVALID_CSN || csn == MAX_COMMITSEQNO) || (curCsnMin == INVALID_CSN || curCsnMin == MAX_COMMITSEQNO)) {
        /*
        * Note: the curCsnMin will be updated in GsAtomicCompareExchangeU64
        *
        * We use a CAS instruction here to make sure the others can see the value asap. Otherwise, they may see a
        * spurious value in their CPU cache and burning cycles in vain (which may incur inconsistencies as well).
        * One should notice that the others must use a similar instruction (CAS or full memory barrier + load) to
        * peek the latest value. The CAS must succeed at its first shot, because we can guarantee that only
        * the thread itself will change it.
        */
        bool isSuccess = GsAtomicCompareExchangeU64(csnMin, &curCsnMin, csn);
        StorageAssert(isSuccess);
        UNUSED_VARIABLE(isSuccess); /* Mute compile warning in release mode. */
    }
}

CommitSeqNo ThreadContext::GetLocalCsn()
{
    StorageAssert(threadCore.xactLock != NULL);
    return GsAtomicReadU64(&threadCore.xact->csnMin);
}

void ThreadContext::SetLocalXid(uint64 xid)
{
    StorageAssert(threadCore.xactLock != nullptr);
    gs_atomic_uint64 *atomicXid = &threadCore.xact->currentActiveXid;
    uint64 curXid = GsAtomicReadU64(atomicXid);

    /*
     * Note: the curXid will be updated in GsAtomicCompareExchangeU64
     *
     * We use a CAS instruction here to make sure the others can see the value asap like function of SetLocalCsn().
     * Only the thread itself will change it.
     */
    bool isSuccess = GsAtomicCompareExchangeU64(atomicXid, &curXid, xid);
    StorageAssert(isSuccess);
    UNUSED_VARIABLE(isSuccess); /* Mute compile warning in release mode. */
}

void ThreadContext::StartTacFirstStatementCsnMinExpiryTimer()
{
    /* Set expiry time for firstStatementCsnmin if firstStatementCsnMin is valid */
    if (!DstoreCsnIsValid(thrd->GetXact()->firstStatementCsnMin)) {
        return;
    }
    StorageAssert(threadCore.xactLock != nullptr);
    gs_atomic_uint64 *firstStmtCsnExpiryTime = &threadCore.xact->firstStatementCsnMinExpiryTimestamp;
    uint64 currExpiryTime = GsAtomicReadU64(firstStmtCsnExpiryTime);
    TimestampTz timeIncrement = static_cast<TimestampTz>(g_storageInstance->GetGuc()->tacGracePeriod);
    uint64 expireTime = static_cast<uint64>(GetCurrentTimestampInSecond() + timeIncrement);
    bool isSuccess = GsAtomicCompareExchangeU64(firstStmtCsnExpiryTime, &currExpiryTime, expireTime);
    StorageAssert(isSuccess);
    UNUSED_VARIABLE(isSuccess); /* Mute compile warning in release mode. */
}

Xid ThreadContext::GetCurrentXid()
{
    Xid xid = INVALID_XID;
    if (unlikely(GetActiveTransaction() != nullptr)) {
        xid = GetActiveTransaction()->GetCurrentXid();
    }
    return xid;
}

void ThreadContext::SetSnapshotCsnForFlashback(CommitSeqNo csn)
{
    GetActiveTransaction()->SetSnapshotCsnForFlashback(csn);
}

void ThreadContext::SetFlashbackCsnMin(CommitSeqNo flashbackCsnMin)
{
    g_storageInstance->GetCsnMgr()->SetFlashbackCsnMin(flashbackCsnMin);
}

CommitSeqNo ThreadContext::GetSnapShotCsn()
{
    return GetActiveTransaction()->GetSnapshotCsn();
}

CommitSeqNo ThreadContext::GetNextCsn()
{
    CommitSeqNo nextCsn;
    if (STORAGE_FUNC_FAIL(g_storageInstance->GetCsnMgr()->GetNextCsn(nextCsn, false))) {
        StorageAssert(0);
    }
    return nextCsn;
}

void ThreadContext::SetSnapshotCsnForTac(CommitSeqNo csn)
{
    GetActiveTransaction()->SetSnapshotCsnForTac(csn);
}

void ThreadContext::CleanupTacSnapshot(bool resetXactFirstStmtCsnMin)
{
    UNUSED_VARIABLE(resetXactFirstStmtCsnMin);
    thrd->m_tacSnapshotReceived.snapshotcsn = INVALID_CSN;
    thrd->m_tacSnapshotToSend.snapshotcsn = INVALID_CSN;
}

void *ThreadContext::GetPrivateBuffer(BufferTag bufTag)
{
    PrivateBufferEntry *entry = nullptr;
    for (uint i = 0; i < m_privateBufCollector.privateBufMaxValidPos; ++i) {
        entry = &m_privateBufCollector.privateBufArray[i];
        if (entry->bufTag.pageId == bufTag.pageId && entry->bufTag.pdbId == bufTag.pdbId) {
            return entry->addr;
        }
    }
    if (m_privateBufCollector.privateBufTable != nullptr) {
        entry = static_cast<PrivateBufferEntry *>(
            hash_search(m_privateBufCollector.privateBufTable, static_cast<void *>(&bufTag), HASH_FIND, nullptr));
        if (entry == nullptr) {
            return nullptr;
        } else {
            return entry->addr;
        }
    }
    return nullptr;
}

void ThreadContext::BuildPrivateBufferTbl()
{
    PrivateBufferEntry *entry = nullptr;
    HASHCTL ctl{};
    ctl.keysize = sizeof(BufferTag);
    ctl.entrysize = sizeof(PrivateBufferEntry);
    ctl.hash = buf_hash;
    m_privateBufCollector.privateBufTable =
        hash_create("PrivateBufferTable", THRD_MAX_PRIVATE_BUF_SIZE, &ctl, HASH_ELEM | HASH_FUNCTION);
    if (m_privateBufCollector.privateBufTable == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Build PrivateBufferTable failed."));
    }
    for (uint i = 0; i < PRIVATEBUFF_ARRAY_ENTRIES; ++i) {
        bool isFound = false;
        entry = static_cast<PrivateBufferEntry *>(hash_search(m_privateBufCollector.privateBufTable,
                                                              &m_privateBufCollector.privateBufArray[i].bufTag,
                                                              HASH_ENTER, &isFound));
        StorageAssert(!isFound && entry != nullptr);
        StorageReleasePanic((entry == nullptr), MODULE_WAL, ErrMsg("Build PrivateBufferTable failed, entry is null."));
        entry->addr = m_privateBufCollector.privateBufArray[i].addr;
    }
    return;
}

void ThreadContext::SetPrivateBuffer(BufferTag bufTag, void *addr)
{
    PrivateBufferEntry *entry;
    /* privateBufArray is not full */
    if (m_privateBufCollector.privateBufMaxValidPos < PRIVATEBUFF_ARRAY_ENTRIES) {
        m_privateBufCollector.privateBufArray[m_privateBufCollector.privateBufMaxValidPos].bufTag.pageId =
            bufTag.pageId;
        m_privateBufCollector.privateBufArray[m_privateBufCollector.privateBufMaxValidPos].bufTag.pdbId = bufTag.pdbId;
        m_privateBufCollector.privateBufArray[m_privateBufCollector.privateBufMaxValidPos].addr = addr;
        m_privateBufCollector.privateBufMaxValidPos++;
    }
    /* Once privateBufArray is full, create PrivateBuffer hashtable. */
    if (m_privateBufCollector.privateBufMaxValidPos == PRIVATEBUFF_ARRAY_ENTRIES &&
        m_privateBufCollector.privateBufTable == nullptr) {
        BuildPrivateBufferTbl();
    }
    if (m_privateBufCollector.privateBufTable != nullptr) {
        bool isFound = false;
        entry = static_cast<PrivateBufferEntry *>(
            hash_search(m_privateBufCollector.privateBufTable, &bufTag, HASH_ENTER, &isFound));
        StorageAssert(!isFound && entry != nullptr);
        entry->addr = addr;
    }
    m_privateBufCollector.privateBufTotalNum++;
    return;
}

void ThreadContext::DelPrivateBuffer(BufferTag bufTag)
{
    bool found = false;
    PrivateBufferEntry *entry = nullptr;
    for (uint i = 0; i < m_privateBufCollector.privateBufMaxValidPos; i++) {
        entry = &m_privateBufCollector.privateBufArray[i];
        /* choosing the last entry to replacing current entry */
        if (entry->bufTag.pageId == bufTag.pageId && entry->bufTag.pdbId == bufTag.pdbId) {
            m_privateBufCollector.privateBufArray[i] =
                m_privateBufCollector.privateBufArray[m_privateBufCollector.privateBufMaxValidPos - 1];
            m_privateBufCollector.privateBufMaxValidPos--;
            found = true;
            break;
        }
    }
    if (m_privateBufCollector.privateBufTable != nullptr) {
        entry = static_cast<PrivateBufferEntry *>(
            hash_search(m_privateBufCollector.privateBufTable, static_cast<void *>(&bufTag), HASH_REMOVE, &found));
        StorageAssert(entry != nullptr);
    }
    if (found) {
        m_privateBufCollector.privateBufTotalNum--;
    }
}

RetStatus ThreadContext::DstoreThreadTryInitialize(const char *threadName, PdbId pdbId)
{
    if (unlikely(DSTORE::ThreadContextInterface::GetCurrentThreadContext() == nullptr)) {
        StorageAssert(g_storageInstance != nullptr);
        (void)ThreadContextInterface::Create();
        if (STORAGE_VAR_NULL(thrd)) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to create thread context for thread %s", threadName));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(thrd->InitializeBasic())) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to initialize basic thread context."));
            goto ERROR_EXIT;
        }
        if (STORAGE_FUNC_FAIL(thrd->InitStorageContext(pdbId))) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to init storage context."));
            goto ERROR_EXIT;
        }
        g_storageInstance->AddVisibleThread(thrd, pdbId, threadName);
        /* InitTransactionRuntime depends on the successful execution of AddVisibleThread. */
        if (STORAGE_FUNC_FAIL(thrd->InitTransactionRuntime(pdbId, nullptr, nullptr, false))) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to init transaction runtime."));
            goto ERROR_EXIT;
        }
    }
    return DSTORE_SUCC;

ERROR_EXIT:
    g_storageInstance->RemoveVisibleThread(thrd);
    DSTORE::ThreadContextInterface::DestroyCurrentThreadContext();
    return DSTORE_FAIL;
}

void ThreadContext::DstoreThreadTryDestory()
{
    if (thrd != nullptr) {
        g_storageInstance->RemoveVisibleThread(thrd);
        DSTORE::ThreadContextInterface::DestroyCurrentThreadContext();
    }
}

RetStatus ThreadContext::CheckforInterrupts()
{
    if (unlikely(threadCore.core != NULL) && unlikely(threadCore.core->interruptHoldoffCount != NULL)) {
        if (unlikely(*(threadCore.core->interruptHoldoffCount) != 0)) {
            return DSTORE_SUCC;
        }
    }

    if (unlikely(IsInterruptPending()) && !AreInterruptsHeld()) {
        storage_set_error(SQL_WARNING_REQUEST_ARE_CANCELED);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Statements of the current request are canceled."));
        ClearInterruptPending();
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void ThreadContext::RefreshWorkingVersionNum()
{
    StorageAssert(g_storageInstance->m_working_version_num != nullptr);
    m_working_version_num = g_storageInstance->GetWorkingVersionNum();
}

void ThreadContext::SetThreadMemLevel(ThreadMemoryLevel level)
{
    m_threadmemlevel = level;
}

void MemsetForThreadcontext(void** ptr, uint32 size)
{
    const size_t MAX_BLOCK = 1024 * 1024 * 1024;  // 1GB per fill
    errno_t rc = 0;
    for (size_t offset = 0; offset < size; offset += MAX_BLOCK) {
        size_t block_size = (offset + MAX_BLOCK <= size) ? MAX_BLOCK : (size - offset);
        rc = memset_s((char *)ptr + offset, block_size, 0, block_size);
        storage_securec_check(rc, "\0", "\0");
    }
}

RetStatus ThreadCoreMgr::Init(uint32 maxThreadNum, uint32 numaNodeNum)
{
    StorageAssert(m_coreArray == nullptr);
    StorageAssert(m_xactArray == nullptr);
    StorageAssert(m_lockArray == nullptr);
    StorageAssert(m_sortCoreIdxArr == nullptr);
    AutoMemCxtSwitch autoSwitch{g_storageInstance->GetMemoryMgr()->GetRoot()};
    /* Number of core resources reserved for dstore internal threads */
#define DSTORE_RESERVED_THREADS_CORE_NUM 128
    m_coreArrayNum = maxThreadNum + DSTORE_RESERVED_THREADS_CORE_NUM;
    m_numaNodeNum = numaNodeNum;

    uint64 size = sizeof(ThreadCore *) * m_numaNodeNum;
    m_freeCoreList = static_cast<ThreadCore **>(DstorePallocAligned(size, DSTORE_CACHELINE_SIZE));
    errno_t rc = memset_s(m_freeCoreList, size, 0, size);
    storage_securec_check(rc, "\0", "\0");

    size = sizeof(DstoreSpinLock) * m_numaNodeNum;
    m_spinLock = static_cast<DstoreSpinLock *>(DstorePallocAligned(size, DSTORE_CACHELINE_SIZE));
    if (STORAGE_VAR_NULL(m_spinLock)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to allocate memory for LockThreadContext m_xactArray."));
        return DSTORE_FAIL;
    }
    rc = memset_s(m_spinLock, size, 0, size);
    storage_securec_check(rc, "\0", "\0");
    for (uint8 i = 0; i < m_numaNodeNum; i++) {
        m_spinLock[i].Init();
    }

    bool useNuma = false;
    m_coreArray = static_cast<ThreadCorePad **>(
        DstorePallocAligned(m_coreArrayNum * sizeof(ThreadCore *), DSTORE_CACHELINE_SIZE));
    m_xactArray = static_cast<ThreadXactPad **>(
        DstorePallocAligned(m_coreArrayNum * sizeof(ThreadXact *), DSTORE_CACHELINE_SIZE));
    m_lockArray = static_cast<LockThreadContext **>(
        DstorePallocAligned(m_coreArrayNum * sizeof(LockThreadContext *), DSTORE_CACHELINE_SIZE));
    m_nameArray = static_cast<char *>(
        DstorePallocAligned(m_coreArrayNum * sizeof(char) * THREAD_NAME_MAX_LENGTH, DSTORE_CACHELINE_SIZE));

    m_sortCoreIdxArr = (uint32 *)DstorePallocAligned(m_coreArrayNum * sizeof(uint32), DSTORE_CACHELINE_SIZE);
    m_sortCoreIdxArrBound = 0;
    LWLockInitialize(&m_coreLock.lock, LWLOCK_GROUP_THREAD_CORE);
    LWLockInitialize(&m_xactLock.lock, LWLOCK_GROUP_THREAD_XACT);

    size = sizeof(ThreadCorePad *) * m_numaNodeNum;
    ThreadCorePad **initCores = static_cast<ThreadCorePad **>(DstorePallocAligned(size, DSTORE_CACHELINE_SIZE));
    rc = memset_s(initCores, size, 0, size);
    storage_securec_check(rc, "\0", "\0");

    size = sizeof(ThreadXactPad *) * m_numaNodeNum;
    ThreadXactPad **initXacts = static_cast<ThreadXactPad **>(DstorePallocAligned(size, DSTORE_CACHELINE_SIZE));
    rc = memset_s(initXacts, size, 0, size);
    storage_securec_check(rc, "\0", "\0");

    size = sizeof(LockThreadContext *) * m_numaNodeNum;
    LockThreadContext **initLocks = static_cast<LockThreadContext **>(DstorePallocAligned(size, DSTORE_CACHELINE_SIZE));
    rc = memset_s(initLocks, size, 0, size);
    storage_securec_check(rc, "\0", "\0");
#ifdef DSTORE_USE_NUMA
    uint32 groupCnt = (m_coreArrayNum + m_numaNodeNum - 1) / m_numaNodeNum;
    if (m_numaNodeNum > 1) {
        useNuma = true;
        for (uint8 nodeNo = 0; nodeNo < m_numaNodeNum; nodeNo++) {
            size_t allocSize = sizeof(ThreadCorePad) * groupCnt;
            initCores[nodeNo] = (ThreadCorePad *)numa_alloc_onnode(allocSize, nodeNo);
            rc = memset_s(initCores[nodeNo], allocSize, 0, allocSize);
            storage_securec_check(rc, "\0", "\0");

            allocSize = sizeof(ThreadXactPad) * groupCnt;
            initXacts[nodeNo] = (ThreadXactPad *)numa_alloc_onnode(allocSize, nodeNo);
            rc = memset_s(initXacts[nodeNo], allocSize, 0, allocSize);
            storage_securec_check(rc, "\0", "\0");

            allocSize = sizeof(LockThreadContext) * groupCnt;
            char *lockMem = (char *)numa_alloc_onnode(allocSize, nodeNo);
            rc = memset_s(lockMem, allocSize, 0, allocSize);
            storage_securec_check(rc, "\0", "\0");
            initLocks[nodeNo] = reinterpret_cast<LockThreadContext *>(lockMem);
        }
    }
#endif
    if (!useNuma) {
        size = m_coreArrayNum * sizeof(ThreadCorePad);
        if (size <= MaxAllocSize) {
            initCores[0] = (ThreadCorePad *)DstorePallocAligned(size, DSTORE_CACHELINE_SIZE);
        } else {
            initCores[0] = (ThreadCorePad *)DstorePallocAlignedHugeMemory(size, DSTORE_CACHELINE_SIZE);
        }

        if (STORAGE_VAR_NULL(initCores[0])) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to alloc memory for thread core initCores, which size is %lu", size));
            return DSTORE_FAIL;
        }

        MemsetForThreadcontext((void**)initCores[0], size);

        size = m_coreArrayNum * sizeof(ThreadXactPad);
        if (size <= MaxAllocSize) {
            initXacts[0] = (ThreadXactPad *)DstorePallocAligned(size, DSTORE_CACHELINE_SIZE);
        } else {
            initXacts[0] = (ThreadXactPad *)DstorePallocAlignedHugeMemory(size, DSTORE_CACHELINE_SIZE);
        }

        if (STORAGE_VAR_NULL(initXacts[0])) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to alloc memory for thread core initXacts, which size is %lu", size));
            return DSTORE_FAIL;
        }

        MemsetForThreadcontext((void**)initXacts[0], size);

        size = m_coreArrayNum * sizeof(LockThreadContext);
        if (size <= MaxAllocSize) {
            initLocks[0] = (LockThreadContext *)DstorePallocAligned(size, DSTORE_CACHELINE_SIZE);
        } else {
            initLocks[0] = (LockThreadContext *)DstorePallocAlignedHugeMemory(size, DSTORE_CACHELINE_SIZE);
        }

        if (STORAGE_VAR_NULL(initLocks[0])) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to alloc memory for thread core initLocks, which size is %lu", size));
            return DSTORE_FAIL;
        }

        MemsetForThreadcontext((void**)initLocks[0], size);
    }

    for (uint32 i = 0; i < m_coreArrayNum; i++) {
        /* set core pointer to actural position */
        m_coreArray[i] = reinterpret_cast<ThreadCorePad *>(
            (reinterpret_cast<char *>(initCores[i % m_numaNodeNum]) + (i / m_numaNodeNum) * sizeof(ThreadCorePad)));
        if (STORAGE_VAR_NULL(m_coreArray[i])) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to allocate memory for LockThreadContext m_coreArray, thread num is: %u.", i));
            DstorePfreeAligned(initCores);
            return DSTORE_FAIL;
        }
        m_xactArray[i] = reinterpret_cast<ThreadXactPad *>(
            (reinterpret_cast<char *>(initXacts[i % m_numaNodeNum]) + (i / m_numaNodeNum) * sizeof(ThreadXactPad)));
        if (STORAGE_VAR_NULL(m_xactArray[i])) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to allocate memory for LockThreadContext m_xactArray, thread num is: %u.", i));
            DstorePfreeAligned(initXacts);
            DstorePfreeAligned(initCores);
            return DSTORE_FAIL;
        }
        /* In order to control memory in a better way (e.g. for numa), we use specific memory alloc function
         * instead of 'new', and the class's constructor is not called.
         * It will coredump immediately when we attempt to access their virtual member functions,
         * because virtual function table does not exist.
         * Using placement new to solve this problem, pass a preallocated memory and construct an object in it.
         * We need to call destructor EXPLICITLY to trigger derived class's destruct.
         */
        m_lockArray[i] = DstoreNew(reinterpret_cast<char *>(initLocks[i % m_numaNodeNum]) +
                                   (i / m_numaNodeNum) * sizeof(LockThreadContext)) LockThreadContext();
        if (STORAGE_VAR_NULL(m_lockArray[i])) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to allocate memory for LockThreadContext m_lockArray, thread num is: %u.", i));
            DstorePfreeAligned(initLocks);
            DstorePfreeAligned(initXacts);
            DstorePfreeAligned(initCores);
            return DSTORE_FAIL;
        }
    }

    ThreadCore **prev = static_cast<ThreadCore **>(DstorePalloc0(sizeof(ThreadCore *) * m_numaNodeNum));
    if (STORAGE_VAR_NULL(prev)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Faied to allocate memory for ThreadCore pointer array."));
        DstorePfreeAligned(initLocks);
        DstorePfreeAligned(initXacts);
        DstorePfreeAligned(initCores);
        return DSTORE_FAIL;
    }

    ThreadCore unusedHead[4];
    for (uint32 i = 0; i < m_numaNodeNum; i++) {
        prev[i] = &unusedHead[i];
    }

    for (uint32 i = 0; i < m_coreArrayNum; i++) {
        /* set core pointer to actural position */
        uint32 numaNodeId = i % m_numaNodeNum;
        if (STORAGE_FUNC_FAIL(m_coreArray[i]->m_core.Init(i, static_cast<int>(numaNodeId)))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Faiiled to initialize thread core, thread id is: %u.", i));
            DstorePfreeAligned(initLocks);
            DstorePfreeAligned(initXacts);
            DstorePfreeAligned(initCores);
            return DSTORE_FAIL;
        }
        m_xactArray[i]->m_xact.Init();
        m_lockArray[i]->InitializeBasic();
        m_coreArray[i]->m_core.selfIdx = i;
        m_coreArray[i]->m_core.regularLockCtx = m_lockArray[i];
        m_coreArray[i]->m_core.threadPerfCounter = (ThreadPerfCounter *)DstoreMemoryContextAlloc(
            g_storageInstance->GetMemoryMgr()->GetRoot(), sizeof(ThreadPerfCounter));
        if (STORAGE_VAR_NULL(m_coreArray[i]->m_core.threadPerfCounter)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Faied to allocate memory for ThreadPerfCounter, thread id is: %u.", i));
            DstorePfreeAligned(initLocks);
            DstorePfreeAligned(initXacts);
            DstorePfreeAligned(initCores);
            return DSTORE_FAIL;
        }

        if (STORAGE_FUNC_FAIL(m_coreArray[i]->m_core.threadPerfCounter->Init())) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Faied to initialize ThreadPerfCounter, thread id is: %u.", i));
            DstorePfreeAligned(initLocks);
            DstorePfreeAligned(initXacts);
            DstorePfreeAligned(initCores);
            return DSTORE_FAIL;
        }

        m_coreArray[i]->m_core.threadName = m_nameArray + i * THREAD_NAME_MAX_LENGTH;
        m_coreArray[i]->m_core.threadName[0] = '\0';
        if (STORAGE_VAR_NULL(m_sortCoreIdxArr)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Faied to set m_sortCoreIdxArr, thread id is: %u.", i));
            DstorePfreeAligned(initLocks);
            DstorePfreeAligned(initXacts);
            DstorePfreeAligned(initCores);
            return DSTORE_FAIL;
        }
        m_sortCoreIdxArr[i] = INVALID_THREAD_CORE_ID;
        prev[numaNodeId]->next = &m_coreArray[i]->m_core;
        prev[numaNodeId] = &m_coreArray[i]->m_core;
    }

    for (uint32 i = 0; i < m_numaNodeNum; i++) {
        prev[i]->next = nullptr;
    }
    for (uint32 i = 0; i < m_numaNodeNum; i++) {
        m_freeCoreList[i] = &m_coreArray[i]->m_core;
    }

    DstorePfreeAligned(initLocks);
    DstorePfreeAligned(initXacts);
    DstorePfreeAligned(initCores);
    return DSTORE_SUCC;
}

void ThreadCoreMgr::Destroy()
{
    bool useNuma = false;
    for (uint32 i = 0; i < m_coreArrayNum; i++) {
        DstorePfreeExt(m_coreArray[i]->m_core.threadPerfCounter->walPerfCounter);
        DstorePfreeExt(m_coreArray[i]->m_core.threadPerfCounter);
        m_lockArray[i]->~LockThreadContext();
    }
#ifdef DSTORE_USE_NUMA
    if (m_numaNodeNum > 1) {
        useNuma = true;
        int groupCnt = static_cast<int>((m_coreArrayNum + m_numaNodeNum - 1) / m_numaNodeNum);
        for (uint8 nodeNo = 0; nodeNo < m_numaNodeNum; nodeNo++) {
            size_t allocSize = sizeof(ThreadCorePad) * groupCnt;
            numa_free(m_coreArray[nodeNo], allocSize);
            allocSize = sizeof(ThreadXactPad) * groupCnt;
            numa_free(m_xactArray[nodeNo], allocSize);
            allocSize = sizeof(LockThreadContext) * groupCnt;
            numa_free(m_lockArray[nodeNo], allocSize);
        }
    }
#endif
    if (!useNuma) {
        DstorePfreeAligned(m_coreArray[0]);
        m_coreArray[0] = nullptr;
        DstorePfreeAligned(m_xactArray[0]);
        m_xactArray[0] = nullptr;
        DstorePfreeAligned(m_lockArray[0]);
        m_lockArray[0] = nullptr;
    }
    DstorePfreeAligned(m_coreArray);
    m_coreArray = nullptr;
    DstorePfreeAligned(m_nameArray);
    m_nameArray = nullptr;
    DstorePfreeAligned(m_xactArray);
    m_xactArray = nullptr;
    DstorePfreeAligned(m_lockArray);
    m_lockArray = nullptr;
    DstorePfreeAligned(m_sortCoreIdxArr);
    m_sortCoreIdxArr = nullptr;
    DstorePfreeAligned(m_freeCoreList);
    m_freeCoreList = nullptr;
    m_usedCoreNum = 0;
    DstorePfreeAligned(m_spinLock);
    m_spinLock = nullptr;
}

RetStatus ThreadCoreMgr::CheckDuplicateThreadId(const ThreadId threadId)
{
    ThreadCore *core = nullptr;
    ThreadIterator iter(this);
    while ((core = iter.GetNextThreadCore()) != nullptr) {
        if (unlikely(core->pid == threadId)) {
            ErrLog(DSTORE_PANIC, MODULE_COMMON,
                   ErrMsg("ThreadId %lu [%s] already exists, start time is [%ld], UnregisterThread before add again.",
                          threadId, core->threadName, core->startTime));
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

RetStatus ThreadCoreMgr::ThreadBindCoreSlot(PdbId pdbId, ThreadContext *threadContext, ThreadCorePayload &threadCore,
                                            volatile uint32_t *InterruptHoldoffCount)
{
    const ThreadId threadId = pthread_self();
    ThreadCore *core = GetFreeCore();
    StorageAssert(core);
    if (unlikely(core == nullptr)) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("Core is empty, Pdb id is: %d.", pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = core->regularLockCtx->Initialize();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
            ErrMsg("Lock thread context init failed, Pdb id is: %d, failed with errcode:%lld.",
            pdbId, StorageGetErrorCode()));
        return ret;
    }
    core->pid = threadId;
    core->lwpid = GET_TID();
    TsAnnotateHappensBefore(&core->pid);
    threadCore.core = core;
    threadCore.core->threadContext = threadContext;
    threadCore.selfIdx = core->selfIdx;
    threadCore.xact = &m_xactArray[core->selfIdx]->m_xact;
    threadCore.xact->pdbId = pdbId;
    threadCore.xactLock = &m_xactLock.lock;
    threadCore.core->interruptHoldoffCount = InterruptHoldoffCount;
    StorageAssert(GsAtomicReadU64(&threadCore.xact->csnMin) != MAX_COMMITSEQNO);

#ifdef DSTORE_USE_ASSERT_CHECKING
    /* We cannot bind same thread twice in different core slot, for in that case,
     * traversing via ThreadIterator and compare by pid might not get the correct threadCore.
     * Since AddVisibleThread is an external interface, we could do an extra check here.
     */
    if (STORAGE_FUNC_FAIL(CheckDuplicateThreadId(threadId))) {
        return DSTORE_FAIL;
    }
#endif
    return DSTORE_SUCC;
}

void ThreadCoreMgr::InsertSortArray(const ThreadCorePayload &threadCore)
{
    errno_t rc;
    uint32 index = 0;
    DstoreLWLockAcquire(&m_coreLock.lock, LW_EXCLUSIVE);
    StorageAssert(m_sortCoreIdxArrBound < m_coreArrayNum);
    if (m_sortCoreIdxArrBound > 0) {
        index = static_cast<uint32>(
            std::lower_bound(m_sortCoreIdxArr, m_sortCoreIdxArr + m_sortCoreIdxArrBound, threadCore.selfIdx) -
            m_sortCoreIdxArr);
        rc = memmove_s(&m_sortCoreIdxArr[index + 1], m_sortCoreIdxArrBound * sizeof(int), &m_sortCoreIdxArr[index],
                       (m_sortCoreIdxArrBound - index) * sizeof(int));
        storage_securec_check(rc, "\0", "\0");
    }
    m_sortCoreIdxArr[index] = threadCore.core->selfIdx;
    m_sortCoreIdxArrBound++;
    StorageAssert(GsAtomicReadU64(&threadCore.xact->csnMin) != MAX_COMMITSEQNO);
    threadCore.core->regularLockCtx->InitializeLazyLockHint();
    LWLockRelease(&m_coreLock.lock);
}

void ThreadCoreMgr::RemoveSortArray(ThreadCorePayload &threadCore)
{
    errno_t rc;
    DstoreLWLockAcquire(&m_coreLock.lock, LW_EXCLUSIVE);
    StorageAssert(m_sortCoreIdxArrBound < m_coreArrayNum);
    StorageAssert(GsAtomicReadU64(&threadCore.xact->csnMin) != MAX_COMMITSEQNO);
    uint32 index = static_cast<uint32>(
        std::lower_bound(m_sortCoreIdxArr, m_sortCoreIdxArr + m_sortCoreIdxArrBound, threadCore.selfIdx) -
        m_sortCoreIdxArr);
    StorageAssert(index != m_sortCoreIdxArrBound);
    StorageAssert(m_sortCoreIdxArr[index] == threadCore.selfIdx);
    rc = memmove_s(&m_sortCoreIdxArr[index], m_sortCoreIdxArrBound * sizeof(int), &m_sortCoreIdxArr[index + 1],
                   ((m_sortCoreIdxArrBound - index) - 1) * sizeof(int));
    storage_securec_check(rc, "\0", "\0");
    m_sortCoreIdxArr[m_sortCoreIdxArrBound - 1] = INVALID_THREAD_CORE_ID;
    m_sortCoreIdxArrBound--;
    threadCore.xact->csnMin = INVALID_CSN;
    threadCore.xact->firstStatementCsnMin = INVALID_CSN;
    threadCore.xact->firstStatementCsnMinExpiryTimestamp = 0;
    threadCore.xact->currentActiveXid = INVALID_XID.m_placeHolder;
    threadCore.xact->pdbId = INVALID_PDB_ID;
    LWLockRelease(&m_coreLock.lock);
}

void ThreadCoreMgr::ThreadUnbindCoreSlot(ThreadCorePayload &threadCore)
{
    StorageAssert(GsAtomicReadU64(&threadCore.xact->csnMin) != MAX_COMMITSEQNO);
    threadCore.core->pid = INVALID_THREAD_ID;
    threadCore.core->threadContext = nullptr;
    threadCore.xact->csnMin = INVALID_CSN;
    threadCore.xact->firstStatementCsnMin = INVALID_CSN;
    threadCore.xact->firstStatementCsnMinExpiryTimestamp = 0;
    threadCore.xact->currentActiveXid = INVALID_XID.m_placeHolder;
    threadCore.xact->pdbId = INVALID_PDB_ID;
    threadCore.selfIdx = INVALID_THREAD_CORE_ID;
    threadCore.core->regularLockCtx->Destroy();
    threadCore.core->threadPerfCounter->walPerfCounter->Reset();
    threadCore.core->immediateAbort = false;
    threadCore.core->interruptHoldoffCount = nullptr;

    AddFreeCore(threadCore.core);
    threadCore.core = nullptr;
}

ThreadCore *ThreadCoreMgr::GetSpecifiedCore(uint32 idx)
{
    if (idx >= m_coreArrayNum) {
        return nullptr;
    }
    return &m_coreArray[idx]->m_core;
}

void ThreadCoreMgr::WakeupAllThreads()
{
    ThreadIterator iter(this);
    ThreadCore *core = nullptr;
    while ((core = iter.GetNextThreadCore()) != nullptr) {
        core->Wakeup();
    }
}

/* GetThreadsInfo() - iter and get all threads' infos. */
RetStatus ThreadCoreMgr::GetThreadsInfo(ThreadStatsInfo **threadInfoArr, size_t *length)
{
    size_t idx = 0;
    DstoreMemoryContext context = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE);
    ThreadStatsInfo *tmpThreadInfoArr =
        (ThreadStatsInfo *)DstoreMemoryContextAllocZero(context, m_coreArrayNum * sizeof(ThreadStatsInfo));
    if (STORAGE_VAR_NULL(tmpThreadInfoArr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("GetThreadsInfo DstoreMemoryContextAlloc fail."));
        return DSTORE_FAIL;
    }

    /* Iter all threads. */
    ThreadCore *core = nullptr;
    ThreadCoreMgr::ThreadIterator iter(this);
    while ((core = iter.GetNextThreadCore()) != nullptr) {
        ThreadStatsInfo *curThreadInfo = &tmpThreadInfoArr[idx];
        curThreadInfo->pid = core->pid;
        curThreadInfo->startTime = core->startTime;
        curThreadInfo->lwpid = core->lwpid;
        if (core->threadName) {
            curThreadInfo->threadName = (char *)DstoreMemoryContextStrdup(context, core->threadName);
            if (STORAGE_VAR_NULL(curThreadInfo->threadName)) {
                ThreadContext::FreeThreadInfoArr(&tmpThreadInfoArr, idx);
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("GetThreadsInfo DstoreMemoryContextStrdup fail."));
                return DSTORE_FAIL;
            }
        } else {
            curThreadInfo->threadName = nullptr;
        }
        idx++;
    }

    *length = idx;
    *threadInfoArr = tmpThreadInfoArr;
    return DSTORE_SUCC;
}

CommitSeqNo ThreadCoreMgr::GetLocalCsnMin()
{
    /* we need both locks to gain consistent */
    DstoreLWLockAcquire(&m_coreLock.lock, LW_SHARED);
    CommitSeqNo localMinCsn = MAX_COMMITSEQNO;
    gs_atomic_uint64 *curCsn;
    uint64 expectCsn;
    for (uint32 i = 0; i < m_sortCoreIdxArrBound; i++) {
        curCsn = &m_xactArray[m_sortCoreIdxArr[i]]->m_xact.csnMin;
        expectCsn = MAX_COMMITSEQNO;
        while (GsAtomicCompareExchangeU64(curCsn, &expectCsn, MAX_COMMITSEQNO)) {
            /* Remember to reset the expect value, cuz it's been updated in GsAtomicCompareExchangeU64 */
            expectCsn = MAX_COMMITSEQNO;
            /*
             * When the current csn is MAX_COMMITSEQNO, the thread local csn is updating itself. We need to spin
             * around here until it's been modified. The reason using a CAS instruction is to make sure we can
             * see the latest value in each turn. It looks like we're modifying the current csn value here, but it
             * is actually just being reset to the same value if the value has not been changed. This is in a way
             * acting like a spin lock. Take a reference to what we're doing in SetLocalCsn.
             */
        }

        /* When we break out of the loop, expectCsn would've been updated to the latest value of curCsn. */
        if (expectCsn != INVALID_CSN && expectCsn < localMinCsn) {
            localMinCsn = expectCsn;
        }
        /*
         * For TAC, we need to check if firstStatementCsn is smaller than the local csn min.
         * This is because the data can still get recycled between the first statement till the end
         * of the transaction if we dont add firstStatementCsn into the localCsnMin calculation.
         */
        uint64 curFirstStatementCsn = GsAtomicReadU64(&m_xactArray[m_sortCoreIdxArr[i]]->m_xact.firstStatementCsnMin);
        uint64 curFirstStatementCsnExpiryTime =
            GsAtomicReadU64(&m_xactArray[m_sortCoreIdxArr[i]]->m_xact.firstStatementCsnMinExpiryTimestamp);
        if (curFirstStatementCsn != INVALID_CSN && curFirstStatementCsn < localMinCsn) {
            TimestampTz currentTime = GetCurrentTimestampInSecond();
            if (static_cast<uint64>(currentTime) < curFirstStatementCsnExpiryTime) {
                localMinCsn = curFirstStatementCsn;
            } else {
                /* Reset firstStatementCsn and its expiry time if firstStatementCsn has expired */
                (void)GsAtomicCompareExchangeU64(&m_xactArray[m_sortCoreIdxArr[i]]->m_xact.firstStatementCsnMin,
                                                 &curFirstStatementCsn, INVALID_CSN);
                (void)GsAtomicCompareExchangeU64(
                    &m_xactArray[m_sortCoreIdxArr[i]]->m_xact.firstStatementCsnMinExpiryTimestamp,
                    &curFirstStatementCsnExpiryTime, 0);
            }
        }
    }
    LWLockRelease(&m_coreLock.lock);
    return localMinCsn;
}

CommitSeqNo ThreadCoreMgr::GetLocalMinCommitedCsn()
{
    DstoreLWLockAcquire(&m_coreLock.lock, LW_SHARED);
    CommitSeqNo localMinCsn = MAX_COMMITSEQNO;
    std::atomic<CommitSeqNo> *curCsn;
    CommitSeqNo expectCsn;
    for (uint32 i = 0; i < m_sortCoreIdxArrBound; i++) {
        curCsn = &m_xactArray[m_sortCoreIdxArr[i]]->m_xact.toBeCommitedCsn;
        expectCsn = COMMITSEQNO_FIRST_NORMAL;
        while (curCsn->compare_exchange_strong(expectCsn, COMMITSEQNO_FIRST_NORMAL)) {
            expectCsn = COMMITSEQNO_FIRST_NORMAL;
            /*
             * When the current csn is COMMITSEQNO_FIRST_NORMAL, the toBeCommitedCsn is updating itself. We need to spin
             * around here until it's been modified. The reason using a CAS instruction is to make sure we can
             * see the latest value in each turn. It looks like we're modifying the current csn value here, but it
             * is actually just being reset to the same value if the value has not been changed. This is in a way
             * acting like a spin lock. Take a reference to what we're doing in SetLocalCsn.
             */
        }

        if (expectCsn != INVALID_CSN && expectCsn < localMinCsn) {
            localMinCsn = expectCsn;
        }
    }
    LWLockRelease(&m_coreLock.lock);
    ErrLog(DSTORE_DEBUG1, MODULE_FRAMEWORK, ErrMsg("Collect local min committed csn %lu", localMinCsn));
    return localMinCsn;
}

void ThreadCoreMgr::FindFreeCoreByNumaId(int numaId, ThreadCore **current)
{
    StorageReleasePanic(numaId < 0, MODULE_FRAMEWORK, ErrMsg("numaId is invalid %d.", numaId));
    m_spinLock[numaId].Acquire();
    *current = m_freeCoreList[numaId];
    if (*current) {
        m_freeCoreList[numaId] = (*current)->next;
        (*current)->prev = nullptr;
        (*current)->next = nullptr;
        m_usedCoreNum.fetch_add(1);
    }
    m_spinLock[numaId].Release();
}

ThreadCore *ThreadCoreMgr::GetFreeCore()
{
    ThreadCore *current = nullptr;
#ifdef DSTORE_USE_NUMA
    if (m_numaNodeNum > 1) {
        int numaId = thrd->GetNumaId();
        if (unlikely(numaId < 0)) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("Numa id is invalid %d", numaId));
        } else {
            FindFreeCoreByNumaId(numaId, &current);
        }
    }
    if (current != nullptr) {
        return current;
    }
#endif
    for (uint i = 0; i < m_numaNodeNum; i++) {
        FindFreeCoreByNumaId(static_cast<int32>(i), &current);
        if (current != nullptr) {
            break;
        }
    }
    if (unlikely(current == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Can not get free core, max core num:%u, used core is %u.",
            m_coreArrayNum, m_usedCoreNum.load()));
    }
    return current;
}

void ThreadCoreMgr::AddFreeCore(ThreadCore *core)
{
    m_spinLock[core->numaId].Acquire();
    core->next = m_freeCoreList[core->numaId];
    m_freeCoreList[core->numaId] = core;
    m_usedCoreNum.fetch_sub(1);
    m_spinLock[core->numaId].Release();
}

RetStatus ThreadCore::Init(uint32 idx, int numaNodeId)
{
    selfIdx = idx;
    futex.DstoreFutexInit();
    lockWaiter.init();
    bufTagArray = (BufferTagArray *)DstoreMemoryContextAlloc(g_storageInstance->GetMemoryMgr()->GetRoot(),
                                                             sizeof(BufferTagArray));
    if (unlikely(!bufTagArray)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Alloc memory for bufTagArray failed."));
        return DSTORE_FAIL;
    }
    bufTagArray->Initialize();
    numaId = numaNodeId;
    pid = INVALID_THREAD_ID;
    immediateAbort = false;
    interruptHoldoffCount = 0;
    prev = nullptr;
    next = nullptr;
    threadContext = nullptr;

    walInsertStatus = (WalBatchInsertStatus *)DstoreMemoryContextAlloc(g_storageInstance->GetMemoryMgr()->GetRoot(),
                                                                       sizeof(WalBatchInsertStatus));
    if (unlikely(!walInsertStatus)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Alloc memory for walInsertStatus failed."));
        return DSTORE_FAIL;
    }
    walInsertStatus->Init();
    return DSTORE_SUCC;
}

void ThreadCore::SetThreadName(const char *name)
{
    const char *realName = (name == nullptr) ? "unknown" : name;
    error_t rc = strncpy_s(threadName, THREAD_NAME_MAX_LENGTH, realName, THREAD_NAME_MAX_LENGTH - 1);
    storage_securec_check(rc, "\0", "\0");
}

void ThreadCore::SetStartTime(TimestampTz time)
{
    startTime = time;
}

void ThreadCore::Wakeup()
{
    futex.DstoreFutexPost();
}

void ThreadXact::Init()
{
    csnMin = INVALID_CSN;
    firstStatementCsnMin = INVALID_CSN;
    firstStatementCsnMinExpiryTimestamp = 0;
    currentActiveXid = INVALID_XID.m_placeHolder;
    toBeCommitedCsn.store(INVALID_CSN, std::memory_order_relaxed);
}

}  // namespace DSTORE
