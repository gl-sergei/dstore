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
 * dstore_transaction.cpp
 *
 * IDENTIFICATION
 *        src/transaction/dstore_transaction.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
 
#include "transaction/dstore_transaction.h"
#include "common/log/dstore_log.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_thread.h"
#include "common/instrument/trace/dstore_transaction_trace.h"
#include "securec.h"
#include "common/dstore_datatype.h"
#include "control/dstore_control_pdbinfo.h"
#include "transaction/dstore_cursor.h"
#include "transaction/dstore_transaction_mgr.h"
#include "transaction/dstore_savepoint.h"
#include "transaction/dstore_transaction_perf_unit.h"
#include "buffer/dstore_buf_refcount.h"
#include "tablespace/dstore_heap_segment.h"
#include "tablespace/dstore_index_segment.h"
#include "tablespace/dstore_heap_normal_segment.h"
#include "tablespace/dstore_index_normal_segment.h"
#include "tablespace/dstore_heap_temp_segment.h"
#include "tablespace/dstore_index_temp_segment.h"

#include "syslog/trace.h"

namespace DSTORE {

static THR_LOCAL TBlockState g_currentTxnBlockState = TBlockState::TBLOCK_DEFAULT;
static THR_LOCAL TransState g_currentTxnState = TransState::TRANS_DEFAULT;

void Xid::Dump(StringInfo str) const
{
    str->append("xid (%lu, %lu)", (uint64_t)m_zoneId, m_logicSlotId);
}

const char *Transaction::BlockStateAsString(TBlockState blockState)
{
    storage_trace_entry(TRACE_ID_Transaction__BlockStateAsString);
    const char *stateStr = "UNRECOGNIZED";

    switch (blockState) {
        case TBlockState::TBLOCK_DEFAULT:
            stateStr = "DEFAULT";
            break;
        case TBlockState::TBLOCK_STARTED:
            stateStr = "STARTED";
            break;
        case TBlockState::TBLOCK_BEGIN:
            stateStr = "BEGIN";
            break;
        case TBlockState::TBLOCK_INPROGRESS:
            stateStr = "INPROGRESS";
            break;
        case TBlockState::TBLOCK_END:
            stateStr = "END";
            break;
        case TBlockState::TBLOCK_ABORT:
            stateStr = "ABORT";
            break;
        case TBlockState::TBLOCK_ABORT_END:
            stateStr = "ABORT END";
            break;
        case TBlockState::TBLOCK_ABORT_PENDING:
            stateStr = "ABORT PEND";
            break;
        default:
            break;
    }
    storage_trace_exit(TRACE_ID_Transaction__BlockStateAsString);
    return stateStr;
}

const char *Transaction::TransStateAsString(TransState state)
{
    storage_trace_entry(TRACE_ID_Transaction__TransStateAsString);
    const char *stateStr = "UNRECOGNIZED";

    switch (state) {
        case TransState::TRANS_DEFAULT:
            stateStr = "DEFAULT";
            break;
        case TransState::TRANS_START:
            stateStr = "START";
            break;
        case TransState::TRANS_INPROGRESS:
            stateStr = "INPROGR";
            break;
        case TransState::TRANS_COMMIT:
            stateStr = "COMMIT";
            break;
        case TransState::TRANS_ABORT:
            stateStr = "ABORT";
            break;
        default:
            break;
    }
    storage_trace_exit(TRACE_ID_Transaction__TransStateAsString);
    return stateStr;
}

void Transaction::InitInfoForNewTrx()
{
    m_currTransState = {
        INVALID_XID,
        TransState::TRANS_DEFAULT,
        TBlockState::TBLOCK_DEFAULT,
        true,
        false,
        false,
        TransAbortStage::AbortNotStart
    };
    m_lastUndoPtr = INVALID_UNDO_RECORD_PTR;
    m_isolationLevel = TrxIsolationType::XACT_READ_COMMITTED;
    m_currentCommandId = FIRST_CID;
    m_currentCommandIdUsed = false;
    m_snapshot.Init();
    m_startTime = GetSystemTimeInMicrosecond();
}

void Transaction::IncreaseCommandCounter()
{
    /*
     * Just use the command ID to mark inserted/updated/deleted tuples.
     * if m_currentCommandIdUsed is false, need not increase command counter.
     */
    if (!m_currentCommandIdUsed) {
        return;
    }
    m_currentCommandId++;
    StorageAssert(m_currentCommandId != INVALID_CID);
    m_currentCommandIdUsed = false;
    m_snapshot.SetCid(m_currentCommandId);
}

void Transaction::PreCommit()
{
    FireCallback(TrxEvent::TRX_EVENT_PRE_COMMIT);
}

RetStatus Transaction::RecordCommit()
{
    storage_trace_entry(TRACE_ID_Transaction__RecordCommit);
    TransactionMgr *transactionMgr = m_instance->GetPdb(m_pdbId)->GetTransactionMgr();
    RetStatus status = DSTORE_SUCC;

    Xid xid = GetCurrentXid();
    if (m_currTransState.readOnly) {
        goto Finish;
    }
    StorageReleasePanic(!xid.IsValid(), MODULE_TRANSACTION, ErrMsg("Invalid xid."));

    status = transactionMgr->CommitTransactionSlot(xid, m_isSpecialAyncCommitAutoTrx);

Finish:
    storage_trace_exit(TRACE_ID_Transaction__RecordCommit);
    return status;
}

void Transaction::PostCommit()
{
    FireCallback(TrxEvent::TRX_EVENT_POST_COMMIT);
}

void Transaction::PreAbort()
{
    FireCallback(TrxEvent::TRX_EVENT_PRE_ABORT);
}

RetStatus Transaction::RecordAbort()
{
    AutoMemCxtSwitch memCxt(m_abortMemoryCtx);
    g_storageInstance->GetStat()->m_reportWaitStatus(
        static_cast<uint32_t>(GsStatWaitState::STATE_TRANSACTION_RECORD_ABORT));
    storage_trace_entry(TRACE_ID_Transaction__RecordAbort);
    TransactionMgr *transactionMgr = nullptr;
    RetStatus status = DSTORE_SUCC;
    Xid xid = GetCurrentXid();
    StoragePdb *pdb = m_instance->GetPdb(m_pdbId);
    if (STORAGE_VAR_NULL(pdb) || STORAGE_VAR_NULL(pdb->GetTransactionMgr())) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("Pdb[%u] is null or transaction mgr is null when record abort.", m_pdbId));
        goto ErrorExit;
    }
    transactionMgr = pdb->GetTransactionMgr();
    if (m_currTransState.readOnly) {
        goto Finish;
    }
    StorageReleasePanic(!xid.IsValid(), MODULE_TRANSACTION, ErrMsg("Invalid xid when do record abort."));

    if (STORAGE_FUNC_FAIL(transactionMgr->RollbackTransactionSlot(xid))) {
        goto ErrorExit;
    }

Finish:
    storage_trace_exit(TRACE_ID_Transaction__RecordAbort);
    g_storageInstance->GetStat()->m_reportWaitStatus(static_cast<uint32_t>(GsStatWaitState::STATE_WAIT_UNDEFINED));
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

void Transaction::PostAbort()
{
    FireCallback(TrxEvent::TRX_EVENT_POST_ABORT);
}

void Transaction::PostTerminate()
{
    /*
     * If abort happened due to the client thread exiting abnormally.
     */
    FireCallback(TrxEvent::TRX_EVENT_POST_TERMINATE);
}

void Transaction::PreCleanUp()
{
    FireCallback(TrxEvent::TRX_EVENT_PREROLLBACK_CLEANUP);
}

RetStatus Transaction::StartInternal()
{
    TraceChainBegin(nullptr);
    m_trxCompleteTimer.Start();
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    if (pdb != nullptr) {
        pdb->AddActiveTransaction();
    }

    StorageAssert(pdb != nullptr);
    if (unlikely(pdb->DisableSqlTransaction())) {
        storage_set_error(TRANSACTION_ERROR_DISABLE_SQL_TRANSACTION);
        return DSTORE_FAIL;
    }

    /* Step 1. Initialize info for new transaction */
    InitInfoForNewTrx();
    m_currTransState.state = TransState::TRANS_START;

    /* Report the current transaction start timestamp. */
    g_storageInstance->GetStat()->ReportXactTimestamp(GetCurrentTimestamp());

    /*
     * Step 2. Initialize resource-management stuff and subsystems
     * and setup virtual Transaction counter
     */
    (void)m_thrd->GetAndIncrementAndSetTransactionVirtualCounter();
    FireCallback(TrxEvent::TRX_EVENT_START);

    /* Step 3. Reset csn in the proc array */
    SetThrdLocalCsn(INVALID_CSN);
    m_thrd->SetLocalXid(INVALID_XID.m_placeHolder);

    m_currTransState.state = TransState::TRANS_INPROGRESS;
#ifdef DSTORE_USE_ASSERT_CHECKING
    (void)m_bufferPrivateRefCount->CheckForBufferPinLeaks();
#endif
    StorageAssert(GetSavepointList()->IsEmpty());
    return DSTORE_SUCC;
}

RetStatus Transaction::CommitInternal(bool cleanUpResource)
{
    RetStatus status = DSTORE_SUCC;
    StorageStat *stat = g_storageInstance->GetStat();
    /* Step 1. Check status */
    if (!InTransaction()) {
        storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
        goto ErrorExit;
    }

    /* Step 2. Do pre-commit processing */
    PreCommit();
    m_currTransState.state = TransState::TRANS_COMMIT;

    /* Step 3. Local commit, write csn to transaction slot. */
    status = RecordCommit();

    /* Step 4. Let others know about no transaction in progress by me. */
    SetThrdLocalCsn(INVALID_CSN);
    m_thrd->SetLocalXid(INVALID_XID.m_placeHolder);
    stat->ReportCurrentXid(INVALID_XID.m_placeHolder);
    stat->ReportCurrentTopXid(INVALID_XID.m_placeHolder);

    /* Step 5. Post-commit cleanup (include closing other subsystems). */
    m_currTransState.xid = INVALID_XID;
    PostCommit();
    if (cleanUpResource) {
        CleanupResource();
    }

    /* Report commit info */
    stat->ReportXactInfo(true);
    stat->ReportXactTimestamp(0);
#ifdef DSTORE_USE_ASSERT_CHECKING
    (void)m_bufferPrivateRefCount->CheckForBufferPinLeaks();
#endif
    m_currTransState.state = TransState::TRANS_DEFAULT;

Finish:
    if (g_storageInstance->GetPdb(m_pdbId)) {
        g_storageInstance->GetPdb(m_pdbId)->SubActiveTransaction();
    }
    m_trxCompleteTimer.End();
    TraceChainEnd();
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

RetStatus Transaction::AbortInternal(bool cleanUpResource)
{
    switch (m_currTransState.abortStage) {
        case TransAbortStage::AbortNotStart:
            /* Step 1. Check status, make sure we have a valid memory context and resource owner. */
            if (unlikely(!InTransaction())) {
                storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
                return DSTORE_FAIL;
            }
            PreAbort();
            m_currTransState.abortStage = TransAbortStage::PreAbortDone;
            /* fallthrough */
        case TransAbortStage::PreAbortDone:
            m_currTransState.state = TransState::TRANS_ABORT;
            /* Step 2. Advertise the fact that we abort the transaction */
            if (GetCurrentXid().IsValid()) {
                if (unlikely(RecordAbort() == DSTORE_FAIL)) {
                    return DSTORE_FAIL;
                }
            }
            m_currTransState.abortStage = TransAbortStage::RecordAbortDone;
            /* fallthrough */
        case TransAbortStage::RecordAbortDone:
            /* Step 3. Let others know about no transaction in progress by me. */
            SetThrdLocalCsn(INVALID_CSN);
            m_thrd->SetLocalXid(INVALID_XID.m_placeHolder);
            /* Report current top xid and current xid with INVALID_XID. */
            g_storageInstance->GetStat()->ReportCurrentXid(INVALID_XID.m_placeHolder);
            g_storageInstance->GetStat()->ReportCurrentTopXid(INVALID_XID.m_placeHolder);
            /* Step 4. Post-abort. */
            PostAbort();
            m_currTransState.abortStage = TransAbortStage::PostAbortDone;
            /* fallthrough */
        case TransAbortStage::PostAbortDone:
            /* Step 5. Clean up resource if needed. */
            if (unlikely(cleanUpResource)) {
                /* Due to the current method of interfacing with the SQL engine,
                 * the cleanUpResource here must be false; otherwise, if resources are released here,
                 * there will be dangling pointers in the subsequent SQL process.
                 */
                CleanupResource();
                m_bufferPrivateRefCount->PReleaseBufferDuringAbort();
            }
            m_currTransState.abortStage = TransAbortStage::CleanUpResourceDone;
            /* fallthrough */
        case TransAbortStage::CleanUpResourceDone:
            /* Step 6. Sub transaction numbers after abort */
            StorageAssert(g_storageInstance->GetPdb(m_pdbId) != nullptr);
            if (g_storageInstance->GetPdb(m_pdbId)) {
                g_storageInstance->GetPdb(m_pdbId)->SubActiveTransaction();
            }
            m_currTransState.abortStage = TransAbortStage::DecreasePdbTransCountDone;
            /* fallthrough */
        case TransAbortStage::DecreasePdbTransCountDone:
            /* The reason for updating the state here is that even if the following operation fails,
             * it will not affect the functionality of the transaction,
             * so there is no need for re-entry.
             */
            m_currTransState.abortStage = TransAbortStage::DstoreAbortCompleted;
            /* Step 7. Report rollback info */
            g_storageInstance->GetStat()->ReportXactInfo(false);
            g_storageInstance->GetStat()->ReportXactTimestamp(0);
            TraceChainEnd();
            m_trxCompleteTimer.End();
            /* fallthrough */
        case TransAbortStage::DstoreAbortCompleted:
            return DSTORE_SUCC;
        default:
            ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
                   ErrMsg("Invalid abort state :%d.", static_cast<int>(m_currTransState.abortStage)));
            storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
            return DSTORE_FAIL;
    }
}

RetStatus Transaction::Cleanup()
{
    storage_trace_entry(TRACE_ID_Transaction__Cleanup);
    RetStatus status = DSTORE_SUCC;
    /* Step 1. Check transaction state */
    if (unlikely(m_currTransState.state != TransState::TRANS_ABORT)) {
        storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
        goto ErrorExit;
    }

    /* Step 2. Do abort cleanup processing */
    PreCleanUp();
    m_currTransState.xid = INVALID_XID;

    /* Step 3. Reset transaction state */
    m_currTransState.state = TransState::TRANS_DEFAULT;

Finish:
    storage_trace_exit(TRACE_ID_Transaction__Cleanup);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

Transaction::Transaction(StorageInstance *instance, ThreadContext *thrdContext, ZoneId zid,
                         BufPrivateRefCount *bufferPrivateRefCount, PdbId pdbId, bool isAutonomous,
                         bool isSpecialAyncCommitAutoTrx)
    : m_memoryCtx(nullptr),
      m_abortMemoryCtx(nullptr),
      m_bufferPrivateRefCount(bufferPrivateRefCount),
      m_prevTransaction(nullptr),
      m_callbckList(nullptr),
      m_dropSegPendingList(nullptr),
      m_pdbId(pdbId),
      m_instance(instance),
      m_thrd(thrdContext),
      m_csnMgr(nullptr),
      m_savepoints(pdbId),
      m_innerSavepoint(pdbId),
      m_zid(zid),
      m_isAutonomous(isAutonomous),
      m_gucLevel(0),
      m_isSpecialAyncCommitAutoTrx(isSpecialAyncCommitAutoTrx),
      m_extraResPtr(nullptr)
{
    m_trxCompleteTimer.Bind(&TransactionPerfUnit::GetInstance().m_trxCompleteLatency);
    InitInfoForNewTrx();
    GetCursorSnapshotList()->Init();
    GetSavepointList()->Init();
}

RetStatus Transaction::Initialize()
{
    StorageAssert(m_bufferPrivateRefCount != nullptr);
    m_csnMgr = m_instance->GetCsnMgr();
    char *memoryCtxName = GetTrxMemCtxName(m_zid);
    if (unlikely(memoryCtxName == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Failed to get trx mem ctx name."));
        return DSTORE_FAIL;
    }
    m_memoryCtx = DstoreAllocSetContextCreate(m_thrd->m_memoryMgr->GetRoot(),
                                              memoryCtxName,
                                              ALLOCSET_DEFAULT_MINSIZE,
                                              ALLOCSET_DEFAULT_INITSIZE,
                                              ALLOCSET_DEFAULT_MAXSIZE,
                                              MemoryContextType::THREAD_CONTEXT);
    if (unlikely(m_memoryCtx == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        DstorePfreeExt(memoryCtxName);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Failed to create trx mem ctx."));
        return DSTORE_FAIL;
    }
    constexpr Size abortMemorySize = 1024 * 16; /* 16K in dstore and 16K in sql engine */
    m_abortMemoryCtx =
        DstoreAllocSetContextCreate(m_thrd->m_memoryMgr->GetRoot(), "DstoreAbortMemoryCtx", abortMemorySize,
                                    abortMemorySize, abortMemorySize, MemoryContextType::THREAD_CONTEXT);
    if (unlikely(m_abortMemoryCtx == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        DstoreMemoryContextDelete(m_memoryCtx);
        m_memoryCtx = nullptr;
        DstorePfreeExt(memoryCtxName);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Failed to create abort trx mem ctx."));
        return DSTORE_FAIL;
    }
    RetStatus ret = m_lockResource.Initialize(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    if (STORAGE_FUNC_FAIL(ret)) {
        DstoreMemoryContextDelete(m_abortMemoryCtx);
        m_abortMemoryCtx = nullptr;
        DstoreMemoryContextDelete(m_memoryCtx);
        m_memoryCtx = nullptr;
        DstorePfreeExt(memoryCtxName);
        memoryCtxName = nullptr;
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Failed to init lock resource for transaction."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

bool Transaction::IsTermMatched()
{
    if (unlikely(g_storageInstance->GetPdb(m_pdbId) == nullptr)) {
        ErrLog(DSTORE_LOG, MODULE_TRANSACTION,
               ErrMsg("Destroy Transaction check pdbterm error, pdb term: null transaction term: %lu.",
                      m_thrd->GetPdbTerm()));
        return false;
    }
    ErrLog(DSTORE_DEBUG1, MODULE_TRANSACTION, ErrMsg("Destroy Transaction check pdbterm:%lu, transaction term: %lu.",
                g_storageInstance->GetPdb(m_pdbId)->GetPdbTerm(), m_thrd->GetPdbTerm()));
    return g_storageInstance->GetPdb(m_pdbId)->GetPdbTerm() == m_thrd->GetPdbTerm();
}

void Transaction::Destroy()
{
    if (m_zid != INVALID_ZONE_ID && g_storageInstance->GetPdb(m_pdbId) &&
        g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr() && IsTermMatched()) {
        (void) g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr()->ReleaseZoneId(m_zid);
    }
    m_zid = INVALID_ZONE_ID;
    m_callbckList = nullptr;
    m_instance = nullptr;
    m_thrd = nullptr;
    m_csnMgr = nullptr;
    if (m_bufferPrivateRefCount != nullptr) {
        AutoMemCxtSwitch autoSwitch(m_memoryCtx);
        m_bufferPrivateRefCount->Destroy();
        delete m_bufferPrivateRefCount;
        m_bufferPrivateRefCount = nullptr;
    }
    if (m_lockResource.CheckForLockLeaks(m_isAutonomous ? "autonomous transaction destroy" : "transaction destroy")) {
        ErrLog(DSTORE_WARNING, MODULE_TRANSACTION, ErrMsg("lock leaks in transaction destroy, force release here."));
#ifndef UT
        m_lockResource.ReleaseAllLocks();
#endif
    }
    m_lockResource.Destroy();
    if (m_abortMemoryCtx != nullptr) {
        DstoreMemoryContextDelete(m_abortMemoryCtx);
        m_abortMemoryCtx = nullptr;
    }
    if (m_memoryCtx != nullptr) {
        DstoreMemoryContextDelete(m_memoryCtx);
        m_memoryCtx = nullptr;
    }
    m_prevTransaction = nullptr;
}

PdbId Transaction::GetPdbId()
{
    return m_pdbId;
}

void Transaction::ResetMemoryContext()
{
    DstoreMemoryContextResetAndDeleteChildren(m_memoryCtx);
    DstoreMemoryContextResetAndDeleteChildren(m_abortMemoryCtx);
}

void Transaction::CleanupResource()
{
    m_innerSavepoint.Release();
    GetCursorSnapshotList()->DeleteAll();
    GetSavepointList()->DeleteAll();
    m_lockResource.ReleaseAllLocks();
    ResetMemoryContext();
}

void Transaction::ReleaseLocksInTransRes()
{
    m_lockResource.ReleaseAllLocks();
}

/*
 * Allocate transaction slot for current transaction.
 */
RetStatus Transaction::AllocTransactionSlot()
{
    RetStatus status = DSTORE_SUCC;
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Pdb[%u] is null when alloc xid.", m_pdbId));
        return DSTORE_FAIL;
    }
    if (unlikely(pdb->GetPdbRoleMode() != PdbRoleMode::PDB_PRIMARY)) {
        storage_set_error(TRANSACTION_ERROR_PDB_FENCE_WRITE);
        return DSTORE_FAIL;
    }
    if (unlikely(!InTransaction())) {
        storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
        return DSTORE_FAIL;
    }
    StorageAssert(pdb != nullptr);
    TransactionMgr *transactionMgr = pdb->GetTransactionMgr();

    if (!m_currTransState.xid.IsValid()) {
        m_lastUndoPtr = INVALID_UNDO_RECORD_PTR;
        status = transactionMgr->AllocTransactionSlot(m_currTransState.xid);
    }
    m_thrd->SetLocalXid(m_currTransState.xid.m_placeHolder);
    g_storageInstance->GetStat()->ReportCurrentXid(m_currTransState.xid.m_placeHolder);
    g_storageInstance->GetStat()->ReportCurrentTopXid(m_thrd->GetTopTransaction()->GetCurrentXid().m_placeHolder);

    m_currTransState.readOnly = false;
    SetCurCidUsed();
    return status;
}

UndoRecPtr Transaction::InsertUndoRecord(UndoRecord *undoRecord)
{
    FAULT_INJECTION_RETURN(DstoreTransactionFI::INSERT_UNDO_RECORD_FAIL, INVALID_UNDO_RECORD_PTR);
    StoragePdb *pdb = m_instance->GetPdb(m_pdbId);
    if (STORAGE_VAR_NULL(pdb) || STORAGE_VAR_NULL(pdb->GetTransactionMgr())) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("Pdb[%u] is null or transaction mgr is null when insert undo.", m_pdbId));
        return INVALID_UNDO_RECORD_PTR;
    }
    TransactionMgr *transactionMgr = pdb->GetTransactionMgr();
    if (!InTransaction()) {
        storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
        return INVALID_UNDO_RECORD_PTR;
    }
    StorageReleasePanic(!m_currTransState.xid.IsValid(), MODULE_TRANSACTION, ErrMsg("Invalid xid."));

    Xid xid = GetCurrentXid();
    undoRecord->SetTxnPreUndoPtr(m_lastUndoPtr);
    m_lastUndoPtr = transactionMgr->InsertUndoRecord(xid, undoRecord);
    return m_lastUndoPtr;
}

UndoRecPtr Transaction::GetLastUndoRecord()
{
    return m_lastUndoPtr;
}

RetStatus Transaction::Start()
{
    storage_trace_entry(TRACE_ID_Transaction__Start);
    RetStatus status = DSTORE_SUCC;
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Pdb[%u] is null when start.", m_pdbId));
        goto ErrorExit;
    }
    if (unlikely(pdb->DisableSqlTransaction())) {
        storage_set_error(TRANSACTION_ERROR_DISABLE_SQL_TRANSACTION);
        goto ErrorExit;
    }
    if (m_isolationLevel == TrxIsolationType::XACT_READ_COMMITTED) {
        m_snapshot.SetCsn(INVALID_CSN);
    }
    /* reset to snapshot cid to transaction's m_currentCommandId */
    m_snapshot.SetCid(m_currentCommandId);

    switch (m_currTransState.blockState) {
        /* if we aren't in a transaction block, we just do our usual StartInternal transaction. */
        case TBlockState::TBLOCK_DEFAULT:
            status = StartInternal();
            if (status == DSTORE_SUCC || IsTransactionRunning()) {
                /*
                 * If StartInternal() failed, we need to set the transaction block
                 * state to started just inner state is changed by StartInternal().
                 * Then we can known to do some clean up work in Abort().
                 * If inner state is default but block state is started, that is strange and unreasonable .
                 */
                m_currTransState.blockState = TBlockState::TBLOCK_STARTED;
            }
            break;

        /*
         * We are somewhere in a transaction block or subtransaction and
         * about to start a new command.  For now we do nothing, but
         * someday we may do command-local resource initialization. (Note
         * that any needed CommandCounterIncrement was done by the
         * previous Commit().)
         */
        case TBlockState::TBLOCK_INPROGRESS:
            /*
             * Create a implicit savepoint for every command,
             * in case something went wrong and need to rollback.
             */
            status = m_innerSavepoint.Create(nullptr);
            if (unlikely(STORAGE_FUNC_FAIL(status))) {
                ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Create inner savepoint failed."));
            }
            break;

        /*
         * Here we are in a failed transaction block (one of the commands
         * caused an abort) so we do nothing but remain in the abort
         * m_state.  Eventually we will get a ROLLBACK command which will
         * get us out of this m_state.  (It is up to other code to ensure
         * that no commands other than ROLLBACK will be processed in these
         * states.)
         */
        case TBlockState::TBLOCK_ABORT:
            break;

        /* These cases are invalid. */
        case TBlockState::TBLOCK_STARTED:
        case TBlockState::TBLOCK_BEGIN:
        case TBlockState::TBLOCK_END:
        case TBlockState::TBLOCK_ABORT_END:
        case TBlockState::TBLOCK_ABORT_PENDING:
        default:
            storage_set_error(TRANSACTION_ERROR_INVALID_TBLOCK_STATE, BlockStateAsString(m_currTransState.blockState));
            goto ErrorExit;
    }

Finish:
    storage_trace_exit(TRACE_ID_Transaction__Start);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

static inline void LogAbortError(TransactionStateData *currTransState)
{
    ErrLog(DSTORE_WARNING, MODULE_TRANSACTION,
           ErrMsg("abort internal for xid(%lu) failed, state: %d, errmsg: %s.", currTransState->xid.m_placeHolder,
                  static_cast<int>(currTransState->blockState), StorageGetMessage()));
}

/*
 * If cleanUpResource is false,
 * caller must use CleanUpResourceAfterCommit() to clean up resource of transaction
 */
RetStatus Transaction::Commit(bool cleanUpResource)
{
    storage_trace_entry(TRACE_ID_Transaction__Commit);
    RetStatus status = DSTORE_SUCC;
    if (m_isolationLevel == TrxIsolationType::XACT_READ_COMMITTED) {
        m_snapshot.SetCsn(INVALID_CSN);
    }
    /* reset to snapshot cid to transaction's m_currentCommandId */
    m_snapshot.SetCid(m_currentCommandId);

    switch (m_currTransState.blockState) {
        /*
         * This shouldn't happen, because it means the previous Start()
         * didn't set the STARTED state appropriately.
         */
        case TBlockState::TBLOCK_DEFAULT:
            storage_set_error(TRANSACTION_WARNING_NO_ACTIVE_TRANSACTION);
            ErrLog(DSTORE_LOG, MODULE_TRANSACTION, ErrMsg("commit an unstarted transaction."));
            status = DSTORE_FAIL;
            break;

        /*
         * If we aren't in a transaction block, just do our usual
         * transaction commit, and return to the idle state.
         */
        case TBlockState::TBLOCK_STARTED:
            status = CommitInternal(cleanUpResource);
            m_currTransState.blockState = TBlockState::TBLOCK_DEFAULT;
            break;

        /*
         * We are completing a "BEGIN TRANSACTION" command, so we change
         * to the "transaction block in progress" state and return.  (We
         * assume the BEGIN did nothing to the database, so we need no
         * CommandCounterIncrement.)
         */
        case TBlockState::TBLOCK_BEGIN:
            m_currTransState.blockState = TBlockState::TBLOCK_INPROGRESS;
            break;

        /*
         * This is the case when we have finished executing a command
         * someplace within a transaction block.  We increment the command
         * counter and return.
         */
        case TBlockState::TBLOCK_INPROGRESS:
            m_innerSavepoint.Release();
            IncreaseCommandCounter();
            break;

        /*
         * We are completing a "COMMIT" command.  Do it and return to the
         * idle state.
         */
        case TBlockState::TBLOCK_END:
            status = CommitInternal(cleanUpResource);
            m_currTransState.blockState = TBlockState::TBLOCK_DEFAULT;
            break;

        /*
         * Here we are in the middle of a transaction block but one of the
         * commands caused an abort so we do nothing but remain in the
         * abort state.  Eventually we will get a ROLLBACK command.
         */
        case TBlockState::TBLOCK_ABORT:
            break;

        /*
         * Here we were in an aborted transaction block and we just got
         * the ROLLBACK command from the user, so clean up the
         * already-aborted transaction and return to the idle state.
         */
        case TBlockState::TBLOCK_ABORT_END:
            status = AbortInternal(cleanUpResource);
            if (STORAGE_FUNC_FAIL(status)) {
                LogAbortError(&m_currTransState);
                return status;
            }
            status = Cleanup();
            m_currTransState.blockState = TBlockState::TBLOCK_DEFAULT;
            break;

        /*
         * Here we were in a perfectly good transaction block but the user
         * told us to ROLLBACK anyway. We have to AbortInternal the transaction
         * and then clean up.
         */
        case TBlockState::TBLOCK_ABORT_PENDING:
            status = AbortInternal(cleanUpResource);
            if (STORAGE_FUNC_FAIL(status)) {
                LogAbortError(&m_currTransState);
                return status;
            }
            status = STORAGE_FUNC_FAIL(Cleanup()) ? DSTORE_FAIL : status;
            m_currTransState.blockState = TBlockState::TBLOCK_DEFAULT;
            break;

        default:
            storage_set_error(TRANSACTION_ERROR_INVALID_TBLOCK_STATE, BlockStateAsString(m_currTransState.blockState));
            goto ErrorExit;
    }

Finish:
    storage_trace_exit(TRACE_ID_Transaction__Commit);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

void Transaction::CleanUpResourceAfterCommit()
{
    CleanupResource();
    CheckLwLockLeak();
}

void Transaction::CleanUpResourceAfterAbort()
{
    CleanupResource();
    m_bufferPrivateRefCount->PReleaseBufferDuringAbort();
    CheckLwLockLeak();
}

/*
 * Rollback current transaction.
 * If terminateCurrTrx is true, means client thread exits abnormally,
 * we need rollback and clean up current transaction.
 */
RetStatus Transaction::Abort(bool terminateCurrTrx, bool cleanUpResource)
{
    storage_trace_entry(TRACE_ID_Transaction__Abort);
    RetStatus status = DSTORE_SUCC;

    /*
     * Here, we just detect whether there are any pending undo actions so that
     * we can skip releasing the locks during abort transaction.  We don't
     * release the locks till we execute undo actions otherwise, there is a
     * risk of deadlock.
     */
    switch (m_currTransState.blockState) {
        case TBlockState::TBLOCK_DEFAULT:
            if (unlikely(terminateCurrTrx)) {
                /*
                 * If a network failure occurred after committing/rolling back the transaction
                 * and before the driver completes reading the query results (e.g., RFQ),
                 * add firststatementcsnmin to the tracker.
                 */
                PostTerminate();
            }
            if (m_currTransState.state == TransState::TRANS_DEFAULT) {
                /* we are idle, so nothing to do */
                storage_set_error(TRANSACTION_WARNING_NO_ACTIVE_TRANSACTION);
                ErrLog(DSTORE_LOG, MODULE_TRANSACTION, ErrMsg("abort an unstarted transaction."));
                status = DSTORE_FAIL;
            } else {
                /*
                 * We can get here after an error during transaction start
                 * (state will be TRANS_START).  Need to clean up the
                 * incompletely started transaction.  First, adjust the
                 * low-level state to suppress warning message from Abort().
                 */
                if (m_currTransState.state == TransState::TRANS_START) {
                    m_currTransState.state = TransState::TRANS_INPROGRESS;
                }
                status = AbortInternal(cleanUpResource);
                if (STORAGE_FUNC_FAIL(status)) {
                    LogAbortError(&m_currTransState);
                    return status;
                }
                status = STORAGE_FUNC_FAIL(Cleanup()) ? DSTORE_FAIL : status;
            }
            break;

        /*
         * if we aren't in a transaction block, we just do the basic abort
         * & cleanup transaction.
         */
        case TBlockState::TBLOCK_STARTED:
            if (unlikely(terminateCurrTrx)) {
                PostTerminate();
            }
            status = AbortInternal(cleanUpResource);
            if (STORAGE_FUNC_FAIL(status)) {
                LogAbortError(&m_currTransState);
                return status;
            }
            status = STORAGE_FUNC_FAIL(Cleanup()) ? DSTORE_FAIL : status;
            m_currTransState.blockState = TBlockState::TBLOCK_DEFAULT;
            break;

        /*
         * If we are in TBLOCK_BEGIN it means something screwed up right
         * after reading "BEGIN TRANSACTION".  We assume that the user
         * will interpret the error as meaning the BEGIN failed to get him
         * into a transaction block, so we should abort and return to idle
         * state.
         */
        case TBlockState::TBLOCK_BEGIN:
            status = AbortInternal(cleanUpResource);
            if (STORAGE_FUNC_FAIL(status)) {
                LogAbortError(&m_currTransState);
                return status;
            }
            status = STORAGE_FUNC_FAIL(Cleanup()) ? DSTORE_FAIL : status;
            m_currTransState.blockState = TBlockState::TBLOCK_DEFAULT;
            break;

        /*
         * We are somewhere in a transaction block and we've gotten a
         * failure, so set up the persistent ABORT state.
         * We will stay in ABORT until we get a ROLLBACK or Commit.
         * We will do real abort when we are in TBLOCK_ABORT_END state. Why?
         * Because we can get a ROLLBACK TO [SAVEPOINT] if we have savepoints in TBLOCK_ABORT STATE.
         */
        case TBlockState::TBLOCK_INPROGRESS:
            if (unlikely(terminateCurrTrx)) {
                PostTerminate();
                status = AbortInternal(cleanUpResource);
                if (STORAGE_FUNC_FAIL(status)) {
                    LogAbortError(&m_currTransState);
                    return status;
                }
                status = STORAGE_FUNC_FAIL(Cleanup()) ? DSTORE_FAIL : status;
                m_currTransState.blockState = TBlockState::TBLOCK_DEFAULT;
            } else {
                m_currTransState.blockState = TBlockState::TBLOCK_ABORT;
                /* Cleanup() happens when we exit TBLOCK_ABORT_END */
            }
            break;

        /*
         * Here, we failed while trying to COMMIT. Clean up the
         * transaction and return to idle state (we do not want to stay in
         * the transaction).
         */
        case TBlockState::TBLOCK_END:
            if (unlikely(terminateCurrTrx)) {
                PostTerminate();
            }
            status = AbortInternal(cleanUpResource);
            if (STORAGE_FUNC_FAIL(status)) {
                LogAbortError(&m_currTransState);
                return status;
            }
            status = STORAGE_FUNC_FAIL(Cleanup()) ? DSTORE_FAIL : status;
            m_currTransState.blockState = TBlockState::TBLOCK_DEFAULT;
            break;

        /*
         * Here, we are already in an aborted transaction state and are
         * waiting for a ROLLBACK, but for some reason we failed again! So
         * we just remain in the abort state.
         */
        case TBlockState::TBLOCK_ABORT:
            if (unlikely(terminateCurrTrx)) {
                status = AbortInternal(cleanUpResource);
                if (STORAGE_FUNC_FAIL(status)) {
                    LogAbortError(&m_currTransState);
                    return status;
                }
                status = Cleanup();
                m_currTransState.blockState = TBlockState::TBLOCK_DEFAULT;
            }
            break;

        /*
         * We are in a live transaction and we got a ROLLBACK command.
         * abort, cleanup, go to idle state.
         */
        case TBlockState::TBLOCK_ABORT_PENDING:
            if (unlikely(terminateCurrTrx)) {
                PostTerminate();
            }
            status = AbortInternal(cleanUpResource);
            if (STORAGE_FUNC_FAIL(status)) {
                LogAbortError(&m_currTransState);
                return status;
            }
            status = STORAGE_FUNC_FAIL(Cleanup()) ? DSTORE_FAIL : status;
            m_currTransState.blockState = TBlockState::TBLOCK_DEFAULT;
            break;

        /*
         * We are in a failed transaction and we got the ROLLBACK command.
         * We have already aborted, we just need to Cleanup and go to idle
         * state.
         */
        case TBlockState::TBLOCK_ABORT_END:
            status = AbortInternal(cleanUpResource);
            if (STORAGE_FUNC_FAIL(status)) {
                LogAbortError(&m_currTransState);
                return status;
            }
            status = Cleanup();
            m_currTransState.blockState = TBlockState::TBLOCK_DEFAULT;
            break;

        default:
            storage_set_error(TRANSACTION_ERROR_INVALID_TBLOCK_STATE, BlockStateAsString(m_currTransState.blockState));
            status = DSTORE_FAIL;
    }

    storage_trace_exit(TRACE_ID_Transaction__Abort);
    return status;
}

/**
 CommitRollbackAndRestoreTrxState must be only called from a transaction block.
 We start a new transaction after executing COMMIT/ROLLBACK,
 and we restore transaction state and block state,
 so there is no separate "BEGIN TRANSACTION" command.
*/
RetStatus Transaction::CommitRollbackAndRestoreTrxState(bool isRollback)
{
    if (unlikely(!InTransaction())) {
        storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
        return DSTORE_FAIL;
    }
    /*
     * COMMIT/ROLLBACK statements must be called in TBLOCK_STARTED or TBLOCK_INPROGRESS.
     * TBLOCK_STARTED is when called before "BEGIN TRANSACTION" command.
     * TBLOCK_INPROGRESS is when called from a block.
     */
    StorageAssert(m_currTransState.blockState == TBlockState::TBLOCK_STARTED ||
                  m_currTransState.blockState == TBlockState::TBLOCK_INPROGRESS);

    /*
     * COMMIT/ROLLBACK execution from STP is not supported in TAC.
     * This gives it a chance to return this information to the client.
     */
    FireCallback(TrxEvent::TRX_EVENT_PRE_STP_TRX_STATEMENT);

    /* Save transaction state and block state. */
    TransState state = m_currTransState.state;
    TBlockState blockState = m_currTransState.blockState;
    bool inTransactionBlock = (blockState == TBlockState::TBLOCK_INPROGRESS);

    if (isRollback) {
        /* Rollback the current transaction. */
        if (inTransactionBlock) {
            if (STORAGE_FUNC_FAIL(UserAbortTransactionBlock())) {
                return DSTORE_FAIL;
            }
        }
        if (STORAGE_FUNC_FAIL(Abort())) {
            return DSTORE_FAIL;
        }
    } else {
        /* Commit the current transaction. */
        if (inTransactionBlock) {
            if (STORAGE_FUNC_FAIL(EndTransactionBlock())) {
                return DSTORE_FAIL;
            }
        }
        if (STORAGE_FUNC_FAIL(Commit())) {
            return DSTORE_FAIL;
        }
    }

    /* Start a new transaction. */
    if (STORAGE_FUNC_FAIL(Start())) {
        return DSTORE_FAIL;
    }

    StorageAssert(m_currTransState.blockState == TBlockState::TBLOCK_STARTED ||
                  m_currTransState.blockState == TBlockState::TBLOCK_INPROGRESS);

    m_currTransState.state = state;
    m_currTransState.blockState = blockState;

    return DSTORE_SUCC;
}

/* ----------------------------------------------------------------
 *                  transaction block support
 * ----------------------------------------------------------------
 */
bool Transaction::IsTxnBlock() const
{
    TBlockState tbs = m_currTransState.blockState;
    return tbs != TBlockState::TBLOCK_DEFAULT && tbs != TBlockState::TBLOCK_STARTED;
}

/*
 * This executes a BEGIN command.
 */
RetStatus Transaction::BeginTransactionBlock()
{
    storage_trace_entry(TRACE_ID_Transaction__BeginTransactionBlock);
    RetStatus status = DSTORE_SUCC;

    switch (m_currTransState.blockState) {
        /*
         * We are not inside a transaction block, so allow one to begin.
         */
        case TBlockState::TBLOCK_STARTED:
            m_currTransState.blockState = TBlockState::TBLOCK_BEGIN;
            break;

        /* Already a transaction block in progress. */
        case TBlockState::TBLOCK_INPROGRESS:
        case TBlockState::TBLOCK_ABORT:
            storage_set_error(TRANSACTION_WARNING_ACTIVE_SQL_TRANSACTION);
            goto ErrorExit;

        /* These cases are invalid. */
        case TBlockState::TBLOCK_DEFAULT:
        case TBlockState::TBLOCK_BEGIN:
        case TBlockState::TBLOCK_ABORT_END:
        case TBlockState::TBLOCK_ABORT_PENDING:
        case TBlockState::TBLOCK_END:
        default:
            storage_set_error(TRANSACTION_ERROR_INVALID_TBLOCK_STATE, BlockStateAsString(m_currTransState.blockState));
            goto ErrorExit;
    }

Finish:
    storage_trace_exit(TRACE_ID_Transaction__BeginTransactionBlock);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

/*
 * This executes a COMMIT command.
 */
RetStatus Transaction::EndTransactionBlock()
{
    storage_trace_entry(TRACE_ID_Transaction__EndTransactionBlock);
    RetStatus status = DSTORE_SUCC;

    switch (m_currTransState.blockState) {
        /* We are in a transaction block, so tell Commit() to COMMIT. */
        case TBlockState::TBLOCK_INPROGRESS:
            m_currTransState.blockState = TBlockState::TBLOCK_END;
            break;

        /*
         * We are in a failed transaction block.  Tell
         * Commit() it's time to exit the block.
         */
        case TBlockState::TBLOCK_ABORT:
            m_currTransState.blockState = TBlockState::TBLOCK_ABORT_END;
            break;

        /*
         * The user issued COMMIT when not inside a transaction.  Issue a WARNING,
         * staying in TBlockState::TBLOCK_STARTED state.  The upcoming call to Commit() will then
         * close the transaction and put us back into the default state.
         */
        case TBlockState::TBLOCK_STARTED:
            storage_set_error(TRANSACTION_WARNING_NO_ACTIVE_TRANSACTION);
            ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("should not get a end when block started."));
            goto ErrorExit;

        /* These cases are invalid. */
        case TBlockState::TBLOCK_DEFAULT:
        case TBlockState::TBLOCK_BEGIN:
        case TBlockState::TBLOCK_END:
        case TBlockState::TBLOCK_ABORT_END:
        case TBlockState::TBLOCK_ABORT_PENDING:
        default:
            storage_set_error(TRANSACTION_ERROR_INVALID_TBLOCK_STATE, BlockStateAsString(m_currTransState.blockState));
            goto ErrorExit;
    }

Finish:
    storage_trace_exit(TRACE_ID_Transaction__EndTransactionBlock);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

/*
 * This rollback to last sqlcmd in Trx
 */
RetStatus Transaction::RollbackLastSQLCmd()
{
    RetStatus status = DSTORE_SUCC;

    switch (m_currTransState.blockState) {
        /*
         * If we execute one sql command failed in transaction, just rollback to last innerSavepoint.
         */
        case TBlockState::TBLOCK_INPROGRESS:
            status = m_innerSavepoint.Rollback();
            m_innerSavepoint.Release();
            break;

        /* These cases are invalid. */
        case TBlockState::TBLOCK_ABORT:
        case TBlockState::TBLOCK_STARTED:
        case TBlockState::TBLOCK_DEFAULT:
        case TBlockState::TBLOCK_BEGIN:
        case TBlockState::TBLOCK_END:
        case TBlockState::TBLOCK_ABORT_PENDING:
        case TBlockState::TBLOCK_ABORT_END:
        default:
            status = DSTORE_FAIL;
            break;
    }

    return status;
}

/*
 * This executes a ROLLBACK command.
 */
RetStatus Transaction::UserAbortTransactionBlock()
{
    RetStatus status = DSTORE_SUCC;
    storage_trace_entry(TRACE_ID_Transaction__UserAbortTransactionBlock);
    switch (m_currTransState.blockState) {
        /*
         * We are inside a transaction block and we got a ROLLBACK command
         * from the user, so tell Commit() to abort and exit the transaction block.
         */
        case TBlockState::TBLOCK_INPROGRESS:
            m_currTransState.blockState = TBlockState::TBLOCK_ABORT_PENDING;
            break;

        /*
         * We are inside a failed transaction block and we got a ROLLBACK
         * command from the user.  abort processing is already done, so
         * Commit() just has to cleanup and go back to idle state.
         */
        case TBlockState::TBLOCK_ABORT:
            m_currTransState.blockState = TBlockState::TBLOCK_ABORT_END;
            break;

        /*
         * The user issued ABORT when not inside a transaction. Issue a
         * WARNING and go to abort state.  The upcoming call to
         * Commit() will then put us back into the default state.
         */
        case TBlockState::TBLOCK_STARTED:
            storage_set_error(TRANSACTION_WARNING_NO_ACTIVE_TRANSACTION);
            ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("should not get a rollback when block started."));
            m_currTransState.blockState = TBlockState::TBLOCK_ABORT_PENDING;
            status = DSTORE_FAIL;
            break;

        /* These cases are invalid. */
        case TBlockState::TBLOCK_DEFAULT:
        case TBlockState::TBLOCK_BEGIN:
        case TBlockState::TBLOCK_END:
        case TBlockState::TBLOCK_ABORT_PENDING:
        case TBlockState::TBLOCK_ABORT_END:
        default:
            storage_set_error(TRANSACTION_ERROR_INVALID_TBLOCK_STATE, BlockStateAsString(m_currTransState.blockState));
            goto ErrorExit;
    }

Finish:
    storage_trace_exit(TRACE_ID_Transaction__UserAbortTransactionBlock);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

/*
 * XidVisibleToSnapshot
 * Is the given XID visible according to the snapshot?.
 */
bool XidVisibleToSnapshot(Snapshot snapshot, Xid xid, Transaction *txn, XidStatus *xidStatus)
{
    storage_trace_entry(TRACE_ID_Transaction__XidVisibleToSnapshot);

    XidStatus xs(xid, txn);
    if (xidStatus == nullptr) {
        xidStatus = &xs;
    }

    if (xidStatus->IsFrozen()) {
        return true;
    }

    if (xidStatus->IsAborted()) {
        return false;
    }
    /* In snapshot dirty, you can see all tuples except the aborted transaction. */
    if (snapshot->GetSnapshotType() == SnapshotType::SNAPSHOT_DIRTY) {
        return true;
    }

    if (!xidStatus->NeedWaitPendingTxn()) {
        return false;
    }

    if (xidStatus->IsCommitted()) {
        StorageAssert(xidStatus->GetCsn() != INVALID_CSN);
        return xidStatus->GetCsn() < snapshot->GetCsn();
    }

    if (xidStatus->IsPendingCommit()) {
        if (g_storageInstance->GetPdb(txn->GetPdbId()) == nullptr) {
            return false;
        }

        TransactionMgr *trxMgr = g_storageInstance->GetPdb(txn->GetPdbId())->GetTransactionMgr();
        bool txnFailed = false;
        if (likely(thrd != nullptr)) {
            thrd->HoldInterrupts();
        }
        RetStatus status = trxMgr->WaitForTransactionEnd(xid, txnFailed);
        if (likely(thrd != nullptr)) {
            thrd->ResumeInterrupts();
        }
        StorageReleasePanic(STORAGE_FUNC_FAIL(status), MODULE_TRANSACTION,
                            ErrMsg("Waiting pendding commit transaction end is failed!"));
        if (txnFailed) {
            return false;
        }
        XidStatus xsTmp(xid, txn);
        if (xsTmp.IsFrozen()) {
            return true;
        }
        StorageAssert(xsTmp.IsCommitted());
        StorageAssert(xsTmp.GetCsn() != INVALID_CSN);
        return xsTmp.GetCsn() < snapshot->GetCsn();
    }

    StorageAssert(xidStatus->GetStatus() != TXN_STATUS_UNKNOWN);
    StorageAssert(xidStatus->GetStatus() != TXN_STATUS_FAILED);

    storage_trace_exit(TRACE_ID_Transaction__XidVisibleToSnapshot);
    return false;
}

/*
 * Read consistent read page according to snapshot.
 *
 * If an appropriate cr page is found in cr buffer, unlock and release base page
 * and return cr buffer desc, we should call release for cr outside; else return
 * nullptr, need to construct cr page outside.
 */
BufferDesc *Transaction::GetCrPage(BufferDesc *baseBufDesc, Snapshot snapshot)
{
    storage_trace_entry(TRACE_ID_Transaction__GetCrPage);
    BufMgrInterface *bufferMgr = m_instance->GetBufferMgr();

    /* Read matched cr page, if not, return nullptr */
    BufferDesc *crBufferDesc = INVALID_BUFFER_DESC;
    if (((baseBufDesc->GetState(false) & Buffer::BUF_OWNED_BY_ME) == 0) &&
        ((baseBufDesc->GetState(false) & Buffer::BUF_READ_AUTHORITY) == 0)) {
        ErrLog(DSTORE_DEBUG1, MODULE_TRANSACTION, ErrMsg("base buffer is not owner and not have read_authority."));
        goto EXIT;
    }
    crBufferDesc = nullptr;
    if (baseBufDesc->GetPage()->GetType() == PageType::INDEX_PAGE_TYPE) {
        crBufferDesc = bufferMgr->ReadCr(baseBufDesc, snapshot);
    }
    if (crBufferDesc != INVALID_BUFFER_DESC) {
        bufferMgr->UnlockAndRelease(baseBufDesc);
#ifdef DSTORE_USE_ASSERT_CHECKING
        ErrLog(DSTORE_DEBUG1, MODULE_TRANSACTION,
               ErrMsg("Can read matched cr page of base page(%d, %u) "
                      "according to snapshot.csn = %lu, page type is %hu",
                      baseBufDesc->GetPageId().m_fileId, baseBufDesc->GetPageId().m_blockId, snapshot->GetCsn(),
                      static_cast<uint16>(crBufferDesc->GetPage()->GetType())));
#endif
    }
EXIT:
    storage_trace_exit(TRACE_ID_Transaction__GetCrPage);
    return crBufferDesc;
}

/*
 * Create consistent read page according to snapshot.
 *
 * First construct cr page in local buffer, useLocalCr will be set in ConstructCr,
 * if useLocalCr is true, return directly; if not, try to alloc cr buffer. If alloc
 * successfully, copy local page to cr buffer page and set crCtx->m_crBufDesc.
 *
 * Before return, we unlock and release base buffer.
 * Note: we need to unpin cr buffer outside.
 * Note: cr page contains deleted and non-inplace-update tuples which shouldn't be visible.
 */
RetStatus Transaction::ConstructCrPage(CRContext *crCtx, BtreeUndoContext *btrUndoContext)
{
    storage_trace_entry(TRACE_ID_Transaction__ConstructCrPage);
    RetStatus status = DSTORE_SUCC;
    BufMgrInterface *bufMgr = crCtx->isGlobalTempTable ? thrd->GetTmpLocalBufMgr() : m_instance->GetBufferMgr();
    Page *basePage = crCtx->baseBufDesc->GetPage();
    PageType pageType = basePage->GetType();

    /* Step 1. Copy base page to local buffer and unlock base page. */
    errno_t rc = memcpy_s(static_cast<char*>(static_cast<void*>(crCtx->localPage)), BLCKSZ,
                          basePage, BLCKSZ);
    storage_securec_check(rc, "\0", "\0");

    uint64 lastestPageWriteTime = crCtx->isGlobalTempTable ? 0 : crCtx->baseBufDesc->GetLastModifyTime();
    bool isPageOwner = crCtx->baseBufDesc->IsPageOwner();
    bufMgr->UnlockContent(crCtx->baseBufDesc, BufferPoolUnlockContentFlag());

    FAULT_INJECTION_NOTIFY(DstoreHeapFI::CONSTRUCT_CR_AFTER_READ_BASE_PAGE);
    FAULT_INJECTION_WAIT(DstoreHeapFI::CONSTRUCT_CR_PAGE);

    /* Step 2. Construct cr page, crCtx->useLocalCr and crCtx->pageMaxCsn will be set in ConstructCR. */
    if (crCtx->localPage->GetType() == PageType::INDEX_PAGE_TYPE) {
        status = (static_cast<BtrPage *>(crCtx->localPage))->ConstructCR(thrd->GetActiveTransaction(), crCtx,
                                                                         btrUndoContext, bufMgr);
    } else {
        status = crCtx->localPage->ConstructCR(this, crCtx);
    }
    if (unlikely(STORAGE_FUNC_FAIL(status))) {
        goto ErrorExit;
    }

    FAULT_INJECTION_CALL_REPLACE(DstoreTransactionFI::READ_PAGE_FROM_CR_BUFFER, &crCtx->useLocalCr);
    crCtx->useLocalCr = crCtx->isGlobalTempTable ? true : (isPageOwner ? crCtx->useLocalCr : true);
    crCtx->useLocalCr = crCtx->snapshot->GetCsn() == MAX_COMMITSEQNO ? true : crCtx->useLocalCr;
    FAULT_INJECTION_CALL_REPLACE_END;

    /* Step 3. If useLocalCr is false, we try to alloc cr buffer and copy local buffer to cr buffer. */
    if (!crCtx->useLocalCr && pageType == PageType::INDEX_PAGE_TYPE) {
        /*
         * 1) If return nullptr, no cr lock.
         * 2) If not, alloc empty cr buffer and hold cr lock, block other threads.
         */
        StorageAssert(!crCtx->isGlobalTempTable);
        BufferDesc *crCommonBufDesc = bufMgr->ReadOrAllocCr(crCtx->baseBufDesc, lastestPageWriteTime);
        if (crCommonBufDesc == INVALID_BUFFER_DESC) {
            /* We have to use local cr buffer. */
            crCtx->useLocalCr = true;
        } else {
            /* We use cr buffer and release cr lock. */
            StorageAssert(crCommonBufDesc->IsCrPage());
            StorageAssert(!crCommonBufDesc->IsCrValid());
            rc = memcpy_s(crCommonBufDesc->GetPage(), BLCKSZ,
                          static_cast<char*>(static_cast<void*>(crCtx->localPage)), BLCKSZ);
            storage_securec_check(rc, "\0", "\0");
            bufMgr->FinishCrBuild(crCommonBufDesc, crCtx->pageMaxCsn);
            crCtx->crBufDesc = crCommonBufDesc;
        }
    }
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_TRANSACTION,
           ErrMsg("CR page is constructed successfully from base "
                  "page(%d, %u) according to snapshot.csn = %lu, "
                  "pageMaxCsn = %lu, is local cr: %d",
                  crCtx->baseBufDesc->GetPageId().m_fileId, crCtx->baseBufDesc->GetPageId().m_blockId, GetSnapshotCsn(),
                  crCtx->pageMaxCsn, crCtx->useLocalCr));
#endif
Finish:
    /* Step 4. Unpin base page. */
    bufMgr->Release(crCtx->baseBufDesc);

    storage_trace_exit(TRACE_ID_Transaction__ConstructCrPage);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

/*
 * Get latest snapshot from CSN node. In read committed, we get new snapshot for every query and
 * one query can only use one snapshot. In transaction snapshot, we get new snapshot for every
 * transaction and one transaction can only use one snapshot.
 *
 * useSnapshotNow: If true, means sql engine wants to use SNAPSHOT_NOW to scan catalog.
 *                 In other scenarios, useSnapshotNow is false.
 */
RetStatus Transaction::SetSnapshotCsn(bool useSnapshotNow)
{
    storage_trace_entry(TRACE_ID_Transaction__SetSnapshotCsn);
    RetStatus status = DSTORE_SUCC;
    CommitSeqNo csn;
    CommitSeqNo minCursorSnapshotCsn;
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    if (unlikely(!InTransaction())) {
        storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
        goto ErrorExit;
    }
    if (pdb != nullptr && pdb->GetPdbRoleMode() == PdbRoleMode::PDB_STANDBY) {
        m_snapshot.SetSnapshotType(SnapshotType::SNAPSHOT_MVCC);
        m_snapshot.SetCsn(UINT64_MAX);
        ErrLog(DSTORE_DEBUG1, MODULE_TRANSACTION,
               ErrMsg("Set PDB_STANDBY snapshot.csn = %lu when isolation level is %d and snapshot type is %d",
                      m_snapshot.GetCsn(), static_cast<int>(m_isolationLevel),
                      static_cast<int>(m_snapshot.GetSnapshotType())));
        return DSTORE_SUCC;
    }

    if (likely(!useSnapshotNow)) {
        m_snapshot.SetSnapshotType(SnapshotType::SNAPSHOT_MVCC);
        /* No need to set SnapshotCsn if we already have a valid one */
        if (GetSnapshotCsn() != INVALID_CSN) {
            goto Finish;
        }
        /*
         * Set local CSN to max, this is for simulating an atomic operation
         * of getting current CSN then set local CSN.
         */
        SetThrdLocalCsn(MAX_COMMITSEQNO);
        if (STORAGE_FUNC_FAIL(m_csnMgr->GetNextCsn(csn, false))) {
            goto ErrorExit;
        }
        /* Get the minimum snapshotCsn from open cursors */
        minCursorSnapshotCsn = GetCursorSnapshotMinCsn();

        /*
         * Update csnMin of our own proc
         * Note: once we call SetSnapshotCsn, we update our own snapshot
         * which means old snapshot will be invalid and the new snapshot
         * will become our min snapshot. Therefore, we could update csnMin
         * directly after checking it with minCursorSnapshotCsn
         */
        StorageAssert(minCursorSnapshotCsn ==  INVALID_CSN || minCursorSnapshotCsn <= csn);
        SetThrdLocalCsn(minCursorSnapshotCsn ==  INVALID_CSN ? csn : minCursorSnapshotCsn);
        m_snapshot.SetCsn(csn);
    } else {
        m_snapshot.SetSnapshotType(SnapshotType::SNAPSHOT_NOW);
    }
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_TRANSACTION,
           ErrMsg("Set snapshot.csn = %lu when isolation level is %d and snapshot type is %d", m_snapshot.GetCsn(),
                  static_cast<int>(m_isolationLevel), static_cast<int>(m_snapshot.GetSnapshotType())));
#endif
Finish:
    storage_trace_exit(TRACE_ID_Transaction__SetSnapshotCsn);
    return status;

ErrorExit:
    SetThrdLocalCsn(INVALID_CSN);
    status = DSTORE_FAIL;
    goto Finish;
}

/*
 * Set flashback snapshot for current scan.
 */
void Transaction::SetSnapshotCsnForFlashback(CommitSeqNo csn)
{
    StorageReleasePanic(!DstoreCsnIsValid(csn), MODULE_TRANSACTION, ErrMsg("input csn is invalid!"));
    StorageReleasePanic(!InTransaction(), MODULE_TRANSACTION, ErrMsg("Not in transaction!"));
    StorageReleasePanic(m_isolationLevel != TrxIsolationType::XACT_READ_COMMITTED, MODULE_TRANSACTION,
                        ErrMsg("Does not support in non READ COMMITTED transaction!"));
    m_snapshot.SetSnapshotType(SnapshotType::SNAPSHOT_MVCC);
    m_snapshot.SetCsn(csn);
}

/*
 * Get latest snapshot from CSN node, used for external interfaces.
 */
RetStatus Transaction::GetSnapshotCsnForFlashback(CommitSeqNo &csn)
{
    StorageAssert(m_snapshot.GetSnapshotType() == SnapshotType::SNAPSHOT_MVCC);
    return m_csnMgr->GetNextCsn(csn, false);
}

/*
 * If the given xid is current transaction's xid.
 */
bool Transaction::IsCurrent(Xid xid)
{
    storage_trace_entry(TRACE_ID_Transaction__IsCurrent);
    bool isCurrent = false;

    if (!xid.IsValid()) {
        isCurrent = false;
        goto Finish;
    }

    if (xid == m_currTransState.xid) {
        isCurrent = true;
        goto Finish;
    }

Finish:
    storage_trace_exit(TRACE_ID_Transaction__IsCurrent);
    return isCurrent;
}

void Transaction::RegisterCallback(TrxCallback callbck, void *arg)
{
    TrxCallBckItem *item = nullptr;
    item = (TrxCallBckItem *)DstoreMemoryContextAlloc(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE),
                                                      sizeof(TrxCallBckItem));
    StorageReleasePanic(item == nullptr, MODULE_TRANSACTION, ErrMsg("memory is not enough!"));
    item->callbckFun = callbck;
    item->arg = arg;
    item->next = m_callbckList;
    m_callbckList = item;
}

void Transaction::CopyCallbackList(TrxCallBckItem *callbckList)
{
    TrxCallBckItem *item = nullptr;
    for (item = callbckList; item; item = item->next) {
        RegisterCallback(item->callbckFun, item->arg);
    }
}

void Transaction::FireCallback(TrxEvent event)
{
    TrxCallBckItem *item = nullptr;
    for (item = m_callbckList; item; item = item->next) {
        (*item->callbckFun)(event, item->arg);
    }
}

void Transaction::SetSnapshotCsnForTac(CommitSeqNo csn)
{
    StorageReleasePanic(!DstoreCsnIsValid(csn), MODULE_TRANSACTION, ErrMsg("input csn is invalid!"));
    StorageReleasePanic(!InTransaction(), MODULE_TRANSACTION, ErrMsg("Not in transaction!"));
    m_snapshot.SetSnapshotType(SnapshotType::SNAPSHOT_MVCC);
    m_snapshot.SetCsn(csn);
}

void Transaction::QueryXidStatus(XidStatus *xidstatus)
{
    if (unlikely(!xidstatus->GetXid().IsValid())) {
        return;
    }

    TransactionSlot trxSlot;
    if (m_instance->GetPdb(m_pdbId) == nullptr) {
        return;
    }
    TransactionMgr *transactionMgr = m_instance->GetPdb(m_pdbId)->GetTransactionMgr();
Retry:
    if (STORAGE_FUNC_FAIL(transactionMgr->GetTransactionSlotCopy(m_pdbId, xidstatus->GetXid(), trxSlot))) {
        goto Retry;
    }
    StorageReleasePanic(trxSlot.status == TXN_STATUS_UNKNOWN, MODULE_TRANSACTION,
        ErrMsg("QueryXidStatus TXN_STATUS_UNKNOWN, pdbId:%u, csn:%lu, xid(%d, %lu)", m_pdbId, trxSlot.csn,
            static_cast<int32>(xidstatus->GetXid().m_zoneId), xidstatus->GetXid().m_logicSlotId));

    if (trxSlot.status == TXN_STATUS_PENDING_COMMIT) {
        /*
         * Because of pendingCsn <= commitCsn, when snapshot csn <= pendingCommitcsn,
         * we can deduce that snapshot csn <= commitCsn. Therefore, this xid is invisible,
         * there's no need to wait transaction end. If GetSnapshotCsn() is INVALID_CSN(0),
         * we must wait until transaction end.
         */
        if (GetSnapshotCsn() != INVALID_CSN && trxSlot.csn >= GetSnapshotCsn()) {
            xidstatus->SetNeedWaitPendingTxn(false);
        }
    }
    if (trxSlot.status == TXN_STATUS_COMMITTED) {
        xidstatus->SetCsn(trxSlot.csn);
    }
    xidstatus->SetStatus(trxSlot.status);
    xidstatus->SetInitialized();
}

char Transaction::StatusCode() const
{
    switch (m_currTransState.blockState) {
        case TBlockState::TBLOCK_DEFAULT:
        case TBlockState::TBLOCK_STARTED:
            return 'I'; /* idle --- not in transaction */
        case TBlockState::TBLOCK_BEGIN:
        case TBlockState::TBLOCK_INPROGRESS:
        case TBlockState::TBLOCK_END:
            return 'T'; /* in transaction */
        case TBlockState::TBLOCK_ABORT:
        case TBlockState::TBLOCK_ABORT_END:
        case TBlockState::TBLOCK_ABORT_PENDING:
            return 'E'; /* in failed transaction */
        default:
            break;
    }

    return 0; /* keep compiler quiet */
}

TBlockState Transaction::GetCurTxnBlockState()
{
    return m_currTransState.blockState;
}

TransAbortStage Transaction::GetCurAbortStage()
{
    return m_currTransState.abortStage;
}
void Transaction::SetCurAbortStage(TransAbortStage abortStage)
{
    m_currTransState.abortStage = abortStage;
}

RetStatus Transaction::AddCursorSnapshot(const char *name)
{
    /* Check current transaction state. */
    if (!InTransaction()) {
        storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(GetCursorSnapshotList()->AddCursorSnapshot(name, m_snapshot))) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("AddCursorSnapshot name %s, snapshot csn %lu, snapshot cid %u fail, errno %lld.",
                name, m_snapshot.GetCsn(), m_snapshot.GetCid(), StorageGetErrorCode()));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus Transaction::DeleteCursorSnapshot(const char *name)
{
    if (STORAGE_FUNC_FAIL(GetCursorSnapshotList()->DeleteCursorSnapshot(name))) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("DeleteCursorSnapshot name %s, fail, errno %lld.",
                name, StorageGetErrorCode()));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus Transaction::CreateSavepoint(const char *name)
{
    if (STORAGE_FUNC_FAIL(m_thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Failed to alloc transaction slot."));
        return DSTORE_FAIL;
    }
    RetStatus ret = GetSavepointList()->AddSavepoint(name);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("Savepoint Type: %d, Savepoint Name: %s add fail, errno %lld.",
                SavepointList::GetSavepointType(name), SavepointList::GetSavepointName(name),
                StorageGetErrorCode()));
    }

    return DSTORE_SUCC;
}

/*
 * Add transaction or subtransaction source
 */
RetStatus Transaction::SaveExtraResPtrToSavepoint(const char *name, void* data)
{
    bool isExist = GetSavepointList()->IsSavepointExist(name);
    if (unlikely(!isExist)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("Savepoint Type: %d, Savepoint Name: %s doesn't exist, save ExtraResPtr fail.",
            SavepointList::GetSavepointType(name), SavepointList::GetSavepointName(name)));
        storage_set_error(TRANSACTION_ERROR_SAVEPOINT_NOT_FOUND, SavepointList::GetSavepointName(name));
        return DSTORE_FAIL;
    }

    return GetSavepointList()->SaveExtraResPtrToSavepoint(name, data);
}

/*
 * Get transaction or subtransaction source
 */
void* Transaction::GetExtraResPtrFromSavepoint(const char* name)
{
    bool isExist = GetSavepointList()->IsSavepointExist(name);
    if (unlikely(!isExist)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("Savepoint Type: %d, Savepoint Name: %s doesn't exist, get ExtraResPtr fail.",
            SavepointList::GetSavepointType(name), SavepointList::GetSavepointName(name)));
        storage_set_error(TRANSACTION_ERROR_SAVEPOINT_NOT_FOUND, SavepointList::GetSavepointName(name));
        return nullptr;
    }
    return GetSavepointList()->GetExtraResPtrFromSavepoint(name);
}

void* Transaction::GetExtraResPtrFromCurrentSavepoint()
{
    bool isEmpty = GetSavepointList()->IsEmpty();
    if (unlikely(isEmpty)) {
        return nullptr;
    }
    return GetSavepointList()->GetExtraResPtrFromCurrentSavepoint();
}

RetStatus Transaction::ReleaseSavepoint(const char *name,
    int16 *userSavepointCounter, int16 *exceptionSavepointCounter)
{
    bool isExist = GetSavepointList()->IsSavepointExist(name);
    if (!isExist) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("Savepoint Type: %d, Savepoint Name: %s doesn't exist, release fail.",
            SavepointList::GetSavepointType(name), SavepointList::GetSavepointName(name)));
        storage_set_error(TRANSACTION_ERROR_SAVEPOINT_NOT_FOUND, SavepointList::GetSavepointName(name));
        return DSTORE_FAIL;
    }

    RetStatus ret = GetSavepointList()->ReleaseSavepoint(name, userSavepointCounter, exceptionSavepointCounter);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("Savepoint Type: %d, Savepoint Name: %s release fail, errno %lld.",
                SavepointList::GetSavepointType(name), SavepointList::GetSavepointName(name),
                StorageGetErrorCode()));
    }
    return ret;
}

RetStatus Transaction::RollbackToSavepoint(const char *name,
    int16 *userSavepointCounter, int16 *exceptionSavepointCounter)
{
    bool isExist = GetSavepointList()->IsSavepointExist(name);
    if (!isExist) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("Savepoint Type: %d, Savepoint Name: %s doesn't exist, rollback fail.",
                SavepointList::GetSavepointType(name), SavepointList::GetSavepointName(name)));
        storage_set_error(TRANSACTION_ERROR_SAVEPOINT_NOT_FOUND, SavepointList::GetSavepointName(name));
        return DSTORE_FAIL;
    }
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("Active transaction is null when rollback to savepoint, pdbId[%u].", m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = GetSavepointList()->RollbackToSavepoint(name, userSavepointCounter, exceptionSavepointCounter);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("Savepoint Type: %d, Savepoint Name: %s rollback fail, errno %lld.",
                SavepointList::GetSavepointType(name), SavepointList::GetSavepointName(name),
                StorageGetErrorCode()));
        return DSTORE_FAIL;
    }
    if (!transaction->GetCurrentXid().IsValid()) {
        /* Current transaction is read-only */
        return ret;
    }

    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(m_pdbId)->GetTransactionMgr();
    ret = transactionMgr->GetLastUndoPtr(GetCurrentXid(), m_lastUndoPtr);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("Savepoint Type: %d, Savepoint Name: %s get last undo fail, errno %lld.",
                SavepointList::GetSavepointType(name), SavepointList::GetSavepointName(name),
                StorageGetErrorCode()));
    }

    if (ret == DSTORE_SUCC && thrd->GetActiveTransaction()->m_currTransState.blockState == TBlockState::TBLOCK_ABORT) {
        /* if we are in a TBLOCK_ABORT transaction, we can rollback to TBLOCK_INPROGRESS after rollback to a sp */
        thrd->GetActiveTransaction()->m_currTransState.blockState = TBlockState::TBLOCK_INPROGRESS;
    }
    return ret;
}

RetStatus Transaction::CheckSavepointCounter(int16 *userSavepointCounter, int16 *exceptionSavepointCounter) const
{
    bool hasCounters = (userSavepointCounter != nullptr && exceptionSavepointCounter != nullptr);
    if (hasCounters) {
        *userSavepointCounter = 0;
        *exceptionSavepointCounter = 0;
    } else if ((userSavepointCounter != nullptr) || (exceptionSavepointCounter != nullptr)) {
        /*
         * If counters are required, they will always be required for both exception and user savepoints,
         * never for just one.
         */
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("ReleaseSavepoint counter error: Both counters should be nullptr or allocated"));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus Transaction::Remember2PLock(const LockTag &tag, LockMode mode, LockMgrType mgrType)
{
    return m_lockResource.RememberLock(tag, mode, mgrType);
}

RetStatus Transaction::Forget2PLock(const LockTag &tag, LockMode mode, LockMgrType mgrType)
{
    m_lockResource.ForgetLock(tag, mode, mgrType);
    return DSTORE_SUCC;
}

bool Transaction::Has2PLock(const LockTag &tag, LockMode mode, LockMgrType mgrType)
{
    return m_lockResource.IsLockExist(tag, mode, mgrType);
}

bool Transaction::HasAny2PLockInMode(LockMode mode)
{
    return m_lockResource.HasLockInMode(mode);
}

LockResource::SubLockResourceID Transaction::Generate2PLockSubResourceID()
{
    return m_lockResource.GenerateSubLockResourceID();
}

void Transaction::Release2PLocksAcquiredAfter(LockResource::SubLockResourceID subResId)
{
    return m_lockResource.ReleaseLocksAcquiredAfter(subResId);
}

uint32 Transaction::GetHold2PLockCnt(const LockTag &tag, LockMode mode, LockMgrType mgrType)
{
    return m_lockResource.GetLockCnt(tag, mode, mgrType);
}

RetStatus Transaction::RememberLazy2PLock(const LockTag &tag, LockMode mode, LockMgrType mgrType, bool &isAlreadyHeld)
{
    return m_lockResource.LazyLock(tag, mode, mgrType, isAlreadyHeld);
}

RetStatus Transaction::ForgetLazy2PLock(const LockTag &tag, LockMode mode, LockMgrType mgrType)
{
    return m_lockResource.ReleaseLazyLock(tag, mode, mgrType);
}

RetStatus Transaction::AcquireLazyLocks()
{
    return m_lockResource.ActuallyAcquireLazyLocks();
}

XidStatus::XidStatus(Xid inxid, Transaction *intrx, TD *td)
    : xid(inxid), trx(intrx), needWaitPendingTxn(true)
{
    status = TXN_STATUS_UNKNOWN;
    csn = INVALID_CSN;
    isInitialized = false;

    if (td == nullptr) {
        return;
    }

    if (td->TestStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE)) {
        status = TXN_STATUS_FROZEN;
        isInitialized = true;
    }

    if (td->TestCsnStatus(IS_CUR_XID_CSN)) {
        status = TXN_STATUS_COMMITTED;
        csn = td->GetCsn();
        isInitialized = true;
    }
}

/*
 * Set snapshot for current transaction, used for external interfaces.
 */
RetStatus Transaction::SetTransactionSnapshotCid(CommandId cid)
{
    if (!InTransaction()) {
        storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
        return DSTORE_FAIL;
    }
    m_snapshot.SetCid(cid);
    return DSTORE_SUCC;
}

/*
 * Set snapshot for current transaction, used for external interfaces.
 */
RetStatus Transaction::SetTransactionSnapshotCsn(CommitSeqNo csn)
{
    if (!InTransaction()) {
        storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
        return DSTORE_FAIL;
    }
    m_snapshot.SetSnapshotType(SnapshotType::SNAPSHOT_MVCC);
    m_snapshot.SetCsn(csn);
    return DSTORE_SUCC;
}

/* Dangerous interface, you must know what you are doing when using it */
void Transaction::SaveCurTransactionState()
{
    g_currentTxnBlockState = m_currTransState.blockState;
    g_currentTxnState = m_currTransState.state;
}

/* Dangerous interface, you must know what you are doing when using it */
void Transaction::RestoreCurTransactionState()
{
    StorageReleasePanic(g_currentTxnBlockState == TBlockState::TBLOCK_DEFAULT, MODULE_TRANSACTION,
        ErrMsg("error use of RestoreCurTransactionState, current saved block state is default!"))
    m_currTransState.blockState = g_currentTxnBlockState;
    m_currTransState.state = g_currentTxnState;

    /* reset to default */
    g_currentTxnBlockState = TBlockState::TBLOCK_DEFAULT;
    g_currentTxnState = TransState::TRANS_DEFAULT;
}

/* Dangerous interface, you must know what you are doing when using it */
void Transaction::SetCurTxnBlockState(TBlockState tBlockState)
{
    m_currTransState.blockState = tBlockState;
}

#ifdef UT
/*
 * Get current transaction's snapshot.
 */
Snapshot Transaction::GetSnapshotData()
{
    if (!InTransaction()) {
        storage_set_error(TRANSACTION_ERROR_INVALID_STATE, TransStateAsString(m_currTransState.state));
        return nullptr;
    }
    return &m_snapshot;
}
#endif

}  // namespace DSTORE
