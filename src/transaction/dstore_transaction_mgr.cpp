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
 * This file defines the transaction manager, which handles functions related
 * to a given Xid.
 *
 * IDENTIFICATION
 *        src/transaction/dstore_transaction_mgr.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <cstring>
#include "common/log/dstore_log.h"
#include "common/instrument/trace/dstore_transaction_trace.h"
#include "errorcode/dstore_undo_error_code.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "lock/dstore_xact_lock_mgr.h"
#include "page/dstore_td.h"
#include "undo/dstore_undo_zone.h"
#include "index/dstore_btree_undo_data_struct.h"
#include "tuple/dstore_index_tuple.h"
#include "undo/dstore_undo_record.h"
#include "transaction/dstore_transaction_types.h"
#include "transaction/dstore_transaction_perf_unit.h"
#include "transaction/dstore_transaction_mgr.h"

namespace DSTORE {

struct ActiveTrxEntry {
    ThreadId threaId;
    uint64 xid;
    CommitSeqNo csn;
};

TransactionMgr::TransactionMgr(UndoMgr *undoMgr, CsnMgr *csnMgr, PdbId pdbId)
    : m_undoMgr(undoMgr), m_csnMgr(csnMgr), m_rollbackTrxTaskMgr(nullptr), m_pdbId(pdbId)
{
}

RetStatus TransactionMgr::StartDispatchRollbackTask()
{
    if (m_rollbackTrxTaskMgr) {
        return DSTORE_SUCC;
    }
    m_rollbackTrxTaskMgr = DstoreNew(g_dstoreCurrentMemoryContext) RollbackTrxTaskMgr(m_pdbId);
    if (STORAGE_VAR_NULL(m_rollbackTrxTaskMgr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_rollbackTrxTaskMgr->Initialize())) {
        return DSTORE_FAIL;
    }
#ifndef UT
    m_rollbackTrxTaskMgr->StartDispatch();
#endif
    return DSTORE_SUCC;
}

void TransactionMgr::StopDispatchRollbackTask()
{
    if (m_rollbackTrxTaskMgr) {
#ifndef UT
        m_rollbackTrxTaskMgr->StopDispatch();
        static constexpr long interval = 1000; /* 1ms */
        while (m_rollbackTrxTaskMgr->IsDispatching()) {
            m_rollbackTrxTaskMgr->WakeupDispatch();
            GaussUsleep(interval);
        }
#endif
        m_rollbackTrxTaskMgr->Destroy();
        delete m_rollbackTrxTaskMgr;
        m_rollbackTrxTaskMgr = nullptr;
    }
}

PdbId TransactionMgr::GetPdbId()
{
    return m_pdbId;
}

Transaction *TransactionMgr::GetNewTransaction(ZoneId zid, BufPrivateRefCount *bufferPrivateRefCount,
                                               bool isAutonomous, bool isSpecialAyncCommitAutoTrx)
{
    DstoreMemoryContext ctx = nullptr;
    if (!isAutonomous) {
        ctx = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_TRANSACTION);
    } else {
        ctx = thrd->GetTransactionMemoryContext();
    }
    Transaction *transaction = DstoreNew(ctx)
        Transaction(g_storageInstance, thrd, zid, bufferPrivateRefCount, m_pdbId, isAutonomous,
        isSpecialAyncCommitAutoTrx);
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Failed to alloc transaction."));
        return nullptr;
    }
    if (STORAGE_FUNC_FAIL(transaction->Initialize())) {
        DstorePfreeExt(transaction);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Failed to init transaction, zoneId: %d", zid));
        return nullptr;
    }
    return transaction;
}

RetStatus TransactionMgr::WaitEndOfAllActiveTrxsEarlierThanSnapshot(CommitSeqNo targetCsn, TimestampTz startTime)
{
    /* Step 1. Collect all active transactions */
    /* Create hash table to save pid of thread of concurrent active transactions which hasn't alloced xid */
    RetStatus ret = DSTORE_SUCC;
    int maxThreadNum = static_cast<int>(g_storageInstance->GetGuc()->ncores);
    HASHCTL hashInfo;
    hashInfo.keysize = sizeof(ActiveTrxEntry);
    hashInfo.entrysize = sizeof(ActiveTrxEntry);
    hashInfo.hash = tag_hash;
    hashInfo.hcxt = g_dstoreCurrentMemoryContext;
    HTAB *activeTrxs = hash_create("activeTrxs", maxThreadNum, &hashInfo, (HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION));
    if (STORAGE_VAR_NULL(activeTrxs)) {
        storage_set_error(TRANSACTION_ERROR_OUT_OF_MEMORY);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("failed to create hash when wait all active transactions end."));
        return DSTORE_FAIL;
    }

    /* Record starting time. We'll stop if waiting timeout */
    startTime = (startTime == TIMESTAMPTZ_MAX) ? GetCurrentTimestampInSecond() : startTime;
    TimestampTz currTime = startTime;

    /* Traverse all threads to collect infomation of transactions */
    ThreadCoreMgr::ThreadIterator *iterator = new ThreadCoreMgr::ThreadIterator(g_storageInstance->GetThreadCoreMgr());
    if (STORAGE_VAR_NULL(iterator)) {
        hash_destroy(activeTrxs);
        storage_set_error(TRANSACTION_ERROR_OUT_OF_MEMORY);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("failed to new thread iterator when wait all active transactions end."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_TRANSACTION,
        ErrMsg("Start to wait active transaction in pdb:%u, target csn:%lu, start time:%ld",
        m_pdbId, targetCsn, startTime));
    ThreadXact *thrdXact = nullptr;
    ThreadId thrdId;
    int numThrd = 0;
    bool found = false;
    ThreadContext *thrdContext = nullptr;
    while ((thrdXact = iterator->GetNextThreadXactInPdb(m_pdbId, &thrdId, &thrdContext)) != nullptr) {
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent  */
            numThrd = 0;
            ret = DSTORE_FAIL;
            break;
        }
        if (thrdId == thrd->GetThreadId()) {
            /* Skip myself to avoid dead lock */
            continue;
        }
        uint64 xid = GsAtomicReadU64(&thrdXact->currentActiveXid);
        CommitSeqNo csn = GsAtomicReadU64(&thrdXact->csnMin);
        if (xid == INVALID_XID.m_placeHolder) {
            CommitSeqNo snapshotCsn = INVALID_CSN;
            if (thrdContext != nullptr && thrdContext->GetActiveTransaction() != nullptr) {
                snapshotCsn = thrdContext->GetActiveTransaction()->GetSnapshotCsn();
            }
            if (snapshotCsn == INVALID_CSN || snapshotCsn > targetCsn) {
                continue;
            }
        }
        numThrd++;
        ActiveTrxEntry trxEntry = {.threaId = thrdId, .xid = xid, .csn = csn};
        hash_search(activeTrxs, static_cast<void *>(&trxEntry), HASH_ENTER, &found);
    }
    delete iterator;

    /* Step 2. Wait for active transaction end */
    /*
     * Scan the transaction of each thread we collected before.
     * Wait if there's and active writing transaction with an Xid allocated,
     * For non-xid case, skip if there's no valid snapshot csn at all, also skip if there's a snapshot for reading that
     * the snapshot csn is already greater than the targetCsn.
     * Otherwise we'll re-collect the transaction infomation from thread and do all of the check again.
     */
    uint64 retryCounter = 0UL;
    UNUSE_PARAM bool trxFailed = false;
    HASH_SEQ_STATUS status;
    hash_seq_init(&status, activeTrxs);
    ActiveTrxEntry *trxEntry = nullptr;
    Xid thrdXid = INVALID_XID;
    CommitSeqNo thrdSnapshotCsn = INVALID_CSN;
    int numOfThreadsWaitEnd = 0;
    while (numOfThreadsWaitEnd < numThrd) {
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent */
            ret = DSTORE_FAIL;
            break;
        }
        currTime = GetCurrentTimestampInSecond();
        if (unlikely(currTime - startTime >= TRANSACTON_MAX_WAIT_TIME_IN_SEC)) {
            /* Waiting timeout */
            storage_set_error(TRANSACTION_ERROR_WAIT_TIMEOUT, retryCounter);
            ret = DSTORE_FAIL;
            break;
        }
        retryCounter++;
        bool currentThrdTrxEnd = false;
        trxEntry = (trxEntry == nullptr) ? static_cast<ActiveTrxEntry *>(hash_seq_search(&status)) : trxEntry;
        if (trxEntry == nullptr) {
            /* Rescan it */
            hash_seq_init(&status, activeTrxs);
            continue;
        }
        thrdId = trxEntry->threaId;
        CommitSeqNo currentCsn = trxEntry->csn;
        if (trxEntry->xid != INVALID_XID.m_placeHolder) {
            ret = WaitForTransactionEnd(Xid(trxEntry->xid), trxFailed);
            /* We don't care if the transaction failed. We just want to wait until all active transactions end. */
            currentThrdTrxEnd = (ret == DSTORE_SUCC);
        } else if (currentCsn == INVALID_CSN || currentCsn > targetCsn) {
            /* No writing transaction yet. The thread local snapshot csn is greater than the targetCsn now, thus any
             * father writing would see the changes made by the given transaction (with committed csn = targetCsn). */
            currentThrdTrxEnd = true;
        }
        if (STORAGE_FUNC_FAIL(ret)) {
            break;
        }
        if (currentThrdTrxEnd) {
            numOfThreadsWaitEnd++;
            hash_search(activeTrxs, static_cast<void *>(trxEntry), HASH_REMOVE, &found);
            trxEntry = nullptr;
            continue;
        }

        /* No writing transaction now and the changes of the given transaction might still be invisible to later
         * writing transactions. */
        /* Firstly update transaction info of the specific thread */
        ThreadId currThrdId = INVALID_THREAD_ID;
        iterator = new ThreadCoreMgr::ThreadIterator(g_storageInstance->GetThreadCoreMgr());
        if (STORAGE_VAR_NULL(iterator)) {
            storage_set_error(TRANSACTION_ERROR_OUT_OF_MEMORY);
            ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
                   ErrMsg("failed to new thread iterator when wait all active transactions end."));
            ret = DSTORE_FAIL;
            break;
        }
        while ((thrdXact = iterator->GetNextThreadXactInPdb(m_pdbId, &currThrdId, &thrdContext)) != nullptr &&
               currThrdId != thrdId) {}
        delete iterator;
        if (STORAGE_VAR_NULL(thrdXact)) {
            /* No active transaction in current instance. We're done */
            break;
        }
        if (currThrdId != thrdId) {
            /* The thread is inactive, meaning there's no in-progress transaction */
            numOfThreadsWaitEnd++;
            hash_search(activeTrxs, static_cast<void *>(trxEntry), HASH_REMOVE, &found);
            trxEntry = nullptr;
            continue;
        }
        thrdXid.m_placeHolder = GsAtomicReadU64(&thrdXact->currentActiveXid);
        thrdSnapshotCsn = GsAtomicReadU64(&thrdXact->csnMin);
        if (thrdXid == INVALID_XID) {
            if (thrdContext != nullptr && thrdContext->GetActiveTransaction() != nullptr) {
                thrdSnapshotCsn = thrdContext->GetActiveTransaction()->GetSnapshotCsn();
            }
        }
        /* We've upadate the transaction info. Go on and check again. */
        trxEntry->xid = thrdXid.m_placeHolder;
        trxEntry->csn = thrdSnapshotCsn;
    }

    ErrLog(DSTORE_LOG, MODULE_INDEX,
        ErrMsg("%s to wait for transaction start before %lu end. waited %d threads, %d end. endtime:%ld, "
        "retried %lu times", (ret == DSTORE_SUCC) ? "Succeeded" : "Failed", targetCsn, numThrd,
        numOfThreadsWaitEnd, currTime, retryCounter));

    hash_destroy(activeTrxs);
    return ret;
}

RetStatus TransactionMgr::WaitForTransactionEnd(Xid xid, bool &txnFailed)
{
    StorageClearError();
    /*
     * If xid transaction belongs to the same storage thread,
     * it will never end until the current active transaction ends.
     */
    if (thrd->ContainsTransaction(xid)) {
        StorageSetErrorCodeOnly(TRANSACTION_INFO_SAME_THREAD_DEADLOCK);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("Same thread transaction, xid(%d, %lu)", static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
        return DSTORE_FAIL;
    }
    RetStatus status = g_storageInstance->GetXactLockMgr()->Wait(m_pdbId, xid);
    if (STORAGE_FUNC_FAIL(status) && StorageGetErrorCode() != UNDO_ERROR_NO_ZONE_OWNER &&
        StorageGetErrorCode() != LOCK_ERROR_NODE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("GetXactLockMgr()->Wait error code is %lld", thrd->GetErrorCode()));
        return DSTORE_FAIL;
    }

    XidStatus xidStatus(xid, thrd->GetActiveTransaction());
    txnFailed = (xidStatus.IsInProgress() || xidStatus.IsPendingCommit());
    return DSTORE_SUCC;
}

RetStatus TransactionMgr::AllocTransactionSlot(Xid &xid)
{
    LatencyStat::Timer timer(&TransactionPerfUnit::GetInstance().m_allocTrxSlotLatency);
    storage_trace_entry(TRACE_ID_TransactionMgr__AllocTransactionSlot);
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Transaction is null when alloc xid, pdbId[%u].", m_pdbId));
        storage_trace_exit(TRACE_ID_TransactionMgr__AllocTransactionSlot);
        return DSTORE_FAIL;
    }
    ZoneId zid = transaction->GetCurrentZoneId();
    UndoZone *uzone = nullptr;
    RetStatus status = DSTORE_SUCC;
    bool needSwitch = false;
    TransactionSlot slot;
    slot.status = TXN_STATUS_IN_PROGRESS;
    slot.csn = INVALID_CSN;
    if (STORAGE_FUNC_FAIL(m_undoMgr->GetUndoZone(zid, &uzone, true))) {
        goto ErrorExit;
    } else if (uzone->IsAsyncRollbacking()) {
        needSwitch = true;
    } else {
        xid = uzone->AllocSlot();
        /*
         * If current undo zone is full, switch a new zone to allocate transaction slot.
         * In most cases, only switching once can satisfy it. If not, we continue to switch.
         */
        if (xid == INVALID_XID) {
            needSwitch = true;
        }
    }

    while (needSwitch) {
        StorageAssert(transaction != nullptr);
        ZoneId retZid = transaction->GetCurrentZoneId();
        if (STORAGE_FUNC_FAIL(m_undoMgr->SwitchZone(uzone, retZid)) || uzone->IsAsyncRollbacking()) {
            continue;
        }
        transaction->SetCurrentZoneId(retZid);
        xid = uzone->AllocSlot();
        if (xid != INVALID_XID) {
            break;
        }
    }
    if (STORAGE_FUNC_FAIL(g_storageInstance->GetXactLockMgr()->Lock(m_pdbId, xid))) {
        ErrLog(DSTORE_WARNING, MODULE_TRANSACTION,
            ErrMsg("Alloc txn slot failed when lock xact: 0x%llx, %s", StorageGetErrorCode(), StorageGetMessage()));
        transaction->SetHoldXactLock(false);
        goto ErrorExit;
    }
    transaction->SetHoldXactLock(true);
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_TRANSACTION,
           ErrMsg("Alloc txn slot for xid(%d, %lu)", static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
#endif
Finish:
    m_undoMgr->WriteTxnInfoToCache(xid, slot, INVALID_CSN, true);
    storage_trace_exit(TRACE_ID_TransactionMgr__AllocTransactionSlot);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

inline PageId GetTransactionSlotPageId(uint64 logicSlotId, PageId startPageId)
{
    PageId res;
    res.m_fileId = startPageId.m_fileId;
    res.m_blockId = startPageId.m_blockId + (logicSlotId / TRX_PAGE_SLOTS_NUM) % TRX_PAGES_PER_ZONE;
    return res;
}

RetStatus TransactionMgr::GetTransactionSlotCopy(PdbId pdbId, Xid xid, TransactionSlot &outTrxSlot)
{
    CommitSeqNo recycleCsnMin = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(pdbId);
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Pdb[%u] is null when get transaction slot copy.", pdbId));
        return DSTORE_FAIL;
    }

    UndoMgr *undoMgr = pdb->GetUndoMgr();
    BufMgrInterface *bufMgr = undoMgr->GetBufferMgr();
    if (!pdb->GetNeedRollbackBarrierInFailover() &&
        STORAGE_FUNC_SUCC(undoMgr->ReadTxnInfoFromCache(xid, outTrxSlot, recycleCsnMin))) {
        return DSTORE_SUCC;
    }
    if (unlikely(pdb->GetNeedRollbackBarrierInFailover() &&
        pdb->GetRollbackBarrierCsnInFailover() != INVALID_CSN)) {
        recycleCsnMin = pdb->GetRollbackBarrierCsnInFailover();
    }
    PageId segmentId = undoMgr->GetUndoZoneSegmentId(xid.m_zoneId, true);
#ifndef UT
    StorageReleasePanic(segmentId.IsInvalid(), MODULE_UNDO, ErrMsg("GetTransactionSlotCopy segmentId (%hu, %u) "
        "invalid, zondId (%lu), slotId(%lu).",
        segmentId.m_fileId, segmentId.m_blockId,
        static_cast<uint64>(xid.m_zoneId), static_cast<uint64>(xid.m_logicSlotId)));
#endif
    /* The first transaction slot page would be the very next page of zone's segment meta page */
    PageId startPageId = {segmentId.m_fileId, segmentId.m_blockId + 1};
    PageId trxSlotPageId = GetTransactionSlotPageId(xid.m_logicSlotId, startPageId);

    /* Read the page from bufferpool by exact pageID */
    StorageReleasePanic(trxSlotPageId.m_blockId - startPageId.m_blockId >= TRX_PAGES_PER_ZONE, MODULE_UNDO,
        ErrMsg("invalid txn pageid(%hu, %u)", trxSlotPageId.m_fileId, trxSlotPageId.m_blockId));
    BufferDesc *trxSlotPageBufDesc = bufMgr->Read(pdbId, trxSlotPageId, LW_SHARED);
    STORAGE_CHECK_BUFFER_PANIC(trxSlotPageBufDesc, MODULE_TRANSACTION, trxSlotPageId);

    /* Obtain target slot page */
    TransactionSlotPage *targetSlotPage = static_cast<TransactionSlotPage*>(trxSlotPageBufDesc->GetPage());
    uint32 slotId = TransactionSlotPage::GetSlotId(xid.m_logicSlotId);
    TransactionSlot *targetTrxSlot = targetSlotPage->GetTransactionSlot(slotId);
    /* Handle target slot is zero slot. */
    if (unlikely(targetTrxSlot->logicSlotId == 0 && targetTrxSlot->logicSlotId != xid.m_logicSlotId)) {
        char *dumpPage = targetSlotPage->Dump();
        TransactionSlotPage::PrevDumpPage(dumpPage);
        DstorePfreeExt(dumpPage);
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("GetTransactionSlotCopy failed, zondId (%lu), slotId(%lu), pdbId(%u)" PAGE_HEADER_FMT,
                      static_cast<uint64>(xid.m_zoneId), static_cast<uint64>(xid.m_logicSlotId), pdbId,
                      PAGE_HEADER_VAL(targetSlotPage)));
        StorageReleaseBufferCheckPanic(true, MODULE_TRANSACTION, trxSlotPageBufDesc->GetBufferTag(),
                                       "GetTransactionSlotCopy failed");
    }
    if (targetTrxSlot->status == TXN_STATUS_FROZEN || targetTrxSlot->logicSlotId != xid.m_logicSlotId) {
        /* Transaction slot location has been recycled */
        outTrxSlot.status = TXN_STATUS_FROZEN;
        outTrxSlot.csn = INVALID_CSN;
    } else {
        outTrxSlot = *targetTrxSlot;
    }
#ifndef UT
    if (unlikely(outTrxSlot.status == TXN_STATUS_UNKNOWN)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
           ErrMsg("transaction status is UNKNOWN. xid:(%d, %lu), pdbid:%u.",
                static_cast<int>(xid.m_zoneId), xid.m_logicSlotId, pdbId));
        StorageReleaseBufferCheckPanic(true, MODULE_TRANSACTION,
            trxSlotPageBufDesc->GetBufferTag(),
            "transaction status is UNKNOWN.");
    }
#endif
    bufMgr->UnlockAndRelease(trxSlotPageBufDesc);
    /*
     * If commit wal not persis yet, we can't use TXN_STATUS_COMMITTED to judge visibility.
     * See details in COMMIT_LOGIC_TAG.
     */
    if (outTrxSlot.status == TXN_STATUS_COMMITTED && !outTrxSlot.IsCommitWalPersist(pdbId)) {
        outTrxSlot.status = TXN_STATUS_PENDING_COMMIT;
    }

    undoMgr->WriteTxnInfoToCache(xid, outTrxSlot, recycleCsnMin);

    return DSTORE_SUCC;
}

RetStatus TransactionMgr::CommitTransactionSlot(Xid xid, bool isSpecialAyncCommitAutoTrx)
{
    storage_trace_entry(TRACE_ID_TransactionMgr__CommitTransactionSlot);
    LatencyStat::Timer pendingTimer(&TransactionPerfUnit::GetInstance().m_pendingCommitLatency, false);
    LatencyStat::Timer commitTimer(&TransactionPerfUnit::GetInstance().m_committingTrxLatency, false);

    RetStatus status = DSTORE_SUCC;
    CommitSeqNo csn = INVALID_CSN;
    ZoneId zid = static_cast<ZoneId>(xid.m_zoneId);
    StorageAssert(zid != INVALID_ZONE_ID);
    UndoZone *uzone = nullptr;
    if (STORAGE_FUNC_FAIL(m_undoMgr->GetUndoZone(zid, &uzone))) {
        goto ErrorExit;
    }
    /*
     * We must set transaction in pending commit before increase global csn. Other session
     * will wait xid when scan the tuples which is in pending commit.
     * */
    pendingTimer.Start();
    g_storageInstance->GetCsnMgr()->m_pendingCommitTrxCnt.fetch_add(1, std::memory_order_acq_rel);
    uzone->Commit<TXN_STATUS_PENDING_COMMIT>(xid, csn, isSpecialAyncCommitAutoTrx);
    if (csn == INVALID_CSN) {
        goto ErrorExit;
    }
    pendingTimer.End();

    commitTimer.Start();
    uzone->Commit<TXN_STATUS_COMMITTED>(xid, csn, isSpecialAyncCommitAutoTrx);
    if (csn == INVALID_CSN) {
        goto ErrorExit;
    }
    commitTimer.End();
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_TRANSACTION,
           ErrMsg("Transaction committed, xid(%d, %lu, csn(%lu)", static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId,
                  csn));
#endif
Finish:
    g_storageInstance->GetCsnMgr()->m_pendingCommitTrxCnt.fetch_sub(1, std::memory_order_acq_rel);
    g_storageInstance->GetXactLockMgr()->Unlock(m_pdbId, xid);
    thrd->GetActiveTransaction()->SetHoldXactLock(false);
    
    storage_trace_exit(TRACE_ID_TransactionMgr__CommitTransactionSlot);
    return status;

ErrorExit:
    pendingTimer.End();
    commitTimer.End();

    status = DSTORE_FAIL;
    goto Finish;
}

RetStatus TransactionMgr::RollbackTransactionSlot(Xid xid)
{
    storage_trace_entry(TRACE_ID_TransactionMgr__RollbackTransactionSlot);
    RetStatus status = DSTORE_SUCC;
    ZoneId zid = static_cast<ZoneId>(xid.m_zoneId);
    StorageAssert(zid != INVALID_ZONE_ID);
    UndoZone *uzone = nullptr;
    if (STORAGE_FUNC_FAIL(m_undoMgr->GetUndoZone(zid, &uzone))) {
        goto ErrorExit;
    }
    if (STORAGE_FUNC_FAIL(uzone->RollbackUndoZone(xid, false))) {
        goto ErrorExit;
    }

#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_TRANSACTION,
           ErrMsg("Transaction rollbacked, xid(%d, %lu)", static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
#endif
Finish:
    if (likely(!g_storageInstance->IsInBackupRestore(m_pdbId)) &&
        g_storageInstance->GetPdb(m_pdbId) != nullptr &&
        !g_storageInstance->GetPdb(m_pdbId)->GetNeedRollbackBarrierInFailover()) {
        if (thrd && thrd->GetActiveTransaction() && thrd->GetActiveTransaction()->HoldXactLock()) {
            g_storageInstance->GetXactLockMgr()->Unlock(m_pdbId, xid);
            thrd->GetActiveTransaction()->SetHoldXactLock(false);
        }
    }
    storage_trace_exit(TRACE_ID_TransactionMgr__RollbackTransactionSlot);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

TrxSlotStatus TransactionMgr::GetTxnSlotStatus(Xid xid)
{
    StorageAssert(xid != INVALID_XID);
Retry:
    TransactionSlot trxSlot;
    if (STORAGE_FUNC_FAIL(GetTransactionSlotCopy(m_pdbId, xid, trxSlot))) {
        goto Retry;
    }
    StorageAssert(trxSlot.status != TXN_STATUS_UNKNOWN);

    return trxSlot.status;
}

#ifdef UT
CommitSeqNo TransactionMgr::GetCsnFromXid(Xid xid)
{
    TransactionSlot trxSlot;
    GetTransactionSlotCopy(m_pdbId, xid, trxSlot);
    StorageAssert(trxSlot.status != TXN_STATUS_UNKNOWN);
    StorageAssert(trxSlot.status != TXN_STATUS_PENDING_COMMIT);
    return trxSlot.GetCsn();
}
#endif

bool TransactionMgr::JudgeTupCommitBeforeSpecCsn(TD *td, ItemPointerData ctid, char *data,
                                                 CommitSeqNo specCsn, CommitSeqNo *tupleCsn)
{
    Xid xid = td->GetXid();
    UndoRecPtr undoRecPtr = td->GetUndoRecPtr();
    CommitSeqNo xidCsn = td->GetCsn();
    StorageAssert(undoRecPtr != INVALID_UNDO_RECORD_PTR);
    UndoRecord undoRecord;

    while (true) {
        if (undoRecPtr == INVALID_ITEM_POINTER) {
            /* Undo record is recycled. */
            return true;
        }
        if (STORAGE_FUNC_FAIL(FetchUndoRecord(xid, &undoRecord, undoRecPtr))) {
            StorageAssert(StorageGetErrorCode() == UNDO_ERROR_RECORD_RECYCLED);
            StorageClearError();
            /* Undo record is recycled. */
            return true;
        }

        /* hit target, but newer than snapshot, return false, implicit last round xidcsn >= snapCsn */
        UndoType type = undoRecord.GetUndoType();
        if (undoRecord.IsHeapUndoRecord()) {
            if (undoRecord.IsMatchedCtid(ctid)) {
                break;
            }
        } else if (type == UNDO_BTREE_INSERT || type == UNDO_BTREE_INSERT_TMP) {
            UndoDataBtreeInsert *undoRecData = static_cast<UndoDataBtreeInsert*>(undoRecord.GetUndoData());
            if (undoRecData->GetHeapCtid() == ctid && data !=nullptr &&
                strncmp(undoRecData->GetValue(), data, undoRecData->GetValueSize()) == 0) {
                break;
            }
        } else if (type == UNDO_BTREE_DELETE || type == UNDO_BTREE_DELETE_TMP) {
            UndoDataBtreeDelete *undoRecData = static_cast<UndoDataBtreeDelete *>(undoRecord.GetUndoData());
            IndexTuple *undoTuple = static_cast<IndexTuple *>(undoRecData->GetData());
            if (undoTuple->GetHeapCtid() == ctid &&
                strncmp(undoTuple->GetValues(), data, undoTuple->GetValueSize()) == 0) {
                break;
            }
        } else {
            StorageAssert(0);
        }

        /* if td is reset before, it means older enough we can return true */
        if (undoRecord.GetTdPreXid() == INVALID_XID) {
            return true;
        }

        /* if xid is changed, a smaller csn need to be processed */
        if (undoRecord.GetTdPreXid() != xid) {
            xidCsn = undoRecord.GetTdPreCsn();
            StorageAssert(xidCsn != INVALID_CSN);
            StorageAssert(undoRecord.GetTdPreCsnStatus() == IS_CUR_XID_CSN);
            /* if older enough we can return true */
            if (xidCsn < specCsn) {
                return true;
            }
        }

        /* prepare to fetch next undo record */
        xid = undoRecord.GetTdPreXid();
        undoRecPtr = undoRecord.GetTdPreUndoPtr();
    }

    /* return the tuple csn if necessary */
    StorageAssert(xidCsn != INVALID_CSN);
    StorageAssert(xidCsn >= specCsn);
    if (tupleCsn != nullptr) {
        *tupleCsn = xidCsn;
    }

    return false;
}

RetStatus TransactionMgr::GetXidFromUndo(Xid &xid, TD *td, ItemPointerData ctid)
{
    xid = td->GetXid();
    UndoRecPtr undoRecPtr = td->GetUndoRecPtr();
    UndoRecord undoRecord;
    while (undoRecPtr != INVALID_ITEM_POINTER) {
        if (STORAGE_FUNC_FAIL(FetchUndoRecord(xid, &undoRecord, undoRecPtr))) {
            if (StorageGetErrorCode() == UNDO_ERROR_RECORD_RECYCLED) {
                break;
            }
            return DSTORE_FAIL;
        }

        if (undoRecord.IsMatchedCtid(ctid)) {
            return DSTORE_SUCC;
        }

        xid = undoRecord.GetTdPreXid();
        undoRecPtr = undoRecord.GetTdPreUndoPtr();
    }

    /* Undo record is recycled. */
    xid = INVALID_XID;
    return DSTORE_SUCC;
}

RetStatus TransactionMgr::ExtendUndoSpaceIfNeeded(Xid xid, uint32 needSize)
{
    FAULT_INJECTION_RETURN(DstoreTransactionFI::EXTEND_UNDO_SPACE_FAIL, DSTORE_FAIL);
    ZoneId zid = static_cast<ZoneId>(xid.m_zoneId);
    UndoZone *uzone = nullptr;
    if (STORAGE_FUNC_FAIL(m_undoMgr->GetUndoZone(zid, &uzone))) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(uzone->ExtendSpaceIfNeeded(needSize))) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

UndoRecPtr TransactionMgr::InsertUndoRecord(Xid xid, UndoRecord *undoRec)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("Active transaction is null when insert undo, xid(%d, %lu), pdbId(%u).",
                      static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId, m_pdbId));
        return INVALID_UNDO_RECORD_PTR;
    }
    ZoneId zid = transaction->GetCurrentZoneId();
    StorageAssert(zid != INVALID_ZONE_ID);
    UndoZone *uzone = nullptr;
    if (STORAGE_FUNC_FAIL(m_undoMgr->GetUndoZone(zid, &uzone))) {
        return INVALID_UNDO_RECORD_PTR;
    }

    /* Check validity before insert it. */
    if (STORAGE_FUNC_FAIL(undoRec->CheckValidity())) {
        storage_set_error(UNDO_ERROR_INVALID_UNDO_RECORD_PTR, undoRec);
        return INVALID_UNDO_RECORD_PTR;
    }

    UndoRecPtr urecPtr = uzone->InsertUndoRecord(undoRec);
    if (unlikely(urecPtr == INVALID_UNDO_RECORD_PTR)) {
        storage_set_error(UNDO_ERROR_INVALID_UNDO_RECORD_PTR, undoRec);
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Insert undo record failed, xid(%d, %lu), pdbId(%u).", static_cast<int32>(xid.m_zoneId),
                      xid.m_logicSlotId, m_pdbId));
        return INVALID_UNDO_RECORD_PTR;
    }

    /* Modify the start or end undo ptr of txn slot. */
    if (unlikely(STORAGE_FUNC_FAIL(uzone->SetSlotUndoPtr(xid, urecPtr, true)))) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("SetSlotUndoPtr fail, xid(%d, %lu), pdbId(%u), urecPtr(%d, %u, %d).",
                      static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId, m_pdbId,
                      urecPtr.GetFileId(), urecPtr.GetBlockNum(), urecPtr.GetOffset()));
        return INVALID_UNDO_RECORD_PTR;
    }

#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(
        DSTORE_DEBUG1, MODULE_TRANSACTION,
        ErrMsg("Insert undo record size: %hu, type is %d, xid(%d, %lu), urecPtr(%d, %u, %d)",
               undoRec->GetUndoDataSize(), static_cast<int32>(undoRec->GetUndoType()), static_cast<int32>(xid.m_zoneId),
               xid.m_logicSlotId, urecPtr.GetFileId(), urecPtr.GetBlockNum(), urecPtr.GetOffset()));
#endif
    return urecPtr;
}

void TransactionMgr::FetchUndoRecordInternal(UndoRecord *undoRec, UndoRecPtr undoRecPtr, bool needStrictCheckUndo,
                                             bool *isVaild)
{
    UndoZone::FetchUndoRecordInternal(m_pdbId, undoRec, undoRecPtr, m_undoMgr->GetBufferMgr(), needStrictCheckUndo,
                                      isVaild);
}

RetStatus TransactionMgr::FetchUndoRecord(Xid xid, UndoRecord *undoRec, UndoRecPtr undoRecPtr)
{
    StorageAssert(xid.m_zoneId != INVALID_ZONE_ID);
    if (STORAGE_FUNC_FAIL(UndoZone::FetchUndoRecord(m_pdbId, undoRec, undoRecPtr, xid, m_undoMgr->GetBufferMgr()))) {
        StorageAssert(StorageGetErrorCode() == UNDO_ERROR_RECORD_RECYCLED);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus TransactionMgr::FetchUndoRecordByMatchedCtidInCurXact(Xid &xid, UndoRecord *undoRec, UndoRecPtr ptr,
                                                                ItemPointerData ctid)
{
    storage_trace_entry(TRACE_ID_TransactionMgr__FetchUndoRecordByMatchedCtid);
    ZoneId zid = static_cast<ZoneId>(xid.m_zoneId);
    StorageAssert(zid != INVALID_ZONE_ID);
    StorageAssert(ctid != INVALID_ITEM_POINTER);
    StorageStat *stat = g_storageInstance->GetStat();
    UndoZone *uzone = nullptr;
    if (STORAGE_FUNC_FAIL(m_undoMgr->GetUndoZone(zid, &uzone))) {
        return DSTORE_FAIL;
    }
    if (unlikely(uzone == nullptr)) {
        return DSTORE_FAIL;
    }
    stat->m_reportWaitStatus(static_cast<uint32_t>(GsStatWaitState::STATE_UNDO_FETCH_RECORD));
    Xid curXid = xid;
    UndoRecPtr curPtr = ptr;
    if (unlikely(UndoZone::IsXidRecycled(m_pdbId, xid))) {
        storage_set_error(UNDO_ERROR_RECORD_RECYCLED);
        stat->m_reportWaitStatus(static_cast<uint32_t>(GsStatWaitState::STATE_WAIT_UNDEFINED));
        return DSTORE_FAIL;
    }

    while (true) {
        if (unlikely(curPtr == INVALID_ITEM_POINTER)) {
            storage_set_error(UNDO_ERROR_RECORD_RECYCLED);
            stat->m_reportWaitStatus(
                static_cast<uint32_t>(GsStatWaitState::STATE_WAIT_UNDEFINED));
            return DSTORE_FAIL;
        }

        FetchUndoRecordInternal(undoRec, curPtr);

        if (undoRec->IsMatchedCtid(ctid)) {
            break;
        }
        Xid preXid = undoRec->GetTdPreXid();
        if (unlikely(preXid != curXid)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO,
                   ErrMsg("Not found undo record in cur xact , xid(%d, %lu), pdbId(%u), ctid({%hu, %u}, %hu).",
                          static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId, m_pdbId, ctid.GetFileId(),
                          ctid.GetBlockNum(), ctid.GetOffset()));
            stat->m_reportWaitStatus(
                static_cast<uint32_t>(GsStatWaitState::STATE_WAIT_UNDEFINED));
            return DSTORE_FAIL;
        }
        curPtr = undoRec->GetTdPreUndoPtr();
    }
    stat->m_reportWaitStatus(static_cast<uint32_t>(GsStatWaitState::STATE_WAIT_UNDEFINED));
    storage_trace_exit(TRACE_ID_TransactionMgr__FetchUndoRecordByMatchedCtid);
    return DSTORE_SUCC;
}

RetStatus TransactionMgr::FetchUndoRecordByMatchedCtid(Xid &xid, UndoRecord *undoRec, UndoRecPtr ptr,
                                                       ItemPointerData ctid, CommitSeqNo *matchedCsn)
{
    storage_trace_entry(TRACE_ID_TransactionMgr__FetchUndoRecordByMatchedCtid);
    ZoneId zid = static_cast<ZoneId>(xid.m_zoneId);
    StorageAssert(zid != INVALID_ZONE_ID);
    StorageAssert(ctid != INVALID_ITEM_POINTER);
    UndoZone *uzone = nullptr;
    if (STORAGE_FUNC_FAIL(m_undoMgr->GetUndoZone(zid, &uzone))) {
        return DSTORE_FAIL;
    }
    if (uzone == nullptr) {
        return DSTORE_FAIL;
    }

    Xid curXid = xid;
    UndoRecPtr curPtr = ptr;
    while (true) {
        if (curPtr == INVALID_ITEM_POINTER) {
            storage_set_error(UNDO_ERROR_RECORD_RECYCLED);
            return DSTORE_FAIL;
        }

        if (STORAGE_FUNC_FAIL(UndoZone::FetchUndoRecord(m_pdbId, undoRec, curPtr, curXid, m_undoMgr->GetBufferMgr()))) {
            StorageAssert(StorageGetErrorCode() == UNDO_ERROR_RECORD_RECYCLED);
            return DSTORE_FAIL;
        }

        if (undoRec->IsMatchedCtid(ctid)) {
            break;
        }
        if (undoRec->GetTdPreXid() != curXid && undoRec->GetTdPreXid() != INVALID_XID) {
            zid = static_cast<ZoneId>(undoRec->GetTdPreXid().m_zoneId);
            StorageAssert(zid != INVALID_ZONE_ID);
            if (STORAGE_FUNC_FAIL(m_undoMgr->GetUndoZone(zid, &uzone))) {
                return DSTORE_FAIL;
            }
            if (uzone == nullptr) {
                return DSTORE_FAIL;
            }
            if (matchedCsn != nullptr) {
                StorageAssert(undoRec->GetTdPreCsn() != INVALID_CSN);
                StorageAssert(undoRec->GetTdPreCsnStatus() == IS_CUR_XID_CSN);
                *matchedCsn = undoRec->GetTdPreCsn();
            }
        }
        curXid = undoRec->GetTdPreXid();
        curPtr = undoRec->GetTdPreUndoPtr();
    }
    xid = curXid; /* for checking visibility by caller */
    storage_trace_exit(TRACE_ID_TransactionMgr__FetchUndoRecordByMatchedCtid);
    return DSTORE_SUCC;
}

RetStatus TransactionMgr::GetLastUndoPtr(Xid xid, UndoRecPtr &ptr)
{
    ZoneId zid = static_cast<ZoneId>(xid.m_zoneId);
    StorageAssert(zid != INVALID_ZONE_ID);
    UndoZone *uzone = nullptr;
    UndoMgr *undoMgr = g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr();
    if (STORAGE_FUNC_FAIL(undoMgr->GetUndoZone(zid, &uzone))) {
        return DSTORE_FAIL;
    }
    uzone->GetSlotCurTailUndoPtr(xid, ptr);
    return DSTORE_SUCC;
}

RetStatus TransactionMgr::RollbackToUndoptr(Xid xid, UndoRecPtr ptr)
{
    ZoneId zid = static_cast<ZoneId>(xid.m_zoneId);
    StorageAssert(zid != INVALID_ZONE_ID);
    UndoZone *uzone = nullptr;
    if (STORAGE_FUNC_FAIL(m_undoMgr->GetUndoZone(zid, &uzone))) {
        return DSTORE_FAIL;
    }

    UndoRecPtr headPtr(ptr.m_placeHolder);
    UndoRecPtr tailPtr;
    uzone->GetSlotCurTailUndoPtr(xid, tailPtr);
    if (STORAGE_FUNC_FAIL(uzone->RollbackUndoRecords(xid, headPtr, tailPtr, false))) {
        return DSTORE_FAIL;
    }

#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_TRANSACTION,
        ErrMsg("Transaction xid(%d, %lu) rollbacked to undo record(%d, %u, %d)",
        static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId, ptr.val.m_pageid.m_fileId,
        ptr.val.m_pageid.m_blockId, ptr.val.m_offset));
#endif
    return DSTORE_SUCC;
}

void TransactionMgr::AsyncRollback(Xid xid, UndoZone *undoZone)
{
    (void)m_rollbackTrxTaskMgr->AddRollbackTrxTask(xid, undoZone);
}

RetStatus TransactionMgr::WaitForOneTransactionEnd(const Xid *xids, uint8 xidNum) const
{
    StorageAssert(xidNum > 0 && xidNum <= ThreadLocalLock::m_waitLockMaxCount);
    StorageClearError();
    /*
     * If xid transaction belongs to the same storage thread,
     * it will never end until the current active transaction ends.
     */
    Xid waitXids[ThreadLocalLock::m_waitLockMaxCount];
    uint32 waitXidsLen = 0;
    for (uint8 i = 0; i < xidNum; i++) {
        if (!thrd->ContainsTransaction(xids[i])) {
            waitXids[waitXidsLen++] = xids[i];
        }
    }
    if (waitXidsLen == 0) {
        StorageSetErrorCodeOnly(TRANSACTION_INFO_SAME_THREAD_DEADLOCK);
        return DSTORE_FAIL;
    }

    PdbId pdbIds[ThreadLocalLock::m_waitLockMaxCount];
    for (uint i = 0; i < ThreadLocalLock::m_waitLockMaxCount; i++) {
        pdbIds[i] = m_pdbId;
    }
    RetStatus status = g_storageInstance->GetXactLockMgr()->WaitForAnyTransactionEnd(pdbIds, waitXids, waitXidsLen);
    if (STORAGE_FUNC_FAIL(status) && StorageGetErrorCode() != UNDO_ERROR_NO_ZONE_OWNER &&
        StorageGetErrorCode() != LOCK_ERROR_NODE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("Wait for any transaction end failed, error code is %lld", thrd->GetErrorCode()));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

char TransactionMgr::GetTacTransactionState(Xid xid) const
{
    if (xid == INVALID_XID) {
        return static_cast<char>(TACTransactionState::TAC_TRX_UNKNOWN);
    }
    XidStatus xidStatus(xid, thrd->GetActiveTransaction());
    TACTransactionState trxState;
    switch (xidStatus.GetStatus()) {
        case TXN_STATUS_IN_PROGRESS:
        case TXN_STATUS_PENDING_COMMIT:
            trxState = TACTransactionState::TAC_TRX_IN_PROGRESS;
            /* Refresh the expiry time to allow the inprogress transaction to finish */
            m_csnMgr->RefreshTacOrphanTrxExpiryTime();
            break;
        case TXN_STATUS_FROZEN:
            trxState = TACTransactionState::TAC_TRX_FROZEN;
            break;
        case TXN_STATUS_COMMITTED:
            trxState = TACTransactionState::TAC_TRX_COMMITTED;
            break;
        case TXN_STATUS_ABORTED:
            trxState = TACTransactionState::TAC_TRX_ABORTED;
            break;
        case TXN_STATUS_UNKNOWN:
        case TXN_STATUS_FAILED:
        default:
            trxState = TACTransactionState::TAC_TRX_UNKNOWN;
            break;
    }
    return static_cast<char>(trxState);
}

bool TransactionMgr::IsAllTaskFinished()
{
    return m_rollbackTrxTaskMgr->IsAllTaskFinished();
}

}
