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
 * dstore_transaction_list.cpp
 *
 *
 * IDENTIFICATION
 *        storage/src/transaction/dstore_transaction_list.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "transaction/dstore_transaction_list.h"
#include <cstring>
#include "buffer/dstore_buf_refcount.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_mgr.h"
#include "lock/dstore_deadlock_detector.h"
#include "errorcode/dstore_transaction_error_code.h"

namespace DSTORE {

RetStatus TransactionList::InitRuntime(bool allocateZoneId, PdbId pdbId)
{
    /* Create a transaction */
    SetPdbId(pdbId);
    return CreateTransaction(allocateZoneId, false);
}

void TransactionList::Destroy()
{
    while (m_activeTransaction != nullptr) {
        Transaction *prevTransaction = m_activeTransaction->m_prevTransaction;
        m_activeTransaction->Destroy();
        delete m_activeTransaction;
        m_activeTransaction = prevTransaction;
    }
    SetPdbId(INVALID_PDB_ID);
}

void TransactionList::ReleaseLocksInAllTransactionRes()
{
    Transaction *curTransaction = m_activeTransaction;
    while (curTransaction != nullptr) {
        curTransaction->ReleaseLocksInTransRes();
        curTransaction = curTransaction->m_prevTransaction;
    }
}

void TransactionList::SetPdbId(PdbId pdbId)
{
    m_pdbId = pdbId;
}

PdbId TransactionList::GetPdbId()
{
    return m_pdbId;
}

Transaction *TransactionList::GetActiveTransaction()
{
    return m_activeTransaction;
}

Transaction *TransactionList::GetTopTransaction()
{
    Transaction *topTransaction = m_activeTransaction;
    while (topTransaction->m_prevTransaction != nullptr) {
        topTransaction = topTransaction->m_prevTransaction;
    }
    return topTransaction;
}

RetStatus TransactionList::CreateTransaction(bool allocateZoneId, bool isAutonomous, bool isSpecialAyncCommitAutoTrx)
{
    /* check params we need to use */
    if (unlikely(thrd->m_memoryMgr == nullptr || thrd->m_memoryMgr->GetRoot() == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
        ErrMsg("CreateTransaction failed for thread memory mgr is null."));
        storage_set_error(TRANSACTION_ERROR_INVALID_PARAM);
        return DSTORE_FAIL;
    }

    StoragePdb *currentPdb = g_storageInstance->GetPdb(m_pdbId);
    if (unlikely(currentPdb == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
        ErrMsg("CreateTransaction failed for pdb is null, pdb id: %u", m_pdbId));
        storage_set_error(TRANSACTION_ERROR_INVALID_PARAM);
        return DSTORE_FAIL;
    }

    if (unlikely(currentPdb->GetTransactionMgr() == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
        ErrMsg("CreateTransaction failed for transaction mgr is null, pdb id: %u", m_pdbId));
        storage_set_error(TRANSACTION_ERROR_INVALID_PARAM);
        return DSTORE_FAIL;
    }

    if (unlikely(currentPdb->GetUndoMgr() == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
        ErrMsg("CreateTransaction failed for undo mgr is null, pdb id: %u", m_pdbId));
        storage_set_error(TRANSACTION_ERROR_INVALID_PARAM);
        return DSTORE_FAIL;
    }

    /* BufPrivateRefCount init */
    char *alignedMem = (char *)DstorePallocAligned(sizeof(BufPrivateRefCount), DSTORE_CACHELINE_SIZE);
    if (STORAGE_VAR_NULL(alignedMem)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to create thread context for alloc alignedMem."));
        return DSTORE_FAIL;
    }
    BufPrivateRefCount *bufferPrivateRefCount = DstoreNew(alignedMem) BufPrivateRefCount();
    if (STORAGE_VAR_NULL(bufferPrivateRefCount)) {
        DstorePfreeAligned(alignedMem);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("CreateTransaction failed to initialize BufPrivateRefCount."));
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return DSTORE_FAIL;
    }
    bufferPrivateRefCount->Initialize();
    
    /*
     * During allocating a zone,
     * we want to use BufPrivateRefCount that will later be
     * assigned to the current active transaction.
     */
    BufPrivateRefCount *oldBufPrivateRefCount = thrd->bufferPrivateRefCount;
    thrd->bufferPrivateRefCount = bufferPrivateRefCount;

    ZoneId zid = INVALID_ZONE_ID;
    if (allocateZoneId) {
        StorageAssert(currentPdb != nullptr);
        if (STORAGE_FUNC_FAIL(currentPdb->GetUndoMgr()->AllocateZoneId(zid))) {
            thrd->bufferPrivateRefCount = oldBufPrivateRefCount;
            bufferPrivateRefCount->Destroy();
            DstorePfreeExt(bufferPrivateRefCount);
            return DSTORE_FAIL;
        }
    }
    thrd->bufferPrivateRefCount = oldBufPrivateRefCount;

    Transaction *transaction = currentPdb->GetTransactionMgr()->
        GetNewTransaction(zid, bufferPrivateRefCount, isAutonomous, isSpecialAyncCommitAutoTrx);
    if (unlikely(transaction == nullptr)) {
        if (allocateZoneId) {
            currentPdb->GetUndoMgr()->ReleaseZoneId(zid);
        }
        bufferPrivateRefCount->Destroy();
        DstorePfreeExt(bufferPrivateRefCount);
        return DSTORE_FAIL;
    }

    if (isAutonomous) {
        StorageAssert(m_activeTransaction != nullptr);
        transaction->CopyCallbackList(m_activeTransaction->m_callbckList);
    }

    /* Save current m_activeTransaction */
    transaction->m_prevTransaction = m_activeTransaction;
    /* Make newly created transaction an active transaction */
    m_activeTransaction = transaction;
    return DSTORE_SUCC;
}

RetStatus TransactionList::CreateAutonomousTransaction(bool isSpecialAyncCommitAutoTrx)
{
    StorageReleasePanic(!m_activeTransaction->InTransaction(), MODULE_TRANSACTION,
                        ErrMsg("CreateAutonomousTransaction must be called inside a transaction!"));
    /* Create an autonomous transaction */
    if (STORAGE_FUNC_FAIL(CreateTransaction(true, true, isSpecialAyncCommitAutoTrx))) {
        return DSTORE_FAIL;
    }
    StorageAssert(m_activeTransaction != nullptr);

    return DSTORE_SUCC;
}

void TransactionList::DestroyAutonomousTransaction()
{
    StorageReleasePanic(!m_activeTransaction->IsAutonomousTransaction(), MODULE_TRANSACTION,
                        ErrMsg("DestroyAutonomousTransaction must be called in a transaction!"));
    StorageAssert(m_activeTransaction != nullptr);
    
    Transaction *transaction = m_activeTransaction;
    Transaction *preTransaction = m_activeTransaction->m_prevTransaction;
    transaction->ReleaseLocksInTransRes();
    transaction->Destroy();
    delete transaction;
    /* Make previously active transaction an active transaction */
    m_activeTransaction = preTransaction;
    return;
}

bool TransactionList::ContainsTransaction(Xid xid) const
{
    for (Transaction *transaction = m_activeTransaction;
        transaction != nullptr;
        transaction = transaction->m_prevTransaction) {
        if (transaction->GetCurrentXid() == xid) {
            return true;
        }
    }
    return false;
}

/**
 * In the autonomous transaction feature, any lock that is not recorded in an active transaction
 * is considered to conflict with the transaction. Since in lock mgr, locks acquired by the same thread
 * do not conflict with each other, some special checks are required here.
 * Caution: This method only considers locks that a transaction has "remembered" as its own.
 */
bool TransactionList::NonActiveTransactionHoldConflict2PLock(const LockTag &tag, LockMode mode,
    LockMgrType mgrType) const
{
    SameThreadDeadlockDetector detector;
    detector.PrepareToCheck(tag, mode);

    for (LockMode holdMode = DSTORE_ACCESS_SHARE_LOCK; holdMode < DSTORE_LOCK_MODE_MAX;
         holdMode = GetNextLockMode(holdMode)) {
        if (!detector.CanLockModeConflict(holdMode)) {
            continue;
        }
 
        for (Transaction *nonActiveTransaction = m_activeTransaction->m_prevTransaction;
             nonActiveTransaction != nullptr;
             nonActiveTransaction = nonActiveTransaction->m_prevTransaction) {
            if (nonActiveTransaction->GetHold2PLockCnt(tag, holdMode, mgrType) > 0) {
                ErrLog(DSTORE_ERROR, MODULE_LOCK,
                    ErrMsg("Inactive transaction already holds lock %s in mode %s, "
                           "and has conflict with requesting mode %s.",
                           tag.ToString().CString(), GetLockModeString(holdMode), GetLockModeString(mode)));
                return true;
            }
        }
    }

    return false;
}

RetStatus TransactionList::ActuallyAcquireLazyLocksOnCurrentThread()
{
    for (Transaction *transaction = m_activeTransaction;
        transaction != nullptr;
        transaction = transaction->m_prevTransaction) {
        if (STORAGE_FUNC_FAIL(transaction->AcquireLazyLocks())) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

bool TransactionList::AllTransactionsHold2PLockMoreThan(const struct LockTag &tag, LockMode mode, LockMgrType mgrType,
    uint32 cnt) const
{
    uint32 transactionHoldLocks = 0;
    for (Transaction *transaction = m_activeTransaction;
        transaction != nullptr;
        transaction = transaction->m_prevTransaction) {
        transactionHoldLocks += transaction->GetHold2PLockCnt(tag, mode, mgrType);
        if (transactionHoldLocks > cnt) {
            return true;
        }
    }
    return false;
}

}  // namespace DSTORE
