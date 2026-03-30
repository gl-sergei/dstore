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
 * dstore_transaction_interface.cpp
 *
 * IDENTIFICATION
 *        src/transaction/dstore_transaction_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "transaction/dstore_transaction_interface.h"
#include "common/log/dstore_log.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "framework/dstore_thread.h"
#include "lock/dstore_xact_lock_mgr.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_types.h"
#include "transaction/dstore_transaction_mgr.h"

namespace TransactionInterface {
using namespace DSTORE;

RetStatus StartTrxCommand()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("StartTrxCommand error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->Start();
}

RetStatus CommitTrxCommand()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("CommitTrxCommand error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->Commit();
}

RetStatus CommitTrxCommandWithoutCleanUpResource()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("CommitTrxCommand error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->CommitWithoutCleanUpResource();
}

void CleanUpResourceAfterCommit()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("CleanUpResource after commit error: transaction is not initialized."));
        return;
    }
    transaction->CleanUpResourceAfterCommit();
}

void CleanUpResourceAfterAbort()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        return;
    }
    transaction->CleanUpResourceAfterAbort();
}

void ForceReleaseAllTransactionLocks()
{
    thrd->ReleaseAllTransactionLocks();
}

RetStatus AbortTrx(bool terminateCurrTrx, bool cleanUpResource)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("AbortTrx error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->Abort(terminateCurrTrx, cleanUpResource);
}

RetStatus CommitRollbackAndRestoreTrxState(bool isRollback)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("CommitRollback and RestoreTrxState error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->CommitRollbackAndRestoreTrxState(isRollback);
}

RetStatus BeginTrxBlock()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("BeginTrxBlock error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->BeginTransactionBlock();
}

RetStatus EndTrxBlock()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("EndTrxBlock error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->EndTransactionBlock();
}

RetStatus RollbackLastSQLCmd()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("Rollback last sql Cmd error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->RollbackLastSQLCmd();
}

RetStatus UserAbortTrxBlock()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("UserAbortTrxBlock error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->UserAbortTransactionBlock();
}

RetStatus SetSnapShot(bool useSnapshotNow)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("SetSnapShot error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->SetSnapshotCsn(useSnapshotNow);
}

void SetSnapshotCsnForFlashback(CommitSeqNo csn)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("SetSnapshotCsn for Flashback error: transaction is not initialized."));
        return;
    }
    return transaction->SetSnapshotCsnForFlashback(csn);
}

RetStatus GetSnapshotCsnForFlashback(CommitSeqNo &csn)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("GetSnapshotCsn for Flashback error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->GetSnapshotCsnForFlashback(csn);
}

void SetIsolationLevel(int isoLevel)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (transaction) {
        transaction->SetIsolationLevel(static_cast<TrxIsolationType>(isoLevel));
    }
}

int GetIsolationLevel()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    StorageReleasePanic(STORAGE_VAR_NULL(transaction), MODULE_TRANSACTION,
                        ErrMsg("Transaction is null when get isolation level!"));
    return static_cast<int>(transaction->GetIsolationLevel());
}

uint64 GetCurrentXid()
{
    if (unlikely(thrd == nullptr || thrd->GetActiveTransaction() == nullptr)) {
        return INVALID_XID.m_placeHolder;
    }
    Xid xid = thrd->GetActiveTransaction()->GetCurrentXid();
    return xid.m_placeHolder;
}

RetStatus SetCurrentXid(uint64_t xid)
{
    if (unlikely(thrd == nullptr || thrd->GetActiveTransaction() == nullptr)) {
        return DSTORE_FAIL;
    }
    thrd->GetActiveTransaction()->SetCurrentXid(static_cast<Xid>(xid));
    return DSTORE_SUCC;
}

bool IsTacXidSent()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("IsTacXidSent error: transaction is not initialized."));
        return false;
    }
    return transaction->IsTacXidSent();
}

void SetTacXidSent(bool tacXidSent)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("SetTacXidSent error: transaction is not initialized."));
        return;
    }
    return transaction->SetTacXidSent(tacXidSent);
}

void ResetSnapshotCsn()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("ResetSnapshotCsn error: transaction is not initialized."));
        return;
    }
    /* only allowed to reset READ_COMMITTED transactions */
    if (transaction->GetIsolationLevel() == TrxIsolationType::XACT_READ_COMMITTED) {
        UNUSE_PARAM RetStatus ret = transaction->SetTransactionSnapshotCsn(INVALID_CSN);
        transaction->SetThrdLocalCsn(INVALID_CSN);
    }
}

RetStatus SetTransactionSnapshotCid(CommandId cid)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("SetTransactionSnapshotCid error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->SetTransactionSnapshotCid(cid);
}

CommandId GetTransactionSnapshotCid()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("GetTransactionSnapshotCid error: transaction is not initialized."));
        return FIRST_CID;
    }
    return transaction->GetSnapshotCid();
}

RetStatus SetTransactionSnapshotCsn(CommitSeqNo csn)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("SetTransactionSnapshotCsn error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->SetTransactionSnapshotCsn(csn);
}

CommitSeqNo GetTransactionSnapshotCsn()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("GetTransactionSnapshotCsn error: transaction is not initialized."));
        return INVALID_CSN;
    }
    return transaction->GetSnapshotCsn();
}

CommitSeqNo GetLatestSnapshotCsn()
{
    /* Current latest snapshot csn */
    return thrd->GetNextCsn();
}

CommandId GetCurCid()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("GetCurCid error: transaction is not initialized."));
        return DSTORE::FIRST_CID;
    }
    return transaction->GetCurCid();
}

bool TrxIsInProgress()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    return transaction != nullptr && transaction->InTransaction();
}

bool TrxIsDefault()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    return transaction != nullptr && transaction->IsTransactionDefault();
}

bool TrxIsStart()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    return transaction != nullptr && transaction->IsTransactionStart();
}

bool TrxBlockIsDefault()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    return transaction == nullptr || transaction->IsTBlockDefault();
}

bool TrxBlockIsAborted()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    return transaction != nullptr && transaction->IsTBlockAborted();
}

DSTORE::TBlockState GetCurrentTBlockState()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("GetCurrentTBlockState error: transaction is not initialized."));
        return DSTORE::TBlockState::TBLOCK_DEFAULT; /* keep compiler quiet */
    }
    return transaction->GetCurTxnBlockState();
}

bool IsTrxBlock()
{
    /*
     * Error handling functions might need to call this function
     * when the dstore thread is not initialized; return false in such case.
     */
    if (thrd == nullptr) {
        return false;
    }
    Transaction *transaction = thrd->GetActiveTransaction();
    return transaction != nullptr && transaction->IsTxnBlock();
}

char GetStatusCode()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("GetStatusCode error: transaction is not initialized."));
        return 0; /* keep compiler quiet */
    }
    return transaction->StatusCode();
}

bool IsAutonomousTransaction()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("IsAutonomousTransaction error: transaction is not initialized."));
        return false;
    }
    return transaction->IsAutonomousTransaction();
}

int GetAutonomousTransactionLevel()
{
    int level = 1;
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("GetAutonomousTransactionLevel error: transaction is not initialized."));
        return 0;
    }

    while (transaction->m_prevTransaction != nullptr && transaction->IsAutonomousTransaction()) {
        transaction = transaction->m_prevTransaction;
        level++;
    }
    return level;
}

RetStatus CreateAutonomousTransaction(bool isSpecialAyncCommitAutoTrx)
{
    return thrd->CreateAutonomousTrx(isSpecialAyncCommitAutoTrx);
}

void DestroyAutonomousTransaction()
{
    thrd->DestroyAutonomousTrx();
}

void *GetTransactionExtraResPtr()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("GetTransactionExtraResPtr error: transaction is not initialized."));
        return nullptr;
    }
    return transaction->GetTransactionExtraResPtr();
}

void SetTransactionExtraResPtr(void *ptr)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("SetTransactionExtraResPtr error: transaction is not initialized."));
        return;
    }
    return transaction->SetTransactionExtraResPtr(ptr);
}

uint16_t GetTransactionGucLevel()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    StorageReleasePanic(STORAGE_VAR_NULL(transaction), MODULE_TRANSACTION,
                        ErrMsg("Transaction is null when get guc level!"));
    return transaction->GetGucLevel();
}

bool IsSavepointListEmpty()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("IsSavepointListEmpty error: transaction is not initialized."));
        return true;
    }
    return transaction->IsSavepointListEmpty();
}

void SetTransactionGucLevel(const uint16_t gucLevel)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("SetTransactionGucLevel error: transaction is not initialized."));
        return;
    }
    return transaction->SetGucLevel(gucLevel);
}

void DeleteAllCursorSnapshots()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("DeleteAllCursorSnapshots error: transaction is not initialized."));
        return;
    }
    return transaction->DeleteAllCursorSnapshots();
}

RetStatus AddCursorSnapshot(const char *name)
{
    if (name == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("AddCursorSnapshot wrong parameter: name cannot be nullptr"));
        return DSTORE_FAIL;
    }
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("AddCursorSnapshot error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->AddCursorSnapshot(name);
}

RetStatus DeleteCursorSnapshot(const char *name)
{
    if (name == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("DeleteCursorSnapshot wrong parameter: name cannot be nullptr"));
        return DSTORE_FAIL;
    }
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("DeleteCursorSnapshot error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->DeleteCursorSnapshot(name);
}

RetStatus CreateSavepoint(const char *name)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("CreateSavepoint error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->CreateSavepoint(name);
}

RetStatus ReleaseSavepoint(const char *name, int16 *userSavepointCounter, int16 *exceptionSavepointCounter)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("ReleaseSavepoint error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    RetStatus rc = transaction->CheckSavepointCounter(userSavepointCounter, exceptionSavepointCounter);
    if (STORAGE_FUNC_FAIL(rc)) {
        return rc;
    }
    return transaction->ReleaseSavepoint(name, userSavepointCounter, exceptionSavepointCounter);
}

DSTORE::RetStatus SaveExtraResPtrToSavepoint(const char *name, void *data)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("SaveExtraResPtrToSavepoint error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->SaveExtraResPtrToSavepoint(name, data);
}

void *GetExtraResPtrFromSavepoint(const char *name)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("GetExtraResPtrFromSavepoint error: transaction is not initialized."));
        return nullptr;
    }
    return transaction->GetExtraResPtrFromSavepoint(name);
}

void *GetExtraResPtrFromCurrentSavepoint()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("GetExtraResPtrFromCurrentSavepoint error: transaction is not initialized."));
        return nullptr;
    }
    return transaction->GetExtraResPtrFromCurrentSavepoint();
}

RetStatus RollbackToSavepoint(const char *name, int16 *userSavepointCounter, int16 *exceptionSavepointCounter)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("RollbackToSavepoint error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    RetStatus rc = transaction->CheckSavepointCounter(userSavepointCounter, exceptionSavepointCounter);
    if (STORAGE_FUNC_FAIL(rc)) {
        return rc;
    }
    return transaction->RollbackToSavepoint(name, userSavepointCounter, exceptionSavepointCounter);
}

bool HasCurrentSavepointName()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("HasCurrentSavepointName error: transaction is not initialized."));
        return false;
    }
    return transaction->HasCurrentSavepointName();
}

/* Note: do not free the pointer returned by GetCurrentSavepointName. */
char *GetCurrentSavepointName()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("GetCurrentSavepointName error: transaction is not initialized."));
        return nullptr;
    }
    return transaction->GetCurrentSavepointName();
}

int32 GetSavepointNestLevel()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("GetSavepointNestLevel error: transaction is not initialized."));
        return 0;
    }
    return transaction->GetSavepointNestLevel();
}

bool IsMvccSnapshot()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("IsMvccSnapshot error: transaction is not initialized."));
        /* default MVCC snapshot */
        return true;
    }
    return transaction->IsMvccSnapshot();
}

RetStatus AssignXid()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("AssignXid error: transaction is not initialized."));
        return DSTORE_FAIL;
    }
    return transaction->AllocTransactionSlot();
}

void IncreaseCommandCounter()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("IncreaseCommandCounter error: transaction is not initialized."));
        return;
    }
    return transaction->IncreaseCommandCounter();
}

void SetCurCidUsed()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("SetCurCidUsed error: transaction is not initialized."));
        return;
    }
    return transaction->SetCurCidUsed();
}

bool IsCurCidUsed()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("IsCurCidUsed error: transaction is not initialized."));
        return false;
    }
    return transaction->IsCurCidUsed();
}

char *Dump(uint64 xidValue)
{
    Xid xid(xidValue);
    StringInfoData xidStr;
    if (unlikely(!xidStr.init())) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Malloc xid info failed!"));
        return nullptr;
    }
    xid.Dump(&xidStr);
    return xidStr.data;
}

bool IsXidValid(uint64 xidValue)
{
    return xidValue != INVALID_XID.m_placeHolder;
}

uint64 GetInvalidXidValue()
{
    return INVALID_XID.m_placeHolder;
}

void GetXidStatusAndCsn(uint64_t xidValue, uint8_t &status, uint64_t &csn)
{
    Xid xid = Xid(xidValue);
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("GetXidStatusAndCsn error: transaction is not initialized."));
        status = (uint8)TXN_STATUS_UNKNOWN;
        csn = INVALID_CSN;
        return;
    }
    XidStatus xs(xid, transaction);
    csn = xs.GetCsn();
    status = (uint8)xs.GetStatus();
}

char GetTacTransactionState(DSTORE::PdbId pdbId, uint64 xidValue)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    StorageReleasePanic(STORAGE_VAR_NULL(transaction), MODULE_TRANSACTION,
                        ErrMsg("Transaction is null when get tac transaction state!"));
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageReleasePanic(STORAGE_VAR_NULL(pdb), MODULE_TRANSACTION,
                        ErrMsg("Pdb[%u] is null when get tac transaction state!", pdbId));
    Xid xid(xidValue);
    TransactionMgr *transactionMgr = pdb->GetTransactionMgr();
    StorageReleasePanic(STORAGE_VAR_NULL(transactionMgr), MODULE_TRANSACTION,
                        ErrMsg("TransactionMgr is null when get tac transaction state, pdbId[%u]!", pdbId));
    return transactionMgr->GetTacTransactionState(xid);
}

void CleanupTacSnapshot(bool resetXactFirstStmtCsnMin)
{
    thrd->CleanupTacSnapshot(resetXactFirstStmtCsnMin);
}

DropSegPendingList *GetDropSegPendingList()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("GetDropSegPendingList error: transaction is not initialized."));
        return nullptr;
    }
    return transaction->m_dropSegPendingList;
}

void SetDropSegPendingList(DropSegPendingList *oldpendinglist)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("SetDropSegPendingList error: transaction is not initialized."));
        return;
    }
    transaction->m_dropSegPendingList = oldpendinglist;
}

const char *GetBlockStateStr(DSTORE::TBlockState state)
{
    return Transaction::BlockStateAsString(state);
}

bool IsAbortStageDstoreCompleted()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("IsAbortStageDstoreCompleted error: transaction is not initialized."));
        return true;
    }
    return transaction->GetCurAbortStage() == TransAbortStage::DstoreAbortCompleted;
}

void SetAbortStageCompleted()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("SetAbortStageCompleted error: transaction is not initialized."));
        return;
    }
    transaction->SetCurAbortStage(TransAbortStage::SqlAbortCompleted);
}

/* Hack interfaces to adapt stored procedure start */
/* A serious warning: these hack interfaces just used for gaussdb stored procedure.
 * They are not regular interfaces, you must know what you are doing when using them.
 */
/* In Gaussdb sql layer, if commit or rollback in stored procedure,
 * it will save state before stp commit&rollback, and restore state after stp commit&rollback,
 * so we add SaveCurTransactionState() and RestoreCurTransactionState() to hack this action.
 */
void SaveCurTransactionState()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("SaveCurTransactionState error: transaction is not initialized."));
        return;
    }
    return transaction->SaveCurTransactionState();
}

void RestoreCurTransactionState()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("RestoreCurTransactionState error: transaction is not initialized."));
        return;
    }
    return transaction->RestoreCurTransactionState();
}

/* In Gaussdb sql layer, in some case in stored procedure,
 * txn state will be change to default, add a hack interface to do this thing.
 */
void SetCurTxnBlockState(DSTORE::TBlockState tBlockState)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("SetCurTxnBlockState error: transaction is not initialized."));
        return;
    }
    return transaction->SetCurTxnBlockState(tBlockState);
}
/* Hack interfaces to adapt stored procedure end */

bool IsTransactionRuntimeInitialized()
{
    return thrd->GetActiveTransaction() != nullptr;
}

}  // namespace TransactionInterface
