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
 * dstore_transaction_interface.h
 *
 * IDENTIFICATION
 *        interface/transaction/dstore_transaction_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef DSTORE_TRANSACTION_INTERFACE_H
#define DSTORE_TRANSACTION_INTERFACE_H

#include "common/dstore_common_utils.h"
#include "table/dstore_table_interface.h"
#include "transaction/dstore_transaction_struct.h"

namespace DSTORE {
union Xid;
struct SnapshotData;
}  // namespace DSTORE

namespace TransactionInterface {
#pragma GCC visibility push(default)

DSTORE::RetStatus StartTrxCommand();
DSTORE::RetStatus CommitTrxCommand();
/* Dangerous interface, just hack for v5 sql engine adapt. */
/* Caller must use CleanUpResourceAfterCommit() to clean up resource of transaction. */
DSTORE::RetStatus CommitTrxCommandWithoutCleanUpResource();
/* Dangerous interface, just hack for v5 sql engine adapt */
/* Only need clean up resource when block state is started or end or Abort. */
void CleanUpResourceAfterCommit();
void CleanUpResourceAfterAbort();

/* Dangerous interface, just hack for v5 sql engine adapt */
/* v5 sql will add transaction locks before transaction start, so we may not release them in abnormal case. Only call
   this interface before thread destroy */
void ForceReleaseAllTransactionLocks();

DSTORE::RetStatus AbortTrx(bool terminateCurrTrx = false, bool cleanUpResource = true);
DSTORE::RetStatus CommitRollbackAndRestoreTrxState(bool isRollback);
DSTORE::RetStatus BeginTrxBlock();
DSTORE::RetStatus EndTrxBlock();
DSTORE::RetStatus UserAbortTrxBlock();
DSTORE::RetStatus SetSnapShot(bool useSnapshotNow = false);
void SetSnapshotCsnForFlashback(DSTORE::CommitSeqNo csn);
DSTORE::RetStatus GetSnapshotCsnForFlashback(DSTORE::CommitSeqNo &csn);
void SetIsolationLevel(int isoLevel);
int GetIsolationLevel();
uint64_t GetCurrentXid();
/* Use with caution, only for stream process */
DSTORE::RetStatus SetCurrentXid(uint64_t xid);
DSTORE::CommandId GetCurCid();
bool TrxIsInProgress();
bool TrxIsDefault();
bool TrxIsStart();
bool TrxBlockIsDefault();
bool TrxBlockIsAborted();
bool IsTrxBlock();
char GetStatusCode();
DSTORE::TBlockState GetCurrentTBlockState();
bool IsAutonomousTransaction();
int GetAutonomousTransactionLevel();
DSTORE::RetStatus CreateAutonomousTransaction(bool isSpecialAyncCommitAutoTrx = false);
void DestroyAutonomousTransaction();
void* GetTransactionExtraResPtr();
void SetTransactionExtraResPtr(void* ptr);
void ResetSnapshotCsn();
DSTORE::RetStatus SetTransactionSnapshotCid(DSTORE::CommandId cid);
DSTORE::CommandId GetTransactionSnapshotCid();
DSTORE::RetStatus SetTransactionSnapshotCsn(DSTORE::CommitSeqNo csn);
DSTORE::CommitSeqNo GetTransactionSnapshotCsn();
bool IsAbortStageDstoreCompleted();
void SetAbortStageCompleted();
/*
 * These interface is for integration with openGauss
 */
DSTORE::CommitSeqNo GetLatestSnapshotCsn();
uint16_t GetTransactionGucLevel();
void SetTransactionGucLevel(const uint16_t gucLevel);
bool IsSavepointListEmpty();

/* Cursor Functions */
void DeleteAllCursorSnapshots();
DSTORE::RetStatus AddCursorSnapshot(const char* name);
DSTORE::RetStatus DeleteCursorSnapshot(const char* name);

/* Savepoint Functions */
DSTORE::RetStatus CreateSavepoint(const char *name);
DSTORE::RetStatus ReleaseSavepoint(const char *name, int16_t *userSavepointCounter = nullptr,
    int16_t *exceptionSavepointCounter = nullptr);
DSTORE::RetStatus RollbackToSavepoint(const char *name, int16_t *userSavepointCounter = nullptr,
    int16_t *exceptionSavepointCounter = nullptr);
DSTORE::RetStatus SaveExtraResPtrToSavepoint(const char *name, void* data);
void* GetExtraResPtrFromSavepoint(const char* name);
void* GetExtraResPtrFromCurrentSavepoint();
DSTORE::RetStatus RollbackLastSQLCmd();
int32_t GetSavepointNestLevel();
bool HasCurrentSavepointName();
char *GetCurrentSavepointName();

bool IsMvccSnapshot();
DSTORE::RetStatus AssignXid();
void IncreaseCommandCounter();
void SetCurCidUsed();
bool IsCurCidUsed();
char *Dump(uint64_t xidValue);
bool IsXidValid(uint64_t xidValue);
uint64_t GetInvalidXidValue();
void GetXidStatusAndCsn(uint64_t xidValue, uint8_t &status, uint64_t &csn);

/* To support Transparent Application Continuity (TAC) */
char GetTacTransactionState(DSTORE::PdbId pdbId, uint64_t xid);
bool IsTacXidSent();
void SetTacXidSent(bool tacXidSent);
void CleanupTacSnapshot(bool resetXactFirstStmtCsnMin = false);

DSTORE::DropSegPendingList *GetDropSegPendingList();
void SetDropSegPendingList(DSTORE::DropSegPendingList  *oldpendinglist);

const char *GetBlockStateStr(DSTORE::TBlockState state);

/* Hack interfaces to adapt stored procedure start */
/* A serious warning: these hack interfaces just used for gaussdb stored procedure.
 * They are not regular interfaces, you must know what you are doing when using them.
 */
/* In Gaussdb sql layer, if commit or rollback in stored procedure,
 * it will save state before stp commit&rollback, and restore state after stp commit&rollback,
 * so we add SaveCurTransactionState() and RestoreCurTransactionState() to hack this action.
 */
void SaveCurTransactionState();
void RestoreCurTransactionState();
/* In Gaussdb sql layer, in some case in stored procedure,
 * txn state will be change to default, add a hack interface to do this thing.
 */
void SetCurTxnBlockState(DSTORE::TBlockState tBlockState);
/* Hack interfaces to adapt stored procedure end */

bool IsTransactionRuntimeInitialized();

#pragma GCC visibility pop
}  // namespace TransactionInterface

#endif
