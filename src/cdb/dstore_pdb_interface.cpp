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
 * dstore_pdb_interface.cpp
 *
 * IDENTIFICATION
 *        src/cdb/dstore_pdb_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "lock/dstore_lock_dummy.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_mgr.h"
#include "pdb/dstore_pdb_interface.h"
#include "framework/dstore_instance.h"
#include "wal/dstore_wal.h"
#include "wal/dstore_wal_write_context.h"

namespace StoragePdbInterface {
using namespace DSTORE;

const uint32 WAIT_ROLLBACK_COMPLETE_SLEEP_TIME = 1000000;

DSTORE::RetStatus SwitchContextToTargetPdb(PdbId pdbId)
{
    thrd->DestroyTransactionRuntime();
    thrd->SetXactPdbId(pdbId);
    ErrLog(DSTORE_LOG, MODULE_COMMON,
           ErrMsg("SwitchContextToTargetPdb SetPdbTerm instance pdbTerm: %lu, thrd pdb term: %lu, pdb: %u.",
           g_storageInstance->GetPdbTerm(pdbId), thrd->GetPdbTerm(), pdbId));
    thrd->SetPdbTerm(g_storageInstance->GetPdbTerm(pdbId));
    if (unlikely(thrd->m_walWriterContext != nullptr)) {
        delete thrd->m_walWriterContext;
        thrd->m_walWriterContext = nullptr;
    }
    if (unlikely(g_storageInstance->GetPdb(pdbId) != nullptr &&
                 (g_storageInstance->GetPdb(pdbId)->GetWalMgr() == nullptr ||
                  g_storageInstance->GetPdb(pdbId)->GetWalMgr()->isUninit()))) {
        ErrLog(DSTORE_PANIC, MODULE_COMMON, ErrMsg("WalManager in pdb %u is not initialized.", pdbId));
    }
    if (STORAGE_FUNC_FAIL(thrd->InitStorageContext(pdbId))) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to initialize sotrage context."));
        return DSTORE_FAIL;
    }
    /* Init tranaction may write undo recycle wal, we need init tranaction run time after set wal context pdbId. */
    if (STORAGE_FUNC_FAIL(thrd->InitTransactionRuntime(pdbId, nullptr, nullptr, true))) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void GetPdbPath(DSTORE::PdbId pdbId, char *pdbPath)
{
    char *path = StorageInstance::GetPdbPath(g_storageInstance->GetGuc()->dataDir, pdbId);
    if (STORAGE_VAR_NULL(path)) {
        return;
    }
    errno_t rc = snprintf_s(pdbPath, MAXPGPATH, MAXPGPATH - 1, "%s", path);
    storage_securec_check_ss(rc);
    DstorePfreeExt(path);
}

RetStatus Checkpoint(PdbId pdbId)
{
    StorageReleasePanic(pdbId == INVALID_PDB_ID, MODULE_FRAMEWORK, ErrMsg("pdbId is INVALID_PDB_ID."));
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("pdb is null."));
    if (!pdb->IsInit()) {
        return DSTORE_SUCC;
    }
    return pdb->FullCheckpoint();
}

void FlushAllDirtyPages(PdbId pdbId)
{
     /* if pdbId is specificed, checkpoint  */
    if (g_storageInstance != nullptr && g_storageInstance->GetBufferMgr() != nullptr) {
        /* if pdbId is not specific, checkpoint all database */
        (void)g_storageInstance->GetBufferMgr()->FlushAll(false, false, pdbId);
    }
}

DSTORE::RetStatus GetBuiltinRelMap(DSTORE::RelMapType type, DSTORE::PdbId pdbid, DSTORE::RelMapNode *nodes, int *count)
{
    DSTORE::StoragePdb *pdb = g_storageInstance->GetPdb(pdbid);
    StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("pdb is null."));
    if (pdb != nullptr) {
        return pdb->GetBuiltinRelMap(type, nodes, count);
    }
    return DSTORE_FAIL;
}

DSTORE::RetStatus WriteBuiltinRelMap(DSTORE::RelMapType type, DSTORE::PdbId pdbid, DSTORE::RelMapNode *nodes, int count)
{
    DSTORE::StoragePdb *pdb = g_storageInstance->GetPdb(pdbid);
    StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("pdb is null."));
    if (pdb != nullptr) {
        return pdb->WriteBuiltinRelMap(type, nodes, count);
    }
    return DSTORE_FAIL;
}

bool waitAllRollbackTaskFinished(DSTORE::PdbId pdbid)
{
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
           ErrMsg("[waitAllRollbackTaskFinished]Start wait for all rollback tasks to end, pdbId %u.", pdbid));
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbid);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[waitAllRollbackTaskFinished]Failed to get pdb: %u.", pdbid));
        return false;
    }

    TransactionMgr *trans_mgr = pdb->GetTransactionMgr();
    if (STORAGE_VAR_NULL(trans_mgr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[waitAllRollbackTaskFinished]Failed to get transactionMgr of pdb: %u.", pdbid));
        return false;
    }

    /* Rollback all inprogress transaction. */
    pdb->AsyncRecoverUndo();
    while (!trans_mgr->IsAllTaskFinished()) {
        CHECK_FOR_INTERRUPTS();
        GaussUsleep(WAIT_ROLLBACK_COMPLETE_SLEEP_TIME);
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
           ErrMsg("[waitAllRollbackTaskFinished]Wait for all rollback tasks to end success, pdbId %u.", pdbid));

    return true;
}
}  // namespace StoragePdbInterface
