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
 * dstore_transaction_list.h
 *
 * IDENTIFICATION
 *        include/transaction/dstore_transaction_list.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TRANSACTION_LIST_H
#define DSTORE_TRANSACTION_LIST_H

#include "common/memory/dstore_mctx.h"
#include "lock/dstore_lock_struct.h"

namespace DSTORE {
union Xid;
enum LockMgrType : uint8;

class TransactionList : public BaseObject {
public:
    TransactionList() : m_activeTransaction(nullptr), m_pdbId(INVALID_PDB_ID) {}
    
    RetStatus InitRuntime(bool allocateZoneId, PdbId pdbId);
    void Destroy();
    void SetPdbId(PdbId pdbId);
    PdbId GetPdbId();
    class Transaction *GetActiveTransaction();
    class Transaction *GetTopTransaction();
    RetStatus CreateAutonomousTransaction(bool isSpecialAyncCommitAutoTrx);
    void DestroyAutonomousTransaction();
    bool ContainsTransaction(Xid xid) const;

    bool NonActiveTransactionHoldConflict2PLock(const struct LockTag &tag, LockMode mode, LockMgrType mgrType) const;
    RetStatus ActuallyAcquireLazyLocksOnCurrentThread();
    bool AllTransactionsHold2PLockMoreThan(const struct LockTag &tag, LockMode mode, LockMgrType mgrType,
        uint32 cnt) const;
    void ReleaseLocksInAllTransactionRes();

    Transaction     *m_activeTransaction;
private:
    PdbId m_pdbId;
    RetStatus CreateTransaction(bool allocateZoneId, bool isAutonomous, bool isSpecialAyncCommitAutoTrx = false);
};

}  // namespace DSTORE

#endif
