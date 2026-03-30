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
 * dstore_data_page.cpp
 *
 * IDENTIFICATION
 *        src/page/dstore_data_page.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/log/dstore_log.h"
#include "common/dstore_datatype.h"
#include "errorcode/dstore_page_error_code.h"
#include "errorcode/dstore_undo_error_code.h"
#include "securec.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_index_page.h"
#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_mgr.h"
#include "port/dstore_port.h"
#include "tuple/dstore_data_tuple.h"
#include "undo/dstore_transaction_slot.h"
#include "page/dstore_data_page.h"
#include "wal/dstore_wal_write_context.h"

namespace DSTORE {

template TdId DataPage::AllocTd<PageType::HEAP_PAGE_TYPE>(TDAllocContext &context);
template TdId DataPage::AllocTd<PageType::INDEX_PAGE_TYPE>(TDAllocContext &context);

OffsetNumber DataPage::GetFreeItemId()
{
    OffsetNumber offset = INVALID_ITEM_OFFSET_NUMBER;
    OffsetNumber limit = OffsetNumberNext(GetMaxOffset());
    if (!HasFreeItemId()) {
        return limit;
    }
    for (offset = FIRST_ITEM_OFFSET_NUMBER; offset < limit; ++offset) {
        ItemId *itemId = GetItemIdPtr(offset);
        if (itemId->IsUnused()) {
            return offset;
        }
    }
    return offset;
}

/*
 * GetFreeSpace
 *		Returns the size of the free (allocatable) space on a page,
 *		reduced by the space needed for a new line pointer.
 */
template<FreeSpaceCondition cond>
uint32 DataPage::GetFreeSpace()
{
    uint32 freeSpace = 0;
    freeSpace = static_cast<uint32>(m_header.m_upper - m_header.m_lower);
    if (cond == FreeSpaceCondition::RAW) {
        return freeSpace;
    }

    StorageAssert(cond == FreeSpaceCondition::EXCLUDE_ITEMID);
    uint32 exclude = sizeof(ItemId);
    if (freeSpace < exclude) {
        return 0;
    }
    return freeSpace - exclude;
}

template uint32 DataPage::GetFreeSpace<FreeSpaceCondition::RAW> ();
template uint32 DataPage::GetFreeSpace<FreeSpaceCondition::EXCLUDE_ITEMID> ();

/*
 * Return page free space used for insert, considering item id allocation.
 */
uint32 DataPage::GetFreeSpaceForInsert()
{
    return GetFreeSpace<FreeSpaceCondition::EXCLUDE_ITEMID>();
}

RetStatus DataPage::ExtendTd(TDAllocContext &context)
{
    uint32 freeSpace = GetFreeSpace<FreeSpaceCondition::RAW>();
    uint8 tdNum = static_cast<uint8>(dataHeader.tdCount);
    /*
     * ExtendTd should be called when page can extend TD array by at least 1 TD AND
     * TD array has not reached its maximum limit of MAX_TD_COUNT.
     */
    uint8 numExtended = DstoreMin(EXTEND_TD_NUM, static_cast<uint8>(MAX_TD_COUNT - tdNum));
    if (tdNum >= MAX_TD_COUNT) {
        storage_set_error(PAGE_ERROR_TD_COUNT_LIMIT);
        return DSTORE_FAIL;
    }
    /*
     * Check the amount of available space for extension of
     * TD array. In case of insufficient space, extend
     * according to free space.
     */
    if (freeSpace < numExtended * sizeof(TD)) {
        if (freeSpace < EXTEND_TD_MIN_NUM * sizeof(TD)) {
            storage_set_error(PAGE_ERROR_NO_SPACE_FOR_TD);
            return DSTORE_FAIL;
        }
        numExtended = EXTEND_TD_MIN_NUM;
    }
    /*
     * Move the line pointers ahead in the page to make room for
     * added transaction slots.
     */
    char *start = GetItemIdArrayStartPtr();
    char *end = GetItemIdArrayEndPtr();
    uint32 size = static_cast<uint32>(end - start);
    if (size != 0) {
        errno_t ret = memmove_s(start + (numExtended * sizeof(TD)), size, start, size);
        storage_securec_check(ret, "", "");
    }

    /* Initialize the new TD slots */
    for (uint8 i = tdNum; i < tdNum + numExtended; i++) {
        TD *td = GetTd(i);
        td->Reset();
    }

    /* Reinitialize number of TD slots and begining of free space in the header */
    dataHeader.tdCount = tdNum + numExtended;
    m_header.m_lower += numExtended * sizeof(TD);

    StorageAssert(CheckSanity());
    /* isDirty must be false when call Begin before allocTd */
    context.allocTd.isDirty = true;
    context.allocTd.extendNum = numExtended;

    return DSTORE_SUCC;
}

void DataPage::RecycleTd(uint8 numRecycled)
{
    uint8 tdNum = dataHeader.tdCount;
    StorageAssert(tdNum >= numRecycled);
    if (numRecycled == 0) {
        return;
    }

    /* Move the line pointers to the recycled td space. */
    char *start = GetItemIdArrayStartPtr();
    char *end = GetItemIdArrayEndPtr();
    uint32 size = static_cast<uint32>(end - start);
    StorageAssert(size < BLCKSZ);
    errno_t rc = memmove_s(start - (numRecycled * sizeof(TD)), size, start, size);
    storage_securec_check(rc, "\0", "\0");

    /* Reinitialize number of TD slots and begining of free space in the header. */
    dataHeader.tdCount = static_cast<uint8>(tdNum - numRecycled);
    m_header.m_lower -= numRecycled * sizeof(TD);

    uint16 offset = 0;
    uint16 maxOffset = GetMaxOffset();
    for (offset = FIRST_ITEM_OFFSET_NUMBER; offset <= maxOffset; ++offset) {
        ItemId *itemId = GetItemIdPtr(offset);
        if (itemId->IsUnused()) {
            continue;
        }

        if (GetType() == PageType::HEAP_PAGE_TYPE) {
            TdId tdId = GetTupleTdId<HeapDiskTuple>(offset);
            if (tdId >= dataHeader.tdCount && !TestTupleTdStatus<HeapDiskTuple>(offset, DETACH_TD)) {
                SetTupleTdStatus<HeapDiskTuple>(offset, DETACH_TD);
            }
        }
        if (GetType() == PageType::INDEX_PAGE_TYPE) {
            TdId tdId = GetTupleTdId<IndexTuple>(offset);
            if (tdId >= dataHeader.tdCount && !TestTupleTdStatus<IndexTuple>(offset, DETACH_TD)) {
                SetTupleTdStatus<IndexTuple>(offset, DETACH_TD);
            }
        }
    }

    StorageAssert(CheckSanity());
}

template<PageType pageType>
TdId DataPage::DoAllocTd(TDAllocContext &context)
{
    Xid curXid = context.txn->GetCurrentXid();
    StorageAssert(curXid.IsValid());
    TdId tdId = INVALID_TD_SLOT;
    uint8 tdNum = GetTdCount();
    TdId firstUseableTdSlot = INVALID_TD_SLOT;

    /* Step 1: Get TD if the current transaction has been assigned */
    for (tdId = 0; tdId < tdNum; ++tdId) {
        TD *td = GetTd(tdId);
        StorageAssert(td->CheckSanity());
        if (td->TestStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE)) {
            /* best effort to reuse frozen td slot */
            if (firstUseableTdSlot == INVALID_TD_SLOT) {
                firstUseableTdSlot = tdId;
            }
        } else if (td->GetXid() == curXid || td->GetLockerXid() == curXid) {
            /* TD already exists */
            return tdId;
        }
    }

    if (firstUseableTdSlot != INVALID_TD_SLOT) {
        return firstUseableTdSlot;
    }

    /* Step 2: try to reuse the existing td slot */
    tdId = TryReuseTdSlots<pageType>(context);
    if (tdId != INVALID_TD_SLOT) {
        return tdId;
    }

    /* Step 4: Unable to find an available TD slot, try to extend TD */
    if (STORAGE_FUNC_SUCC(ExtendTd(context))) {
        return tdNum;
    }

    ErrLog(DSTORE_WARNING, MODULE_HEAP,
           ErrMsg("Allocate td failed in page(%hu, %u)", GetSelfPageId().m_fileId, GetSelfPageId().m_blockId));

    return INVALID_TD_SLOT;
}

template <PageType pageType>
TdId DataPage::AllocTd(TDAllocContext &context)
{
    static constexpr int allocTdFailedThreshold = 100;
    TdId tdId = INVALID_TD_SLOT;
    for (int i = 0; i < allocTdFailedThreshold; ++i) {
        tdId = DataPage::DoAllocTd<pageType>(context);
        if (likely(tdId != INVALID_TD_SLOT)) {
            break;
        }
        StorageAssert(!context.allocTd.isDirty);
        context.tryLocalXidOnly = false;
        GaussUsleep(10L);
    }

    if (likely(tdId != INVALID_TD_SLOT)) {
        context.DisableRetryAllocTd();
    } else {
        context.EnableRetryAllocTd();
    }

    StorageAssert(tdId == INVALID_TD_SLOT || tdId < GetTdCount());

    return tdId;
}

template<PageType pageType>
DataPage::TdReuseState DataPage::TryReuseOneTdSlot(TDAllocContext &context, TdId tdId)
{
    TdReuseState state = TdReuseState::TD_IGNORE;
    TD *td = GetTd(tdId);
    XidStatus xs(td->GetXid(), context.txn, td);
    context.allocTd.xidStatus[tdId] = xs.GetStatus();
    switch (xs.GetStatus()) {
        /* if the td may be reused, the status of csn in td must be IS_CUR_XID_CSN */
        case TXN_STATUS_FROZEN:
        case TXN_STATUS_COMMITTED: {
            (void)td->FillCsn(context.txn, &xs);
            context.allocTd.xidStatus[tdId] = TXN_STATUS_FAILED;
            Xid tdLockerXid = td->GetLockerXid();
            if (tdLockerXid != INVALID_XID) {
                XidStatus lockerStatus(tdLockerXid, context.txn);
                if (tdLockerXid != td->GetXid() && lockerStatus.IsInProgress()) {
                    context.AddTdReuseWaitXid(tdLockerXid);
                    break;
                }
                td->SetLockerXid(INVALID_XID);
                td->SetStatus(TDStatus::OCCUPY_TRX_END);
            }

            StorageAssert(td->TestCsnStatus(IS_CUR_XID_CSN));
            if (xs.IsFrozen() || (td->GetCsn() < context.recycleMinCsn)) {
                context.allocTd.xidStatus[tdId] = TXN_STATUS_FROZEN;
                state = TdReuseState::TD_RECYCLE_UNUSED;
            } else {
                context.allocTd.xidStatus[tdId] = TXN_STATUS_COMMITTED;
                state = TdReuseState::TD_RECYCLE_REUSE;
            }
            break;
        }

        case TXN_STATUS_ABORTED: {
            /*
             * If the transaction that triggers index page splitting is aborted, the remaining td on another page needs
             * to be rolled back.
             */
            StorageAssert(pageType == PageType::INDEX_PAGE_TYPE);
            td->RollbackTdToPreTxn(context.m_pdbId);
            state = TdReuseState::TD_CONTENT_UPDATED;
            break;
        }

        case TXN_STATUS_PENDING_COMMIT: {
            context.AddTdReuseWaitXid(td->GetXid());
            break;
        }

        case TXN_STATUS_IN_PROGRESS: {
            if (unlikely(!td->TestStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS))) {
                ErrLog(DSTORE_ERROR, MODULE_PAGE,
                    ErrMsg("Transaction XidStatus{xid(%d, %lu), csn(%lu)} is inprogress while td(%hhu) status is %d. "
                           "TD{csn(%lu), csnStatus(%hhu), cid(%u), undoRecPtr(%hu, %u, %hu), lockerXid(%d, %lu)}.",
                    static_cast<int>(xs.GetXid().m_zoneId), xs.GetXid().m_logicSlotId, xs.GetCsn(), tdId,
                    static_cast<int>(td->GetStatus()), td->GetCsn(), td->GetCsnStatus(), td->GetCommandId(),
                    td->GetUndoRecPtr().GetFileId(), td->GetUndoRecPtr().GetBlockNum(), td->GetUndoRecPtr().GetOffset(),
                    static_cast<int>(td->GetLockerXid().m_zoneId), td->GetLockerXid().m_logicSlotId));
            }
            state = TdReuseState::TD_IS_IN_PROGRESS;
            context.AddTdReuseWaitXid(td->GetXid());
            break;
        }
        case TXN_STATUS_UNKNOWN:
        case TXN_STATUS_FAILED:
        default: {
            StorageAssert(0);
        }
    }
    return state;
}

template <PageType pageType>
TdId DataPage::TryReuseTdSlots(TDAllocContext &context)
{
    uint8 tdNum = dataHeader.tdCount;
    TdRecycleStatus tdRecycle[MAX_TD_COUNT];
    uint8 tdRecycleNum = 0;
    context.allocTd.tdNum = tdNum;

    /*
     * If there is a transaction in progress on the page, we can not call td->Reset(), because we don't know if we can
     * rollback the tuple to DETACH_TD state if the transaction aborts.
     */
    bool canResetTd = true;

    /* Step 1: identify freeze or commit transaction if the transaction is visible to any active snapshot */
    TdId tdId = 0;
    while (tdId < tdNum) {
        TD *td = GetTd(tdId);
        __builtin_prefetch(static_cast<TD *>(td + sizeof(TD)), 0, 1);
        StorageAssert(td->CheckSanity());
        context.allocTd.xidStatus[tdId] = TXN_STATUS_UNKNOWN;
        if (td->GetXid() != INVALID_XID) {
            ZoneId zid = static_cast<ZoneId>(td->GetXid().m_zoneId);
            if (context.tryLocalXidOnly &&
                !g_storageInstance->GetPdb(context.m_pdbId)->GetUndoMgr()->IsZoneOwned(zid)) {
                canResetTd = false;
                ++tdId;
                continue;
            }
            TdReuseState state = TryReuseOneTdSlot<pageType>(context, tdId);
            if (state == TdReuseState::TD_IS_IN_PROGRESS) {
                canResetTd = false;
            } else if (state == TdReuseState::TD_CONTENT_UPDATED) {
                if (context.allocTd.hasRollbackTd == false) {
                    /* Init context.allocTd.RollbackTds. */
                    errno_t rc = memset_s(context.allocTd.rollbackTds, MAX_TD_COUNT * sizeof(bool),
                        0, MAX_TD_COUNT * sizeof(bool));
                    storage_securec_check(rc, "\0", "\0");
                }
                context.allocTd.hasRollbackTd = true;
                context.allocTd.rollbackTds[tdId] = true;
                continue;
            } else if (state != TdReuseState::TD_IGNORE) {
                tdRecycle[tdRecycleNum].unused = (state == TdReuseState::TD_RECYCLE_UNUSED);
                tdRecycle[tdRecycleNum].id = tdId;
                tdRecycleNum++;
            }
        } else {
            StorageAssert(td->GetUndoRecPtr() == INVALID_UNDO_RECORD_PTR);
            Xid tdLockerXid = td->GetLockerXid();
            if (tdLockerXid != INVALID_XID) {
                ZoneId zid = static_cast<ZoneId>(tdLockerXid.m_zoneId);
                /*
                 * tryLocalXidOnly == true means we only want to reuse td which xid belongs to this node, thus no
                 * need to access remote transaction info .
                 */
                if (context.tryLocalXidOnly &&
                    !g_storageInstance->GetPdb(context.m_pdbId)->GetUndoMgr()->IsZoneOwned(zid)) {
                    canResetTd = false;
                    ++tdId;
                    continue;
                }
                XidStatus lockerStatus(tdLockerXid, context.txn);
                if (lockerStatus.IsInProgress()) {
                    ++tdId;
                    context.AddTdReuseWaitXid(tdLockerXid);
                    continue;
                }
                td->SetLockerXid(INVALID_XID);
                td->SetStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE);
            }
            context.allocTd.xidStatus[tdId] = TXN_STATUS_FROZEN;
            tdRecycle[tdRecycleNum].unused = true;
            tdRecycle[tdRecycleNum].id = tdId;
            tdRecycleNum++;
        }
        ++tdId;
    }

    /*
     * Step 2: Try to reuse TD slots of committed transactions.
     * This is just like above but it will maintain a link to the previous
     * transaction undo record in this slot.  This is to ensure that if there
     * is still any alive snapshot to which this transaction is not visible,
     * it can fetch the record from undo and check the visibility.
     * The old td->xid should be reserved for rollback, so we need
     * td->IsFrozen to mark it frozen other than INVALID_XID.
     */
    if (tdRecycleNum > 0) {
        context.allocTd.isDirty = true;
        if (pageType == PageType::HEAP_PAGE_TYPE) {
            RefreshTupleTdStatus<HeapDiskTuple>(tdRecycleNum, tdRecycle);
        } else if (pageType == PageType::INDEX_PAGE_TYPE) {
            RefreshTupleTdStatus<IndexTuple>(tdRecycleNum, tdRecycle);
        }
        return GetAvailableTd(tdRecycleNum, tdRecycle, canResetTd);
    }
    return INVALID_TD_SLOT;
}

/*
 * freeze_tuple - Clear the slot information or set invalid xact flags.
 *
 * Process all the tuples on the page and match their transaction slot with
 * the input slot array, if tuple is pointing to the slot then set the tuple
 * slot as HEAPTUP_SLOT_FROZEN if is frozen is true otherwise set
 * HEAP_TUP_INVALID_TD_SLOT flag on the tuple.
 */
template<typename tup_type>
void DataPage::RefreshTupleTdStatus(int numSlot, const TdRecycleStatus *slots)
{
    uint16 maxOffset = GetMaxOffset();
    for (OffsetNumber offset = FIRST_ITEM_OFFSET_NUMBER; offset <= maxOffset; ++offset) {
        /* It's OK to start with offset = 1 for index page. (The line pointer with offset = 1 represents High Key.
         * And the first offset for data is 2 if the page is not the rightmost page.)
         * Because High Key is set dead when created thus will be skipped when checking itemId flags. */
        ItemId *itemId = GetItemIdPtr(offset);
        if (itemId->IsUnused()) {
            continue;
        }

        TdId lockerTdId = GetTupleLockerTdId<tup_type>(itemId);
        uint32 tdStatus = 0;
        if (unlikely(itemId->IsNoStorage())) {
            tdStatus = itemId->redirect.m_tdStatus;
        } else {
            tup_type *diskTup = static_cast<tup_type *>(GetRowData(itemId));
            tdStatus = diskTup->m_info.val.m_tdStatus;
        }
        bool isTdStatusDetached = TestTupleTdStatus<tup_type>(static_cast<TupleTdStatus>(tdStatus), DETACH_TD);
        bool isLockTdIdInvalid = lockerTdId == INVALID_TD_SLOT;
        if (isTdStatusDetached && isLockTdIdInvalid) {
            continue;
        }

        TdId tdId = GetTupleTdId<tup_type>(itemId);
        for (int i = 0; i < numSlot; i++) {
            if (isTdStatusDetached && isLockTdIdInvalid) {
                break;
            }
            /* Step 0: Proc lockerTdId */
            TdRecycleStatus slot = slots[i];
            if (slot.id == lockerTdId) {
                StorageAssert(itemId->IsNormal());
                static_cast<HeapDiskTuple *>(GetRowData(offset))->SetLockerTdId(INVALID_TD_SLOT);
                isLockTdIdInvalid = true;
            }

            /* Step 1: Proc tdId */
            if (TestTupleTdStatus<tup_type>(static_cast<TupleTdStatus>(tdStatus), DETACH_TD) || slot.id != tdId)  {
                continue;
            }
            /* Set td slots of tuple as frozen to indicate tuple is all visible and mark the deleted itemids as dead. */
            if (slot.unused) {
                SetTupleTdStatus<tup_type>(offset, DETACH_TD);
            } else if (!TestTupleTdStatus<tup_type>(static_cast<TupleTdStatus>(tdStatus), DETACH_TD)) {
                SetTupleTdStatus<tup_type>(offset, ATTACH_TD_AS_HISTORY_OWNER);
            }
            isTdStatusDetached = true;
        }
    }
}

TdId DataPage::GetAvailableTd(int numSlot, const TdRecycleStatus *slots, bool canResetTd)
{
    /*
     * We can reuse TD if:
     * 1. TD is unused (UNOCCUPY_AND_PRUNEABLE TD status) and TD#GetLockerXid() is invalid.
     * 2. TD#Xid transaction has ended (OCCUPY_TRX_END TD status) and TD#GetLockerXid() is invalid.
     *
     * TryReuseTdSlots has set TD#m_lockerXid with INVALID_XID if TD#GetLockerXid() transaction has ended.
     */
    TdId firstReusableTdSlot = INVALID_TD_SLOT;
    TdId firstCommitTdSlot = INVALID_TD_SLOT;
    CommitSeqNo csn = MAX_COMMITSEQNO;
    /* refresh td status */
    for (int i = 0; i < numSlot; ++i) {
        TdRecycleStatus slot = slots[i];
        TD *td = GetTd(slot.id);
        __builtin_prefetch(static_cast<TD *>(td + sizeof(TD)), 0, 1);
        if (canResetTd && slot.unused) {
            td->Reset();
            if (firstReusableTdSlot == INVALID_TD_SLOT) {
                firstReusableTdSlot = slot.id;
            }
        } else if (!td->TestStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE)) {
            td->SetStatus(TDStatus::OCCUPY_TRX_END);
            /* choose the smaller csn td, or at least choose one td */
            if ((td->TestCsnStatus(IS_CUR_XID_CSN) && td->GetCsn() <= csn) ||
                (firstCommitTdSlot == INVALID_TD_SLOT)) {
                firstCommitTdSlot = slot.id;
                csn = td->GetCsn();
            }
        } else if (firstReusableTdSlot == INVALID_TD_SLOT) {
            firstReusableTdSlot = slot.id;
        }
    }

    if (firstReusableTdSlot != INVALID_TD_SLOT) {
        return firstReusableTdSlot;
    }
    if (firstCommitTdSlot != INVALID_TD_SLOT) {
        return firstCommitTdSlot;
    }

    StorageReleasePanic((firstReusableTdSlot == INVALID_TD_SLOT && firstCommitTdSlot == INVALID_TD_SLOT), MODULE_HEAP,
        ErrMsg("Recycle num is not zero, but TdId invalid."));
    return INVALID_TD_SLOT;
}

template TdId DataPage::GetTupleTdId<HeapDiskTuple> (ItemId*);
template TdId DataPage::GetTupleTdId<IndexTuple> (ItemId*);

template<typename tup_type>
TdId DataPage::GetTupleTdId(ItemId* itemId)
{
    if (unlikely(itemId->IsUnused())) {
        return INVALID_TD_SLOT;
    }
    if (unlikely(itemId->IsNoStorage())) {
        return itemId->GetTdId();
    }
    tup_type *diskTup = static_cast<tup_type *>(GetRowData(itemId));
    return diskTup->GetTdId();
}

template TdId DataPage::GetTupleTdId<HeapDiskTuple> (OffsetNumber offset);
template TdId DataPage::GetTupleTdId<IndexTuple> (OffsetNumber offset);

template<typename tup_type>
TdId DataPage::GetTupleTdId(OffsetNumber offset)
{
    ItemId *itemId = GetItemIdPtr(offset);

    if (unlikely(itemId->IsUnused())) {
        return INVALID_TD_SLOT;
    }
    if (unlikely(itemId->IsNoStorage())) {
        return itemId->GetTdId();
    }
    tup_type *diskTup = static_cast<tup_type *>(GetRowData(itemId));
    return diskTup->GetTdId();
}

template TdId DataPage::GetTupleLockerTdId<HeapDiskTuple> (ItemId *itemId);
template TdId DataPage::GetTupleLockerTdId<IndexTuple> (ItemId *itemId);
template <typename tup_type>
TdId DataPage::GetTupleLockerTdId(ItemId *itemId)
{
    if (unlikely(itemId->IsNoStorage() || itemId->IsUnused())) {
        return INVALID_TD_SLOT;
    }
    tup_type *diskTup = static_cast<tup_type *>(GetRowData(itemId));
    return diskTup->GetLockerTdId();
}

template TdId DataPage::GetTupleLockerTdId<HeapDiskTuple> (OffsetNumber offset);
template TdId DataPage::GetTupleLockerTdId<IndexTuple> (OffsetNumber offset);
template <typename tup_type>
TdId DataPage::GetTupleLockerTdId(OffsetNumber offset)
{
    ItemId *itemId = GetItemIdPtr(offset);

    if (unlikely(itemId->IsNoStorage() || itemId->IsUnused())) {
        return INVALID_TD_SLOT;
    }
    tup_type *diskTup = static_cast<tup_type *>(GetRowData(itemId));
    return diskTup->GetLockerTdId();
}

template TupleTdStatus DataPage::GetTupleTdStatus<HeapDiskTuple>(OffsetNumber);
template TupleTdStatus DataPage::GetTupleTdStatus<IndexTuple>(OffsetNumber);
template<typename tup_type>
TupleTdStatus DataPage::GetTupleTdStatus(OffsetNumber offset)
{
    ItemId *itemId = GetItemIdPtr(offset);

    if (unlikely(itemId->IsNoStorage())) {
        return itemId->GetTdStatus();
    } else {
        tup_type *diskTup = static_cast<tup_type *>(GetRowData(itemId));
        return diskTup->GetTdStatus();
    }
}

template bool DataPage::TestTupleTdStatus<HeapDiskTuple>(TupleTdStatus, TupleTdStatus);
template bool DataPage::TestTupleTdStatus<IndexTuple>(TupleTdStatus, TupleTdStatus);
template<typename tup_type>
bool DataPage::TestTupleTdStatus(TupleTdStatus currentStatus, TupleTdStatus status)
{
    return currentStatus == status;
}

template bool DataPage::TestTupleTdStatus<HeapDiskTuple>(ItemId*, TupleTdStatus);
template bool DataPage::TestTupleTdStatus<IndexTuple>(ItemId*, TupleTdStatus);
template<typename tup_type>
bool DataPage::TestTupleTdStatus(ItemId *itemId, TupleTdStatus status)
{
    if (unlikely(itemId->IsNoStorage())) {
        return itemId->TestTdStatus(status);
    }
    tup_type *diskTup = static_cast<tup_type *>(GetRowData(itemId));
    return diskTup->TestTdStatus(status);
}

template bool DataPage::TestTupleTdStatus<HeapDiskTuple>(OffsetNumber, TupleTdStatus);
template bool DataPage::TestTupleTdStatus<IndexTuple>(OffsetNumber, TupleTdStatus);
template<typename tup_type>
bool DataPage::TestTupleTdStatus(OffsetNumber offset, TupleTdStatus status)
{
    ItemId *itemId = GetItemIdPtr(offset);

    if (unlikely(itemId->IsNoStorage())) {
        return itemId->TestTdStatus(status);
    }
    tup_type *diskTup = static_cast<tup_type *>(GetRowData(itemId));
    return diskTup->TestTdStatus(status);
}

template void DataPage::SetTupleTdStatus<HeapDiskTuple>(OffsetNumber, TupleTdStatus);
template<typename tup_type>
void DataPage::SetTupleTdStatus(OffsetNumber offset, TupleTdStatus status)
{
    ItemId *itemId = GetItemIdPtr(offset);

    if (unlikely(itemId->IsNoStorage())) {
        itemId->SetTdStatus(status);
    } else {
        tup_type *diskTup = static_cast<tup_type *>(GetRowData(itemId));
        diskTup->SetTdStatus(status);
    }
}

template bool DataPage::JudgeTupCommitBeforeSpecCsn<HeapDiskTuple, true>(
    PdbId, OffsetNumber, CommitSeqNo, bool &, CommitSeqNo *);
template bool DataPage::JudgeTupCommitBeforeSpecCsn<IndexTuple, true>(
    PdbId, OffsetNumber, CommitSeqNo, bool &, CommitSeqNo *);
template bool DataPage::JudgeTupCommitBeforeSpecCsn<IndexTuple, false>(
    PdbId, OffsetNumber, CommitSeqNo, bool &, CommitSeqNo *);
/*
 * If needFillCSN is true,  the caller must hold LW_EXCLUSIVE lock on page
 */
template<typename tup_type, bool needFillCSN>
bool DataPage::JudgeTupCommitBeforeSpecCsn(
    PdbId pdbId, OffsetNumber offset, CommitSeqNo specCsn, bool &isDirty, CommitSeqNo *tupleCsn)
{
#ifdef DSTORE_USE_ASSERT_CHECKING
    /* If do not hold exclusive lock , we can't fill TD of page */
    if (needFillCSN) {
        g_storageInstance->GetBufferMgr()->AssertHasHoldBufLock(pdbId, GetSelfPageId(), LW_EXCLUSIVE);
    }
#endif

    Transaction *transaction = thrd->GetActiveTransaction();
    StorageAssert(g_storageInstance->GetPdb(pdbId) != nullptr);
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(pdbId)->GetTransactionMgr();
    TD *tdInPage = GetTd(GetTupleTdId<tup_type>(offset));
    TD tdTmp = *tdInPage;
    TD *td = needFillCSN ? tdInPage : &tdTmp;
    isDirty = false;

    /* step 1: judge by csn in td */
    switch (GetTupleTdStatus<tup_type>(offset)) {
        case ATTACH_TD_AS_HISTORY_OWNER: {
            if (!td->TestCsnStatus(IS_INVALID) && td->GetCsn() < specCsn) {
                return true;
            }
            if (td->TestCsnStatus(IS_INVALID)) {
                isDirty = td->FillCsn(transaction) != INVALID_CSN;
                if (td->GetCsn() < specCsn) {
                    return true;
                }
            }
            break;
        }

        case ATTACH_TD_AS_NEW_OWNER: {
            /* if the tuple attach td as new owner, we can judge the csn by now */
            if (!td->TestCsnStatus(IS_CUR_XID_CSN)) {
                isDirty = td->FillCsn(transaction) != INVALID_CSN;
            }
            if (tupleCsn != nullptr) {
                *tupleCsn = td->GetCsn();
            }
            return td->GetCsn() < specCsn;
        }

        case DETACH_TD: {
            return true;
        }

        default: {
            StorageAssert(false);
        }
    }

    /* step 2: judge csn from undo */
    StorageAssert(TestTupleTdStatus<tup_type>(offset, ATTACH_TD_AS_HISTORY_OWNER));
    ItemPointerData ctid{GetSelfPageId(), offset};
    char *data = nullptr;
    if (GetType() == PageType::INDEX_PAGE_TYPE) {
        IndexTuple *itup = static_cast<IndexTuple *>(GetRowData(offset));
        data = itup->GetValues();
        ctid = itup->GetHeapCtid();
    }
    return transactionMgr->JudgeTupCommitBeforeSpecCsn(td, ctid, data, specCsn, tupleCsn);
}

void DataPage::FillCsn(Transaction *transaction, UndoMgr *undoMgr)
{
    StorageAssert(CheckSanity());
    uint8 tdNum = dataHeader.tdCount;
    for (TdId tdId = 0; tdId < tdNum; tdId++) {
        TD *td = GetTd(tdId);
        StorageAssert(td->CheckSanity());
        if (td->TestStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE) || td->TestCsnStatus(IS_CUR_XID_CSN)) {
            continue;
        }

        if (!undoMgr->IsZoneOwned(td->GetXid().m_zoneId)) {
            continue;
        }

        (void)td->FillCsn(transaction);
        StorageAssert(td->CheckSanity());
    }
}


/*
 * Page rollback for service, In order to reduce wait time.
 */

RetStatus DataPage::RollbackByXid(PdbId pdbId, Xid xid, BufMgrInterface *bufMgr, BufferDesc *bufferDesc,
                                  BtreeUndoContext *btrUndoContext)
{
    uint8 tdNum = GetTdCount();
    for (TdId tdId = 0; tdId < tdNum; ++tdId) {
        TD *td = GetTd(tdId);
        if (td->m_xid == xid.m_placeHolder) {
            if (STORAGE_FUNC_FAIL((RollbackTdOneXidAsNeed(pdbId, *td, bufMgr, bufferDesc, btrUndoContext)))) {
                ErrLog(DSTORE_ERROR, MODULE_PAGE, ErrMsg("PageId{%hu, %u}, tdId(%hhu, xid {%d %lu} fault.",
                    GetFileId(), GetBlockNum(), tdId, static_cast<int>(xid.m_zoneId), xid.m_logicSlotId));
                return DSTORE_FAIL;
            }
            /* After the xid is specified for page rollback, the lockerXid corresponding to td on the page should also
             * be cleared. */
            td->m_lockerXid = INVALID_XID.m_placeHolder;
            /* One transaction only has one td in one page */
            break;
        } else if (td->m_xid == INVALID_XID.m_placeHolder && td->m_lockerXid == xid.m_placeHolder) {
            /* After the xid is specified for page rollback, the lockerXid corresponding to td on the page should also
             * be cleared. */
            td->Reset();
        }
    }
    return DSTORE_SUCC;
}

/*
 * Rollback the undo record, restore the previous td status.
 */
RetStatus DataPage::RollbackByUndoRec(UndoRecord *undoRec, BtreeUndoContext *btrUndoContext, TD *tdOnPage)
{
    PageType pageType = GetType();
    if (pageType == PageType::HEAP_PAGE_TYPE) {
        return static_cast<HeapPage *>(this)->UndoHeap(undoRec);
    }
    if (pageType == PageType::INDEX_PAGE_TYPE) {
        return static_cast<BtrPage *>(this)->UndoBtree(undoRec, btrUndoContext, tdOnPage);
    }

    ErrLog(DSTORE_ERROR, MODULE_PAGE,
           ErrMsg("Unknow UndoRec type %hhu of DataPage", static_cast<uint8>(undoRec->GetUndoType())));
    return DSTORE_FAIL;
}

uint16 DataPage::CompactTuples(ItemIdCompact compactItems, uint32 nItems)
{
    uint32 i = 0;
    uint16 upper = GetSpecialOffset();
    uint32 head, tail = 0;
    ItemIdCompact compactItem = nullptr;
    errno_t rc;
    bool needMove = false;

    /* Step 1: find if need move */
    while (i < nItems) {
        compactItem = &compactItems[i];
        if (upper != compactItem->tupOffset + compactItem->itemLen) {
            needMove = true;
            break;
        }
        upper -= compactItem->itemLen;
        i++;
    }

    /* All tuples are continuous. */
    if (!needMove) {
        return upper;
    }

    /* Step 2: move continue item one by one */
    tail = head = compactItem->tupOffset + compactItem->itemLen;
    for (; i < nItems; i++) {
        compactItem = &compactItems[i];
        if (head != compactItem->tupOffset + compactItem->itemLen) {
            rc = memmove_s(static_cast<char *>(static_cast<void *>(this)) + upper, tail - head,
                           static_cast<char *>(static_cast<void *>(this)) + head, tail - head);
            storage_securec_check(rc, "\0", "\0");

            tail = compactItem->tupOffset + compactItem->itemLen;
        }

        upper -= compactItem->itemLen;
        head = compactItem->tupOffset;
        /* readjust the right offset */
        GetItemIdPtr(compactItem->itemOffnum)->SetOffset(upper);
    }

    /* Step 3: remove the remaining */
    StorageAssert(tail != head);
    rc = memmove_s(static_cast<char *>(static_cast<void *>(this)) + upper, tail - head,
                   static_cast<char *>(static_cast<void *>(this)) + head, tail - head);
    storage_securec_check(rc, "\0", "\0");

    return upper;
}

/* this func only called by RollbackByXid . */
RetStatus DataPage::RollbackTdOneXidAsNeed(
    PdbId pdbId, TD &td, BufMgrInterface *bufMgr, BufferDesc *bufferDesc, BtreeUndoContext *btrUndoContext)
{
    StorageAssert(g_storageInstance->GetPdb(pdbId) != nullptr);
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(pdbId)->GetTransactionMgr();
    UndoRecord undoRecord;
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(walContext) || STORAGE_VAR_NULL(transaction)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Invalid wal context or transaction when rollback td one xid as need."));
        return DSTORE_FAIL;
    }
    Xid curXid = transaction->GetCurrentXid();
    WalType walType =
        TestType(PageType::INDEX_PAGE_TYPE) ? WAL_UNDO_BTREE_PAGE_ROLL_BACK : WAL_UNDO_HEAP_PAGE_ROLL_BACK;
    Xid xid = td.GetXid();

    for (;;) {
        if (STORAGE_FUNC_FAIL(transactionMgr->FetchUndoRecord(xid, &undoRecord, td.GetUndoRecPtr()))) {
            if (StorageGetErrorCode() == UNDO_ERROR_RECORD_RECYCLED) {
                /* The undoRecord has been recycled. */
                td.Reset();
                break;
            }
            return DSTORE_FAIL;
        }

        StorageAssert(undoRecord.IsUndoDataValid());
        if (GetType() == PageType::INDEX_PAGE_TYPE) {
            StorageAssert(btrUndoContext != nullptr);
            bool pageRollbackSkipCase = false;
            if (STORAGE_FUNC_FAIL(btrUndoContext->SetUndoInfo(xid, &undoRecord))) {
                return DSTORE_FAIL;
            }
            if (!btrUndoContext->DoesUndoRecMatchCurrPage(pageRollbackSkipCase)) {
                if (unlikely(btrUndoContext->m_skipRollbackTd)) {
                    /* TD rollback is only allowed to be skipped when PAGE_ROLLBACK */
                    StorageAssert(btrUndoContext->m_undoType == BtreeUndoContextType::PAGE_ROLLBACK);
                    btrUndoContext->ClearUndoRec();
                    /* Keep wal same with page. Do not write wal if nothing changed on page. */
                    break;
                }
                StorageAssert(btrUndoContext->m_offset == INVALID_ITEM_OFFSET_NUMBER);
                td.RollbackTdInfo(&undoRecord);
            } else if (STORAGE_FUNC_FAIL(RollbackByUndoRec(&undoRecord, btrUndoContext, &td))) {
                return DSTORE_FAIL;
            }

            /* For BtrPage, rollback may eventually executed on a different page with the page that we have saved in
             * the undo record because of btree splitting.
             * We would copy the undoRec into wal record, and redo the wal record with the ctid from undoRec directly
             * when recovery. Thus the ctid of undoRec must be updated to where the rollback actually happens
             */
            undoRecord.SetCtid({GetSelfPageId(), btrUndoContext->m_offset});
            btrUndoContext->ClearUndoRec();
            if (unlikely(pageRollbackSkipCase)) {
                StorageAssert(btrUndoContext->m_undoType == BtreeUndoContextType::PAGE_ROLLBACK);
                /* hit all-same-key cases that we cannot handle when page rollback.
                 * just skip it and wait until async rollback finished */
                break;
            }
        } else if (STORAGE_FUNC_FAIL(RollbackByUndoRec(&undoRecord))) {
            return DSTORE_FAIL;
        }
        StorageAssert(bufMgr != nullptr);
        (void)bufMgr->MarkDirty(bufferDesc);
        StorageAssert(walContext != nullptr);
        walContext->BeginAtomicWal(curXid);
        UndoZone::GenerateWalForRollback(bufferDesc, undoRecord, walType);
        (void)walContext->EndAtomicWal();

        if (td.GetXid() != xid) {
            break;
        }
    }
    return DSTORE_SUCC;
}

/*
 * if isCurrentXid, a visible cid means construct cr finished.
 */
RetStatus DataPage::RollbackTdOneXidForCRAsNeed(
    PdbId pdbId, TD &td, bool isCurrentXid, BtreeUndoContext *btrUndoContext, Snapshot snapshot)
{
    bool isIndexPage = (GetType() == PageType::INDEX_PAGE_TYPE);
    bool needStrictCheckUndo = true;
    bool isUndoValid = true;
    if (isIndexPage && btrUndoContext == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
            ErrMsg("Pagetype is index but btrUndoContext is null."));
        return DSTORE_FAIL;
    }

    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(pdbId)->GetTransactionMgr();
    UndoRecord undoRecord;
    Xid xid = td.GetXid();
    if (UndoZone::IsXidRecycled(pdbId, xid)) {
        storage_set_error(UNDO_ERROR_RECORD_RECYCLED);
        /* The undoRecord has been recycled. */
        td.Reset();
        return DSTORE_SUCC;
    }

    /*
     * When building the cr page of the index, there is a scenario where, after the index page splits, the td occupied
     * by the aborted transaction remains on the page. For this scenario, there is no need to verify the legitimacy of
     * the content of the undo record. When the content is illegal, it is sufficient to verify on the outer layer
     * whether the transaction has been frozen.
     */
    if (isIndexPage) {
        needStrictCheckUndo = false;
    }

    for (;;) {
        transactionMgr->FetchUndoRecordInternal(&undoRecord, td.GetUndoRecPtr(), needStrictCheckUndo, &isUndoValid);
        if (unlikely(!isUndoValid && !needStrictCheckUndo)) {
            if (UndoZone::IsXidRecycled(pdbId, xid)) {
                storage_set_error(UNDO_ERROR_RECORD_RECYCLED);
                /* The undoRecord has been recycled. */
                td.Reset();
                return DSTORE_SUCC;
            } else {
                ErrLog(DSTORE_ERROR, MODULE_UNDO,
                       ErrMsg("Fetch invalid undo record when rollback index page td xid for cr as need."));
                return DSTORE_FAIL;
            }
        }
        StorageAssert(undoRecord.IsUndoDataValid());
        if (isCurrentXid && CidVisibleToSnapshot(thrd->GetActiveTransaction(), snapshot, undoRecord.GetCid())) {
            break;
        }
        RetStatus ret = DSTORE_SUCC;
        if (GetType() ==  PageType::INDEX_PAGE_TYPE) {
            if (STORAGE_FUNC_FAIL(btrUndoContext->SetUndoInfo(xid, &undoRecord))) {
                ret = DSTORE_FAIL;
            } else if (!btrUndoContext->DoesUndoRecMatchCurrCrPage()) {
                td.RollbackTdInfo(&undoRecord);
            } else {
                /* btrUndoContext->m_offset may be invalid after the tuple is pruned. */
                ret = RollbackByUndoRec(&undoRecord, btrUndoContext, &td);
            }
            btrUndoContext->ClearUndoRec();
        } else {
            StorageAssert(GetType() ==  PageType::HEAP_PAGE_TYPE);
            ret = RollbackByUndoRec(&undoRecord);
        }
        if (STORAGE_FUNC_FAIL(ret)) {
            return DSTORE_FAIL;
        }
        if (td.GetXid() != xid) {
            break;
        }
    }
    return DSTORE_SUCC;
}

RetStatus DataPage::RollbackTdsForCR(binaryheap *tdHeap, Transaction *transaction, CRContext *crCtx,
                                     BtreeUndoContext *btrUndoContext)
{
    StorageAssert(tdHeap != nullptr);
    if (btrUndoContext != nullptr) {
        /* We've rollback in-progress transaction. Now dealing with committed transactions. */
        btrUndoContext->m_forCommittedTxrRollback = true;
    }

    CommitSeqNo snapshotCsn = crCtx->snapshot->snapshotCsn;
    BinaryheapBuild(tdHeap);
    while (!binaryheap_empty(tdHeap)) {
        TD *td = static_cast<TD *>(static_cast<void *>(DatumGetPointer(BinaryheapFirst(tdHeap))));
        Xid xid = td->GetXid();
        CommitSeqNo topCsn = td->GetCsn();
        StorageAssert(xid != INVALID_XID);
        StorageAssert(topCsn != INVALID_CSN);
        if (topCsn < snapshotCsn) {
            crCtx->pageMaxCsn = (topCsn > crCtx->pageMaxCsn) ? topCsn : crCtx->pageMaxCsn;
            break;
        }
        crCtx->useLocalCr = true;
        if (STORAGE_FUNC_FAIL(
            (RollbackTdOneXidForCRAsNeed(crCtx->pdbId, *td, false, btrUndoContext, crCtx->snapshot)))) {
            return DSTORE_FAIL;
        }
        xid = td->GetXid();
        if (xid == INVALID_XID) {
            StorageAssert(td->GetUndoRecPtr() == INVALID_UNDO_RECORD_PTR);
            BinaryheapRemoveFirst(tdHeap);
            continue;
        }
        if (!td->TestCsnStatus(IS_CUR_XID_CSN)) {
            (void)td->FillCsn(transaction);
        }
        StorageAssert(td->TestCsnStatus(IS_CUR_XID_CSN));
        /* update heap with new td. */
        BinaryheapReplaceFirst(tdHeap, PointerGetDatum(td));
    }
    BinaryheapFree(tdHeap);
    StorageAssert(crCtx->pageMaxCsn != INVALID_CSN);
    return DSTORE_SUCC;
}

template<typename tupleType>
void DataPage::UpdateTupleTdStatus(TdId tdId, TupleTdStatus src, TupleTdStatus dest)
{
    for (OffsetNumber off = 1; off < GetMaxOffset(); off = OffsetNumberNext(off)) {
        if (GetTupleTdId<tupleType>(off) != tdId || !TestTupleTdStatus<tupleType>(off, src)) {
            continue;
        }
        SetTupleTdStatus<tupleType>(off, dest);
    }
}

/*
 * In first loop, traversal all tds to rollback some undo records generated by current transaction but later
 * than snapshot.cid, and transactions which is in progress.
 * In second loop, rollback all transactions descending by csn, util the max csn in page < snapshot.csn.
 *
 * If rollback any undo record or current xid modified this page, set useLocalCr is true, That means we
 * construct cr page in local other than in cr buffer.
 */
RetStatus DataPage::ConstructCR(Transaction *transaction, CRContext *crCtx, BtreeUndoContext *btrUndoContext)
{
    RetStatus status = DSTORE_SUCC;
    StorageAssert(g_storageInstance->GetPdb(crCtx->pdbId) != nullptr);
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(crCtx->pdbId)->GetTransactionMgr();
    binaryheap *tdHeap = nullptr;
    CommitSeqNo maxCsnInPage = COMMITSEQNO_FIRST_NORMAL;
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("[Construct CR] Construct Cr entry, bufTag:(%hhu, %hu, %u), "
        "lsnInfo(walId %lu glsn %lu plsn %lu), crCtx(mvcc %d csn %lu cid %u xid(%d, %lu)).", crCtx->pdbId,
        GetFileId(), GetBlockNum(), GetWalId(), GetGlsn(), GetPlsn(),
        static_cast<int>(crCtx->snapshot->GetSnapshotType()), crCtx->snapshot->GetCsn(), crCtx->snapshot->GetCid(),
        static_cast<int>(crCtx->currentXid.m_zoneId), crCtx->currentXid.m_logicSlotId));
#endif
    /*
     * Step 1: Get which td need do rollback. If the csn of td is bigger than snapshot csn,
     * the td need be rollback.  We need handle current transaction and in-progress transaction
     * which transaction's csn has not been set.
     * */
    TdId tdId = 0;
    while (tdId < GetTdCount()) {
        TD *td = GetTd(tdId++);
        __builtin_prefetch(static_cast<TD *>(td + sizeof(TD)), 0, 1);
        Xid xid = td->GetXid();
        XidStatus xidStatus(xid, transaction, td);
#ifdef DSTORE_USE_ASSERT_CHECKING
        ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("[Construct CR] bufTag:(%hhu, %hu, %u), lsnInfo(walId %lu glsn %lu "
            "plsn %lu), crCtx(mvcc %d csn %lu cid %u xid(%d, %lu)), tdInfo(tdId %hhu csn %lu csnStatus %hu cid %u "
            "xid(%d, %lu)), xidStatus(status %d csn %lu).", crCtx->pdbId, GetFileId(), GetBlockNum(), GetWalId(),
            GetGlsn(), GetPlsn(), static_cast<int>(crCtx->snapshot->GetSnapshotType()), crCtx->snapshot->GetCsn(),
            crCtx->snapshot->GetCid(), static_cast<int>(crCtx->currentXid.m_zoneId), crCtx->currentXid.m_logicSlotId,
            tdId, td->GetCsn(), td->GetCsnStatus(), td->GetCommandId(), static_cast<int>(xid.m_zoneId),
            xid.m_logicSlotId, xidStatus.GetStatus(), xidStatus.GetCsn()));
#endif
        if (xidStatus.IsFrozen()) {
            continue;
        }
        if (xid != INVALID_XID && xid == crCtx->currentXid) {
            crCtx->useLocalCr = true;
            if (td->GetCommandId() >= crCtx->snapshot->GetCid()) {
                status = RollbackTdOneXidForCRAsNeed(crCtx->pdbId, *td, true, btrUndoContext, crCtx->snapshot);
#ifdef DSTORE_USE_ASSERT_CHECKING
                ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("[Construct CR] Current Cid branch bufTag:(%hhu, %hu, %u), "
                    "lsnInfo(walId %lu glsn %lu plsn %lu), crCtx(mvcc %d csn %lu cid %u xid(%d, %lu)), "
                    "tdInfo(tdId %hhu csn %lu csnStatus %hu cid %u xid(%d, %lu)), xidStatus(status %d csn %lu).",
                    crCtx->pdbId, GetFileId(), GetBlockNum(), GetWalId(), GetGlsn(), GetPlsn(),
                    static_cast<int>(crCtx->snapshot->GetSnapshotType()), crCtx->snapshot->GetCsn(),
                    crCtx->snapshot->GetCid(), static_cast<int>(crCtx->currentXid.m_zoneId),
                    crCtx->currentXid.m_logicSlotId, tdId, td->GetCsn(), td->GetCsnStatus(), td->GetCommandId(),
                    static_cast<int>(xid.m_zoneId), xid.m_logicSlotId, xidStatus.GetStatus(), xidStatus.GetCsn()));
#endif
            }
            if (STORAGE_FUNC_FAIL(status)) {
                return status;
            }
            if (xid == td->GetXid()) {
                /* We've rollback current transaction to the visible version according to command ID.
                 * Now we need to check if the previous transaction on the TD is visible to us */

                /* Consider the following case:
                 * 1. The current transaction start with snapshot CSN_1.
                 * 2. Transaction A has written td_1 and then been committed with CSN_2, which is greater than CSN_1.
                 * 3. The current transaction re-used td_1 and made some changes on the page.
                 * 4. The current transaction start to scan the page, using snapshot CSN_1.
                 * In this case, the current transaction should not see changes made by Transaction A because the
                 * committed csn CSN_2 of Transaction A is greater the the current transaction's snapshot csn CSN_1.
                 * Thus we need to check the previous transaction's CSN of TD */
                if (!td->TestCsnStatus(IS_PREV_XID_CSN) || td->GetCsn() < crCtx->snapshot->GetCsn()) {
                    /* In this branch, we have 2 cases:
                     * case 1. the csn recorded in the td is not "previous transaction's csn", meaning the current
                     *         transaction is not "RE-USING" the td. There's no previous transaction that is needed
                     *         to be considered. We've done all for this td, just go on to the next one.
                     * case 2. the csn of previous transaction in this td is smaller than current transaction's
                     *         snapshot csn. There's no need to rollback anything. */
                    continue;
                }
                /* The current transaction is using an old snapshot to construct CR.
                 * Need to rollback current transaction infos in td, then continue to check the previous one */
                td->RollbackTdToPreTxn(crCtx->pdbId);
                /* Also we need to set td status to detach for tuples that using this td now . */
                if (btrUndoContext != nullptr) {
                    UpdateTupleTdStatus<IndexTuple>(tdId, ATTACH_TD_AS_NEW_OWNER, DETACH_TD);
                } else {
                    UpdateTupleTdStatus<HeapDiskTuple>(tdId, ATTACH_TD_AS_NEW_OWNER, DETACH_TD);
                }
#ifdef DSTORE_USE_ASSERT_CHECKING
                ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("[Construct CR] Current Xid branch bufTag:(%hhu, %hu, "
                    "%u), lsnInfo(walId %lu glsn %lu plsn %lu), crCtx(mvcc %d csn %lu cid %u xid(%d, %lu)), "
                    "tdInfo(tdId %hhu csn %lu csnStatus %hu cid %u xid(%d, %lu)), xidStatus(status %d csn %lu).",
                    crCtx->pdbId, GetFileId(), GetBlockNum(), GetWalId(), GetGlsn(), GetPlsn(),
                    static_cast<int>(crCtx->snapshot->GetSnapshotType()), crCtx->snapshot->GetCsn(),
                    crCtx->snapshot->GetCid(), static_cast<int>(crCtx->currentXid.m_zoneId),
                    crCtx->currentXid.m_logicSlotId, tdId, td->GetCsn(), td->GetCsnStatus(), td->GetCommandId(),
                    static_cast<int>(xid.m_zoneId), xid.m_logicSlotId, xidStatus.GetStatus(), xidStatus.GetCsn()));
#endif
            }
        } else if (xidStatus.IsPendingCommit() && crCtx->snapshot->GetSnapshotType() != SnapshotType::SNAPSHOT_DIRTY) {
            bool txnFailed = false;
            if (xidStatus.NeedWaitPendingTxn()) {
                status = transactionMgr->WaitForTransactionEnd(xidStatus.GetXid(), txnFailed);
            }
            if (unlikely(txnFailed) || (!xidStatus.NeedWaitPendingTxn())) {
                BtreeUndoContext::SetRollbackAbortedForCr(btrUndoContext, txnFailed);
                status = RollbackTdOneXidForCRAsNeed(crCtx->pdbId, *td, false, btrUndoContext, crCtx->snapshot);
                BtreeUndoContext::SetRollbackAbortedForCr(btrUndoContext, false);
                crCtx->useLocalCr = true;
#ifdef DSTORE_USE_ASSERT_CHECKING
                ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("[Construct CR] Pending branch bufTag:(%hhu, %hu, %u), "
                    "lsnInfo(walId %lu glsn %lu plsn %lu), crCtx(mvcc %d csn %lu cid %u xid(%d, %lu)), "
                    "tdInfo(tdId %hhu csn %lu csnStatus %hu cid %u xid(%d, %lu)), xidStatus(status %d csn %lu).",
                    crCtx->pdbId, GetFileId(), GetBlockNum(), GetWalId(), GetGlsn(), GetPlsn(),
                    static_cast<int>(crCtx->snapshot->GetSnapshotType()), crCtx->snapshot->GetCsn(),
                    crCtx->snapshot->GetCid(), static_cast<int>(crCtx->currentXid.m_zoneId),
                    crCtx->currentXid.m_logicSlotId, tdId, td->GetCsn(), td->GetCsnStatus(), td->GetCommandId(),
                    static_cast<int>(xid.m_zoneId), xid.m_logicSlotId, xidStatus.GetStatus(), xidStatus.GetCsn()));
#endif
            }
        } else if (xidStatus.IsInProgress() && crCtx->snapshot->GetSnapshotType() != SnapshotType::SNAPSHOT_DIRTY) {
            /* If xid is in progress (no in-progress txn on cr page), rollback TD chain until a different xid */
            crCtx->useLocalCr = true;
            status = RollbackTdOneXidForCRAsNeed(crCtx->pdbId, *td, false, btrUndoContext, crCtx->snapshot);
#ifdef DSTORE_USE_ASSERT_CHECKING
            ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("[Construct CR] Inprogress branch bufTag:(%hhu, %hu, %u), "
                "lsnInfo(walId %lu glsn %lu plsn %lu), crCtx(mvcc %d csn %lu cid %u xid(%d, %lu)), "
                "tdInfo(tdId %hhu csn %lu csnStatus %hu cid %u xid(%d, %lu)), xidStatus(status %d csn %lu).",
                crCtx->pdbId, GetFileId(), GetBlockNum(), GetWalId(), GetGlsn(), GetPlsn(),
                static_cast<int>(crCtx->snapshot->GetSnapshotType()), crCtx->snapshot->GetCsn(),
                crCtx->snapshot->GetCid(), static_cast<int>(crCtx->currentXid.m_zoneId),
                crCtx->currentXid.m_logicSlotId, tdId, td->GetCsn(), td->GetCsnStatus(), td->GetCommandId(),
                static_cast<int>(xid.m_zoneId), xid.m_logicSlotId, xidStatus.GetStatus(), xidStatus.GetCsn()));
#endif
        } else if (xidStatus.IsAborted()) {
            /*
             * The xid in td may be aborted in 2 cases.
             * 1. Before copying from base page, the trx don't roll back the data in base page. After copying,
             *    the trx do rollback in base page and trx slot page, but data in cr page also need to be rolled
             *    back here. This case is for both heap and index page.
             * 2. If one trx causes btree split and the trx do abort, there will be aborted xid in td. Here we
             *    only roll back td and don't need to roll back data. This case is only for index page.
             */
            BtreeUndoContext::SetRollbackAbortedForCr(btrUndoContext, true);
            status = RollbackTdOneXidForCRAsNeed(crCtx->pdbId, *td, false, btrUndoContext, crCtx->snapshot);
            BtreeUndoContext::SetRollbackAbortedForCr(btrUndoContext, false);
#ifdef DSTORE_USE_ASSERT_CHECKING
            ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("[Construct CR] Abort branch bufTag:(%hhu, %hu, %u), "
                "lsnInfo(walId %lu glsn %lu plsn %lu), crCtx(mvcc %d csn %lu cid %u xid(%d, %lu)), "
                "tdInfo(tdId %hhu csn %lu csnStatus %hu cid %u xid(%d, %lu)), xidStatus(status %d csn %lu).",
                crCtx->pdbId, GetFileId(), GetBlockNum(), GetWalId(), GetGlsn(), GetPlsn(),
                static_cast<int>(crCtx->snapshot->GetSnapshotType()), crCtx->snapshot->GetCsn(),
                crCtx->snapshot->GetCid(), static_cast<int>(crCtx->currentXid.m_zoneId),
                crCtx->currentXid.m_logicSlotId, tdId, td->GetCsn(), td->GetCsnStatus(), td->GetCommandId(),
                static_cast<int>(xid.m_zoneId), xid.m_logicSlotId, xidStatus.GetStatus(), xidStatus.GetCsn()));
#endif
        }

        if (STORAGE_FUNC_FAIL(status)) {
            return status;
        }

        if (td->GetXid() == INVALID_XID) {
            StorageAssert(td->GetUndoRecPtr() == INVALID_UNDO_RECORD_PTR);
            continue;
        }
        (void)td->FillCsn(transaction);
#ifdef DSTORE_USE_ASSERT_CHECKING
        ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("[Construct CR] After fill csn bufTag:(%hhu, %hu, %u), "
            "lsnInfo(walId %lu glsn %lu plsn %lu), crCtx(mvcc %d csn %lu cid %u xid(%d, %lu)), "
            "tdInfo(tdId %hhu csn %lu csnStatus %hu cid %u xid(%d, %lu)), xidStatus(status %d csn %lu).",
            crCtx->pdbId, GetFileId(), GetBlockNum(), GetWalId(), GetGlsn(), GetPlsn(),
            static_cast<int>(crCtx->snapshot->GetSnapshotType()), crCtx->snapshot->GetCsn(), crCtx->snapshot->GetCid(),
            static_cast<int>(crCtx->currentXid.m_zoneId), crCtx->currentXid.m_logicSlotId, tdId, td->GetCsn(),
            td->GetCsnStatus(), td->GetCommandId(), static_cast<int>(xid.m_zoneId), xid.m_logicSlotId,
            xidStatus.GetStatus(), xidStatus.GetCsn()));
#endif
        if (crCtx->snapshot->GetSnapshotType() == SnapshotType::SNAPSHOT_DIRTY) {
            /* The csn of the transaction being processed is 0. Snapshot dirty csn is MAX_COMMITSEQNO.
               snapshot dirty page is not placed in localCr. don't need maxCsnInPage.
             */
            continue;
        }
        StorageAssert(td->TestCsnStatus(IS_CUR_XID_CSN));
        if (td->GetCsn() >= crCtx->snapshot->GetCsn()) {
            tdHeap = tdHeap == nullptr ? BinaryheapAllocate(GetTdCount(), TD::RollbackComparator, nullptr) : tdHeap;
            BinaryheapAddUnordered(tdHeap, PointerGetDatum(td));
        } else {
            maxCsnInPage = maxCsnInPage < td->GetCsn() ? td->GetCsn() : maxCsnInPage;
        }
    }

    crCtx->pageMaxCsn = maxCsnInPage;
    /* Step 2: There are pre-tds need to rollback. */
    if (tdHeap != nullptr) {
        status = RollbackTdsForCR(tdHeap, transaction, crCtx, btrUndoContext);
    }

    StorageAssert(crCtx->pageMaxCsn != INVALID_CSN);

    if (unlikely(GetIsCrExtend())) {
        crCtx->useLocalCr = true;
    }
    return status;
}

void DataPage::DumpDataPageHeader(StringInfo str)
{
    Page::DumpHeader(str);
    static const char *strTdStatus[3] = {
        "UNOCCUPY_AND_PRUNEABLE",
        "OCUPPY_TRX_IN_PROGRESS",
        "OCUPPY_TRX_END        ",
    };

    static const char *strTdCsnStatus[3] = {
        "is invalid         ",
        "is previous xid csn",
        "is current xid csn ",
    };

    if (GetType() == PageType::HEAP_PAGE_TYPE) {
        for (TdId tdId = 0; tdId < GetTdCount(); tdId++) {
            TD *td = GetTd(tdId);
            str->append("TD %hhu:\t", tdId);
            str->append("[status] %s\t", strTdStatus[td->m_status]);
            str->append("[xid] (%d, %lu)\t", static_cast<int32>(td->GetXid().m_zoneId), td->GetXid().m_logicSlotId);
            str->append("[lockerxid] (%d, %lu)\t", static_cast<int32>(td->GetLockerXid().m_zoneId),
                td->GetLockerXid().m_logicSlotId);
            str->append("[csn] %lu\t", td->GetCsn());
            str->append("[csnStatus] %s", strTdCsnStatus[td->GetCsnStatus()]);
            str->append("[undoRecPtr] (%hu, %u, %hu)\n", td->GetUndoRecPtr().GetFileId(),
                        td->GetUndoRecPtr().GetBlockNum(), td->GetUndoRecPtr().GetOffset());
        }
    } else {
        for (TdId tdId = 0; tdId < GetTdCount(); tdId++) {
            TD *td = GetTd(tdId);
            str->append("TD %hhu:\t", tdId);
            str->append("[status] %s\t", strTdStatus[td->m_status]);
            str->append("[xid] (%d, %lu)\t", static_cast<int32>(td->GetXid().m_zoneId), td->GetXid().m_logicSlotId);
            str->append("[commandId] (%u)\t", td->GetCommandId());
            str->append("[csn] %lu\t", td->GetCsn());
            str->append("[csnStatus] %s", strTdCsnStatus[td->GetCsnStatus()]);
            str->append("[undoRecPtr] (%hu, %u, %hu)\n", td->GetUndoRecPtr().GetFileId(),
                        td->GetUndoRecPtr().GetBlockNum(), td->GetUndoRecPtr().GetOffset());
        }
    }
}

#ifdef DSTORE_USE_ASSERT_CHECKING
bool DataPage::CheckSanity()
{
    if (unlikely(PageNoInit())) {
        return true;
    }
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
    uint32 pageSize = GetIsCrExtend() ? EXTEND_PAGE_SIZE : PAGE_SIZE;
    bool corruptPage = m_header.m_lower < DATA_PAGE_HEADER_SIZE || m_header.m_lower > m_header.m_upper ||
                       m_header.m_upper > m_header.m_special.m_offset || m_header.m_special.m_offset > pageSize;
    StorageAssert(!corruptPage);

    /* Check Td space is valid. */
    uint16 dataHeaderSize = DataHeaderSize();
    uint16 tdSpaceSize = dataHeader.tdCount * sizeof(TD);
    StorageAssert(m_header.m_lower >= (dataHeaderSize + tdSpaceSize));

    /* Check ItemId space is valid. */
    uint16 itemIdSpaceSize = static_cast<uint16>(m_header.m_lower - (dataHeaderSize + tdSpaceSize));
    uint16 itemIdCount = itemIdSpaceSize / sizeof(ItemId);
    StorageAssert(itemIdSpaceSize % sizeof(ItemId) == 0);

    /* Check item id point to a valid tuple if it is in normal state. */
    ItemId **checkItems = static_cast<ItemId **>(DstorePalloc(sizeof(ItemId *) * itemIdCount));
    if (checkItems != nullptr) {
        uint16 nItems = 0;
        for (OffsetNumber offset = 1; offset <= itemIdCount; offset++) {
            ItemId *id = GetItemIdPtr(offset);
            if (id->IsNormal()) {
                StorageAssert(CheckItemIdSanity(id));
                checkItems[nItems++] = id;
            }
        }

        /* Check tuples don't overlap with each other. */
        qsort(checkItems, nItems, sizeof(ItemId *), ItemId::DescendingSortByOffsetCompare);
        uint32 limit = m_header.m_special.m_offset;
        for (uint16 i = 0; i < nItems; i++) {
            StorageAssert((checkItems[i]->GetOffset() + checkItems[i]->GetLen()) <= limit);
            limit = checkItems[i]->GetOffset();
        }

        DstorePfree(checkItems);
    }

    return true;
}

bool DataPage::CheckItemIdSanity(ItemId *id) const
{
    if (id->GetOffset() < m_header.m_upper || id->GetOffset() + id->GetLen() > m_header.m_special.m_offset) {
        StorageAssert(false);
    }
    return true;
}
#endif

void DataPage::ExtendCrPage()
{
    StorageAssert(!GetIsCrExtend());
    errno_t rc = memmove_s(static_cast<char*>(static_cast<void*>(this)) + GetUpper() + BLCKSZ,
                           BLCKSZ - GetUpper(),
                           static_cast<char*>(static_cast<void*>(this)) + GetUpper(),
                           BLCKSZ - GetUpper());
    StorageReleasePanic(rc != 0, MODULE_BUFFER, ErrMsg("memmove tuples and tail failed!"));

    /* update offset */
    for (uint16 i = 1; i <= GetMaxOffset(); i++) {
        ItemId *ii = GetItemIdPtr(i);
        ii->SetOffset(ii->GetOffset() + BLCKSZ);
    }

    SetUpper(GetUpper() + BLCKSZ);
    SetSpecialOffset(GetSpecialOffset() + BLCKSZ);
    SetIsCrExtend(true);
    SetChecksum(true);
    StorageAssert(CheckSanity());
}

}  // namespace DSTORE
