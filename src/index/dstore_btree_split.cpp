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
 * dstore_btree_split.cpp
 *
 * IDENTIFICATION
 *        dstore/src/index/dstore_btree_split.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "index/dstore_btree_split.h"
#include "index/dstore_btree_split_strategy.h"
#include "index/dstore_btree_undo_data_struct.h"
#include "common/log/dstore_log.h"
#include "errorcode/dstore_index_error_code.h"
#include "index/dstore_btree_wal.h"
#include "index/dstore_btree_page_recycle.h"
#include "transaction/dstore_csn_mgr.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_mgr.h"
#include "wal/dstore_wal_write_context.h"
#include "index/dstore_btree_perf_unit.h"
#include "fault_injection/fault_injection.h"

namespace DSTORE {

#ifdef UT
constexpr long SLEEP_TIME_BEFORE_RETRY_IN_MICRO_SEC = 1; /* 1 us */
#else
constexpr long SLEEP_TIME_BEFORE_RETRY_IN_MICRO_SEC = 10000; /* 10 ms */
#endif

SplitContext g_utSplitContext = {INVALID_ITEM_OFFSET_NUMBER, INVALID_ITEM_OFFSET_NUMBER, false};

BtreeSplit::BtreeSplit(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKeyInfo, bool keepStack)
    : Btree(indexRel, indexInfo, scanKeyInfo), m_cid(INVALID_CID), m_needRecordUndo(true),
      m_needRetrySearchBtree(false), m_retryCount(0), m_leafStack(nullptr), m_keepStackAfterSearch(keepStack),
      m_needTruncateHikey(true)
{
    bool needWal = !(indexRel != nullptr && indexRel->btreeSmgr != nullptr && indexRel->btreeSmgr->IsGlobalTempIndex());
    m_tdContext.Init((indexRel != nullptr) ? indexRel->m_pdbId : INVALID_PDB_ID, needWal);
    InitSplittingTarget(INVALID_BUFFER_DESC);
}

BtreeSplit::~BtreeSplit()
{
    m_leafStack = nullptr;
    InitSplittingTarget(INVALID_BUFFER_DESC);
}

RetStatus BtreeSplit::FormIndexTuple(Datum *values, bool *isnull, ItemPointer heapCtid, IndexTuple **indexTuple)
{
    if (unlikely(values == nullptr || isnull == nullptr || heapCtid == nullptr || indexTuple == nullptr)) {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("input parameter is nullptr when FormIndexTuple"));
        return DSTORE_FAIL;
    }
    uint32 tupleSize = BLCKSZ;
    *indexTuple = IndexTuple::FormTuple(m_indexInfo->attributes, values, isnull, &tupleSize);
    if (unlikely(tupleSize > MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE)) {
        m_indexInfo->extraInfo = tupleSize;
        DstorePfreeExt(*indexTuple);
    }
    if (unlikely(*indexTuple == nullptr)) {
        return DSTORE_FAIL;
    }
    (*indexTuple)->SetHeapCtid(heapCtid);
    (*indexTuple)->SetTdId(INVALID_TD_SLOT);
    (*indexTuple)->SetCcindexStatus(m_indexInfo->btrIdxStatus);
    if (m_needRecordUndo) {
        (*indexTuple)->SetTdStatus(TupleTdStatus::ATTACH_TD_AS_NEW_OWNER);
    } else {
        (*indexTuple)->SetTdStatus(TupleTdStatus::DETACH_TD);
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Form index tuple for heapCtid(%hu, %u %hu)",
        heapCtid->GetFileId(), heapCtid->GetBlockNum(), heapCtid->GetOffset()));

    return DSTORE_SUCC;
}

/* targetBuf and childBuf will be unlocked and released after calling, no matter AddTupleToInternal succeeds or not */
RetStatus BtreeSplit::AddTupleToInternal(IndexTuple *insTuple, OffsetNumber insertOff,
                                         BtrStack stack, BufferDesc *targetBuf, BufferDesc *childBuf)
{
    StorageAssert(insTuple != nullptr);
    StorageAssert(childBuf != INVALID_BUFFER_DESC);
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    WalRecord *walRecord = nullptr;
    WalRecordBtreeInsertOnInternal *walData = nullptr;
    uint32 walDataSize = 0;
    RetStatus ret = DSTORE_SUCC;

    auto *insPage = static_cast<BtrPage *>(targetBuf->GetPage());
    PageId targetPageId = targetBuf->GetPageId();
    uint32 targetPageLevel = insPage->GetLinkAndStatus()->GetLevel();
    bool isSingleInLevel = (insPage->GetLinkAndStatus()->IsLeftmost() && insPage->GetLinkAndStatus()->IsRightmost());
    StorageAssert(!insPage->GetLinkAndStatus()->IsRoot() || isSingleInLevel);

    /* Step 1. Split page if full */
    if (insPage->GetFreeSpaceForInsert() < insTuple->GetSize()) {
        /* Split page and add new indexTuple on either target page or new right page. */
        InitSplittingTarget(targetBuf);
        return BtreeSplit::SplitAndAddDownlink(insTuple, insertOff, stack, childBuf);
    }

    /* Step 2. Add indexTuple to current page */
    if (likely(NeedWal())) {
        /* Allocate wal record space in advance to prevent from missing log of insert parent but
         * updating child's split status. */
        walDataSize = sizeof(WalRecordBtreeInsertOnInternal) + insTuple->GetSize();
        walData = static_cast<WalRecordBtreeInsertOnInternal *>(DstorePalloc(walDataSize));
        if (unlikely(walData == nullptr)) {
            PageId rightPageId = insTuple->GetLowlevelIndexpageLink();
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when allocate wal record space for adding"
                "internal tuple, right btrPage(%d, %u), left btrPage(%d, %u), parent btrPage(%d, %u).",
                rightPageId.m_fileId, rightPageId.m_blockId,
                childBuf->GetPageId().m_fileId, childBuf->GetPageId().m_blockId,
                targetBuf->GetPageId().m_fileId, targetBuf->GetPageId().m_blockId));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            goto RELEASE_AND_RETURN_FAIL;
        }
    }
    if (insPage->AddTuple(insTuple, insertOff, insTuple->GetTdId()) != insertOff) {
        storage_set_error(INDEX_ERROR_ADD_ITEM_FAIL);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to add indexTuple(%d, %u %d) in(%s), segment(%hu, %u) "
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            targetBuf->GetPageId().m_fileId, targetBuf->GetPageId().m_blockId, insertOff, m_indexInfo->indexRelName,
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            BTR_PAGE_HEADER_VAL(insPage), BTR_PAGE_LINK_AND_STATUS_VAL(insPage->GetLinkAndStatus())));
        goto RELEASE_AND_RETURN_FAIL;
    }

    /* Step 3. Update child's split status. */
    UpdateChildSplitStatus(childBuf);

    /* Step 4. Generate Undo record and WAL record */
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(targetBuf));
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(childBuf));
    if (likely(NeedWal())) {
        /* Generate wal recodrd */
        StorageAssert(NeedWal());
        StorageAssert(walData);
        StorageAssert(walDataSize);
        walData->SetHeader(insPage, walDataSize, (insPage->GetWalId() != thrd->m_walWriterContext->GetWalId()),
                           insertOff, targetBuf->GetFileVersion());
        walData->SetTupleData(insTuple);
        walRecord = walData;
        /* There must not have any wal atomic group began */
        StorageReleasePanic(walContext->HasAlreadyBegin(), MODULE_INDEX, ErrMsg("Wal has already atomic began."));
        /* Begin wal atomic group */
        walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
        walContext->RememberPageNeedWal(targetBuf);
        walContext->PutNewWalRecord(walRecord);
        DstorePfree(walRecord);
        InsertChildStatusWal(childBuf);
    }

    m_bufMgr->UnlockAndRelease(childBuf);
    m_bufMgr->UnlockAndRelease(targetBuf);
    /* Update Btree Meta if we've split a fast root (but not the root) */
    if (isSingleInLevel) {
        ret = UpdateBtrMetaLowestSinglePage(targetPageId, targetPageLevel);
    }
    if (likely(NeedWal())) {
        UNUSED_VARIABLE(walContext->EndAtomicWal());
    }
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Finish insert indexTuple(%hu, %u, %hu)", targetPageId.m_fileId, targetPageId.m_blockId,
                  insertOff));
    return DSTORE_SUCC;

RELEASE_AND_RETURN_FAIL:
    m_bufMgr->UnlockAndRelease(targetBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
    m_bufMgr->UnlockAndRelease(childBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
    return DSTORE_FAIL;
}

void BtreeSplit::UpdateChildSplitStatus(BufferDesc *childBuf)
{
    if (childBuf == INVALID_BUFFER_DESC) {
        return;
    }

    /* We've checked page type at the beginning, no need to recheck */
    BtrPage *childPage = static_cast<BtrPage *>(childBuf->GetPage());
    childPage->GetLinkAndStatus()->SetSplitStatus(BtrPageSplitStatus::SPLIT_COMPLETE);
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Finished splitting page {%d, %u} by added downlink to parent",
        childBuf->GetPageId().m_fileId, childBuf->GetPageId().m_blockId));
}

void BtreeSplit::InsertChildStatusWal(BufferDesc *childBuf)
{
    StorageAssert(childBuf != INVALID_BUFFER_DESC);
    /* Do not end atomic group at the end of this function since we also need to update
     * BtrMeta(for insert) or right libling(for split) if needed */
    if (unlikely(!NeedWal())) {
        return;
    }

    /* Write wal record for splitting status change */
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    /* Wal atomic group must have already began */
    StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_INDEX, ErrMsg("Wal has not atomic begin."));
    walContext->RememberPageNeedWal(childBuf);
    BtrPage *childPage = static_cast<BtrPage *>(childBuf->GetPage());
    WalRecordBtreeUpdateSplitStatus walData;
    walData.SetHeader(childPage, (childPage->GetWalId() != walContext->GetWalId()), childBuf->GetFileVersion());
    walContext->PutNewWalRecord(&walData);
}

RetStatus BtreeSplit::SearchBtree(BufferDesc **pageBuf, bool strictlyGreaterThanKey, bool forceUpdate,
                                  bool needWriteLock, bool needCheckCreatedXid)
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeSearchForWriteLatency);

    if (STORAGE_FUNC_FAIL(GetRoot(pageBuf, forceUpdate)) || *pageBuf == INVALID_BUFFER_DESC) {
        /* A new root would be created for an updating process if btree is empty. */
        *pageBuf = INVALID_BUFFER_DESC;
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed %s root for index(%s:%u), segment(%hu, %u), createdXid(%d, %lu)",
            forceUpdate ? "create" : "get", m_indexInfo->indexRelName, m_indexRel->relOid,
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            static_cast<int32>(GetBtreeSmgr()->GetMetaCreateXid().m_zoneId),
            GetBtreeSmgr()->GetMetaCreateXid().m_logicSlotId));
        return DSTORE_FAIL;
    }

    BtreePagePayload pagePayload;
    pagePayload.InitByBuffDesc(*pageBuf);
    LWLockMode access = LW_SHARED;

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Start search btree from root {%d, %u} for insertion",
        pagePayload.GetPageId().m_fileId, pagePayload.GetPageId().m_blockId));

    ClearStack();
    if (pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)) {
        /* Root is leaf. GetRoot always return a root page's buffer descriptor with a read lock.
         * We must hold a write-lock for update on leaf page. */
        access = needWriteLock ? LW_EXCLUSIVE : access;
        *pageBuf = ReleaseOldGetNewBuf(*pageBuf, pagePayload.GetPageId(), access, false);
        if (STORAGE_VAR_NULL(*pageBuf)) {
            return DSTORE_FAIL;
        }
        /* Then init pagePayload using new pageBuf with a write-lock */
        pagePayload.InitByBuffDesc(*pageBuf);
    }

    while (*pageBuf != INVALID_BUFFER_DESC) {
        if (unlikely(needCheckCreatedXid) &&
            unlikely(!BtrPage::IsBtrPageValid((*pageBuf)->GetPage(), GetBtreeSmgr()->GetMetaCreateXid()))) {
            BtrPage *page = static_cast<BtrPage *>((*pageBuf)->GetPage());
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                ErrMsg("BtrPage(%hu, %u) is not valid" BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                (*pageBuf)->GetPageId().m_fileId, (*pageBuf)->GetPageId().m_blockId,
                BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));
            m_bufMgr->UnlockAndRelease(*pageBuf);
            *pageBuf = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        /* Need to step to right one if:
         * 1. page was split and the high key is lower than the scan key we are searching
         * 2. page is recycled or going to be recycled that is unlinked from btree */
        if (STORAGE_FUNC_FAIL(StepRightIfNeeded(pageBuf, access, strictlyGreaterThanKey, needCheckCreatedXid))) {
            /* *pageBuf would be already unlocked and released if StepRightIfNeeded failed */
            *pageBuf = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        pagePayload.InitByBuffDesc(*pageBuf);
        if (pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)) {
            StorageAssert(!needWriteLock || access == LW_EXCLUSIVE);
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                   ErrMsg("Finish btree search, result btrPage at level %u with free space %u, pageId = {%d, %u}",
                          pagePayload.GetLinkAndStatus()->GetLevel(),
                          pagePayload.GetPage()->GetFreeSpace<FreeSpaceCondition::RAW>(),
                          pagePayload.GetPageId().m_fileId, pagePayload.GetPageId().m_blockId));
            break;
        }

        StorageAssert(pagePayload.GetLinkAndStatus()->GetLevel() > 0);

        /* Still on internal page. need to find a downlink to descend to */
        PageId nextPageId = INVALID_PAGE_ID;
        OffsetNumber childOffset = BinarySearchOnPage(pagePayload.GetPage(), strictlyGreaterThanKey);
        if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to binary search btrPage(%d, %u) since compare function returns null.",
                    pagePayload.GetPageId().m_fileId, pagePayload.GetPageId().m_blockId));
            pagePayload.Drop(m_bufMgr, false);
            *pageBuf = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        if (childOffset > pagePayload.GetPage()->GetMaxOffset()) {
            /* The internal page is empty. Need to go ahead to the right page */
            nextPageId = pagePayload.GetLinkAndStatus()->GetRight();
            StorageAssert(nextPageId != INVALID_PAGE_ID);
        } else {
            IndexTuple *childTuple = pagePayload.GetPage()->GetIndexTuple(childOffset);
            if (unlikely(!childTuple->IsPivot())) {
                storage_set_error(INDEX_ERROR_TUPLE_DAMAGED);
                Btree::DumpDamagedTuple(childTuple, pagePayload.GetPage(), childOffset);
                Xid btrCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("IndexTuple is damaged. pdb:%u, index(%s:%u), "
                    "createXid(%d, %lu), segment(%hu, %u), currXid(%d, %lu)",
                    GetPdbId(), m_indexInfo->indexRelName, m_indexRel->relOid,
                    static_cast<int>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                    GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                    static_cast<int>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId));
                pagePayload.Drop(m_bufMgr, false);
                *pageBuf = INVALID_BUFFER_DESC;
                return DSTORE_FAIL;
            }
            nextPageId = childTuple->GetLowlevelIndexpageLink();
            /* Change the read lock to write lock if we're about to reach to leaf level */
            access = (pagePayload.GetLinkAndStatus()->GetLevel() == 1 && needWriteLock) ? LW_EXCLUSIVE : LW_SHARED;
            m_leafStack = BtrStackData::SaveNewStack(pagePayload.GetPageId(), childOffset, m_leafStack);
            if (unlikely(m_leafStack == nullptr)) {
                m_bufMgr->UnlockAndRelease(*pageBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
                *pageBuf = INVALID_BUFFER_DESC;
                return DSTORE_FAIL;
            }
        }
        if (unlikely(!nextPageId.IsValid())) {
            storage_set_error(INDEX_ERROR_MOVE_END, m_indexInfo->indexRelName);
            Xid btrCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Right page is invalid. "
                "pdb:%u, index(%s:%u), createXid(%d, %lu), segment(%hu, %u), currXid(%d, %lu)"
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                GetPdbId(), m_indexInfo->indexRelName, m_indexRel->relOid,
                static_cast<int>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                static_cast<int>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
                BTR_PAGE_HEADER_VAL(pagePayload.GetPage()),
                BTR_PAGE_LINK_AND_STATUS_VAL(pagePayload.GetLinkAndStatus())));
            pagePayload.Drop(m_bufMgr);
            *pageBuf = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        *pageBuf = ReleaseOldGetNewBuf(*pageBuf, nextPageId, access, false);
        if (STORAGE_VAR_NULL(*pageBuf)) {
            return DSTORE_FAIL;
        }
    }

    if (!m_keepStackAfterSearch) {
        ClearStack();
    }
    StorageAssert(pagePayload.GetLinkAndStatus()->IsRightmost() ||
                  CompareKeyToTuple(pagePayload.GetPage(), pagePayload.GetLinkAndStatus(), BTREE_PAGE_HIKEY,
                                    pagePayload.GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)) <
                      (strictlyGreaterThanKey ? 0 : 1));
    return DSTORE_SUCC;
}

RetStatus BtreeSplit::StepRightIfNeeded(BufferDesc **pageBuf, LWLockMode access, bool strictlyGreaterThanKey,
                                        bool needCheckCreatedXid)
{
    StorageAssert(*pageBuf != INVALID_BUFFER_DESC);

    BtreePagePayload currPage;
    int cmpRet = 0;
    int goRightVal = strictlyGreaterThanKey ? 0 : 1;
    while (*pageBuf != INVALID_BUFFER_DESC) {
        LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_stepRightLatency);
        if (unlikely(needCheckCreatedXid) &&
            unlikely(!BtrPage::IsBtrPageValid((*pageBuf)->GetPage(), GetBtreeSmgr()->GetMetaCreateXid()))) {
            BtrPage *page = static_cast<BtrPage *>((*pageBuf)->GetPage());
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                ErrMsg("BtrPage(%hu, %u) is not valid" BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                (*pageBuf)->GetPageId().m_fileId, (*pageBuf)->GetPageId().m_blockId,
                BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));
            m_bufMgr->UnlockAndRelease(*pageBuf);
            *pageBuf = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        currPage.InitByBuffDesc(*pageBuf);
        BtrPageLinkAndStatus *linkAndStatus = currPage.GetLinkAndStatus();
        if (linkAndStatus->IsRightmost()) {
            /* reached the end of this level */
            break;
        }

        /* Finish splitting if incomplete */
        if (unlikely(!linkAndStatus->IsSplitComplete())) {
            PageId splitTargetPageId = currPage.GetPageId();
            if (STORAGE_FUNC_FAIL(CompleteSplit(*pageBuf, m_leafStack, access))) {
                return DSTORE_FAIL;
            }
            /* buffer is released after splitting, get buffer again */
            *pageBuf = Btree::ReadAndCheckBtrPage(splitTargetPageId, access, m_bufMgr, this->GetPdbId(), false);
            if (STORAGE_VAR_NULL(*pageBuf)) {
                return DSTORE_FAIL;
            }
            continue;
        }

        if (!linkAndStatus->IsUnlinked()) {
            cmpRet = CompareKeyToTuple(currPage.GetPage(), linkAndStatus, BTREE_PAGE_HIKEY,
                                       linkAndStatus->TestType(BtrPageType::INTERNAL_PAGE));
            if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to compare tuple since compare function returns null."));
                currPage.Drop(m_bufMgr);
                *pageBuf = INVALID_BUFFER_DESC;
                return DSTORE_FAIL;
            }
            if (cmpRet < goRightVal) {
                /* Found! */
                break;
            }
        }

        /* recycled or going to be recycled if page is unlinked */
        /* split & target key is on right page if compare result >= goRightVal */
        PageId rightPageId = currPage.GetLinkAndStatus()->GetRight();
        if (unlikely(!rightPageId.IsValid())) {
            storage_set_error(INDEX_ERROR_MOVE_END, m_indexInfo->indexRelName);
            Xid btrCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Right page is invalid when BtreeSplit::StepRightIfNeeded. "
                "pdb:%u, index(%s:%u), createXid(%d, %lu), segment(%hu, %u), currXid(%d, %lu)"
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                GetPdbId(), m_indexInfo->indexRelName, m_indexRel->relOid,
                static_cast<int>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                static_cast<int>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
                BTR_PAGE_HEADER_VAL(currPage.GetPage()), BTR_PAGE_LINK_AND_STATUS_VAL(currPage.GetLinkAndStatus())));
            currPage.Drop(m_bufMgr);
            *pageBuf = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("Step right from {%d, %u} to {%d, %u} when SearchBtree", (*pageBuf)->GetPageId().m_fileId,
                      (*pageBuf)->GetPageId().m_blockId, rightPageId.m_fileId, rightPageId.m_blockId));

        *pageBuf = ReleaseOldGetNewBuf(currPage.GetBuffDesc(), rightPageId, access, false);
        if (STORAGE_VAR_NULL(*pageBuf)) {
            return DSTORE_FAIL;
        }
    }

    if (unlikely(currPage.GetLinkAndStatus()->IsUnlinked())) {
        storage_set_error(INDEX_ERROR_MOVE_END, m_indexInfo->indexRelName);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Reach end of of btree level when SearchBtree(%s), segment(%hu, %u) "
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, m_indexInfo->indexRelName,
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            BTR_PAGE_HEADER_VAL(currPage.GetPage()),
            BTR_PAGE_LINK_AND_STATUS_VAL(currPage.GetLinkAndStatus())));
        m_bufMgr->UnlockAndRelease(*pageBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

BufferDesc *BtreeSplit::GetParentBufDesc(const PageId childPageId, BtrStack stack, LWLockMode access)
{
    PageId parentPageId = stack->currItem.GetPageId();
    OffsetNumber origDownlinkOffset = stack->currItem.GetOffset();
    OffsetNumber start = origDownlinkOffset;
    int stepCounter = 1;
    while (parentPageId.IsValid()) {
        BtreePagePayload parent;
        if (STORAGE_FUNC_FAIL(parent.Init(this->GetPdbId(), parentPageId, access, m_bufMgr))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to get parent for btrPage(%hu, %u), parent btrPage(%hu, %u)",
                    childPageId.m_fileId, childPageId.m_blockId, parentPageId.m_fileId, parentPageId.m_blockId));
            return INVALID_BUFFER_DESC;
        }
        /* Finish splitting if incomplete */
        if (!parent.GetLinkAndStatus()->IsSplitComplete()) {
            if (STORAGE_FUNC_FAIL(CompleteSplit(parent.GetBuffDesc(), stack->parentStack, access))) {
                ErrLog(DSTORE_WARNING, MODULE_INDEX,
                    ErrMsg("Failed to get parent for btrPage(%hu, %u) since fail to complete split btrPage(%hu, %u)",
                    childPageId.m_fileId, childPageId.m_blockId, parentPageId.m_fileId, parentPageId.m_blockId));
                break;
            }
            /* buffer is released after splitting */
            continue;
        }

        if (unlikely(parent.GetLinkAndStatus()->IsUnlinked())) {
            /* Skip unlinked pages */
            ErrLog(DSTORE_WARNING, MODULE_INDEX, ErrMsg("Skip unlinked btrPage(%hu, %u) while Get Parent, plsn: %lu",
                parentPageId.m_fileId, parentPageId.m_blockId, parent.GetPage()->GetPlsn()));
            parentPageId = parent.GetLinkAndStatus()->GetRight();
            start = INVALID_ITEM_OFFSET_NUMBER;
            parent.Drop(m_bufMgr);
            continue;
        }

        OffsetNumber minOffsetNumber = parent.GetLinkAndStatus()->GetFirstDataOffset();
        OffsetNumber maxOffsetNumber = parent.GetPage()->GetMaxOffset();
        /* 1. start = INVALID_ITEM_OFFSET_NUMBER, need to search the whole page */
        /* 2. The page is rightmost now while it wasn't before. start was BTREE_PAGE_HIKEY, but now we should
            *    begin searching from BTREE_PAGE_FIRSTKEY */
        start = start < minOffsetNumber ? minOffsetNumber : start;
        for (OffsetNumber i = start; i <= maxOffsetNumber; i = OffsetNumberNext(i)) {
            if (parent.GetPage()->GetIndexTuple(i)->GetLowlevelIndexpageLink() == childPageId) {
                /* Found the parent. Update links in stack */
                stack->currItem.SetPageId(parentPageId);
                stack->currItem.SetOffset(i);
                return parent.GetBuffDesc();
            }
        }
        /* Page might also be split that start > masOffsetNumber. Need to start from the last indextuple */
        start = start > maxOffsetNumber ? maxOffsetNumber : OffsetNumberPrev(start);
        for (OffsetNumber i = start; i >= minOffsetNumber; i = OffsetNumberPrev(i)) {
            if (parent.GetPage()->GetIndexTuple(i)->GetLowlevelIndexpageLink() == childPageId) {
                /* Found the parent. Update links in stack */
                stack->currItem.SetPageId(parentPageId);
                stack->currItem.SetOffset(i);
                return parent.GetBuffDesc();
            }
        }
        /* Didn't find parent on the page. May be moved right */
        parentPageId = parent.GetLinkAndStatus()->GetRight();
        start = INVALID_ITEM_OFFSET_NUMBER;
        ErrLog(DSTORE_WARNING, MODULE_INDEX,
               ErrMsg("Step to btrPage(%hu, %u) from btrPage(%hu, %u) glsn: %lu plsn: %lu while Get Parent for "
                      "btrPage(%hu, %u), stepCounter:%d", parentPageId.m_fileId, parentPageId.m_blockId,
                      parent.GetPageId().m_fileId, parent.GetPageId().m_blockId,
                      parent.GetPage()->GetGlsn(), parent.GetPage()->GetPlsn(),
                      childPageId.m_fileId, childPageId.m_blockId, stepCounter));
        parent.Drop(m_bufMgr);
        if (unlikely(parentPageId == INVALID_PAGE_ID)) {
            break;
        }
        stepCounter++;
    }
    ErrLog(DSTORE_ERROR, MODULE_INDEX,
        ErrMsg("Failed to GetParent for btrPage(%hu, %u). Traversed from btrPage(%hu, %u) to btrPage(%hu, %u), "
        "stepped %d pages in(%s), segment(%hu, %u)", childPageId.m_fileId, childPageId.m_blockId,
        stack->currItem.GetPageId().m_fileId, stack->currItem.GetPageId().m_blockId,
        parentPageId.m_fileId, parentPageId.m_blockId, stepCounter, m_indexInfo->indexRelName,
        GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId));
    return INVALID_BUFFER_DESC;
}

BufferDesc *BtreeSplit::GetParentBufDescFromRoot(const PageId targetPageId, uint32 targetLevel, OffsetNumber &childPos)
{
    /* get root from the real meta page */
    BufferDesc *btrMetaBuf = INVALID_BUFFER_DESC;
    BtrMeta *btrMeta = GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    if (unlikely(btrMeta == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Can not get btr meta when GetParentBufDescFromRoot"));
        return btrMetaBuf;
    }
    PageId rootPageId = btrMeta->GetRootPageId();
    uint32 rootLevel = btrMeta->GetRootLevel();
    m_bufMgr->UnlockAndRelease(btrMetaBuf);

    StorageAssert(rootLevel > targetLevel);
    LWLockMode access = (rootLevel == targetLevel + 1) ? LW_EXCLUSIVE : LW_SHARED;
    BtreePagePayload currPage;
    if (STORAGE_FUNC_FAIL(currPage.Init(this->GetPdbId(), rootPageId, access, m_bufMgr))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to get parent for btrPage(%hu, %u), root btrPage(%hu, %u)",
            targetPageId.m_fileId, targetPageId.m_blockId, rootPageId.m_fileId, rootPageId.m_blockId));
        return INVALID_BUFFER_DESC;
    }
    uint32 currLevel = rootLevel;
    int stepCounter = 1;
    int cmpRet = 1;
    while (currLevel > targetLevel) {
        PageId nextPageId;
        while (!currPage.GetLinkAndStatus()->IsRightmost()) {
            cmpRet = 1;
            if (!currPage.GetLinkAndStatus()->IsUnlinked()) {
                cmpRet = CompareKeyToTuple(currPage.GetPage(), currPage.GetLinkAndStatus(), BTREE_PAGE_HIKEY,
                                           currPage.GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE));
            }
            if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to compare tuple since compare function returns null."));
                currPage.Drop(m_bufMgr);
                return INVALID_BUFFER_DESC;
            }
            if (cmpRet < 1) {
                break;
            }
            /* Step right if page is unlinked of hikey is smaller than searching target */
            nextPageId = currPage.GetLinkAndStatus()->GetRight();
            StorageReleasePanic(!nextPageId.IsValid(), MODULE_INDEX,
                ErrMsg("Failed to get parent for {%hu, %u}", targetPageId.m_fileId, targetPageId.m_blockId));
            currPage.Drop(m_bufMgr);
            if (STORAGE_FUNC_FAIL(currPage.Init(this->GetPdbId(), nextPageId, access, m_bufMgr))) {
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to get parent for btrPage(%hu, %u), next btrPage(%hu, %u), currLevel:%u",
                    targetPageId.m_fileId, targetPageId.m_blockId, nextPageId.m_fileId, nextPageId.m_blockId,
                    currLevel));
                return INVALID_BUFFER_DESC;
            }
        }

        OffsetNumber childOffset = BinarySearchOnPage(currPage.GetPage());
        if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to binary search btrPage(%d, %u) since compare function returns null.",
                    currPage.GetPageId().m_fileId, currPage.GetPageId().m_blockId));
            currPage.Drop(m_bufMgr, false);
            return INVALID_BUFFER_DESC;
        }
        IndexTuple *childTuple = currPage.GetPage()->GetIndexTuple(childOffset);
        if (unlikely(!childTuple->IsPivot())) {
            storage_set_error(INDEX_ERROR_TUPLE_DAMAGED);
            Btree::DumpDamagedTuple(childTuple, currPage.GetPage(), childOffset);
            Xid btrCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("IndexTuple is damaged. pdb:%u, index(%s:%u), "
                "createXid(%d, %lu), segment(%hu, %u), currXid(%d, %lu)",
                GetPdbId(), m_indexInfo->indexRelName, m_indexRel->relOid,
                static_cast<int>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                static_cast<int>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId));
            currPage.Drop(m_bufMgr, false);
            return INVALID_BUFFER_DESC;
        }
        PageId childPageId = childTuple->GetLowlevelIndexpageLink();

        if (currLevel == targetLevel + 1) {
            if (childPageId == targetPageId) {
                /* Found parent */
                childPos = childOffset;
                break;
            }
            /* We've reached the target level, step right */
            nextPageId = currPage.GetLinkAndStatus()->GetRight();
            ErrLog(DSTORE_WARNING, MODULE_INDEX,
                   ErrMsg("Step to btrPage(%hu, %u) from btrPage(%hu, %u) glsn: %lu plsn: %lu while Get Parent for "
                          "btrPage(%hu, %u) from root, stepCounter:%d", nextPageId.m_fileId, nextPageId.m_blockId,
                          currPage.GetPageId().m_fileId, currPage.GetPageId().m_blockId,
                          currPage.GetPage()->GetGlsn(), currPage.GetPage()->GetPlsn(),
                          targetPageId.m_fileId, targetPageId.m_blockId, stepCounter));
            stepCounter++;
        } else {
            /* Go down to next level */
            nextPageId = childPageId;
            if (--currLevel == targetLevel + 1) {
                access = LW_EXCLUSIVE;
            }
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                   ErrMsg("Go down to {%hu, %u} from {%hu, %u} plsn: %lu while Get Parent for {%hu, %u}",
                          nextPageId.m_fileId, nextPageId.m_blockId,
                          currPage.GetPageId().m_fileId, currPage.GetPageId().m_blockId, currPage.GetPage()->GetPlsn(),
                          targetPageId.m_fileId, targetPageId.m_blockId));
        }

        currPage.Drop(m_bufMgr);
        if (unlikely(nextPageId == INVALID_PAGE_ID) ||
            STORAGE_FUNC_FAIL(currPage.Init(this->GetPdbId(), nextPageId, access, m_bufMgr))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Failed to get parent for btrPage(%hu, %u), next btrPage(%hu, %u), currLevel:%u",
                targetPageId.m_fileId, targetPageId.m_blockId, nextPageId.m_fileId, nextPageId.m_blockId, currLevel));
            return INVALID_BUFFER_DESC;
        }
    }
    return currPage.GetBuffDesc();
}

RetStatus BtreeSplit::SplitAndAddDownlink(IndexTuple *insTuple, OffsetNumber insertOff,
                                          BtrStack stack, BufferDesc *childBuf)
{
    auto *insPage = static_cast<BtrPage *>(m_splitBuf->GetPage());
    PageId targetPageId = m_splitBuf->GetPageId();
    bool isLeaf = insPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE);
    if (m_tdContext.allocTd.isDirty) {
        /*
         * SplitPage will reset m_tdContext if we still need to allocate a TD slot, then all tdAlloc info we have
         * now will be lost.
         * Also if SplitPage failed, there's no way to rollback the changes we have made on TDs.
         * Thus we need to write WAL log for AllocTD before splitting.
         */
        (void)m_bufMgr->MarkDirty(m_splitBuf);
        GenerateAllocTdWal(m_splitBuf, m_tdContext);
    }
    if (unlikely(m_tdContext.allocTd.hasRollbackTd)) {
        (void)m_bufMgr->MarkDirty(m_splitBuf);
        GenerateRollbackTdWal(m_splitBuf, m_tdContext);
    }

    /* Split page and add new indexTuple on either target page or new right page. */
    if (STORAGE_FUNC_FAIL(BtreeSplit::SplitPage(insTuple, insertOff, childBuf))) {
        m_bufMgr->UnlockAndRelease(m_splitBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        m_splitBuf = INVALID_BUFFER_DESC;

        /* fail on td allocation, wait for any transaction on page to finish, then try again. */
        if (isLeaf && m_tdContext.NeedRetryAllocTd() && STORAGE_FUNC_FAIL(WaitTxnEndForTdRealloc())) {
            /* Something wrong when waiting for other transaction end. Give up retry and return fail */
            m_needRetrySearchBtree = false;
        }
        return DSTORE_FAIL;
    }
    /* Note: for now, splitBuf is still write-locked after SplitPage. */
    /*
     * New right page is unlocked and released after SplitPage. It's safe because
     *  1. Splitting target -- its left sibling -- is still write locked that no one can step right to it
     *  2. Parent page doesn't have a downlink to it yet that no one can access it by descending the btree
     */
    if (STORAGE_FUNC_FAIL(AddPageDownlinkToParent(stack, insPage->GetLinkAndStatus()->IsRoot()))) {
        /* splitBuf is already unlocked and released. */
        return DSTORE_FAIL;
    }
    /* Note: for now, we're not holding any locks on splitBuf. */
#ifndef UT
    bool needRecycleBackground = !GetBtreeSmgr()->IsGlobalTempIndex() &&
                                 g_storageInstance->GetGuc()->numObjSpaceMgrWorkers > 0;
    if (needRecycleBackground) {
        if (STORAGE_FUNC_FAIL(BtreePageRecycle::TryRegisterRecycleBtreeTask(GetBtreeSmgr()->GetMetaCreateXid(),
            m_indexInfo, m_scanKeyValues.scankeys, GetBtreeSmgr()->GetTablespaceId(),
            GetBtreeSmgr()->GetSegMetaPageId(), GetBtreeSmgr()->m_pdbId))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Failed to register btree Task for SegMetaPageId ({%hu, %u}) with tablespace id (%hu)",
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                GetBtreeSmgr()->GetTablespaceId()));
            return DSTORE_FAIL;
        }
    } else {
        BtreePageRecycle recycle(m_indexRel);
        if (STORAGE_FUNC_FAIL(recycle.BatchRecycleBtreePage(m_indexInfo, m_scanKeyValues.scankeys))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Failed to recycle btree for SegMetaPageId ({%hu, %u}) with tablespace id (%hu)",
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                GetBtreeSmgr()->GetTablespaceId()));
            return DSTORE_FAIL;
        }
    }
#endif
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("Finish insert index tuple. Original target {%d, %u} %d and target has been split",
        targetPageId.m_fileId, targetPageId.m_blockId, insertOff));
    return DSTORE_SUCC;
}

RetStatus BtreeSplit::PrepareSplittingAndRightPage(BtrPage *leftPage, IndexTuple *insTuple, SplitContext &splitCxt,
                                                   BufferDesc **oldRBuf)
{
    BtrPage *splitPage = static_cast<BtrPage *>(m_splitBuf->GetPage());
    BtrPageLinkAndStatus *splitLinkStat = splitPage->GetLinkAndStatus();
    OffsetNumber targetOff = splitLinkStat->GetFirstDataOffset();
    bool isSameWithLastLeft = false;

    /* Step 1. Add high key to splitting left according to SplitContext */
    if (STORAGE_FUNC_FAIL(AddHikeyToTempSplitLeftPage(leftPage, insTuple, splitCxt, isSameWithLastLeft))) {
        return DSTORE_FAIL;
    }

    /* Step 2. Create a new page as splitting right page and initialize page info */
    if (STORAGE_FUNC_FAIL(CreateSplitRightPageWithHikey(leftPage))) {
        SetErrorAndPutFailedPageBackToFreePageQueue(INDEX_ERROR_FAIL_ADD_HIGH_KEY_TO_RIGHT);
        return DSTORE_FAIL;
    }

    /* Step 3. Copy Items from splitting target page to new pages, and add insertion tuple to correct page */
    /* Copy items to left page */
    if (STORAGE_FUNC_FAIL(splitPage->CopyItemsFromSplitPage(targetOff, leftPage, insTuple, true, splitCxt))) {
        SetErrorAndPutFailedPageBackToFreePageQueue(INDEX_ERROR_FAIL_ADD_NEW_ITEM_TO_LEFT);
        return DSTORE_FAIL;
    }

    /* Copy items to right page */
    if (STORAGE_FUNC_FAIL(splitPage->CopyItemsFromSplitPage(targetOff, static_cast<BtrPage *>(m_newRightBuf->GetPage()),
                                                            insTuple, false, splitCxt, isSameWithLastLeft))) {
        SetErrorAndPutFailedPageBackToFreePageQueue(INDEX_ERROR_FAIL_ADD_NEW_ITEM_TO_RIGHT);
        return DSTORE_FAIL;
    }

    /* Step 4. Link new right page to original right page */
    if (STORAGE_FUNC_FAIL(LinkNewRightToOrigRightIfNeeded(oldRBuf))) {
        SetErrorAndPutFailedPageBackToFreePageQueue(INDEX_ERROR_RIGHT_LEFT_LINK_NOT_MATCH);
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus BtreeSplit::SplitPage(IndexTuple *insTuple, OffsetNumber insOff, BufferDesc *childBuf)
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreePageSplitLatency);

    BtrPage *splitPage = static_cast<BtrPage *>(m_splitBuf->GetPage());
    BtrPageLinkAndStatus *splitLinkStat = splitPage->GetLinkAndStatus();

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Going to split %s%s page {%u}, left{%u}, origRight{%u}",
        splitLinkStat->IsRightmost() ? "rightmost " : "",
        splitLinkStat->TestType(BtrPageType::LEAF_PAGE) ? "leaf" : "internal",
        m_splitBuf->GetPageId().m_blockId, splitLinkStat->GetLeft().m_blockId, splitLinkStat->GetRight().m_blockId));

    BtreeSplitStrategy btreeSplitStrategy(m_indexRel, m_indexInfo, splitPage, insOff, insTuple);
    SplitContext splitCxt{insOff, INVALID_OFFSET, true};
    OffsetNumber insOffOnPage = insOff;

    /* Step 1. Create a memory page as temporary splitting left and retry alloccTD if failed earlier */
    BtrPage leftPage;
    leftPage.InitMemLeftForSplit(splitPage);
    uint8 addedTdCount = 0;
    BufferDesc *oldRBuf = INVALID_BUFFER_DESC;
    if (m_needRecordUndo && splitLinkStat->TestType(BtrPageType::LEAF_PAGE) && insTuple->GetTdId() == INVALID_TD_SLOT) {
        addedTdCount = RetryAllocAndSetTdWhenSplit(&leftPage, insTuple);
        if (addedTdCount > EXTEND_TD_NUM) {
            goto CLEAN_AND_RETURN_FAIL;
        }
    }
    StorageAssert(insTuple->IsPivot() || !m_needRecordUndo || insTuple->GetTdId() != INVALID_TD_SLOT);

    /* Step 2. Find split point */
    if (STORAGE_FUNC_FAIL(btreeSplitStrategy.InitSplitPoint(addedTdCount))) {
        goto CLEAN_AND_RETURN_FAIL;
    }
    FAULT_INJECTION_CALL_REPLACE(DstoreIndexFI::SET_SPLIT_POINT, &splitCxt)
    splitCxt.firstRightOff = btreeSplitStrategy.FindSplitLoc(&splitCxt.insertOnLeft);
    FAULT_INJECTION_CALL_REPLACE_END;
    if (splitCxt.firstRightOff == INVALID_ITEM_OFFSET_NUMBER) {
        /* Could not get a feasible split point due to TD allocation. Try again
         * to wait for some in-progress transaction ends then retry insertion. */
        if (m_retryCount % MAX_RETRY_COUNT == 0) {
            storage_set_error(INDEX_ERROR_COULD_NOT_FIND_FEASIBLE_SPLIT_POINT, m_indexInfo->indexRelName);
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Could not find a feasible split point for index \"%s\"."
                "splitPage tdCound: %hhu, leftPage tdCound: %hhu,"
                "freespace: %u, tuple size %u, addedTdCount %hhu, m_retryCount %hu,"
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                m_indexInfo->indexRelName, splitPage->GetTdCount(), (&leftPage)->GetTdCount(),
                splitPage->GetFreeSpaceForInsert(), insTuple->GetSize(), addedTdCount, m_retryCount,
                BTR_PAGE_HEADER_VAL(splitPage), BTR_PAGE_LINK_AND_STATUS_VAL(splitPage->GetLinkAndStatus())));
        }
        if (m_retryCount != MAX_RETRY_COUNT) {
            GaussUsleep(10000); /* 10 ms */
            m_needRetrySearchBtree = true;
            m_retryCount++;
        }
        goto CLEAN_AND_RETURN_FAIL;
    }

    /* Step 3. Prepare splitting left and right pages
     * 1. Copy index tuples to splitting left and right page according to split context
     * 2. Link new right page to the old right */
    if (STORAGE_FUNC_FAIL(PrepareSplittingAndRightPage(&leftPage, insTuple, splitCxt, &oldRBuf))) {
        goto CLEAN_AND_RETURN_FAIL;
    }

    insOffOnPage = splitCxt.insertOff; /* Record the actual insert offset on page for later undo record generation */
    splitCxt.insertOff = insOff; /* Restore the original insert offset for later wal record generation */

    /* Step 4. Update child's split status if splitting internal page */
    UpdateChildSplitStatus(childBuf);

    /* Step 5 Write Redo logs and Undo log if leaf, OR write Redo logs only for internal page */
    if (STORAGE_FUNC_FAIL(splitLinkStat->TestType(BtrPageType::LEAF_PAGE) ?
                          InsertUndoAndWalForSplitLeaf(insTuple, splitCxt, insOffOnPage, oldRBuf, &leftPage) :
                          InsertOnlyWalForSplitPage(insTuple, splitCxt, oldRBuf, childBuf, &leftPage))) {
        SetErrorAndPutFailedPageBackToFreePageQueue(0); /* We've already set error code. Do not recover it here */
        goto CLEAN_AND_RETURN_FAIL;
    }

    /* Step 6. Clear resources */
    ClearPagesAfterSplit(childBuf, oldRBuf, BufferPoolUnlockContentFlag());
    /* Update liveStatus of the new right page. It is possible that the splitting page is already empty and in the
     * recyclable queue. Update the liveStatus flag if the insertion is not on the new right page. */
    if (splitCxt.insertOnLeft) {
        BtreePageRecycle recycle(m_indexRel);
        /* PutIntoRecycleQueueIfEmpty would unlock and release m_newRightBuf no matter succeeded or not */
        (void)recycle.PutIntoRecycleQueueIfEmpty(m_newRightBuf);
    } else {
        m_bufMgr->UnlockAndRelease(m_newRightBuf);
    }
    m_newRightBuf = INVALID_BUFFER_DESC;

    if (unlikely(static_cast<uint64>(g_traceSwitch) & BTREE_STATISTIC_INFO_MIN_TRACE_LEVEL)) {
        GetBtreeSmgr()->RecordBtreeOperInLevel(BtreeOperType::BTR_OPER_SPLIT_WHEN_INSERT,
                                               splitLinkStat->GetLevel());
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("Split %s {%hu, %u} at %hu new right {%hu, %u} insert %d on %s td(%hhu), snapshot csn %lu",
        splitLinkStat->TestType(BtrPageType::LEAF_PAGE) ? "leaf" : "internal", m_splitBuf->GetPageId().m_fileId,
        m_splitBuf->GetPageId().m_blockId, splitCxt.firstRightOff, splitPage->GetRight().m_fileId,
        splitPage->GetRight().m_blockId, splitCxt.insertOff, splitCxt.insertOnLeft ? "left" : "right",
        insTuple->GetTdId(), thrd->GetSnapShotCsn()));

    /* Note: we will hold write lock for splitBuf! */
    return DSTORE_SUCC;

CLEAN_AND_RETURN_FAIL:
    m_tdContext.allocTd.isDirty = false;
    ClearPagesAfterSplit(childBuf, oldRBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
    return DSTORE_FAIL;
}

RetStatus BtreeSplit::LinkNewRightToOrigRightIfNeeded(BufferDesc **oldRightBuf)
{
    BtrPage *newRightPage = static_cast<BtrPage *>(m_newRightBuf->GetPage());
    if (newRightPage->GetLinkAndStatus()->IsRightmost()) {
        /* Nothing to update for rightmost page */
        *oldRightBuf = INVALID_BUFFER_DESC;
        return DSTORE_SUCC;
    }

    /* It's safe to lock the original right sibling page without deadlock risk here because we always lock pages in
     * the order of left to right. */
    PageId oldRightPageId = newRightPage->GetRight();
    *oldRightBuf = Btree::ReadAndCheckBtrPage(oldRightPageId, LW_EXCLUSIVE, m_bufMgr, GetPdbId());
    if (STORAGE_VAR_NULL(*oldRightBuf)) {
        BtrPage *splitPage = static_cast<BtrPage *>(m_splitBuf->GetPage());
        ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("Failed to read oldRight. pdb:%u, old right btrPage(%hu, %u), new right btrPage(%hu, %u), splitting:"
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                GetPdbId(), oldRightPageId.m_fileId, oldRightPageId.m_blockId,
                m_newRightBuf->GetPageId().m_fileId, m_newRightBuf->GetPageId().m_blockId,
                BTR_PAGE_HEADER_VAL(splitPage), BTR_PAGE_LINK_AND_STATUS_VAL(splitPage->GetLinkAndStatus())));
    } else {
        BtrPage *oldRightPage = static_cast<BtrPage *>((*oldRightBuf)->GetPage());
        /* Check correctness of page link */
        if (likely(oldRightPage->GetLeft() == newRightPage->GetLeft())) {
            /* Update left link of original right page */
            oldRightPage->GetLinkAndStatus()->SetLeft(m_newRightBuf->GetPageId());

            /* Do not release oldRightBuf now because we need to write wal record for oldRightBuf later */
            return DSTORE_SUCC;
        }

        BtrPage *splitPage = static_cast<BtrPage *>(m_splitBuf->GetPage());
        PageId origRightLeft = oldRightPage->GetLeft();
        PageId newRightLeft = newRightPage->GetLeft();
        char *clusterBufferInfo = m_bufMgr->GetClusterBufferInfo(GetPdbId(),
                                                                oldRightPageId.m_fileId, oldRightPageId.m_blockId);
        if (likely(clusterBufferInfo != nullptr)) {
            ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
            DstorePfreeExt(clusterBufferInfo);
        }
        ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("Mismatch origRight btrPage(%hu, %u) with left btrPage(%hu, %u) and newRight btrPage(%hu, %u) with "
                    "left btrPage(%hu, %u). origRight has bufferdesc status = %lu, responseType = %d, pdb:%u"
                    "origRight: " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT
                    "splitPage: " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                    (*oldRightBuf)->GetPageId().m_fileId, (*oldRightBuf)->GetPageId().m_blockId, origRightLeft.m_fileId,
                    origRightLeft.m_blockId, m_newRightBuf->GetPageId().m_fileId, m_newRightBuf->GetPageId().m_blockId,
                    newRightLeft.m_fileId, newRightLeft.m_blockId, (*oldRightBuf)->GetState(),
                    m_bufMgr->GetBufDescResponseType(*oldRightBuf), GetPdbId(),
                    BTR_PAGE_HEADER_VAL(oldRightPage), BTR_PAGE_LINK_AND_STATUS_VAL(oldRightPage->GetLinkAndStatus()),
                    BTR_PAGE_HEADER_VAL(splitPage), BTR_PAGE_LINK_AND_STATUS_VAL(splitPage->GetLinkAndStatus())));

        if (origRightLeft.IsValid()) {
            /* We should always access pages in the order of left to right. Access a page when we hold it's right
                * sibling's lock may cause dead lock with others. */
            m_bufMgr->UnlockAndRelease(*oldRightBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
            BufferDesc *oriRightLeftBuf =
                Btree::ReadAndCheckBtrPage(origRightLeft, LW_SHARED, m_bufMgr, this->GetPdbId(), false);
            if (!STORAGE_VAR_NULL(oriRightLeftBuf)) {
                BtrPage *oriRightLeftPage = static_cast<BtrPage *>(oriRightLeftBuf->GetPage());
                ErrLog(DSTORE_LOG, MODULE_INDEX,
                        ErrMsg("Left page info of origin right page: bufferdesc status = %lu, responseType = "
                                "%d, " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                                oriRightLeftBuf->GetState(), m_bufMgr->GetBufDescResponseType(oriRightLeftBuf),
                                BTR_PAGE_HEADER_VAL(oriRightLeftPage),
                                BTR_PAGE_LINK_AND_STATUS_VAL(oriRightLeftPage->GetLinkAndStatus())));
            }
            clusterBufferInfo = m_bufMgr->GetClusterBufferInfo(GetPdbId(),
                                                            origRightLeft.m_fileId, origRightLeft.m_blockId);
            if (likely(clusterBufferInfo != nullptr)) {
                ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
                DstorePfreeExt(clusterBufferInfo);
            }
        }
    }
    ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("LinkNewRightToOrigRightIfNeeded failed."));
    return DSTORE_FAIL;
}

RetStatus BtreeSplit::AddHikeyToTempSplitLeftPage(BtrPage *leftPage, IndexTuple *insTuple,
                                                  const SplitContext splitContext, bool &isSameWithLastLeft)
{
    auto *splitPage = static_cast<BtrPage *>(m_splitBuf->GetPage());
    BtrPageLinkAndStatus *splitLinkStat = splitPage->GetLinkAndStatus();

    IndexTuple *leftHikey = CreateSplittingLeftHikey(splitPage, insTuple, splitContext, isSameWithLastLeft);
    if (unlikely(leftHikey == nullptr)) {
        return DSTORE_FAIL;
    }
    if (unlikely(leftPage->AddTuple(leftHikey, BTREE_PAGE_HIKEY, INVALID_TD_SLOT) != BTREE_PAGE_HIKEY)) {
        storage_set_error(INDEX_ERROR_ADD_ITEM_FAIL);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to add indexTuple(%d, %u %d) in(%s), segment(%hu, %u) "
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, m_splitBuf->GetPageId().m_fileId,
            m_splitBuf->GetPageId().m_blockId, BTREE_PAGE_HIKEY, m_indexInfo->indexRelName,
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            BTR_PAGE_HEADER_VAL(splitPage), BTR_PAGE_LINK_AND_STATUS_VAL(splitLinkStat)));
        return DSTORE_FAIL;
    }
    if (splitLinkStat->TestType(BtrPageType::LEAF_PAGE)) {
        DstorePfree(leftHikey);
    }
    return DSTORE_SUCC;
}

RetStatus BtreeSplit::CreateSplitRightPageWithHikey(BtrPage *tempLeft)
{
    BtreePagePayload newRight;
    auto *splitPage = static_cast<BtrPage *>(m_splitBuf->GetPage());
    /* Get a new page from btree storage manager */
    if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->GetNewPage(newRight, tempLeft->GetTdCount()))) {
        ErrLog(
            DSTORE_ERROR, MODULE_INDEX,
            ErrMsg(
                "[%s] get new page faild for btree with Meta btrPage(%d, %u) created xid(%d, %lu)", __FUNCTION__,
                GetBtreeSmgr()->GetBtrMetaPageId().m_fileId, GetBtreeSmgr()->GetBtrMetaPageId().m_blockId,
                static_cast<int32>(GetBtreeSmgr()->GetMetaCreateXid().m_zoneId),
                GetBtreeSmgr()->GetMetaCreateXid().m_logicSlotId));
        return DSTORE_FAIL;
    }

    newRight.GetPage()->InitNewRightForSplit(splitPage, tempLeft);
    m_newRightBuf = newRight.GetBuffDesc();

    /* Write hikey for splitting right page if it's not rightmost */
    if (!newRight.GetLinkAndStatus()->IsRightmost()) {
        /* Hikey of right page should be the same with high key of original page (splitting target page) */
        IndexTuple *rightHikey = splitPage->GetIndexTuple(BTREE_PAGE_HIKEY);
        if (unlikely(newRight.GetPage()->AddTuple(rightHikey, BTREE_PAGE_HIKEY, INVALID_TD_SLOT) != BTREE_PAGE_HIKEY)) {
            return DSTORE_FAIL;
        }
    }
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Get new right{%u}", newRight.GetPageId().m_blockId));

    return DSTORE_SUCC;
}

void BtreeSplit::SetErrorAndPutFailedPageBackToFreePageQueue(ErrorCode errCode)
{
    if (errCode != 0) {
        storage_set_error(errCode);
    }
    ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("%s", StorageGetMessage()));
    if (m_newRightBuf == INVALID_BUFFER_DESC) {
        return;
    }
    PageId failedPageId = m_newRightBuf->GetPageId();
    BtrPage *newRightPage = static_cast<BtrPage *>(m_newRightBuf->GetPage());
    newRightPage->GetLinkAndStatus()->SetLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_NO_SIB);
    m_bufMgr->UnlockAndRelease(m_newRightBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
    m_newRightBuf = INVALID_BUFFER_DESC;
    (void)GetBtreeSmgr()->PutIntoFreeQueue(failedPageId);
    ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg(BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
        BTR_PAGE_HEADER_VAL(newRightPage), BTR_PAGE_LINK_AND_STATUS_VAL(newRightPage->GetLinkAndStatus())));
}

IndexTuple *BtreeSplit::CreateSplittingLeftHikey(BtrPage *splitPage, IndexTuple *insTuple,
                                                 const SplitContext splitContext, bool &isSameWithLastLeft)
{
    IndexTuple *lastLeft;
    IndexTuple *firstRight;
    bool insFirstRight = false;
    bool isLeafPage = splitPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE);
    if (unlikely(splitContext.insertOff == splitContext.firstRightOff)) {
        /* When the insert position has the same offset number with right first item, need to check insertOnLeft flag
         * to determine the insert position should be on left or right */
        if (splitContext.insertOnLeft) {
            lastLeft = insTuple;
            firstRight = splitPage->GetIndexTuple(splitContext.firstRightOff);
        } else {
            lastLeft = splitPage->GetIndexTuple(OffsetNumberPrev(splitContext.firstRightOff));
            firstRight = insTuple;
            insFirstRight = true;
        }
    } else {
        lastLeft = splitPage->GetIndexTuple(OffsetNumberPrev(splitContext.firstRightOff));
        firstRight = splitPage->GetIndexTuple(splitContext.firstRightOff);
    }

    /* Truncate the suffix key attributes if we can distinguish the two tuples by the first n key attributes. */
    /* We should only ever truncate non-internal tuples from leaf pages. */
    IndexTuple *leftHikey = nullptr;
    if (likely(isLeafPage)) {
        leftHikey = CreateTruncateInternalTuple(lastLeft, firstRight, m_needTruncateHikey);
        if (unlikely(leftHikey == nullptr)) {
            if (StorageGetErrorCode() == INDEX_ERROR_TUPLE_DAMAGED) {
                Btree::DumpDamagedTuple(firstRight, insFirstRight ? nullptr : splitPage, splitContext.firstRightOff);
            }
            return nullptr;
        }
        if (unlikely(leftHikey->HasCtidBreaker() && (leftHikey->GetHeapCtid() == firstRight->m_link.heapCtid))) {
            isSameWithLastLeft = true;
            leftHikey->SetSameWithLastLeft();
        }
    } else {
        leftHikey = firstRight;
    }
    return leftHikey;
}

RetStatus BtreeSplit::CompleteSplit(BufferDesc *splitBuf, BtrStack stack, LWLockMode access)
{
    StorageAssert(splitBuf != INVALID_BUFFER_DESC);
    BtrPageLinkAndStatus *splitLinkAndStatus = static_cast<BtrPage *>(splitBuf->GetPage())->GetLinkAndStatus();

    /* Step 1. Promote to write lock if we are holding a read lock */
    if (access != LW_EXCLUSIVE) {
        /* Need a write-lock to complete split */
        m_bufMgr->UnlockContent(splitBuf);
        if (STORAGE_FUNC_FAIL(m_bufMgr->LockContent(splitBuf, LW_EXCLUSIVE))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Lock content {%d, %u} failed when execute function %s.",
                splitBuf->GetPageId().m_fileId, splitBuf->GetPageId().m_blockId, __FUNCTION__));
            m_bufMgr->Release(splitBuf);
            splitBuf = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        /* Need to re-check. Split may be finished by others during we were changing lock */
        if (splitLinkAndStatus->IsSplitComplete()) {
            m_bufMgr->UnlockAndRelease(splitBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
            return DSTORE_SUCC;
        }
    }
    StorageAssert(!splitLinkAndStatus->IsSplitComplete());

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Going to finish incomplete split page {%d, %u}",
        splitBuf->GetPageId().m_fileId, splitBuf->GetPageId().m_blockId));

    /* Step 2. Check if splitting target was the root when no parent found */
    bool wasRoot = false;
    if (stack == nullptr) {
        BufferDesc *btrMetaBuf = INVALID_BUFFER_DESC;
        BtrMeta *btrMeta = GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
        if (unlikely(btrMeta == nullptr)) {
            m_bufMgr->UnlockAndRelease(splitBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Can not get btrMeta when CompleteSplit."));
            return DSTORE_FAIL;
        }
        wasRoot = btrMeta->GetRootPageId() == splitBuf->GetPageId();
        m_bufMgr->UnlockAndRelease(btrMetaBuf);
    }

    /* Step 3. Check if splitting target was the only page in level if it was root. */
    if (unlikely(wasRoot)) {
        StorageAssert(splitLinkAndStatus->IsRoot());
        /* We create a new root of higher level iff the splitting target is root which must be a single page in level
         * Anyone steps right from the original root must have finished splitting the old root and created a new root
         * in higher level. In that case, warRoot should be false because Btree meta must have been update at the same
         * time with a new root created. */

        /* Splitting target was root. It should still be the leftmost */
        if (!splitLinkAndStatus->IsLeftmost()) {
            storage_set_error(INDEX_ERROR_BTREE_STRUCTURE_DAMAGED);
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Wrong btree structure. root btrPage(%d, %u) is not leftmost in(%s), segment(%hu, %u) "
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                splitBuf->GetPageId().m_fileId, splitBuf->GetPageId().m_blockId, m_indexInfo->indexRelName,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                BTR_PAGE_HEADER_VAL(static_cast<BtrPage *>(splitBuf->GetPage())),
                BTR_PAGE_LINK_AND_STATUS_VAL(splitLinkAndStatus)));
            m_bufMgr->UnlockAndRelease(splitBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
            return DSTORE_FAIL;
        }
        /* And its right sibling should be the rightmost */
        BufferDesc *rightBuf =
            Btree::ReadAndCheckBtrPage(splitLinkAndStatus->GetRight(), LW_SHARED, m_bufMgr, this->GetPdbId());
        if (STORAGE_VAR_NULL(rightBuf)) {
            m_bufMgr->UnlockAndRelease(splitBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to read right sibling when complete split."));
            return DSTORE_FAIL;
        }
        BtrPageLinkAndStatus *rightLinkAndStatus = static_cast<BtrPage *>(rightBuf->GetPage())->GetLinkAndStatus();
        if (!rightLinkAndStatus->IsRightmost()) {
            storage_set_error(INDEX_ERROR_BTREE_STRUCTURE_DAMAGED);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Wrong btree structure. newly splitting root's right btrPage"
                "(%d, %u) is not rightmost in(%s), segment(%hu, %u) " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                rightBuf->GetPageId().m_fileId, rightBuf->GetPageId().m_blockId, m_indexInfo->indexRelName,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                BTR_PAGE_HEADER_VAL(static_cast<BtrPage *>(rightBuf->GetPage())),
                BTR_PAGE_LINK_AND_STATUS_VAL(rightLinkAndStatus)));
            m_bufMgr->UnlockAndRelease(splitBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
            m_bufMgr->UnlockAndRelease(rightBuf);
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("Going to create a new root during complete split. Old root {%d, %u} and its right {%d, %u}",
                      splitBuf->GetPageId().m_fileId, splitBuf->GetPageId().m_blockId,
                      rightBuf->GetPageId().m_fileId, rightBuf->GetPageId().m_blockId));
        m_bufMgr->UnlockAndRelease(rightBuf);
    }

    /* Step 4. Add downlink of right page to parent */
    InitSplittingTarget(splitBuf);
    return AddPageDownlinkToParent((stack == nullptr) ? nullptr : stack, wasRoot);
}

/* splitBuf will be unlocked and released after calling no matter AddPageDownlinkToParent succeeds or not */
RetStatus BtreeSplit::AddPageDownlinkToParent(BtrStack stack, bool isRoot)
{
    StorageAssert(m_splitBuf != INVALID_BUFFER_DESC);

    /* Step 1. Deal with root split and construct a new root */
    if (unlikely(isRoot)) {
        StorageAssert(stack == nullptr);
        return CreateNewInternalRoot();
    }

    /* Step 2. Create downlink tuple for target's right sibling */
    BtreePagePayload splitPagePayload;
    splitPagePayload.InitByBuffDesc(m_splitBuf);
    /* A downlink to right page on internal page has the same keys with its left sibling's hikey */
    IndexTuple *rightDownlink = splitPagePayload.GetPage()->GetIndexTuple(BTREE_PAGE_HIKEY)->Copy();
    if (unlikely(rightDownlink == nullptr)) {
        if (StorageGetErrorCode() == INDEX_ERROR_TUPLE_DAMAGED) {
            Btree::DumpDamagedTuple(splitPagePayload.GetPage()->GetIndexTuple(BTREE_PAGE_HIKEY),
                splitPagePayload.GetPage(), BTREE_PAGE_HIKEY);
        }
        m_bufMgr->UnlockAndRelease(m_splitBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        m_splitBuf = INVALID_BUFFER_DESC;
        return DSTORE_FAIL;
    }
    StorageAssert(rightDownlink->GetKeyNum(m_indexInfo->indexAttrsNum) <= m_indexInfo->indexKeyAttrsNum);

    /* If the process has reached this point, it must have already been initialized,
     * so linkAndStatus must not be nullptr.
     */
    StorageReleasePanic(splitPagePayload.GetLinkAndStatus() == nullptr, MODULE_INDEX,
                        ErrMsg("splitPagePayload getLinkAndStatus is nullptr."));
    /* Set downlink to tuples */
    PageId rightPageId = splitPagePayload.GetLinkAndStatus()->GetRight();
    rightDownlink->SetLowlevelIndexpageLink(rightPageId);

    /* Step 3. Find and write-lock the parent of splitting target */
    OffsetNumber childDownlinkOffset = INVALID_ITEM_OFFSET_NUMBER;
    BufferDesc *parentBuf = (stack == nullptr) ?
        GetParentBufDescFromRoot(splitPagePayload.GetPageId(), splitPagePayload.GetLinkAndStatus()->GetLevel(),
        childDownlinkOffset) : GetParentBufDesc(splitPagePayload.GetPageId(), stack, LW_EXCLUSIVE);
    if (unlikely(parentBuf == INVALID_BUFFER_DESC)) {
        StringInfoData splitPageInfo;
        if (splitPageInfo.init()) {
            splitPageInfo.append(BTR_PAGE_HEADER_FMT, BTR_PAGE_HEADER_VAL(splitPagePayload.GetPage()));
            splitPageInfo.append(" ");
            splitPageInfo.append(BTR_PAGE_LINK_AND_STATUS_FMT,
                BTR_PAGE_LINK_AND_STATUS_VAL(splitPagePayload.GetLinkAndStatus()));
        }
        /* Get splitPageInfo before m_splitBuf release */
        m_bufMgr->UnlockAndRelease(m_splitBuf);
        m_splitBuf = INVALID_BUFFER_DESC;
        PageId origParentPageId = stack == nullptr ? INVALID_PAGE_ID : stack->currItem.GetPageId();
        StringInfoData parentPageInfo;
        bool strInfoInitSucc = parentPageInfo.init();
        if (stack != nullptr) {
            BufferDesc *origParentDesc = m_bufMgr->Read(this->GetPdbId(), origParentPageId, LW_SHARED);
            if (unlikely(origParentDesc == INVALID_BUFFER_DESC)) {
                ErrLog(DSTORE_WARNING, MODULE_INDEX, ErrMsg("Faild to read btrPage(%hu, %u).",
                    origParentPageId.m_fileId, origParentPageId.m_blockId));
            } else {
                BtrPage *parentPage = static_cast<BtrPage*>(origParentDesc->GetPage());
                if (parentPage != nullptr && strInfoInitSucc) {
                    parentPageInfo.append(BTR_PAGE_HEADER_FMT, BTR_PAGE_HEADER_VAL(parentPage));
                    parentPageInfo.append(" ");
                    parentPageInfo.append(BTR_PAGE_LINK_AND_STATUS_FMT,
                        BTR_PAGE_LINK_AND_STATUS_VAL(parentPage->GetLinkAndStatus()));
                }
                m_bufMgr->UnlockAndRelease(origParentDesc);
            }
        }
        storage_set_error(INDEX_ERROR_FAIL_REFIND_PARENT_KEY, m_indexInfo->indexRelName,
                          splitPagePayload.GetPageId().m_fileId, splitPagePayload.GetPageId().m_blockId);
        ErrLog(DSTORE_PANIC, MODULE_INDEX,
            ErrMsg("Failed to find parent of splitting btrPage(%hu, %u), %s, stack parent was btrPage(%hu, %u), %s",
                splitPagePayload.GetPageId().m_fileId, splitPagePayload.GetPageId().m_blockId,
                splitPageInfo.data == nullptr ? "" : splitPageInfo.data,
                origParentPageId.m_fileId, origParentPageId.m_blockId,
                parentPageInfo.data == nullptr ? "" : parentPageInfo.data));
        DstorePfree(rightDownlink);
        return DSTORE_FAIL;
    }

    BtrPage *parentPage = static_cast<BtrPage *>(parentBuf->GetPage());
    OffsetNumber rightDownlinkOffset;
    if (stack != nullptr) {
        rightDownlinkOffset = OffsetNumberNext(stack->currItem.GetOffset());
        /* Check if we're not splitting lowest single page */
        StorageAssert(rightDownlinkOffset > parentPage->GetLinkAndStatus()->GetFirstDataOffset());
    } else {
        rightDownlinkOffset = OffsetNumberNext(childDownlinkOffset);
    }

    /* Step 4. Check if downlink is alread inserted */
    if (rightDownlinkOffset <= parentPage->GetMaxOffset() &&
        parentPage->GetIndexTuple(rightDownlinkOffset)->GetLowlevelIndexpageLink() == rightPageId) {
        /* downlink exists. no need to insert */
        splitPagePayload.GetLinkAndStatus()->SetSplitStatus(BtrPageSplitStatus::SPLIT_COMPLETE);
        (void)m_bufMgr->MarkDirty(m_splitBuf, false);
        if (NeedWal()) {
            AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
            walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
            InsertChildStatusWal(m_splitBuf);
            walContext->EndAtomicWal();
        }
        m_bufMgr->UnlockAndRelease(parentBuf);
        m_bufMgr->UnlockAndRelease(m_splitBuf);
        m_splitBuf = INVALID_BUFFER_DESC;
        DstorePfree(rightDownlink);
        return DSTORE_SUCC;
    }

    /* Step 5. Insert downlink tuple into the parent */
    RetStatus ret = AddTupleToInternal(rightDownlink, rightDownlinkOffset,
                                       (stack == nullptr) ? nullptr : stack->parentStack, parentBuf, m_splitBuf);
    DstorePfree(rightDownlink);

    return ret;
}

RetStatus BtreeSplit::CreateNewInternalRoot()
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeDepthIncreaseLatency);
    StorageAssert(m_splitBuf != INVALID_BUFFER_DESC);
    BtrPage *splitPage = static_cast<BtrPage *>(m_splitBuf->GetPage());
    PageId origRootPageId = m_splitBuf->GetPageId();

    /* Step 1. Create downlink tuple for old root and its right sibling */
    /* The first data item on an internal page should have a key value of "minus infinity" that's reliably less
     * than any real key value that could appear int the left page. */
    IndexTuple *leftDownlink = IndexTuple::CreateMinusinfPivotTuple();
    if (unlikely(leftDownlink == nullptr)) {
        m_bufMgr->UnlockAndRelease(m_splitBuf);
        m_splitBuf = INVALID_BUFFER_DESC;
        return DSTORE_FAIL;
    }
    /* A downlink to right page on internal page has the same keys with its left sibling's hikey */
    IndexTuple *rightDownlink = splitPage->GetIndexTuple(BTREE_PAGE_HIKEY)->Copy();
    if (unlikely(rightDownlink == nullptr)) {
        if (StorageGetErrorCode() == INDEX_ERROR_TUPLE_DAMAGED) {
            Btree::DumpDamagedTuple(splitPage->GetIndexTuple(BTREE_PAGE_HIKEY), splitPage, BTREE_PAGE_HIKEY);
        }
        m_bufMgr->UnlockAndRelease(m_splitBuf);
        m_splitBuf = INVALID_BUFFER_DESC;
        DstorePfree(leftDownlink);
        return DSTORE_FAIL;
    }
    /* Set downlink to tuples */
    leftDownlink->SetLowlevelIndexpageLink(m_splitBuf->GetPageId());
    rightDownlink->SetLowlevelIndexpageLink(splitPage->GetRight());

    /* Step 2. Create and initialize a new root page */
    BtreePagePayload newRoot;
    if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->GetNewPage(newRoot))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("CreateNewInternalRoot get new page faild for btree with Meta btrPage(%d, %u) created xid(%d, %lu)",
            GetBtreeSmgr()->GetBtrMetaPageId().m_fileId, GetBtreeSmgr()->GetBtrMetaPageId().m_blockId,
            static_cast<int32>(GetBtreeSmgr()->GetMetaCreateXid().m_zoneId),
            GetBtreeSmgr()->GetMetaCreateXid().m_logicSlotId));
        m_bufMgr->UnlockAndRelease(m_splitBuf);
        m_splitBuf = INVALID_BUFFER_DESC;
        DstorePfree(leftDownlink);
        DstorePfree(rightDownlink);
        return DSTORE_FAIL;
    }

    newRoot.GetLinkAndStatus()->InitPageMeta(GetBtreeSmgr()->GetBtrMetaPageId(),
                                             splitPage->GetLevel() + 1, true);
    newRoot.GetPage()->SetBtrMetaCreateXid(GetBtreeSmgr()->GetMetaCreateXid());

    /* Step 3. Write downlink tuples onto new root page */
    OffsetNumber leftOffset = BTREE_PAGE_HIKEY; /* First data tuple on page. Root doesn't have a hikey. */
    OffsetNumber rightOffset = OffsetNumberNext(leftOffset);
    if (unlikely(leftOffset != newRoot.GetPage()->AddTuple(leftDownlink, leftOffset) ||
                 rightOffset != newRoot.GetPage()->AddTuple(rightDownlink, rightOffset))) {
        storage_set_error(INDEX_ERROR_ADD_ITEM_FAIL);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to add downlink to new root btrPage(%d, %u) in(%s), segment(%hu, %u)"
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            newRoot.GetPageId().m_fileId, newRoot.GetPageId().m_blockId, m_indexInfo->indexRelName,
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            BTR_PAGE_HEADER_VAL(newRoot.GetPage()), BTR_PAGE_LINK_AND_STATUS_VAL(newRoot.GetLinkAndStatus())));
        DstorePfree(leftDownlink);
        DstorePfree(rightDownlink);
        m_bufMgr->UnlockAndRelease(m_splitBuf);
        m_splitBuf = INVALID_BUFFER_DESC;
        (void)GetBtreeSmgr()->PutIntoFreeQueue(newRoot.GetPageId());
        return DSTORE_FAIL;
    }
    DstorePfree(leftDownlink);
    DstorePfree(rightDownlink);
    StorageAssert(newRoot.GetPage()->CheckSanity());

    /* Step 4. Update Btree Meta */
    BufferDesc *btrMetaBuf = INVALID_BUFFER_DESC;
    BtrMeta *btrMeta = GetBtreeSmgr()->GetBtrMeta(LW_EXCLUSIVE, &btrMetaBuf);
    StorageReleasePanic(btrMeta == nullptr, MODULE_INDEX, ErrMsg("Can not get btrMeta when CreateNewInternalRoot."));
    PageId origLowestSinglePageId = btrMeta->GetLowestSinglePage();
    uint32 origLowestSingleLevel = btrMeta->GetLowestSinglePageLevel();
    btrMeta->SetBtreeMetaInfo(newRoot.GetPageId(), newRoot.GetPageId(),
                              newRoot.GetLinkAndStatus()->GetLevel(), newRoot.GetLinkAndStatus()->GetLevel());
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(newRoot.GetBuffDesc()));
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(btrMetaBuf));

    /* Step 5. Update splitting target's status */
    splitPage->GetLinkAndStatus()->SetRoot(false);
    UpdateChildSplitStatus(m_splitBuf);
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_splitBuf));

    /* Step 5. Record wal */
    /* Wal atomic group must not have began for internal pages */
    if (likely(NeedWal()) && STORAGE_FUNC_FAIL(InsertNewRootWal(newRoot.GetBuffDesc(), origRootPageId, btrMetaBuf))) {
        /* Failed insert wal record, need to restore pages because internal pages and BtrMeta page don't have UndoRec */
        /* Restore BtrMeta page */
        btrMeta->SetBtreeMetaInfo(origRootPageId, origLowestSinglePageId, splitPage->GetLevel(), origLowestSingleLevel);
        m_bufMgr->UnlockAndRelease(btrMetaBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        /* Restore original root page */
        splitPage->GetLinkAndStatus()->SetSplitStatus(BtrPageSplitStatus::SPLIT_INCOMPLETE);
        splitPage->GetLinkAndStatus()->SetRoot(true);
        m_bufMgr->UnlockAndRelease(m_splitBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        m_splitBuf = INVALID_BUFFER_DESC;
        /* Put new root page back to free queue */
        newRoot.GetLinkAndStatus()->SetLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_NO_SIB);
        m_bufMgr->UnlockAndRelease(newRoot.GetBuffDesc(), BufferPoolUnlockContentFlag::DontCheckCrc());
        (void)GetBtreeSmgr()->PutIntoFreeQueue(newRoot.GetPageId());
        return DSTORE_FAIL;
    }

    /* Unlock and release all page */
    m_bufMgr->UnlockAndRelease(m_splitBuf);
    m_splitBuf = INVALID_BUFFER_DESC;
    m_bufMgr->UnlockAndRelease(newRoot.GetBuffDesc());
    m_bufMgr->UnlockAndRelease(btrMetaBuf);
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Created new root page {%d, %u} when split", newRoot.GetPageId().m_fileId,
                  newRoot.GetPageId().m_blockId));

    return DSTORE_SUCC;
}

RetStatus BtreeSplit::UpdateBtrMetaLowestSinglePage(const PageId pageId, uint32 pageLevel)
{
    /* May modify BtrMeta page and it may be changed by others, so we cannot use Metacache here, need to re-read it. */
    BufferDesc *btrMetaBuf = INVALID_BUFFER_DESC;
    BtrMeta *btrMeta = GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    if (unlikely(btrMeta == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Can not get btrMeta when UpdateBtrMetaLowestSinglePage."));
        return DSTORE_FAIL;
    }
    if (btrMeta->GetLowestSinglePageLevel() >= pageLevel) {
        m_bufMgr->UnlockAndRelease(btrMetaBuf);
        return DSTORE_SUCC;
    }

    /* Change to a write-lock */
    m_bufMgr->UnlockContent(btrMetaBuf);
    if (STORAGE_FUNC_FAIL(m_bufMgr->LockContent(btrMetaBuf, LW_EXCLUSIVE))) {
        storage_set_error(INDEX_ERROR_FAILED_UPDATE_BTRMETA_ROOT);
        ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("Lock content Meta btrPage(%d, %u) failed when execute function %s.",
            btrMetaBuf->GetPageId().m_fileId, btrMetaBuf->GetPageId().m_blockId, __FUNCTION__));
        m_bufMgr->Release(btrMetaBuf);
        btrMetaBuf = INVALID_BUFFER_DESC;
        return DSTORE_FAIL;
    }
    /* Recheck if other has changed Btree Meta during we were changing lock */
    BtrPage *metaPage = static_cast<BtrPage *>(btrMetaBuf->GetPage());
    btrMeta = static_cast<BtrMeta *>(static_cast<void *>(metaPage->GetData()));
    if (btrMeta->GetLowestSinglePageLevel() >= pageLevel) {
        m_bufMgr->UnlockAndRelease(btrMetaBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        return DSTORE_SUCC;
    }

    /* Update the lowest single page */
    btrMeta->SetLowestSinglePage(pageId);
    btrMeta->SetLowestSinglePageLevel(pageLevel);

    UNUSED_VARIABLE(m_bufMgr->MarkDirty(btrMetaBuf));

    /* Generate wal Record */
    if (likely(NeedWal())) {
        AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
        /* Wal atomic group must have already began */
        StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_INDEX, ErrMsg("Wal has not atomic begin."));
        walContext->RememberPageNeedWal(btrMetaBuf);

        bool glsnChanged = (metaPage->GetWalId() != walContext->GetWalId());
        WalRecordBtreeUpdateMetaPage walData;
        walData.SetHeader(metaPage, glsnChanged, false, btrMetaBuf->GetFileVersion());
        walContext->PutNewWalRecord(&walData);
        /* Do not end atomic wal group here. outer caller will do it. */
    }
    m_bufMgr->UnlockAndRelease(btrMetaBuf);
    return DSTORE_SUCC;
}

bool BtreeSplit::CheckTupleVisibility(BtrPage *btrPage, OffsetNumber checkOff, Xid *waitXid)
{
    IndexTuple *checkTuple = btrPage->GetIndexTuple(checkOff);
    /* Tuple is visible if TD is shared or FROZEN. */
    if (checkTuple->TestTdStatus(ATTACH_TD_AS_HISTORY_OWNER) || (checkTuple->TestTdStatus(DETACH_TD))) {
        return true;
    }

    TD *td = btrPage->GetTd(checkTuple->GetTdId());
    Xid checkTupleXid = td->GetXid();
    StorageAssert(checkTupleXid != INVALID_XID);
    StorageAssert(!td->TestStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE));
    XidStatus xs(checkTupleXid, thrd->GetActiveTransaction(), td);
    if (xs.IsCurrentTxn() || xs.IsCommitted() || xs.IsFrozen()) {
        return true;
    }
    /* The tuple related transaction is not finished for now. Need to wait it end. */
    if (waitXid != nullptr) {
        *waitXid = checkTupleXid;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("checking xidStatus{%d, %lu}, status:%hhu, csn:%lu",
        static_cast<int32>(xs.GetXid().m_zoneId), xs.GetXid().m_logicSlotId, xs.GetStatus(), xs.GetCsn()));
    return false;
}

RetStatus BtreeSplit::WaitForTxnEndIfNeeded(const PageId &pageId, Xid xid)
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_waitForTxnEndLatency);

    bool txnFailed = false;
    if (STORAGE_FUNC_FAIL(g_storageInstance->GetPdb(this->GetPdbId())->GetTransactionMgr()->
        WaitForTransactionEnd(xid, txnFailed))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Error when wait transaction xid(%d, %lu) end",
            static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
        return DSTORE_FAIL;
    }

    if (!txnFailed) {
        return DSTORE_SUCC;
    }

    /*
     * Consider meeting a failed transaction which should be rolled back in recovery.
     * Failed transaction do not hold xid lock, so should judge transaction status specially.
     * In order to reduce wait time, we do page rollback here.
     */
    PdbId pdbId = GetPdbId();
    BufferDesc *bufDesc = Btree::ReadAndCheckBtrPage(pageId, LW_EXCLUSIVE, m_bufMgr, pdbId);
    if (STORAGE_VAR_NULL(bufDesc)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("ReadAndCheckBtrPage failed when wait transaction xid(%d, %lu) end",
            static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
        return DSTORE_FAIL;
    }
    BtrPage *page = static_cast<BtrPage *>(bufDesc->GetPage());
    BtreeUndoContext btrUndoContext(pdbId, page->GetBtrMetaPageId(), m_indexInfo, m_bufMgr,
        page->GetBtrMetaCreateXid(), BtreeUndoContextType::PAGE_ROLLBACK);
    RetStatus ret = DSTORE_SUCC;
    if (STORAGE_FUNC_SUCC(btrUndoContext.InitWithBtrPage(page, bufDesc))) {
        page->RollbackByXid(pdbId, xid, m_bufMgr, bufDesc, &btrUndoContext);
    }

    m_bufMgr->UnlockAndRelease(bufDesc);
    return ret;
}

RetStatus BtreeSplit::WaitTxnEndForTdRealloc()
{
    StorageAssert(m_tdContext.NeedRetryAllocTd());
    StorageAssert(m_needRetrySearchBtree);

    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(this->GetPdbId())->GetTransactionMgr();
    if (STORAGE_FUNC_FAIL(transactionMgr->WaitForOneTransactionEnd(m_tdContext.waitXids.xids,
                                                                   m_tdContext.waitXids.xidNum))) {
        /* Something wrong when waiting for other transaction end. Give up retry and return fail */
        return DSTORE_FAIL;
    }
    /* For now, a transaction that occupied the td slot on current page ends. re-initialize td context and retry */
    m_tdContext.Init(this->GetPdbId(), NeedWal());
    return DSTORE_SUCC;
}

RetStatus BtreeSplit::GenerateUndoRecordForInsert(IndexTuple *insTuple, OffsetNumber insOff, BtrPage *insPage,
                                                  UndoRecord **undoRecord)
{
    /* Only write Undo record for leaf page */
    StorageAssert(insPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    *undoRecord = nullptr;
    /* Create Undo data. Insert undo data contains bitmap and keys */
    uint32 undoDataSize = sizeof(UndoDataBtreeInsert) + (insTuple->GetSize() - sizeof(IndexTuple));
    auto *undoData = static_cast<UndoDataBtreeInsert *>(DstorePalloc(undoDataSize));
    if (unlikely(undoData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when GenerateUndoRecordForInsert."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    undoData->InitBtreeInsertUndoData();
    undoData->SetHeapCtid(insTuple->m_link.heapCtid);
    undoData->SetInsertForDelete(insTuple->IsDeleted() && insTuple->IsInsertDeletedForCCindex());
    Xid metaCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
    StorageAssert(metaCreateXid != INVALID_XID);
    undoData->SetMetaCreateXid(metaCreateXid);
    if (insTuple->HasNull()) {
        undoData->SetHasNull();
        undoData->AppendBitmap(insTuple->GetNullBitmap(), static_cast<uint8>(m_indexInfo->indexAttrsNum));
    }
    undoData->SetHasVariable(insTuple->HasVariable());
    undoData->AppendData(insTuple->GetValues(), insTuple->GetSize() - insTuple->GetDataOffset());

    /* Create Undo record */
    Transaction *transaction = thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    Xid currXid = transaction->GetCurrentXid();
    TransactionMgr *txnMgr = g_storageInstance->GetPdb(this->GetPdbId())->GetTransactionMgr();
    ItemPointerData ctid(insPage->GetSelfPageId(), insOff);
    uint8 tdID = insTuple->GetTdId();
    UndoType type = GetBtreeSmgr()->IsGlobalTempIndex() ? UNDO_BTREE_INSERT_TMP : UNDO_BTREE_INSERT;
    *undoRecord = DstoreNew(g_dstoreCurrentMemoryContext)
        UndoRecord(type, tdID, insPage->GetTd(tdID), ctid, m_cid);
    if (unlikely((*undoRecord) == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when GenerateUndoRecordForInsert."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        DstorePfree(undoData);
        return DSTORE_FAIL;
    }
    StorageAssert(*undoRecord != nullptr);
    RetStatus ret = (*undoRecord)->Append(static_cast<char *>(static_cast<void *>(undoData)), undoData->GetSize());
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when append undo record."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        DstorePfree(undoData);
        return DSTORE_FAIL;
    }
    DstorePfree(undoData);
    return txnMgr->ExtendUndoSpaceIfNeeded(currXid, (*undoRecord)->GetRecordSize());
}

RetStatus BtreeSplit::InsertUndoRecAndSetTd(uint8 tdID, OffsetNumber insOff, BtrPage *insPage,
                                            UndoRecord *undoRecord)
{
    /* Insert Undo record */
    Transaction *transaction = thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    Xid currXid = transaction->GetCurrentXid();
    UndoRecPtr undoRecPtr = transaction->InsertUndoRecord(undoRecord);
    if (unlikely(undoRecPtr == INVALID_UNDO_RECORD_PTR)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to insert undo record."));
        return DSTORE_FAIL;
    }

    /* Update TD slot on page */
    insPage->SetTd(tdID, currXid, undoRecPtr, m_cid);
    IndexTuple *tupleOnPage = insPage->GetIndexTuple(insOff);
    tupleOnPage->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    return DSTORE_SUCC;
}

/*
 * Since no failure would be allowed once we starts a wal atomic group, here we separate logging steps (including
 * Undo record and Wal record) into two phases.
 * In phase one, we deal with operations that might fail for non-coding reasons, such as memory failure or buffer
 * descriptor reading failure. We don't start a wal atomic group in phase one to avoid any possible interruption.
 * When we've succeeded in all functions of phase one, we can step to phase two, start a wal atomic group, writing
 * undo and wal records and update splitting page in the same wal atomic group.
 */
RetStatus BtreeSplit::InsertUndoAndWalForSplitLeaf(IndexTuple *insTuple, SplitContext &splitCxt,
    OffsetNumber insertOffOnPage, BufferDesc *oldRightBuf, BtrPage *newLeft)
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    /* Wal atomic group must not have began */
    StorageReleasePanic(walContext->HasAlreadyBegin(), MODULE_INDEX, ErrMsg("Wal atomic group has already began."));
    WalRecord *splitWalRecord = nullptr;
    WalRecord *newRightWalRecord = nullptr;

    BtrPage *splitPage = static_cast<BtrPage *>(m_splitBuf->GetPage());
    StorageAssert(splitPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    BtrPage *rightPage = static_cast<BtrPage *>(m_newRightBuf->GetPage());
    BtrPage *insPage = splitCxt.insertOnLeft ? newLeft : rightPage;

    Transaction *transaction = thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    Xid currXid = transaction->GetCurrentXid();
    uint8 tdID = insTuple->GetTdId();
    StorageAssert(!m_needRecordUndo || tdID != INVALID_TD_SLOT);
    bool needSetTD = (m_needRecordUndo && !splitCxt.insertOnLeft && newLeft->GetTd(tdID)->GetXid() != currXid);

    /* Phase 1. Generate Undo and Wal Record. */
    /* Step 1.1. Generate Undo Record */
    UndoRecPtr undoRecPtrOnLeft = INVALID_UNDO_RECORD_PTR;
    UndoRecord *undoRec = nullptr;
    if (m_needRecordUndo &&
        STORAGE_FUNC_FAIL(GenerateUndoRecordForInsert(insTuple, insertOffOnPage, insPage, &undoRec))) {
        /* No need to restore splitting page since all changes is on temp left now */
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to extend undo space."));
        return DSTORE_FAIL;
    }

    /* Step 1.2. Generate WAL record */
    if (likely(NeedWal())) {
        /* When insertion is on the left page, record both split context and insertion info for splitting target. */
        /* When insertion is on the right page, record only split context for splitting target. */
        splitWalRecord = (splitCxt.insertOnLeft ? GenerateSplitInsertLeafWal(insTuple, splitCxt, newLeft)
                                                : GenerateSplitLeafWal(splitCxt, needSetTD, tdID, newLeft));
        /* Wal for new right */
        newRightWalRecord = GenerateNewRightWal();
        if (unlikely(splitWalRecord == nullptr || newRightWalRecord == nullptr)) {
            goto ABORT_SPLIT_AND_RETURN_FAIL;
        }
    }

    /* Phase 2. Insert Undo and Wal Record and update splitting page */
    walContext->BeginAtomicWal(currXid);

    /* Step 2.1. Insert Undo record */
    if (m_needRecordUndo && STORAGE_FUNC_FAIL(InsertUndoRecAndSetTd(tdID, insertOffOnPage, insPage, undoRec))) {
        /* Do not call HandleErrorWhenGetUndoBuffer here because it would rollback only the insertion, while we have
         * to rollback both insertion and split */
        /* No need to restore splitting page since all changes is on temp left now */
        walContext->ResetForAbort();
        goto ABORT_SPLIT_AND_RETURN_FAIL;
    }
    delete undoRec;

    /* Undo record has written, no failure will be allowed until wal atomic group ends. */
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_splitBuf));
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_newRightBuf));
    if (oldRightBuf != INVALID_BUFFER_DESC) {
        UNUSED_VARIABLE(m_bufMgr->MarkDirty(oldRightBuf));
    }

    if (unlikely(!NeedWal())) {
        splitPage->Clone(newLeft);
        UNUSED_VARIABLE(walContext->EndAtomicWal());
        return DSTORE_SUCC;
    }
    /* Step 2.2. Update wal record */
    /* In step 1.2, we generated wal records according to the "OLD" page without UndoRecPtr (because we just got the
     * UndoRecPtr after InsertUndoRecAndSetTd succeeded). Now we must update the wal records to fill in UndoRecPtr */
    static_cast<WalRecordBtreeNewRight *>(newRightWalRecord)->SetPage(rightPage);
    if (m_needRecordUndo) {
        undoRecPtrOnLeft = newLeft->GetTd(tdID)->GetUndoRecPtr();
        if (splitCxt.insertOnLeft) {
            /* Update UndoRecPtr only for insertOnLeft case, because we've already records TD ID */
            static_cast<WalRecordBtreeSplitInsertLeaf *>(splitWalRecord)->SetUndoRecPtr(undoRecPtrOnLeft);
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Update left {%hu, %u}, %hu UndoRecPtr %lu of TD %hhu for "
                "WalRecordBtreeSplitInsertLeaf.", newLeft->GetSelfPageId().m_fileId, newLeft->GetSelfPageId().m_blockId,
                insertOffOnPage, undoRecPtrOnLeft.m_placeHolder, tdID));
        } else {
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Update right page {%hu, %u} for WalRecordBtreeNewRight",
                m_newRightBuf->GetPageId().m_fileId, m_newRightBuf->GetPageId().m_blockId));
            if (needSetTD) {
                /* Also need to change undo pointer in wal record because we saved the old one on page before */
                static_cast<WalRecordBtreeSplitLeaf *>(splitWalRecord)->SetUndoRecPtr(
                    rightPage->GetTd(tdID)->GetUndoRecPtr().m_placeHolder, undoRecPtrOnLeft.m_placeHolder);
                /*
                * If new item is on left, we only write current xid on left page td.
                *
                * If new item is on right, we must write current xid onto left page td too.
                * In one scenario, we have the timeline in one transaction as follows:
                * TIMELINE 1: Scan page A and construct cr page of A.
                * TIMELINE 2: Insert one tuple causing page A split and new tuple is assigned to new right page B.
                * TIMELINE 3: Scan page A
                * At TIMELINE 3, the new tuple must be visible to current transaction. If all tds of page A
                * don't have current xid, the scan at TIMELINE 3 will reuse cr page of A built at TIMELINE 1.
                * This causes current transaction can't see new tuple in page B because right link of cr page A
                * isn't page B. So we must write current xid into page A, the scan in TIMELINE 3 will rebuild
                * cr page of A.
                */
                newLeft->SetTd(tdID, currXid, rightPage->GetTd(tdID)->GetUndoRecPtr(), m_cid);
                ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Update UndoRecPtr %lu of TD %hhu on left {%hu, %u}, "
                    "when insert is on right %hu", rightPage->GetTd(tdID)->GetUndoRecPtr().m_placeHolder, tdID,
                    newLeft->GetSelfPageId().m_fileId, newLeft->GetSelfPageId().m_blockId, splitCxt.insertOff));
            }
        }
    }

    /* Step 2.3. Update splitting target by copy the new temporary left to the original page */
    splitPage->Clone(newLeft);

    /* Step 2.4. Insert wal */
    walContext->RememberPageNeedWal(m_splitBuf);
    walContext->PutNewWalRecord(splitWalRecord);
    walContext->RememberPageNeedWal(m_newRightBuf);
    walContext->PutNewWalRecord(newRightWalRecord);
    if (oldRightBuf != INVALID_BUFFER_DESC) {
        InsertOldRightWal(oldRightBuf);
    }

    UNUSED_VARIABLE(walContext->EndAtomicWal());

    DstorePfree(splitWalRecord);
    DstorePfree(newRightWalRecord);
    return DSTORE_SUCC;

ABORT_SPLIT_AND_RETURN_FAIL:
    if (oldRightBuf != INVALID_BUFFER_DESC) {
        BtrPage *oldRightPage = static_cast<BtrPage *>(oldRightBuf->GetPage());
        /* Reset the oldRight's left page to split target's PageId */
        oldRightPage->GetLinkAndStatus()->SetLeft(m_splitBuf->GetPageId());
    }

    delete undoRec;
    DstorePfreeExt(splitWalRecord);
    DstorePfreeExt(newRightWalRecord);
    return DSTORE_FAIL;
}

RetStatus BtreeSplit::InsertOnlyWalForSplitPage(IndexTuple *insTuple, SplitContext &splitCxt, BufferDesc *oldRightBuf,
                                                BufferDesc *childBuf, BtrPage *newLeft)
{
    BtrPage *splitPage = static_cast<BtrPage *>(m_splitBuf->GetPage());
    if (childBuf != INVALID_BUFFER_DESC) {
        UNUSED_VARIABLE(m_bufMgr->MarkDirty(childBuf));
    }
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_splitBuf));
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_newRightBuf));
    if (oldRightBuf != INVALID_BUFFER_DESC) {
        UNUSED_VARIABLE(m_bufMgr->MarkDirty(oldRightBuf));
    }

    if (unlikely(!NeedWal())) {
        splitPage->Clone(newLeft);
        return DSTORE_SUCC;
    }

    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    /* Wal atomic group must not have began */
    StorageReleasePanic(walContext->HasAlreadyBegin(), MODULE_INDEX, ErrMsg("Wal atomic group has already began."));

    WalRecord *splitWalRecord = nullptr;
    if (splitPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)) {
        splitWalRecord = splitCxt.insertOnLeft ? GenerateSplitInsertInternalWal(insTuple, splitCxt, newLeft)
                                               : GenerateSplitInternalWal(splitCxt, newLeft);
    } else {
        splitWalRecord = splitCxt.insertOnLeft ? GenerateSplitInsertLeafWal(insTuple, splitCxt, newLeft)
                                               : GenerateSplitLeafWal(splitCxt, false, INVALID_TD_SLOT, newLeft);
    }
    WalRecord *newRightWalRecord = GenerateNewRightWal();
    if (unlikely(splitWalRecord == nullptr || newRightWalRecord == nullptr)) {
        /* No need to restore splitting page since all changes is on temp left now */
        goto ABORT_WAL_AND_RETURN_FAIL;
    }

    static_cast<WalRecordBtreeNewRight *>(newRightWalRecord)->SetPage(static_cast<BtrPage *>(m_newRightBuf->GetPage()));
    walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
    walContext->RememberPageNeedWal(m_splitBuf);
    walContext->PutNewWalRecord(splitWalRecord);
    walContext->RememberPageNeedWal(m_newRightBuf);
    walContext->PutNewWalRecord(newRightWalRecord);
    if (oldRightBuf != INVALID_BUFFER_DESC) {
        InsertOldRightWal(oldRightBuf);
    }
    if (childBuf != INVALID_BUFFER_DESC) {
        InsertChildStatusWal(childBuf);
    }
    /* Update splitting target by copy the new temporary left to the original page */
    splitPage->Clone(newLeft);
    UNUSED_VARIABLE(walContext->EndAtomicWal());

    DstorePfree(splitWalRecord);
    DstorePfree(newRightWalRecord);
    return DSTORE_SUCC;

ABORT_WAL_AND_RETURN_FAIL:
    walContext->ResetForAbort();

    if (oldRightBuf != INVALID_BUFFER_DESC) {
        BtrPage *oldRightPage = static_cast<BtrPage *>(oldRightBuf->GetPage());
        /* Reset the oldRight's left page to split target's PageId */
        oldRightPage->GetLinkAndStatus()->SetLeft(m_splitBuf->GetPageId());
    }

    DstorePfreeExt(splitWalRecord);
    DstorePfreeExt(newRightWalRecord);
    return DSTORE_FAIL;
}

/* Generate Wal Record for splitting target page that insertion if not on the splitting target (meaning we only do split
 * for the splitting target page, but insert the index tuple on the new right page). For leaf pages only */
WalRecord *BtreeSplit::GenerateSplitLeafWal(const SplitContext &splitCxt, bool needSetTD, uint8 tdID, BtrPage *newLeft)
{
    IndexTuple *leftHikey = newLeft->GetIndexTuple(BTREE_PAGE_HIKEY);
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdContext);
    uint32 walDataSize = sizeof(WalRecordBtreeSplitLeaf) + leftHikey->GetSize() + allocTdSize +
                         (needSetTD ? (sizeof(uint8) + sizeof(uint64)) : 0);
    WalRecordBtreeSplitLeaf *walData = static_cast<WalRecordBtreeSplitLeaf *>(DstorePalloc(walDataSize));
    if (unlikely(walData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when GenerateSplitLeafWal."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return nullptr;
    }
    /* get file version */
    if (m_newRightBuf->fileVersion == INVALID_FILE_VERSION) {
        m_newRightBuf->SetFileVersion(WalUtils::GetFileVersion(m_newRightBuf->GetPdbId(),
            m_newRightBuf->GetPageId().m_fileId));
    }
    walData->SetHeader(newLeft, walDataSize, (newLeft->GetWalId() != thrd->m_walWriterContext->GetWalId()),
                       m_newRightBuf->GetPageId(), splitCxt.firstRightOff, m_newRightBuf->GetFileVersion());
    uint32 length = needSetTD ? walData->SetTdData(0, newLeft, tdID) : 0U;
    length = walData->AppendTupleData(walData->GetDataField(), leftHikey, length);
    walData->SetAllocTd(walData->GetDataField(), m_tdContext, length, allocTdSize);

    return walData;
}

/* Simillar to GenerateSplitLeafWal, generate Wal Record for splitting target page that insertion if not on the
 * splitting target, but for internal pages only */
WalRecord *BtreeSplit::GenerateSplitInternalWal(const SplitContext &splitCxt, BtrPage *newLeft)
{
    IndexTuple *leftHikey = newLeft->GetIndexTuple(BTREE_PAGE_HIKEY);
    uint32 walDataSize = sizeof(WalRecordBtreeSplitInternal) + leftHikey->GetSize();
    WalRecordBtreeSplitInternal *walData = static_cast<WalRecordBtreeSplitInternal *>(DstorePalloc(walDataSize));
    if (unlikely(walData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when GenerateSplitInternalWal."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return nullptr;
    }
    walData->SetHeader(newLeft, walDataSize, (newLeft->GetWalId() != thrd->m_walWriterContext->GetWalId()),
                       m_newRightBuf->GetPageId(), splitCxt.firstRightOff, m_newRightBuf->GetFileVersion());
    (void)walData->AppendTupleData(walData->GetDataField(), leftHikey, 0U);

    return walData;
}

/* Generate Wal Record for splitting target page that insertion if also on the splitting target (left page). For leaf
 * pages only */
WalRecord *BtreeSplit::GenerateSplitInsertLeafWal(IndexTuple *insTuple, const SplitContext &splitCxt, BtrPage *newLeft)
{
    IndexTuple *hikey = newLeft->GetIndexTuple(BTREE_PAGE_HIKEY);
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdContext);
    uint32 walDataSize = sizeof(WalRecordBtreeSplitInsertLeaf) + hikey->GetSize() + insTuple->GetSize() + allocTdSize;
    WalRecordBtreeSplitInsertLeaf *walData = static_cast<WalRecordBtreeSplitInsertLeaf *>(DstorePalloc(walDataSize));
    if (unlikely(walData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when GenerateSplitInsertLeafWal."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return nullptr;
    }
    /* get file version */
    if (m_newRightBuf->fileVersion == INVALID_FILE_VERSION) {
        m_newRightBuf->SetFileVersion(WalUtils::GetFileVersion(m_newRightBuf->GetPdbId(),
            m_newRightBuf->GetPageId().m_fileId));
    }
    walData->SetHeader(newLeft, walDataSize, (newLeft->GetWalId() != thrd->m_walWriterContext->GetWalId()),
                       m_newRightBuf->GetPageId(), splitCxt, m_newRightBuf->GetFileVersion());
    uint32 length = walData->AppendTupleData(walData->GetDataField(), hikey, 0U);
    length = walData->AppendTupleData(walData->GetDataField(), insTuple, length);
    walData->SetAllocTd(walData->GetDataField(), m_tdContext, length, allocTdSize);

    return walData;
}

/* Simillar to GenerateSplitInsertLeafWal, generate Wal Record for splitting target page that insertion if also on the
 * splitting target, but for internal pages only */
WalRecord *BtreeSplit::GenerateSplitInsertInternalWal(IndexTuple *insTuple, const SplitContext &splitCxt,
                                                      BtrPage *newLeft)
{
    IndexTuple *leftHikey = newLeft->GetIndexTuple(BTREE_PAGE_HIKEY);
    uint32 walDataSize = sizeof(WalRecordBtreeSplitInsertInternal) + leftHikey->GetSize() + insTuple->GetSize();
    WalRecordBtreeSplitInsertInternal *walData =
        static_cast<WalRecordBtreeSplitInsertInternal *>(DstorePalloc(walDataSize));
    if (unlikely(walData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("DstorePalloc fail when GenerateSplitInsertInternalWal."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return nullptr;
    }
    walData->SetHeader(newLeft, walDataSize, (newLeft->GetWalId() != thrd->m_walWriterContext->GetWalId()),
                       m_newRightBuf->GetPageId(), splitCxt, m_newRightBuf->GetFileVersion());
    uint32 length = walData->AppendTupleData(walData->GetDataField(), leftHikey, 0U);
    (void)walData->AppendTupleData(walData->GetDataField(), insTuple, length);

    return walData;
}

/* Generate Wal Record for new splitting right page. No need to separate the case of "copy some of tuples from
 * splitting target to the new right page" and the case of "copy some tuples to new right page and insert the new
 * tuple to new right page also", because we will record the whole content on this page. It doesn't matter
 * that which case we hit here */
WalRecord *BtreeSplit::GenerateNewRightWal()
{
    BtrPage *page = static_cast<BtrPage *>(m_newRightBuf->GetPage());
    uint32 walDataSize = sizeof(WalRecordBtreeNewRight) +
                         static_cast<uint32>(page->GetLower() - page->DataHeaderSize()) +
                         static_cast<uint32>((BLCKSZ - page->GetUpper()));
    WalRecordBtreeNewRight *walData = static_cast<WalRecordBtreeNewRight *>(DstorePalloc(walDataSize));
    if (unlikely(walData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when GenerateNewRightWal."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return nullptr;
    }
    /* get file version */
    if (m_newRightBuf->fileVersion == INVALID_FILE_VERSION) {
        m_newRightBuf->SetFileVersion(WalUtils::GetFileVersion(m_newRightBuf->GetPdbId(),
            m_newRightBuf->GetPageId().m_fileId));
    }
    walData->SetHeader(page, walDataSize, (page->GetWalId() != thrd->m_walWriterContext->GetWalId()),
                       m_newRightBuf->GetFileVersion());

    return walData;
}

void BtreeSplit::InsertOldRightWal(BufferDesc *oldRightBuf)
{
    StorageAssert(oldRightBuf != INVALID_BUFFER_DESC);
    /* Wal atomic group must have already began */
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_INDEX, ErrMsg("Wal has not atomic begin."));
    walContext->RememberPageNeedWal(oldRightBuf);

    BtrPage *oldRightPage = static_cast<BtrPage *>(oldRightBuf->GetPage());
    WalRecordBtreeUpdateSibLink walData;
    walData.SetHeader(oldRightPage, (oldRightPage->GetWalId() != walContext->GetWalId()),
                      m_newRightBuf->GetPageId(), true, oldRightBuf->GetFileVersion());
    walContext->PutNewWalRecord(&walData);
}

RetStatus BtreeSplit::InsertNewRootWal(BufferDesc *rootBuf, const PageId origRoot, BufferDesc *btrMetaBuf)
{
    StorageAssert(NeedWal());

    BtrPage *rootPage = static_cast<BtrPage *>(rootBuf->GetPage());
    IndexTuple *rightDownlink = rootPage->GetIndexTuple(rootPage->GetMaxOffset());
    uint32 walSize = sizeof(WalRecordBtreeNewInternalRoot) + rightDownlink->GetSize();
    auto *rootWalData = static_cast<WalRecordBtreeNewInternalRoot *>(DstorePalloc(walSize));
    if (unlikely(rootWalData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when InsertNewRootWal."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }

    /* Begin wal atomic group */
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());

    /* Insert wal record for new root */
    rootWalData->SetHeader(rootPage, walSize, (rootPage->GetWalId() != walContext->GetWalId()), origRoot,
                           rootBuf->GetFileVersion());
    rootWalData->SetTupleData(rightDownlink);
    walContext->RememberPageNeedWal(rootBuf);
    walContext->PutNewWalRecord(rootWalData);
    DstorePfree(rootWalData);

    /* Insert wal record for btree meta */
    BtrPage *metaPage = static_cast<BtrPage *>(btrMetaBuf->GetPage());
    WalRecordBtreeUpdateMetaPage metaWalData;
    metaWalData.SetHeader(metaPage, (metaPage->GetWalId() != walContext->GetWalId()), true,
                          btrMetaBuf->GetFileVersion());
    walContext->RememberPageNeedWal(btrMetaBuf);
    walContext->PutNewWalRecord(&metaWalData);

    /* Insert wal record for original root */
    InsertChildStatusWal(m_splitBuf);

    UNUSED_VARIABLE(walContext->EndAtomicWal());
    return DSTORE_SUCC;
}

uint8 BtreeSplit::AllocAndSetTd(BtrPage *page, IndexTuple *indexTuple)
{
    uint8 tdID = INVALID_TD_SLOT;
    PdbId pdbId = this->GetPdbId();
    if (likely(m_needRecordUndo && page->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE))) {
        m_tdContext.Begin(pdbId, g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(pdbId));
        /* We'll re-alloc TD during splitting if TD space has run out. */
        tdID = page->AllocTd(m_tdContext);
        indexTuple->SetTdId(tdID);
    }
    return tdID;
}

uint8 BtreeSplit::RetryAllocAndSetTdWhenSplit(BtrPage *page, IndexTuple *indexTuple)
{
    /* Re-try allocate TD slot if failed in outer caller */
    uint8 origTdCount = page->GetTdCount();
    uint8 tdID = AllocAndSetTd(page, indexTuple);
    StorageAssert(page->CheckSanity());
    if (m_needRecordUndo && tdID == INVALID_TD_SLOT) {
        /* leftPage is empty with only TD slots and no tuples on page. Failed to AllocAndSetTd meaning the max
         * number(128) of TD slots are used off and no more extension is allowed. We have to unlock the page
         * for now and wait for some in-progress transaction ends then retry insertion. */
        m_needRetrySearchBtree = true;
        storage_set_error(INDEX_ERROR_FAIL_SPLIT_FOR_NO_TD_SPACE, m_splitBuf->GetPageId().m_blockId);
        ErrLog(DSTORE_WARNING,
            MODULE_INDEX, ErrMsg("Split btrPage(%hu, %u) failed for no td slot "
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            m_splitBuf->GetPageId().m_fileId, m_splitBuf->GetPageId().m_blockId, BTR_PAGE_HEADER_VAL(page),
            BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));
        return MAX_TD_COUNT;
    }
    return static_cast<uint8>(page->GetTdCount() - origTdCount);
}

void BtreeSplit::ClearStack()
{
    BtrStack currStack = m_leafStack;
    while (currStack != nullptr) {
        BtrStack parent = currStack->parentStack;
        DstorePfree(currStack);
        currStack = parent;
    }
    m_leafStack = nullptr;
}

void BtreeSplit::ClearPagesAfterSplit(BufferDesc *childBuf, BufferDesc *oldRBuf, BufferPoolUnlockContentFlag flag)
{
    if (childBuf != INVALID_BUFFER_DESC) {
        m_bufMgr->UnlockAndRelease(childBuf, flag);
    }

    if (oldRBuf != INVALID_BUFFER_DESC) {
        m_bufMgr->UnlockAndRelease(oldRBuf, flag);
    }
}

/*
 * The caller must hold the exclusive lock on buffer and don't generate WAL when undo this modified page.
 * We must restore the upper of page for kinds of insertion in order to keep that page is same in primary
 * and standby node. By here, the btree page has been modified and an error happens when get undo buffer.
 * So we need to undo the modified btree buffer page, but it is different with transaction abort.
 */
void BtreeSplit::HandleErrorWhenGetUndoBuffer(BtrPage *btrPage, UndoRecord *undoRec, IndexTuple *undoTuple)
{
    if (undoRec == nullptr) {
        return;
    }
    OffsetNumber offset = undoRec->GetCtid().GetOffset();
    UndoType undoType = undoRec->GetUndoType();
    if (undoType == UNDO_BTREE_INSERT || undoType == UNDO_BTREE_INSERT_TMP) {
        uint16 oldUpper = btrPage->GetUpper();
        ItemId *itemId = btrPage->GetItemIdPtr(offset);
        /*
         * For adding tuple, page->upper will minus tuple size. So we need fix upper of page. but we don't fix lower of
         * page due to replay wal algorithms.
         */
        btrPage->SetUpper(oldUpper + itemId->GetLen());
    }
    /* This UndoBtree will not change upper of page */
    BtreeUndoContext btrUndoContext{ this->GetPdbId(), btrPage->GetBtrMetaPageId(), m_indexInfo, m_bufMgr,
        btrPage->GetBtrMetaCreateXid() };
    if (STORAGE_FUNC_FAIL(btrUndoContext.InitWithBtrPage(btrPage))) {
        ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("undo btree page failed for failed to InitWithBtrPage"));
    }
    btrUndoContext.SetUnfoInfoWithIndexTuple(thrd->GetCurrentXid(), undoRec, undoTuple);
    btrUndoContext.m_offset = offset;
    /* We won't write any wal record due to the earlier error. Set m_undoWithNoWal = true to tell UndoBtree to restore
     * the page to the very original version, despite any lowkey changing */
    btrUndoContext.m_undoWithNoWal = true;
    if (STORAGE_FUNC_FAIL(btrPage->UndoBtree(undoRec, &btrUndoContext))) {
        ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("undo btree page failed"));
    }
}

} /* The end of DSTORE */
