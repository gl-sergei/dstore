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
 * dstore_btree_page_recycle.cpp
 *
 * IDENTIFICATION
 *        storage/src/index/dstore_btree_page_recycle.cpp
 *
 * ---------------------------------------------------------------------------------------*/

#include "common/memory/dstore_memory_allocator_stack.h"
#include "index/dstore_btree_wal.h"
#include "transaction/dstore_csn_mgr.h"
#include "transaction/dstore_transaction.h"
#include "wal/dstore_wal_write_context.h"
#include "index/dstore_btree_perf_unit.h"
#include "index/dstore_btree_recycle_partition.h"
#include "index/dstore_btree_page_recycle.h"

namespace DSTORE {

RetStatus BtreePageRecycle::BatchRecycleBtreePage(IndexInfo *indexInfo, ScanKey scanKey, bool *needRetry)
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreePageBatchRecycleLatency);

    RetStatus ret = DSTORE_SUCC;
    uint64 numRecyclable = 0U;
    uint64 numRecycled = 0U;
    uint64 numSkip = 0U;
    bool segmentChanged = false;
    PageId btrMetaPageId = GetBtreeSmgr()->GetBtrMetaPageId();
    bool recycledAll = true;
    bool isBgTask = !GetBtreeSmgr()->IsGlobalTempIndex() && g_storageInstance->GetGuc()->numObjSpaceMgrWorkers > 0 &&
                    m_startTime != 0;

    if (STORAGE_VAR_NULL(thrd->m_walWriterContext)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr \"AtomicWalWriterContext\" in current thread.", __FUNCTION__));
        return DSTORE_FAIL;
    }

    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    if (STORAGE_VAR_NULL(csnMgr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"CsnMgr\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    PdbId pdbId = this->GetPdbId();
    CommitSeqNo recycleCsn = csnMgr->GetRecycleCsnMin(pdbId);

    BtreeRecyclePartition *btrRecyclePartition = GetBtreeSmgr()->GetSegment()->m_btrRecyclePartition;
    if (STORAGE_VAR_NULL(btrRecyclePartition)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"BtreeRecyclePartition\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    Xid createdXid = btrRecyclePartition->createdXid;
    if (isBgTask) {
        /* Set createdXid checking on for background recycle task */
        btrRecyclePartition->SetCreatedXidCheckingOn();
    }

    PageId recyclablePageId;
    if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->GetFromRecycleQueue(recyclablePageId, recycleCsn, &numSkip))) {
        return DSTORE_FAIL;
    }

    DstoreMemoryContext recyelMemCtx = DstoreAllocSetContextCreate(thrd->m_memoryMgr->GetRoot(),
                                                                   "BtrBatchRecycleCtx",
                                                                   ALLOCSET_DEFAULT_SIZES);
    if (STORAGE_VAR_NULL(recyelMemCtx)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Failed to create BtrBatchRecycleCtx.", __FUNCTION__));
        return DSTORE_FAIL;
    }
    DstoreMemoryContext oldMemContext = DstoreMemoryContextSwitchTo(recyelMemCtx);

    while (recyclablePageId.IsValid()) {
        StorageClearError();
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent  */
            ErrLog(DSTORE_LOG, MODULE_INDEX,
                ErrMsg("BtreeRecycle task is canceled by user. segment (%hu, %u) createdXid(%d, %lu)",
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                static_cast<int32>(createdXid.m_zoneId), createdXid.m_logicSlotId));
            ret = DSTORE_FAIL;
            break;
        }
        numRecyclable++;
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("sgment(%hu, %u) poped (%hu, %u) from the RecycleQueue with recycleCsn %lu.",
                      GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                      recyclablePageId.m_fileId, recyclablePageId.m_blockId, recycleCsn));
        BufferDesc *recycleBuf = m_bufMgr->Read(pdbId, recyclablePageId, LW_EXCLUSIVE);
        if (unlikely(recycleBuf == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_LOG, MODULE_INDEX,
                ErrMsg("Failed to read recyclable btrPage (%hu, %u) from segment (%hu, %u)",
                recyclablePageId.m_fileId, recyclablePageId.m_blockId,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId));
            segmentChanged = true;
            ret = DSTORE_FAIL;
            break;
        }
        BtrPage *recyclePage = static_cast<BtrPage *>(recycleBuf->GetPage());
        if (STORAGE_VAR_NULL(recyclePage)) {
            char *clusterBufferInfo = m_bufMgr->GetClusterBufferInfo(pdbId, recyclablePageId.m_fileId,
                                                                     recyclablePageId.m_blockId);
            ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
            DstorePfreeExt(clusterBufferInfo);
            PrintBackTrace();
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Page in bufferDesc {%u, %hu, %u} is empty.",
               pdbId, recyclablePageId.m_fileId, recyclablePageId.m_blockId));
            m_bufMgr->UnlockAndRelease(recycleBuf);
            segmentChanged = true;
            ret = DSTORE_FAIL;
            break;
        }

        if (unlikely(!BtrPage::IsBtrPageValid(recyclePage, createdXid, BtrPageType::LEAF_PAGE, btrMetaPageId))) {
            ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("BtreeSegment(%hu, %u) xid(%d, %lu) has been changed after "
                "btrPage(%hu, %u) push into recycle queue. currXid(%d, %lu) pageXid(%d, %lu)"
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                static_cast<int32>(createdXid.m_zoneId), createdXid.m_logicSlotId,
                recyclablePageId.m_fileId, recyclablePageId.m_blockId,
                static_cast<int32>(GetBtreeSmgr()->GetMetaCreateXid().m_zoneId),
                GetBtreeSmgr()->GetMetaCreateXid().m_logicSlotId,
                static_cast<int32>(recyclePage->GetBtrMetaCreateXid().m_zoneId),
                recyclePage->GetBtrMetaCreateXid().m_logicSlotId, BTR_PAGE_HEADER_VAL(recyclePage),
                BTR_PAGE_LINK_AND_STATUS_VAL(recyclePage->GetLinkAndStatus())));
            m_bufMgr->UnlockAndRelease(recycleBuf);
            segmentChanged = true;
            ret = DSTORE_FAIL;
            break;
        }

        BtreePageUnlink *btrUnlink = DstoreNew(recyelMemCtx)
                                     BtreePageUnlink(recycleBuf, m_indexRel, indexInfo, scanKey);
        if (STORAGE_VAR_NULL(btrUnlink)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Failed to create btrUnlink object.", __FUNCTION__));
            ret = DSTORE_FAIL;
            recycledAll = false;
            break;
        }
        /* The CSN of each tuple might be filled when checking the page. */
        bool isEmptyAndRecyclable = btrUnlink->IsPageEmptyAndRecyclable();
        if (!isEmptyAndRecyclable) {
            m_bufMgr->UnlockAndRelease(recycleBuf);
            if (btrUnlink->IsRecyclable()) {
                ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                       ErrMsg("PageId (%hu, %u) is empty, but not dead. "
                              "Pushing it to the PruneQueue with CSN %lu.",
                              recyclablePageId.m_fileId, recyclablePageId.m_blockId,
                              btrUnlink->GetCurrMaxCsn()));
                ret = GetBtreeSmgr()->PutIntoRecycleQueue({btrUnlink->GetCurrMaxCsn(), recyclablePageId});
            }
        } else {
            /* Keep a write-lock and try unlink immediately, avoiding to lock the same page twice. */
            ret = TryRecycleBtreePage(*btrUnlink, &numRecycled);
        }
        recycledAll = !btrUnlink->IsRecyclable();
        delete btrUnlink;
        btrUnlink = nullptr;
        if (STORAGE_FUNC_FAIL(ret)) {
            segmentChanged = true;
            break;
        }
        if (isBgTask) {
            TimestampTz currTime = GetCurrentTimestampInSecond();
            if (unlikely(currTime - m_startTime > BTREE_RECYCLE_MAX_WAIT_TIME_IN_SEC)) {
                /* We've spend too much time on this single task. Return in this round and register a new task */
                recycledAll = false;
                ErrLog(DSTORE_LOG, MODULE_INDEX,
                    ErrMsg("BtreeRecycle(%s)segment(%hu, %u) createdXid(%d, %lu), recyclable:%lu recycled:%lu "
                        "skipped:%lu. Single task timeout. Will continue to recycle this btree in the next task.",
                        indexInfo->indexRelName, GetBtreeSmgr()->GetSegMetaPageId().m_fileId,
                        GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                        static_cast<int32>(createdXid.m_zoneId), createdXid.m_logicSlotId,
                        numRecyclable, numRecycled, numSkip));
                break;
            }
        }
        recycleCsn = csnMgr->GetRecycleCsnMin(pdbId);
        if (STORAGE_FUNC_FAIL(ret = GetBtreeSmgr()->GetFromRecycleQueue(recyclablePageId, recycleCsn, &numSkip))) {
            segmentChanged = true;
            break;
        }
        DstoreMemoryContextReset(recyelMemCtx);
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("BtreeRecycle(%s)segment(%hu, %u) createdXid(%d, %lu), recyclable:%lu recycled:%lu skipped:%lu %s",
        indexInfo->indexRelName, GetBtreeSmgr()->GetSegMetaPageId().m_fileId,
        GetBtreeSmgr()->GetSegMetaPageId().m_blockId, static_cast<int32>(createdXid.m_zoneId), createdXid.m_logicSlotId,
        numRecyclable, numRecycled, numSkip, segmentChanged ? "segment changed" : ""));

    if (!segmentChanged && !recycledAll && needRetry != nullptr) {
        *needRetry = true;
    }

    DstoreMemoryContextSwitchTo(oldMemContext);
    DstoreMemoryContextDelete(recyelMemCtx);
    return ret;
}

RetStatus BtreePageRecycle::TryRecycleBtreePage(BtreePageUnlink &btrUnlink, uint64 *numRecycledPages)
{
    StorageAssert(btrUnlink.GetBufDesc() != INVALID_BUFFER_DESC);

    PageId unlinkPageId = btrUnlink.GetBufDesc()->GetPageId();
    /* Allocate a Unused Slot From FreeQueue */
    FreeQueueSlot slot;
    if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->GetSlotFromFreeQueue(slot, unlinkPageId))) {
        btrUnlink.Clear(m_bufMgr);
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Failed to init recycle part when allocate slot from FreeQueue"));
        return DSTORE_FAIL;
    }
    if (!slot.IsValid()) {
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Failed to allocate slot from FreeQueue"));
        btrUnlink.Clear(m_bufMgr);
        return DSTORE_SUCC;
    }

    /* Unlink page from Btree */
    if (STORAGE_FUNC_FAIL(btrUnlink.TryUnlinkPageFromBtree())) {
        if (btrUnlink.IsRecyclable()) {
            if (btrUnlink.GetCurrMaxCsn() == INVALID_CSN) {
                btrUnlink.SetCurrMaxCsn(thrd->GetNextCsn());
            }
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                   ErrMsg("Failed to unlink page from Btree. "
                          "Pushing (%d, %u) to the RecycleQueue with CSN %lu.",
                          unlinkPageId.m_fileId, unlinkPageId.m_blockId, btrUnlink.GetCurrMaxCsn()));
            if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->PutIntoRecycleQueue({btrUnlink.GetCurrMaxCsn(), unlinkPageId}))) {
                return DSTORE_FAIL;
            }
        }
        return DSTORE_SUCC;
    }

    if (likely(numRecycledPages != nullptr)) {
        (*numRecycledPages)++;
    }

    /* Write Reusable Page into the Allocated FreeQueue Slot */
    return GetBtreeSmgr()->WriteSlotToFreeQueue(slot, unlinkPageId);
}

RetStatus BtreePageRecycle::PutIntoRecycleQueueIfEmpty(BufferDesc *pageBuf)
{
    StorageAssert(pageBuf != INVALID_BUFFER_DESC);

    PageId pageId = pageBuf->GetPageId();
    auto *page = static_cast<BtrPage *>(pageBuf->GetPage());
    BtrPageLinkAndStatus *linkStat = page->GetLinkAndStatus();
    if (!linkStat->TestLiveStatus(BtrPageLiveStatus::NORMAL_USING)) {
        m_bufMgr->UnlockAndRelease(pageBuf);
        return DSTORE_FAIL;
    }

    /* Step 1. Skip unrecyclable page */
    uint16 numNonDeletedTuple = page->GetNonDeletedTupleNum();
    if (linkStat->IsRoot() || linkStat->IsRightmost() || !linkStat->TestType(BtrPageType::LEAF_PAGE) ||
        numNonDeletedTuple > 0) {
        /* Empty page should only have hikey */
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("segment(%hu, %u) failed to recycle Btree page {%d, %u}, %d tuples in total, %d non-deleted",
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            pageId.m_fileId, pageId.m_blockId, page->GetMaxOffset(), numNonDeletedTuple));
        m_bufMgr->UnlockAndRelease(pageBuf);
        return DSTORE_FAIL;
    }

    /* Step 2. Set page live status as EMPTY_HAS_PARENT_HAS_SIB and write wal record */
    StorageAssert(linkStat->TestLiveStatus(BtrPageLiveStatus::NORMAL_USING));
    linkStat->SetLiveStatus(BtrPageLiveStatus::EMPTY_HAS_PARENT_HAS_SIB);
    StorageAssert(page->CheckSanity());

    UNUSED_VARIABLE(m_bufMgr->MarkDirty(pageBuf));
    if (NeedWal()) {
        AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
        if (STORAGE_VAR_NULL(walContext)) {
            storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                   ErrMsg("[%s] Unexpected nullptr \"AtomicWalWriterContext\" in current thread.", __FUNCTION__));
            m_bufMgr->UnlockAndRelease(pageBuf);
            return DSTORE_FAIL;
        }
        walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
        walContext->RememberPageNeedWal(pageBuf);
        WalRecordBtreeUpdateLiveStatus walRecord;
        walRecord.SetHeader(page, (page->GetWalId() != walContext->GetWalId()),
                            BtrPageLiveStatus::EMPTY_HAS_PARENT_HAS_SIB, pageBuf->GetFileVersion());
        walContext->PutNewWalRecord(&walRecord);
        UNUSED_VARIABLE(walContext->EndAtomicWal());
    }

    /* Step 3. Put empty page into pending free queue */
    if (unlikely(static_cast<uint64>(g_traceSwitch) & BTREE_STATISTIC_INFO_MIN_TRACE_LEVEL)) {
        GetBtreeSmgr()->RecordBtreeOperInLevel(BtreeOperType::BTR_OPER_MARK_RECYCLABLE, linkStat->GetLevel());
    }

    m_bufMgr->UnlockAndRelease(pageBuf);
    /* Release page and put it into FSM after releasing buffer */
    CommitSeqNo recycleCsn = thrd->GetNextCsn();
    StorageAssert(recycleCsn != INVALID_CSN && recycleCsn != MAX_COMMITSEQNO);
    if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->PutIntoRecycleQueue({recycleCsn, pageId}))) {
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("segment(%hu, %u) failed to recycle Btree page(%d, %u) csn(%lu), %d tuples in total, %d non-deleted",
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            pageId.m_fileId, pageId.m_blockId, recycleCsn, page->GetMaxOffset(), numNonDeletedTuple));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("segment(%hu, %u) succeed to recycle Btree page(%d, %u) csn(%lu), %d tuples in total, %d non-deleted",
        GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
        pageId.m_fileId, pageId.m_blockId, recycleCsn, page->GetMaxOffset(), numNonDeletedTuple));

    return DSTORE_SUCC;
}

bool BtreePageRecycle::NeedRegisterTask(const TablespaceId tbsId, const PageId segmentId, const Xid createdXid,
                                        ObjSpaceMgrTaskType type, PdbId pdbId)
{
    /* Disable any sort of btree page unlinking. */
    if (unlikely(g_storageInstance->GetGuc()->disableBtreePageRecycle)) {
        return false;
    }

    bool isTaskRegistered = false;
    uint32 recycleProb = 0U;
    if (type == ObjSpaceMgrTaskType::RECYCLE_BTREE_TASK) {
        recycleProb = static_cast<uint32>(g_storageInstance->GetGuc()->probOfRecycleBtree);
        isTaskRegistered = ObjSpaceMgr::IsRecycleBtreeTaskRegistered(tbsId, segmentId, createdXid, pdbId);
    } else if (type == ObjSpaceMgrTaskType::RECLAIM_BTREE_RECYCLE_PARTITION_TASK) {
        recycleProb = static_cast<uint32>(g_storageInstance->GetGuc()->probOfRecycleFsm);
        isTaskRegistered = ObjSpaceMgr::IsReclaimColdBtrRecyclePartTaskRegistered(tbsId, segmentId, createdXid, pdbId);
    }
    if (isTaskRegistered) {
        return false;
    }

    uint8 randomNum = 0;
    if (STORAGE_FUNC_FAIL(DstoreGetRandomNum(randomNum))) {
        /* Don't do Recycle task if anything wrong here */
        return false;
    }

    static constexpr uint8 PERCENTAGE = 100;
    return ((randomNum % PERCENTAGE) < recycleProb);
}

RetStatus BtreePageRecycle::TryRegisterRecycleBtreeTask(const Xid createdXid, IndexInfo *indexInfo, ScanKey scanKey,
                                                        TablespaceId tbsId, const PageId segmentId, PdbId pdbId)
{
    if (indexInfo->btrIdxStatus != BtrCcidxStatus::NOT_CCINDEX) {
        /* Do not start recycle task until ccindex finished */
        return DSTORE_SUCC;
    }
    bool needRecycleBtreeTask =
        NeedRegisterTask(tbsId, segmentId, createdXid, ObjSpaceMgrTaskType::RECYCLE_BTREE_TASK, pdbId);
    if (!needRecycleBtreeTask) {
        return DSTORE_SUCC;
    }

    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr \"StoragePdb\" for pdbId:%u.", __FUNCTION__, pdbId));
        return DSTORE_FAIL;
    }
    ObjSpaceMgr *objSpaceMgr = pdb->GetObjSpaceMgr();
    if (STORAGE_VAR_NULL(objSpaceMgr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr \"ObjSpaceMgr\" for pdbId:%u.", __FUNCTION__, pdbId));
        return DSTORE_FAIL;
    }
    /* taskInfo does not own the memory of indexInfo and scanKey, don't free them in destructor */
    bool needFree = false;
    ObjSpaceMgrRecycleBtreeTaskInfo taskInfo(pdbId, tbsId, segmentId, createdXid, indexInfo, scanKey);
    taskInfo.NeedFreeIndexMeta(needFree);

    return objSpaceMgr->RegisterObjSpaceMgrTaskIfNeeded(&taskInfo);
}

RetStatus BtreePageRecycle::TryRegisterRecycleBtreeTask(ObjSpaceMgrTaskInfo *taskInfo)
{
    if (unlikely(taskInfo->GetTaskType() != ObjSpaceMgrTaskType::RECYCLE_BTREE_TASK)) {
        return DSTORE_FAIL;
    }
    ObjSpaceMgrRecycleBtreeTaskInfo *recycleBtreeInfo = static_cast<ObjSpaceMgrRecycleBtreeTaskInfo *>(taskInfo);
    Xid createdXid = recycleBtreeInfo->m_createdXid;
    IndexInfo *indexInfo = recycleBtreeInfo->m_indexInfo;
    ScanKey scanKey = recycleBtreeInfo->m_scanKey;
    TablespaceId tbsId = recycleBtreeInfo->GetTablespaceId();
    PageId segmentId = recycleBtreeInfo->GetSegmentId();
    PdbId pdbId = recycleBtreeInfo->GetPdbId();

    if (STORAGE_VAR_NULL(indexInfo) || STORAGE_VAR_NULL(scanKey) || unlikely(segmentId.IsInvalid())) {
        return DSTORE_FAIL;
    }

    return BtreePageRecycle::TryRegisterRecycleBtreeTask(createdXid, indexInfo, scanKey, tbsId, segmentId, pdbId);
}

RetStatus BtreePageRecycle::TryRegisterColdRecyclePartitionReclaimTask(TablespaceId tbsId, const PageId segmentId,
                                                                       const Xid createdXid, PdbId pdbId)
{
    /* We cannot recycle a RecycleBtreePartition in a single node since there's only one partition */
    if (g_storageInstance->GetType() == StorageInstanceType::SINGLE) {
        return DSTORE_SUCC;
    }
    bool needTask = NeedRegisterTask(tbsId, segmentId, createdXid,
                                     ObjSpaceMgrTaskType::RECLAIM_BTREE_RECYCLE_PARTITION_TASK, pdbId);
    if (!needTask) {
        return DSTORE_SUCC;
    }

    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr \"StoragePdb\" for pdbId:%u.", __FUNCTION__, pdbId));
        return DSTORE_FAIL;
    }
    ObjSpaceMgr *objSpaceMgr = pdb->GetObjSpaceMgr();
    if (STORAGE_VAR_NULL(objSpaceMgr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr \"ObjSpaceMgr\" for pdbId:%u.", __FUNCTION__, pdbId));
        return DSTORE_FAIL;
    }
    ObjSpaceMgrReclaimBtrRecyclePartTaskInfo taskInfo(pdbId, tbsId, segmentId, createdXid);
    return objSpaceMgr->RegisterObjSpaceMgrTaskIfNeeded(&taskInfo);
}

void BtreeRecycleWorker::BtreeRecycleThreadMain()
{
    InitSignalMask();
    void *sqlThrd = nullptr;
    SQLThrdInitCtx context = {m_pdbId, "IndexRecyle", InternalThreadType::THREAD_INDEX_RECYCLE, &sqlThrd, false};
    bool initResult = g_storageInstance->InitSQLThreadContext(&context);
    if (!initResult) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("BtreeRecycleThreadMain sql thread init fail, pdbId[%u]", m_pdbId));
        return;
    }

    if (thrd == nullptr) {
        g_storageInstance->CreateThreadAndRegister(m_pdbId, false, "IndexRecyle", true,
                                                   ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    }
    if (unlikely(thrd == nullptr)) {
        return;
    }
    thrd->SetNeedCommBuffer(true);
    thrd->RefreshWorkingVersionNum();
#ifdef __aarch64__
    thrd->SetNumaId(1);
#endif
    Transaction *txn = thrd->GetActiveTransaction();
    if (unlikely(STORAGE_VAR_NULL(txn))) {
        (void)thrd->InitTransactionRuntime(m_pdbId, nullptr, nullptr, false);
        txn = thrd->GetActiveTransaction();
    }

    if (unlikely(txn == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("BtreeRecycleThreadMain get txn fail, pdbId[%u]", m_pdbId));
        goto EXIT;
    }
    m_recycleThreadId = thrd->GetThreadId();
    m_recycleThreadStartTime = thrd->GetCore()->startTime;
    m_recycleThreadCore = thrd->GetCore();

    while (!m_stopRecyleThread) {
        thrd->Sleep();
        ObjSpaceMgrTask* task = m_btrRecycleTask.load(std::memory_order_acquire);
        if (task) {
            m_btreeRecycleResult = task->ExecuteRecycleBtreeTask();
            m_btrRecycleTask.store(nullptr, std::memory_order_release);
        }
    }

EXIT:
    g_storageInstance->ReleaseSQLThreadContext(&context);
    if (thrd != nullptr) {
        g_storageInstance->UnregisterThread();
    }
}

RetStatus BtreeRecycleWorker::BtreeRecycleExecute(ObjSpaceMgrTask *task)
{
    /* Step 1. Check if background thread alive */
    if (unlikely(!IsThreadValid())) {
        /* Asyn thread has exited, we need to create a new thread */
        if (m_recycleThread) {
            m_recycleThread->detach();
            delete m_recycleThread;
        }
        m_recycleThread = nullptr;
        m_recycleThread = new std::thread(&BtreeRecycleWorker::BtreeRecycleThreadMain, this);
        ErrLog(DSTORE_LOG, MODULE_INDEX,
               ErrMsg("[BtrRecycle] BtreeRecycleExecute re-start workerId[%u], pdbId[%u]", m_workeId, m_pdbId));
    }

    /* Step 2. Wait until the background thread ready to execute recycle task */
    static int64 MAX_WAIT_INIT_THREAD_TIMES = 120; /* Wait at most 120s for thread init */
    int initWaitCounter = 0;
    while (m_recycleThread != nullptr && initWaitCounter < MAX_WAIT_INIT_THREAD_TIMES) {
        if (likely(IsThreadValid())) {
            break;
        }
        /* wait for child thread init completely. */
        GaussUsleep(NANO_PER_MILLI_SEC);
        initWaitCounter++;
    }

    if (unlikely(!IsThreadValid())) {
        ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("[BtrRecycle] Failed to start BtreeRecycleWorker for recycle workerId[%u], pdbId[%u]. ",
                   m_workeId, m_pdbId));
        (void)BtreePageRecycle::TryRegisterRecycleBtreeTask(task->m_taskInfo);
        return DSTORE_FAIL;
    }

    /* Step 3. Set task and notice bg thread to start tastk */
    SetTask(task);
    m_recycleThreadCore->futex.DstoreFutexPost();

    /* Step 4. Check bg thread's result */
    TimestampTz taskStartTime = GetCurrentTimestampInSecond();
    TimestampTz currTime = taskStartTime;
    while (IsThreadValid()) {
        if (m_btrRecycleTask.load(std::memory_order_acquire)) {
            /* wait for recycle task finish */
            GaussUsleep(MICRO_PER_MILLI_SEC);
            m_waitCnt++;
            if (m_waitCnt % BTREE_RECYCLE_WORKER_WAIT_INTERVAL == 0) {
                ErrLog(DSTORE_LOG, MODULE_INDEX,
                       ErrMsg("[BtrRecycle] BtreeRecycleExecute wait for recycle task finish workerId[%u], pdbId[%u], "
                              "waitCnt[%u]",
                              m_workeId, m_pdbId, m_waitCnt));
            }
            currTime = GetCurrentTimestampInSecond();
            if (unlikely(currTime - taskStartTime > BTREE_RECYCLE_MAX_WAIT_TIME_IN_SEC)) {
                /* Waiting timeout. */
                BtreeRecycleWorkerStop();
                break;
            }
            continue;
        }
        return m_btreeRecycleResult;
    }

    ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("[BtrRecycle] BtreeRecycleExecute worker execute fail, workeId[%u], pdbId[%u]", m_workeId, m_pdbId));

    return DSTORE_FAIL;
}
}
