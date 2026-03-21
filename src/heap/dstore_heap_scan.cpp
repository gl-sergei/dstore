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
 * dstore_heap_scan.cpp
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        storage/src/heap/dstore_heap_scan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "heap/dstore_heap_scan.h"
#include "heap/dstore_heap_perf_unit.h"
#include "common/dstore_datatype.h"
#include "framework/dstore_thread.h"
#include "framework/dstore_instance.h"
#include "common/memory/dstore_mctx.h"
#include "common/log/dstore_log.h"
#include "common/dstore_common_utils.h"
#include "transaction/dstore_transaction.h"
#include "catalog/dstore_function.h"
#include "page/dstore_heap_page.h"
#include "errorcode/dstore_buf_error_code.h"
#include "common/fault_injection/dstore_heap_fault_injection.h"
#include "fault_injection/fault_injection.h"

namespace DSTORE {

HeapScanHandler::HeapScanHandler(StorageInstance *instance, ThreadContext *thread,
                                 StorageRelation heapRel, bool isLobOperation, bool isUseRingBuf /* = false */)
    : HeapHandler(instance, thread, heapRel, isLobOperation, isUseRingBuf),
      m_startFlag(false),
      m_scanOver(false),
      m_segScanContext(nullptr),
      m_curOffset(INVALID_ITEM_OFFSET_NUMBER),
      m_localCrPage(nullptr),
      m_crBufDesc(INVALID_BUFFER_DESC),
      m_bigTuple(nullptr),
      m_curPageId(INVALID_PAGE_ID),
      m_curPageDesc(INVALID_BUFFER_DESC),
      m_numScanKey(0),
      m_scanKey(nullptr),
      m_tupDesc(nullptr),
      m_showAnyTupForDebug(nullptr)
{
    m_resTuple.m_diskTuple = nullptr;
    m_snapshot.Init();
}

RetStatus HeapScanHandler::Begin(Snapshot snapshot, bool showAnyTupForDebug)
{
    StorageReleasePanic(GetTableSmgr() == nullptr, MODULE_HEAP, ErrMsg("Invalid table smgr."));
    m_resTuple.SetDiskTupleSize(0);
    m_resTuple.SetDiskTuple(nullptr);
    StorageAssert(m_segScanContext == nullptr);

    if (m_useRingBuf) {
        m_ringBuf = CreateBufferRing(BufferAccessType::BAS_BULKREAD);
        StorageReleasePanic(m_ringBuf == nullptr, MODULE_HEAP, ErrMsg("Invalid buffer ring."));
    }
    StorageReleasePanic(snapshot == nullptr, MODULE_HEAP, ErrMsg("Invalid snapshot."));
    m_snapshot = *snapshot;
    if (unlikely(showAnyTupForDebug)) {
        ShowAnyTupleContext::CreateShowAnyTupleContext(m_heapRel->m_pdbId, m_showAnyTupForDebug);
    }
    return DSTORE_SUCC;
}

void HeapScanHandler::ReScan()
{
    if (m_segScanContext == nullptr) {
        m_segScanContext = DstoreNew(m_thrd->GetTopTransactionMemoryContext())
            DataSegmentScanContext(m_bufMgr, m_heapRel, m_isLob ? SmgrType::LOB_SMGR : SmgrType::HEAP_SMGR);
        if (m_segScanContext == NULL) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("ReScan failed because of Memory allocation failure!"));
            return;
        }
    }
    StorageAssert(m_segScanContext != nullptr);
    m_segScanContext->Reset();
    m_segScanContext->SetRescan();
    ClearLastScanBigTuple();
    ReleaseLastBuffer();
    m_startFlag = false;
    m_scanOver = false;

    if (unlikely(m_useRingBuf && m_ringBuf == nullptr)) {
        m_ringBuf = CreateBufferRing(BufferAccessType::BAS_BULKREAD);
        StorageReleasePanic(m_ringBuf == nullptr, MODULE_HEAP, ErrMsg("Invalid buffer ring."));
    } else if (unlikely(!m_useRingBuf && m_ringBuf != nullptr)) {
        DestroyBufferRing(&m_ringBuf);
        StorageReleasePanic(m_ringBuf != nullptr, MODULE_HEAP, ErrMsg("Buffer ring is not null."));
    }

    if (unlikely(m_showAnyTupForDebug != nullptr)) {
        m_showAnyTupForDebug->CheckBeforeItemFetch(INVALID_ITEM_POINTER);
    }
}

void HeapScanHandler::ResetController()
{
    if (m_segScanContext == nullptr) {
        m_segScanContext = DstoreNew(m_thrd->GetTopTransactionMemoryContext())
            DataSegmentScanContext(m_bufMgr, m_heapRel, m_isLob ? SmgrType::LOB_SMGR : SmgrType::HEAP_SMGR);
        if (m_segScanContext == NULL) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("ResetController failed because of Memory allocation failure!"));
            return;
        }
    }
    m_segScanContext->SetRescan();
}

RetStatus HeapScanHandler::SetParallelController(ParallelWorkController *controller, int smpId)
{
    if (m_segScanContext == nullptr) {
        m_segScanContext = DstoreNew(m_thrd->GetTopTransactionMemoryContext())
            DataSegmentScanContext(m_bufMgr, m_heapRel, m_isLob ? SmgrType::LOB_SMGR : SmgrType::HEAP_SMGR);
        if (m_segScanContext == NULL) {
            m_failureInfo.SetReason(HeapHandlerFailureReason::READ_BUFFER_FAILED);
            return DSTORE_FAIL;
        }
    }
    StorageAssert(m_segScanContext);
    if (!controller->IsInitialized()) {
        /* If the controller fail to initialize workload, don't go into parallel mode */
        if (STORAGE_FUNC_FAIL(controller->InitParallelHeapScan(
            m_heapRel->m_pdbId, GetTableSmgr()->GetSegment()->GetSegmentMetaPageId()))) {
            return DSTORE_FAIL;
        }
    }
    StorageAssert(GetTableSmgr()->GetSegment()->GetSegmentMetaPageId() == controller->GetMetaPageId());
    m_segScanContext->SetParallelController(controller, smpId);
    m_thrd->GetActiveTransaction()->SetCurrentXid(controller->GetTransactionId());
    return DSTORE_SUCC;
}

void HeapScanHandler::SetScanKey(TupleDesc desc, int nkeys, ScanKey key)
{
    /* sanity check */
    StorageAssert(nkeys != 0);

    /* fill scan key information if given */
    m_numScanKey = nkeys;
    m_scanKey = key;
    m_tupDesc = desc;
}

void HeapScanHandler::End() noexcept
{
    ClearScanKey();
    ClearLastScanBigTuple();
    ReleaseLastBuffer();
    m_tupDesc = nullptr;
    m_startFlag = true;
    m_scanOver = true;
    delete m_segScanContext;
    m_segScanContext = nullptr;

    if (m_ringBuf != nullptr) {
        DestroyBufferRing(&m_ringBuf);
        StorageReleasePanic(m_ringBuf != nullptr, MODULE_HEAP, ErrMsg("Buffer ring is not null."));
    }

    if (m_localCrPage != nullptr) {
        DstorePfreeExt(m_localCrPage);
    }

    if (unlikely(m_showAnyTupForDebug != nullptr)) {
        ShowAnyTupleContext::DestoryShowAnyTupleContext(m_showAnyTupForDebug, false);
    }
}

void HeapScanHandler::ClearScanKey() noexcept
{
    if (m_scanKey != nullptr) {
        DstorePfree(m_scanKey);
        m_scanKey = nullptr;
        m_numScanKey = 0;
    }
}

void HeapScanHandler::ClearLastScanBigTuple() noexcept
{
    if (m_bigTuple != nullptr) {
        DstorePfree(m_bigTuple);
        m_bigTuple = nullptr;
    }
}

/*
 * SeqScan() will copy page to local buffer which m_resTuple points to,
 * so do not need to free space for m_resTuple.
 */
HeapTuple *HeapScanHandler::SeqScan()
{
    HeapTuple *tuple = nullptr;
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    if (m_segScanContext == nullptr) {
        m_segScanContext = DstoreNew(m_thrd->GetTopTransactionMemoryContext())
            DataSegmentScanContext(m_bufMgr, m_heapRel, m_isLob ? SmgrType::LOB_SMGR : SmgrType::HEAP_SMGR);
        if (m_segScanContext == NULL) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Begin scan failed because of Memory allocation failure!"));
            return tuple;
        }
    }

    if (unlikely(m_localCrPage == nullptr)) {
        m_localCrPage = static_cast<HeapPage *>(DstorePalloc(sizeof(HeapPage)));
        if (unlikely(m_localCrPage == nullptr)) {
            m_failureInfo.SetReason(HeapHandlerFailureReason::READ_BUFFER_FAILED);
            return tuple;
        }
    }
    do {
        /* Step 0. free last scanned big tuple */
        ClearLastScanBigTuple();

        /* Step 1. fetch the next tuple out */
        tuple = likely(m_showAnyTupForDebug == nullptr) ? SeqScanNext() : SeqScanForShowAnyTup();
        /* Step 2. break the loop if we got nothing */
        if (tuple == nullptr) {
            if (m_failureInfo.reason == HeapHandlerFailureReason::READ_BUFFER_FAILED) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP,
                    ErrMsg("Scan failed due to read invalid buffer. The number of pushed down scan keys is %d.",
                            m_numScanKey));
                return tuple;
            }
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP,
                ErrMsg("Tuple not found. The number of pushed down scan keys is %d.", m_numScanKey));
            break;
        }

        /* Step  3. Do the key test if we have any */
        bool match = true;
        if (m_numScanKey != 0 && m_scanKey != nullptr) {
            match = ExecMatchScanKey(tuple);
        }

        /* Step 4. If the tuple passed the key test, break the loop */
        if (match) {
            break;
        }
    } while (true);

    /* Step 5. Finally, return fetched tuple here */
    StorageAssert(((tuple == nullptr) || (tuple->GetDiskTupleSize() != 0)));

    /* Temp: Clears the error set by the internal normal process of the sequence scan. */
    if (unlikely(StorageIsErrorSet() && (tuple != nullptr || m_scanOver))) {
        StorageClearError();
    }
    ReportHeapBufferReadStat(m_heapRel);
    return tuple;
}

HeapTuple *HeapScanHandler::SeqScanNext()
{
    PageId oldPageId = INVALID_PAGE_ID;
    HeapPage *crPage = nullptr;
    HeapTuple *tuple = nullptr;

    while (!m_scanOver) {
        /* Step 1. determine the crPage, current or next one. */
        if (STORAGE_FUNC_FAIL(PrepareValidCrPage(crPage, oldPageId))) {
            return nullptr;
        }
        if (unlikely(crPage == nullptr)) {
            /* scan over */
            break;
        }

        /* Step 2. determine the offset. */
        m_curOffset = (crPage->GetSelfPageId() == oldPageId) ? m_curOffset + 1 : FIRST_ITEM_OFFSET_NUMBER;

        /*
         * Step 3. Fetch the next tuple we want.
         * We should skip dead and aborted items, also the invisible(delete or non-inplace-update) tuples.
         */
        ItemId *itemId = crPage->GetItemIdPtr(m_curOffset);
        StorageAssert(itemId != nullptr);
        if (itemId->IsUnused() || itemId->IsNoStorage()) {
            continue;
        }

        crPage->GetTuple(&m_resTuple, m_curOffset);
        StorageAssert(m_resTuple.GetDiskTuple() != nullptr);
        StorageReleasePanic(m_resTuple.GetDiskTuple()->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_SAME_PAGE_UPDATE),
                            MODULE_HEAP, ErrMsg("Invalid heap disk tuple live mode(OLD_TUPLE_BY_SAME_PAGE_UPDATE)."));
        if (m_resTuple.GetDiskTuple()->TestLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_DELETE) ||
            m_resTuple.GetDiskTuple()->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE)) {
            continue;
        }
        tuple = &m_resTuple;

        /* Step 4. Check if fetched tuple belongs to a big tuple */
        HeapDiskTuple *diskTuple = m_resTuple.GetDiskTuple();
        StorageAssert(diskTuple != nullptr);
        if (diskTuple->IsLinked()) {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Scan big tuple({%hu, %u}, %hu).",
                tuple->GetCtid()->GetFileId(), tuple->GetCtid()->GetBlockNum(), tuple->GetCtid()->GetOffset()));
            if (diskTuple->IsFirstLinkChunk()) {
                /* It is the first chunk of a linked tuple, palloc a big tuple here */
                m_bigTuple = FetchBigTuple(tuple, true);
                tuple = m_bigTuple;
            } else {
                /* For other chunks of a linked tuple, ignore it and scan the next tuple */
                tuple = nullptr;
                continue;
            }
        }

        /* Step 5: a complelted tuple found. */
        break;
    }

    return tuple;
}

RetStatus HeapScanHandler::PrepareValidCrPage(HeapPage *&crPage, PageId &oldPageId)
{
    /* Step 1: init curId. */
    PageId curId = INVALID_PAGE_ID;
    StorageAssert(m_segScanContext != nullptr);

    if (likely(m_startFlag)) {
        if (m_crBufDesc != INVALID_BUFFER_DESC) {
            crPage = static_cast<HeapPage *>(m_crBufDesc->GetPage());
            StorageAssert(crPage != nullptr);
            oldPageId = crPage->GetSelfPageId();
        } else {
            oldPageId = m_localCrPage->GetSelfPageId();
            crPage = m_localCrPage;
        }

        StorageAssert(crPage != nullptr);
        if (m_curOffset < crPage->GetMaxOffset()) {
            return DSTORE_SUCC;
        }
        curId = m_segScanContext->GetNextPageId();
    } else {
        curId = m_segScanContext->GetFirstPageId();
        m_startFlag = true;
    }

    /* Step 2: find the next available basebuffer. */
    while (!m_scanOver) {
        if (unlikely(!curId.IsValid())) {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Sequence scan finished."));
            m_scanOver = true;
            crPage = nullptr;
            return DSTORE_SUCC;
        }

        FAULT_INJECTION_WAIT(DstoreHeapFI::CONSTRUCT_CR_BEFORE_READ_BASE_PAGE);

        if (m_crBufDesc != INVALID_BUFFER_DESC) {
            m_bufMgr->Release(m_crBufDesc);
            m_crBufDesc = INVALID_BUFFER_DESC;
        }

        /*
         * Be sure to check for interrupts at least once per page.
         */
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent  */
            return DSTORE_FAIL;
        }

        ConsistentReadContext crContext;
        crContext.pdbId = m_heapRel->m_pdbId;
        crContext.pageId = curId;
        crContext.currentXid = m_thrd->GetActiveTransaction()->GetCurrentXid();
        crContext.snapshot = &m_snapshot;
        crContext.dataPageExtraInfo = nullptr;
        crContext.destPage = m_localCrPage;
        crContext.crBufDesc = nullptr;

        RetStatus ret = m_bufMgr->ConsistentRead(crContext, m_ringBuf);
        if (STORAGE_FUNC_FAIL(ret)) {
            /*
             * hack for workaround fix: there are a few fsm pages in data extent, so we have to skip the pages. Fix it
             *                          after the fsm pages have been removed from data extent.
             */
            if (StorageGetErrorCode() == BUFFER_INFO_CONSTRUCT_CR_NOT_DATA_PAGE) {
                curId = m_segScanContext->GetNextPageId();
                continue;
            }
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Construct cr error."));
            m_failureInfo.SetReason(HeapHandlerFailureReason::READ_BUFFER_FAILED);
            return ret;
        }

        if (crContext.crBufDesc != nullptr) {
            m_crBufDesc = crContext.crBufDesc;
            StorageAssert(m_crBufDesc->IsCrPage());
            crPage = static_cast<HeapPage *>(m_crBufDesc->GetPage());
        } else {
            crPage = m_localCrPage;
        }

        /* If current page is empty, just skip it. */
        if (crPage->GetMaxOffset() == 0) {
            curId = m_segScanContext->GetNextPageId();
            continue;
        }

        CheckBufferedPage(crPage, curId);
        StorageAssert(crPage->CheckSanity());
        break;
    }

    return DSTORE_SUCC;
}

template<bool isInProgress>
RetStatus HeapScanHandler::TupleStatByLiveMode(HeapSampleScanContext *sampleScanContext, HeapPage *curHeapPage,
    int &liveRows, int &deadRows, int &numTuples)
{
    HeapDiskTupLiveMode mode = m_resTuple.m_diskTuple->GetLiveMode();
    switch (mode) {
        case HeapDiskTupLiveMode::TUPLE_BY_NORMAL_DELETE:
        case HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE: {
            if (m_resTuple.m_diskTuple->IsLinked() && !m_resTuple.m_diskTuple->IsFirstLinkChunk()) {
                break;
            }
            isInProgress ? liveRows++ : deadRows++;
            break;
        }
        case HeapDiskTupLiveMode::TUPLE_BY_NORMAL_INSERT:
        case HeapDiskTupLiveMode::NEW_TUPLE_BY_SAME_PAGE_UPDATE:
        case HeapDiskTupLiveMode::NEW_TUPLE_BY_ANOTHER_PAGE_UPDATE: {
            if (m_resTuple.m_diskTuple->IsLinked() && !m_resTuple.m_diskTuple->IsFirstLinkChunk()) {
                break;
            }
            if (!isInProgress) {
                RetStatus status = StatEndtuple(sampleScanContext, curHeapPage, isInProgress, liveRows, numTuples);
                if (status == DSTORE_FAIL) {
                    return DSTORE_FAIL;
                }
            }
            break;
        }
        case HeapDiskTupLiveMode::NEW_TUPLE_BY_INPLACE_UPDATE: {
            RetStatus status =
                StatTupleByInplaceUpdate(sampleScanContext, curHeapPage, isInProgress, liveRows, numTuples);
            if (status == DSTORE_FAIL) {
                return DSTORE_FAIL;
            }
            break;
        }
        case HeapDiskTupLiveMode::OLD_TUPLE_BY_SAME_PAGE_UPDATE: {
            ErrLog(DSTORE_PANIC,
                   MODULE_HEAP, ErrMsg("Invalid heap disk tuple live mode(OLD_TUPLE_BY_SAME_PAGE_UPDATE) when scan."));
            break;
        }
        default: {
            ErrLog(DSTORE_PANIC, MODULE_HEAP, ErrMsg("Unknown tuple live mode(%d).", static_cast<int>(mode)));
            break;
        }
    }

    return DSTORE_SUCC;
}

RetStatus HeapScanHandler::TupleNumStat(HeapSampleScanContext *sampleScanContext, HeapPage *curHeapPage,
    int &liveRows, int &deadRows, int &numTuples)
{
    m_curOffset = FIRST_ITEM_OFFSET_NUMBER;
    while (m_curOffset <= curHeapPage->GetMaxOffset()) {
        /* Step 3.1: Get ItemId of current tuple first */
        ItemId *curItemId = curHeapPage->GetItemIdPtr(m_curOffset);
        /* Step 3.2: Jump over unused ItemId or known recycled tuple */
        if (!curItemId->IsNormal()) {
            /* for recycled tuple, we count it as deleted one */
            if (curItemId->IsNoStorage()) {
                deadRows++;
            }

            m_curOffset++;
            continue;
        }

        /* Step 3.3: Fetch cur tuple to examine its status */
        curHeapPage->GetTuple(&m_resTuple, m_curOffset);
        uint8 tdId = m_resTuple.GetTdId();
        TD *td = curHeapPage->GetTd(tdId);

        /*
         * The tuple state must not be aborted, because we use the synchronous rollback policy.
         * So we just check whether the tuple is in progress.
         */
        bool isInProgress;
        if (!m_resTuple.m_diskTuple->TestTdStatus(ATTACH_TD_AS_NEW_OWNER)) {
            isInProgress = false;
        } else {
            XidStatus xidStatus(td->GetXid(), m_thrd->GetActiveTransaction(), td);
            isInProgress = xidStatus.IsInProgress();
        }

        if (isInProgress) {
            RetStatus status =
            TupleStatByLiveMode<true>(sampleScanContext, curHeapPage, liveRows, deadRows, numTuples);
            if (status == DSTORE_FAIL) {
                return DSTORE_FAIL;
            }
        } else {
            RetStatus status =
                TupleStatByLiveMode<false>(sampleScanContext, curHeapPage, liveRows, deadRows, numTuples);
            if (status == DSTORE_FAIL) {
                return DSTORE_FAIL;
            }
        }

        /* Step 3.4 Advance current offset to scan next tuple */
        m_curOffset++;
    }

    return DSTORE_SUCC;
}
RetStatus HeapScanHandler::SampleScan(HeapSampleScanContext* sampleScanContext)
{
    int liveRows = 0;
    int deadRows = 0;
    int numTuples = 0;
    int blockNum = sampleScanContext->curBlockNum - sampleScanContext->lastBlockNum;
    HeapPage *curHeapPage;
    RetStatus status = DSTORE_SUCC;

    if (m_segScanContext == nullptr) {
        m_segScanContext = DstoreNew(m_thrd->GetTopTransactionMemoryContext())
            DataSegmentScanContext(m_bufMgr, m_heapRel, m_isLob ? SmgrType::LOB_SMGR : SmgrType::HEAP_SMGR);
        if (m_segScanContext == NULL) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Begin scan failed because of Memory allocation failure!"));
            return DSTORE_FAIL;
        }
    }
    /* Step 1: Find destinated page we want */
    if (sampleScanContext->curPageId == INVALID_PAGE_ID) {
        sampleScanContext->curPageId = m_segScanContext->GetFirstPageId();
    }

    for (; blockNum > 0; blockNum--) {
        sampleScanContext->curPageId = m_segScanContext->GetNextPageId();
    }
    /* Make sure we have a valid page id to find */
    StorageAssert(sampleScanContext->curPageId != INVALID_PAGE_ID);

    /* Step 2: Fetch the destinated page. */
    m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, sampleScanContext->curPageId, LW_SHARED);
    if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("Read page(%hu, %u) failed when sample scan.", sampleScanContext->curPageId.m_fileId,
                      sampleScanContext->curPageId.m_blockId));
        return DSTORE_FAIL;
    }
    Page *curPage = m_bufferDesc->GetPage();
    if (curPage->TestType(PageType::FSM_META_PAGE_TYPE) || curPage->TestType(PageType::FSM_PAGE_TYPE)) {
        /*
         * Under FSM partition, we may enconter FSM Meta page or FSM data page here.
         * Since they are not HeapPage, they do not contain any tuple, and we don't
         * scan them here. Just tell the caller no tuple on this page directly
         */
        goto finish;
    } else {
        curHeapPage = static_cast<HeapPage *>(m_bufferDesc->GetPage());
        CheckBufferedPage(curHeapPage, sampleScanContext->curPageId);
        StorageAssert(curHeapPage->CheckSanity());
    }

    /* Step 3: Scan through all tuples in this page and collect informations */
    status = TupleNumStat(sampleScanContext, curHeapPage, liveRows, deadRows, numTuples);
    if (status == DSTORE_FAIL) {
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        m_bufferDesc = INVALID_BUFFER_DESC;
        return DSTORE_FAIL;
    }

finish:
    /* Step 4: Release acquired base buffer after scanning */
    m_bufMgr->UnlockAndRelease(m_bufferDesc);
    m_bufferDesc = INVALID_BUFFER_DESC;

    /* Step 5: Record all informations into context so caller will get it */
    sampleScanContext->numLiveTuples = liveRows;
    sampleScanContext->numDeadTuples = deadRows;
    sampleScanContext->numTuples = numTuples;
    ReportHeapBufferReadStat(m_heapRel);
    return status;
}

RetStatus HeapScanHandler::StatTupleByInplaceUpdate(HeapSampleScanContext *sampleScanContext, HeapPage *curHeapPage,
    bool isInProgress, int &liveRows, int &numTuples)
{
    /*
     * For inplace-updated tuple, we check it separately. We have following cases:
     *     1. Updated by an in-process or aborted transaction, fetch visible tuple from undo.
     *     2. Otherwise, count it as a live tuple and return it for later sampling.
     */
    if (m_resTuple.m_diskTuple->IsLinked() && !m_resTuple.m_diskTuple->IsFirstLinkChunk()) {
        return DSTORE_SUCC;
    }

    if (!isInProgress) {
        RetStatus retStatus =
            HeapScanHandler::StatEndtuple(sampleScanContext, curHeapPage, isInProgress, liveRows, numTuples);
        if (retStatus != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
        return DSTORE_SUCC;
    }

    return StatVisibleTuple(sampleScanContext, curHeapPage, liveRows, numTuples);
}

RetStatus HeapScanHandler::StatEndtuple(HeapSampleScanContext *sampleScanContext, HeapPage *curHeapPage,
    bool isInProgress, int &liveRows, int &numTuples)
{
    if (isInProgress) {
        return DSTORE_SUCC;
    }

    if (m_resTuple.m_diskTuple->IsLinked()) {
        RetStatus status = StatVisibleTuple(sampleScanContext, curHeapPage, liveRows, numTuples);
        if (status == DSTORE_FAIL) {
            return DSTORE_FAIL;
        }
    } else {
        liveRows++;
        sampleScanContext->tuples[numTuples] = m_resTuple.Copy();
        if (sampleScanContext->tuples[numTuples] == nullptr) {
            for (int i = 0; i < numTuples; i++) {
                DstorePfreeExt(sampleScanContext->tuples[i]);
            }
            return DSTORE_FAIL;
        }
        numTuples++;
    }
    return DSTORE_SUCC;
}

RetStatus HeapScanHandler::StatVisibleTuple(HeapSampleScanContext *sampleScanContext, HeapPage *curHeapPage,
    int &liveRows, int &numTuples)
{
    HeapTuple *visibleTuple = curHeapPage->GetVisibleTuple(
    m_heapRel->m_pdbId, m_thrd->GetActiveTransaction(), *m_resTuple.GetCtid(), &m_snapshot, m_isLob);
    if (visibleTuple != nullptr) {
        liveRows++;

        /* check if this tuple is a BigTuple again */
        if (visibleTuple->m_diskTuple->IsLinked()) {
            StorageAssert(visibleTuple->m_diskTuple->IsFirstLinkChunk());
            sampleScanContext->tuples[numTuples] = FetchBigTuple(visibleTuple, true);
            DstorePfree(visibleTuple);
            if (sampleScanContext->tuples[numTuples] == nullptr) {
                for (int i = 0; i < numTuples; i++) {
                    DstorePfreeExt(sampleScanContext->tuples[i]);
                }
                return DSTORE_FAIL;
            }
        } else {
            sampleScanContext->tuples[numTuples] = visibleTuple;
        }
        numTuples++;
    } else if (StorageGetErrorCode() == COMMON_ERROR_MEMORY_ALLOCATION) {
        for (int i = 0; i < numTuples; i++) {
            DstorePfreeExt(sampleScanContext->tuples[i]);
        }
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when stat tuple by inplace update."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

/*
 * FetchTuple() will create space and copy tuple to resTuple,
 * so need to call DstorePfree(resTuple).
 */
HeapTuple *HeapScanHandler::FetchTuple(ItemPointerData &ctid, bool needCheckVisibility)
{
    LatencyStat::Timer timer(&HeapPerfUnit::GetInstance().m_heapFetchTupleLatency);
    ItemPointerData oldCtid = ctid;
    StorageAssert(m_snapshot.GetCsn() != INVALID_CSN);
    StorageReleasePanic(m_snapshot.GetCsn() == INVALID_CSN, MODULE_HEAP, ErrMsg("Invalid snapshot for fetch tuple."));

    /* AnyTuple doesn't support fetchtuple by ctid, such as index fetch tuple */
    StorageAssert(m_showAnyTupForDebug == nullptr);

    UNUSE_PARAM AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    HeapTuple *tuple = NULL;
    if (likely(needCheckVisibility)) {
        tuple = FetchVisibleDiskTuple(ctid) ;
    } else {
        tuple = FetchNewestDiskTuple(ctid);
    }
    if (tuple == nullptr) {
        if (StorageGetErrorCode() == COMMON_ERROR_MEMORY_ALLOCATION) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when fetch tuple."));
        } else if (StorageGetErrorCode() == BUFFER_ERROR_FAIL_READ_PAGE) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Read page(%hu, %u) failed when fetch tuple.",
                ctid.GetPageId().m_fileId, ctid.GetPageId().m_blockId));
        } else {
            ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Visible disk tuple not found, ctid({%hu, %u}, %hu).",
                                                      oldCtid.GetFileId(), oldCtid.GetBlockNum(), oldCtid.GetOffset()));
        }
        ReportHeapBufferReadStat(m_heapRel);
        return nullptr;
    }
    StorageAssert(*(tuple->GetCtid()) == oldCtid);

    /* If it is a small tuple, return it. */
    if (likely(!tuple->GetDiskTuple()->IsLinked())) {
        StorageAssert(tuple->GetDiskTupleSize() != 0);
        ReportHeapBufferReadStat(m_heapRel);
        return tuple;
    }

    if (m_snapshot.GetSnapshotType() == SnapshotType::SNAPSHOT_NOW) {
        SnapshotData snapshot = m_snapshot;
        m_snapshot.SetSnapshotType(SnapshotType::SNAPSHOT_MVCC);
        m_snapshot.SetCsn(TransactionInterface::GetLatestSnapshotCsn());
        m_snapshot.SetCid(INVALID_CID);
        HeapTuple *bigTuple = FetchTuple(oldCtid, needCheckVisibility);
        m_snapshot = snapshot;
        return bigTuple;
    }

    /* If it is a linked tuple, reassemble it. */
    ErrLog(DSTORE_DEBUG1, MODULE_HEAP,
           ErrMsg("Fetch big tuple({%hu, %u}, %hu).", oldCtid.GetFileId(), oldCtid.GetBlockNum(), oldCtid.GetOffset()));
    HeapTuple *bigTuple = FetchBigTuple(tuple, needCheckVisibility);
    DstorePfreeExt(tuple);
    StorageAssert(bigTuple == nullptr || bigTuple->GetDiskTupleSize() != 0);
    ReportHeapBufferReadStat(m_heapRel);
    return bigTuple;
}

RetStatus HeapScanHandler::FetchAllVisiableTupleInPage(PageId pageId, OffsetNumber *offsetArray, uint16 arrayLen,
                                                       uint16 &validLen)
{
    /* AnyTuple doesn't support fetchtuple by ctid, such as index fetch tuple */
    StorageAssert(m_showAnyTupForDebug == nullptr);
    HeapPage *crPage = nullptr;
    HeapTuple tuple;
    validLen = 0;
    UNUSE_PARAM AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    if (m_localCrPage == nullptr) {
        m_localCrPage = static_cast<HeapPage *>(DstorePalloc(sizeof(HeapPage)));
        if (unlikely(m_localCrPage == nullptr)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            return DSTORE_FAIL;
        }
    }
    /* Step 1. determine the crPage. */
    ConsistentReadContext crContext;
    crContext.pdbId = m_heapRel->m_pdbId;
    crContext.pageId = pageId;
    crContext.currentXid = m_thrd->GetActiveTransaction()->GetCurrentXid();
    crContext.snapshot = &m_snapshot;
    crContext.dataPageExtraInfo = nullptr;
    crContext.destPage = m_localCrPage;
    crContext.crBufDesc = nullptr;

    RetStatus ret = m_bufMgr->ConsistentRead(crContext, m_ringBuf);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Construct cr error."));
        m_failureInfo.SetReason(HeapHandlerFailureReason::READ_BUFFER_FAILED);
        return DSTORE_FAIL;
    }

    if (crContext.crBufDesc != nullptr) {
        m_crBufDesc = crContext.crBufDesc;
        StorageAssert(m_crBufDesc->IsCrPage());
        crPage = static_cast<HeapPage *>(m_crBufDesc->GetPage());
    } else {
        crPage = m_localCrPage;
    }

    /*
     * Step 3. Fetch the next tuple we want.
     * We should skip dead and aborted items, also the invisible(delete or non-inplace-update) tuples.
     */
    for (OffsetNumber offset = FIRST_ITEM_OFFSET_NUMBER; offset <= crPage->GetMaxOffset(); ++offset) {
        ItemId *itemId = crPage->GetItemIdPtr(offset);
        StorageAssert(itemId != nullptr);
        if (itemId->IsUnused() || itemId->IsNoStorage()) {
            continue;
        }
        crPage->GetTuple(&tuple, offset);
        StorageAssert(tuple.GetDiskTuple() != nullptr);
        StorageReleasePanic(tuple.GetDiskTuple()->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_SAME_PAGE_UPDATE),
                            MODULE_HEAP, ErrMsg("Invalid heap disk tuple live mode(OLD_TUPLE_BY_SAME_PAGE_UPDATE)."));
        if (tuple.GetDiskTuple()->TestLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_DELETE) ||
            tuple.GetDiskTuple()->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE)) {
            continue;
        }
        /* Step 4. Check if fetched tuple belongs to a big tuple */
        HeapDiskTuple *diskTuple = tuple.GetDiskTuple();
        StorageAssert(diskTuple != nullptr);
        if (diskTuple->IsLinked()) {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP,
                   ErrMsg("Scan big tuple({%hu, %u}, %hu).", tuple.GetCtid()->GetFileId(),
                          tuple.GetCtid()->GetBlockNum(), tuple.GetCtid()->GetOffset()));
            /* For other chunks of a linked tuple, ignore it and scan the next tuple */
            if (!diskTuple->IsFirstLinkChunk()) {
                continue;
            }
        }
        offsetArray[validLen++] = offset;
        StorageReleasePanic(validLen > arrayLen, MODULE_HEAP,
                            ErrMsg("Invalid heap disk tuple live mode(OLD_TUPLE_BY_SAME_PAGE_UPDATE)."));
    }
    /* free memory for m_localCrPage in scan_handler_tbl_endscan */
    ReScan();
    return DSTORE_SUCC;
}

HeapTuple *HeapScanHandler::FetchBigTuple(HeapTuple *tuple, bool needCheckVisibility)
{
    UNUSE_PARAM AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    StorageAssert(tuple->GetDiskTuple()->IsFirstLinkChunk());
    uint32 numTupChunks = tuple->GetDiskTuple()->GetNumChunks();
    HeapTuple **tupChunks = static_cast<HeapTuple **>(DstorePalloc(sizeof(HeapTuple *) * numTupChunks));
    if (unlikely(tupChunks == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%u) when heap scan fetch big tuple.",
            static_cast<uint32>(sizeof(HeapTuple *) * numTupChunks)));
        return nullptr;
    }
    uint32 i = 0;
    UNUSE_PARAM const uint32 linkTupChunkHeaderSize = sizeof(HeapDiskTuple) + LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE;

    /* Add first chunk into array. */
    tupChunks[i++] = tuple;
    StorageAssert(tuple->GetDiskTupleSize() > linkTupChunkHeaderSize);

    ItemPointerData ctid = tuple->GetDiskTuple()->GetNextChunkCtid();
    UNUSE_PARAM ItemPointerData tupleCtid = ctid;
    UNUSE_PARAM ItemPointerData lastCtid = INVALID_ITEM_POINTER;
    while (ctid != INVALID_ITEM_POINTER) {
        if (likely(m_showAnyTupForDebug == nullptr)) {
            tuple = needCheckVisibility ? FetchVisibleDiskTuple(ctid) : FetchNewestDiskTuple(ctid);
        } else {
            tuple = FetchHistoricTupleForDebug(ctid);
        }
        if (unlikely(tuple == nullptr)) {
            for (uint32 j = 1; j < i; j++) {
                DstorePfreeExt(tupChunks[j]);
            }
            if (StorageGetErrorCode() == COMMON_ERROR_MEMORY_ALLOCATION) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when fetch big tuple."));
            } else if (StorageGetErrorCode() == BUFFER_ERROR_FAIL_READ_PAGE) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Read page(%hu, %u) failed when fetch big tuple.",
                    ctid.GetPageId().m_fileId, ctid.GetPageId().m_blockId));
            }  else {
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Fetch tuple fail when heap scan."));
            }
            return nullptr;
        }
        /* Add next chunk into array. */
        tupChunks[i++] = tuple;
        StorageAssert(tuple->GetDiskTupleSize() > linkTupChunkHeaderSize);
        lastCtid = ctid;
        ctid = tuple->GetDiskTuple()->GetNextChunkCtid();
    }

    if (i != numTupChunks) {
        /* dump first page */
        ItemPointerData* fistChunkCtid = tupChunks[0]->GetCtid();
        Datum data;
        if (m_bufferDesc != NULL && m_bufferDesc->bufTag.pageId != fistChunkCtid->GetPageId()) {
            DumpPage(fistChunkCtid->GetPageId(), data);
        } else {
            HeapPage *firstPage = static_cast<HeapPage *>(m_bufferDesc->GetPage());
            char *dumpPage = firstPage->Dump(false);
            if (dumpPage != nullptr) {
                firstPage->PrevDumpPage(dumpPage);
            }
        }
        /* dump last page */
        ItemPointerData* lastChunkCtid = tupChunks[i - 1]->GetCtid();
        DumpPage(lastChunkCtid->GetPageId(), data);
        ErrLog(DSTORE_PANIC, MODULE_HEAP,
            ErrMsg("wrong chunknum, chunknum(%u), missing chunk(%u), firstCtid:(%hu, %u, %hu), lastCtid:(%hu, %u, %hu)",
            numTupChunks, i, tupleCtid.GetFileId(), tupleCtid.GetBlockNum(), tupleCtid.GetOffset(),
            lastCtid.GetFileId(), lastCtid.GetBlockNum(), lastCtid.GetOffset()));
    }

    /* Reassemble to a big tuple. */
    HeapTuple *bigTuple = AssembleTuples(tupChunks, i);

    /* Free small tuples and temporary array. */
    for (uint32 j = 1; j < i; j++) {
        DstorePfreeExt(tupChunks[j]);
    }

    if (unlikely(bigTuple == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Assemble tuples fail when heap scan."));
        return nullptr;
    }

    return bigTuple;
}

HeapTuple *HeapScanHandler::FetchNewestDiskTuple(ItemPointerData ctid)
{
    PageId pageId = ctid.GetPageId();
    OffsetNumber offset = ctid.GetOffset();
    BufferDesc *bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, pageId, LW_SHARED);
    if (unlikely(bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("Read page(%hu, %u) failed when fetch newest disk tuple.", pageId.m_fileId, pageId.m_blockId));
        storage_set_error(BUFFER_ERROR_FAIL_READ_PAGE);
        return nullptr;
    }
    CheckBufferedPage(bufferDesc->GetPage(), pageId);

    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());

    StorageAssert(page->GetItemIdPtr(offset)->IsNormal());

    HeapTuple *tuple = page->CopyTuple(offset);

    m_bufMgr->UnlockAndRelease(bufferDesc);

    if (unlikely(tuple == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when fetch newest disk tuple."));
        return nullptr;
    }

    return tuple;
}

HeapTuple *HeapScanHandler::FetchVisibleDiskTuple(ItemPointerData &ctid)
{
    bool isMatchedCr = false;
    BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
    HeapPage *page = nullptr;
    HeapTuple *resTuple = nullptr;
    Transaction *transaction = m_thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    PageId pageId = ctid.val.m_pageid;

    StorageAssert(pageId != INVALID_PAGE_ID);
    if (pageId == m_curPageId) {
        if (!m_curPageDesc->IsCrPage()) {
            if (STORAGE_FUNC_FAIL(m_bufMgr->LockContent(m_curPageDesc, LW_SHARED))) {
                storage_set_error(BUFFER_ERROR_FAIL_READ_PAGE);
                ErrLog(DSTORE_ERROR, MODULE_HEAP,
                    ErrMsg("FetchVisibleDiskTuple lock buffer content failed after transaction end."));
                m_bufMgr->Release(m_curPageDesc);
                m_curPageDesc = nullptr;
                return nullptr;
            }
            isMatchedCr = false;
        } else {
            isMatchedCr = true;
        }
    } else {
        if (m_curPageDesc != INVALID_BUFFER_DESC) {
            /* Switch to other page, we should release base buffer or cr buffer used in last fetch. */
            m_bufMgr->Release(m_curPageDesc);
            m_curPageDesc = INVALID_BUFFER_DESC;
        }
        BufferDesc *baseBufDesc = m_bufMgr->Read(m_heapRel->m_pdbId, pageId, LW_SHARED);
        if (unlikely(baseBufDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                   ErrMsg("Read page(%hu, %u) failed when fetch visible disk tuple.",
                       pageId.m_fileId, pageId.m_blockId));
            storage_set_error(BUFFER_ERROR_FAIL_READ_PAGE);
            return nullptr;
        }
        CheckBufferedPage(baseBufDesc->GetPage(), pageId);

        bufferDesc = baseBufDesc;
        isMatchedCr = false;
        m_curPageId = pageId;
        m_curPageDesc = bufferDesc;
    }

    page = static_cast<HeapPage *>(m_curPageDesc->GetPage());
    if (isMatchedCr) {
        /* If have matched cr page, read it directly */
        resTuple = page->CopyTuple(ctid.GetOffset());
        if (unlikely(resTuple == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when fetch visible disk tuple."));
            return nullptr;
        }
        StorageReleasePanic(resTuple->GetDiskTuple()->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_SAME_PAGE_UPDATE),
                            MODULE_HEAP, ErrMsg("Invalid heap disk tuple live mode(OLD_TUPLE_BY_SAME_PAGE_UPDATE)."));
        if (resTuple->GetDiskTuple()->TestLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_DELETE) ||
            resTuple->GetDiskTuple()->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE)) {
            DstorePfreeExt(resTuple);
        }
    } else {
        /* If not have matched cr page, read visible version according MVCC */
        resTuple = page->GetVisibleTuple(m_heapRel->m_pdbId, transaction, ctid, &m_snapshot, m_isLob);
    }

    if (unlikely(resTuple == nullptr)) {
        ErrLog(DSTORE_WARNING, MODULE_HEAP,
               ErrMsg("ResTuple is null, isMatchedCr is %d, glsn is %lu, plsn is %lu, ctid is {%u, %hu}.",
                      isMatchedCr, page->GetGlsn(), page->GetPlsn(), ctid.GetFileId(), ctid.GetBlockNum()));
    }

    if (!isMatchedCr) {
        m_bufMgr->UnlockContent(m_curPageDesc);
    }

    return resTuple;
}

bool HeapScanHandler::ExecMatchScanKey(HeapTuple *tuple)
{
    int nkeys = m_numScanKey;
    ScanKey curKeys = m_scanKey;
    bool result = true;
    for (; nkeys--; curKeys++) {
        Datum  at;
        bool   isnull;
        Datum  test;
        if (curKeys->skFlags & SCAN_KEY_ISNULL) {
            (result) = false;
            break;
        }

        at = (tuple)->GetAttr(curKeys->skAttno, (m_tupDesc), &isnull);
        if (isnull) {
            (result) = false;
            break;
        }
        StorageReleasePanic(curKeys->skFunc.fnAddr == nullptr, MODULE_HEAP, ErrMsg("Match fnAddr is null."));
        test = FunctionCall2Coll(&curKeys->skFunc, curKeys->skCollation, at, curKeys->skArgument);
        if (!DatumGetBool(test)) {
            (result) = false;
            break;
        }
    }
    return result;
}

void HeapScanHandler::DumpScanPage(DSTORE::Datum &fileId, DSTORE::Datum &blockId, DSTORE::Datum &data)
{
    if (m_curPageDesc == INVALID_BUFFER_DESC) {
        return;
    }
    if (!m_curPageDesc->IsCrPage()) {
        bool isLockSuc = m_bufMgr->TryLockContent(m_curPageDesc, LW_SHARED);
        if (!isLockSuc) {
            return;
        }
    }
    HeapPage *page = static_cast<HeapPage *>(m_curPageDesc->GetPage());
    PageId pageId = page->GetSelfPageId();
    fileId = pageId.m_fileId;
    blockId = pageId.m_blockId;
    data = (Datum)(page->Dump());

    char *clusterBufferInfo = m_bufMgr->GetClusterBufferInfo(m_heapRel->m_pdbId, fileId, blockId);
    if (likely(clusterBufferInfo != nullptr)) {
        ErrLog(DSTORE_LOG, MODULE_HEAP, ErrMsg("%s", clusterBufferInfo));
        DstorePfreeExt(clusterBufferInfo);
    }

    if (!m_curPageDesc->IsCrPage()) {
        m_bufMgr->UnlockContent(m_curPageDesc);
    }
}

void HeapScanHandler::DumpPage(PageId pageId, DSTORE::Datum &data)
{
    BufferDesc* baseBufDesc = m_bufMgr->Read(m_heapRel->m_pdbId, pageId, LW_SHARED);
    if (baseBufDesc == INVALID_BUFFER_DESC) {
        return;
    }
    HeapPage* page = static_cast<HeapPage *>(baseBufDesc->GetPage());
    if (page == nullptr) {
        m_bufMgr->UnlockAndRelease(baseBufDesc);
        return;
    }
    
    FileId fileId = pageId.m_fileId;
    BlockNumber blockId = pageId.m_blockId;
    char* dumpResult = page->Dump(false);
    if (dumpResult == nullptr) {
        m_bufMgr->UnlockAndRelease(baseBufDesc);
        return;
    }
    data = (Datum)dumpResult;

    char *clusterBufferInfo = m_bufMgr->GetClusterBufferInfo(m_heapRel->m_pdbId, fileId, blockId);
    if (likely(clusterBufferInfo != nullptr)) {
        ErrLog(DSTORE_LOG, MODULE_HEAP, ErrMsg("%s", clusterBufferInfo));
        DstorePfreeExt(clusterBufferInfo);
    }

    page->PrevDumpPage((char *)data);
    m_bufMgr->UnlockAndRelease(baseBufDesc);
    DstorePfree(dumpResult);
}

HeapTuple *HeapScanHandler::SeqScanForShowAnyTup()
{
    HeapPage *curPage = nullptr;
    HeapTuple *tuple = nullptr;

    StorageAssert(m_showAnyTupForDebug != nullptr);
    while (!m_scanOver) {
        /* Step 1. determine the crPage, current or next one. */
        if (STORAGE_FUNC_FAIL(PreparePageForShowAnyTup(curPage))) {
            return nullptr;
        }
        if (unlikely(curPage == nullptr)) {
            /* scan over */
            break;
        }

        /* Step 2: make the item's anyTuple context */
        ItemPointerData ctid(curPage->GetSelfPageId(), m_curOffset);
        m_showAnyTupForDebug->CheckBeforeItemFetch(ctid);

        /* Step 3: Look for another historic tuple of this item */
        tuple = curPage->ShowAnyTupleFetch(m_showAnyTupForDebug);
        if (tuple == nullptr) {
            /* All tuple from CR page or undo segment were fetched. */
            StorageAssert(m_showAnyTupForDebug->IsItemFetchFinished());

            /* Move next item since no more historic tuple for this item. */
            m_curOffset += 1;
            continue;
        } else if (tuple->GetDiskTuple()->IsNotFirstLinkChunk()) {
            /* no first linked tuple would be accessed randomly, skip it while traversal items. */
            tuple = nullptr;
            m_curOffset += 1;
            continue;
        }

        /* Step 4: Check if fetched tuple belongs to a big tuple */
        if (tuple->GetDiskTuple()->IsFirstLinkChunk()) {
            m_bigTuple = FetchBigTuple(tuple, true);
            tuple = m_bigTuple;
            if (tuple == nullptr) {
                /* incomplete historic big tuple, ignore it. */
                continue;
            }
        }

        /*
         * Step 5：Found a historic tuple.
         * It was copyed from page or undo, and will be freed at the next fetch.
          */
        break;
    }

    return tuple;
}

/*
 * switch and load base page for anytuple scan.
 */
RetStatus HeapScanHandler::PreparePageForShowAnyTup(HeapPage *&curPage)
{
    /* Step 1: init curPageId. */
    PageId curPageId = INVALID_PAGE_ID;
    StorageAssert(m_segScanContext != nullptr);
    curPage = m_localCrPage;

    if (likely(m_startFlag)) {
        StorageAssert(curPage->GetMaxOffset() > 0);
        if (m_curOffset <= curPage->GetMaxOffset()) {
            return DSTORE_SUCC;
        }
        curPageId = m_segScanContext->GetNextPageId();
    } else {
        curPageId = m_segScanContext->GetFirstPageId();
        m_startFlag = true;
    }

    /* Step 2: find the next available basebuffer. */
    while (!m_scanOver) {
        if (unlikely(!curPageId.IsValid())) {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Sequence scan finished."));
            m_scanOver = true;
            curPage = nullptr;
            return DSTORE_SUCC;
        }

        /* load base page. */
        BufferDesc *bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, curPageId, LW_SHARED);
        STORAGE_CHECK_BUFFER_PANIC(bufferDesc, MODULE_HEAP, curPageId);

        /* If current page is non-heap or empty, just skip it. */
        HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
        if (page->GetType() != PageType::HEAP_PAGE_TYPE || page->GetMaxOffset() == 0) {
            m_bufMgr->UnlockAndRelease(bufferDesc);
            curPageId = m_segScanContext->GetNextPageId();
            continue;
        }

        /*  Copy the base page to local page, and fetch tuples on it. */
        errno_t rc = memcpy_s(static_cast<char*>(static_cast<void*>(curPage)),
                              BLCKSZ, bufferDesc->GetPage(), BLCKSZ);
        StorageReleasePanic(rc != 0, MODULE_BUFFER, ErrMsg("memcpy base page to cr page failed!"));

        m_bufMgr->UnlockAndRelease(bufferDesc);
        StorageAssert(curPage->CheckSanity());
        break;
    }

    /* Step 3: scan a new page, start from the first item. */
    m_curOffset = FIRST_ITEM_OFFSET_NUMBER;
    return DSTORE_SUCC;
}

/*
 * Fetch the matched historic version of spcified ctid. If not found， nullptr is returned.
 * Version is defined in ShowAnyTupleContext: the lastTuple's deletionXid or insertionXid and its cid.
 * NOTE: Invoker is responsible for freeing memroy of the returned tuple.
 */
HeapTuple *HeapScanHandler::FetchHistoricTupleForDebug(ItemPointerData &ctid)
{
    /* Step 1: load the page */
    PageId pageId = ctid.val.m_pageid;
    BufferDesc *baseBufDesc = m_bufMgr->Read(m_heapRel->m_pdbId, pageId, LW_SHARED);
    if (baseBufDesc == INVALID_BUFFER_DESC) {
        return nullptr;
    }
    HeapPage *page = static_cast<HeapPage *>(baseBufDesc->GetPage());

    /*
     * Following three variable act as the snapshot to find historic tuple.
     */
    Xid snapXid;            /* historic tuple's xid. */
    CommandId snapCid;      /* historic tuple's commandId. */
    bool deletedTuple;      /* TRUE: matchXid is deletion xid, FALSE: insertion xid */

    /* Step 2: Get the first linked tuple's snapshot. */
    m_showAnyTupForDebug->GetLastTupleSnap(snapXid, snapCid, deletedTuple);

    /* Step 3: init bigtuple's show any tuple context */
    ShowAnyTupleContext *bigTupleAnyTuple = nullptr;
    ShowAnyTupleContext::CreateShowAnyTupleContext(m_heapRel->m_pdbId, bigTupleAnyTuple);
    bigTupleAnyTuple->CheckBeforeItemFetch(ctid);

    /* Step 4: Fetch the historic tuple using the snapshot. */
    HeapTuple *resTuple = nullptr;
    while (!m_scanOver) {
        resTuple = page->ShowAnyTupleFetch(bigTupleAnyTuple);
        if (bigTupleAnyTuple->IsSnapMatchedTup(snapXid, snapCid, deletedTuple)) {
            break;
        } else {
            /* resTuple will be released at next fetch time. */
            resTuple = nullptr;
        }
    }

    /* Step 5: cleanup showanyTuple context, and release page. */
    ShowAnyTupleContext::DestoryShowAnyTupleContext(bigTupleAnyTuple, true);
    m_bufMgr->UnlockAndRelease(baseBufDesc);

    return resTuple;
}

}
