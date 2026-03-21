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
 * dstore_btree_build.cpp
 *
 * IDENTIFICATION
 *        dstore/src/index/dstore_btree_build.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "index/dstore_btree_build.h"
#include "common/log/dstore_log.h"
#include "heap/dstore_heap_scan.h"
#include "page/dstore_index_page.h"
#include "index/dstore_btree_wal.h"
#include "wal/dstore_wal_write_context.h"
#include "errorcode/dstore_index_error_code.h"
#include "transaction/dstore_transaction.h"
#include "systable/dstore_relation.h"
#include "index/dstore_btree_perf_unit.h"
#include "index/dstore_btree_recycle_partition.h"
#include "catalog/dstore_typecache.h"

namespace DSTORE {

BtreeBuild::BtreeBuild(StorageRelation indexRel, IndexBuildInfo *indexBuildInfo, ScanKey scanKey)
    : Btree(indexRel, &indexBuildInfo->baseInfo, scanKey),
      m_indexBuildInfo(indexBuildInfo), m_tuplesortMgr(nullptr), m_leafLevel(nullptr)
{
    m_indexBuildInfo->heapTuples = 0;
    m_indexBuildInfo->indexTuples = 0;

    for (uint32 level = 0; level < BTREE_HIGHEST_LEVEL; level++) {
        m_splitCount[level] = 0U;
    }
}

RetStatus BtreeBuild::BuildIndex()
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeBuildLatency, false);
    timer.Start();

    /* Step 1: Prepare tuple sort manager. */
    m_tuplesortMgr = TuplesortMgr::CreateIdxTupleSortMgr(this->GetPdbId(), m_indexBuildInfo->baseInfo.indexRelId);
    if (unlikely(m_tuplesortMgr == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to new TuplesortMgr."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_tuplesortMgr->PrepareTupleSortInfo(m_indexBuildInfo->baseInfo,
        g_storageInstance->GetGuc()->maintenanceWorkMem, m_scanKeyValues.scankeys))) {
        HandleErrAndClear();
        return DSTORE_FAIL;
    }

    /* Step 2: Read heap tuple from tableSmgr, extract index key and put into tuple sort manager. */
    if (STORAGE_FUNC_FAIL(IsGlobalIndex() ? ScanTableForGlobalIndexBuild()
                                          : ScanTableForLocalOrNonPartitionIndexBuild())) {
        HandleErrAndClear();
        return DSTORE_FAIL;
    }

    /* Step 3: Do the real sort and unique check. */
    RetStatus ret = m_tuplesortMgr->PerformSortTuple();
    if (STORAGE_FUNC_FAIL(ret)) {
        HandleErrAndClear();
        return DSTORE_FAIL;
    }

    /* Step 4: Build btree from bottom to top */
    if (STORAGE_FUNC_FAIL(WriteSortedTuples())) {
        HandleErrAndClear();
        return DSTORE_FAIL;
    }
    timer.End();

    HandleErrAndClear();
    return DSTORE_SUCC;
}

RetStatus BtreeBuild::BuildIndexParallel(int parallelWorkers)
{
    if (parallelWorkers < 1 ||  STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
        HandleErrAndClear();
        return DSTORE_FAIL;
    }

    /* Step 1: Prepare final tuple sort manager and parallel build control
     * It will merge all the sorted runs provided by different threads
     */
    if (!m_indexBuildInfo->lpiParallelMethodIsPartition) {
        m_tuplesortMgr = TuplesortMgr::CreateIdxTupleSortMgr(this->GetPdbId(), m_indexBuildInfo->baseInfo.indexRelId);
        if (unlikely(m_tuplesortMgr == nullptr)) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to new TuplesortMgr."));
            return DSTORE_FAIL;
        }
    } else {
        m_tuplesortMgr = nullptr;
    }

    RetStatus ret = DSTORE_FAIL;
    int workMem = g_storageInstance->GetGuc()->maintenanceWorkMem;
    /* m_allowedMem is forced to be at least 64KB, the current minimum valid value for the work_mem GUC. */
    const int minWorkMemKB = 64;

    if (workMem < minWorkMemKB) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Not enough memory for parallel index build"));
        return DSTORE_FAIL;
    }

    ParallelBtreeBuild parallelBuilder(parallelWorkers,
                                       workMem,
                                       m_indexBuildInfo,
                                       m_scanKeyValues.scankeys,
                                       m_tuplesortMgr);
    if (STORAGE_FUNC_FAIL(parallelBuilder.Init())) {
        goto END;
    }

    /*
     * Step 2: Call BuildLpiParallelCrossPartition if we need to build LPI in partition parallel mode,
     * i.e., one thread builds one btree index for one partition.
     */
    if (m_indexBuildInfo->lpiParallelMethodIsPartition) {
        if (STORAGE_FUNC_SUCC(BuildLpiParallelCrossPartition(&parallelBuilder))) {
            ret = DSTORE_SUCC;
        } else {
            ret = DSTORE_FAIL;
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to build LPI in partition parallel mode!"));
        }
        goto END;
    }

    /* Step 3: Read heap tuple from tableSmgr, extract index key and put into tuple sort manager. */
    if (STORAGE_FUNC_FAIL(IsGlobalIndex() ? ScanTableForGlobalIndexBuildParallel(&parallelBuilder)
                                          : ScanTableForLocalOrNonPartitionIndexBuildParallel(&parallelBuilder))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to scan and build index in parallel mode!"));
        goto END;
    }
    if (STORAGE_FUNC_FAIL(m_tuplesortMgr->PerformSortTupleMainThread())) {
        goto END;
    }

    /* Step 4: Build btree from bottom to top */
    if (STORAGE_FUNC_FAIL(WriteSortedTuplesParallel())) {
        goto END;
    }

    ret = DSTORE_SUCC;
END:
    HandleErrAndClear();
    parallelBuilder.Destroy();
    return ret;
}

void BtreeBuild::CopyDuplicateTupleFromTupleSortMgr()
{
    if (unlikely(m_tuplesortMgr == nullptr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"m_tuplesortMgr\".", __FUNCTION__));
        PrintBackTrace();
        return;
    }
    if (unlikely(m_tuplesortMgr->GetDuplicateTuple() == nullptr)) {
        if (thrd->GetErrorCode() != INDEX_ERROR_MEMORY_ALLOC &&
            thrd->GetErrorCode() != COMMON_ERROR_MEMORY_ALLOCATION) {
            storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        }
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"duplicateTuple\".", __FUNCTION__));
        PrintBackTrace();
        return;
    }
    StorageAssert(m_indexBuildInfo->duplicateTuple == nullptr);
    m_indexBuildInfo->duplicateHeapCtid1 = UInt64GetDatum(m_tuplesortMgr->m_duplicateHeapCtid1.m_placeHolder);
    m_indexBuildInfo->duplicateHeapCtid2 = UInt64GetDatum(m_tuplesortMgr->m_duplicateHeapCtid2.m_placeHolder);
    m_indexBuildInfo->duplicateTuple = m_tuplesortMgr->GetDuplicateTuple();
    if (unlikely(m_indexBuildInfo->duplicateTuple == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to copy duplicate tuple."));
        return;
    }

    storage_set_error(TUPLESORT_ERROR_COULD_NOT_CREATE_UNIQUE_INDEX);
    ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to build index for unique check failure. "
        "heapCtid1(%hu, %u, %hu) and heapCtid2(%hu, %u, %hu) have the same key values",
        m_tuplesortMgr->m_duplicateHeapCtid1.GetFileId(), m_tuplesortMgr->m_duplicateHeapCtid1.GetBlockNum(),
        m_tuplesortMgr->m_duplicateHeapCtid1.GetOffset(), m_tuplesortMgr->m_duplicateHeapCtid2.GetFileId(),
        m_tuplesortMgr->m_duplicateHeapCtid2.GetBlockNum(), m_tuplesortMgr->m_duplicateHeapCtid2.GetOffset()));
}

RetStatus BtreeBuild::ScanTableForGlobalIndexBuild()
{
    /* m_indexBuildInfo->heapRelNum = 1 if non-partition index or local partition index,
     * m_indexBuildInfo->heapRelNum >= 1 if global partition index */
    /* For global partition index, we need to scan all partition's tableSmgr. */
    if (unlikely(m_indexBuildInfo->heapRelNum < 1 || m_indexBuildInfo->allPartTuples == nullptr)) {
        storage_set_error(INDEX_ERROR_FAIL_BUILD_GLOBAL_INDEX);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to build global index for empty partitions."));
        return DSTORE_FAIL;
    }

    RetStatus retStatus = DSTORE_SUCC;
    Datum *values = static_cast<Datum *>(DstorePalloc(m_indexBuildInfo->baseInfo.indexAttrsNum * sizeof(Datum)));
    if (unlikely(values == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when ScanTableForGlobalIndexBuild."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    bool *isNulls = static_cast<bool *>(DstorePalloc(m_indexBuildInfo->baseInfo.indexAttrsNum * sizeof(bool)));
    if (unlikely(isNulls == nullptr)) {
        DstorePfree(values);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when ScanTableForGlobalIndexBuild."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }

    for (int i = 0; i < m_indexBuildInfo->heapRelNum; i++) {
        double heapTuples = 0;
        if (STORAGE_FUNC_FAIL(retStatus = CollectTuplesFromTable(m_indexBuildInfo->heapRels[i],
            m_indexBuildInfo->allPartOids[i], values, isNulls, heapTuples))) {
            break;
        }
        m_indexBuildInfo->allPartTuples[i] = heapTuples;
        m_indexBuildInfo->heapTuples += heapTuples;
    }
    DstorePfree(values);
    DstorePfree(isNulls);

    return retStatus;
}

RetStatus BtreeBuild::ScanTableForGlobalIndexBuildParallel(ParallelBtreeBuild *parallelBuilder)
{
    /* For global partition index, we need to scan all partition's tableSmgr. */
    if (unlikely(m_indexBuildInfo->heapRelNum < 1 || m_indexBuildInfo->allPartTuples == nullptr)) {
        storage_set_error(INDEX_ERROR_FAIL_BUILD_GLOBAL_INDEX);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to build global index for empty partitions."));
        return DSTORE_FAIL;
    }

    RetStatus retStatus = DSTORE_SUCC;
    double heapTuplesLast = 0;
    for (int i = 0; i < m_indexBuildInfo->heapRelNum; i++) {
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent  */
            return DSTORE_FAIL;
        }
        double heapTuples = 0;
        bool isLastPartition = (i == m_indexBuildInfo->heapRelNum - 1);
        if (STORAGE_FUNC_FAIL(retStatus =
                            parallelBuilder->CollectTuplesFromTable(m_indexBuildInfo->heapRels[i],
                                                                    m_indexBuildInfo->allPartOids[i],
                                                                    heapTuples,
                                                                    isLastPartition))) {
            return retStatus;
        }
        m_indexBuildInfo->allPartTuples[i] = heapTuples - heapTuplesLast;
        heapTuplesLast = heapTuples;
    }

    retStatus = parallelBuilder->MergeWorkerResults();

    return retStatus;
}

RetStatus BtreeBuild::BuildLpiParallelCrossPartition(ParallelBtreeBuild *parallelBuilder)
{
    if (unlikely(m_indexBuildInfo == nullptr || m_indexBuildInfo->heapRels == nullptr ||
        m_indexBuildInfo->heapRels[0] == nullptr || m_indexBuildInfo->indexRels == nullptr ||
        m_indexBuildInfo->allPartOids == nullptr || parallelBuilder == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unexpected error happened when build lpi parallel "
            "cross partition."));
        return DSTORE_FAIL;
    }
    
    m_indexBuildInfo->currentBuildPartIdx = 0;
    m_indexBuildInfo->heapTuples = 0;
    m_indexBuildInfo->indexTuples = 0;
    PdbId pdbId = m_indexBuildInfo->heapRels[0]->m_pdbId;

    if (unlikely(pdbId == INVALID_PDB_ID)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Invalid pdb id when build lpi parallel cross partition."));
        return DSTORE_FAIL;
    }

    RetStatus retStatus = parallelBuilder->BuildLpiParallelCrossPartition(pdbId);
    return retStatus;
}

RetStatus BtreeBuild::ScanTableForLocalOrNonPartitionIndexBuild()
{
    /* For non-partition index and local partition index, we only scan one tableSmgr. */
    if (unlikely(m_indexBuildInfo->heapRelNum != 1)) {
        storage_set_error(INDEX_ERROR_FAIL_BUILD_GLOBAL_INDEX);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to build local index for too many tableSmgr."));
        return DSTORE_FAIL;
    }

    Datum *values = static_cast<Datum *>(DstorePalloc(m_indexBuildInfo->baseInfo.indexAttrsNum * sizeof(Datum)));
    if (unlikely(values == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("DstorePalloc fail when ScanTableForLocalOrNonPartitionIndexBuild."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    bool *isNulls = static_cast<bool *>(DstorePalloc0(m_indexBuildInfo->baseInfo.indexAttrsNum * sizeof(bool)));
    if (unlikely(isNulls == nullptr)) {
        DstorePfree(values);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("DstorePalloc fail when ScanTableForLocalOrNonPartitionIndexBuild."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    RetStatus retStatus =
        CollectTuplesFromTable(m_indexBuildInfo->heapRels[0], m_indexBuildInfo->heapRelationOid, values,
                               isNulls, m_indexBuildInfo->heapTuples);
    DstorePfree(values);
    DstorePfree(isNulls);

    return retStatus;
}

RetStatus BtreeBuild::ScanTableForLocalOrNonPartitionIndexBuildParallel(ParallelBtreeBuild *parallelBuilder)
{
    /* For non-partition index and local partition index, we only scan one tableSmgr. */
    if (unlikely(m_indexBuildInfo->heapRelNum != 1)) {
        storage_set_error(INDEX_ERROR_FAIL_BUILD_GLOBAL_INDEX);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to build local index for too many tableSmgr."));
        return DSTORE_FAIL;
    }
    double heapTupleCount = 0;
    RetStatus retStatus = parallelBuilder->CollectTuplesFromTable(m_indexBuildInfo->heapRels[0],
        m_indexBuildInfo->heapRelationOid, heapTupleCount);
    if (STORAGE_FUNC_FAIL(retStatus)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Fail to collect tuples in the table of local index"));
        return retStatus;
    }

    retStatus = parallelBuilder->MergeWorkerResults();
    return retStatus;
}

/*
 * Add an item to a disk page from the sort output
 * +----------------+--------+------------------------------+
 * | PageHeaderData | itemId0 itemId1 itemId2 ...           |
 * +----------------+--------+- ----------------------------+
 * | ... itemIdN |                                          |
 * +-------------+------------------------------------------+
 * |             ^ lastItemOffset                            |
 * |                                                        |
 * +-------------+------------------------------------------+
 * |             | tupleN ...                               |
 * +---------------+----------------------+-----------------+
 * |             ... tuple3 tuple2 tuple1 | "special space" |
 * +--------------------------------------+-----------------+
 * 'lastItemOffse' points to the last offset added to the page.
 * "itemId0" is reserved for high key. The corresponding index tuple would be the last added tuple on the page. We'll
 * re-write linker of high key to point to the last added tuple, and remove for the last itemId. The real data tuple
 * with a ctid to heap table tha have the same key values with the last added tuple would be move to it right sibling
 * page.
 */
RetStatus BtreeBuild::AddTupleToPage(IndexTuple *indexTuple, BtreePageLevelBuilder *currLevel)
{
    /* Step 1. Split page if full */
    /* Check if there's enough space on current page to add the indexTuple */
    uint32 indexTupleSize = indexTuple->GetSize();
    uint32 freePageSpace = static_cast<DataPage *>(currLevel->currPageBuf->GetPage())->
        GetFreeSpace<FreeSpaceCondition::EXCLUDE_ITEMID>();
    uint32 extraItemPointerSize = currLevel->level == 0 ? static_cast<uint32>(sizeof(ItemPointerData)) : 0;
    /* For leaf page, an extra ItemPointerData might be added into a truncate tuple when all key values are equal.
     * So, an extra size of ItemPointerData should be in count for free space check */
    if (indexTupleSize > MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE) {
        storage_set_error(INDEX_ERROR_FAIL_FOR_HUGE_INDEX_TUPLE, indexTupleSize, freePageSpace);
        m_indexBuildInfo->baseInfo.extraInfo = UInt32GetDatum(indexTupleSize);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Tuple size %u is huge", indexTupleSize));
        return DSTORE_FAIL;
    }

    if (freePageSpace < indexTupleSize + extraItemPointerSize ||
        (freePageSpace <= currLevel->pageFreeSpaceLimit && currLevel->lastItemOffset > BTREE_PAGE_FIRSTKEY)) {
        /* Current page space is insufficient, need to split */
        if (STORAGE_FUNC_FAIL(SplitPage(currLevel))) {
            return DSTORE_FAIL;
        }
    }

    /* Step 2. Add indexTuple to page */
    /* Now we have sufficient free space for indexTuple either on original page or new-created page */
    BtrPage *currPage = static_cast<BtrPage *>(currLevel->currPageBuf->GetPage());
    OffsetNumber currOffset = OffsetNumberNext(currLevel->lastItemOffset);
    StorageAssert(currOffset >= BTREE_PAGE_FIRSTKEY);
    if (currPage->AddTuple(indexTuple, currOffset) != currOffset) {
        storage_set_error(INDEX_ERROR_ADD_ITEM_FAIL);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to insert indexTuple(%hu, %u, %hu)" BTR_PAGE_HEADER_FMT
            BTR_PAGE_LINK_AND_STATUS_FMT, currLevel->currPageBuf->GetPageId().m_fileId,
            currLevel->currPageBuf->GetPageId().m_blockId, currOffset, BTR_PAGE_HEADER_VAL(currPage),
            BTR_PAGE_LINK_AND_STATUS_VAL(currPage->GetLinkAndStatus())));
        return DSTORE_FAIL;
    }

    /* Step 3. Update BtreePageLevelBuilder for next insertion */
    currLevel->lastItemOffset = currOffset;
    StorageAssert(currLevel->lastItemOffset == currPage->GetMaxOffset());

    return DSTORE_SUCC;
}

RetStatus BtreeBuild::CreateBtreeMeta(const PageId rootPageId, uint32 rootLevel)
{
#ifndef UT
#ifndef DSTORE_TEST_TOOL
    if (unlikely(m_indexBuildInfo->baseInfo.m_indexSupportProcInfo->supportProcs == nullptr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"supportProcs\".", __FUNCTION__));
        PrintBackTrace();
        return DSTORE_FAIL;
    }
#endif
#endif
    PageId metaPageId = GetBtreeSmgr()->GetBtrMetaPageId();
    BufferDesc *btrMetaBuf = m_bufMgr->Read(this->GetPdbId(), metaPageId, LW_EXCLUSIVE,
        BufferPoolReadFlag::CreateNewPage());
    PageId loadMetaPageId = btrMetaBuf->GetPageId();
    if (unlikely(btrMetaBuf == INVALID_BUFFER_DESC || loadMetaPageId != metaPageId)) {
        if (btrMetaBuf == INVALID_BUFFER_DESC) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Read BtrMeta page {%u, %u} failed. Errcode from buffer: %lld",
                metaPageId.m_fileId, metaPageId.m_blockId, StorageGetErrorCode()));
            storage_set_error(INDEX_ERROR_FAIL_READ_PAGE);
        } else if (loadMetaPageId != metaPageId) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Page {%d, %u} was requested but {%d, %u} returned in bufTag, pdbId:%u.",
                    metaPageId.m_fileId, metaPageId.m_blockId, loadMetaPageId.m_fileId,
                    loadMetaPageId.m_blockId, this->GetPdbId()));
            m_bufMgr->UnlockAndRelease(btrMetaBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
            storage_set_error(INDEX_ERROR_PAGE_DIFF_IN_BUFTAG, metaPageId.m_fileId, metaPageId.m_blockId,
                              loadMetaPageId.m_fileId, loadMetaPageId.m_blockId, GetPdbId());
        }
        char *clusterBufferInfo = m_bufMgr->GetClusterBufferInfo(this->GetPdbId(),
                                                                 metaPageId.m_fileId, metaPageId.m_blockId);
        if (likely(clusterBufferInfo != nullptr)) {
            ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
            DstorePfreeExt(clusterBufferInfo);
        }
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    /* Btree Meta page is the first data page of index segment that reserved for btree. It might have not been
     * initialized. Need to initialize it as a btree meta page before write. */
    BtrPage::InitBtrPage(btrMetaBuf, GetBtreeSmgr()->GetBtrMetaPageId());
    BtrPage *btrMetaPage = static_cast<BtrPage *>(btrMetaBuf->GetPage());
    if (STORAGE_VAR_NULL(btrMetaPage)) {
        char *clusterBufferInfo = m_bufMgr->GetClusterBufferInfo(GetPdbId(), metaPageId.m_fileId, metaPageId.m_blockId);
        ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
        DstorePfreeExt(clusterBufferInfo);
        PrintBackTrace();
        ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("Btree meta page in bufferDesc {%u, %hu, %u} is empty.",
               GetPdbId(), metaPageId.m_fileId, metaPageId.m_blockId));
        m_bufMgr->UnlockAndRelease(btrMetaBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        return DSTORE_FAIL;
    }
    BtrMeta *btrMeta = static_cast<BtrMeta *>(static_cast<void *>(btrMetaPage->GetData()));
    /* Set lower point to the end of Btree Meta data to avoid losing data during redo log compressing */
    btrMetaPage->SetLower(btrMetaPage->GetLower() + static_cast<uint16>(sizeof(BtrMeta)));
    /* Set btree page type as META_PAGE. Note that this type is different with PageType. It's only used within btree */
    btrMetaPage->GetLinkAndStatus()->SetType(BtrPageType::META_PAGE);
    /* Set root page info */
    btrMeta->rootPage = rootPageId;
    btrMeta->rootLevel = rootLevel;
    btrMeta->lowestSinglePage = rootPageId;
    btrMeta->lowestSinglePageLevel = rootLevel;
    /* Set attribute type info */
    btrMeta->nkeyAtts = m_indexBuildInfo->baseInfo.indexKeyAttrsNum;
    btrMeta->natts = m_indexBuildInfo->baseInfo.indexAttrsNum;
    btrMeta->createXid = GetBtreeSmgr()->GetMetaCreateXid();
    btrMeta->relKind = m_indexBuildInfo->baseInfo.relKind;
    btrMeta->tableOidAtt = m_indexBuildInfo->baseInfo.tableOidAtt;
#ifndef UT
#ifndef DSTORE_TEST_TOOL
    /* numSupportProc comes from sql engine */
    IndexSupportProcInfo *procInfo = m_indexBuildInfo->baseInfo.m_indexSupportProcInfo;
    if (procInfo != nullptr) {
        btrMeta->numSupportProc = m_indexBuildInfo->baseInfo.m_indexSupportProcInfo->numSupportProc;
        /* numSupportProc and supportProcs comes from sql engine */
        for (uint16 i = 0; i < btrMeta->nkeyAtts * btrMeta->numSupportProc; i++) {
            btrMeta->functionOids[i] = m_indexBuildInfo->baseInfo.m_indexSupportProcInfo->supportProcs[i];
        }
    } else {
        btrMeta->numSupportProc = 0;
    }
#endif
#endif
    for (uint16 i = 0; i < btrMeta->nkeyAtts; i++) {
        btrMeta->opcinTypes[i] = m_indexBuildInfo->baseInfo.opcinType[i];
    }
    for (uint16 i = 0; i < btrMeta->natts; i++) {
        btrMeta->attTypeIds[i] = m_indexBuildInfo->baseInfo.attributes->attrs[i]->atttypid;
        btrMeta->attlen[i] = m_indexBuildInfo->baseInfo.attributes->attrs[i]->attlen;
        btrMeta->attbyval[i] = m_indexBuildInfo->baseInfo.attributes->attrs[i]->attbyval;
        btrMeta->attalign[i] = m_indexBuildInfo->baseInfo.attributes->attrs[i]->attalign;
        btrMeta->attColIds[i] = m_indexBuildInfo->baseInfo.attributes->attrs[i]->attcollation;
    }
    errno_t rc = memcpy_s(btrMeta->indexOption, btrMeta->nkeyAtts * sizeof(int16),
                          m_indexBuildInfo->baseInfo.indexOption, btrMeta->nkeyAtts * sizeof(int16));
    storage_securec_check(rc, "", "");

    btrMeta->InitStatisticsInfo();
    if (unlikely(static_cast<uint64>(g_traceSwitch) & BTREE_STATISTIC_INFO_MIN_TRACE_LEVEL)) {
        StorageAssert(rootLevel < BTREE_HIGHEST_LEVEL);
        for (uint32 level = 0U; level <= rootLevel; level++) {
            btrMeta->operCount[static_cast<int>(BtreeOperType::BTR_OPER_SPLIT_WHEN_BUILD)][level] = m_splitCount[level];
        }
    }

    /* Mark dirty before write wal */
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(btrMetaBuf));
    if (NeedWal()) {
        GenerateInitMetaPageWal(btrMetaBuf);
    }
    m_bufMgr->UnlockAndRelease(btrMetaBuf);
    return DSTORE_SUCC;
}

void* BtreeBuild::ExprInit(CallbackFunc exprCallback)
{
    CacheHashManager *chManager = g_storageInstance->GetCacheHashMgr();
    if (STORAGE_VAR_NULL(chManager)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr in storage instance \"CacheHashManager\"", __FUNCTION__));
        return nullptr;
    }
    IndexCommonCb commonCb = chManager->GetIndexCommonCb();
    if (unlikely(commonCb == nullptr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr in CacheHashManager \"GetIndexCommonCb\"", __FUNCTION__));
        return nullptr;
    }
    FunctionCallInfoData fcInfo;
    Datum result = 0;

    if (STORAGE_FUNC_FAIL(commonCb(exprCallback, &fcInfo, &result))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] commonCb fail", __FUNCTION__));
        return nullptr;
    }

    return DatumGetPointer(result);
}

RetStatus BtreeBuild::GetExprValue(CallbackFunc exprCallback, HeapTuple *heapTuple, Datum *values, bool *isNulls,
                                   void *exprCxt)
{
    FunctionCallInfoData fcInfo;
    fcInfo.prealloc_arg[0] = PointerGetDatum(heapTuple);
    fcInfo.prealloc_arg[1] = PointerGetDatum(values);
    fcInfo.prealloc_arg[2] = PointerGetDatum(isNulls);
    fcInfo.prealloc_arg[3] = PointerGetDatum(exprCxt);

#ifdef UT
    (void)exprCallback(&fcInfo);
    return DSTORE_SUCC;
#endif

    CacheHashManager *chManager = g_storageInstance->GetCacheHashMgr();
    if (STORAGE_VAR_NULL(chManager)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr in storage instance \"CacheHashManager\"", __FUNCTION__));
        return DSTORE_FAIL;
    }
    IndexCommonCb commonCb = chManager->GetIndexCommonCb();
    if (unlikely(commonCb == nullptr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr in CacheHashManager \"GetIndexCommonCb\"", __FUNCTION__));
        return DSTORE_FAIL;
    }

    Datum result = 0;
    if (STORAGE_FUNC_FAIL(commonCb(exprCallback, &fcInfo, &result))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] commonCb fail", __FUNCTION__));
        return DSTORE_FAIL;
    }

    return (RetStatus)(DATUM_GET_INT8(result));
}

RetStatus BtreeBuild::CollectTuplesFromTable(StorageRelation heapRel, Oid tableOid, Datum *values,
                                             bool *isNulls, double &numHeapTuples)
{
    if (unlikely(heapRel == nullptr || heapRel->tableSmgr == nullptr)) {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Invalid table smgr, pdbId:%u, tableOid:%u",
            heapRel ? heapRel->m_pdbId : INVALID_PDB_ID, tableOid));
        return DSTORE_FAIL;
    }
    HeapTuple *heapTuple = nullptr;
    HeapScanHandler *heapScan = DstoreNew(thrd->GetTransactionMemoryContext())
        HeapScanHandler(g_storageInstance, thrd, heapRel);
    if (unlikely(heapScan == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to alloc CurrentMemoryContext for HeapTuple."));
        return DSTORE_FAIL;
    }

    UNUSE_PARAM AutoMemCxtSwitch autoMcxtSwitch{m_tuplesortMgr->GetTupleMcxt()};
    if (STORAGE_FUNC_FAIL(heapScan->Begin(thrd->GetActiveTransaction()->GetSnapshot()))) {
        delete heapScan;
        return DSTORE_FAIL;
    }
    void *exprCxt = nullptr;
    if (m_indexBuildInfo->baseInfo.exprInitCallback) {
        exprCxt = BtreeBuild::ExprInit(m_indexBuildInfo->baseInfo.exprInitCallback);
        if (unlikely(exprCxt == nullptr)) {
            storage_set_error(INDEX_ERROR_EXPRESSION_VALUE_ERR);
            heapScan->End();
            delete heapScan;
            return DSTORE_FAIL;
        }
    }
    while ((heapTuple = heapScan->SeqScan()) != nullptr) {
        /* For GPI, the last index column is the partition oid, but it is not key column. */
        heapTuple->SetTableOid(tableOid);
        uint32 tupleSize = BLCKSZ;
        if (m_indexBuildInfo->baseInfo.exprCallback) {
            if (STORAGE_FUNC_FAIL(BtreeBuild::GetExprValue(m_indexBuildInfo->baseInfo.exprCallback, heapTuple, values,
                                                           isNulls, exprCxt))) {
                storage_set_error(INDEX_ERROR_EXPRESSION_VALUE_ERR);
                break;
            }
        }
        IndexTuple *indexTuple = IndexTuple::FormTuple(heapTuple, m_indexBuildInfo, values, isNulls, &tupleSize);
        if (unlikely(tupleSize > MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE)) {
            m_indexInfo->extraInfo = UInt32GetDatum(tupleSize);
            DstorePfreeExt(indexTuple);
        }
        if (unlikely(indexTuple == nullptr)) {
            break;
        }
        indexTuple->SetHeapCtid(heapTuple->GetCtid());
        indexTuple->SetCcindexStatus(m_indexInfo->btrIdxStatus);
        if (STORAGE_FUNC_FAIL(m_tuplesortMgr->PutIndexTuple(indexTuple))) {
            break;
        }
        numHeapTuples++;
        m_indexBuildInfo->indexTuples++;
    }
    heapScan->End();
    delete heapScan;

    if (m_indexBuildInfo->baseInfo.exprDestroyCallback) {
        m_indexBuildInfo->baseInfo.exprDestroyCallback(exprCxt);
    }

    if (unlikely(StorageIsErrorSet())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

/* Read tuples in correct sorted order from tuplesortMgr, and load them into btree leaves. */
RetStatus BtreeBuild::WriteSortedTuples()
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeBatchWriteLatency);
    /* Step 1. Allocate transaction slot for current build transaction. */
    if (STORAGE_FUNC_FAIL(thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        return DSTORE_FAIL;
    }

    if (STORAGE_VAR_NULL(thrd->m_walWriterContext)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Wal writer context is null in current thread."));
        return DSTORE_FAIL;
    }

    if (likely(GetBtreeSmgr()->GetSegment()->m_btrRecyclePartition == nullptr) &&
        STORAGE_FUNC_FAIL(GetBtreeSmgr()->GetSegment()->InitBtrRecyclePartition())) {
        return DSTORE_FAIL;
    }
    GetBtreeSmgr()->SetBuildingXid(GetBtreeSmgr()->GetSegment()->m_btrRecyclePartition->createdXid);

    PageId rootPageId = INVALID_PAGE_ID;
    uint32 rootLevel = 0;
    if (m_indexBuildInfo->indexTuples > 0) {
        /* Table is not empty. Start to build index. */
        /* Step 2. Create a BtreePageLevelBuilder for the first level of btree. (Leaf pages level with level = 0) */
        if (STORAGE_FUNC_FAIL(CreatePageLevelBuilder(nullptr, &m_leafLevel))) {
            return DSTORE_FAIL;
        }

        /* Step 3. Add sorted tuple into page one by one. */
        double tupCounter = 0;
        IndexTuple *indexTuple = nullptr;
        for (;;) {
            if (STORAGE_FUNC_FAIL(m_tuplesortMgr->GetNextIndexTuple(&indexTuple))) {
                return DSTORE_FAIL;
            }
            if (indexTuple == nullptr) {
                if (unlikely(abs(tupCounter - m_indexBuildInfo->indexTuples) > 0.01)) {
                    if (!StorageIsErrorSet()) {
                        storage_set_error(INDEX_ERROR_FAILED_TO_GET_SORTED_TUPLE);
                    }
                    ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to get next sorted tuple. %f in total, %f wrote.",
                        m_indexBuildInfo->indexTuples, tupCounter));
                    return DSTORE_FAIL;
                }
                break;
            }
            tupCounter++;
            StorageAssert(indexTuple->m_link.heapCtid.GetPageId().IsValid());
            if (STORAGE_FUNC_FAIL(AddTupleToPage(indexTuple, m_leafLevel))) {
                return DSTORE_FAIL;
            }
        }
        /* Step 4. Write down rightmost pages in each level */
        if (STORAGE_FUNC_FAIL(CompleteRightmostPages(rootPageId, rootLevel))) {
            return DSTORE_FAIL;
        }
    }

    /* Step 5. create and write btree meta page */
    /* We also create btree meta for empty table */
    return CreateBtreeMeta(rootPageId, rootLevel);
}

RetStatus BtreeBuild::WriteSortedTuplesParallel()
{
#ifndef UT
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeBatchWriteLatency);
#endif
    /* Step 1. Allocate transaction slot for current build transaction. */
    if (likely(GetBtreeSmgr()->GetSegment()->m_btrRecyclePartition == nullptr) &&
        STORAGE_FUNC_FAIL(GetBtreeSmgr()->GetSegment()->InitBtrRecyclePartition())) {
        return DSTORE_FAIL;
    }
    GetBtreeSmgr()->SetBuildingXid(GetBtreeSmgr()->GetSegment()->m_btrRecyclePartition->createdXid);

    PageId rootPageId = INVALID_PAGE_ID;
    uint32 rootLevel = 0;
    if (m_indexBuildInfo->indexTuples > 0) {
        /* Table is not empty. Start to build index. */
        /* Step 2. Create a BtreePageLevelBuilder for the first level of btree. (Leaf pages level with level = 0) */
        if (STORAGE_FUNC_FAIL(CreatePageLevelBuilder(nullptr, &m_leafLevel))) {
            return DSTORE_FAIL;
        }

        /* Step 3. Add sorted tuple into page one by one. */
        double tupCounter = 0;
        IndexTuple *indexTuple = nullptr;
        for (;;) {
            if (STORAGE_FUNC_FAIL(m_tuplesortMgr->GetNextIndexTupleMainThread(&indexTuple))) {
                return DSTORE_FAIL;
            }
            if (indexTuple == nullptr) {
                if (unlikely(abs(tupCounter - m_indexBuildInfo->indexTuples) > 0.01)) {
                    storage_set_error(INDEX_ERROR_FAILED_TO_GET_SORTED_TUPLE);
                    ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to get next sorted tuple. %f in total, %f wrote.",
                        m_indexBuildInfo->indexTuples, tupCounter));
                    return DSTORE_FAIL;
                }
                break;
            }
            tupCounter++;
            if (STORAGE_FUNC_FAIL(AddTupleToPage(indexTuple, m_leafLevel))) {
                return DSTORE_FAIL;
            }

            if (STORAGE_FUNC_FAIL(m_tuplesortMgr->ReadNextTupleFromWorkers())) {
                return DSTORE_FAIL;
            }
            if (unlikely(!m_tuplesortMgr->UniqueCheckSucc())) {
                return DSTORE_FAIL;
            }
        }
        /* Step 4. Write down rightmost pages in each level */
        if (STORAGE_FUNC_FAIL(CompleteRightmostPages(rootPageId, rootLevel))) {
            return DSTORE_FAIL;
        }
    }

    /* Step 5. create and write btree meta page */
    /* We also create btree meta for empty table */
    return CreateBtreeMeta(rootPageId, rootLevel);
}

RetStatus BtreeBuild::CreatePageLevelBuilder(BtreePageLevelBuilder *childLevel, BtreePageLevelBuilder **newLevel)
{
    uint32 level = childLevel == nullptr ? 0 : childLevel->level + 1;
    int fillFactor = (childLevel == nullptr ? GetBtreeSmgr()->GetFillFactor() : BTREE_NONLEAF_FILLFACTOR);

    *newLevel = static_cast<BtreePageLevelBuilder *>(DstorePalloc(sizeof(BtreePageLevelBuilder)));
    if (unlikely(*newLevel == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when CreatePageLevelBuilder."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL((*newLevel)->Init(level, fillFactor))) {
        DstorePfreeExt(*newLevel);
        return DSTORE_FAIL;
    }
    if (childLevel != nullptr) {
        childLevel->higherLevelBuilder = *newLevel;
    }
    return AddNewPageToLevelBuilder(*newLevel);
}

RetStatus BtreeBuild::AddNewPageToLevelBuilder(BtreePageLevelBuilder *currLevel)
{
    StorageAssert(currLevel->currPageBuf == INVALID_BUFFER_DESC);

    BtreePagePayload pagePayload;
    if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->GetNewPage(pagePayload)) ||
        pagePayload.GetBuffDesc() == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }
    pagePayload.GetPage()->InitNewPageForBuild(GetBtreeSmgr()->GetBtrMetaPageId(), GetBtreeSmgr()->GetMetaCreateXid(),
        currLevel->level, false, currLevel->level == 0 ? DEFAULT_TD_COUNT : 0);
    currLevel->currPageBuf = pagePayload.GetBuffDesc();

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Create new page {%d, %u} in level %u for building",
                  pagePayload.GetPageId().m_fileId, pagePayload.GetPageId().m_blockId, currLevel->level));
    return DSTORE_SUCC;
}

RetStatus BtreeBuild::SplitPage(BtreePageLevelBuilder *currLevel)
{
    /*
     * Be sure to check for interrupts at least once per page.
     */
    if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
        /* Cancel request sent  */
        return DSTORE_FAIL;
    }

    /* Step 1. Current page is full, link it to its parent */
    if (STORAGE_FUNC_FAIL(AddPageDownlinkToParent(currLevel))) {
        return DSTORE_FAIL;
    }

    /* Step 2. Get first data tuple for right page */
    BtreePagePayload currPage;
    currPage.InitByBuffDesc(currLevel->currPageBuf);
    IndexTuple *lastAddedTuple = currPage.GetPage()->GetIndexTuple(currLevel->lastItemOffset);
    IndexTuple *rightFirst;
    if (currPage.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)) {
        /* The last added item on left page is about to be moved to its right sibling page (new page) as the first
         * data item */
        rightFirst = lastAddedTuple->Copy();
        if (unlikely(rightFirst == nullptr)) {
            if (StorageGetErrorCode() == INDEX_ERROR_TUPLE_DAMAGED) {
                Btree::DumpDamagedTuple(lastAddedTuple, currPage.GetPage(), currLevel->lastItemOffset);
            }
            return DSTORE_FAIL;
        }
    } else {
        /* The first data item on an internal page should have a key value of "minus infinity" that's reliably less
         * than any real key value that could appear int the left page. */
        rightFirst = IndexTuple::CreateMinusinfPivotTuple();
        if (unlikely(rightFirst == nullptr)) {
            return DSTORE_FAIL;
        }
        rightFirst->SetLowlevelIndexpageLink(lastAddedTuple->GetLowlevelIndexpageLink());
    }

    /* Step 3. Create a high key for current page and write if down */
    if (STORAGE_FUNC_FAIL(CreateAndWriteHikey(currLevel))) {
        return DSTORE_FAIL;
    }

    /* Step 4. Create a new page as splitting right and link pages */
    /* It's safe to set currLevel->m_currPageBuf pointer to nullptr now because we've already load it to currPage */
    currLevel->currPageBuf = nullptr;
    /* Create a new page as splitting right */
    if (STORAGE_FUNC_FAIL(AddNewPageToLevelBuilder(currLevel)) ||
        currLevel->currPageBuf == INVALID_BUFFER_DESC) {
        currPage.Drop(m_bufMgr);
        return DSTORE_FAIL;
    }
    BtreePagePayload rightPage;
    rightPage.InitByBuffDesc(currLevel->currPageBuf);
    /* link pages */
    currPage.GetLinkAndStatus()->SetRight(rightPage.GetPageId());
    rightPage.GetLinkAndStatus()->SetLeft(currPage.GetPageId());

    /* Step 5. Write down left page (currPage) */
    WritePage(currPage.GetBuffDesc());
    /* The page would unlocked after writing */
    currPage.buffDesc = INVALID_BUFFER_DESC;

    /* Step 6. Move last added tuple of current level to right page */
    if (rightPage.GetPage()->AddTuple(rightFirst, BTREE_PAGE_FIRSTKEY) != BTREE_PAGE_FIRSTKEY) {
        storage_set_error(INDEX_ERROR_ADD_ITEM_FAIL);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to insert indexTuple(%hu, %u, %hu)" BTR_PAGE_HEADER_FMT
            BTR_PAGE_LINK_AND_STATUS_FMT, rightPage.GetPageId().m_fileId, rightPage.GetPageId().m_blockId,
            BTREE_PAGE_FIRSTKEY, BTR_PAGE_HEADER_VAL(rightPage.GetPage()),
            BTR_PAGE_LINK_AND_STATUS_VAL(rightPage.GetLinkAndStatus())));
        rightPage.Drop(m_bufMgr);
        return DSTORE_FAIL;
    }
    DstorePfree(rightFirst);
    currLevel->lastItemOffset = BTREE_PAGE_FIRSTKEY;
    StorageAssert(currLevel->lastItemOffset == rightPage.GetPage()->GetMaxOffset());

    if (unlikely(static_cast<uint64>(g_traceSwitch) & BTREE_STATISTIC_INFO_MIN_TRACE_LEVEL)) {
        m_splitCount[currLevel->level]++;
    }

    return DSTORE_SUCC;
}

RetStatus BtreeBuild::AddPageDownlinkToParent(BtreePageLevelBuilder *currLevel)
{
    if (unlikely(currLevel->lastPageHikey == nullptr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"lastPageHikey\".", __FUNCTION__));
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    /* Create a higher level for parent page if current page doesn't have a parent yet */
    BtreePageLevelBuilder *parentLevel = currLevel->higherLevelBuilder;
    if (parentLevel == nullptr) {
        if (STORAGE_FUNC_FAIL(CreatePageLevelBuilder(currLevel, &parentLevel))) {
            return DSTORE_FAIL;
        }
    }
    if (unlikely(currLevel->currPageBuf == INVALID_BUFFER_DESC)) {
        storage_set_error(INDEX_ERROR_FAIL_READ_PAGE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("No valid pageBuf in currLevel when AddPageDownlinkToParent in level:%u", currLevel->level));
        return DSTORE_FAIL;
    }

    /* Check correctness of m_lastPageHikey */
    BtreePagePayload currPage;
    currPage.InitByBuffDesc(currLevel->currPageBuf);
    uint16 numTupAtts = currLevel->lastPageHikey->GetKeyNum(m_indexBuildInfo->baseInfo.indexAttrsNum);
    if ((numTupAtts > 0 && numTupAtts <= m_indexBuildInfo->baseInfo.indexKeyAttrsNum) ||
        (numTupAtts == 0 && currPage.GetLinkAndStatus()->IsLeftmost())) {
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("Add downlink {%d, %u} to parent {%d, %u} %d",
                      currPage.GetPageId().m_fileId, currPage.GetPageId().m_blockId,
                      parentLevel->currPageBuf->GetPageId().m_fileId,
                      parentLevel->currPageBuf->GetPageId().m_blockId,
                      OffsetNumberNext(parentLevel->lastItemOffset)));
    } else {
        storage_set_error(INDEX_ERROR_FOR_INVALID_LAST_HIKEY);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("last hikey is invalid for {%d, %u} in level %u" BTR_PAGE_HEADER_FMT
            BTR_PAGE_LINK_AND_STATUS_FMT, currPage.GetPageId().m_fileId, currPage.GetPageId().m_blockId,
            currLevel->level, BTR_PAGE_HEADER_VAL(currPage.GetPage()),
            BTR_PAGE_LINK_AND_STATUS_VAL(currPage.GetLinkAndStatus())));
        return DSTORE_FAIL;
    }

    /* Add downlink to parent level */
    currLevel->lastPageHikey->SetLowlevelIndexpageLink(currPage.GetPageId());
    if (STORAGE_FUNC_FAIL(AddTupleToPage(currLevel->lastPageHikey, parentLevel))) {
        return DSTORE_FAIL;
    }
    /* Then we can release m_lastPageHikey of current level. */
    DstorePfree(currLevel->lastPageHikey);
    currLevel->lastPageHikey = nullptr;
    return DSTORE_SUCC;
}

RetStatus BtreeBuild::CreateAndWriteHikey(BtreePageLevelBuilder *currLevel)
{
    /* Step 1. Construct high key of left page */
    BtreePagePayload currPage;
    currPage.InitByBuffDesc(currLevel->currPageBuf);

    /* The last added item is about to be moved to its right sibling page (new page) as the first data item */
    IndexTuple *rightFirst = currPage.GetPage()->GetIndexTuple(currLevel->lastItemOffset);
    /* Item on left of the last added item would become the last item on the left page */
    IndexTuple *leftLast = currPage.GetPage()->GetIndexTuple(OffsetNumberPrev(currLevel->lastItemOffset));
    /* We should only truncate non-interval tuples on leaf page */
    /* We don't need to copy since memory used by leftHikey of a leaf page is allocated by calling
     * CreateTruncateInternalTuple. It's not read from a page, thus it won't be stale after the current left
     * page unlocked and released */
    /* For internal page, leftHikey was read from page. must copy. */
    IndexTuple *leftHikey = currPage.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE) ?
                            CreateTruncateInternalTuple(leftLast, rightFirst) : rightFirst->Copy();
    if (unlikely(leftHikey == nullptr)) {
        if (StorageGetErrorCode() == INDEX_ERROR_TUPLE_DAMAGED) {
            Btree::DumpDamagedTuple(rightFirst, currPage.GetPage(), currLevel->lastItemOffset);
        }
        return DSTORE_FAIL;
    }

    /* Step 2. Write high key on left page then save it as currLevel->m_lastPageHikey for next split */
    /* Current page is splitting left */
    ItemId *leftHikeyItemId = currPage.GetPage()->GetItemIdPtr(BTREE_PAGE_HIKEY);
    ItemId *levelLastItemId = currPage.GetPage()->GetItemIdPtr(currLevel->lastItemOffset);
    /* Copy level-last-ItemId to hikey since */
    *leftHikeyItemId = *levelLastItemId;
    /* Then remove levelLastItemId */
    levelLastItemId->SetUnused();
    currPage.GetPage()->RemoveLastItem();

    /* Overwrite high key if it is on leaf and has been truncated */
    if (currPage.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)) {
        if (STORAGE_FUNC_FAIL(currPage.GetPage()->OverwriteTuple(BTREE_PAGE_HIKEY, leftHikey))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to overwrite hikey on {%d, %u}",
                currPage.GetPageId().m_fileId, currPage.GetPageId().m_blockId));
            return DSTORE_FAIL;
        }
    }
    currLevel->lastPageHikey = leftHikey;
    return DSTORE_SUCC;
}

void BtreeBuild::WritePage(BufferDesc *pageBuf)
{
    /* Check page correctness before writing */
    StorageAssert(static_cast<BtrPage *>(pageBuf->GetPage())->CheckSanity());
    /* Must mark dirty before writing redo log */
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(pageBuf));
    if (NeedWal()) {
        GenerateWriteBtreePageWal(pageBuf);
    }
    m_bufMgr->UnlockAndRelease(pageBuf);
}

RetStatus BtreeBuild::CompleteRightmostPages(PageId &rootPageId, uint32 &rootLevel)
{
    /* Begin to process rightmost page from leaf level */
    BtreePageLevelBuilder *currLevel = m_leafLevel;
    while (currLevel != nullptr && currLevel->currPageBuf != INVALID_BUFFER_DESC) {
        /* Step 1. Remove reserved high key space for rightmost page */
        if (STORAGE_FUNC_FAIL(RemoveHikeySpaceOnRightmostPage(currLevel))) {
            return DSTORE_FAIL;
        }

        BtreePagePayload currPage;
        currPage.InitByBuffDesc(currLevel->currPageBuf);
        BtreePageLevelBuilder *parentLevel = currLevel->higherLevelBuilder;
        if (parentLevel == nullptr) {
            /* Step 2. Set root if no more parent pages */
            /* Check correctness of root page */
            if (currPage.GetLinkAndStatus()->GetLeft().IsValid() || currPage.GetLinkAndStatus()->GetRight().IsValid()) {
                storage_set_error(INDEX_ERROR_FAIL_BUILD_INDEX_FOR_MISSING_ROOT,
                                  currPage.GetPageId().m_fileId, currPage.GetPageId().m_blockId);
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Missing root while complete {%d, %u} in level %u"
                    BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, currPage.GetPageId().m_fileId,
                    currPage.GetPageId().m_blockId, currLevel->level, BTR_PAGE_HEADER_VAL(currPage.GetPage()),
                    BTR_PAGE_LINK_AND_STATUS_VAL(currPage.GetLinkAndStatus())));
                return DSTORE_FAIL;
            }
            currPage.GetLinkAndStatus()->SetRoot(true);
            rootPageId = currPage.GetPageId();
            rootLevel = currLevel->level;
            /* No parent to add downlink. free it if any */
            if (currLevel->lastPageHikey != nullptr) {
                DstorePfree(currLevel->lastPageHikey);
                currLevel->lastPageHikey = nullptr;
            }
        } else {
            /* Step 3. Add downlink to parent if currlevel has a parent */
            if (STORAGE_FUNC_FAIL(AddPageDownlinkToParent(currLevel))) {
                return DSTORE_FAIL;
            }
        }

        /* Step 4. Write down the rightmost page in current level */
        WritePage(currPage.GetBuffDesc());
        /* currPage would be unlocked and released after writing down. no need to keep the buffer descriptor pointer */
        DstorePfree(currLevel);
        currLevel = parentLevel;
    }
    m_leafLevel = nullptr;
    return DSTORE_SUCC;
}

/* We've reserved space for high key ItemId for each page. However rightmost pages don't need high key. Calling this
 * function to remove the reserved high keys for rightmost pages */
RetStatus BtreeBuild::RemoveHikeySpaceOnRightmostPage(BtreePageLevelBuilder *currLevel)
{
    BtreePagePayload rightmost;
    rightmost.InitByBuffDesc(currLevel->currPageBuf);
    /* Check rightmost page */
    OffsetNumber maxOffsetNumOnPage = rightmost.GetPage()->GetMaxOffset();
    if (unlikely(!rightmost.GetLinkAndStatus()->IsRightmost() || maxOffsetNumOnPage < BTREE_PAGE_FIRSTKEY)) {
        storage_set_error(INDEX_ERROR_FAIL_BUILD_INDEX_FOR_PAGE_DAMAGED,
                          rightmost.GetPageId().m_fileId, rightmost.GetPageId().m_blockId);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Rightmost {%d, %u} in level %u is damaged" BTR_PAGE_HEADER_FMT
            BTR_PAGE_LINK_AND_STATUS_FMT, rightmost.GetPageId().m_fileId, rightmost.GetPageId().m_blockId,
            currLevel->level, BTR_PAGE_HEADER_VAL(rightmost.GetPage()),
            BTR_PAGE_LINK_AND_STATUS_VAL(rightmost.GetLinkAndStatus())));
        return DSTORE_FAIL;
    }
    /* Move ItemIds */
    ItemId *currItemId = rightmost.GetPage()->GetItemIdPtr(BTREE_PAGE_HIKEY);
    for (OffsetNumber offNum = BTREE_PAGE_FIRSTKEY; offNum <= maxOffsetNumOnPage; offNum = OffsetNumberNext(offNum)) {
        ItemId *nextItemId = rightmost.GetPage()->GetItemIdPtr(offNum);
        *currItemId = *nextItemId;
        currItemId = nextItemId;
    }
    /* Update lower to remove the last ItemId */
    rightmost.GetPage()->SetLower(static_cast<uint16>(rightmost.GetPage()->GetLower() -
                                                      static_cast<uint16>(sizeof(ItemId))));
    StorageAssert(rightmost.GetPage()->CheckSanity());
    return DSTORE_SUCC;
}

void BtreeBuild::HandleErrAndClear()
{
    BtreePageLevelBuilder *currLevel = m_leafLevel;
    while (currLevel != nullptr) {
        if (currLevel->lastPageHikey != nullptr) {
            DstorePfree(currLevel->lastPageHikey);
        }
        if (currLevel->currPageBuf != INVALID_BUFFER_DESC) {
            /* currPageBuf would be unlock and released after building process finished unless returning with error.
             * No need to check page before unlock in this branch because of error */
            m_bufMgr->UnlockAndRelease(currLevel->currPageBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        }
        BtreePageLevelBuilder *parentLevel = currLevel->higherLevelBuilder;
        DstorePfree(currLevel);
        currLevel = parentLevel;
    }

    if (m_tuplesortMgr != nullptr) {
        if (unlikely(!m_tuplesortMgr->UniqueCheckSucc())) {
            /* Copy duplicated keys from TuplesortMgr to IndexBuildInfo for outer callers when unique check fails */
            CopyDuplicateTupleFromTupleSortMgr();
        }
        m_tuplesortMgr->Destroy();
        delete m_tuplesortMgr;
        m_tuplesortMgr = nullptr;
    }
}

void BtreeBuild::GenerateWriteBtreePageWal(BufferDesc *btrPageBuf)
{
    BtrPage *page = static_cast<BtrPage *>(btrPageBuf->GetPage());
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
    walContext->RememberPageNeedWal(btrPageBuf);
    WalRecordBtreeBuild redoData;
    redoData.SetHeader(page, (page->GetWalId() != walContext->GetWalId()), btrPageBuf->GetFileVersion());
    redoData.SetData(page);
    walContext->PutNewWalRecord(&redoData);
    UNUSED_VARIABLE(walContext->EndAtomicWal());
}

void BtreeBuild::GenerateInitMetaPageWal(BufferDesc *btrMetaBuf)
{
    BtrPage *page = static_cast<BtrPage *>(btrMetaBuf->GetPage());
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());

    WalRecordBtreeInitMetaPage redoData;
    redoData.SetHeader(page, glsnChangedFlag, btrMetaBuf->GetFileVersion());
    redoData.SetData(page);

    walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
    walContext->RememberPageNeedWal(btrMetaBuf);
    walContext->PutNewWalRecord(&redoData);
    UNUSED_VARIABLE(walContext->EndAtomicWal());
}
}
