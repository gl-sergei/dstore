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
 * dstore_index_temp_segment.cpp
 *
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_index_temp_segment.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tablespace/dstore_index_temp_segment.h"
#include "common/log/dstore_log.h"
#include "index/dstore_btree_recycle_wal.h"
#include "lock/dstore_lock_datatype.h"
#include "transaction/dstore_transaction.h"
#include "wal/dstore_wal_write_context.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "tablespace/dstore_tablespace_utils.h"
#include "index/dstore_btree_recycle_partition.h"
#include "page/dstore_btr_recycle_root_meta_page.h"
#include "page/dstore_index_page.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"
#include "tablespace/dstore_table_space_perfunit.h"

namespace DSTORE {

IndexTempSegment::IndexTempSegment(PdbId pdbId, const PageId &segmentId,
    TablespaceId tablespaceId, BufMgrInterface *bufMgr, DstoreMemoryContext ctx)
    : IndexSegment(pdbId, segmentId, tablespaceId, bufMgr, ctx, SegmentType::INDEX_TEMP_SEGMENT_TYPE)
{}

IndexTempSegment::~IndexTempSegment()
{}

RetStatus IndexTempSegment::DropSegment()
{
    if (unlikely(!m_isInitialized)) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return DSTORE_FAIL;
    }
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(DropSegmentInternal())) {
        return DSTORE_FAIL;
    }
    m_isDrop = true;
    return DSTORE_SUCC;
}

RetStatus IndexTempSegment::GetNewPageInternal(PageId *newPageId)
{
    LatencyStat::Timer timer(&TableSpacePerfUnit::GetInstance().m_indexNewPageLatency);

    uint16 numFreeDataPages;
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
    if (STORAGE_FUNC_FAIL(PrepareFreeDataPages(&numFreeDataPages, segMetaPageBuf))) {
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        return DSTORE_FAIL;
    }

    if (unlikely(numFreeDataPages == 0)) {
        storage_set_error(TBS_ERROR_SEGMENT_HAS_NO_SPACE);
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        return DSTORE_FAIL;
    }

    uint16 numNewPages = DstoreMin(numFreeDataPages, this->m_maxAddNewPageCount);
    PageId newPages[PAGES_ADD_TO_FSM_PER_TIME];
    if (STORAGE_FUNC_FAIL(GetFreeDataPageIds(segMetaPageBuf, numNewPages, newPages))) {
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        return DSTORE_FAIL;
    }

    DataSegmentMetaPage *metaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    metaPage->AddDataPages(newPages[0], newPages[numNewPages - 1], numNewPages);
    bool pagesIsReused = metaPage->lastExtentIsReused;
    (void)m_bufMgr->MarkDirty(segMetaPageBuf);
    uint16 numNewBtrPages = numNewPages;
    CreateNewPages(newPages, numNewBtrPages, pagesIsReused);
    if (numNewPages > 1) {
        if (STORAGE_FUNC_FAIL(
            m_btrRecyclePartition->FreeListBatchPushNewPages(&newPages[1], static_cast<uint16>(numNewPages - 1),
                                                             numNewBtrPages))) {
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed FreeListBatchPushNewPages %u", numNewPages));
            return DSTORE_FAIL;
        }
    }

    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    /* Make sure we have a valid PageId returned here */
    *newPageId = newPages[0];
    return DSTORE_SUCC;
}

PageId IndexTempSegment::GetNewPage(UNUSE_PARAM bool isExtendBg)
{
    if (unlikely(!m_isInitialized)) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return INVALID_PAGE_ID;
    }
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return INVALID_PAGE_ID;
    }
    StorageAssert(m_btrRecyclePartition != nullptr);

    PageId targetPageId = INVALID_PAGE_ID;
    bool needRetry = false;
    targetPageId = m_btrRecyclePartition->FreeListPop(needRetry);
    if (targetPageId != INVALID_PAGE_ID) {
        return targetPageId;
    }

    if (STORAGE_FUNC_FAIL(GetNewPageInternal(&targetPageId))) {
        return INVALID_PAGE_ID;
    }
    return targetPageId;
}

PageId IndexTempSegment::GetNewPageFromUnassignedPages()
{
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
    DataSegmentMetaPage *metaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    uint16 unassignedCount = metaPage->GetUnassignedPageCount();
    if (unassignedCount == 0) {
        PageId newExtMetaPageId = INVALID_PAGE_ID;
        if (STORAGE_FUNC_FAIL(GetNewExtent(&newExtMetaPageId, segMetaPageBuf))) {
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return INVALID_PAGE_ID;
        }
    }

    metaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    PageId newPageId = {metaPage->addedPageId.m_fileId,
                        metaPage->addedPageId.m_blockId + 1};
    metaPage->addedPageId.m_blockId += 1;
    (void)m_bufMgr->MarkDirty(segMetaPageBuf);
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    return newPageId;
}

RetStatus IndexTempSegment::InitIndexTempSegMetaInfo(PdbId pdbId, BufMgrInterface *bufMgr, PageId segMetaPageId,
    bool isReuseFlag)
{
    BufferDesc *segMetaPageBuf = bufMgr->Read(pdbId, segMetaPageId, LW_EXCLUSIVE, BufferPoolReadFlag::CreateNewPage());
    if (unlikely(segMetaPageBuf == INVALID_BUFFER_DESC)) {
        return DSTORE_FAIL;
    }
    DataSegmentMetaPage *segMetaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    StoragePdb *storagePdb = g_storageInstance->GetPdb(pdbId);
    if (storagePdb == nullptr) {
        bufMgr->UnlockAndRelease(segMetaPageBuf);
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("fail to get pdb %hhu", pdbId));
        return DSTORE_FAIL;
    }
    WalStreamManager *walStreamMgr = storagePdb->GetWalMgr()->GetWalStreamManager();
    WalId *walStreamIds = nullptr;
    uint32 walStreamCnt = walStreamMgr->GetOwnedStreamIds(&walStreamIds);
    if (walStreamCnt == 0) {
        bufMgr->UnlockAndRelease(segMetaPageBuf);
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("pdb %s has 0 live wal stream", storagePdb->GetPdbName()));
        return DSTORE_FAIL;
    }
    uint64 plsn = walStreamMgr->GetWalStream(*walStreamIds)->GetMaxAppendedPlsn();
    uint64 glsn = segMetaPage->GetGlsn();
    if (STORAGE_FUNC_FAIL(segMetaPage->InitDataSegmentMetaPage(SegmentType::INDEX_TEMP_SEGMENT_TYPE, segMetaPageId,
        TEMP_TABLE_EXT_SIZE, plsn, glsn))) {
        bufMgr->UnlockAndRelease(segMetaPageBuf);
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("Init index temp segment(%hu, %u) failed. pdb %u",
            segMetaPageId.m_fileId, segMetaPageId.m_blockId, pdbId));
        return DSTORE_FAIL;
    }

    uint32 numStaticAlloc = NUM_BTR_META_PAGE + NUM_RECYCLE_ROOT_META_PAGE;
    PageId lastAllocPage = {segMetaPageId.m_fileId, segMetaPageId.m_blockId + numStaticAlloc};

    segMetaPage->InitSegmentInfo(lastAllocPage, isReuseFlag);
    (void)bufMgr->MarkDirty(segMetaPageBuf);
    bufMgr->UnlockAndRelease(segMetaPageBuf);
    return DSTORE_SUCC;
}

void IndexTempSegment::AllocTempRecycleRoot(PdbId pdbId, PageId segMetaPageId, BufMgrInterface *bufMgr)
{
    /* The BtrRecycleRootMeta always comes after BtrMetaPage. */
    PageId recycleRootMetaPageId = {segMetaPageId.m_fileId, segMetaPageId.m_blockId + NUM_BTR_META_PAGE + 1};
    /* Lock recycle root meta page and init */
    BufferDesc *recycleRootMetaBuf = bufMgr->Read(pdbId, recycleRootMetaPageId, LW_EXCLUSIVE,
                                                  BufferPoolReadFlag::CreateNewPage());
    STORAGE_CHECK_BUFFER_PANIC(recycleRootMetaBuf, MODULE_SEGMENT, recycleRootMetaPageId);
    BtrRecycleRootMetaPage *metaPage = static_cast<BtrRecycleRootMetaPage *>(recycleRootMetaBuf->GetPage());
    metaPage->InitRecycleRootMetaPage(recycleRootMetaPageId, thrd->GetCurrentXid());
    (void)bufMgr->MarkDirty(recycleRootMetaBuf);
    bufMgr->UnlockAndRelease(recycleRootMetaBuf);
}

PageId IndexTempSegment::AllocIndexTempSegment(PdbId pdbId, TablespaceId tablespaceId,
    BufMgrInterface *bufMgr, Oid tableOid)
{
    if (unlikely(tablespaceId == INVALID_TABLESPACE_ID || bufMgr == nullptr)) {
        storage_set_error(TBS_ERROR_SEGMENT_PARAMETER_INVALID);
        return INVALID_PAGE_ID;
    }

    /* Step 1. Alloc extent from tablespace for new IndexSegment */
    PageId segMetaPageId;
    bool isReuseFlag = false;
    if (STORAGE_FUNC_FAIL(
        AllocExtent(pdbId, tablespaceId, TEMP_TABLE_EXT_SIZE, &segMetaPageId, &isReuseFlag))) {
        if (StorageGetErrorCode() == TBS_ERROR_TABLESPACE_USE_UP) {
            ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
            ErrMsg("Alloc index temp segment fail, pdb %u tablespace %hu has no space", pdbId, tablespaceId));
        }
        return INVALID_PAGE_ID;
    }

    /* Step 2. Init segment meta page */
    /* It's OK to hold lock for segMetaPageBuf until all initialization process finish because no one would know
     * or access segMetaPageId before we return */
    if (STORAGE_FUNC_FAIL(InitIndexTempSegMetaInfo(pdbId, bufMgr, segMetaPageId, isReuseFlag))) {
        return INVALID_PAGE_ID;
    }

    /* Step 3. Allocate recycle root for index segment */
    if (STORAGE_FUNC_FAIL(thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        return INVALID_PAGE_ID;
    }
    AllocTempRecycleRoot(pdbId, segMetaPageId, bufMgr);

    /* Step 4. Construct IndexTempSegment object */

    IndexTempSegment segment(pdbId, segMetaPageId, tablespaceId, bufMgr, g_dstoreCurrentMemoryContext);
    if (STORAGE_FUNC_FAIL(segment.InitSegment())) {
        /* IndexSegment init failed */
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Index temp segment(%hu, %u) initialization failed, oid %u. pdb %u, tablespace %hu",
                   segMetaPageId.m_fileId, segMetaPageId.m_blockId, tableOid, pdbId, tablespaceId));
        return INVALID_PAGE_ID;
    }

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
        ErrMsg("Alloc index temp segment for oid %u success, SegmentId (%hu, %u) %s reused. pdb %u tablespace %hu",
            tableOid, segMetaPageId.m_fileId,
            segMetaPageId.m_blockId, (isReuseFlag ? "is" : "isn't"), pdbId, tablespaceId));
    return segment.GetSegmentMetaPageId();
}
}
