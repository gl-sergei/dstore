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
 * dstore_index_normal_segment.cpp
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_index_normal_segment.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tablespace/dstore_index_normal_segment.h"
#include "common/log/dstore_log.h"
#include "index/dstore_btree_recycle_wal.h"
#include "lock/dstore_lock_datatype.h"
#include "lock/dstore_table_lock_mgr.h"
#include "page/dstore_data_segment_meta_page.h"
#include "page/dstore_index_page.h"
#include "wal/dstore_wal_write_context.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "tablespace/dstore_tablespace_utils.h"
#include "index/dstore_btree_recycle_partition.h"
#include "page/dstore_btr_recycle_root_meta_page.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"
#include "tablespace/dstore_table_space_perfunit.h"
#include "transaction/dstore_transaction.h"

namespace DSTORE {

IndexNormalSegment::IndexNormalSegment(PdbId pdbId, const PageId &segmentId, TablespaceId tablespaceId,
                                       BufMgrInterface *bufMgr, DstoreMemoryContext ctx)
    : IndexSegment(pdbId, segmentId, tablespaceId, bufMgr, ctx)
{}

IndexNormalSegment::~IndexNormalSegment()
{}

RetStatus IndexNormalSegment::InitIndexSegMetaInfo(PdbId pdbId, BufMgrInterface *bufMgr, PageId segMetaPageId,
    bool isReUsedFlag)
{
	BufferDesc *segMetaPageBuf = bufMgr->Read(pdbId, segMetaPageId, LW_EXCLUSIVE);
    if (unlikely(segMetaPageBuf == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("fail to read buffer(%hu, %u), pdb %u",
            segMetaPageId.m_fileId, segMetaPageId.m_blockId, pdbId));
        return DSTORE_FAIL;
    }

    DataSegmentMetaPage *segMetaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    StoragePdb *storagePdb = g_storageInstance->GetPdb(pdbId);
    if (storagePdb == nullptr) {
        bufMgr->UnlockAndRelease(segMetaPageBuf);
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("fail to get pdb %u", pdbId));
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
    if (STORAGE_FUNC_FAIL(segMetaPage->InitDataSegmentMetaPage(SegmentType::INDEX_SEGMENT_TYPE,
        segMetaPageId, FIRST_EXT_SIZE, plsn, glsn))) {
        bufMgr->UnlockAndRelease(segMetaPageBuf);
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("Init index segment(%hu, %u) failed. pdb %u",
            segMetaPageId.m_fileId, segMetaPageId.m_blockId, pdbId));
        return DSTORE_FAIL;
    }

    uint32 numStaticAlloc = NUM_BTR_META_PAGE + NUM_RECYCLE_ROOT_META_PAGE;
    PageId lastAllocPage = {segMetaPageId.m_fileId, segMetaPageId.m_blockId + numStaticAlloc};
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;

    /* Initialize the SegmentMetaPage */
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    segMetaPage->InitSegmentInfo(lastAllocPage, isReUsedFlag);
    (void)bufMgr->MarkDirty(segMetaPageBuf);

    /* Init segment meta page wal */
    bool glsnChangedFlag = (segMetaPage->GetWalId() != walWriterContext->GetWalId());
    /* Init segment meta page wal */
    WalRecordTbsInitDataSegment segMetaWalData;
    segMetaWalData.SetHeader({WAL_TBS_INIT_DATA_SEGMENT_META, sizeof(segMetaWalData), segMetaPageId,
                              segMetaPage->GetWalId(), segMetaPage->GetPlsn(), segMetaPage->GetGlsn(), glsnChangedFlag,
                              segMetaPageBuf->GetFileVersion()});
    segMetaWalData.SetData(SegmentType::INDEX_SEGMENT_TYPE, lastAllocPage, plsn, glsn, isReUsedFlag);
    walWriterContext->RememberPageNeedWal(segMetaPageBuf);
    walWriterContext->PutNewWalRecord(&segMetaWalData);
    (void)walWriterContext->EndAtomicWal();

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
        ErrMsg("Init index segment(%hu, %u) success, plsn is %lu, glsn is %lu, pdb is %u.",
            segMetaPageId.m_fileId, segMetaPageId.m_blockId, plsn, glsn, pdbId));

	bufMgr->UnlockAndRelease(segMetaPageBuf);
    return DSTORE_SUCC;
}


RetStatus IndexNormalSegment::DropSegment()
{
    if (unlikely(!m_isInitialized)) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return DSTORE_FAIL;
    }
    LockTag tag;
    tag.SetTableExtensionLockTag(this->GetPdbId(), GetSegmentMetaPageId());
    LockErrorInfo error = {0};
    TableLockMgr *lockMgr = g_storageInstance->GetTableLockMgr();
    if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK, false, &error))) {
        return DSTORE_FAIL;
    }
    if (unlikely(m_isDrop)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK);
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(DropSegmentInternal())) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK);
        return DSTORE_FAIL;
    }
    m_isDrop = true;
    lockMgr->Unlock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK);
    return DSTORE_SUCC;
}

PageId IndexNormalSegment::AllocIndexNormalSegment(PdbId pdbId, TablespaceId tablespaceId,
                                                                BufMgrInterface *bufMgr, Oid tableOid)
{
    if (unlikely(tablespaceId == INVALID_TABLESPACE_ID || bufMgr == nullptr)) {
        storage_set_error(TBS_ERROR_SEGMENT_PARAMETER_INVALID);
        return INVALID_PAGE_ID;
    }

    /* Step 1. Alloc extent from tablespace for new IndexNormalSegment */
    PageId segMetaPageId;
    bool isReuseFlag = false;
    if (STORAGE_FUNC_FAIL(AllocExtent(pdbId, tablespaceId, FIRST_EXT_SIZE, &segMetaPageId, &isReuseFlag))) {
        if (StorageGetErrorCode() == TBS_ERROR_TABLESPACE_USE_UP) {
            ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
                ErrMsg("Alloc new index segment fail, pdb %hhu tablespace %hu has no space", pdbId, tablespaceId));
        }
        return INVALID_PAGE_ID;
    }

    /* Step 2. Init segment meta page */
    /* It's OK to hold lock for segMetaPageBuf until all initialization process finish because no one would know
     * or access segMetaPageId before we return */
    if (STORAGE_FUNC_FAIL(InitIndexSegMetaInfo(pdbId, bufMgr, segMetaPageId, isReuseFlag))) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Index data segment(%hu, %u) init meta info failed.",
            segMetaPageId.m_fileId, segMetaPageId.m_blockId));
        return INVALID_PAGE_ID;
    }

    /* Step 3. Allocate recycle root for index segment */
    /* We're going to write xid of creation onto the recycle root page. Get an Xid first. */
    if (STORAGE_FUNC_FAIL(thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Index data segment(%hu, %u) AllocTransactionSlot failed.", segMetaPageId.m_fileId,
                      segMetaPageId.m_blockId));
        return INVALID_PAGE_ID;
    }
    Xid currentXid = thrd->GetCurrentXid();
    StorageAssert(currentXid != INVALID_XID);
    AllocRecycleRoot(pdbId, segMetaPageId, bufMgr, currentXid);

    /* Step 4. Construct IndexNormalSegment object */
    IndexNormalSegment segment(pdbId, segMetaPageId, tablespaceId, bufMgr, g_dstoreCurrentMemoryContext);

    if (STORAGE_FUNC_FAIL(segment.InitSegment())) {
        /* IndexNormalSegment init failed */
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Index data segment (%hu, %u) initialization failed.", segMetaPageId.m_fileId,
                      segMetaPageId.m_blockId));
        return INVALID_PAGE_ID;
    }

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
        ErrMsg("Alloc new index segment for oid %u success, SegmentId (%hu, %u) %s reused."
            " xid(%u, %lu) pdb %u tbs %u", tableOid, segMetaPageId.m_fileId, segMetaPageId.m_blockId,
            (isReuseFlag ? "is" : "isn't"), static_cast<uint32>(thrd->GetCurrentXid().m_zoneId),
            thrd->GetCurrentXid().m_logicSlotId, pdbId, tablespaceId));
    return segment.GetSegmentMetaPageId();
}

void IndexNormalSegment::AllocRecycleRoot(PdbId pdbId, PageId segMetaPageId, BufMgrInterface *bufMgr, Xid currXid)
{
    /* The BtrRecycleRootMeta always comes after BtrMetaPage. */
    PageId recycleRootMetaPageId = {segMetaPageId.m_fileId, segMetaPageId.m_blockId + NUM_BTR_META_PAGE + 1};
    /* Lock recycle root meta page and init */
    BufferDesc *recycleRootMetaBuf = bufMgr->Read(pdbId, recycleRootMetaPageId, LW_EXCLUSIVE);
    BtrRecycleRootMetaPage *metaPage = static_cast<BtrRecycleRootMetaPage *>(recycleRootMetaBuf->GetPage());
    metaPage->InitRecycleRootMetaPage(recycleRootMetaPageId, currXid);

    (void)bufMgr->MarkDirty(recycleRootMetaBuf);

    /* Record wal */
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    walWriterContext->RememberPageNeedWal(recycleRootMetaBuf);
    WalRecordBtrRecycleRootMetaInit redoData;
    uint16 redoDataSize = sizeof(WalRecordBtrRecycleRootMetaInit);

    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_ROOT_META_INIT,
                                .size = redoDataSize,
                                .pageId = recycleRootMetaBuf->GetPageId(),
                                .preWalId = metaPage->GetWalId(),
                                .prePlsn = metaPage->GetPlsn(),
                                .preGlsn = metaPage->GetGlsn(),
                                .glsnChangedFlag = metaPage->GetWalId() != walWriterContext->GetWalId(),
                                .preVersion = recycleRootMetaBuf->GetFileVersion()};

    redoData.SetHeader(header, currXid);
    walWriterContext->PutNewWalRecord(&redoData);
    (void)walWriterContext->EndAtomicWal();

    bufMgr->UnlockAndRelease(recycleRootMetaBuf);
}

}
