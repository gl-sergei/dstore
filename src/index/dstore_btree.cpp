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
 * dstore_btree.cpp
 *
 * IDENTIFICATION
 *        dstore/src/index/dstore_btree.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "catalog/dstore_function.h"
#include "index/dstore_btree_recycle_partition.h"
#include "index/dstore_btree_wal.h"
#include "index/dstore_btree_scan.h"
#include "page/dstore_btr_recycle_root_meta_page.h"
#include "wal/dstore_wal_write_context.h"
#include "errorcode/dstore_index_error_code.h"
#include "tablespace/dstore_index_temp_segment.h"
#include "undo/dstore_undo_wal.h"

namespace DSTORE {

BtreeStorageMgr::BtreeStorageMgr(PdbId pdbId, const int fillFactor, const RelationPersistence persistenceMethod)
    : m_segment(nullptr),
      m_segMetaPageId(INVALID_PAGE_ID),
      m_lowestSingleDescCache(nullptr),
      m_pdbId(pdbId),
      m_btrMetaPageId(INVALID_PAGE_ID),
      m_lastInsertionPageId(INVALID_PAGE_ID),
      m_btrMetaCache(nullptr),
      m_fillFactor(fillFactor == INVALID_INDEX_FILLFACTOR ? BTREE_DEFAULT_FILLFACTOR : fillFactor),
      m_persistenceMethod(persistenceMethod),
      m_ctx(nullptr),
      m_buildingXid(INVALID_XID)
{}

BtreeStorageMgr::~BtreeStorageMgr()
{
    ReleaseMetaCache();
    if (m_segment != nullptr) {
        delete m_segment;
        m_segment = nullptr;
    }
    m_btrMetaCache = nullptr;
    m_lowestSingleDescCache = nullptr;
    m_ctx = nullptr;
}


bool BtreeStorageMgr::IsBtrSmgrValid(BufMgrInterface *bufMgr, Xid *createdXid)
{
    StorageAssert(createdXid != nullptr);
    *createdXid = INVALID_XID;
    if (likely(m_segment->m_btrRecyclePartition == nullptr) &&
        STORAGE_FUNC_FAIL(m_segment->InitBtrRecyclePartition())) {
        return false;
    }
    Xid recyclePartXid = m_segment->m_btrRecyclePartition->createdXid;
    if (unlikely(recyclePartXid == INVALID_XID)) {
        return false;
    }

    PageId segmentId = m_segment->GetSegmentMetaPageId();
    PageId btrMetaPageId = {segmentId.m_fileId, segmentId.m_blockId + NUM_PAGES_FOR_SEGMENT_META};

    /* Check BtreeMeta */
    BufferDesc *btrMetaBuf = bufMgr->Read(m_segment->GetPdbId(), btrMetaPageId, LW_SHARED);
    if (unlikely(btrMetaBuf == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_WARNING, MODULE_INDEX, ErrMsg("Failed index m_segment {%hu, %u} is invalid, btrMeta missed",
                                                    segmentId.m_fileId, segmentId.m_blockId));
        return false;
    }
    BtrPage *metaPage = static_cast<BtrPage *>(btrMetaBuf->GetPage());
    if (unlikely(metaPage->GetType() != PageType::INDEX_PAGE_TYPE) ||
        unlikely(!metaPage->GetLinkAndStatus()->TestType(BtrPageType::META_PAGE))) {
        ErrLog(DSTORE_WARNING, MODULE_INDEX, ErrMsg("Failed index m_segment {%hu, %u} is invalid, btrMeta invalid",
                                                    segmentId.m_fileId, segmentId.m_blockId));
        bufMgr->UnlockAndRelease(btrMetaBuf);
        return false;
    }
    BtrMeta *btrMeta =  STATIC_CAST_PTR_TYPE(metaPage->GetData(), BtrMeta *);
    if (unlikely(btrMeta->initializedtMagicNum != BTREE_META_STAT_INIT_MAGIC_NUM) ||
        unlikely(btrMeta->createXid == INVALID_XID) ||
        unlikely(btrMeta->nkeyAtts <= 0 || btrMeta->nkeyAtts > INDEX_MAX_KEY_NUM)) {
        ErrLog(DSTORE_WARNING, MODULE_INDEX, ErrMsg("Failed index m_segment {%hu, %u} is invalid, btrMeta changed",
                                                    segmentId.m_fileId, segmentId.m_blockId));
        bufMgr->UnlockAndRelease(btrMetaBuf);
        return false;
    }
    bool ret = (recyclePartXid == btrMeta->createXid);
    *createdXid = btrMeta->createXid;
    if (STORAGE_FUNC_FAIL(SetMetaCache(btrMeta))) {
        ret = false;
    }
    bufMgr->UnlockAndRelease(btrMetaBuf);

    return ret;
}

RetStatus BtreeStorageMgr::Init(const PageId segmentId, TablespaceId tablespaceId, DstoreMemoryContext context)
{
    /*
     * Store memory for SMGR inside instance-level MEMORY_CONTEXT_SMGR memory group
     * SEGTODO: This is workaround. Change this to be session level MEMORY_CONTEXT_SMGR after supported
     */
    m_ctx = context;
    if (!IsGlobalTempIndex()) {
        m_segment = DstoreNew(m_ctx) IndexNormalSegment(m_pdbId, segmentId, tablespaceId,
            g_storageInstance->GetBufferMgr(), m_ctx);
    } else {
        m_segment = DstoreNew(m_ctx) IndexTempSegment(m_pdbId, segmentId, tablespaceId,
            thrd->GetTmpLocalBufMgr(), m_ctx);
    }
    if (STORAGE_VAR_NULL(m_segment)) {
        storage_set_error(INDEX_ERROR_FAIL_CREATE_BTREE_SMGR, segmentId.m_fileId, segmentId.m_blockId);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("Failed to new index sgement {%d, %u}", segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_segment->InitSegment())) {
        storage_set_error(INDEX_ERROR_FAIL_CREATE_BTREE_SMGR, segmentId.m_fileId, segmentId.m_blockId);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("Failed to init index sgement {%d, %u}", segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }
    m_segMetaPageId = segmentId;
    m_btrMetaPageId = {m_segMetaPageId.m_fileId, m_segMetaPageId.m_blockId + NUM_PAGES_FOR_SEGMENT_META};
    return DSTORE_SUCC;
}

BtrMeta *BtreeStorageMgr::GetBtrMeta(LWLockMode access, BufferDesc **desc)
{
    if (m_btrMetaPageId == INVALID_PAGE_ID) {
        storage_set_error(INDEX_ERROR_UNKNOWN_META_PAGE, m_segMetaPageId.m_fileId, m_segMetaPageId.m_blockId);
        ErrLog(DSTORE_PANIC, MODULE_INDEX,
               ErrMsg("Btree meta page id is unkown. segment is{%d, %u}", m_segMetaPageId.m_fileId,
                      m_segMetaPageId.m_blockId));
    }
    BufMgrInterface *bufMgr = IsGlobalTempIndex() ? thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr();
    BufferDesc* metaDesc = Btree::ReadAndCheckBtrPage(m_btrMetaPageId, access, bufMgr, m_pdbId, false);
    if (STORAGE_VAR_NULL(metaDesc)) {
        *desc = nullptr;
        return nullptr;
    }
    if (desc != nullptr) {
        *desc = metaDesc;
    }
    BtrPage *metaPage = static_cast<BtrPage *>(metaDesc->GetPage());
    BtrMeta *btreeMeta = static_cast<BtrMeta *>(static_cast<void *>(metaPage->GetData()));
    return btreeMeta;
}

RetStatus BtreeStorageMgr::GetNewPage(BtreePagePayload &payload, uint8 tdcount, Xid checkingXid)
{
    PageId freePageId = GetFromFreeQueue(checkingXid);
    if (unlikely(!freePageId.IsValid())) {
        storage_set_error(INDEX_ERROR_FAIL_CREATE_NEW_PAGE, m_segMetaPageId.m_fileId, m_segMetaPageId.m_blockId);
        return DSTORE_FAIL;
    }
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("Get page {%hu, %u}, SegMeta{%hu, %u}", freePageId.m_fileId, freePageId.m_blockId,
        m_segMetaPageId.m_fileId, m_segMetaPageId.m_blockId));
#endif
    BufMgrInterface *bufMgr = IsGlobalTempIndex() ? thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr();
    BufferDesc *freePageBuf = Btree::ReadAndCheckBtrPage(freePageId, LW_EXCLUSIVE, bufMgr, m_pdbId, false);
    if (unlikely(freePageBuf == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("GetNewPage: Read page {%hu, %u} failed, SegMeta{%hu, %u}",
            freePageId.m_fileId, freePageId.m_blockId, m_segMetaPageId.m_fileId, m_segMetaPageId.m_blockId));
        return DSTORE_FAIL;
    }
    BtrPage *freePage = static_cast<BtrPage *>(freePageBuf->GetPage());
    freePage->Reset(freePageId);

    /* Clear page, update live status and allocate td space if needed */
    payload.InitByBuffDesc(freePageBuf);

    payload.GetPage()->EmptyPage();
    payload.GetLinkAndStatus()->SetLiveStatus(BtrPageLiveStatus::NORMAL_USING);

    if (tdcount > 0) {
        payload.GetPage()->AllocateTdSpace(tdcount);
    }
    return DSTORE_SUCC;
}

RetStatus BtreeStorageMgr::PutIntoRecycleQueue(const RecyclablePage recyclablePage)
{
    if (STORAGE_FUNC_SUCC(m_segment->PutIntoRecycleQueue(recyclablePage)) &&
        likely(m_segment->m_btrRecyclePartition->createdXid == GetMetaCreateXid())) {
        return DSTORE_SUCC;
    }
    return DSTORE_FAIL;
}

RetStatus BtreeStorageMgr::PutIntoFreeQueue(const PageId pageId)
{
    if (STORAGE_FUNC_SUCC(m_segment->PutIntoFreeQueue(pageId)) &&
        likely(m_segment->m_btrRecyclePartition->createdXid == GetMetaCreateXid())) {
        return DSTORE_SUCC;
    }
    return DSTORE_FAIL;
}

RetStatus BtreeStorageMgr::GetFromRecycleQueue(PageId &recyclablePageId, CommitSeqNo minCsn, uint64 *numSkippedPage)
{
    if (STORAGE_FUNC_SUCC(m_segment->GetFromRecycleQueue(recyclablePageId, minCsn, numSkippedPage)) &&
        likely(m_segment->m_btrRecyclePartition->createdXid == GetMetaCreateXid())) {
        return DSTORE_SUCC;
    }
    return DSTORE_FAIL;
}

PageId BtreeStorageMgr::GetFromFreeQueue(Xid checkingXid)
{
    PageId pageId = m_segment->GetFromFreeQueue();
    if (unlikely(!pageId.IsValid())) {
        return pageId;
    }
    checkingXid = likely(checkingXid == INVALID_XID) ? GetMetaCreateXid() : checkingXid;
    if (unlikely(m_segment->m_btrRecyclePartition->createdXid != checkingXid)) {
        return INVALID_PAGE_ID;
    }
    return pageId;
}

RetStatus BtreeStorageMgr::GetSlotFromFreeQueue(FreeQueueSlot &slot, const PageId emptyPage)
{
    if (STORAGE_FUNC_SUCC(m_segment->GetSlotFromFreeQueue(slot, emptyPage)) &&
        likely(m_segment->m_btrRecyclePartition->createdXid == GetMetaCreateXid())) {
        return DSTORE_SUCC;
    }
    return DSTORE_FAIL;
}

RetStatus BtreeStorageMgr::WriteSlotToFreeQueue(const FreeQueueSlot slot, const PageId emptyPage)
{
    CommitSeqNo nextCsn = INVALID_CSN;
    if (unlikely(g_storageInstance->GetCsnMgr() == nullptr) ||
        STORAGE_FUNC_FAIL(g_storageInstance->GetCsnMgr()->GetNextCsn(nextCsn, false))) {
        return DSTORE_FAIL;
    }
    ReusablePage reusablePage = {nextCsn, emptyPage};

    if (STORAGE_FUNC_SUCC(m_segment->WriteSlotToFreeQueue(slot, reusablePage)) &&
        likely(m_segment->m_btrRecyclePartition->createdXid == GetMetaCreateXid())) {
        return DSTORE_SUCC;
    }
    return DSTORE_FAIL;
}

#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
void BtreeStorageMgr::SetRecycleFailReason(BtrPageRecycleFailReason recycleFailReasonCode)
{
    m_segment->SetRecycleFailReason(recycleFailReasonCode);
}
#endif


void BtreeStorageMgr::RecordBtreeOperInLevel(BtreeOperType type, uint32 level)
{
    if (likely((static_cast<uint64>(g_traceSwitch) & BTREE_STATISTIC_INFO_MIN_TRACE_LEVEL) == 0)) {
        return;
    }
    if (unlikely(level >= BTREE_HIGHEST_LEVEL)) {
        ErrLog(DSTORE_WARNING, MODULE_INDEX,
            ErrMsg("Failed to collect index info. Current level %u exceeds the supported highest level %u.",
            level, BTREE_HIGHEST_LEVEL));
        return;
    }
    if (unlikely(!(type >= BtreeOperType::BTR_OPER_SPLIT_WHEN_BUILD && type < BtreeOperType::BTR_OPER_MAX))) {
        ErrLog(DSTORE_WARNING, MODULE_INDEX,
            ErrMsg("Failed to collect index info. Unknow operation type %d.", static_cast<int>(type)));
        return;
    }

    BufMgrInterface *bufMgr = IsGlobalTempIndex() ? thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr();
    BufferDesc *metaDesc = INVALID_BUFFER_DESC;
    BtrMeta *btreeMeta = GetBtrMeta(LW_EXCLUSIVE, &metaDesc);
    StorageReleasePanic(btreeMeta == nullptr, MODULE_INDEX, ErrMsg("Can not get btrMeta when RecordBtreeOperInLevel."));
    /* Initialize btree statistics info first if needed */
    if (unlikely(btreeMeta->initializedtMagicNum != BTREE_META_STAT_INIT_MAGIC_NUM)) {
        btreeMeta->InitStatisticsInfo();
    }

    btreeMeta->operCount[static_cast<int>(type)][level]++;

    (void)bufMgr->MarkDirty(metaDesc, false);
    bufMgr->UnlockAndRelease(metaDesc);
}

Btree::Btree(StorageRelation indexRel) : Btree(indexRel, nullptr)
{
    m_scanKeyValues.Init();
}

Btree::Btree(StorageRelation indexRel, IndexInfo *indexInfo)
    : m_btrMagicNum(BTREE_MAGIC),
      m_indexRel(indexRel),
      m_indexInfo(indexInfo),
      m_bufMgr((indexRel != nullptr && indexRel->btreeSmgr != nullptr && indexRel->btreeSmgr->IsGlobalTempIndex()) ?
               thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr())
{
    m_scanKeyValues.Init();
}

Btree::Btree(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKeyInfo)
    : m_btrMagicNum(BTREE_MAGIC),
      m_indexRel(indexRel),
      m_indexInfo(indexInfo),
      m_bufMgr((indexRel != nullptr && indexRel->btreeSmgr != nullptr && indexRel->btreeSmgr->IsGlobalTempIndex()) ?
               thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr())
{
    InitScanKey(scanKeyInfo);
}

Btree::~Btree()
{
    m_btrMagicNum = 0U;
    m_indexRel = nullptr;
    m_indexInfo = nullptr;
    m_bufMgr = nullptr;
}

BufferDesc *Btree::ReadAndCheckBtrPage(const PageId pageId, LWLockMode access, BufMgrInterface *bufMgr,
                                       PdbId pdbId, bool checkBtrPage)
{
    if (unlikely(pageId == INVALID_PAGE_ID)) {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("PageId{%d, %u} in pdbId:%u is invalid.",
            pageId.m_fileId, pageId.m_blockId, pdbId));
        return INVALID_BUFFER_DESC;
    }

    BufferDesc *buf = bufMgr->Read(pdbId, pageId, access);
    if (unlikely(buf == INVALID_BUFFER_DESC)) {
        char *clusterBufferInfo = bufMgr->GetClusterBufferInfo(pdbId, pageId.m_fileId, pageId.m_blockId);
        if (likely(clusterBufferInfo != nullptr)) {
            ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
            DstorePfreeExt(clusterBufferInfo);
        }
        PrintBackTrace();
        storage_set_error(INDEX_ERROR_FAIL_READ_PAGE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Read page {%d, %u} failed, pdbId:%u.",
            pageId.m_fileId, pageId.m_blockId, pdbId));
        return INVALID_BUFFER_DESC;
    }
    if (unlikely(buf->GetPageId() != pageId)) {
        char *clusterBufferInfo = bufMgr->GetClusterBufferInfo(pdbId, pageId.m_fileId, pageId.m_blockId);
        if (likely(clusterBufferInfo != nullptr)) {
            ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
            DstorePfreeExt(clusterBufferInfo);
        }
        PrintBackTrace();
        storage_set_error(INDEX_ERROR_PAGE_DIFF_IN_BUFTAG, pageId.m_fileId, pageId.m_blockId,
                          buf->GetPageId().m_fileId, buf->GetPageId().m_blockId, pdbId);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Page {%d, %u} was requested but {%d, %u} returned in bufTag, pdbId:%u.",
                pageId.m_fileId, pageId.m_blockId, buf->GetPageId().m_fileId, buf->GetPageId().m_blockId, pdbId));
        bufMgr->UnlockAndRelease(buf);
        return INVALID_BUFFER_DESC;
    }
    if (STORAGE_VAR_NULL(buf->GetPage())) {
        char *clusterBufferInfo = bufMgr->GetClusterBufferInfo(pdbId, pageId.m_fileId, pageId.m_blockId);
        ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
        DstorePfreeExt(clusterBufferInfo);
        PrintBackTrace();
        storage_set_error(INDEX_ERROR_PAGE_EMPTY, pdbId, pageId.m_fileId, pageId.m_blockId, pdbId);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Page in bufferDesc {%u, %hu, %u} is empty, pdbId:%u.",
               pdbId, pageId.m_fileId, pageId.m_blockId, pdbId));
        bufMgr->UnlockAndRelease(buf);
        return INVALID_BUFFER_DESC;
    }

    if (!checkBtrPage) {
        return buf;
    }

    BtrPage *page = static_cast<BtrPage *>(buf->GetPage());
    PageId readPageId = page->GetSelfPageId();
    BtrPageLinkAndStatus *linkAndStat = page->GetLinkAndStatus();
    if (unlikely(pageId != readPageId)) {
        char *clusterBufferInfo = bufMgr->GetClusterBufferInfo(pdbId, pageId.m_fileId, pageId.m_blockId);
        if (likely(clusterBufferInfo != nullptr)) {
            ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
            DstorePfreeExt(clusterBufferInfo);
        }
        PrintBackTrace();
        storage_set_error(INDEX_ERROR_PAGE_DIFF_IN_HEADER, pageId.m_fileId, pageId.m_blockId, readPageId.m_fileId,
                          readPageId.m_blockId, pdbId);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Page {%d, %u} was requested but {%d, %u} returned in header, pdbId:%u."
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                pageId.m_fileId, pageId.m_blockId, readPageId.m_fileId, readPageId.m_blockId, pdbId,
                BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));
        bufMgr->UnlockAndRelease(buf);
        return INVALID_BUFFER_DESC;
    }
    if (unlikely(!page->TestType(PageType::INDEX_PAGE_TYPE))) {
        char *clusterBufferInfo = bufMgr->GetClusterBufferInfo(pdbId, pageId.m_fileId, pageId.m_blockId);
        if (likely(clusterBufferInfo != nullptr)) {
            ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
            DstorePfreeExt(clusterBufferInfo);
        }
        PrintBackTrace();
        storage_set_error(INDEX_ERROR_PAGE_IS_NOT_BTRPAGE, pageId.m_fileId, pageId.m_blockId, pdbId);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Page {%d, %u} is not BtrPage, pdbId:%u."
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            pageId.m_fileId, pageId.m_blockId, pdbId,
            BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));
        bufMgr->UnlockAndRelease(buf);
        return INVALID_BUFFER_DESC;
    }
    if (unlikely(linkAndStat->TestType(BtrPageType::INVALID_BTR_PAGE))) {
        char *clusterBufferInfo = bufMgr->GetClusterBufferInfo(pdbId, pageId.m_fileId, pageId.m_blockId);
        if (likely(clusterBufferInfo != nullptr)) {
            ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
            DstorePfreeExt(clusterBufferInfo);
        }
        PrintBackTrace();
        storage_set_error(INDEX_ERROR_PAGE_IS_NOT_INITIALIZED_AS_BTRPAGE, pageId.m_fileId, pageId.m_blockId, pdbId);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Page {%d, %u} has not been initialized as BtrPage, pdbId:%u."
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                pageId.m_fileId, pageId.m_blockId, pdbId,
                BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));
        bufMgr->UnlockAndRelease(buf);
        return INVALID_BUFFER_DESC;
    }
    return buf;
}

void Btree::InitScanKey(ScanKey scanKeyInfo)
{
    m_scanKeyValues.Init();
    if (scanKeyInfo == nullptr) {
        /* BTREE_REFACTOR_LATER: set error. Btree class must have a ScanKey.
         * BtreeScan should rearannge to pass scankey earlier. */
        return;
    }

    m_scanKeyValues.keySize = m_indexInfo->indexKeyAttrsNum;
    m_scanKeyValues.cmpFastFlag = true; /* initial assumption */
    for (uint16 i = 0; i < m_indexInfo->indexKeyAttrsNum; i++) {
        uint32 indexOption = static_cast<uint32>(static_cast<uint16>(m_indexInfo->indexOption[i]));
        m_scanKeyValues.scankeys[i] = scanKeyInfo[i];
        m_scanKeyValues.scankeys[i].skAttno = static_cast<AttrNumber>(i + 1);
        m_scanKeyValues.scankeys[i].skFlags = indexOption << SCANKEY_INDEX_OPTION_SHIFT;
        m_scanKeyValues.scankeys[i].skArgument = static_cast<Datum>(0);

        /* Substitute with internal function */
        FillProcFmgrInfo(m_indexInfo->m_indexSupportProcInfo, scanKeyInfo[i].skFunc.fnOid,
            MAINTAIN_ORDER, m_scanKeyValues.scankeys[i].skFunc, m_scanKeyValues.scankeys[i].skAttno);
        if (scanKeyInfo[i].skFunc.fnOid == DSTORE_INVALID_OID) {
            ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("scanKeyInfo[i].skFunc.fnOid is INVALID_OID"));
        }
    }
}


void Btree::SetScanKeyWithVals(BtrScanKeyValues *scanKeyValues)
{
    StorageAssert(scanKeyValues != nullptr);
    StorageAssert(scanKeyValues->keySize == m_indexInfo->indexKeyAttrsNum);

    errno_t rc = memcpy_s(&m_scanKeyValues, sizeof(BtrScanKeyValues), scanKeyValues, sizeof(BtrScanKeyValues));
    if (likely(rc == 0) && scanKeyValues->cmpFastFlag) {
        m_scanKeyValues.values = static_cast<void *>(m_scanKeyValues.fastKeys);
        rc = memcpy_s(m_scanKeyValues.values, sizeof(Datum) * INDEX_MAX_KEY_NUM,
                      scanKeyValues->values, sizeof(Datum) * INDEX_MAX_KEY_NUM);
    }

    if (unlikely(rc != 0)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("failed to memocpy_s, ret=%d", rc));
    }
}

/*
 *  CreateTruncateInternalTuple()
 *  Truncate the suffix key attributes if we can distinguish the two tuples by the first n key attributes.
 */
IndexTuple *Btree::CreateTruncateInternalTuple(IndexTuple *left, IndexTuple *right, bool needTruncateHikey)
{
    if (unlikely(left->IsPivot())) {
        storage_set_error(INDEX_ERROR_TUPLE_DAMAGED);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("left is pivot when CreateTruncateInternalTuple"));
        return nullptr;
    }
    if (unlikely(right->IsPivot())) {
        storage_set_error(INDEX_ERROR_TUPLE_DAMAGED);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("right is pivot when CreateTruncateInternalTuple"));
        return nullptr;
    }

    TupleDesc tupleDesc = m_indexInfo->attributes;
    uint16 numAttKeeps = 1;
    for (uint16 i = 0; i < m_indexInfo->indexKeyAttrsNum; i++) {
        bool isLeftNull;
        bool isRightNull;
        Datum leftDatum = left->GetAttr(m_scanKeyValues.scankeys[i].skAttno, tupleDesc, &isLeftNull);
        Datum rightDatum = right->GetAttr(m_scanKeyValues.scankeys[i].skAttno, tupleDesc, &isRightNull);

        if (isLeftNull != isRightNull) {
            /* Found enough distinguishing key attributes if one of the attribute is NULL while the other is not */
            break;
        }
        if (!isLeftNull &&
            DatumGetInt32(FunctionCall2Coll(&m_scanKeyValues.scankeys[i].skFunc,
                                            m_scanKeyValues.scankeys[i].skCollation, leftDatum, rightDatum)) != 0) {
            /* Found enough distinguishing key attributes if one of the attribute is not equal to the other */
            break;
        }
        numAttKeeps++;
    }

    if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to compare tuple since compare function returns null."));
        return nullptr;
    }

    if (unlikely(!needTruncateHikey)) {
        numAttKeeps = DstoreMax(m_indexInfo->indexKeyAttrsNum, numAttKeeps);
    }
    IndexTuple *internalTuple = right->Truncate(tupleDesc, DstoreMin(m_indexInfo->indexKeyAttrsNum, numAttKeeps));
    if (unlikely(internalTuple == nullptr)) {
        return nullptr;
    }

    if (numAttKeeps <= m_indexInfo->indexKeyAttrsNum) {
        /* We can distinguish the to tuples within number of key attributs, then we're done. */
        internalTuple->SetKeyNum(numAttKeeps, false);
        return internalTuple;
    }

    Oid leftTableOid = DSTORE_INVALID_OID;
    if (m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX)) {
        bool isLeftNull;
        leftTableOid = left->GetAttr(m_indexInfo->tableOidAtt, tupleDesc, &isLeftNull);
    }

    /* Need to use ctid to distinguish these two tuples as all key attributes are equal */
    /* A truncated internal tuple should distinguish the right side of the split from the left side. Replace the ctid
     * with left's to make sure that key values (including ctid) of the truncated internal tuple is the greatest on the
     * left child and any searching process with a scankey that has a greater key value should be leading to the
     * its right child. */
    IndexTuple *internalCtidTuple =
        internalTuple->CopyWithTableOidAndCtid(m_indexInfo->indexKeyAttrsNum, leftTableOid, left->GetHeapCtid());
    DstorePfree(internalTuple);

    return internalCtidTuple;
}

BufferDesc *Btree::ReleaseOldGetNewBuf(BufferDesc *old, const PageId newPage, LWLockMode access, bool needCheck)
{
    if (unlikely(!newPage.IsValid())) {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        Xid btrCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
        if (old != INVALID_BUFFER_DESC) {
            BtrPage *oldPage = static_cast<BtrPage *>(old->GetPage());
            BtrPageLinkAndStatus *oldPageLinkStat = oldPage->GetLinkAndStatus();
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("NewPage is invalid when ReleaseOldGetNewBuf. "
                "pdb:%u, index(%s:%u), createXid(%d, %lu), segment(%hu, %u), currXid(%d, %lu), oldPage:"
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                GetPdbId(), m_indexInfo->indexRelName, m_indexRel->relOid,
                static_cast<int>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                static_cast<int>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
                BTR_PAGE_HEADER_VAL(oldPage), BTR_PAGE_LINK_AND_STATUS_VAL(oldPageLinkStat)));
            m_bufMgr->UnlockAndRelease(old);
        } else {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("NewPage is invalid when ReleaseOldGetNewBuf. "
                "pdb:%u, index(%s:%u), createXid(%d, %lu), segment(%hu, %u), currXid(%d, %lu), oldPage:is null",
                GetPdbId(), m_indexInfo->indexRelName, m_indexRel->relOid,
                static_cast<int>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                static_cast<int>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId));
        }
        return INVALID_BUFFER_DESC;
    }
    PdbId pdbId = INVALID_PDB_ID;
    if (old != INVALID_BUFFER_DESC) {
        /* NOTE: We ignore check BUF_CONTENT_DIRTY flag here.
         * Because when do insert or delete, it may lock the page in exclusive mode without modify the page
         * before it find the right position to insert or delete.
         */
        pdbId = old->GetPdbId();
        m_bufMgr->UnlockAndRelease(old, BufferPoolUnlockContentFlag::DontCheckCrc());
    } else {
        pdbId = this->GetPdbId();
    }

    return Btree::ReadAndCheckBtrPage(newPage, access, m_bufMgr, pdbId, needCheck);
}

/* Return DSTORE_SUCC when INVALID_BUFFER_DESC is expected. Return DSTORE_FAIL when something wrong. */
RetStatus Btree::GetRootFromMetaCache(BufferDesc **bufDesc)
{
    if (unlikely(bufDesc == nullptr)) {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("BufferDesc is nullptr when GetRootFromMetaCache."));
        return DSTORE_FAIL;
    }
    if (!GetBtreeSmgr()->HasMetaCache()) {
        *bufDesc = INVALID_BUFFER_DESC;
        return DSTORE_SUCC;
    }
    PageId rootPageIdInMeta = GetBtreeSmgr()->GetLowestSinglePageIdFromMetaCache();
    BufferDesc* rootPageDesc = GetBtreeSmgr()->GetLowestSinglePageDescFromCache();
    PageId rootPageId = (rootPageDesc != INVALID_BUFFER_DESC) ? rootPageDesc->GetPageId() : rootPageIdInMeta;
    if (!rootPageIdInMeta.IsValid() || rootPageId != rootPageIdInMeta) {
        /* Meta cache may be demaged. Clear it */
        GetBtreeSmgr()->ReleaseLowestSinglePageCache();
        GetBtreeSmgr()->ReleaseMetaCache();
        *bufDesc = INVALID_BUFFER_DESC;
        return DSTORE_SUCC;
    }
    if (rootPageDesc != INVALID_BUFFER_DESC) {
        PinBuffer(rootPageDesc);
        rootPageId = rootPageDesc->GetPageId();
        /* If rootPage in cache is a CR page at this moment, release root cache, read it from meta */
        if ((rootPageDesc->GetState(false) & Buffer::BUF_CR_PAGE) || (rootPageId != rootPageIdInMeta)) {
            UnpinBuffer(rootPageDesc);
            GetBtreeSmgr()->ReleaseLowestSinglePageCache();
            rootPageDesc = ReadAndCheckBtrPage(rootPageIdInMeta, LW_SHARED, m_bufMgr, GetPdbId());
            if (STORAGE_VAR_NULL(rootPageDesc)) {
                *bufDesc = INVALID_BUFFER_DESC;
                return DSTORE_FAIL;
            }
        } else if (STORAGE_FUNC_FAIL(m_bufMgr->LockContent(rootPageDesc, LW_SHARED))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Lock content {%d, %u} failed when execute function %s.",
                rootPageDesc->GetPageId().m_fileId, rootPageDesc->GetPageId().m_blockId, __FUNCTION__));
            m_bufMgr->Release(rootPageDesc);
            rootPageDesc = INVALID_BUFFER_DESC;
            *bufDesc = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
    } else {
        rootPageDesc = ReadAndCheckBtrPage(rootPageId, LW_SHARED, m_bufMgr, GetPdbId());
        if (STORAGE_VAR_NULL(rootPageDesc)) {
            *bufDesc = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
    }
    BtreePagePayload root;
    root.InitByBuffDesc(rootPageDesc);
    uint32 rootLevel = root.GetLinkAndStatus()->GetLevel();
    /* Meta cache is very likely to be stale, check carefully */
    if (!root.GetLinkAndStatus()->IsUnlinked() &&
        rootLevel < BTREE_HIGHEST_LEVEL && rootLevel == GetBtreeSmgr()->GetLowestSingleLevelFromMetaCache() &&
        root.GetLinkAndStatus()->IsLeftmost() && root.GetLinkAndStatus()->IsRightmost() &&
        root.GetPage()->GetBtrMetaCreateXid() == GetBtreeSmgr()->GetMetaCreateXid()) {
        *bufDesc = root.GetBuffDesc();
        return DSTORE_SUCC;
    }

    /* Well, it's stale. unlock the page. */
    m_bufMgr->UnlockAndRelease(root.GetBuffDesc());
    /* And clear cache */
    GetBtreeSmgr()->ReleaseLowestSinglePageCache();
    GetBtreeSmgr()->ReleaseMetaCache();
    *bufDesc = INVALID_BUFFER_DESC;
    return DSTORE_SUCC;
}

/*
 * CreateFirstNewRoot
 *
 * Create the first root for an empty btree. Caller must ensure that btrMetaBuf is locked with a write lock
 */
RetStatus Btree::CreateFirstNewRoot(BufferDesc *btrMetaBuf, BufferDesc **newRootBuf)
{
    StorageAssert(newRootBuf != nullptr);
    *newRootBuf = INVALID_BUFFER_DESC;

    /* Btree meta might be updated during we were changing lock. Check again. */
    StorageAssert(btrMetaBuf != INVALID_BUFFER_DESC);
    BtrPage *btrMetaPage = static_cast<BtrPage *>(btrMetaBuf->GetPage());
    BtrMeta *btrMeta = static_cast<BtrMeta *>(static_cast<void *>(btrMetaPage->GetData()));
    if (unlikely(!btrMetaPage->TestType(PageType::INDEX_PAGE_TYPE)) ||
        unlikely(!btrMetaPage->GetLinkAndStatus()->TestType(BtrPageType::META_PAGE)) ||
        unlikely(btrMeta->createXid == INVALID_XID)) {
        ErrLog(DSTORE_PANIC, MODULE_INDEX,
            ErrMsg("BtrMeta page{%d, %u} has been changed. " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            GetBtreeSmgr()->GetBtrMetaPageId().m_fileId, GetBtreeSmgr()->GetBtrMetaPageId().m_blockId,
            BTR_PAGE_HEADER_VAL(btrMetaPage), BTR_PAGE_LINK_AND_STATUS_VAL(btrMetaPage->GetLinkAndStatus())));
    }
    if (btrMeta->GetRootPageId() != INVALID_PAGE_ID) {
        /* Now we have a root. No need to create. */
        /* Caller should call GetRoot again if retstatus is DSTORE_SUCC while rootBuf is invalid. */
        if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->SetMetaCache(btrMeta))) {
            return DSTORE_FAIL;
        }
        return DSTORE_SUCC;
    }

    /* Create root now. */
    /* Get a new page for root */
    BtreePagePayload root;
    if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->GetNewPage(root, DEFAULT_TD_COUNT, btrMeta->createXid))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("CreateFirstNewRoot get new page faild for btree with BtrMeta {%d, %u} created xid(%d, %lu)",
            GetBtreeSmgr()->GetBtrMetaPageId().m_fileId, GetBtreeSmgr()->GetBtrMetaPageId().m_blockId,
            static_cast<int32>(btrMeta->createXid.m_zoneId),
            btrMeta->createXid.m_logicSlotId));
        return DSTORE_FAIL;
    }
    /* Initialize the new page as a root */
    root.GetLinkAndStatus()->InitPageMeta(btrMetaBuf->GetPageId(), 0, true);
    root.GetPage()->SetBtrMetaCreateXid(btrMeta->GetCreateXid());
    /* Update btree meta */
    btrMeta->SetBtreeMetaInfo(root.GetPageId(), root.GetPageId(), 0, 0);

    /* Must mark dirty before record wal */
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(root.GetBuffDesc()));
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(btrMetaBuf));

    if (NeedWal()) {
        GenerateNewLeafRootWal(root.GetBuffDesc(), btrMetaBuf);
    }

    /* Swap root write lock for read lock. We don't need to double check the root after lock changed since
     * btree meta is still locked that now one knows where the root is. */
    m_bufMgr->UnlockContent(root.GetBuffDesc());
    RetStatus retStatus = m_bufMgr->LockContent(root.GetBuffDesc(), LW_SHARED);
    StorageReleasePanic(retStatus == DSTORE_FAIL, MODULE_INDEX,
        ErrMsg("Lock btree root page {%d, %u} failed", root.GetPageId().m_fileId, root.GetPageId().m_blockId));

    /* Update meta cache and unlock meta buffer */
    if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->SetMetaCache(btrMeta))) {
        return DSTORE_FAIL;
    }
    GetBtreeSmgr()->UpdateLowestSinglePageCache(root.GetBuffDesc());
    *newRootBuf = root.GetBuffDesc();
    return DSTORE_SUCC;
}

/*
 * GetRoot
 *
 * Returns the lowest single page of btree if exists.
 * the returned page buffer descriptor has a LW_SHARED read lock.
 * Caller should change to write lock itself if needed.
 */
RetStatus Btree::GetRoot(BufferDesc **bufDesc, bool forUpdate)
{
    if (unlikely(bufDesc == nullptr)) {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("BufferDesc is nullptr when GetRoot."));
        return DSTORE_FAIL;
    }
    /* Step 1. Try get root from cached btree meta. Note that we always get root here with a read lock */
    if (STORAGE_FUNC_FAIL(GetRootFromMetaCache(bufDesc))) {
        return DSTORE_FAIL;
    }
    if (*bufDesc != INVALID_BUFFER_DESC) {
        return DSTORE_SUCC;
    }

    /* Step 2. Read btree meta who knows where the root is */
    BufferDesc *btrMetaBuf = INVALID_BUFFER_DESC;
    /* Read btree meta page and get root page id */
    BtrMeta *btrMeta = GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    if (unlikely(btrMeta == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Can not get btrMeta when GetRoot."));
        return DSTORE_FAIL;
    }
    BtrPage *metaPage = static_cast<BtrPage *>(btrMetaBuf->GetPage());
    uint64 btrMetaPlsn = static_cast<BtrPage *>(btrMetaBuf->GetPage())->GetPlsn();
    PageId rootPageId = btrMeta->GetRootPageId();
    uint32 rootPageLevel = btrMeta->GetRootLevel();
    PageId lowestSinglePageId = btrMeta->GetLowestSinglePage();
    uint32 lowestSinglePageLevel = btrMeta->GetLowestSinglePageLevel();
    if (unlikely(!metaPage->TestType(PageType::INDEX_PAGE_TYPE)) ||
        unlikely(!metaPage->GetLinkAndStatus()->TestType(BtrPageType::META_PAGE)) ||
        unlikely(GetBtreeSmgr()->GetSegment()->m_btrRecyclePartition != nullptr &&
                 btrMeta->GetCreateXid() != GetBtreeSmgr()->GetSegment()->m_btrRecyclePartition->createdXid)) {
        StorageReleasePanic(!forUpdate, MODULE_INDEX,
            ErrMsg("BtrMeta(%hu, %u) damaged, createXid(%d, %lu). " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            btrMetaBuf->GetPageId().m_fileId, btrMetaBuf->GetPageId().m_blockId,
            static_cast<int32>(btrMeta->GetCreateXid().m_zoneId), btrMeta->GetCreateXid().m_logicSlotId,
            BTR_PAGE_HEADER_VAL(metaPage), BTR_PAGE_LINK_AND_STATUS_VAL(metaPage->GetLinkAndStatus())));
        m_bufMgr->UnlockAndRelease(btrMetaBuf);
        return DSTORE_FAIL;
    }
    RetStatus ret = DSTORE_SUCC;
    if (rootPageId != INVALID_PAGE_ID && !GetBtreeSmgr()->HasMetaCache()) {
        /* Create a meta cache if we don't have one */
        ret = GetBtreeSmgr()->SetMetaCache(btrMeta);
    }
    m_bufMgr->UnlockAndRelease(btrMetaBuf);
    if (STORAGE_FUNC_FAIL(ret)) {
        return ret;
    }
    if (rootPageId == INVALID_PAGE_ID) {
        /* Step 3. Create root while reading root for update if we don't have a root page yet */
        /* If not reading for update, we are not expected to create root now */
        if (!forUpdate) {
            *bufDesc = INVALID_BUFFER_DESC;
            return DSTORE_SUCC;
        }

        /* Change for write lock to update btree meta */
        btrMetaBuf = m_bufMgr->Read(this->GetPdbId(), GetBtreeSmgr()->GetBtrMetaPageId(), LW_EXCLUSIVE);
        if (unlikely(btrMetaBuf == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Read BtrMeta page{%d, %u} failed, idx:%s %u, xid:%lu,"
                "pdb:%u, segMetaPageId{%d, %u}, tbsId:%d, snapcsn:%ld",
                GetBtreeSmgr()->GetBtrMetaPageId().m_fileId, GetBtreeSmgr()->GetBtrMetaPageId().m_blockId,
                m_indexInfo->indexRelName, m_indexRel->relOid, thrd->GetCurrentXid().m_placeHolder, this->GetPdbId(),
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                GetBtreeSmgr()->GetTablespaceId(), thrd->GetSnapShotCsn()));
            return DSTORE_FAIL;
        }

        /* Then create a root */
        ret = CreateFirstNewRoot(btrMetaBuf, bufDesc);
        m_bufMgr->UnlockAndRelease(btrMetaBuf);
        if (STORAGE_FUNC_FAIL(ret)) {
            return ret;
        }
        if (*bufDesc == INVALID_BUFFER_DESC) {
            return GetRoot(bufDesc, forUpdate);
        }
        return DSTORE_SUCC;
    }
    StorageAssert(rootPageId.IsValid());
    if (unlikely(lowestSinglePageId.IsInvalid())) {
        lowestSinglePageId = rootPageId;
        lowestSinglePageLevel = rootPageLevel;
    }
    /* Step 4. We've already had a root. Then get the lowest single page of Btree as a "fast root" */
    BtreePagePayload root;
    if (STORAGE_FUNC_FAIL(root.Init(this->GetPdbId(), lowestSinglePageId, LW_SHARED, m_bufMgr))) {
        return DSTORE_FAIL;
    }
    if (root.GetLinkAndStatus()->GetLevel() != lowestSinglePageLevel) {
        storage_set_error(INDEX_ERROR_ROOT_LEVEL_NOT_MATCHED, root.GetPageId().m_fileId, root.GetPageId().m_blockId,
            m_indexInfo->indexRelName, root.GetLinkAndStatus()->GetLevel(), lowestSinglePageLevel,
            GetBtreeSmgr()->GetBtrMetaPageId().m_fileId,
            GetBtreeSmgr()->GetBtrMetaPageId().m_blockId);
        ErrLog(DSTORE_PANIC, MODULE_INDEX,
            ErrMsg("Root{%hu, %u} level %u doesn't match with BtrMeta (plsn %lu)'s root level %u"
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, root.GetPageId().m_fileId, root.GetPageId().m_blockId,
            root.GetLinkAndStatus()->GetLevel(), btrMetaPlsn, lowestSinglePageLevel,
            BTR_PAGE_HEADER_VAL(root.GetPage()), BTR_PAGE_LINK_AND_STATUS_VAL(root.GetLinkAndStatus())));
        root.Drop(m_bufMgr);
        return DSTORE_FAIL;
    }

    /* Step 5. The fast root may move right after a splitting then recycling. Check and get the correct page */
    while (root.GetLinkAndStatus()->IsUnlinked()) {
        /* it's recycled or going to be recycled. step right */
        if (root.GetLinkAndStatus()->IsRightmost()) {
            storage_set_error(INDEX_ERROR_NO_ROOT_PAGE, m_indexInfo->indexRelName);
            ErrLog(DSTORE_PANIC, MODULE_INDEX,
                ErrMsg("No root in BtrMeta {%d, %u}. saved root{%d, %u} is invalid" BTR_PAGE_HEADER_FMT
                BTR_PAGE_LINK_AND_STATUS_FMT, GetBtreeSmgr()->GetBtrMetaPageId().m_fileId,
                GetBtreeSmgr()->GetBtrMetaPageId().m_blockId, root.GetPageId().m_fileId,
                root.GetPageId().m_blockId, BTR_PAGE_HEADER_VAL(root.GetPage()),
                BTR_PAGE_LINK_AND_STATUS_VAL(root.GetLinkAndStatus())));
            root.Drop(m_bufMgr);
            return DSTORE_FAIL;
        }
        BufferDesc *buf = ReleaseOldGetNewBuf(root.GetBuffDesc(), root.GetLinkAndStatus()->GetRight(), LW_SHARED);
        if (STORAGE_VAR_NULL(buf)) {
            return DSTORE_FAIL;
        }
        root.InitByBuffDesc(buf);
    }

    *bufDesc = root.GetBuffDesc();
    GetBtreeSmgr()->UpdateLowestSinglePageCache(root.GetBuffDesc());
    return DSTORE_SUCC;
}

RetStatus Btree::SearchBtree(BufferDesc **leafBuf, UNUSE_PARAM bool strictlyGreaterThanKey,
    UNUSE_PARAM bool forceUpdate, UNUSE_PARAM bool needWriteLock, UNUSE_PARAM bool needCheckCreatedXid)
{
    *leafBuf = INVALID_BUFFER_DESC;
    ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("Should not access Btree::SearchBtree"));
    return DSTORE_FAIL;
}

RetStatus Btree::StepRightIfNeeded(BufferDesc **buf, UNUSE_PARAM LWLockMode access,
                                   UNUSE_PARAM bool strictlyGreaterThanKey, UNUSE_PARAM bool needCheckCreatedXid)
{
    *buf = INVALID_BUFFER_DESC;
    ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("Should not access Btree::StepRightIfNeeded"));
    return DSTORE_FAIL;
}

bool Btree::UpdateScanKeyWithValues(IndexTuple *comparingKeyTuple)
{
    bool hasNull = false;
    if (comparingKeyTuple == nullptr) {
        /* Nothing to do */
        return hasNull;
    }

    uint16 numTupleAtts = comparingKeyTuple->GetKeyNum(m_indexInfo->indexAttrsNum);
    m_scanKeyValues.keySize = DstoreMin(m_indexInfo->indexKeyAttrsNum, numTupleAtts);
    m_scanKeyValues.heapCtid = comparingKeyTuple->GetHeapCtid();
    if (m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX)) {
        m_scanKeyValues.tableOid = comparingKeyTuple->GetTableOid(m_indexInfo);
    }

    TupleDesc tupleDesc = m_indexInfo->attributes;
    for (int i = 0; i < static_cast<int>(m_indexInfo->indexKeyAttrsNum); i++) {
        StorageAssert(m_scanKeyValues.scankeys[i].skAttno == i + 1);
        bool isNull;
        if (i < static_cast<int>(numTupleAtts)) {
            m_scanKeyValues.scankeys[i].skArgument = comparingKeyTuple->GetAttr(i + 1, tupleDesc, &isNull);
        } else {
            /* treat truncated attributes as NULL values */
            isNull = true;
        }

        uint32 indexOption = static_cast<uint32>(static_cast<uint16>(m_indexInfo->indexOption[i]));
        m_scanKeyValues.scankeys[i].skFlags = indexOption << SCANKEY_INDEX_OPTION_SHIFT;
        if (isNull) {
            hasNull = true;
            m_scanKeyValues.cmpFastFlag = false;
            m_scanKeyValues.scankeys[i].skFlags |= SCAN_KEY_ISNULL;
        } else {
            m_scanKeyValues.cmpFastFlag = m_scanKeyValues.cmpFastFlag &&
                                          tupleDesc->attrs[i]->atttypid == INT4OID &&
                                          tupleDesc->attrs[i]->attbyval;
        }
    }
    if (m_scanKeyValues.cmpFastFlag) {
        m_scanKeyValues.values = static_cast<void *>(m_scanKeyValues.fastKeys);
        errno_t rc = memcpy_s(m_scanKeyValues.values, sizeof(Datum) * INDEX_MAX_KEY_NUM,
                              comparingKeyTuple->GetValues(), comparingKeyTuple->GetValueSize());
        storage_securec_check(rc, "\0", "\0");
    }
    return hasNull;
}

OffsetNumber Btree::BinarySearchOnPage(BtrPage *btrPage, bool strictlyGreaterThanKey)
{
    /* Requesting nextkey semantics while using scantid seems nonsensical */
    StorageAssert(!strictlyGreaterThanKey || m_scanKeyValues.heapCtid == INVALID_ITEM_POINTER);
    BtrPageLinkAndStatus *linkAndStatus = btrPage->GetLinkAndStatus();
    bool isInternalPage = linkAndStatus->TestType(BtrPageType::INTERNAL_PAGE);
    uint16 low = linkAndStatus->GetFirstDataOffset();
    uint16 high = btrPage->GetMaxOffset();
    if (unlikely(high < low)) {
        /* No keys on the page for the page is really empty, or deleted to only high key left.
         * return the first available slot */
        return low;
    }

    /* Binary search to find the first key >= scan key,
     * or first key > scankey when requests index tuple strictly greater than scankey. */
    int goRightVal = strictlyGreaterThanKey ? 0 : 1;  /* select comparison value */
    high++;
    uint16 len = high - low;

    const uint16 half = 2;
    while (len > 0) {
        uint16 rem = len % half;
        len /= half;

        if (CompareKeyToTuple(btrPage, linkAndStatus, low + len, isInternalPage) >= goRightVal) {
            low += len + rem;
        }
    }

    /* On a leaf page, we always return the first key >= scankey (resp. > scan key), which could be the last slot + 1 */
    if (linkAndStatus->TestType(BtrPageType::LEAF_PAGE)) {
        return low;
    }
    /* On a internal leaf page, return the last key < scan key (resp. <= scan key). */
    return OffsetNumberPrev(low);
}

void Btree::GenerateNewLeafRootWal(BufferDesc *leafRootBuf, BufferDesc *btrMetaBuf)
{
    BtrPage *metaPage = static_cast<BtrPage *>(btrMetaBuf->GetPage());
    BtrMeta *btrMeta = static_cast<BtrMeta *>(static_cast<void *>(metaPage->GetData()));
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(thrd->GetCurrentXid());
    walContext->RememberPageNeedWal(leafRootBuf);
    BtrPage *rootPage = static_cast<BtrPage *>(leafRootBuf->GetPage());
    WalRecordBtreeNewLeafRoot walData;
    walData.SetHeader(rootPage, (rootPage->GetWalId() != walContext->GetWalId()), GetBtreeSmgr()->GetBtrMetaPageId(),
                      btrMeta->GetCreateXid(), leafRootBuf->GetFileVersion());
    walContext->PutNewWalRecord(&walData);

    /* Generate wal record for btree meta */
    walContext->RememberPageNeedWal(btrMetaBuf);
    WalRecordBtreeUpdateMetaPage metaWalData;
    metaWalData.SetHeader(metaPage, (metaPage->GetWalId() != walContext->GetWalId()), true,
                          btrMetaBuf->GetFileVersion());
    walContext->PutNewWalRecord(&metaWalData);
    UNUSED_VARIABLE(walContext->EndAtomicWal());
}

/*
 * CompareNIntKeyWithoutNulls
 *
 * fast compare scankey and tuple.
 * Make sure that all attributes are int(int32) type and non-nulls, which should be guaranteed by caller.
 */
int Btree::CompareNIntKeyWithoutNulls(IndexTuple *cmpTuple, int numCmpAtts)
{
    int32 result = 0;
    auto *keyPtr = static_cast<int32 *>(static_cast<void *>(m_scanKeyValues.values));
    auto *attPtr = static_cast<int32 *>(static_cast<void *>(cmpTuple->GetValues()));
    for (int i = 0; i < numCmpAtts; i++, attPtr++, keyPtr++) {
        if (*attPtr == *keyPtr) {
            continue;
        }
        result = (*keyPtr > *attPtr) ? 1 : -1;
        if ((m_scanKeyValues.scankeys[i].skFlags & SCANKEY_DESC) != 0U) {
            /* Descending order index */
            InvertCompareResult(&result);
        }
        return result;
    }
    return result;
}

int Btree::CompareNKeys(IndexTuple *cmpTuple, int numCmpAtts)
{
    int result = -1;
    for (int i = 0; i < numCmpAtts; i++) {
        bool isNull;
        ScanKeyData scankey = m_scanKeyValues.scankeys[i];
        Datum datum = cmpTuple->GetAttr(scankey.skAttno, m_indexInfo->attributes, &isNull);
        if ((!(scankey.skFlags & SCAN_KEY_ISNULL)) && !isNull) {
            switch (scankey.skFunc.fnOid) {
                case BTREEINT48CMP:
                    result = (int64)(int32)(datum) == (int64)scankey.skArgument ? 0 :
                         ((int64)(int32)datum > (int64)scankey.skArgument ? 1 : -1);
                    break;
                case BTREEINT8CMP:
                    result = (int64)datum == (int64)scankey.skArgument ? 0 :
                         ((int64)datum > (int64)scankey.skArgument ? 1 : -1);
                    break;
                case BTREEINT84CMP:
                    result = (int64)datum == (int64)(int32)scankey.skArgument ? 0 :
                         ((int64)datum > (int64)(int32)scankey.skArgument ? 1 : -1);
                    break;
                case BTREEINT4CMP:
                    result = (int32)datum == (int32)scankey.skArgument ? 0 :
                         ((int32)datum > (int32)scankey.skArgument ? 1 : -1);
                    break;
                default:
                    result = DatumGetInt32(
                        FunctionCall2Coll(&scankey.skFunc, scankey.skCollation, datum, scankey.skArgument));
                    break;
            }
            if ((scankey.skFlags & SCANKEY_DESC) == 0U) {
                /* Descending order index */
                InvertCompareResult(&result);
            }
        } else {
            if (scankey.skFlags & SCAN_KEY_ISNULL) {
                if (isNull) {       /* key is NULL */
                    result = 0;     /* NULL "=" NULL */
                } else if ((scankey.skFlags & SCANKEY_NULLS_FIRST) != 0U) {
                    result = -1;    /* NULL "<" NOT_NULL */
                } else {
                    result = 1;     /* NULL ">" NOT_NULL */
                }
            } else if (isNull) {    /* key is NOT_NULL and item is NULL */
                if ((scankey.skFlags & SCANKEY_NULLS_FIRST) != 0U) {
                    result = 1;     /* NOT_NULL ">" NULL */
                } else {
                    result = -1;    /* NOT_NULL "<" NULL */
                }
            }
        }

        /* if the keys are unequal, return the difference */
        if (result != 0) {
            return result;
        }
    }
    return result;
}

int32 Btree::CompareKeyToTuple(BtrPage *btrPage, BtrPageLinkAndStatus *linkAndStatus, OffsetNumber offsetNumber,
                               bool isInternalPage)
{
    if (isInternalPage && offsetNumber == linkAndStatus->GetFirstDataOffset()) {
        return 1;
    }

    IndexTuple *compareTuple = btrPage->GetIndexTuple(offsetNumber);
    uint16 numTupAtts =
        isInternalPage ? compareTuple->m_link.val.num : compareTuple->GetKeyNum(m_indexInfo->indexAttrsNum);
    uint16 numCmpAtts = DstoreMin(numTupAtts, m_scanKeyValues.keySize);

    int result;
    if (m_scanKeyValues.cmpFastFlag && !compareTuple->HasNull()) {
        result = CompareNIntKeyWithoutNulls(compareTuple, static_cast<int>(numCmpAtts));
    } else {
        result = CompareNKeys(compareTuple, static_cast<int>(numCmpAtts));
    }
    if (result != 0) {
        return result;
    }

    if (m_scanKeyValues.keySize > numTupAtts) {
        /* compareTuple is truncated. We treat truncated attributes as minus infinity.
         * So, scankey is considered greater */
        return 1;
    }

    if (unlikely(m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX))) {
        Oid compareTupleTableOid = compareTuple->GetTableOid(m_indexInfo);

        /* heapCtid == nullptr represent check unique, just only compare key */
        if (m_scanKeyValues.heapCtid == INVALID_ITEM_POINTER) {
            if (m_scanKeyValues.keySize == numTupAtts && compareTupleTableOid == DSTORE_INVALID_OID) {
                /* A scankey is considered greater than a truncated tuple if the scankey has equal values for
                 * attributes up to and including the least significant untruncated attribute in tuple. */
                return 1;
            }
            /* All key arguments are equal, only attributes key values are
             * considered. Return "equal" for they have the same key values. */
            return 0;
        }

        StorageAssert(m_scanKeyValues.tableOid != DSTORE_INVALID_OID);

        if (compareTupleTableOid == DSTORE_INVALID_OID) {
            /* Scankey has a table oid while tuple not. treat NULL ctid as minus infinity */
            return 1;
        }

        if (m_scanKeyValues.tableOid != compareTupleTableOid) {
            return m_scanKeyValues.tableOid > compareTupleTableOid ? 1 : -1;
        }
    }

    /* key arguments are equal, need to compare ctid */
    ItemPointerData compareTupleHeapCtid = compareTuple->GetHeapCtid();
    if (m_scanKeyValues.heapCtid == INVALID_ITEM_POINTER) {
        if (m_scanKeyValues.keySize == numTupAtts && compareTupleHeapCtid == INVALID_ITEM_POINTER) {
            /* A scankey is considered greater than a truncated tuple if the scankey has equal values for
             * attributes up to and including the least significant untruncated attribute in tuple. */
            return 1;
        }
        /* All key arguments are equal. Scankey doesn't have a heap ctid, meaning only attributes key values are
         * considered. Return "equal" for they have the same key values. */
        return 0;
    }

    StorageAssert(m_scanKeyValues.keySize == m_indexInfo->indexKeyAttrsNum);
    if (compareTupleHeapCtid == INVALID_ITEM_POINTER) {
        /* Scankey has a heap ctid while tuple not. treat NULL ctid as minus infinity */
        return 1;
    }

    /* finally compare ctids */
    /* We'll compare heapCtids only when all scan keys are equal, meaning both scan key and the comparing tuple have
     * all key attributes. */
    StorageAssert(numTupAtts >= m_indexInfo->indexKeyAttrsNum);
    return ItemPointerData::Compare(&m_scanKeyValues.heapCtid, &compareTupleHeapCtid);
}

IndexInfo *Btree::CopyIndexInfo(IndexInfo *oldInfo)
{
    uint16_t numKeyAttrs = oldInfo->indexKeyAttrsNum;
    Size infoSize = MAXALIGN(sizeof(IndexInfo));
    Size opcintypeSize = MAXALIGN(sizeof(Oid) * static_cast<Size>(numKeyAttrs));
    Size indoptionSize = MAXALIGN(sizeof(int16) * static_cast<Size>(numKeyAttrs));

    /* Allocate new IndexInfo here */
    char *newInfoPointer = (char *)DstorePalloc0(infoSize + opcintypeSize + indoptionSize);
    if (STORAGE_VAR_NULL(newInfoPointer)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Allocate memory for CopyIndexInfo failed."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return nullptr;
    }
    IndexInfo *newInfo = static_cast<IndexInfo *>(static_cast<void *>(newInfoPointer));

    /* Copy information from old IndexInfo */
    Size indexRelNameSize = sizeof(oldInfo->indexRelName);
    errno_t rc = memcpy_s(newInfo->indexRelName, indexRelNameSize, oldInfo->indexRelName, indexRelNameSize);
    storage_securec_check(rc, "\0", "\0");
    newInfo->relKind = oldInfo->relKind;
    newInfo->isUnique = oldInfo->isUnique;
    newInfo->indexAttrsNum = oldInfo->indexAttrsNum;
    newInfo->indexKeyAttrsNum = oldInfo->indexKeyAttrsNum;
    newInfo->tableOidAtt = oldInfo->tableOidAtt;
    newInfo->opcinType = static_cast<Oid *>(static_cast<void *>(newInfoPointer + infoSize));
    rc = memcpy_s(newInfo->opcinType, opcintypeSize, oldInfo->opcinType, opcintypeSize);
    storage_securec_check(rc, "\0", "\0");
    newInfo->indexOption = static_cast<int16 *>(static_cast<void *>(newInfoPointer + infoSize + opcintypeSize));
    rc = memcpy_s(newInfo->indexOption, indoptionSize, oldInfo->indexOption, indoptionSize);
    storage_securec_check(rc, "\0", "\0");
    newInfo->attributes = oldInfo->attributes->Copy();
    newInfo->exprInitCallback = oldInfo->exprInitCallback;
    newInfo->exprCallback = oldInfo->exprCallback;
    newInfo->exprDestroyCallback = oldInfo->exprDestroyCallback;

#ifndef UT
#ifndef DSTORE_TEST_TOOL
    if (oldInfo->m_indexSupportProcInfo == nullptr) {
        newInfo->m_indexSupportProcInfo = nullptr;
        return newInfo;
    }
    IndexSupportProcInfo *suppInfo = static_cast<IndexSupportProcInfo *>(DstorePalloc0(sizeof(IndexSupportProcInfo)));
    if (STORAGE_VAR_NULL(suppInfo)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Allocate memory for CopyIndexInfo failed."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        DstorePfreeExt(newInfo);
        return nullptr;
    }
    suppInfo->numSupportProc = oldInfo->m_indexSupportProcInfo->numSupportProc;
    suppInfo->numKeyAtts = oldInfo->m_indexSupportProcInfo->numKeyAtts;
    if (oldInfo->m_indexSupportProcInfo->opfamily != nullptr) {
        Size opfSize = sizeof(Oid) * newInfo->indexKeyAttrsNum;
        suppInfo->opfamily = static_cast<Oid *>(DstorePalloc(opfSize));
        if (STORAGE_VAR_NULL(suppInfo->opfamily)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Allocate memory for CopyIndexInfo failed."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            DstorePfreeExt(suppInfo);
            DstorePfreeExt(newInfo);
            return nullptr;
        }
        rc = memcpy_s(suppInfo->opfamily, opfSize,
                      oldInfo->m_indexSupportProcInfo->opfamily, opfSize);
        storage_securec_check(rc, "\0", "\0");
    }
    if (oldInfo->m_indexSupportProcInfo->supportProcs != nullptr && suppInfo->numSupportProc != 0) {
        Size supProcInfoSize = sizeof(Oid) * newInfo->indexKeyAttrsNum * suppInfo->numSupportProc;
        suppInfo->supportProcs = static_cast<Oid *>(DstorePalloc(supProcInfoSize));
        if (STORAGE_VAR_NULL(suppInfo->supportProcs)) {
            DstorePfreeExt(suppInfo->opfamily);
            DstorePfreeExt(suppInfo);
            DstorePfreeExt(newInfo);
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Allocate memory for CopyIndexInfo failed."));
            return nullptr;
        }
        rc = memcpy_s(suppInfo->supportProcs, supProcInfoSize,
                      oldInfo->m_indexSupportProcInfo->supportProcs, supProcInfoSize);
        storage_securec_check(rc, "\0", "\0");
    }
    newInfo->m_indexSupportProcInfo = suppInfo;
#endif
#endif
    return newInfo;
}

ScanKeyData *Btree::CopyScanKeys(ScanKeyData *scanKeys, uint16_t nKeys)
{
    Size totalScanKeyDataSize = sizeof(ScanKeyData) * static_cast<Size>(nKeys);

    /* Allocate new ScanKeyData here */
    ScanKeyData *newKey = static_cast<ScanKeyData *>(DstorePalloc(MAXALIGN(totalScanKeyDataSize)));
    if (STORAGE_VAR_NULL(newKey)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Allocate memory for CopyScanKeys failed."));
        return nullptr;
    }

    /* Copy ScanKey info here */
    errno_t rc = memcpy_s(newKey, totalScanKeyDataSize, scanKeys, totalScanKeyDataSize);
    storage_securec_check(rc, "\0", "\0");

    return newKey;
}

/* only when allocTd.isDirty is true, we need to record alloctd wal. If tdId is invalid, isDirty flag must be false */
void Btree::GenerateAllocTdWal(BufferDesc *bufferDesc, TDAllocContext &tdContext)
{
    if (unlikely(!NeedWal())) {
        tdContext.allocTd.isDirty = false;
        return;
    }
    StorageAssert(thrd->GetActiveTransaction() != nullptr);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    walWriterContext->RememberPageNeedWal(bufferDesc);

    BtrPage *page = static_cast<BtrPage *>(bufferDesc->GetPage());
    bool glsnChangedFlag = (page->GetWalId() != walWriterContext->GetWalId());
    /* Leaf pages have already called BeginAtomicWal before writing undo log */
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(tdContext);
    uint32 walDataSize = sizeof(WalRecordBtreeAllocTd) + allocTdSize;
    StorageReleasePanic(walDataSize > MAX_TD_WAL_DATA, MODULE_INDEX,
                        ErrMsg("walDataSize(%u) exceedes MAX_TD_WAL_DATA.", walDataSize));
    WalRecordBtreeAllocTd *walData = (WalRecordBtreeAllocTd *)(tdContext.allocTd.walData);
    walData->SetHeader(page, walDataSize, glsnChangedFlag, bufferDesc->GetFileVersion());
    walData->SetAllocTd(tdContext);

    walWriterContext->PutNewWalRecord(walData);
    (void)walWriterContext->EndAtomicWal();
}

void Btree::GenerateRollbackTdWal(BufferDesc *bufferDesc, TDAllocContext &tdContext)
{
    if (unlikely(!NeedWal())) {
        tdContext.allocTd.hasRollbackTd = false;
        return;
    }
    TdId tdId = INVALID_TD_SLOT;
    uint8 tdNum = tdContext.allocTd.tdNum;
    for (tdId = 0; tdId < tdNum; ++tdId) {
        if (!tdContext.allocTd.rollbackTds[tdId]) {
            continue;
        }
        StorageAssert(thrd->GetActiveTransaction() != nullptr);
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        walWriterContext->RememberPageNeedWal(bufferDesc);

        BtrPage *page = static_cast<BtrPage *>(bufferDesc->GetPage());
        TD *td = page->GetTd(tdId);
        bool glsnChangedFlag = (page->GetWalId() != walWriterContext->GetWalId());
        WalRecordRollbackForData *redoData = static_cast<WalRecordRollbackForData *>(walWriterContext->GetTempWalBuf());
        StorageReleasePanic(redoData == nullptr, MODULE_UNDO,
                            ErrMsg("Redo data is nullptr when generate wal for rollback."));
        uint32 size = static_cast<uint32>(sizeof(WalRecordRollbackForData));
        redoData->SetHeader({WAL_UNDO_BTREE, size, page->GetSelfPageId(), page->GetWalId(), page->GetPlsn(),
            page->GetGlsn(), glsnChangedFlag, bufferDesc->GetFileVersion()});
        redoData->RecordUndoRecHdr(UNDO_BTREE_INSERT, tdId, td->GetCsnStatus(), INVALID_ITEM_OFFSET_NUMBER,
            td->GetXid().m_placeHolder, td->GetUndoRecPtr().m_placeHolder, td->GetCsn());

        walWriterContext->PutNewWalRecord(redoData);
        (void)walWriterContext->EndAtomicWal();
    }
    tdContext.allocTd.hasRollbackTd = false;
    errno_t rc = memset_s(tdContext.allocTd.rollbackTds, MAX_TD_COUNT * sizeof(bool), 0, MAX_TD_COUNT * sizeof(bool));
    storage_securec_check(rc, "\0", "\0");
}

void Btree::DumpDamagedTuple(IndexTuple *tuple, BtrPage *page, OffsetNumber offset)
{
    StringInfoData str;
    if (!str.init()) {
        return;
    }
    if (page != nullptr) {
        str.append(BTR_PAGE_HEADER_FMT, BTR_PAGE_HEADER_VAL(page));
        str.append(BTR_PAGE_LINK_AND_STATUS_FMT, BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus()));
        if (offset != INVALID_OFFSET) {
            ItemId *ii = page->GetItemIdPtr(offset);
            str.append("ItemID(flag(%hu), offset(%hu), len(%hu)) ", ii->GetFlags(), ii->GetOffset(), ii->GetLen());
        }
    }
    if (tuple != nullptr) {
        str.append("IndexTuple(m_link(heapctid(%hu, %u, %hu), value(low(%hu, %u), num(%hu), ctidBreaker(%hu), "
            "posting(%hu), tableOid(%hu)))) ", tuple->m_link.heapCtid.GetFileId(), tuple->m_link.heapCtid.GetBlockNum(),
            tuple->m_link.heapCtid.GetOffset(), tuple->m_link.val.lowlevelIndexpageLink.m_fileId,
            tuple->m_link.val.lowlevelIndexpageLink.m_blockId, tuple->m_link.val.num, tuple->m_link.val.hasCtidBreaker,
            tuple->m_link.val.isposting, tuple->m_link.val.hasTableOid);
    }
    ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Damaged IndexTuple Info: %s", str.data));
}

RetStatus Btree::GetFirstLeafPage(PageId &firstLeafPageId)
{
    BufferDesc *buf;
    IndexTuple *itup;
    firstLeafPageId = INVALID_PAGE_ID;

    RetStatus status = GetRoot(&buf, false);
    if (STORAGE_FUNC_FAIL(status)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("GetFirstLeafPage: find index root failed \"%s\"", m_indexInfo->indexRelName));
        return DSTORE_FAIL;
    }

    if (buf == INVALID_BUFFER_DESC) {
        ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("GetFirstLeafPage: \"%s\" is empty.", m_indexInfo->indexRelName));
        return DSTORE_SUCC;
    }

    PageId pageId = buf->GetPageId();
    BtrPage *page = static_cast<BtrPage *>(buf->GetPage());
    BtrPageLinkAndStatus *pageMeta = page->GetLinkAndStatus();

    ErrLog(DSTORE_LOG, MODULE_INDEX,
           ErrMsg("GetFirstLeafPage: \"%s\" root page info: " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                  m_indexInfo->indexRelName, BTR_PAGE_HEADER_VAL(page),
                  BTR_PAGE_LINK_AND_STATUS_VAL(pageMeta)));

    while (page->GetLinkAndStatus()->GetLevel() > 0) {
        OffsetNumber firstDataOffset = pageMeta->GetFirstDataOffset();
        OffsetNumber maxOffset = page->GetMaxOffset();
        while (pageMeta->IsUnlinked() || firstDataOffset > maxOffset) {
            pageId = pageMeta->next;
            if (pageId == INVALID_PAGE_ID) {
                ErrLog(DSTORE_LOG, MODULE_INDEX,
                       ErrMsg("GetFirstLeafPage: fell off the end of index \"%s\"", m_indexInfo->indexRelName));
                m_bufMgr->UnlockAndRelease(buf);
                return DSTORE_SUCC;
            }
            buf = ReleaseOldGetNewBuf(buf, pageId, LW_SHARED);
            if (STORAGE_VAR_NULL(buf)) {
                return DSTORE_FAIL;
            }
            page = static_cast<BtrPage *>(buf->GetPage());
            pageMeta = page->GetLinkAndStatus();
        }

        itup = page->GetIndexTuple(firstDataOffset);
        pageId = itup->GetLowlevelIndexpageLink();

        if (pageMeta->GetLevel() == 1) {
            StorageAssert(pageId != INVALID_PAGE_ID);
            break;
        }

        buf = ReleaseOldGetNewBuf(buf, pageId, LW_SHARED);
        if (STORAGE_VAR_NULL(buf)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                   ErrMsg("GetFirstLeafPage: read page {%d, %u} fail \"%s\"", pageId.m_fileId, pageId.m_blockId,
                          m_indexInfo->indexRelName));
            return DSTORE_FAIL;
        }
        page = static_cast<BtrPage *>(buf->GetPage());
        pageMeta = page->GetLinkAndStatus();
    }

    firstLeafPageId = pageId;
    m_bufMgr->UnlockAndRelease(buf);
    return DSTORE_SUCC;
}
}
