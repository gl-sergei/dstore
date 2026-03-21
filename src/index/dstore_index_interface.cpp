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
 * dstore_index_interface.cpp
 *
 * IDENTIFICATION
 *        dstore/src/index/dstore_index_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "index/dstore_index_interface.h"
#include "common/log/dstore_log.h"
#include "index/dstore_btree_build.h"
#include "index/dstore_btree_delete.h"
#include "index/dstore_index_handler.h"
#include "index/dstore_btree_vacuum.h"
#include "index/dstore_btree_insert.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "index/concurrent/dstore_btree_incomplete_index_for_ccindex.h"
#include "index/concurrent/dstore_btree_delta_index_for_ccindex.h"

namespace IndexInterface {
using namespace DSTORE;

void ReportIndexBufferReadStat(DSTORE::StorageRelation relation)
{
    Oid relOid = relation->relOid;
    PdbId pdbId = relation->m_pdbId;
    if (likely(!g_storageInstance->GetGuc()->enableTrackActivities)) {
        return;
    }
    if (unlikely(relOid == DSTORE_INVALID_OID || IsTemplate(pdbId) || !g_storageInstance->IsInit() ||
                 g_storageInstance->IsBootstrapping() || thrd->m_bufferReadStat.isReporting)) {
        return;
    }
    /* Error code may be overwritten by the following DFX operations. Need to back up the error code. */
    Error oldErr;
    if (unlikely(StorageGetErrorCode() != STORAGE_OK)) {
        oldErr.CopyError(thrd->error);
    }

    thrd->m_bufferReadStat.isReporting = true;
    g_storageInstance->GetStat()->ReportCountBuffer(relation, thrd->m_bufferReadStat.bufferReadCount,
                                                    thrd->m_bufferReadStat.bufferReadHit);
    thrd->m_bufferReadStat.resetBufferReadStat();
    if (unlikely(oldErr.GetErrorCode() != STORAGE_OK)) {
        thrd->error->CopyError(&oldErr);
    }
}

static RetStatus IndexInfoCheck(IndexInfo *indexInfo, const char *function)
{
    if (STORAGE_VAR_NULL(indexInfo)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexInfo\".", function));
        return DSTORE_FAIL;
    }

    if (STORAGE_VAR_NULL(indexInfo->opcinType)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr in IndexInfo \"opcinType\".", function));
        return DSTORE_FAIL;
    }

    if (STORAGE_VAR_NULL(indexInfo->indexOption)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr in IndexInfo \"indexOption\".", function));
        return DSTORE_FAIL;
    }

    if (STORAGE_VAR_NULL(indexInfo->attributes)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr in IndexInfo \"attributes\".", function));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

static RetStatus IndexRelCheck(StorageRelation indexRel, const char *function)
{
    if (STORAGE_VAR_NULL(indexRel)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexRel\".", function));
        return DSTORE_FAIL;
    }

    if (STORAGE_VAR_NULL(indexRel->btreeSmgr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr in IndexRel \"btreeSmgr\".", function));
        return DSTORE_FAIL;
    }

    if (STORAGE_VAR_NULL(indexRel->btreeSmgr->m_segment)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr in IndexRel \"m_segment\".", function));
        return DSTORE_FAIL;
    }

    if (unlikely(indexRel->m_pdbId == INVALID_PDB_ID)) {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Invalid PDB ID in IndexRel.", function));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

static inline RetStatus ParamScanKeyCheck(const ScanKey paramScanKey, int nkeys)
{
    if (STORAGE_VAR_NULL(paramScanKey)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unexpected nullptr \"Scankey\"."));
        return DSTORE_FAIL;
    }
    ScanKey skey = paramScanKey;
    for (int i = 0; i < nkeys; i++, skey++) {
        if (unlikely(skey->skStrategy > MAX_STRATEGY_NUM)) {
            storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
            return DSTORE_FAIL;
        }
        /* we check that input keys are correctly ordered */
        int16_t preAttno = 0;
        if (unlikely(skey->skAttno < preAttno)) {
            storage_set_error(INDEX_ERROR_INDEX_KEYS_MUST_BE_ORDERED_BY_ATTRIBUTE);
            return DSTORE_FAIL;
        }
        preAttno = skey->skAttno;
    }
    return DSTORE_SUCC;
}

static inline RetStatus ParamInsertAndDeleteCommonDataCheck(const BtreeInsertAndDeleteCommonData &commonData,
                                                            const char *function)
{
    if (STORAGE_FUNC_FAIL(IndexInfoCheck(commonData.indexInfo, function)) ||
        STORAGE_FUNC_FAIL(IndexRelCheck(commonData.indexRel, function)) ||
        STORAGE_FUNC_FAIL(ParamScanKeyCheck(commonData.skey, commonData.indexInfo->indexKeyAttrsNum))) {
        return DSTORE_FAIL;
    }
    if (STORAGE_VAR_NULL(commonData.heapCtid)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"heapCtid\".", function));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus Build(StorageRelation indexRel, ScanKey skey, IndexBuildInfo *indexBuildInfo)
{
    RetStatus ret = DSTORE_FAIL;
    if (STORAGE_VAR_NULL(indexBuildInfo)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexBuildInfo\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(IndexInfoCheck(&indexBuildInfo->baseInfo, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(IndexRelCheck(indexRel, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(ParamScanKeyCheck(skey, indexBuildInfo->baseInfo.indexKeyAttrsNum))) {
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    IndexInfo *baseInfo = &(indexBuildInfo->baseInfo);
    ErrLog(DSTORE_LOG, MODULE_INDEX,
        ErrMsg("Index(%s:%u) on pdb(%u) table(%u) Build start: keyAttrsNum[%u], isUnique[%u], relKind[%d], "
               "tableNum[%u], segment(%hu, %u), snapshot(%lu), xid(%u, %lu)",
        baseInfo->indexRelName, indexRel->relOid, indexRel->m_pdbId, indexBuildInfo->heapRelationOid,
        baseInfo->indexAttrsNum, baseInfo->isUnique, baseInfo->relKind, indexBuildInfo->heapRelNum,
        indexRel->btreeSmgr->GetSegMetaPageId().m_fileId, indexRel->btreeSmgr->GetSegMetaPageId().m_blockId,
        thrd->GetSnapShotCsn(),
        static_cast<uint32>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId));
    BtreeBuild btreeBuilder(indexRel, indexBuildInfo, skey);
    ret = btreeBuilder.BuildIndex();
    ErrLog(DSTORE_LOG, MODULE_INDEX,
        ErrMsg("Index(%s:%u) on pdb(%u) table (%u) Build end: segment(%hu, %u), snapshot(%lu), xid(%u, %lu), ret(%s), "
               "heapTuples(%f), indexTuples(%f)",
        baseInfo->indexRelName, indexRel->relOid, indexRel->m_pdbId, indexBuildInfo->heapRelationOid,
        indexRel->btreeSmgr->GetSegMetaPageId().m_fileId, indexRel->btreeSmgr->GetSegMetaPageId().m_blockId,
        thrd->GetSnapShotCsn(),
        static_cast<uint32>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
        ret == DSTORE_SUCC ? "succ" : "fail", indexBuildInfo->heapTuples, indexBuildInfo->indexTuples));
    IndexInterface::ReportIndexBufferReadStat(indexRel);
    return ret;
}

RetStatus BuildParallel(StorageRelation indexRel, ScanKey skey, IndexBuildInfo *indexBuildInfo, int parallelWorkers)
{
    RetStatus ret = DSTORE_FAIL;
    if (STORAGE_VAR_NULL(indexBuildInfo)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexBuildInfo\".", __FUNCTION__));
        PrintBackTrace();
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(IndexInfoCheck(&indexBuildInfo->baseInfo, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(IndexRelCheck(indexRel, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(ParamScanKeyCheck(skey, indexBuildInfo->baseInfo.indexKeyAttrsNum))) {
        PrintBackTrace();
        return DSTORE_FAIL;
    }
    if (unlikely(parallelWorkers < 1)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("wrong parameters in index parallel build: parallelWorkers = %d.", parallelWorkers));
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    IndexInfo *baseInfo = &(indexBuildInfo->baseInfo);
    ErrLog(DSTORE_LOG, MODULE_INDEX,
        ErrMsg("Index(%s:%u) on pdb(%u) table(%u) Build start: keyAttrsNum[%u], isUnique[%u], relKind[%d], "
               "tableNum[%u], segment(%hu, %u), snapshot(%lu), xid(%u, %lu), parallelWorkers[%d]",
        baseInfo->indexRelName, indexRel->relOid, indexRel->m_pdbId, indexBuildInfo->heapRelationOid,
        baseInfo->indexAttrsNum, baseInfo->isUnique, baseInfo->relKind, indexBuildInfo->heapRelNum,
        indexRel->btreeSmgr->GetSegMetaPageId().m_fileId, indexRel->btreeSmgr->GetSegMetaPageId().m_blockId,
        thrd->GetSnapShotCsn(),
        static_cast<uint32>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId, parallelWorkers));
    BtreeBuild btreeBuilder(indexRel, indexBuildInfo, skey);
    ret = btreeBuilder.BuildIndexParallel(parallelWorkers);
    ErrLog(DSTORE_LOG, MODULE_INDEX,
        ErrMsg("Index(%s:%u) on pdb(%u) table(%u) Build end: segment(%hu, %u), snapshot(%lu), xid(%u, %lu), ret(%s), "
               "heapTuples(%f), indexTuples(%f)",
        baseInfo->indexRelName, indexRel->relOid, indexRel->m_pdbId, indexBuildInfo->heapRelationOid,
        indexRel->btreeSmgr->GetSegMetaPageId().m_fileId, indexRel->btreeSmgr->GetSegMetaPageId().m_blockId,
        thrd->GetSnapShotCsn(),
        static_cast<uint32>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
        ret == DSTORE_SUCC ? "succ" : "fail", indexBuildInfo->heapTuples, indexBuildInfo->indexTuples));
    IndexInterface::ReportIndexBufferReadStat(indexRel);
    return ret;
}

RetStatus Insert(BtreeInsertAndDeleteCommonData commonData, bool isCatalog, bool checkImmediate, bool *satisfiesUnique)
{
    if (STORAGE_FUNC_FAIL(ParamInsertAndDeleteCommonDataCheck(commonData, __FUNCTION__))) {
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    /* Step 1. Prepare a BtreeInsert context */
    bool needDefferCheck = isCatalog ? false : (!checkImmediate);
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    BtreeInsert *btreeInsert;
    if (commonData.indexInfo->btrIdxStatus == BtrCcidxStatus::WRITE_ONLY_INDEX) {
        btreeInsert = DstoreNew(g_dstoreCurrentMemoryContext)
                      IncompleteBtreeInsertForCcindex(commonData.indexRel, commonData.indexInfo, commonData.skey);
    } else {
        StorageAssert(commonData.indexInfo->btrIdxStatus == BtrCcidxStatus::NOT_CCINDEX);
        btreeInsert = DstoreNew(g_dstoreCurrentMemoryContext)
                      BtreeInsert(commonData.indexRel, commonData.indexInfo, commonData.skey, needDefferCheck);
    }
    if (STORAGE_VAR_NULL(btreeInsert)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to create BtreeInsert object."));
        return DSTORE_FAIL;
    }

    /* Step 2. Do the insert here */
    RetStatus ret = btreeInsert->BtreeInsert::InsertTuple(commonData.values, commonData.isnull, commonData.heapCtid,
                                                          satisfiesUnique, commonData.cid);
    if (STORAGE_FUNC_FAIL(ret)) {
        PageId btreeMetaPageId = commonData.indexRel->btreeSmgr->GetBtrMetaPageId();
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("Failed to insert index tuple. btreeMetaPageId: {%d, %u}", btreeMetaPageId.m_fileId,
                      btreeMetaPageId.m_blockId));
    }
    /* Step 3. Return the result here */
    delete btreeInsert;
    btreeInsert = nullptr;
    IndexInterface::ReportIndexBufferReadStat(commonData.indexRel);
    return ret;
}

RetStatus Delete(BtreeInsertAndDeleteCommonData commonData)
{
    if (STORAGE_FUNC_FAIL(ParamInsertAndDeleteCommonDataCheck(commonData, __FUNCTION__))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to check delete params for index \"%s\".",
            (commonData.indexInfo)->indexRelName));
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    /* Step 1. Prepare a BtreeDelete */
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    BtreeDelete *btreeDelete = nullptr;
    if (commonData.indexInfo->btrIdxStatus == BtrCcidxStatus::WRITE_ONLY_INDEX) {
        btreeDelete = DstoreNew(g_dstoreCurrentMemoryContext)
                      IncompleteBtreeDeleteForCcindex(commonData.indexRel, commonData.indexInfo, commonData.skey);
    } else {
        StorageAssert(commonData.indexInfo->btrIdxStatus == BtrCcidxStatus::NOT_CCINDEX);
        btreeDelete = DstoreNew(g_dstoreCurrentMemoryContext)
                      BtreeDelete(commonData.indexRel, commonData.indexInfo, commonData.skey);
    }
    if (STORAGE_VAR_NULL(btreeDelete)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to create btreeDelete object."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    /* Step 2. Do the delete here */
    RetStatus ret = btreeDelete->DeleteTuple(commonData.values, commonData.isnull, commonData.heapCtid, commonData.cid);
    if (STORAGE_FUNC_FAIL(ret)) {
        PageId btreeMetaPageId = commonData.indexRel->btreeSmgr->GetBtrMetaPageId();
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("Failed to delete index tuple on heap {%d, %u} %d. "
                      "btreeMetaPageId: {%d, %u}",
                      commonData.heapCtid->GetFileId(), commonData.heapCtid->GetBlockNum(),
                      commonData.heapCtid->GetOffset(), btreeMetaPageId.m_fileId, btreeMetaPageId.m_blockId));
        IndexInterface::ReportIndexBufferReadStat(commonData.indexRel);
        delete btreeDelete;
        btreeDelete = nullptr;
        return DSTORE_FAIL;
    }

    /* Step 3. Return success here */
    delete btreeDelete;
    btreeDelete = nullptr;
    IndexInterface::ReportIndexBufferReadStat(commonData.indexRel);
    return DSTORE_SUCC;
}

RetStatus BtreeLazyVacuum(StorageRelation indexRel, IndexInfo *indexInfo)
{
    RetStatus ret = DSTORE_FAIL;
    if (STORAGE_FUNC_FAIL(IndexInfoCheck(indexInfo, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(IndexRelCheck(indexRel, __FUNCTION__))) {
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    BtreeVacuum btreeVacuum(indexRel, indexInfo);
    ret =  btreeVacuum.BtreeLazyVacuum();
    IndexInterface::ReportIndexBufferReadStat(indexRel);
    return ret;
}

RetStatus BtreeGPIVacuum(StorageRelation indexRel, GPIPartOidCheckInfo *gpiCheckInfo)
{
    if (STORAGE_FUNC_FAIL(IndexRelCheck(indexRel, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(IndexInfoCheck(indexRel->index, __FUNCTION__))) {
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    bool paraValid = false;
    if (STORAGE_VAR_NULL(gpiCheckInfo)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unexpected nullptr \"gpiCheckInfo\"."));
    } else if (STORAGE_VAR_NULL(gpiCheckInfo->hook)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unexpected nullptr in gpiCheckInfo \"hook\"."));
    } else if (STORAGE_VAR_NULL(gpiCheckInfo->dropPartTree)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unexpected nullptr in gpiCheckInfo \"dropPartTree\"."));
    } else {
        paraValid = true;
    }

    if (unlikely(!paraValid)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    BtreeStorageMgr *btrSmgr = indexRel->btreeSmgr;
    if (!btrSmgr->HasMetaCache()) {
        if (unlikely(btrSmgr->GetBtrMetaPageId() == INVALID_PAGE_ID)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("BtreeGPIVacuum: invalid btrMetaPageId."));
            return DSTORE_FAIL;
        }
        BufferDesc *btrMetaBuf = INVALID_BUFFER_DESC;
        BufMgrInterface *bufMgr =
            btrSmgr->IsGlobalTempIndex() ? thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr();
        BtrMeta *btrMeta = btrSmgr->GetBtrMeta(LW_SHARED, &btrMetaBuf);
        if (STORAGE_FUNC_FAIL(btrSmgr->SetMetaCache(btrMeta))) {
            bufMgr->UnlockAndRelease(btrMetaBuf);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("BtreeGPIVacuum: SetMetaCache failed."));
            return DSTORE_FAIL;
        }
        bufMgr->UnlockAndRelease(btrMetaBuf);
    }

    BtreeVacuum btreeVacuum(indexRel, indexRel->index);
    return btreeVacuum.BtreeVacuumGPI(gpiCheckInfo);
}

IndexScanHandler *ScanBegin(DSTORE::StorageRelation indexRel, IndexInfo *indexInfo, int numKeys,
                            int numOrderbys, bool showAnyTuples)
{
    IndexScanHandler *scanHandler = nullptr;
    __attribute__((__unused__)) AutoMemCxtSwitch autoMemCxtSwitch(thrd->GetQueryMemoryContext());

    /* parameters check */
    if (STORAGE_FUNC_FAIL(IndexRelCheck(indexRel, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(IndexInfoCheck(indexInfo, __FUNCTION__))) {
        PrintBackTrace();
        return nullptr;
    }
    if (unlikely(numKeys > INDEX_MAX_KEY_NUM)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("wrong parameters in indexscanhandler creation."));
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        PrintBackTrace();
        return nullptr;
    }

    /* Step 1. Create a IndexScanHandler and store parameters */
    scanHandler = IndexScanHandler::Create(indexRel, indexInfo, numKeys, numOrderbys, showAnyTuples);
    if (scanHandler == nullptr || scanHandler->m_scan == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to create IndexScanHandler."));
        delete scanHandler;
        return nullptr;
    }
    scanHandler->SetStorageRelOid(indexRel->relOid);
    /* Step 2. Initialization of btreeScan */
    if (STORAGE_FUNC_FAIL(scanHandler->BeginScan())) {
        delete scanHandler;
        return nullptr;
    }

    return scanHandler;
}

RetStatus ScanNext(IndexScanHandler *scanHandler, ScanDirection direction, bool *found, bool *recheck)
{
    /* parameters check */
    if (unlikely(scanHandler == nullptr || found == nullptr || recheck == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("wrong parameters in index scan."));
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        PrintBackTrace();
        return DSTORE::DSTORE_FAIL;
    }

    IndexScanDesc scanDesc = scanHandler->GetScanDesc();

    /* Tries to get next tid */
    *found = false;
    RetStatus ret = scanHandler->GetNextTuple(direction, found);
    if (STORAGE_FUNC_SUCC(ret)) {
        *recheck = scanDesc->needRecheck;
    }

    /* Step 2. Return fetched tid */
    return ret;
}

ItemPointer GetResultHeapCtid(IndexScanHandler *scanHandler)
{
    /* parameters check */
    if (STORAGE_VAR_NULL(scanHandler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanHandler\".", __FUNCTION__));
        return nullptr;
    }

    return scanHandler->GetResultHeapCtid();
}

IndexTuple *IndexScanGetIndexTuple(IndexScanHandler *scanHandler)
{
    /* parameters check */
    if (STORAGE_VAR_NULL(scanHandler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanHandler\".", __FUNCTION__));
        return nullptr;
    }

    IndexScanDesc scanDesc = scanHandler->GetScanDesc();
    return scanDesc->itup;
}

TupleDesc IndexScanGetTupleDesc(IndexScanHandler *scanHandler)
{
    /* parameters check */
    if (STORAGE_VAR_NULL(scanHandler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanHandler\".", __FUNCTION__));
        return nullptr;
    }

    IndexScanDesc scanDesc = scanHandler->GetScanDesc();
    if (STORAGE_VAR_NULL(scanDesc)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanDesc\".", __FUNCTION__));
        return nullptr;
    }

    return scanDesc->itupDesc;
}

RetStatus ScanSetWantItup(DSTORE::IndexScanHandler *scanHandler, bool wantItup)
{
    if (STORAGE_VAR_NULL(scanHandler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanHandler\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    IndexScanDesc scanDesc = scanHandler->GetScanDesc();
    if (STORAGE_VAR_NULL(scanDesc)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanDesc\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    scanDesc->wantItup = wantItup;
    if (wantItup && scanDesc->wantXid) {
        return scanHandler->SetShowAnyTuples(true);
    }
    return DSTORE::DSTORE_SUCC;
}

void IndexScanSetSnapshot(DSTORE::IndexScanHandler *scanHandler,  DSTORE::Snapshot snapshot)
{
    /* caller should make sure the scanHandler is not nullptr */
    StorageReleasePanic(scanHandler == nullptr, MODULE_INDEX, ErrMsg("scanHandler is nullptr."));
    scanHandler->InitSnapshot(snapshot);
}

void IndexScanGetInsertAndDeleteXids(IndexScanHandler *scanHandler, Datum &insertXid, Datum &deleteXid)
{
    /* caller should make sure the scanHandler is not nullptr */
    IndexScanDesc scanDesc = scanHandler->GetScanDesc();
    if (STORAGE_VAR_NULL(scanDesc)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanDesc\".", __FUNCTION__));
        return;
    }
    insertXid = (scanDesc->insertXid).m_placeHolder;
    deleteXid = (scanDesc->deleteXid).m_placeHolder;
}

IndexTuple *OnlyScanNext(IndexScanHandler *scanHandler, ScanDirection direction, TupleDesc *tupdesc, bool *recheck)
{
    /* parameters check */
    if (unlikely(scanHandler == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("wrong parameters in index only scan."));
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        return nullptr;
    }

    IndexTuple *itup = nullptr;
    IndexScanDesc scanDesc = scanHandler->GetScanDesc();
    /* Step 1. Tries to get next index tuple */
    bool found;
    if (STORAGE_FUNC_SUCC(scanHandler->GetNextTuple(direction, &found)) && found) {
        itup = scanDesc->itup;
        *tupdesc = scanDesc->itupDesc;
        *recheck = scanDesc->needRecheck;
    }

    /* Step 2. Return fetched index tuple */
    return itup;
}

RetStatus ScanEnd(IndexScanHandler *scanHandler)
{
    /* parameters check */
    if (STORAGE_VAR_NULL(scanHandler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanHandler\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    scanHandler->EndScan();
    delete scanHandler;
    scanHandler = nullptr;
    return DSTORE_SUCC;
}

RetStatus ScanRescan(IndexScanHandler *scanHandler, ScanKey skey)
{
    /* parameters check */
    if (STORAGE_VAR_NULL(scanHandler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanHandler\".", __FUNCTION__));
        return DSTORE_FAIL;
    }

    return scanHandler->ReScan(skey);
}

ScanKey GetScanKeyInfo(IndexScanHandler *scanHandler, int &numberOfKeys)
{
    if (STORAGE_VAR_NULL(scanHandler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanHandler\".", __FUNCTION__));
        return nullptr;
    }

    return scanHandler->GetScanKeyInfo(numberOfKeys);
}

RetStatus ResetArrCondInfo(IndexScanHandler *scanHandler, int numKeys, Datum **values, bool **isnulls, int *numElem)
{
    if (STORAGE_VAR_NULL(scanHandler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanHandler\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    if (unlikely(values == nullptr || isnulls == nullptr || numElem == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("wrong parameters in index rescan."));
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        PrintBackTrace();
        return DSTORE_FAIL;
    }
    return scanHandler->ResetArrCondInfo(numKeys, values, isnulls, numElem);
}

Oid GPIGetScanHeapPartOid(IndexScanHandler *scanHandler)
{
    if (STORAGE_VAR_NULL(scanHandler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanHandler\".", __FUNCTION__));
        return DSTORE_INVALID_OID;
    }
    return scanHandler->GetPartHeapOid();
}

RetStatus UpdateDeltaDmlForCcindex(DSTORE::BtreeInsertAndDeleteCommonData commonData,
                                   DSTORE::ItemPointer duplicateDeltaRec, bool isLpi, int retryTimes)
{
    RetStatus ret = DSTORE_FAIL;
    if (STORAGE_FUNC_FAIL(ParamInsertAndDeleteCommonDataCheck(commonData, __FUNCTION__))) {
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    ret = BtreeDeltaDmlForCcindex::UpdateDeltaDmlRec({commonData.indexRel, commonData.indexInfo, commonData.skey,
        isLpi, retryTimes}, commonData.values, commonData.isnull, commonData.heapCtid, duplicateDeltaRec);
    IndexInterface::ReportIndexBufferReadStat(commonData.indexRel);
    return ret;
}

RetStatus CheckExistence(DSTORE::BtreeInsertAndDeleteCommonData commonData, bool isLpi, DSTORE::ItemPointer deltaRec)
{
    RetStatus ret = DSTORE_FAIL;
    if (STORAGE_FUNC_FAIL(IndexInfoCheck(commonData.indexInfo, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(IndexRelCheck(commonData.indexRel, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(ParamScanKeyCheck(commonData.skey, commonData.indexInfo->indexKeyAttrsNum))) {
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    ret = BtreeDeltaDmlForCcindex::CheckExistence({commonData.indexRel, commonData.indexInfo, commonData.skey,
                                                  isLpi, 0}, commonData.values, commonData.isnull, deltaRec);
    IndexInterface::ReportIndexBufferReadStat(commonData.indexRel);
    return ret;
}

void UpdateCcindexBtrBuildHandler(CcindexBtrBuildHandler *handler, StorageRelation indexRel,
                                  IndexBuildInfo *indexBuildInfo)
{
    if (STORAGE_VAR_NULL(handler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"CcindexBtrBuildHandler\".", __FUNCTION__));
        PrintBackTrace();
        return;
    }
    if (STORAGE_VAR_NULL(indexBuildInfo)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexBuildInfo\".", __FUNCTION__));
        return;
    }
    if (STORAGE_FUNC_FAIL(IndexInfoCheck(&indexBuildInfo->baseInfo, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(IndexRelCheck(indexRel, __FUNCTION__))) {
        PrintBackTrace();
        return;
    }
    handler->UpdateIndexRel(indexRel);
    handler->UpdateBtrBuildInfo(indexBuildInfo);
}

CcindexBtrBuildHandler *CreateCcindexBuildHandler(StorageRelation indexRel, ScanKey skey,
                                                  IndexBuildInfo *indexBuildInfo, bool isLpi, bool isRebuild)
{
    /* Check params */
    if (STORAGE_VAR_NULL(indexBuildInfo)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexBuildInfo\".", __FUNCTION__));
        return nullptr;
    }
    if (STORAGE_FUNC_FAIL(IndexInfoCheck(&indexBuildInfo->baseInfo, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(IndexRelCheck(indexRel, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(ParamScanKeyCheck(skey, indexBuildInfo->baseInfo.indexKeyAttrsNum))) {
        PrintBackTrace();
        return nullptr;
    }

    /* Create BuildConcurrently object */
    CcindexBtrBuildHandler *btreeBuilderCcindex = nullptr;
    UNUSE_PARAM AutoMemCxtSwitch autoMemCxtSwitch(thrd->GetSessionMemoryCtx());
    btreeBuilderCcindex = CcindexBtrBuildHandler::Create(indexRel, indexBuildInfo, skey, isLpi);
    if (STORAGE_VAR_NULL(btreeBuilderCcindex)) {
        return nullptr;
    }
    if (isRebuild) {
        btreeBuilderCcindex->SetRebuildFlag();
    }

    IndexInfo *baseInfo = &indexBuildInfo->baseInfo;
    ErrLog(DSTORE_LOG, MODULE_INDEX,
        ErrMsg("[CCINDEX] Index(%s:%u) on pdb(%u) table(%u) create ccindex handler: keyAttrsNum[%u], isUnique[%u], "
               "relKind[%d], tableNum[%u], segment(%hu, %u), snapshot(%lu)",
        baseInfo->indexRelName, indexRel->relOid, indexRel->m_pdbId, indexBuildInfo->heapRelationOid,
        baseInfo->indexAttrsNum, baseInfo->isUnique, baseInfo->relKind, indexBuildInfo->heapRelNum,
        indexRel->btreeSmgr->GetSegMetaPageId().m_fileId, indexRel->btreeSmgr->GetSegMetaPageId().m_blockId,
        thrd->GetSnapShotCsn()));

    return btreeBuilderCcindex;
}

RetStatus WaitForTrxVisibleForAll(PdbId pdbId, CcindexBtrBuildHandler *ccindexBtree, uint64_t xid)
{
    if (STORAGE_VAR_NULL(ccindexBtree)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"CcindexBtrBuildHandler\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    Xid txnId = static_cast<Xid>(xid);
    if (unlikely(pdbId == INVALID_PDB_ID || txnId == INVALID_XID)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Wrong param when wait for active trxs of PDB:%u end for ccindex, Waiting xid(%u, %lu)",
            pdbId, static_cast<uint32>(txnId.m_zoneId), txnId.m_logicSlotId));
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    return ccindexBtree->WaitForTrxVisibleForAll(pdbId, txnId);
}

RetStatus BuildBtreeForCcindex(CcindexBtrBuildHandler *ccindexBtree)
{
    if (STORAGE_VAR_NULL(ccindexBtree)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"CcindexBtrBuildHandler\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    if (STORAGE_VAR_NULL(ccindexBtree->GetIndexBuildInfo())) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexBuildInfo\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(IndexInfoCheck(&ccindexBtree->GetIndexBuildInfo()->baseInfo, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(IndexRelCheck(ccindexBtree->GetIndexRel(), __FUNCTION__))) {
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    RetStatus ret = ccindexBtree->BuildIndexConcurrently();
    IndexInterface::ReportIndexBufferReadStat(ccindexBtree->GetIndexRel());
    return ret;
}

RetStatus MergeDeltaDmlForCcindex(CcindexBtrBuildHandler *ccindexBtree, StorageRelation deltaDmlIdxRel,
                                  IndexInfo *deltaDmlIndexInfo)
{
    if (STORAGE_VAR_NULL(ccindexBtree)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"CcindexBtrBuildHandler\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(IndexInfoCheck(deltaDmlIndexInfo, __FUNCTION__)) ||
        STORAGE_FUNC_FAIL(IndexRelCheck(deltaDmlIdxRel, __FUNCTION__))) {
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    RetStatus ret = ccindexBtree->MergeDeltaDml(deltaDmlIdxRel, deltaDmlIndexInfo);
    IndexInterface::ReportIndexBufferReadStat(deltaDmlIdxRel);
    return ret;
}

RetStatus DestroyCcindexBuildHandler(CcindexBtrBuildHandler *ccindexBtree)
{
    if (STORAGE_VAR_NULL(ccindexBtree)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"CcindexBtrBuildHandler\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    ccindexBtree->Destroy();
    delete ccindexBtree;
    return DSTORE_SUCC;
}

void DumpScanPage(IndexScanHandler *scanHandler, DSTORE::Datum &fileId, DSTORE::Datum &blockId, DSTORE::Datum &data)
{
    scanHandler->DumpScanPage(fileId, blockId, data);
}

RetStatus MarkPosition(DSTORE::IndexScanHandler *scanHandler)
{
    if (STORAGE_VAR_NULL(scanHandler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanHandler\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    
    scanHandler->MarkPosition();
    return DSTORE_SUCC;
}

RetStatus RestorePosition(DSTORE::IndexScanHandler *scanHandler)
{
    if (STORAGE_VAR_NULL(scanHandler)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        PrintBackTrace();
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"IndexScanHandler\".", __FUNCTION__));
        return DSTORE_FAIL;
    }

    scanHandler->RestorePosition();
    return DSTORE_SUCC;
}

}  // namespace IndexInterface

namespace DSTORE {

void IndexInfo::Free()
{
    DstorePfree(attributes);
#ifndef UT
#ifndef DSTORE_TEST_TOOL
    if (m_indexSupportProcInfo != nullptr) {
        DstorePfreeExt(m_indexSupportProcInfo->supportProcs);
        DstorePfreeExt(m_indexSupportProcInfo->opfamily);
        DstorePfreeExt(m_indexSupportProcInfo->supportFmgrInfo);
        DstorePfreeExt(m_indexSupportProcInfo);
    }
#endif
#endif
    DstorePfree(this);
}

}
