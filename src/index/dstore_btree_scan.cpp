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
 */

#include "index/dstore_index_handler.h"
#include "index/dstore_btree_perf_unit.h"
#include "page/dstore_index_page.h"
#include "lock/dstore_lock_dummy.h"
#include "errorcode/dstore_index_error_code.h"
#include "common/datatype/dstore_array_utils.h"
#include "transaction/dstore_transaction.h"
#include "undo/dstore_undo_zone.h"
#include "transaction/dstore_transaction_mgr.h"

namespace DSTORE {

BtreeScan::BtreeScan(StorageRelation indexRel, IndexInfo *indexInfo)
    : Btree(indexRel, indexInfo),
      m_anyValidScanKey(false),
      m_keysConflictFlag(false),
      m_numberOfKeys(0),
      m_numberOfKeysIncludeArr(0),
      m_numArrCond(0),
      m_showAnyTuples(false),
      m_arrCondInfo(nullptr),
      m_checkTupleMatchFastFlag(false),
      m_uniqueTupleSearch(false),
      m_fastcheckKeys{},
      m_scanKeyForCheck(nullptr),
      m_scanKeyIncludeArry(nullptr)
{
    InitScanContext();
}

BtreeScan::~BtreeScan()
{
    m_arrCondInfo = nullptr;
    m_scanKeyForCheck = nullptr;
    m_scanKeyIncludeArry = nullptr;
}

RetStatus BtreeScan::BeginScan(IndexScanDesc scan)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(this->GetPdbId());
    if (STORAGE_VAR_NULL(pdb)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"StoragePdb\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    TransactionMgr *transactionMgr = pdb->GetTransactionMgr();
    if (STORAGE_VAR_NULL(transactionMgr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"TransactionMgr\".", __FUNCTION__));
        return DSTORE_FAIL;
    }

    m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
    m_currentStatus.crBufDesc = INVALID_BUFFER_DESC;
    InvalidateScanPos();

    if (scan->numberOfKeys > 0) {
        m_scanKeyForCheck = (ScanKey)DstorePalloc(static_cast<uint>(scan->numberOfKeys) * sizeof(ScanKeyData));
        if (unlikely(m_scanKeyForCheck == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when BeginScan."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return DSTORE_FAIL;
        }
    } else {
        m_scanKeyForCheck = nullptr;
        m_scanKeyIncludeArry = nullptr;
    }
    return DSTORE_SUCC;
}

RetStatus BtreeScan::ReScan(IndexScanDesc scan)
{
    /* we should not keep any locks but maybe pinned cr page. unpin it */
    if (IsScanPosValid()) {
        UnpinScanPosIfPinned();
        InvalidateScanPos();
    }
    m_currentStatus.prevMatchedHeapCtid = INVALID_ITEM_POINTER;
    m_currentStatus.prevMatchedTableOid = DSTORE_INVALID_OID;
    m_markItemIndex = -1;
    ProcessScanKey(scan->keyData, scan->numberOfKeys, m_scanKeyForCheck, m_numberOfKeys, false);
    if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        /* compare function failed comparing */
        return DSTORE_FAIL;
    }
    if (unlikely(m_keysConflictFlag)) {
        /* Although there's no valid scankey found, we should not return fail because we've finished the process
         * successfully in dstore. */
        return DSTORE_SUCC;
    }

    if (STORAGE_FUNC_FAIL(InitArrayCondition())) {
        return DSTORE_FAIL;
    }
    m_uniqueTupleSearch = IsUniqueTupleSearch();
    return DSTORE_SUCC;
}

bool BtreeScan::IsUniqueTupleSearch()
{
    if (m_arrCondInfo || !m_indexInfo->isUnique || m_numberOfKeys != m_indexInfo->indexKeyAttrsNum ||
        m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX)) {
        return false;
    }
    for (int i = 0; i < m_numberOfKeys; i++) {
        ScanKey cur = &m_scanKeyForCheck[i];
        if ((cur->skFlags & SCAN_KEY_SEARCHNULL) || (cur->skFlags & SCAN_KEY_SEARCHNOTNULL)) {
            return false;
        }
        if (cur->skStrategy != SCAN_ORDER_EQUAL) {
            return false;
        }
    }
    return true;
}

RetStatus BtreeScan::GetNextTuple(IndexScanDesc scan, ScanDirection dir, bool *found)
{
    StorageAssert(found != nullptr);
    *found = false;
    if (unlikely(m_keysConflictFlag)) {
        return DSTORE_SUCC;
    }

    if (!IsScanPosValid()) {
        StorageClearError();
        if (m_numArrCond != 0) {
            if (unlikely(m_numArrCond < 0)) {
                return DSTORE_SUCC;
            }
            /* dealing with array key */
            /* initialize the array keys by chosing the first array key value for each key for the first call */
            if (STORAGE_FUNC_FAIL(GetFirstArrCondition(dir))) {
                return DSTORE_FAIL;
            }
        }
    }
    // perf opt: LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeGetNextTupleLatency);
    do {
        if (STORAGE_FUNC_FAIL(GetNextTupleInternal(scan, dir, found))) {
            /* We may has no change to call EndScan, release BuffDesc we pinned to avoid pin leak */
            UnpinScanPosIfPinned();
            return DSTORE_FAIL;
        }
    } while (!*found && StepToNextArrCondition(dir));
    return DSTORE_SUCC;
}

RetStatus BtreeScan::GetNextTupleInternal(IndexScanDesc scan, ScanDirection dir, bool *found)
{
    StorageAssert(found != nullptr);
    /*
     * For the first time to get index tuple,
     * we'll descend to leaf from the root/(fast root) and find the first scan position on leaf.
     */
    if (!IsScanPosValid()) {
        if (m_numArrCond != 0) {
            ProcessScanKey(m_scanKeyIncludeArry, m_numberOfKeysIncludeArr, m_scanKeyForCheck, m_numberOfKeys, true);
        }
        /* construct scan keys at the first call */
        m_anyValidScanKey = MakePositioningKeys(dir);
        if (unlikely(m_keysConflictFlag)) {
            return DSTORE_SUCC;
        }
    }
    if (unlikely(!IsScanPosValid()) && STORAGE_FUNC_FAIL(DescendToLeaf(dir))) {
        return DSTORE_FAIL;
    }
    /* find! store results. */
    if (STORAGE_FUNC_FAIL(ScanOnLeaf(dir, found))) {
        return DSTORE_FAIL;
    }
    if (likely(*found)) {
        IndexTuple *itup = m_currentStatus.matchItems[m_currentStatus.itemIndex];
        ItemPointerData heapCtid = itup->GetHeapCtid();
        if (unlikely(heapCtid == INVALID_ITEM_POINTER)) {
            ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("Index tuple on leaf page has null heap ctid."));
        }
        scan->heapCtid = heapCtid;
        if (unlikely(scan->wantItup)) {
            scan->itup = itup;
        }
        if (unlikely(scan->wantPartOid)) {
            bool isNull;
            scan->currPartOid = DatumGetUInt32(itup->GetAttr((m_indexInfo->tableOidAtt), m_indexInfo->attributes,
                &isNull));
            StorageAssert(!isNull && m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX));
        }
        if (unlikely(m_showAnyTuples && scan->wantXid)) {
            scan->insertXid = m_currentStatus.matchInsertXids[m_currentStatus.itemIndex];
            scan->deleteXid = m_currentStatus.matchDeleteXids[m_currentStatus.itemIndex];
        }
        dir == ScanDirection::FORWARD_SCAN_DIRECTION ? (m_currentStatus.itemIndex++) : (m_currentStatus.itemIndex--);
    }
    return DSTORE_SUCC;
}

void BtreeScan::EndScan()
{
    /* we aren't holding any read locks, but gotta drop the pins */
    if (IsScanPosValid()) {
        UnpinScanPosIfPinned();
    }

    /* Release storage */
    if (m_scanKeyForCheck != nullptr) {
        DstorePfree(m_scanKeyForCheck);
        m_scanKeyForCheck = nullptr;
    }

    if (m_scanKeyIncludeArry != nullptr) {
        DstorePfree(m_scanKeyIncludeArry);
        m_scanKeyIncludeArry = nullptr;
    }

    /* Release matchInsertXids and matchDeleteXids. */
    if (m_showAnyTuples) {
        DstorePfreeExt(m_currentStatus.matchInsertXids);
        DstorePfreeExt(m_currentStatus.matchDeleteXids);
    }
}

RetStatus BtreeScan::InitArrayCondition()
{
    /* Step 1. find all array keys */
    int nArrKeys = 0;
    for (int i = 0; i < m_numberOfKeys; i++) {
        ScanKey cur = &m_scanKeyForCheck[i];
        if ((cur->skFlags & SCAN_KEY_SEARCHARRAY) != 0U) {
            nArrKeys++;
            StorageAssert(!(cur->skFlags & (SCAN_KEY_ROW_HEADER | SCAN_KEY_SEARCHNULL | SCAN_KEY_SEARCHNOTNULL)));
            /* If any arrays are null as a whole, we can quit right now since NULL equals to nothing. */
            if ((cur->skFlags & SCAN_KEY_ISNULL) != 0U) {
                /* must set m_numArrCond to -1 to separate from non-array-key case. */
                m_numArrCond = -1;
                return DSTORE_SUCC;
            }
        }
    }
    if (nArrKeys == 0) {
        DstorePfreeExt(m_arrCondInfo);
        DstorePfreeExt(m_scanKeyIncludeArry);
        return DSTORE_SUCC;
    }

    /* Step 2. extract elements from array keys */
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->GetTopTransactionMemoryContext());
    /* free old keys if any since we re starting a new scan */
    DstorePfreeExt(m_arrCondInfo);
    m_arrCondInfo =
        static_cast<BtrArrayCondInfo *>(DstorePalloc0(sizeof(BtrArrayCondInfo) * static_cast<uint32>(nArrKeys)));
    if (STORAGE_VAR_NULL(m_scanKeyForCheck)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when InitArrayCondition."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    /* free old keys if any since we re starting a new scan */
    DstorePfreeExt(m_scanKeyIncludeArry);
    m_scanKeyIncludeArry = (ScanKey)DstorePalloc(m_numberOfKeys * sizeof(ScanKeyData));
    if (unlikely(m_scanKeyIncludeArry == nullptr)) {
        DstorePfreeExt(m_arrCondInfo);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc m_scanKeyIncludeArry fail when InitArrayCondition."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }

    errno_t rc = memcpy_s(m_scanKeyIncludeArry, sizeof(ScanKeyData) * m_numberOfKeys, m_scanKeyForCheck,
                          sizeof(ScanKeyData) * m_numberOfKeys);
    storage_securec_check(rc, "\0", "\0");
    m_numberOfKeysIncludeArr = m_numberOfKeys;
    return DSTORE_SUCC;
}

RetStatus BtreeScan::ResetArrCondInfo(int numKeys, Datum **values, bool **isnulls, int *numElem)
{
    if (unlikely(numKeys != m_numberOfKeysIncludeArr)) {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }

    int arrKeyCount = 0;
    bool needFreeArrScanKey = false;
    bool needFreeArrVal = false;
    ScanKey arrScanKey = nullptr;
    for (int i = 0; i < m_numberOfKeysIncludeArr; i++) {
        ScanKey curScanKey = &m_scanKeyIncludeArry[i];
        if ((curScanKey->skFlags & SCAN_KEY_SEARCHARRAY) == 0U) {
            continue;
        }

        /* save all non-null values */
        int nNonNullValues = 0;
        for (int j = 0; j < numElem[i]; j++) {
            if (!isnulls[i][j]) {
                values[i][nNonNullValues++] = values[i][j];
            }
        }

        if (nNonNullValues == 0) {
            arrKeyCount = -1;
            break;
        }

        ArrayType *arrVal = DatumGetArray(curScanKey->skArgument);
        needFreeArrVal = (PointerGetDatum(arrVal) != curScanKey->skArgument);
        if (STORAGE_VAR_NULL(arrVal)) {
            return DSTORE_FAIL;
        }
        if (curScanKey->skStrategy != SCAN_ORDER_EQUAL) {
            arrScanKey = SelectScanFuncForArgs(arrVal->elemtype, arrVal->elemtype, curScanKey, needFreeArrScanKey);
            if (STORAGE_VAR_NULL(arrScanKey)) {
                DstorePfreeExt(m_arrCondInfo);
                if (needFreeArrVal) {
                    DstorePfreeExt(arrVal);
                }
                return DSTORE_FAIL;
            }
            curScanKey->skArgument = FindExtremeArrayElement(arrScanKey, values[i], numElem[i]);
            if (needFreeArrScanKey) {
                DstorePfreeExt(arrScanKey);
            }
            if (needFreeArrVal) {
                DstorePfreeExt(arrVal);
            }
            /* skip other values, save extreme element only.
             * no need to save BtrArrayKey Info for single element */
            continue;
        }

        BtrArrayCondInfo *curArrKeyInfo = &m_arrCondInfo[arrKeyCount++];
        curArrKeyInfo->checkingKeyIdx = i;
        curArrKeyInfo->elemType = arrVal->elemtype;
        curArrKeyInfo->numElem = nNonNullValues;
        curArrKeyInfo->elemValues = values[i];
        if (needFreeArrVal) {
            DstorePfreeExt(arrVal);
        }
    }
    m_numArrCond = arrKeyCount;
    if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        /* compare function failed comparing */
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

Datum BtreeScan::FindExtremeArrayElement(ScanKey skey, Datum *values, int nElems) const
{
    StrategyNumber strat = skey->skStrategy;
    StorageAssert(strat != SCAN_ORDER_EQUAL);
    StorageAssert(nElems > 0);

    Datum result = values[--nElems];
    if (strat == SCAN_ORDER_LESS || strat == SCAN_ORDER_LESSEQUAL) {
        while (--nElems >= 0) {
            if (DatumGetInt32(FunctionCall2Coll(&skey->skFunc, skey->skCollation, values[nElems], result)) > 0) {
                result = values[nElems];
            }
        }
    } else if (strat == SCAN_ORDER_GREATER || strat == SCAN_ORDER_GREATEREQUAL) {
        while (--nElems >= 0) {
            if (DatumGetInt32(FunctionCall2Coll(&skey->skFunc, skey->skCollation, values[nElems], result)) < 0) {
                result = values[nElems];
            }
        }
    } else {
        ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("unknown StrategyNumber: %hu.", skey->skStrategy));
    }

    return result;
}

RetStatus BtreeScan::RearrangeArrayKeysAndDedup(BtrArrayCondInfo *curArrKeyInfo, TuplesortMgr *sortMgr,
                                                ScanKey checkingKey)
{
    bool needFreeArrScanKey = false;
    ScanKey arrScanKey = SelectScanFuncForArgs(curArrKeyInfo->elemType, curArrKeyInfo->elemType,
                                               checkingKey, needFreeArrScanKey);
    if (STORAGE_VAR_NULL(arrScanKey)) {
        return DSTORE_FAIL;
    }
    /* rearrange array keys and dedup */
    int prevUniq = -1;
    for (int j = 0; j < curArrKeyInfo->numElem; j++) {
        sortMgr->GetNextDatum(&curArrKeyInfo->elemValues[j]);
        bool eq = false;
        if (prevUniq >= 0) {
            eq = DatumGetBool(FunctionCall2Coll(&arrScanKey->skFunc, arrScanKey->skCollation,
                curArrKeyInfo->elemValues[prevUniq], curArrKeyInfo->elemValues[j]));
        }

        if (!eq) {
            curArrKeyInfo->elemValues[++prevUniq] = curArrKeyInfo->elemValues[j];
        }
    }
    curArrKeyInfo->numElem = prevUniq + 1;
    if (needFreeArrScanKey) {
        DstorePfreeExt(arrScanKey);
    }
    return DSTORE_SUCC;
}

RetStatus BtreeScan::GetFirstArrCondition(ScanDirection dir)
{
    /* This scenario does not use a temporary file, but for code robustness, we give it a name.
     * This base temp file name is same as index build,
     * but we will be ok because build and scan never happen at same time. */
    TuplesortMgr *sortMgr = TuplesortMgr::CreateIdxTupleSortMgr(m_indexRel->m_pdbId, m_indexInfo->indexRelId);
    if (STORAGE_VAR_NULL(sortMgr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when CreateIdxTupleSortMgr."));
        return DSTORE_FAIL;
    }
    /* dealing with array key */
    /* initialize the array keys by chosing the first array key value for each key for the first call */
    RetStatus ret = DSTORE_SUCC;
    for (int i = 0; i < m_numArrCond; i++) {
        BtrArrayCondInfo *curArrKeyInfo = &m_arrCondInfo[i];
        ScanKey checkingKey = &m_scanKeyIncludeArry[curArrKeyInfo->checkingKeyIdx];
        if (curArrKeyInfo->numElem == 0) {
            checkingKey->skArgument = static_cast<Datum>(0);
            checkingKey->skFlags |= SCAN_KEY_ISNULL;
            continue;
        }
        StorageAssert(curArrKeyInfo->numElem > 0);
        curArrKeyInfo->curElem = (dir == ScanDirection::BACKWARD_SCAN_DIRECTION) ? curArrKeyInfo->numElem - 1 : 0;
        checkingKey->skArgument = curArrKeyInfo->elemValues[curArrKeyInfo->curElem];
        if (curArrKeyInfo->numElem == 1) {
            continue;
        }
        /* sort array keys so that we can keep output in order */
        if (STORAGE_FUNC_FAIL(
            sortMgr->PrepareDatumSortInfo(m_indexInfo, g_storageInstance->GetGuc()->maintenanceWorkMem, checkingKey))) {
            ret = DSTORE_FAIL;
            break;
        }

        for (int j = 0; j < curArrKeyInfo->numElem; j++) {
            sortMgr->PutDatum(curArrKeyInfo->elemValues[j], false);
        }
        if (STORAGE_FUNC_FAIL(sortMgr->PerformSortDatum())) {
            ret = DSTORE_FAIL;
            break;
        }
        if (checkingKey->skStrategy == SCAN_ORDER_EQUAL) {
            if (STORAGE_FUNC_FAIL(RearrangeArrayKeysAndDedup(curArrKeyInfo, sortMgr, checkingKey))) {
                break;
            }
            checkingKey->skArgument = curArrKeyInfo->elemValues[curArrKeyInfo->curElem];
        }
        sortMgr->Clear();
    }
    sortMgr->Destroy();
    delete sortMgr;
    if (STORAGE_FUNC_FAIL(ret)) {
        return ret;
    }

    /* alse update fast check keys for INT4 type if any */
    if (m_checkTupleMatchFastFlag) {
        m_checkTupleMatchFastFlag = TryMakeFastCheckKeys(m_scanKeyIncludeArry, m_numberOfKeysIncludeArr);
    }
    return DSTORE_SUCC;
}

bool BtreeScan::StepToNextArrCondition(ScanDirection dir)
{
    bool found = false;
    if (STORAGE_VAR_NULL(m_scanKeyIncludeArry)) {
        return found;
    }
    for (int i = m_numArrCond - 1; i >= 0; i--) {
        BtrArrayCondInfo *curArrKey = &m_arrCondInfo[i];

        found = false;
        curArrKey->curElem = (dir == ScanDirection::FORWARD_SCAN_DIRECTION) ?
                             curArrKey->curElem + 1 : curArrKey->curElem - 1;
        if (curArrKey->curElem < 0) {
            curArrKey->curElem = curArrKey->numElem - 1; /* Set to the last one. We're stepping to the next array key */
        } else if (curArrKey->curElem >= curArrKey->numElem) {
            curArrKey->curElem = 0; /* Set to the first one. We're stepping to the next array key */
        } else {
            found = true;
        }

        m_scanKeyIncludeArry[curArrKey->checkingKeyIdx].skArgument = curArrKey->elemValues[curArrKey->curElem];

        if (found) {
            break;
        }
    }
    return found;
}

/* We can short-circuit most of the work if there's just one scankey */
void BtreeScan::ProcessSingleScanKey(ScanKey skey, ScanKey outkeys, int16 *indoption)
{
    StorageAssert(!(skey->skFlags & SCAN_KEY_ROW_HEADER));

    if (!TransferScanKeyStrategy(skey, indoption)) {
        m_keysConflictFlag = true;
    }

    SaveValidScanKey(skey, outkeys, skey->skAttno == 1);
    /* At last, try to make fast-check-keys. If successful,
    * we will use memory compare to check if index tuple can satisfied the scankey fastly. */
    m_checkTupleMatchFastFlag = TryMakeFastCheckKeys(outkeys, 1);
}

/*
 * Process scankeys from executor(stored in parameter scan->keyData)
 * and the processed ones will be stored in private class member m_scanKeyForCheck.
 * We do three things here:
 * 1. Make sure if there are conflict scankeys. like, x = 1 and x = 2.
 * 2. Merge the scankeys. like, "x > 10 and x >= 11" would be merged to "x >= 11".
 * 3. Mark the scankey if it's possible to end the scan.
 *
 * We illustrate the point 3:
 * When we scan on leaf page, if an index tuple fails to match scankeys, We may not stop the scan process. i.e.,
 * we continue scan.
 * Cause there is a common sense that the index is sorted as the first attribute, then the second one,
 * then the thrid one... Such as, if we only look at the second attribute, the index tuple are absolutely disordered.
 * So, if there is a scankey only about the second attribute, we'll scan the whole index tuple no matter
 * any of them matches or dismatches the scankey.
 *
 * Here, this function have to mark scankeys which have ability to stop the scan process.
 * The rule is quit simple:
 * 1. all scankeys ranked as the prefix of index attrributes.
 * That is, with the index col of a, b, c, d, The scankey can be about: (a); (a, b); (a, b, c); (a, b, c, d)
 * 2. The scankey can be marked to stop the scan if the strategy of before ones are "=".
 * That is:
 *    (a = 1, b </>/<=/>= 1) mark a, mark b;
 *    (a </<=/>/>= 1, b = 1) mark a, cannot mark b;
 */
void BtreeScan::ProcessScanKey(ScanKey inkeys, int inkeysNum, ScanKey outkeys, int &outkeysNum, bool checkArrKey)
{
    int numOfProcessKeys = inkeysNum;  /* number of scankeys need to be processed. */
    int newNumberOfKeys;
    int numberOfEqualCols;
    ScanKey cur;
    ScanKey stratBucket[MAX_STRATEGY_NUM];
    int i;
    AttrNumber attno;

    /* initialize result variables */
    m_keysConflictFlag = false;
    m_checkTupleMatchFastFlag = false;
    outkeysNum = 0;

    if (numOfProcessKeys < 1) {
        return;    /* nothing to do */
    }

    if (numOfProcessKeys == 1) {
        ProcessSingleScanKey(inkeys, outkeys, m_indexInfo->indexOption);
        outkeysNum = 1;
        return;
    }

    newNumberOfKeys = 0;
    numberOfEqualCols = 0;  /* used to mark if it can stop scan process on leaf */
    cur = &inkeys[0];
    attno = 1;
    errno_t rc = memset_s(stratBucket, sizeof(stratBucket), 0, sizeof(stratBucket));
    storage_securec_check(rc, "\0", "\0");

    /* Begin. */
    for (i = 0; i < numOfProcessKeys + 1; i++, cur++) {
        if (i == numOfProcessKeys || cur->skAttno != attno) {
            ProcecssScanKeyForOneAttr(stratBucket, outkeys, newNumberOfKeys,
                                      numberOfEqualCols, (numberOfEqualCols == attno - 1));

            if (unlikely(m_keysConflictFlag)) {
                return;
            }

            /* Re-initialize strategy bucket for current new attr. */
            attno = cur->skAttno;
            rc = memset_s(stratBucket, sizeof(stratBucket), 0, sizeof(stratBucket));
            storage_securec_check(rc, "\0", "\0");
        }

        if (i == numOfProcessKeys) {
            break;
        }

        /* Apply indoption to scankey (might change skStrategy!) */
        if (!TransferScanKeyStrategy(cur, m_indexInfo->indexOption)) {
            /* NULL can't be matched, so give up */
            m_keysConflictFlag = true;
            return;
        }

        /* array scankey hasnot value now, just save, compare in GetNextTuple */
        if ((!checkArrKey) && ((cur->skFlags & SCAN_KEY_SEARCHARRAY) != 0U)) {
            SaveValidScanKey(cur, &outkeys[newNumberOfKeys++], (numberOfEqualCols == attno - 1));
            continue;
        }

        /* collect scankey for cur attr */
        bool res = CollectScanKeyForOneAttr(cur, stratBucket);

        if (unlikely(m_keysConflictFlag)) {
            return;
        }

        /* collect fails, we cannot process this scankey, just save it */
        if (!res) {
            SaveValidScanKey(cur, &outkeys[newNumberOfKeys++], (numberOfEqualCols == attno - 1));
        }
    }

    outkeysNum = newNumberOfKeys;
    /*
     * At last, try to make fast-check-keys. If successful,
     * we will use memory compare to check if index tuple can satisfy the scankey.
     */
    m_checkTupleMatchFastFlag = TryMakeFastCheckKeys(outkeys, outkeysNum);
    return;
}


void BtreeScan::MergeEqualStratScanKeyForOneAttr(ScanKey (&stratBucket)[MAX_STRATEGY_NUM])
{
    bool argCompareResult;
    ScanKey eq = stratBucket[SCAN_ORDER_EQUAL - 1];

    for (int i = MAX_STRATEGY_NUM; --i >= 0;) {
        ScanKey chk = stratBucket[i];

        if (!chk || i == (SCAN_ORDER_EQUAL - 1)) {
            continue;
        }
        if (eq->skFlags & SCAN_KEY_SEARCHNULL) {
            /* IS NULL is contradictory to anything else */
            m_keysConflictFlag = true;
            return;
        }
        if (CompareScankeyArgs(eq, chk, chk, &argCompareResult)) {
            if (!argCompareResult) {
                /* keys proven mutually contradictory */
                m_keysConflictFlag = true;
                return;
            }
            /* else discard the redundant non-equality key */
            stratBucket[i] = nullptr;
        }
        /* else, cannot determine redundancy, keep both keys */
    }
    return;
}

/*
 * Merge scankeys for a particular attr.
 * e.g.,
 * x >= 10 and x > 20 merge to x > 20;
 * x = 10 and x > 20 => conflict, quit;
 * x <= 10 and x < 20 merge to x <= 10;
 * Note: every strategy is only kept one scankey here.
 */
void BtreeScan::MergeScanKeyForOneAttr(ScanKey (&stratBucket)[MAX_STRATEGY_NUM])
{
    bool argCompareResult;

    /* try to merge = with others */
    if (stratBucket[SCAN_ORDER_EQUAL - 1]) {
        MergeEqualStratScanKeyForOneAttr(stratBucket);
        if (m_keysConflictFlag) {
            /* no need to do other merge process */
            return;
        }
    }

    /* try to merge <, <= */
    if (stratBucket[SCAN_ORDER_LESS - 1] && stratBucket[SCAN_ORDER_LESSEQUAL - 1]) {
        ScanKey lt = stratBucket[SCAN_ORDER_LESS - 1];
        ScanKey le = stratBucket[SCAN_ORDER_LESSEQUAL - 1];
        if (CompareScankeyArgs(lt, le, le, &argCompareResult)) {
            argCompareResult ? (stratBucket[SCAN_ORDER_LESSEQUAL - 1] = nullptr)
                             : (stratBucket[SCAN_ORDER_LESS - 1] = nullptr);
        }
    }

    /* try to merge >, >= */
    if (stratBucket[SCAN_ORDER_GREATER - 1] && stratBucket[SCAN_ORDER_GREATEREQUAL - 1]) {
        ScanKey gt = stratBucket[SCAN_ORDER_GREATER - 1];
        ScanKey ge = stratBucket[SCAN_ORDER_GREATEREQUAL - 1];
        if (CompareScankeyArgs(gt, ge, ge, &argCompareResult)) {
            argCompareResult ? (stratBucket[SCAN_ORDER_GREATEREQUAL - 1] = nullptr)
                             : (stratBucket[SCAN_ORDER_GREATER - 1] = nullptr);
        }
    }

    return;
}

bool BtreeScan::CollectScanKeyForOneAttr(ScanKey cur, ScanKey (&stratBucket)[MAX_STRATEGY_NUM])
{
    bool argCompareResult = true; /* default assumption */
    int i = cur->skStrategy - 1;

    /* save and return. */
    if (stratBucket[i] == nullptr) {
        stratBucket[i] = cur;
        return true;
    }
    /*
     * now, we keep only the more restrictive key. i.e., merge scankey with the same strategy number.
     * Here: e.g., "x > 10 and x > 20" =>  "x > 20";
     * Note: The scenario of merge different strategy number would be handle in MergeScanKeyForOneAttr.
     * MergeScanKeyForOneAttr: e.g., "x > 10" and "x >= 20" => "x >= 20";
     */
    if (CompareScankeyArgs(cur, stratBucket[i], cur, &argCompareResult)) {
        if (argCompareResult) {
            stratBucket[i] = cur;
        } else if (i == (SCAN_ORDER_EQUAL - 1)) {
            /* key == a && key == b, but a != b */
            m_keysConflictFlag = true;
        }
        /* else old key is more restrictive, keep it */
        return true;
    } else {
        /* We can't determine which key is more restrictive. push this one directly to the output array. */
        return false;
    }
}

void BtreeScan::SaveValidScanKey(ScanKey skey, ScanKey outkeys, bool markStop) const
{
    if (markStop) {
        MarkScanKeyPossibleStopScan(skey);
    }
    errno_t rc = memcpy_s(outkeys, sizeof(ScanKeyData), skey, sizeof(ScanKeyData));
    storage_securec_check(rc, "\0", "\0");
    return;
}

void BtreeScan::ProcecssScanKeyForOneAttr(ScanKey (&stratBucket)[MAX_STRATEGY_NUM], const ScanKey &outkeys,
                                          int &newNumberOfKeys, int &numberOfEqualCols, bool markStop)
{
    /* step1: merge scankey. */
    MergeScanKeyForOneAttr(stratBucket);

    if (unlikely(m_keysConflictFlag)) {
        return;
    }

    /* step2: save valid scankey. If the scankey can stop scan process, mark it. */
    for (int j = MAX_STRATEGY_NUM; --j >= 0;) {
        if (stratBucket[j]) {
            SaveValidScanKey(stratBucket[j], &outkeys[newNumberOfKeys++], markStop);
        }
    }
    if (stratBucket[SCAN_ORDER_EQUAL - 1]) {
        numberOfEqualCols++;     /* keep track. */
    }
    return;
}

/* Transfer scankey's strategy for btree scan. */
bool BtreeScan::TransferScanKeyStrategy(ScanKey skey, int16 *indoption) const
{
    StorageAssert(!(skey->skFlags & SCAN_KEY_ROW_HEADER));
    uint32 addflags;

    /* inherit system wide use flags */
    addflags = static_cast<uint32>(static_cast<int32>(indoption[skey->skAttno - 1])) << SCANKEY_INDEX_OPTION_SHIFT;

    /* Handle "x IS/(IS NOT) NULL" clause.
     * Transfer:
     * 1. "x IS NULL"  =>  "x = NULL";
     * 2. "x IS NOT NULL" => "x > NULL", if NULL are treated as the smallest value. i.e., NULL rank first in index.
     *    "x IS NOT NULL" => "x < NULL", if NULL are treated as the greatest value. i.e., NULL rank last in index.
     */
    if (skey->skFlags & SCAN_KEY_ISNULL) {
        StorageAssert(!(skey->skFlags & SCAN_KEY_ROW_HEADER));
        skey->skFlags |= addflags;
        skey->skSubtype = DSTORE_INVALID_OID;
        skey->skCollation = DSTORE_INVALID_OID;
        if (skey->skFlags & SCAN_KEY_SEARCHNULL) {
            skey->skStrategy = SCAN_ORDER_EQUAL;
        } else if (skey->skFlags & SCAN_KEY_SEARCHNOTNULL) {
            if (skey->skFlags & SCANKEY_NULLS_FIRST) {
                skey->skStrategy = SCAN_ORDER_GREATER;
            } else {
                skey->skStrategy = SCAN_ORDER_LESS;
            }
        } else {
            /* conflict, cannot handle. */
            return false;
        }
        /* Needn't do the rest */
        return true;
    }

    /* Adjust strategy for DESC, if we didn't already */
    if ((addflags & SCANKEY_DESC) && !(skey->skFlags & SCANKEY_DESC)) {
        skey->skStrategy = CommuteStrategyNumber(skey->skStrategy);
    }
    skey->skFlags |= addflags;
    return true;
}

ScanKey BtreeScan::SelectScanFuncForArgs(Oid leftType, Oid rightType, ScanKey operatorKey, bool &needFreeKey) const
{
    /*
     * The caller is going to compare the key arguments of leftarg and rightarg. But the scan function
     * for leftarg v.s. rightarg may be different with the one for leftarg v.s. column_val
     * (or righttarg v.s. column_val) in the operatorKey.
     * Thus we need to select a correct scan function that matches leftarg type and right arg type. */
    Oid columnType = m_indexInfo->opcinType[operatorKey->skAttno - 1];
    leftType = (leftType == DSTORE_INVALID_OID) ? columnType : leftType;
    rightType = (rightType == DSTORE_INVALID_OID) ? columnType : rightType;

    Oid operatorType = (operatorKey->skSubtype == DSTORE_INVALID_OID) ? columnType : operatorKey->skSubtype;
    if (likely(leftType == columnType && rightType == columnType && operatorType == columnType)) {
        needFreeKey = false;
        return operatorKey;
    }

    ScanKey chosenKey = static_cast<ScanKey>(DstorePalloc0(sizeof(ScanKeyData)));
    if (STORAGE_VAR_NULL(chosenKey)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when SelectScanFuncForArgs."));
        return nullptr;
    }
    chosenKey->skFlags = operatorKey->skFlags;
    chosenKey->skAttno = operatorKey->skAttno;
    chosenKey->skStrategy = operatorKey->skStrategy;
    chosenKey->skSubtype = operatorKey->skSubtype;
    chosenKey->skCollation = operatorKey->skCollation;
    chosenKey->skArgument = operatorKey->skArgument;

    StrategyNumber strat = operatorKey->skStrategy;
    /*
     * If the skStrategy was flipped by TransferScanKeyStrategy,
     * we have to un-flip it to get the correct compare function.
     */
    if (operatorKey->skFlags & SCANKEY_DESC) {
        strat = CommuteStrategyNumber(strat);
    }
    FillOpfamilyStratFmgrInfo(m_indexInfo->m_indexSupportProcInfo, leftType, rightType, operatorKey->skAttno,
                              strat, chosenKey->skFunc);
    if (unlikely(chosenKey->skFunc.fnOid == DSTORE_INVALID_OID)) {
        storage_set_error(INDEX_ERROR_UNSUPPORTED_DATA_TYPE, columnType);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("failed to find compare function for type[%u] vs type[%u] on %s(column[%d])",
                   leftType, rightType, m_indexInfo->indexRelName, operatorKey->skAttno));
        DstorePfreeExt(chosenKey);
        needFreeKey = false;
        return nullptr;
    }
    needFreeKey = true;
    return chosenKey;
}

bool BtreeScan::TryMakeFastCheckKeys(ScanKey outkeys, int keysNum)
{
    /*
    * for fast check the index tuple if satisfied scankey.
    * only satisfying all conditions below here it will be used:
    * 1. all scankey are not null and op equals '=' and the type is INT4OID
    * 2. the order of skAttno in scankeys is the same as index order,
    * eg:
    *      index column: a, b, c;
    *      scankey: a = 1, b = 1 -> checkKeysFast: true
    *      scankey: a = 1, c = 1 -> checkKeysFast: false
    */
    TupleDesc tupdesc = m_indexInfo->attributes;
    ScanKey cur = outkeys;
    auto fastPtr = static_cast<int32*>(static_cast<void*>(m_fastcheckKeys));
    for (int i = 0; i < keysNum; i++, cur++) {
        if ((cur->skFlags & SCAN_KEY_ISNULL) != 0U ||
            cur->skSubtype != INT4OID ||
            cur->skStrategy != SCAN_ORDER_EQUAL ||
            cur->skAttno != i + 1 ||         /* judge if it can satisfied the second condition. */
            !tupdesc->attrs[i]->attbyval ||
            tupdesc->attrs[i]->atttypid != INT4OID) {
            return false;
        }
        *(fastPtr + i) = DatumGetInt32(cur->skArgument);
    }
    return true;
}

void BtreeScan::MarkScanKeyPossibleStopScan(ScanKey skey) const
{
    uint32 addflags = 0;

    switch (skey->skStrategy) {
        case SCAN_ORDER_LESS:
        case SCAN_ORDER_LESSEQUAL:
            addflags = SCANKEY_STOP_FORWARD;
            break;
        case SCAN_ORDER_EQUAL:
            addflags = SCANKEY_STOP_FORWARD | SCANKEY_STOP_BACKWARD;
            break;
        case SCAN_ORDER_GREATEREQUAL:
        case SCAN_ORDER_GREATER:
            addflags = SCANKEY_STOP_BACKWARD;
            break;
        default:
            ErrLog(DSTORE_PANIC, MODULE_INDEX,
                   ErrMsg("unrecognized StrategyNumber: %d.", static_cast<int>(skey->skStrategy)));
    }

    skey->skFlags |= addflags;
}

void BtreeScan::CompareScankeyArgsHavingNulls(ScanKey leftarg, ScanKey rightarg, ScanKey op, bool *result) const
{
    StorageAssert((leftarg->skFlags | rightarg->skFlags) & SCAN_KEY_ISNULL);
    StrategyNumber strat;
    int leftnull;
    int rightnull;
    if (leftarg->skFlags & SCAN_KEY_ISNULL) {
        StorageAssert(leftarg->skFlags & (SCAN_KEY_SEARCHNULL |
            SCAN_KEY_SEARCHNOTNULL));
        leftnull = 1;
    } else {
        leftnull = 0;
    }
    if (rightarg->skFlags & SCAN_KEY_ISNULL) {
        StorageAssert(rightarg->skFlags & (SCAN_KEY_SEARCHNULL |
            SCAN_KEY_SEARCHNOTNULL));
        rightnull = 1;
    } else {
        rightnull = 0;
    }
    /*
     * We treat NULL as either greater than or less than all other values.
     * Since true > false, the tests below work correctly for NULLS LAST
     * logic.  If the index is NULLS FIRST, we need to flip the strategy.
     */
    strat = static_cast<StrategyNumber>(op->skStrategy);
    if (op->skFlags & SCANKEY_NULLS_FIRST) {
        strat = static_cast<StrategyNumber>(CommuteStrategyNumber(strat));
    }
    switch (strat) {
        case SCAN_ORDER_LESS:
            *result = (leftnull < rightnull);
            break;
        case SCAN_ORDER_LESSEQUAL:
            *result = (leftnull <= rightnull);
            break;
        case SCAN_ORDER_EQUAL:
            *result = (leftnull == rightnull);
            break;
        case SCAN_ORDER_GREATEREQUAL:
            *result = (leftnull >= rightnull);
            break;
        case SCAN_ORDER_GREATER:
            *result = (leftnull > rightnull);
            break;
        default:
            ErrLog(DSTORE_PANIC, MODULE_INDEX,
                   ErrMsg("unrecognized StrategyNumber: %d.", static_cast<int>(strat)));
    }
}

/*
* Compare two scankey values using a specified operator.
* For a particular attr, comapre their skArgument. This is used to merge scankey.
* we store the operator result in *result and return true. We return false if the comparison could not be made.
* Note: this routine needs to be insensitive to any DESC option applied to the index column.
* For example, "x < 4" is a tighter constraint than "x < 5" regardless of which way the index is sorted.
*/
bool BtreeScan::CompareScankeyArgs(ScanKey leftarg, ScanKey rightarg, ScanKey op, bool *result) const
{
    bool comparable = true;
    bool needFreeKey = false;
    ScanKey cmpKey = SelectScanFuncForArgs(leftarg->skSubtype, rightarg->skSubtype, op, needFreeKey);
    if (STORAGE_VAR_NULL(cmpKey)) {
        /* Failed to get a scankey. Tell caller incomparable */
        return false;
    }
    /*
     * First, deal with cases where one or both args are NULL.  This should
     * only happen when the scankeys represent IS NULL/NOT NULL conditions.
     */
    if ((leftarg->skFlags | rightarg->skFlags) & SCAN_KEY_ISNULL) {
        CompareScankeyArgsHavingNulls(leftarg, rightarg, cmpKey, result);
    } else if (leftarg->skAttno == rightarg->skAttno) {
        *result = DatumGetBool((FunctionCall2Coll(&cmpKey->skFunc,
                                                  cmpKey->skCollation,
                                                  leftarg->skArgument,
                                                  rightarg->skArgument)));
    } else {
        comparable = false;
    }
    if (needFreeKey) {
        DstorePfreeExt(cmpKey);
    }
    return comparable;
}

/*
 * This function is used for making positioning keys to descend to the leaf.
 * We manually construct a scankey to help find the position
 * to start the scan by a implicit boundary of the given scankey.
 * An example would help to get the rule:
 * The index attribute is: a;
 * The scankey passed by sql engine is: a <= 2 and the scan direciton is forward;
 * Now we want to find the first item to start scan on leaf.
 * As we can see, the scankey a <= 2 can only be used to stop the scan rather to start with the forward direction.
 * But a <= 2 implies that a != nulls, So if our index is nulls first, we can construct a scankey: a > nulls to get
 * the start position of the attribute a. If our index is nulls last,
 * we cannot make any posistioning keys but to start the scan from the the most left item of whole btree.
 */
bool BtreeScan::ConstructNotNullScanKey(ScanKey notNullKey, ScanKey normalKey, ScanDirection dir) const
{
    if ((normalKey->skFlags & SCANKEY_NULLS_FIRST) ? dir == ScanDirection::FORWARD_SCAN_DIRECTION :
        dir == ScanDirection::BACKWARD_SCAN_DIRECTION) {
        notNullKey->skFlags = (SCAN_KEY_SEARCHNOTNULL |
                               SCAN_KEY_ISNULL |
                               (normalKey->skFlags & (SCANKEY_DESC | SCANKEY_NULLS_FIRST)));
        notNullKey->skAttno = normalKey->skAttno;
        notNullKey->skStrategy = ((normalKey->skFlags & SCANKEY_NULLS_FIRST) ? SCAN_ORDER_GREATER : SCAN_ORDER_LESS);
        notNullKey->skCollation = normalKey->skCollation;
        notNullKey->skArgument = static_cast<Datum>(0);
        return true;
    }
    return false;
}

inline void BtreeScan::MakePositioningKeysFillFmgrInfo(ScanKey &chosen, AttrNumber curAttr, FmgrInfo &fmgrInfo)
{
    IndexSupportProcInfo *procInfo = m_indexInfo->m_indexSupportProcInfo;
    StorageAssert(chosen->skAttno >= 1 && chosen->skAttno <= m_indexInfo->indexKeyAttrsNum);
    if (chosen->skSubtype == m_indexInfo->opcinType[chosen->skAttno - 1] || chosen->skSubtype == DSTORE_INVALID_OID) {
        FillProcFmgrInfo(procInfo, m_indexInfo->opcinType[chosen->skAttno - 1], curAttr, MAINTAIN_ORDER, fmgrInfo);
    } else {
        FillOpfamilyProcFmgrInfo(procInfo, m_indexInfo->opcinType[chosen->skAttno - 1], chosen->skSubtype, curAttr,
                                 fmgrInfo);
    }
}

/*
 * Construct keys to find the position where we need to start the scan.
 * Note: it should consider the scan direction.
 * these are =, >, or >= keys for a forward scan or =, <, <= keys for a backwards scan.
 * the prior attributes had only =, >= (resp. =, <=) keys. Once we accept
 * a > or < boundary or find an attribute with no boundary (which can be
 * thought of as the same as "> -infinity"), we can't use keys for any
 * attributes to its right, because it would break our simplistic notion of what initial positioning strategy to use.
 * A simple case: If the processed scankeys are "a >= 1" and "a <= 10",
 * we would start the scan only use a >= 1 when the scan direction is forward,
 * Or only use a <= 10 whern the scan direction is backward. Such us,
 * we'll construct a new set of scankey to descend to leaf.
 * Note: the results stored in class member m_scanKeyValues.
 */
bool BtreeScan::MakePositioningKeys(ScanDirection dir)
{
    if (m_numberOfKeys <= 0) {
        return false;
    }
    AttrNumber curattr = 1;
    ScanKey chosen = nullptr;
    ScanKey impliesNN = nullptr;
    ScanKey cur;
    int i;
    int arrKCount = 0;
    uint16 keysCount = 0;
    ScanKey positioningKey;    /* entry */
    m_scanKeyValues.Init();

    for (cur = m_scanKeyForCheck, i = 0;; cur++, i++) {
        if (i >= m_numberOfKeys || cur->skAttno != curattr) {
            positioningKey = m_scanKeyValues.scankeys + keysCount;
            /* Try to make an implicit scankey */
            if (chosen == nullptr && impliesNN != nullptr && ConstructNotNullScanKey(positioningKey, impliesNN, dir)) {
                keysCount++;
                /* Cause the skStrategy of not null key is always SCAN_ORDER_GREATER or SCAN_ORDER_LESS, quit */
                break;
            }
            if (chosen == nullptr) {
                break;
            }
            /*
             * Construct ordinary comparison key, just Transform the search-style scan key
             * to an positioning key by replacing the sk_func with the appropriate btree comparison function
             */
            errno_t rc = memcpy_s(positioningKey, sizeof(ScanKeyData), chosen, sizeof(ScanKeyData));
            storage_securec_check(rc, "\0", "\0");
            StorageAssert(keysCount + 1 == chosen->skAttno);
            MakePositioningKeysFillFmgrInfo(chosen, curattr, positioningKey->skFunc);
            if (unlikely(positioningKey->skFunc.fnOid == DSTORE_INVALID_OID)) {
                storage_set_error(INDEX_ERROR_UNSUPPORTED_DATA_TYPE, m_indexInfo->opcinType[chosen->skAttno - 1]);
                m_keysConflictFlag = false;
                return false;
            }
            StorageAssert(positioningKey->skFunc.fnAddr != nullptr);

            /* dealing with array key */
            /* initialize the array keys by chosing the first array key value for each key for the first call */
            arrKCount = ((chosen->skFlags & SCAN_KEY_SEARCHARRAY) != 0U && m_numArrCond > 0) ?
                        MakeArrPositioningKey(positioningKey, arrKCount, i - 1) : arrKCount;

            keysCount++;
            /* Quit if we have stored a > or < key. */
            if (chosen->skStrategy == SCAN_ORDER_GREATER || chosen->skStrategy == SCAN_ORDER_LESS) {
                break;
            }
            if (i >= m_numberOfKeys || cur->skAttno != curattr + 1) {
                break;
            }
            curattr = cur->skAttno;
            chosen = nullptr;
            impliesNN = nullptr;
        }

        if (cur->skStrategy == SCAN_ORDER_EQUAL) {
            /* override any non-equality choice */
            chosen = cur;
            continue;
        }

        ScanDirection chosenDir = static_cast<uint16>(cur->skStrategy) < static_cast<uint16>(SCAN_ORDER_EQUAL)
                                        ? ScanDirection::BACKWARD_SCAN_DIRECTION
                                        : ScanDirection::FORWARD_SCAN_DIRECTION;
        bool chose = (chosen == nullptr && dir == chosenDir);
        bool imply = (chosen == nullptr && dir != chosenDir);
        chosen = chose ? cur : chosen;
        impliesNN = imply ? cur : impliesNN;
    }
    if (keysCount == 0) {
        return false;
    }
    /* initialize other parameter */
    StorageAssert(keysCount <= INDEX_MAX_KEY_NUM);
    m_scanKeyValues.keySize = keysCount;
    m_scanKeyValues.cmpFastFlag = TryMakeFastCompareKeys();
    return true;
}

int BtreeScan::MakeArrPositioningKey(ScanKey skey, int arrKeyIdx, int checkingKeyIdx)
{
    while (arrKeyIdx < m_numArrCond && m_arrCondInfo[arrKeyIdx].checkingKeyIdx != checkingKeyIdx) {
        arrKeyIdx++;
    }
    if (m_arrCondInfo[arrKeyIdx].checkingKeyIdx == checkingKeyIdx) {
        skey->skArgument = m_arrCondInfo[arrKeyIdx].elemValues[m_arrCondInfo[arrKeyIdx].curElem];
    }
    return arrKeyIdx;
}

bool BtreeScan::TryMakeFastCompareKeys()
{
    if (m_scanKeyValues.keySize == 0) {
        return false;
    }
    TupleDesc tupdesc = m_indexInfo->attributes;
    m_scanKeyValues.values = m_scanKeyValues.fastKeys;
    ScanKey cur;
    for (int i = 0; i < m_scanKeyValues.keySize; i++) {
        cur = &m_scanKeyValues.scankeys[i];
        if ((cur->skFlags & SCAN_KEY_ISNULL) != 0U ||
            cur->skSubtype != INT4OID ||
            !tupdesc->attrs[cur->skAttno - 1]->attbyval ||
            tupdesc->attrs[cur->skAttno - 1]->atttypid != INT4OID) {
            return false;
        }
        *((int32 *)m_scanKeyValues.values + i) = DatumGetInt32(cur->skArgument);
    }
    return true;
}

RetStatus BtreeScan::DescendToLeaf(ScanDirection dir)
{
    /*
     * If there are no usable keys to decide where we can start,
     * just descend to the first or last key and scan from there.
     * Otherwise, Use binary search to descend to the leaf page which the first satisfied index tuple on.
    */
    OffsetNumber offnum;
    RetStatus ret = DSTORE_SUCC;
    if (!m_anyValidScanKey) {
        ret = DownToEdge(dir, &offnum);
    } else {
        ret = DownToLoc(dir, &offnum);
    }
    if (STORAGE_FUNC_FAIL(ret)) {
        return ret;
    }

    /* It's an empty index. */
    if (!IsScanPosValid()) {
        return DSTORE_SUCC;
    }

    InitializeScanDirection(dir);
    (void)FilterOutMatchTuple(dir, offnum);
    if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        /* compare function failed comparing */
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus BtreeScan::ScanOnLeaf(ScanDirection dir, bool *hasMore)
{
    StorageAssert(hasMore != nullptr);
    RetStatus ret = DSTORE_SUCC;
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_leafPageScanLatency);
    if (!IsScanPosValid()) {
        *hasMore = false;     /* empty index. */
    } else if (HasCachedResults(dir)) {
        *hasMore = true;    /* there are some cached results. */
    } else {
        ret = (dir == ScanDirection::FORWARD_SCAN_DIRECTION) ? WalkRightOnLeaf(hasMore) : WalkLeftOnLeaf(hasMore);
    }
    return ret;
}

inline bool BtreeScan::HasCachedResults(ScanDirection dir) const
{
    return dir == ScanDirection::FORWARD_SCAN_DIRECTION ? (m_currentStatus.itemIndex <= m_currentStatus.lastItem) :
           (m_currentStatus.itemIndex >= m_currentStatus.firstItem);
}

void BtreeScan::CopyCurToMarkBeforeStepPage()
{
    StorageAssert(m_markItemIndex >= 0);
    errno_t rc;
    if (m_currentStatus.crBufDesc != INVALID_BUFFER_DESC) {
        /* Copy cr buffer to local page,
         * to ensure the data after the cr buffer unpined.
         */
        rc = memcpy_s(m_currentStatus.localCrPage, LOCAL_CR_PAGE_NUM * sizeof(BtrPage),
                      m_currentStatus.crBufDesc->GetPage(), sizeof(BtrPage));
        storage_securec_check(rc, "", "");
    }
    /* copy m_currentStatus to m_markStatus */
    rc = memcpy_s(&m_markStatus, sizeof(BtrScanContext),
                  &m_currentStatus, sizeof(BtrScanContext));
    storage_securec_check(rc, "", "");
    m_markStatus.baseBufDesc = INVALID_BUFFER_DESC;
    m_markStatus.itemIndex = m_markItemIndex;
    m_markStatus.prevMatchedHeapCtid = m_currentStatus.prevMatchedHeapCtid;
    m_markStatus.prevMatchedTableOid = m_currentStatus.prevMatchedTableOid;
    m_markItemIndex = -1;
}

RetStatus BtreeScan::WalkRightOnLeaf(bool *hasMore)
{
    StorageAssert(hasMore != nullptr);
    *hasMore = false;

    BtrPage *page;
    BtrPageLinkAndStatus *pageMeta;
    m_currentStatus.moreLeft = true;
    if (unlikely(m_markItemIndex >= 0)) {
        /* We should store m_currentStatus to m_markSatus for markpos,
         * because m_currentStatus.crBufDesc will be UnpinScanPosIfPinned
         * and m_currentStatus.currPage will be InvalidateScanPos next,
         * whether or not it actually walked into right page.
         */
        CopyCurToMarkBeforeStepPage();
    }
    UnpinScanPosIfPinned();

    while (m_currentStatus.nextPage != INVALID_PAGE_ID && m_currentStatus.moreRight) {
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent  */
            InvalidateScanPos();
            return DSTORE_FAIL;
        }
        LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_walkRightOnLeafLatency);
        /* Attention!!! This page should not be recycled! */
        m_currentStatus.currPage = m_currentStatus.nextPage;
        if (STORAGE_FUNC_FAIL(MakeLeafCrPage())) {
            return DSTORE_FAIL;
        }
        page = GetCurrCrPage();
        pageMeta = page->GetLinkAndStatus();
        m_currentStatus.nextPage = pageMeta->GetRight();
        /* check for deleted/recycled page */
        if (!pageMeta->IsUnlinked()) {
            if (FilterOutMatchTuple(ScanDirection::FORWARD_SCAN_DIRECTION, pageMeta->GetFirstDataOffset())) {
                *hasMore = true;
                return DSTORE_SUCC;
            }
            if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
                /* compare function failed comparing */
                InvalidateScanPos();
                return DSTORE_FAIL;
            }
        } else {
            /* keep going */
            UnpinScanPosIfPinned();
        }
    }
    InvalidateScanPos();
    return DSTORE_SUCC;
}

/*
 * cause the prev page we got from currpage->prev could be split or deleted.
 * Such as, the prev page stored in currpage is not the real next left page we want.
 * We handle this situation by move right several times, if still not find,
 * we recheck the currpage->prev to see if the split process has completed the modify the prev pointer correctly.
 *
 * Split case: find the left page of pageB
 * e.g.,
 * pageA --> pageA' --> pageB
 *   ^                    |
 *   |____________________|
 * When we locked pageB, the prev of pageB points to the pageA and the pageA is splitting.
 * Then we unlock the pageB and read the pageA (we must first unlock pageB, or it may cause dead lock), meanwhile,
 * pageA splits into (pageA and pageA'). So we try to move right to pageA'
 * in order not to miss all data on non-splitted pageA.
 * Actually, in this process, the pageA' can be further split into (pageA' and pageA''),
 * like:
 * pageA --> pageA' --> pageA'' --> pageB --> pageC
 *   ^                                |
 *   |________________________________|
 * Here we try maxTries=4 to move right. Because the pageB could be deleted during move-right process,
 * which causes we can't find a page whose next is pageB.
 * check it and move right to first non-deleted page after pageB (Here is the pageC).
 * Then try again to find the first left page of pageC.
 */
RetStatus BtreeScan::FindNextLeftPage(bool &hasLeft)
{
    m_currentStatus.baseBufDesc =
        Btree::ReadAndCheckBtrPage(m_currentStatus.currPage, LW_SHARED, m_bufMgr, this->GetPdbId());
    if (STORAGE_VAR_NULL(m_currentStatus.baseBufDesc)) {
        m_currentStatus.currPage = INVALID_PAGE_ID;
        return DSTORE_FAIL;
    }
    BtrPage *page;
    BtrPageLinkAndStatus *pageMeta;
    const int maxTries = 4;
    int retryCounter = 0;

    page = static_cast<BtrPage *>(m_currentStatus.baseBufDesc->GetPage());
    BtrPageLinkAndStatus *origPageMeta = page->GetLinkAndStatus();
    BufferDesc *buf = m_currentStatus.baseBufDesc;

    while (!origPageMeta->IsLeftmost() && retryCounter++ < maxTries) {
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent  */
            m_bufMgr->UnlockAndRelease(buf);
            m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
            m_currentStatus.currPage = INVALID_PAGE_ID;
            return DSTORE_FAIL;
        }
        PageId origPageId = buf->GetPageId();
        PageId expectedLink = origPageId;
        PageId realLeftPageId = origPageMeta->GetLeft();     /* initial assumption */

        /* Step1: try maxTries times to find real left page by move right. */
        int singlePageRetries = 0;
        while (singlePageRetries++ < maxTries) {
            if (unlikely(!realLeftPageId.IsValid())) {
                storage_set_error(INDEX_ERROR_MOVE_END, m_indexInfo->indexRelName);
                Xid btrCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Right page is invalid when FindNextLeftPage. "
                    "pdb:%u, index(%s:%u), createXid(%d, %lu), segment(%hu, %u), currXid(%d, %lu), snapshot: %lu"
                    BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                    GetPdbId(), m_indexInfo->indexRelName, m_indexRel->relOid,
                    static_cast<int>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                    GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                    static_cast<int>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
                    m_snapshot.snapshotCsn, BTR_PAGE_HEADER_VAL(page),
                    BTR_PAGE_LINK_AND_STATUS_VAL(origPageMeta)));
                m_bufMgr->UnlockAndRelease(buf);
                m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
                m_currentStatus.currPage = INVALID_PAGE_ID;
                return DSTORE_FAIL;
            }
            buf = ReleaseOldGetNewBuf(buf, realLeftPageId, LW_SHARED);
            if (STORAGE_VAR_NULL(buf)) {
                m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
                m_currentStatus.currPage = INVALID_PAGE_ID;
                return DSTORE_FAIL;
            }
            StorageAssert(buf != INVALID_BUFFER_DESC);
            page = static_cast<BtrPage *>(buf->GetPage());
            pageMeta = page->GetLinkAndStatus();
            if (pageMeta->next == expectedLink) {
                if (pageMeta->IsUnlinked()) {
                    /* We've found the left page but it's unlinked. need to go one more left */
                    expectedLink = realLeftPageId;
                    realLeftPageId = pageMeta->GetLeft();
                    continue;
                }
                /* Found desired page. */
                m_currentStatus.baseBufDesc = buf;
                m_currentStatus.currPage = realLeftPageId;
                hasLeft = true;
                return DSTORE_SUCC;
            }
            if (pageMeta->IsRightmost()) {
                break;
            }
            realLeftPageId = pageMeta->next;
        }

        /* Step2: Check if our orignal page has been deleted. */
        ErrLog(DSTORE_WARNING, MODULE_INDEX,
            ErrMsg("Failed to find left sibling of btrPage(%d, %u) in index \"%s\" in this round. "
                    "retried %d times. current accessed page: " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                    origPageId.m_fileId, origPageId.m_blockId, m_indexInfo->indexRelName, retryCounter,
                    BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(origPageMeta)));

        buf = ReleaseOldGetNewBuf(buf, origPageId, LW_SHARED);
        if (STORAGE_VAR_NULL(buf)) {
            m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
            m_currentStatus.currPage = INVALID_PAGE_ID;
            return DSTORE_FAIL;
        }
        page = static_cast<BtrPage *>(buf->GetPage());
        origPageMeta = page->GetLinkAndStatus();
        if (origPageMeta->IsUnlinked()) {
            while (origPageMeta->IsUnlinked()) {
                /* Impossible, cause btree delete will not delete rightmost page. */
                StorageAssert(!origPageMeta->IsRightmost());
                buf = ReleaseOldGetNewBuf(buf, origPageMeta->next, LW_SHARED);
                if (STORAGE_VAR_NULL(buf)) {
                    m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
                    m_currentStatus.currPage = INVALID_PAGE_ID;
                    return DSTORE_FAIL;
                }
                page = static_cast<BtrPage *>(buf->GetPage());
                origPageMeta = page->GetLinkAndStatus();
            }
        }
        ErrLog(DSTORE_WARNING, MODULE_INDEX,
            ErrMsg("Going to find left sibling of btrPage(%d, %u) in index \"%s\". orig btrPage(%hu, %u), "
                    "retried %d times. next base page: " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                    page->GetSelfPageId().m_fileId, page->GetSelfPageId().m_blockId, m_indexInfo->indexRelName,
                    origPageId.m_fileId, origPageId.m_blockId, retryCounter,
                    BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(origPageMeta)));
        /* find the new orignal page or use new prev as the real left page assumption */
    }
    m_bufMgr->UnlockAndRelease(buf);
    m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
    m_currentStatus.currPage = INVALID_PAGE_ID;
    hasLeft = false;
    return DSTORE_SUCC;
}

RetStatus BtreeScan::WalkLeftOnLeaf(bool *hasMore)
{
    StorageAssert(hasMore != nullptr);
    *hasMore = false;
    BtrPage *page;
    m_currentStatus.moreRight = true;
    if (unlikely(m_markItemIndex >= 0)) {
        /* We should store m_currentStatus to m_markSatus for markpos,
         * because m_currentStatus.crBufDesc will be UnpinScanPosIfPinned
         * and m_currentStatus.currPage will be InvalidateScanPos next,
         * whether or not it actually walked into left page.
         */
        CopyCurToMarkBeforeStepPage();
    }
    UnpinScanPosIfPinned();

    if (!m_currentStatus.moreLeft) {
        InvalidateScanPos();
        return DSTORE_SUCC;
    }
    bool hasLeft = true;
    while (m_currentStatus.moreLeft) {
        if (STORAGE_FUNC_FAIL(FindNextLeftPage(hasLeft))) {
            return DSTORE_FAIL;
        }
        if (!hasLeft) {
            break;
        }
        if (STORAGE_FUNC_FAIL(MakeLeafCrPage())) {
            return DSTORE_FAIL;
        }
        page = GetCurrCrPage();
        if (FilterOutMatchTuple(ScanDirection::BACKWARD_SCAN_DIRECTION, page->GetMaxOffset())) {
            *hasMore = true;
            return DSTORE_SUCC;
        }
    }

    InvalidateScanPos();
    if (unlikely(StorageGetErrorCode() == SQL_WARNING_REQUEST_ARE_CANCELED ||
        StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

/*
* Read leaf cr page given the page id from m_currentStatus.currPage.
* if the buffer description (baseBufDesc) is provided, then construct cr page directly,
* otherwise this function would construct cr page on the page owner node if guc switch enabled.
* In our design, we would read/construct the cr page to replace lock the real page.
* so, at last, three things should be ensured:
* 1. baseBufDesc should be unpinned and related page shouled be released.
* 2. m_currentStatus.crBufDesc should be valid.
* 3. m_currentStatus.currPage should be valid.
*/
RetStatus BtreeScan::MakeLeafCrPage()
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_crPageMakeLatency);

    StorageAssert(m_currentStatus.currPage != INVALID_PAGE_ID);
    StorageAssert(m_currentStatus.crBufDesc == INVALID_BUFFER_DESC);

    BufMgrInterface *bufMgr = nullptr;
    BtreeStorageMgr *btreeSmgr = GetBtreeSmgr();
    if (unlikely(btreeSmgr->IsGlobalTempIndex())) {
        bufMgr = thrd->GetTmpLocalBufMgr();
    } else {
        bufMgr = g_storageInstance->GetBufferMgr();
    }

    BtrPageLinkAndStatus *pageMeta;
    Transaction *transaction = thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    PageId pageId = m_currentStatus.currPage;
    BufferDesc *currentRootPageInCache = btreeSmgr->GetLowestSinglePageDescFromCache();
    if (currentRootPageInCache != nullptr) {
        if (currentRootPageInCache->GetPageId() == pageId) {
            btreeSmgr->ReleaseLowestSinglePageCache();
        }
    }

    BtreeUndoContext btrUndoContext(this->GetPdbId(), btreeSmgr->GetBtrMetaPageId(), m_indexInfo, m_bufMgr,
                                    btreeSmgr->GetMetaCreateXid(), BtreeUndoContextType::CONSTRUCT_CR);
    if (unlikely(m_currentStatus.baseBufDesc == nullptr || g_storageInstance->GetGuc()->enableRemoteCrConstruction)) {
        if (m_currentStatus.baseBufDesc != nullptr) {
            bufMgr->UnlockAndRelease(m_currentStatus.baseBufDesc, BufferPoolUnlockContentFlag());
            /* Set m_currentStatus.baseBufDesc to invalid to avoid double release. */
            m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
        }
        /*
         * Base page is not provided, use consistent read in buffer manager to avoid
         * reading undo pages remotely if the page does not belong to self node.
         */
        ConsistentReadContext crContext;
        crContext.pdbId = this->GetPdbId();
        crContext.pageId = pageId;
        crContext.currentXid = thrd->GetActiveTransaction()->GetCurrentXid();
        crContext.snapshot = &m_snapshot;
        crContext.dataPageExtraInfo = static_cast<void *>(&btrUndoContext);
        crContext.destPage = m_currentStatus.localCrPage;
        crContext.crBufDesc = nullptr;

        RetStatus ret = m_bufMgr->ConsistentRead(crContext);
        if (STORAGE_FUNC_FAIL(ret)) {
            btrUndoContext.Destroy();
            return DSTORE_FAIL;
        }

        if (crContext.crBufDesc != nullptr) {
            m_currentStatus.crBufDesc = crContext.crBufDesc;
            StorageAssert(m_currentStatus.crBufDesc->IsCrPage());
        }
    } else if (btreeSmgr->IsGlobalTempIndex() ||
        (m_currentStatus.crBufDesc = transaction->GetCrPage(m_currentStatus.baseBufDesc, &m_snapshot)) ==
         INVALID_BUFFER_DESC) {
        /* If the base page is already provided, then construct cr page directly. */
        CRContext crCtx{this->GetPdbId(), INVALID_CSN, m_currentStatus.localCrPage,
                        m_currentStatus.baseBufDesc, nullptr, false, btreeSmgr->IsGlobalTempIndex(),
                        &m_snapshot,
                        thrd->GetActiveTransaction()->GetCurrentXid()};
        if (STORAGE_FUNC_FAIL(transaction->ConstructCrPage(&crCtx, &btrUndoContext))) {
            btrUndoContext.Destroy();
            /* Set m_currentStatus.baseBufDesc to invalid to avoid double release. */
            m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        StorageAssert((crCtx.useLocalCr) || (crCtx.crBufDesc != INVALID_BUFFER_DESC));
        m_currentStatus.crBufDesc = crCtx.crBufDesc;
    }
    m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;  /* already released related page in Cr module */
    btrUndoContext.Destroy();
    if (unlikely(m_currentStatus.crBufDesc != nullptr && m_currentStatus.crBufDesc->GetPage() == nullptr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s]btrPage(%hu, %u) in CR buffDesc is null.", __FUNCTION__, pageId.m_fileId, pageId.m_blockId));
        PrintBackTrace();
        return DSTORE_FAIL;
    }

    BtrPage *crPage = GetCurrCrPage();
    pageMeta = crPage->GetLinkAndStatus();

    StorageAssert(m_currentStatus.currPage == crPage->GetSelfPageId());
    m_currentStatus.nextPage = pageMeta->next;
    return DSTORE_SUCC;
}

bool BtreeScan::CheckCommittedTuple(BtrPage* page, IndexTuple *itup) const
{
    /* The uncommitted tuple has been rolled back when the snapshot mvcc or snapshot now is used to build the CR page.
     */
    if (likely(m_snapshot.GetSnapshotType() != SnapshotType::SNAPSHOT_DIRTY)) {
        return true;
    }
    if (itup->TestTdStatus(ATTACH_TD_AS_HISTORY_OWNER) || (itup->TestTdStatus(DETACH_TD))) {
        return true;
    }

    TD *td = page->GetTd(itup->GetTdId());
    StorageAssert(td != nullptr);
    Xid checkTupleXid = td->GetXid();
    StorageAssert(checkTupleXid != INVALID_XID);
    StorageAssert(!td->TestStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE));
    XidStatus xs(checkTupleXid, thrd->GetActiveTransaction(), td);
    return !(xs.IsInProgress() || xs.IsPendingCommit());
}

bool BtreeScan::SaveItupIfNeeded(int itemIndex, BtrPage *page, OffsetNumber offnum,
                                 ScanDirection dir, bool &continuescan)
{
    bool needSave = false;
    ItemId *iid = page->GetItemIdPtr(offnum);
    if (!iid->IsNormal()) {
        return needSave;
    }
    IndexTuple *itup = page->GetIndexTuple(iid);
    if (itup->IsDeleted() && CheckCommittedTuple(page, itup) &&!m_showAnyTuples) {
        return needSave;
    }
    ItemPointerData heapCtid = itup->m_link.heapCtid;
    if (unlikely(m_currentStatus.prevMatchedHeapCtid == heapCtid)) {
        if ((m_indexInfo->relKind != static_cast<char>(SYS_RELKIND_GLOBAL_INDEX)) ||
            m_currentStatus.prevMatchedTableOid == itup->GetTableOid(m_indexInfo)) {
            return needSave;
        }
    }
    if (!CheckTuple(itup, m_indexInfo->indexKeyAttrsNum, dir, &continuescan)) {
        return needSave;
    }

    needSave = true;
    int savePos = (dir == ScanDirection::FORWARD_SCAN_DIRECTION) ? itemIndex : itemIndex - 1;
    m_currentStatus.prevMatchedHeapCtid = heapCtid;
    if (m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX)) {
        m_currentStatus.prevMatchedTableOid = itup->GetTableOid(m_indexInfo);
    }
    m_currentStatus.matchItems[savePos] = itup;
    if (m_showAnyTuples) {
        GetItupInsertAndDeleteXids(page, itup, m_currentStatus.matchInsertXids[savePos],
                                   m_currentStatus.matchDeleteXids[savePos]);
    } else if (m_uniqueTupleSearch) {
        continuescan = false;
    }
    return needSave;
}

/*
* Filter out matches tuple from page. The filter range is:
* [Offnum, LastDataOffset] if the direction is forward.
* [FirstDataOffset, Offnum] if the direction is backward.
* The satisfied tuples are stored in m_currentStatus.matchItems.
* Note: If there are no satisfied items, release cr buffer timely.
*/
bool BtreeScan::FilterOutMatchTuple(ScanDirection dir, OffsetNumber offnum)
{
    BtrPage* page = GetCurrCrPage();

    int itemIndex = (dir == ScanDirection::FORWARD_SCAN_DIRECTION) ? 0 : MAX_CTID_PER_BTREE_PAGE;
    OffsetNumber minoff = page->GetLinkAndStatus()->GetFirstDataOffset();
    OffsetNumber maxoff = page->GetMaxOffset();
    bool continuescan = true;

    OffsetNumber startOff = offnum;
    if (dir == ScanDirection::FORWARD_SCAN_DIRECTION) {
        for (offnum = DstoreMax(offnum, minoff); offnum <= maxoff && continuescan; offnum = OffsetNumberNext(offnum)) {
            if (SaveItupIfNeeded(itemIndex, page, offnum, dir, continuescan)) {
                itemIndex++;
            }
        }
        StorageAssert(itemIndex <= MAX_CTID_PER_BTREE_PAGE);
        m_currentStatus.firstItem = 0;
        m_currentStatus.lastItem = static_cast<int>(itemIndex) - 1;
        m_currentStatus.itemIndex = 0;
    } else {
        for (offnum = DstoreMin(offnum, maxoff); offnum >= minoff && continuescan; offnum = OffsetNumberPrev(offnum)) {
            if (SaveItupIfNeeded(itemIndex, page, offnum, dir, continuescan)) {
                itemIndex--;
            }
        }
        m_currentStatus.firstItem = itemIndex;
        m_currentStatus.lastItem = MAX_CTID_PER_BTREE_PAGE - 1;
        m_currentStatus.itemIndex = MAX_CTID_PER_BTREE_PAGE - 1;
    }
    StorageAssert(thrd->GetActiveTransaction() != nullptr);
    int numVisible = (m_currentStatus.lastItem - m_currentStatus.firstItem) + 1;
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Visible index tuple num is %d, page (%d, %u), is most right page: %d, snapshot csn is %lu",
                  (m_currentStatus.lastItem - m_currentStatus.firstItem) + 1, page->GetSelfPageId().m_fileId,
                  page->GetSelfPageId().m_blockId, page->GetLinkAndStatus()->IsRightmost(),
                  thrd->GetActiveTransaction()->GetSnapshotCsn()));

    MarkContinueScan(page, &continuescan, dir);
    bool res = (m_currentStatus.firstItem <= m_currentStatus.lastItem);
    if (!res) {
        if (continuescan) {
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                ErrMsg("WalkRight:index(%s) " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT
                " min:%hu, max:%hu, startOff:%hu, visible:%d",
                m_indexInfo->indexRelName,
                BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus()),
                minoff, maxoff, startOff, numVisible));
        }
        UnpinScanPosIfPinned();
    }
    if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        /* compare function failed comparing */
        return false;
    }
    return res;
}

/*
* If we found no usable boundary keys, we have to start from one end of
* the tree.  Walk down that edge to the first or last key, and scan from
* there. At last, the m_currentStatus.baseBufDesc is pinned and read-locked.
*/
RetStatus BtreeScan::DownToEdge(ScanDirection dir, OffsetNumber *offnum)
{
    StorageAssert(offnum != nullptr);
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeSearchForReadLatency, false);
    timer.Start();

    BufferDesc *buf;
    *offnum = INVALID_ITEM_OFFSET_NUMBER;
    IndexTuple *itup;

    RetStatus status = GetRoot(&buf, false);
    if (STORAGE_FUNC_FAIL(status)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("find index root failed \"%s\"", m_indexInfo->indexRelName));
        return status;
    }

    if (buf == INVALID_BUFFER_DESC) {
        StorageAssert(m_currentStatus.currPage == INVALID_PAGE_ID);
        StorageAssert(m_currentStatus.baseBufDesc == INVALID_BUFFER_DESC);
        return DSTORE_SUCC;
    }

    PageId pageId = buf->GetPageId();
    BtrPage *page = static_cast<BtrPage *>(buf->GetPage());
    BtrPageLinkAndStatus *pageMeta = page->GetLinkAndStatus();
    m_currentStatus.baseBufDesc = pageMeta->GetLevel() == 0 ? buf : INVALID_BUFFER_DESC;
    m_currentStatus.currPage = pageId;

    while (page->GetLinkAndStatus()->GetLevel() > 0) {
        OffsetNumber firstDataOffset = pageMeta->GetFirstDataOffset();
        OffsetNumber maxOffset = page->GetMaxOffset();
        while (pageMeta->IsUnlinked() || firstDataOffset > maxOffset ||
               (dir == ScanDirection::BACKWARD_SCAN_DIRECTION && !pageMeta->IsRightmost())) {
            pageId = pageMeta->GetRight();
            if (unlikely(!pageId.IsValid())) {
                storage_set_error(INDEX_ERROR_MOVE_END, m_indexInfo->indexRelName);
                Xid btrCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Right page is invalid when DownToEdge. "
                    "pdb:%u, index(%s:%u), createXid(%d, %lu), segment(%hu, %u), currXid(%d, %lu), snapshot: %lu"
                    BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                    GetPdbId(), m_indexInfo->indexRelName, m_indexRel->relOid,
                    static_cast<int>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                    GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                    static_cast<int>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
                    m_snapshot.snapshotCsn, BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(pageMeta)));
                m_bufMgr->UnlockAndRelease(buf);
                m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
                m_currentStatus.currPage = INVALID_PAGE_ID;
                return DSTORE_FAIL;
            }
            if (pageId == INVALID_PAGE_ID) {
                ErrLog(DSTORE_PANIC, MODULE_INDEX,
                       ErrMsg("fell off the end of index \"%s\"", m_indexInfo->indexRelName));
            }
            buf = ReleaseOldGetNewBuf(buf, pageId, LW_SHARED);
            if (STORAGE_VAR_NULL(buf)) {
                return DSTORE_FAIL;
            }
            page = static_cast<BtrPage *>(buf->GetPage());
            pageMeta = page->GetLinkAndStatus();
        }

        *offnum = (dir == ScanDirection::FORWARD_SCAN_DIRECTION) ?
            pageMeta->GetFirstDataOffset() : page->GetMaxOffset();
        itup = page->GetIndexTuple(*offnum);
        pageId = itup->GetLowlevelIndexpageLink();

        if (pageMeta->GetLevel() == 1) {
            m_bufMgr->UnlockAndRelease(buf, BufferPoolUnlockContentFlag::DontCheckCrc());
            m_currentStatus.currPage = pageId;
            StorageAssert(pageId != INVALID_PAGE_ID);
            break;
        }

        buf = ReleaseOldGetNewBuf(buf, pageId, LW_SHARED);
        if (STORAGE_VAR_NULL(buf)) {
            return DSTORE_FAIL;
        }
        page = static_cast<BtrPage *>(buf->GetPage());
        pageMeta = page->GetLinkAndStatus();
    }

    for (;;) {
        if (STORAGE_FUNC_FAIL(MakeLeafCrPage())) {
            return DSTORE_FAIL;
        }
        page = GetCurrCrPage();
        pageMeta = page->GetLinkAndStatus();
        if (pageMeta->IsUnlinked() || (dir == ScanDirection::BACKWARD_SCAN_DIRECTION && !pageMeta->IsRightmost())) {
            if (unlikely(m_currentStatus.nextPage == INVALID_PAGE_ID)) {
                ErrLog(DSTORE_PANIC, MODULE_INDEX,
                       ErrMsg("fell off the end of leaf page \"%s\"", m_indexInfo->indexRelName));
            }
            m_currentStatus.currPage = m_currentStatus.nextPage;
            m_currentStatus.nextPage = pageMeta->GetRight();
            UnpinScanPosIfPinned();
        } else {
            break;
        }
    }
    timer.End();
    *offnum = (dir == ScanDirection::FORWARD_SCAN_DIRECTION) ? pageMeta->GetFirstDataOffset() : page->GetMaxOffset();
    return DSTORE_SUCC;
}

/*
 * down to the correct location by using binary search from root to leaf to get the position to begin scan.
 * Here we should adapt to BtreeSearch and BinarySearchOnPage,
 * cause these two function has always found the the first Item >/(>=) Scankey.
 * But sometimes we need to find the first item </(<=) Scankey when the scan direction is backward.
 * We use two variables to control the search process.
 * e.g., the index item x is sorted as: 0, 0, 1, 1, 2, 2 and we want find all items satisfy the condition: x < 1,
 * so, we leverage BtreeSearch to find the first item x >= 1, that is, 0, 0, 1, 1, 2, 2.
 *                                                                           ^
 * Then we go back(left) one offset and get the position of 0, that is, 0, 0, 1, 1, 2, 2.
 *                                                                         ^
 * At last scan bakward: 0, 0, 1, 1, 2, 2. To get all x < 1 items.
 *             scan <-----  ^
 * e.g., the index item x is sorted as: 0, 0, 1, 1, 2, 2 and we want find all items satisfy the condition: x <= 1
 * so, we leverage BtreeSearch to find the first item x > 1, that is, 0, 0, 1, 1, 2, 2.
 *                                                                                ^
 * Then we go back(left) one offset and get the position of 1, that is, 0, 0, 1, 1, 2, 2.
 *                                                                               ^
 * At last scan bakward: 0, 0, 1, 1, 2, 2. To get all x <= 1 items.
 *                   scan <-----  ^
 * All conditions can be coverd by set the variable of strictlyGreaterThanKey in BtreeSearch and the goLeft here.
 * This function is mainly to find the first position on leaf where we should start scan.
 */
RetStatus BtreeScan::DownToLoc(ScanDirection dir, OffsetNumber *offnum)
{
    StorageAssert(offnum != nullptr);
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeSearchForReadLatency);

    /* m_scanKeyValues is used to descend to the leaf. */
    StorageAssert(m_scanKeyValues.keySize != 0);
    bool strictlyGreaterThanKey = false;
    bool goLeft = false;
    *offnum = INVALID_ITEM_OFFSET_NUMBER;
    /* Only need to adjust the last strategy of last attribute. This is ensured by the rules of Makepositioningkeys.
     * See Makepositioningkeys for more details. */
    StrategyNumber lastAttrStrat = m_scanKeyValues.scankeys[m_scanKeyValues.keySize - 1].skStrategy;
    switch (lastAttrStrat) {
        case SCAN_ORDER_LESS:
            strictlyGreaterThanKey = false;
            goLeft = true;
            break;

        case SCAN_ORDER_LESSEQUAL:
            strictlyGreaterThanKey = true;
            goLeft = true;
            break;

        case SCAN_ORDER_EQUAL:
            if (dir == ScanDirection::BACKWARD_SCAN_DIRECTION) {
                strictlyGreaterThanKey = true;
                goLeft = true;
            } else {
                strictlyGreaterThanKey = false;
                goLeft = false;
            }
            break;

        case SCAN_ORDER_GREATEREQUAL:
            strictlyGreaterThanKey = false;
            goLeft = false;
            break;

        case SCAN_ORDER_GREATER:
            strictlyGreaterThanKey = true;
            goLeft = false;
            break;

        default:
            /* can't get here */
            ErrLog(DSTORE_PANIC, MODULE_INDEX,
                   ErrMsg("unrecognized StrategyNumber: %d.", static_cast<int>(lastAttrStrat)));
    }

    if (STORAGE_FUNC_FAIL(SearchFromLastAccessedPage(strictlyGreaterThanKey))) {
        return DSTORE_FAIL;
    }
    if (!IsScanPosValid() && STORAGE_FUNC_FAIL(SearchBtree(&m_currentStatus.baseBufDesc, strictlyGreaterThanKey))) {
        return DSTORE_FAIL;
    }

    /* may empty index */
    if (m_currentStatus.currPage == INVALID_PAGE_ID) {
        InvalidateScanPos();
        return DSTORE_SUCC;
    }

    if (STORAGE_FUNC_FAIL(MakeLeafCrPage())) {
        return DSTORE_FAIL;
    }
    BtrPage *crPage = GetCurrCrPage();
    *offnum = BinarySearchOnPage(crPage, strictlyGreaterThanKey);
    if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Failed to binary search btrPage(%d, %u) since compare function returns null.",
                crPage->GetSelfPageId().m_fileId, crPage->GetSelfPageId().m_blockId));
        return DSTORE_FAIL;
    }

    if (goLeft) {
        *offnum = OffsetNumberPrev(*offnum);
    }
    return DSTORE_SUCC;
}

bool BtreeScan::NeedContinueScan(ScanKey skey, bool attrIsNull, ScanDirection dir) const
{
    if (((skey->skFlags & SCANKEY_STOP_FORWARD) && dir == ScanDirection::FORWARD_SCAN_DIRECTION) ||
        ((skey->skFlags & SCANKEY_STOP_BACKWARD) && dir == ScanDirection::BACKWARD_SCAN_DIRECTION)) {
        return false;
    }
    if (!(skey->skFlags & SCAN_KEY_ISNULL) && attrIsNull) {
        if (skey->skFlags & (SCANKEY_STOP_FORWARD | SCANKEY_STOP_BACKWARD)) {
            if (((skey->skFlags & SCANKEY_NULLS_FIRST) && dir == ScanDirection::BACKWARD_SCAN_DIRECTION) ||
                (!(skey->skFlags & SCANKEY_NULLS_FIRST) && dir == ScanDirection::FORWARD_SCAN_DIRECTION)) {
                return false;
            }
        }
    }
    return true;
}

bool BtreeScan::CheckTuple(IndexTuple *tuple, uint16 tupnatts, ScanDirection dir, bool *continuescan)
{
    StorageAssert(tuple->GetKeyNum(tupnatts) == tupnatts);
    TupleDesc tupdesc;
    int keysz;
    int ikey;
    *continuescan = true;        /* default assumption */

    tupdesc = m_indexInfo->attributes;
    ScanKey key = m_scanKeyForCheck;
    keysz = m_numberOfKeys;

    if (m_checkTupleMatchFastFlag && !tuple->HasNull()) {
        return CheckTupleFast(tuple, tupnatts, continuescan);
    }

    for (ikey = 0; ikey < keysz; key++, ikey++) {
        if (key->skAttno > tupnatts) {
            StorageAssert(dir == ScanDirection::FORWARD_SCAN_DIRECTION);
            StorageAssert(tuple->IsPivot());
            continue;
        }

        bool isNull;
        Datum datum = tuple->GetAttr(key->skAttno, tupdesc, &isNull);
        if ((key->skFlags & SCAN_KEY_ISNULL) != 0U) {
            /* Handle IS NULL/NOT NULL tests */
            if (((key->skFlags & SCAN_KEY_SEARCHNULL) && isNull) ||
                ((key->skFlags & SCAN_KEY_SEARCHNOTNULL) && !isNull)) {
                continue; /* tuple satisfies this qual */
            }
            *continuescan = NeedContinueScan(key, isNull, dir);
            /* In any case, this indextuple doesn't match the qual. */
            return false;
        }

        if (isNull) {
            *continuescan = NeedContinueScan(key, isNull, dir);
            /* In any case, this indextuple doesn't match the qual. */
            return false;
        }

        if (!DatumGetBool(FunctionCall2Coll(&key->skFunc, key->skCollation, datum, key->skArgument))) {
            *continuescan = NeedContinueScan(key, isNull, dir);
            /* In any case, this indextuple doesn't match the qual. */
            return false;
        }
    }
    StorageAssert(*continuescan);
    return true;
}

Xid BtreeScan::GetItupInsertXidFromUndo(BtrPage *page, IndexTuple *tuple, uint8 insertTdId)
{
    if (insertTdId == INVALID_TD_SLOT || insertTdId >= page->GetTdCount()) {
        return FROZEN_XACT_ID;
    }
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(this->GetPdbId())->GetTransactionMgr();
    TD* td = page->GetTd(insertTdId);
    Xid xid = td->GetXid();
    UndoRecPtr undoRecPtr = td->GetUndoRecPtr();
    UndoRecord undoRecord;
    ItemPointerData ctid = tuple->GetHeapCtid();
    char *data = tuple->GetValues();
    while (true) {
        if (undoRecPtr == INVALID_ITEM_POINTER) {
            /* Undo record is recycled. */
            return FROZEN_XACT_ID;
        }
        if (STORAGE_FUNC_FAIL(transactionMgr->FetchUndoRecord(xid, &undoRecord, undoRecPtr))) {
            /* Undo record is recycled. */
            StorageClearError();
            return FROZEN_XACT_ID;
        }
        UndoType type = undoRecord.GetUndoType();
        if (type == UNDO_BTREE_INSERT || type == UNDO_BTREE_INSERT_TMP) {
            UndoDataBtreeInsert *undoRecData = static_cast<UndoDataBtreeInsert*>(undoRecord.GetUndoData());
            if (undoRecData->GetHeapCtid() == ctid &&
                strncmp(undoRecData->GetValue(), data, undoRecData->GetValueSize()) == 0) {
                return xid;
            }
        }
        /* Prepare to fetch next undo record. */
        xid = undoRecord.GetTdPreXid();
        undoRecPtr = undoRecord.GetTdPreUndoPtr();
    }
}

Xid BtreeScan::GetItupDeleteXidFromUndo(BtrPage *page, IndexTuple *tuple, uint8 &insertTdId)
{
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(this->GetPdbId())->GetTransactionMgr();
    TD* td = page->GetTd(tuple->GetTdId());
    Xid xid = td->GetXid();
    UndoRecPtr undoRecPtr = td->GetUndoRecPtr();
    UndoRecord undoRecord;
    ItemPointerData ctid = tuple->GetHeapCtid();
    char *data = tuple->GetValues();
    while (true) {
        if (undoRecPtr == INVALID_ITEM_POINTER) {
            /* Undo record is recycled. */
            insertTdId = INVALID_TD_SLOT;
            return FROZEN_XACT_ID;
        }
        if (STORAGE_FUNC_FAIL(transactionMgr->FetchUndoRecord(xid, &undoRecord, undoRecPtr))) {
            /* Undo record is recycled. */
            StorageClearError();
            insertTdId = INVALID_TD_SLOT;
            return FROZEN_XACT_ID;
        }
        UndoType type = undoRecord.GetUndoType();
        if (type == UNDO_BTREE_DELETE || type == UNDO_BTREE_DELETE_TMP) {
            IndexTuple *undoTuple = GetIndexTupleFromUndoRec(&undoRecord);
            StorageReleasePanic(undoTuple == nullptr, MODULE_INDEX,
                                ErrMsg("undoRecord is damaged. %s", undoRecord.Dump()));
            if (undoTuple->GetHeapCtid() == ctid &&
                strncmp(undoTuple->GetValues(), data, undoTuple->GetValueSize()) == 0) {
                if (undoTuple->GetTdStatus() != DETACH_TD) {
                    insertTdId = undoTuple->GetTdId();
                }
                return xid;
            }
        }
        /* Prepare to fetch next undo record. */
        xid = undoRecord.GetTdPreXid();
        undoRecPtr = undoRecord.GetTdPreUndoPtr();
    }
}

void BtreeScan::GetItupInsertAndDeleteXids(BtrPage *page, IndexTuple *tuple, Xid &insertXid, Xid &deleteXid)
{
    StorageAssert(tuple);
    TupleTdStatus status = tuple->GetTdStatus();
    if (!tuple->IsDeleted()) {
        deleteXid = INVALID_XID;
        switch (status) {
            case ATTACH_TD_AS_NEW_OWNER: {
                insertXid = page->GetTd(tuple->GetTdId())->GetXid();
                return;
            }
            case ATTACH_TD_AS_HISTORY_OWNER: {
                insertXid = GetItupInsertXidFromUndo(page, tuple, tuple->GetTdId());
                return;
            }
            case DETACH_TD: {
                insertXid = FROZEN_XACT_ID;
                return;
            }
            default: {
                StorageAssert(false);
            }
        }
    } else {
        switch (status) {
            case ATTACH_TD_AS_NEW_OWNER: {
                deleteXid = page->GetTd(tuple->GetTdId())->GetXid();
                uint8 insertTdId = INVALID_TD_SLOT;
                GetItupDeleteXidFromUndo(page, tuple, insertTdId);
                insertXid = GetItupInsertXidFromUndo(page, tuple, insertTdId);
                return;
            }
            case ATTACH_TD_AS_HISTORY_OWNER: {
                uint8 insertTdId = INVALID_TD_SLOT;
                deleteXid = GetItupDeleteXidFromUndo(page, tuple, insertTdId);
                insertXid = GetItupInsertXidFromUndo(page, tuple, insertTdId);
                return;
            }
            case DETACH_TD: {
                insertXid = deleteXid = FROZEN_XACT_ID;
                return;
            }
            default: {
                StorageAssert(false);
            }
        }
    }
}

inline bool BtreeScan::CheckTupleFast(IndexTuple *tuple, int tupnatts, bool *continuescan) const
{
    *continuescan = static_cast<bool>(!memcmp(m_fastcheckKeys, tuple->GetValues(),
        sizeof(int32) * static_cast<uint32>(DstoreMin(tupnatts, m_numberOfKeys))));
    return *continuescan;
}

void BtreeScan::MarkContinueScan(BtrPage* page, bool *continuescan, ScanDirection dir)
{
    if (!(*continuescan)) {
        dir == ScanDirection::FORWARD_SCAN_DIRECTION ? (m_currentStatus.moreRight = false) :
        (m_currentStatus.moreLeft = false);
        return;
    }
    /* trick, use highkey to avoid read more right page if possible. */
    if (dir == ScanDirection::FORWARD_SCAN_DIRECTION && !page->GetLinkAndStatus()->IsRightmost()) {
        ItemId *iid = page->GetItemIdPtr(BTREE_PAGE_HIKEY);
        IndexTuple *itup = page->GetIndexTuple(iid);
        uint16 truncatt = itup->GetKeyNum(m_indexInfo->indexKeyAttrsNum);
        bool res = CheckTuple(itup, truncatt, dir, continuescan);
        UNUSED_VARIABLE(res);
        if (!(*continuescan)) {
            m_currentStatus.moreRight = false;
        }
        return;
    }

    /* trick, even the first tuple has been deleted, the order of the related tuple is correct.
    Use it to avoid to read more left page if possible. */
    if (dir == ScanDirection::BACKWARD_SCAN_DIRECTION) {
        OffsetNumber maxoff = page->GetMaxOffset();
        OffsetNumber minoff = page->GetLinkAndStatus()->GetFirstDataOffset();
        if (maxoff < minoff) {
            return;
        }
        ItemId *iid = page->GetItemIdPtr(minoff);
        if (likely(!iid->IsUnused())) {
            IndexTuple *itup = page->GetIndexTuple(iid);
            if (itup->IsDeleted()) {
                bool res = CheckTuple(itup, m_indexInfo->indexKeyAttrsNum, dir, continuescan);
                UNUSED_VARIABLE(res);
                m_currentStatus.moreLeft = *continuescan;   /* may get false to avoid more scan */
            }
        }
        return;
    }
}

void BtreeScan::CleanLastUsedInfo()
{
    m_currentStatus.lastUsedInfo.lastUsedParentPage = INVALID_PAGE_ID;
    m_currentStatus.lastUsedInfo.lastUsedPage = INVALID_PAGE_ID;
    m_currentStatus.lastUsedInfo.isValid = false;
}

RetStatus BtreeScan::SearchFromLastAccessedPage(bool strictlyGreaterThanKey)
{
    StorageAssert(m_currentStatus.baseBufDesc == INVALID_BUFFER_DESC);
    StorageAssert(m_currentStatus.crBufDesc == INVALID_BUFFER_DESC);
    StorageAssert(m_currentStatus.currPage == INVALID_PAGE_ID);
    StorageAssert(m_currentStatus.nextPage == INVALID_PAGE_ID);

    if (!m_currentStatus.lastUsedInfo.isValid || !m_currentStatus.GetLastUsedParentPage().IsValid()) {
        return DSTORE_SUCC;
    }

    BtreePagePayload currPage;
    if (STORAGE_FUNC_FAIL(currPage.Init(GetPdbId(), m_currentStatus.GetLastUsedParentPage(), LW_SHARED, m_bufMgr))) {
        currPage.Drop(m_bufMgr);
        CleanLastUsedInfo();
        return DSTORE_SUCC;
    }
    BtrPage *parentPage = currPage.GetPage();
    BtrPageLinkAndStatus *linkAndStatus = currPage.GetLinkAndStatus();
    bool isInternalPage = linkAndStatus->TestType(BtrPageType::INTERNAL_PAGE);
    OffsetNumber minOff = parentPage->GetLinkAndStatus()->GetFirstDataOffset();
    OffsetNumber maxOff = parentPage->GetMaxOffset();
    bool notMatched = (minOff >= maxOff ||
        CompareKeyToTuple(parentPage, linkAndStatus, OffsetNumberNext(minOff), isInternalPage) < 0) ||
        (!linkAndStatus->IsRightmost() &&
        CompareKeyToTuple(parentPage, linkAndStatus, BTREE_PAGE_HIKEY, isInternalPage) >= 0);
    if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to compare tuple since compare function returns null."));
        currPage.Drop(m_bufMgr);
        return DSTORE_FAIL;
    }
    if (notMatched) {
        currPage.Drop(m_bufMgr);
        /* The last used page cache is expired. Clear cache. */
        CleanLastUsedInfo();
        return DSTORE_SUCC;
    }

    if (STORAGE_FUNC_FAIL(SearchBtreeFromInternalPage(currPage.GetBuffDesc(), &m_currentStatus.baseBufDesc,
                                                      strictlyGreaterThanKey))) {
        /* SearchBtreeFromInternalPage has already unlocked and released any page we have before return failed */
        return DSTORE_FAIL;
    }

    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeSearchHitCacheForScanLatency);
    return DSTORE_SUCC;
}

RetStatus BtreeScan::SearchBtreeFromInternalPage(BufferDesc *parentBuf, BufferDesc **leafBuf,
                                                 bool strictlyGreaterThanKey)
{
    *leafBuf = parentBuf;
    BtreePagePayload pagePayload;
    pagePayload.InitByBuffDesc(*leafBuf);
    CleanLastUsedInfo();
    while (*leafBuf != INVALID_BUFFER_DESC) {
        if (STORAGE_FUNC_FAIL(StepRightIfNeeded(leafBuf, LW_SHARED, strictlyGreaterThanKey))) {
            if (*leafBuf != INVALID_BUFFER_DESC) {
                m_bufMgr->UnlockAndRelease(*leafBuf);
            }
            m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        pagePayload.InitByBuffDesc(*leafBuf);
        if (pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)) {
            m_currentStatus.baseBufDesc = *leafBuf;
            m_currentStatus.currPage = pagePayload.GetPageId();
            m_currentStatus.lastUsedInfo.lastUsedPage = pagePayload.GetPageId();
            m_currentStatus.lastUsedInfo.isValid = true;
            StorageAssert(pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE) &&
                          (*leafBuf != INVALID_BUFFER_DESC));
            return DSTORE_SUCC;
        }
        m_currentStatus.lastUsedInfo.lastUsedParentPage = pagePayload.GetPageId();

        /* Still on internal page. need to find a downlink to descend to */
        OffsetNumber childOffset = BinarySearchOnPage(pagePayload.GetPage(), strictlyGreaterThanKey);
        if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to binary search btrPage(%d, %u) since compare function returns null.",
                    pagePayload.GetPageId().m_fileId, pagePayload.GetPageId().m_blockId));
            pagePayload.Drop(m_bufMgr, false);
            m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        IndexTuple *childTuple = pagePayload.GetPage()->GetIndexTuple(childOffset);
        if (unlikely(!childTuple->IsPivot())) {
            storage_set_error(INDEX_ERROR_TUPLE_DAMAGED);
            Btree::DumpDamagedTuple(childTuple, pagePayload.GetPage(), childOffset);
            Xid btrCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("IndexTuple is damaged. pdb:%u, index(%s:%u), "
                "createXid(%d, %lu), segment(%hu, %u), currXid(%d, %lu) ",
                GetPdbId(), m_indexInfo->indexRelName, m_indexRel->relOid,
                static_cast<int>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                static_cast<int>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId));
            pagePayload.Drop(m_bufMgr, false);
            m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        PageId childPage = childTuple->GetLowlevelIndexpageLink();

        *leafBuf = ReleaseOldGetNewBuf(*leafBuf, childPage, LW_SHARED);
        if (STORAGE_VAR_NULL(*leafBuf)) {
            m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
    }

    ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Search btree from internal failed \"%s\"", m_indexInfo->indexRelName));
    m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
    return DSTORE_FAIL;
}

RetStatus BtreeScan::SearchBtree(BufferDesc **leafBuf, bool strictlyGreaterThanKey, UNUSE_PARAM bool forceUpdate,
                                 UNUSE_PARAM bool needWriteLock, UNUSE_PARAM bool needCheckCreatedXid)
{
    if (STORAGE_FUNC_FAIL(GetRoot(leafBuf, false))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("find index root failed \"%s\"", m_indexInfo->indexRelName));
        return DSTORE_FAIL;
    }

    if (*leafBuf == INVALID_BUFFER_DESC) {
        /* No root page found/created for a reading process if btree is empty. */
        return DSTORE_SUCC;
    }

    return SearchBtreeFromInternalPage(*leafBuf, leafBuf, strictlyGreaterThanKey);
}

RetStatus BtreeScan::StepRightIfNeeded(BufferDesc **pageBuf, LWLockMode access, bool strictlyGreaterThanKey,
                                       UNUSE_PARAM bool needCheckCreatedXid)
{
    StorageAssert(*pageBuf != INVALID_BUFFER_DESC);

    BtreePagePayload currPage;
    int cmpRet = 0;
    int goRightVal = strictlyGreaterThanKey ? 0 : 1;
    while (*pageBuf != INVALID_BUFFER_DESC) {
        if (unlikely(!BtrPage::IsBtrPageValid((*pageBuf)->GetPage(), GetBtreeSmgr()->GetMetaCreateXid()))) {
            BtrPage *page = static_cast<BtrPage *>((*pageBuf)->GetPage());
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("btrPage(%hu, %u) is not valid" BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                (*pageBuf)->GetPageId().m_fileId, (*pageBuf)->GetPageId().m_blockId,
                BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));
            m_bufMgr->UnlockAndRelease(*pageBuf);
            *pageBuf = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        currPage.InitByBuffDesc(*pageBuf);
        BtrPageLinkAndStatus *linkAndStatus = currPage.GetLinkAndStatus();
        bool isInternalPage = linkAndStatus->TestType(BtrPageType::INTERNAL_PAGE);
        if (linkAndStatus->IsRightmost()) {
            break;
        }

        if (!linkAndStatus->IsUnlinked()) {
            cmpRet = CompareKeyToTuple(currPage.GetPage(), linkAndStatus, BTREE_PAGE_HIKEY, isInternalPage);
            if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to compare tuple since compare function returns null."));
                currPage.Drop(m_bufMgr);
                *pageBuf = INVALID_BUFFER_DESC;
                return DSTORE_FAIL;
            }
            if (cmpRet < goRightVal) {
                break;
            }
        }

        *pageBuf = ReleaseOldGetNewBuf(currPage.GetBuffDesc(), linkAndStatus->GetRight(), access);
        if (STORAGE_VAR_NULL(*pageBuf)) {
            return DSTORE_FAIL;
        }
    }

    if (currPage.GetLinkAndStatus()->IsUnlinked()) {
        storage_set_error(INDEX_ERROR_MOVE_END, m_indexInfo->indexRelName);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Fail scan for Reaching end in(%s), segment(%hu, %u) "
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, m_indexInfo->indexRelName,
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            BTR_PAGE_HEADER_VAL(currPage.GetPage()), BTR_PAGE_LINK_AND_STATUS_VAL(currPage.GetLinkAndStatus())));
        m_bufMgr->UnlockAndRelease(*pageBuf);
        *pageBuf = INVALID_BUFFER_DESC;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

inline bool BtreeScan::IsScanPosValid() const
{
    StorageAssert(m_currentStatus.currPage.IsValid() || m_currentStatus.baseBufDesc == INVALID_BUFFER_DESC);
    return m_currentStatus.currPage.IsValid();
}

inline void BtreeScan::UnpinScanPosIfPinned()
{
    if (m_currentStatus.crBufDesc != INVALID_BUFFER_DESC) {
        m_bufMgr->Release(m_currentStatus.crBufDesc);
        m_currentStatus.crBufDesc = INVALID_BUFFER_DESC;
        m_markStatus.crBufDesc = INVALID_BUFFER_DESC;
    }
    m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
}

inline void BtreeScan::InvalidateScanPos()
{
    m_currentStatus.baseBufDesc = INVALID_BUFFER_DESC;
    m_currentStatus.crBufDesc = INVALID_BUFFER_DESC;
    m_currentStatus.currPage = INVALID_PAGE_ID;
    m_currentStatus.nextPage = INVALID_PAGE_ID;
}

inline void BtreeScan::InitializeScanDirection(ScanDirection dir)
{
    if (dir == ScanDirection::FORWARD_SCAN_DIRECTION) {
        m_currentStatus.moreLeft = false;
        m_currentStatus.moreRight = true;
    } else {
        m_currentStatus.moreLeft = true;
        m_currentStatus.moreRight = false;
    }
}

RetStatus BtreeScan::SetShowAnyTuples(bool showAnyTuples)
{
    m_showAnyTuples = showAnyTuples;
    if (m_showAnyTuples && !m_currentStatus.matchInsertXids && !m_currentStatus.matchDeleteXids) {
        m_currentStatus.matchInsertXids = static_cast<Xid*>(DstorePalloc0(MAX_CTID_PER_BTREE_PAGE * sizeof(Xid)));
        m_currentStatus.matchDeleteXids = static_cast<Xid*>(DstorePalloc0(MAX_CTID_PER_BTREE_PAGE * sizeof(Xid)));
        if (STORAGE_VAR_NULL(m_currentStatus.matchInsertXids) || STORAGE_VAR_NULL(m_currentStatus.matchDeleteXids)) {
            DstorePfreeExt(m_currentStatus.matchInsertXids);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc0 fail when SetShowAnyTuples."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return DSTORE_FAIL;
        }
        /* Init matchInsertXids and matchDeleteXids to INVALID_XID, i.e., -1. */
        errno_t rc = memset_s(m_currentStatus.matchInsertXids, MAX_CTID_PER_BTREE_PAGE * sizeof(Xid), -1,
            MAX_CTID_PER_BTREE_PAGE * sizeof(Xid));
        storage_securec_check(rc, "\0", "\0");
        rc = memset_s(m_currentStatus.matchDeleteXids, MAX_CTID_PER_BTREE_PAGE * sizeof(Xid), -1,
            MAX_CTID_PER_BTREE_PAGE * sizeof(Xid));
        storage_securec_check(rc, "\0", "\0");
    }
    return DSTORE_SUCC;
}

void BtreeScan::DumpScanPage(Datum &fileId, Datum &blockId, Datum &data)
{
    BtrPage* page = GetCurrCrPage();
    PageId pageId = page->GetSelfPageId();
    fileId = pageId.m_fileId;
    blockId = pageId.m_blockId;
    BufferDesc *btrMetaBuf = INVALID_BUFFER_DESC;
    GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    if (btrMetaBuf == nullptr) {
        return;
    }
    BtrPage *metaPage = static_cast<BtrPage *>(btrMetaBuf->GetPage());
    data = (Datum)(page->Dump(metaPage, false));

    BufMgrInterface *bufMgr = GetBtreeSmgr()->IsGlobalTempIndex() ?
        thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr();
    char *clusterBufferInfo = bufMgr->GetClusterBufferInfo(this->GetPdbId(), fileId, blockId);
    if (likely(clusterBufferInfo != nullptr)) {
        ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("%s", clusterBufferInfo));
        DstorePfreeExt(clusterBufferInfo);
    }

    m_bufMgr->UnlockAndRelease(btrMetaBuf);
}

void BtreeScan::MarkPosition()
{
    m_markItemIndex = (IsScanPosValid()) ? m_currentStatus.itemIndex : -1;

    if (m_numArrCond) {
        /* Mark array keys */
        for (int i = 0; i < m_numArrCond; i++) {
            BtrArrayCondInfo *curArrayKey = &m_arrCondInfo[i];
            curArrayKey->markElem = curArrayKey->curElem;
        }
    }
}

void BtreeScan::RestorePosition()
{
    if (m_numArrCond) {
        /* Restore array keys */
        bool isChange = false;
        for (int i = 0; i < m_numArrCond; i++) {
            BtrArrayCondInfo *curArrayKey = &m_arrCondInfo[i];
            ScanKey skey = &m_scanKeyIncludeArry[curArrayKey->checkingKeyIdx];
            if (curArrayKey->curElem != curArrayKey->markElem) {
                curArrayKey->curElem = curArrayKey->markElem;
                skey->skArgument = curArrayKey->elemValues[curArrayKey->markElem];
                isChange = true;
            }
        }

        if (isChange) {
            ProcessScanKey(m_scanKeyIncludeArry, m_numberOfKeysIncludeArr, m_scanKeyForCheck, m_numberOfKeys, true);
        }
    }

    if (m_markItemIndex >= 0) {
        m_currentStatus.itemIndex = m_markItemIndex;
    } else {
        /* Index cross-page scanning */
        if (IsScanPosValid()) {
            UnpinScanPosIfPinned();
            InvalidateScanPos();
        }

        if (m_markStatus.currPage.IsValid()) {
            errno_t rc = memcpy_s(&m_currentStatus, sizeof(BtrScanContext),
                                  &m_markStatus, sizeof(BtrScanContext));
            storage_securec_check(rc, "", "");
        }
    }
}

#ifdef UT
void BtreeScan::GetProcessedScanKeyInfo(bool &keysConflictFlag, int &numberOfKeys, int &numArrayKeys, ScanKey &skey,
                                        bool &isCrExtend)
{
    keysConflictFlag = m_keysConflictFlag;
    numberOfKeys = m_numberOfKeys;
    numArrayKeys = m_numArrCond;
    skey = m_scanKeyForCheck;
    isCrExtend = m_currentStatus.localCrPage->GetIsCrExtend();
}
#endif

}
