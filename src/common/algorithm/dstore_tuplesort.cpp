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
 * IDENTIFICATION
 *        src/common/algorithm/dstore_tuplesort.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <string>
#include "common/algorithm/dstore_tuplesort.h"
#include "common/algorithm/dstore_logtape.h"
#include "common/algorithm/dstore_sorttemplate.h"
#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"
#include "catalog/dstore_fake_relation.h"
#include "catalog/dstore_typecache.h"
#include "tuple/dstore_memheap_tuple.h"
#include "errorcode/dstore_common_error_code.h"
#include "common/error/dstore_error.h"

namespace DSTORE {

constexpr int INITIAL_MEMTUPSIZE = DstoreMax(1024, BLCKSZ / sizeof(SortTuple) + 1);

constexpr int MINNUMOFTAPE = 6;
constexpr int MAXNUMOFTAPE = 500;
constexpr int TAPE_BUFFER_OVERHEAD = BLCKSZ;
constexpr int MERGE_BUFFER_SIZE = (BLCKSZ * 32);

TuplesortMgr::TuplesortMgr(const char *tmpFileNameBase, const PdbId pdbId)
    : m_uniqueCheckSucc(true), m_duplicateTuple(nullptr), m_duplicateHeapCtid1(INVALID_ITEM_POINTER),
      m_duplicateHeapCtid2(INVALID_ITEM_POINTER), m_pdbId(pdbId),
      m_status(TupSortStatus::SORT_IN_MEMORY), m_nKeys(0), m_availMem(0),
      m_allowedMem(0), m_maxNumOfTapes(0), m_mainContext(nullptr), m_sortContext(nullptr), m_tupleContext(nullptr),
      m_tapeBlockMgr(nullptr), m_memTupArray(nullptr), m_memTupArraySize(INITIAL_MEMTUPSIZE), m_memTupCount(0),
      m_memTupleArrayCanGrow(true), m_memTupReadCount(0), m_slabAllocatorUsed(false), m_slabMemoryHead(nullptr),
      m_slabMemoryTail(nullptr), m_slabFreeHead(nullptr), m_lastReturnedTuple(nullptr), m_currentRun(0),
      m_inputTapes(nullptr), m_nInputTapes(0), m_nInputRuns(0), m_outputTapes(nullptr), m_nOutputTapes(0),
      m_nOutputRuns(0), m_destTape(nullptr), m_resultTape(nullptr), m_tupDesc(nullptr), m_sortKeys(nullptr),
      m_enforceUnique(false), m_workerTupleSorts(nullptr), m_maxAggregate(0), m_nAggregatedWorkerTupleSorts(0),
      m_tableOidAtt(DSTORE_INVALID_OID)
{
    StorageAssert(tmpFileNameBase != nullptr && tmpFileNameBase[0] != '\0' &&
                  strlen(tmpFileNameBase) < MAX_SORT_TMP_FILE_BASE_NAME_LEN);
    std::fill(m_tmpFileNameBase, m_tmpFileNameBase + MAX_SORT_TMP_FILE_BASE_NAME_LEN, 0);
    errno_t ret = strncpy_s(m_tmpFileNameBase, MAX_SORT_TMP_FILE_BASE_NAME_LEN, tmpFileNameBase,
                            strlen(tmpFileNameBase));
    storage_securec_check(ret, "\0", "\0");
}

void TuplesortMgr::Clear()
{
    if (m_tapeBlockMgr != nullptr) {
        m_tapeBlockMgr->Destroy();
        delete m_tapeBlockMgr;
        m_tapeBlockMgr = nullptr;
    }
    DstorePfreeExt(m_slabMemoryHead);

    m_status = TupSortStatus::SORT_IN_MEMORY;
    m_nKeys = 0;
    m_availMem = m_allowedMem;
    m_maxNumOfTapes = 0;
    m_memTupCount = 0;
    m_memTupReadCount = 0;
    m_slabMemoryTail = nullptr;
    m_slabFreeHead = nullptr;
    m_lastReturnedTuple = nullptr;
    m_currentRun = 0;
    m_inputTapes = nullptr;
    m_nInputTapes = 0;
    m_nInputRuns = 0;
    m_outputTapes = nullptr;
    m_nOutputTapes = 0;
    m_nOutputRuns = 0;
    m_destTape = nullptr;
    m_resultTape = nullptr;
    m_tupDesc = nullptr;
    m_sortKeys = nullptr;
    m_enforceUnique = false;
    m_tableOidAtt = DSTORE_INVALID_OID;
}

void TuplesortMgr::Destroy()
{
    if (m_tapeBlockMgr) {
        m_tapeBlockMgr->Destroy();
        delete m_tapeBlockMgr;
        m_tapeBlockMgr = nullptr;
    }
    if (m_memTupArray != nullptr) {
        DstorePfree(m_memTupArray);
        m_memTupArray = nullptr;
    }
    if (m_slabMemoryHead != nullptr) {
        DstorePfree(m_slabMemoryHead);
        m_slabMemoryHead = nullptr;
    }
    if (m_outputTapes != nullptr) {
        DstorePfree(m_outputTapes);
        m_outputTapes = nullptr;
    }
    if (m_inputTapes != nullptr) {
        DstorePfree(m_inputTapes);
        m_inputTapes = nullptr;
    }
    if (m_sortKeys) {
        if (m_sortKeys->ssupExtra) {
            DstorePfree(m_sortKeys->ssupExtra);
            m_sortKeys->ssupExtra = nullptr;
        }
        DstorePfree(m_sortKeys);
        m_sortKeys = nullptr;
    }

    if (m_tupleContext) {
        DstoreMemoryContextDelete(m_tupleContext);
        m_tupleContext = nullptr;
    }
    if (m_sortContext) {
        DstoreMemoryContextDelete(m_sortContext);
        m_sortContext = nullptr;
    }
    if (m_mainContext) {
        DstoreMemoryContextDelete(m_mainContext);
        m_mainContext = nullptr;
    }
}

/*
 * Shim function for calling an old-style comparator
 */
int TuplesortMgr::ComparisonShim(Datum x, Datum y, SortSupport ssup)
{
    SortShimExtra *extra = ssup->ssupExtra;

    extra->fcinfo.arg[0] = x;
    extra->fcinfo.arg[1] = y;

    /* just for paranoia's sake, we reset isnull each time */
    extra->fcinfo.isnull = false;

    Datum result = FunctionCallInvoke(&extra->fcinfo);

    /* Check for nullptr result, since caller is clearly not expecting one */
    if (extra->fcinfo.isnull) {
        storage_set_error(COMMON_ERROR_FUNCTION_RETURN_NULL, extra->flinfo.fnOid);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("ComparisonShim failed %s", thrd->GetErrorMessage()));
        StorageAssert(0);
    }

    return DatumGetInt32(result);
}

/*
 * Set up a shim function to allow use of an old-style btree comparison
 * function as if it were a sort support comparator.
 */
RetStatus TuplesortMgr::PrepareSortSupportComparisonShim(FmgrInfo *flinfo, SortSupport ssup) const
{
    SortShimExtra *extra = static_cast<SortShimExtra *>(DstorePalloc(sizeof(SortShimExtra)));
    if (unlikely(extra == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("DstorePalloc fail when PrepareSortSupportComparisonShim."));
        return DSTORE_FAIL;
    }

    /* Lookup the comparison function */
    StorageAssert(flinfo != nullptr);
    if (unlikely(flinfo->fnAddr == nullptr)) {
        storage_set_error(TUPLESORT_ERROR_MISSING_SUPPORT_FUNCTION_FOR_FUNCOID, flinfo->fnOid);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unsuportted func with OID=%u", flinfo->fnOid));
        return DSTORE_FAIL;
    }
    /* We can initialize the callinfo just once and re-use it */
    DstoreInitFunctionCallInfoData(extra->fcinfo, &extra->flinfo, flinfo->fnNargs, ssup->ssupCollation,
                                   flinfo->fnMcxt, nullptr);
    extra->fcinfo.flinfo->fnAddr = flinfo->fnAddr;
    extra->fcinfo.flinfo->fnOid = flinfo->fnOid;
    extra->fcinfo.flinfo->fnStrict = flinfo->fnStrict;
    extra->fcinfo.flinfo->fnExtra = flinfo->fnExtra;
    extra->fcinfo.flinfo->fnExpr = flinfo->fnExpr;
    extra->fcinfo.flinfo->fn_rettype = flinfo->fn_rettype;
    extra->fcinfo.flinfo->fn_rettypemod = flinfo->fn_rettypemod;
    extra->fcinfo.flinfo->fnMcxt = flinfo->fnMcxt;
    errno_t rc = strncpy_s(extra->fcinfo.flinfo->fnName, NAMEDATALEN, flinfo->fnName, NAMEDATALEN);
    storage_securec_check(rc, "\0", "\0");

    ssup->ssupExtra = extra;
    ssup->comparator = ComparisonShim;
    return DSTORE_SUCC;
}

/*
 * Fill in SortSupport given an index relation, attribute, and strategy.
 *
 * Caller must previously have zeroed the SortSupportData structure and then
 * filled in ssupCxt, ssupAttno, ssupCollation, and ssupNullsFirst.
 * This will fill in ssupReverse (based on the supplied strategy), as well as the comparator function pointer.
 */
RetStatus TuplesortMgr::PrepareSortSupportFromIndexInfo(const IndexInfo &baseInfo, int16 strategy, SortSupport ssup)
{
    uint16_t curAttr = baseInfo.attributes->attrs[ssup->ssupAttno - 1]->attnum;
    Oid opcintype = baseInfo.opcinType[ssup->ssupAttno - 1];
    Oid collid = (ssup->ssupCollation == DSTORE_INVALID_OID) ?
                 baseInfo.attributes->attrs[ssup->ssupAttno - 1]->attcollation : ssup->ssupCollation;

    StorageAssert(ssup->comparator == nullptr);
    ssup->ssupReverse = (strategy == SCAN_ORDER_GREATER);

    /* Look for a sort support function */
    FmgrInfo fmgrInfo;
    const IndexSupportProcInfo *procInfo = baseInfo.getIndexSupportProcInfo();
    /* Look for a sort support function */
    Oid sortSupportFunction =
        g_storageInstance->GetCacheHashMgr()->GetFnOidFromArgType(opcintype, opcintype, SORT_SUPPORT);
    if (DstoreOidIsValid(sortSupportFunction)) {
        FmgrInfo flinfo;
        fill_fmgr_info(sortSupportFunction, &flinfo, ssup->ssupCxt);
        if (likely(flinfo.fnAddr != nullptr)) {
            (void)FunctionCall1Coll(&flinfo, collid, PointerGetDatum(ssup));
        }
    }

    /* if not found in SORT_SUPPORT, search in MAINTAIN_ORDER */
    if (ssup->comparator == nullptr) {
        FillProcFmgrInfo(procInfo, opcintype, curAttr, MAINTAIN_ORDER, fmgrInfo);
        if (unlikely(fmgrInfo.fnOid == DSTORE_INVALID_OID)) {
            storage_set_error(TUPLESORT_ERROR_MISSING_SUPPORT_FUNCTION_FOR_TYPE, MAINTAIN_ORDER, opcintype, opcintype);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unsuportted type with OID=%u", opcintype));
            return DSTORE_FAIL;
        }
        /* We'll use a shim to call the old-style btree comparator */
        if (STORAGE_FUNC_FAIL(PrepareSortSupportComparisonShim(&fmgrInfo, ssup))) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::InitMemoryContext()
{
    m_mainContext = DstoreAllocSetContextCreate(g_dstoreCurrentMemoryContext, "TupleSort main", ALLOCSET_DEFAULT_SIZES);
    if (STORAGE_VAR_NULL(m_mainContext))
        return DSTORE_FAIL;
    m_sortContext = DstoreAllocSetContextCreate(m_mainContext, "TupleSort sort", ALLOCSET_DEFAULT_SIZES);
    if (STORAGE_VAR_NULL(m_sortContext)) {
        DstoreMemoryContextDelete(m_mainContext);
        m_mainContext = nullptr;
        return DSTORE_FAIL;
    }
    m_tupleContext = DstoreAllocSetContextCreate(m_sortContext, "Caller tuples", ALLOCSET_DEFAULT_SIZES);
    if (STORAGE_VAR_NULL(m_tupleContext)) {
        DstoreMemoryContextDelete(m_sortContext);
        DstoreMemoryContextDelete(m_mainContext);
        m_mainContext = nullptr;
        m_sortContext = nullptr;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::PrepareSortInfo(int workMem)
{
    UNUSE_PARAM AutoMemCxtSwitch autoSwitch(m_mainContext);
    /* m_allowedMem is forced to be at least 64KB, the current minimum valid value for the work_mem GUC. */
    const int64 minValue = 64;
    const int64 kiloByte = 1024;
    m_allowedMem = DstoreMax(minValue * kiloByte,
        DstoreMin(static_cast<int64>(workMem) * kiloByte, static_cast<int64>(MaxAllocSize)));

    m_availMem = m_allowedMem;
    if (m_memTupArray == nullptr) {
        m_memTupArray =
            static_cast<SortTuple *>(DstorePalloc(static_cast<uint32>(m_memTupArraySize) * sizeof(SortTuple)));
        if (unlikely(m_memTupArray == nullptr)) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when PrepareTupleSortInfo."));
            return DSTORE_FAIL;
        }
        UseMem(static_cast<int64>(DstoreGetMemoryChunkSpace(m_memTupArray)));
    }

    /* workMem must be large enough for the minimal m_memTupArray array */
    if (LackMem()) {
        storage_set_error(TUPLESORT_ERROR_INSUFFICIENT_MEMORY_ALLOWED);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("%s", thrd->GetErrorMessage()));
        return DSTORE_FAIL;
    }

    /* Prepare SortSupport data for each column */
    m_sortKeys = static_cast<SortSupport>(DstorePalloc0(static_cast<uint32>(m_nKeys) * sizeof(SortSupportData)));
    if (unlikely(m_sortKeys == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when PrepareTupleSortInfo."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::PrepareTupleSortInfo(IndexInfo &baseInfo, int workMem, ScanKey scanKeys)
{
    m_nKeys = static_cast<int>(baseInfo.indexKeyAttrsNum);
    m_enforceUnique = baseInfo.isUnique;
    m_tupDesc = baseInfo.attributes;
    m_tableOidAtt = baseInfo.tableOidAtt;

    if (STORAGE_FUNC_FAIL(InitMemoryContext())) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(PrepareSortInfo(workMem))) {
        return DSTORE_FAIL;
    }

    UNUSE_PARAM AutoMemCxtSwitch autoSwitch(m_mainContext);
    for (int i = 0; i < m_nKeys; i++) {
        SortSupport sortKey = m_sortKeys + i;
        ScanKey     scanKey = scanKeys + i;

        sortKey->ssupCxt = m_sortContext;
        sortKey->ssupCollation = scanKey->skCollation;
        sortKey->ssupNullsFirst = (scanKey->skFlags & SCANKEY_NULLS_FIRST) != 0;
        sortKey->ssupAttno = scanKey->skAttno;

        StorageAssert(sortKey->ssupAttno != 0);

        StrategyNumber strategy = (scanKey->skFlags & SCANKEY_DESC) != 0 ? SCAN_ORDER_GREATER : SCAN_ORDER_LESS;

        if (STORAGE_FUNC_FAIL(PrepareSortSupportFromIndexInfo(baseInfo, static_cast<int16>(strategy), sortKey))) {
            baseInfo.extraInfo = Int32GetDatum(i);
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::PrepareDatumSortInfo(IndexInfo *baseInfo, int workMem, ScanKey scanKey)
{
    m_nKeys = 1;
    m_tableOidAtt = baseInfo->tableOidAtt;
    if (STORAGE_FUNC_FAIL(InitMemoryContext())) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(PrepareSortInfo(workMem))) {
        return DSTORE_FAIL;
    }

    UNUSE_PARAM AutoMemCxtSwitch autoSwitch(m_mainContext);
    SortSupport sortKey = m_sortKeys; /* we has only one sort key for datum sort */
    sortKey->ssupCxt = m_sortContext;
    sortKey->ssupCollation = scanKey->skCollation;
    sortKey->ssupNullsFirst = (scanKey->skFlags & SCANKEY_NULLS_FIRST) != 0;
    sortKey->ssupAttno = scanKey->skAttno;

    StorageAssert(sortKey->ssupAttno != 0);
    StrategyNumber strategy = (scanKey->skFlags & SCANKEY_DESC) != 0 ? SCAN_ORDER_GREATER : SCAN_ORDER_LESS;

    return PrepareSortSupportFromIndexInfo(*baseInfo, static_cast<int16>(strategy), sortKey);
}

RetStatus TuplesortMgr::GrowMemTupleArray()
{
    uint32 newMemTupSize;
    uint32 memTupSize = static_cast<uint32>(m_memTupArraySize);
    StorageAssert(m_allowedMem >= m_availMem);
    uint32 memNowUsed = static_cast<uint32>(m_allowedMem - m_availMem);

    if (!m_memTupleArrayCanGrow) {
        return DSTORE_SUCC;
    }

    /* strategy for growing: double m_memTupArraySize if memory allowed, else use all of the rest memory */
    if (memNowUsed <= m_availMem) {
        if (m_memTupArraySize < INT_MAX >> 1) {
            newMemTupSize = memTupSize << 1;
        } else {
            newMemTupSize = static_cast<uint32>(INT_MAX);
            m_memTupleArrayCanGrow = false;
        }
    } else {
        double growRatio = static_cast<double>(m_allowedMem) / static_cast<double>(memNowUsed);
        if (memTupSize * growRatio < INT_MAX) {
            newMemTupSize = static_cast<uint32>(memTupSize * growRatio);
        } else {
            newMemTupSize = static_cast<uint32>(INT_MAX);
        }
        m_memTupleArrayCanGrow = false;
    }

    if (newMemTupSize <= memTupSize) {
        /* If for any reason we didn't realloc, shut off future attempts */
        m_memTupleArrayCanGrow = false;
        return DSTORE_SUCC;
    }
    if (newMemTupSize >= static_cast<uint32>((MaxAllocSize / sizeof(SortTuple)))) {
        newMemTupSize = static_cast<uint32>(MaxAllocSize / sizeof(SortTuple));
        m_memTupleArrayCanGrow = false;
    }

    if (m_availMem < static_cast<int64>((newMemTupSize - memTupSize) * sizeof(SortTuple))) {
        /* If for any reason we didn't realloc, shut off future attempts */
        m_memTupleArrayCanGrow = false;
        return DSTORE_SUCC;
    }

    FreeMem(static_cast<int64>(DstoreGetMemoryChunkSpace(m_memTupArray)));
    m_memTupArraySize = static_cast<int>(newMemTupSize);
    m_memTupArray = static_cast<SortTuple *>(
        DstoreRepalloc(m_memTupArray, static_cast<uint32>(m_memTupArraySize) * sizeof(SortTuple)));
    if (STORAGE_VAR_NULL(m_memTupArray)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstoreRepalloc fail when GrowMemTupleArray."));
        return DSTORE_FAIL;
    }

    UseMem(static_cast<int64>(DstoreGetMemoryChunkSpace(m_memTupArray)));
    if (LackMem()) {
        storage_set_error(TUPLESORT_ERROR_UNEXPECTED_OUT_OF_MEMORY);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("%s", thrd->GetErrorMessage()));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::PutIndexTuple(IndexTuple* indexTuple)
{
    DstoreMemoryContext oldContext = DstoreMemoryContextSwitchTo(m_tupleContext);
    SortTuple stup;
    stup.tuple = indexTuple;
    UseMem(static_cast<int64>(DstoreGetMemoryChunkSpace(stup.tuple)));
    stup.datum1 = indexTuple->GetAttr(1, m_tupDesc, &stup.isnull1);
    (void)DstoreMemoryContextSwitchTo(m_sortContext);
    if (STORAGE_FUNC_FAIL(PutSortTuple(&stup))) {
        return DSTORE_FAIL;
    }
    (void)DstoreMemoryContextSwitchTo(oldContext);
    return DSTORE_SUCC;
}

void TuplesortMgr::PutDatum(Datum datum, bool isNull)
{
    SortTuple stup;
    stup.tuple = nullptr;
    stup.datum1 = datum;
    stup.isnull1 = isNull;
    UNUSE_PARAM AutoMemCxtSwitch memSwitch(m_sortContext);
    (void)PutSortTuple(&stup);
    StorageAssert(m_status == TupSortStatus::SORT_IN_MEMORY);
}

RetStatus TuplesortMgr::PutSortTuple(SortTuple *tuple)
{
    RetStatus ret = DSTORE_FAIL;
    switch (m_status) {
        case TupSortStatus::SORT_IN_MEMORY: {
            /* Save the tuple into the unsorted array. */
            if (m_memTupCount >= m_memTupArraySize - 1) {
                ret = GrowMemTupleArray();
                StorageAssert(m_memTupCount < m_memTupArraySize);
                if (STORAGE_VAR_NULL(m_memTupArray)) {
                    /* grow fail may make m_memTupArray null. */
                    return DSTORE_FAIL;
                }
                if (m_memTupCount >= m_memTupArraySize) {
                    /* if memory grow fail, persist to reduce m_memTupCount. */
                    ret = InitLogicalTapes();
                    ret = STORAGE_FUNC_SUCC(ret) ? DumpTuples(false) : ret;
                    if (STORAGE_FUNC_FAIL(ret) || unlikely(m_memTupCount >= m_memTupArraySize)) {
                        /* if fail to reduce m_memTupCount, return fail. */
                        return DSTORE_FAIL;
                    }
                }
            }
            m_memTupArray[m_memTupCount++] = *tuple;
            if (m_memTupCount < m_memTupArraySize && !LackMem()) {
                return DSTORE_SUCC;
            }
            /* switch to tape-based sort. */
            ret = InitLogicalTapes();
            ret = STORAGE_FUNC_SUCC(ret) ? DumpTuples(false) : ret;
            break;
        }
        case TupSortStatus::SORT_ON_TAPE: {
            if (STORAGE_VAR_NULL(m_memTupArray) || unlikely(m_memTupCount >= m_memTupArraySize)) {
                return DSTORE_FAIL;
            }
            m_memTupArray[m_memTupCount++] = *tuple;
            ret = DumpTuples(false);
            break;
        }
        case TupSortStatus::SORT_FINAL_MERGE:
        default:
            storage_set_error(TUPLESORT_ERROR_INVALID_TUPLESORT_STATE);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unkown tuplesort state: %d", static_cast<int32>(m_status)));
    }
    return ret;
}

RetStatus TuplesortMgr::PerformSortTuple()
{
    if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch autoMemSwitch{m_sortContext};

    RetStatus ret = DSTORE_FAIL;
    switch (m_status) {
        case TupSortStatus::SORT_IN_MEMORY:
            ret = SortMemTuples();
            break;
        case TupSortStatus::SORT_ON_TAPE:
            ret = DumpTuples(true);
            ret = STORAGE_FUNC_SUCC(ret) ? MergeRuns() : ret;
            break;
        case TupSortStatus::SORT_FINAL_MERGE:
        default:
            storage_set_error(TUPLESORT_ERROR_INVALID_TUPLESORT_STATE);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unkown tuplesort state: %d", static_cast<int32>(m_status)));
            break;
    }

    return ret;
}

RetStatus TuplesortMgr::PerformSortDatum()
{
    UNUSE_PARAM AutoMemCxtSwitch autoSwitch(m_sortContext);
    return SortMemTuples();
}

RetStatus TuplesortMgr::GetNextSortTuple(SortTuple *stup, bool &hasNext)
{
    switch (m_status) {
        case TupSortStatus::SORT_IN_MEMORY:
            StorageAssert(!m_slabAllocatorUsed);
            if (m_memTupReadCount < m_memTupCount) {
                *stup = m_memTupArray[m_memTupReadCount++];
                hasNext = true;
                return DSTORE_SUCC;
            }
            hasNext = false;
            return DSTORE_SUCC;

        case TupSortStatus::SORT_ON_TAPE:
            StorageAssert(0);
            StorageAssert(m_slabAllocatorUsed);

            /* The slot that held the tuple that we returned in previous gettuple call can now be reused. */
            if (m_lastReturnedTuple) {
                ReleaseSlabSlot(m_lastReturnedTuple);
                m_lastReturnedTuple = nullptr;
            }
            if (!hasNext) {
                return DSTORE_SUCC;
            }
            if (STORAGE_FUNC_FAIL(ReadNextSortTuple(m_resultTape, stup, hasNext))) {
                return DSTORE_FAIL;
            }
            m_lastReturnedTuple = stup->tuple;
            return DSTORE_SUCC;

        case TupSortStatus::SORT_FINAL_MERGE:
            StorageAssert(m_slabAllocatorUsed);

            /* The slab slot holding the tuple that we returned in previous gettuple call can now be reused. */
            if (m_lastReturnedTuple) {
                ReleaseSlabSlot(m_lastReturnedTuple);
                m_lastReturnedTuple = nullptr;
            }

            if (m_memTupCount > 0) {
                int         srcTapeIndex = m_memTupArray[0].srcTapeIndex;
                LogicalTape *srcTape = m_inputTapes[srcTapeIndex];
                SortTuple   newtup;

                *stup = m_memTupArray[0];

                /* Remember the tuple we return, so that we can recycle its memory on next call. */
                m_lastReturnedTuple = stup->tuple;

                if (STORAGE_FUNC_FAIL(ReadNextSortTuple(srcTape, &newtup, hasNext))) {
                    return DSTORE_FAIL;
                }
                if (hasNext) {
                    newtup.srcTapeIndex = srcTapeIndex;
                    HeapReplaceTop(&newtup);
                    hasNext = true;
                    return DSTORE_SUCC;
                } else {
                    HeapDeleteTop();
                    m_nInputRuns--;
                    srcTape->Destroy();
                    delete srcTape;
                    srcTape = nullptr;
                    hasNext = true;
                    return DSTORE_SUCC;
                }
            }
            hasNext = false;
            return DSTORE_SUCC;

        default:
            storage_set_error(TUPLESORT_ERROR_INVALID_TUPLESORT_STATE);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unkown tuplesort state: %d", static_cast<int32>(m_status)));
    }
    return DSTORE_FAIL;
}

RetStatus TuplesortMgr::GetNextIndexTuple(IndexTuple** tuple)
{
    DstoreMemoryContext oldContext = DstoreMemoryContextSwitchTo(m_sortContext);
    SortTuple   stup;
    bool hasNext = true;
    if (STORAGE_FUNC_FAIL(GetNextSortTuple(&stup, hasNext))) {
        return DSTORE_FAIL;
    }
    if (!hasNext) {
        stup.tuple = nullptr;
    }

    (void)DstoreMemoryContextSwitchTo(oldContext);
    *tuple = static_cast<IndexTuple*>(stup.tuple);
    return DSTORE_SUCC;
}

void TuplesortMgr::GetNextDatum(Datum *datum)
{
    UNUSE_PARAM AutoMemCxtSwitch autoSwitch(m_sortContext);
    SortTuple   stup;
    bool hasNext = true;
    (void)GetNextSortTuple(&stup, hasNext);
    if (!hasNext) {
        stup.datum1 = static_cast<Datum>(0);
    }
    *datum = stup.datum1;
}

int TuplesortMgr::GetNumOfTapes(int64 allowedMem) const
{
    int numOfTapes = static_cast<int>(allowedMem / (2 * TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE));
    numOfTapes = DstoreMax(numOfTapes, MINNUMOFTAPE);
    numOfTapes = DstoreMin(numOfTapes, MAXNUMOFTAPE);
    return numOfTapes;
}

/*
 * Helper function to calculate how much memory to allocate for the read buffer of each input tape in a merge pass.
 *
 * 'avail_mem' is the amount of memory available for the buffers of all the tapes, both input and output.
 * 'nInputTapes' and 'nInputRuns' are the number of input tapes and runs.
 * 'maxOutputTapes' is the max. number of output tapes we should produce.
 */
int64 TuplesortMgr::MergeReadBufferSize(int64 availMem, int nInputTapes, int nInputRuns, int maxOutputTapes) const
{
    /*
     * How many output tapes will we produce in this pass?
     *
     * This is nInputRuns / nInputTapes, rounded up.
     */
    int nOutputRuns = (nInputRuns + nInputTapes - 1) / nInputTapes;

    int nOutputTapes = DstoreMin(nOutputRuns, maxOutputTapes);

    return DstoreMax((availMem - TAPE_BUFFER_OVERHEAD * nOutputTapes) / nInputTapes, 0);
}

RetStatus TuplesortMgr::InitLogicalTapes()
{
    m_maxNumOfTapes = GetNumOfTapes(m_allowedMem);

    int64 tapeSpace = static_cast<int64>(m_maxNumOfTapes) * TAPE_BUFFER_OVERHEAD;
    if (tapeSpace + static_cast<int64>(DstoreGetMemoryChunkSpace(m_memTupArray)) < m_allowedMem) {
        UseMem(tapeSpace);
    }
    m_tapeBlockMgr = DstoreNew(g_dstoreCurrentMemoryContext) LogicalTapeBlockMgr(m_tmpFileNameBase);
    if (unlikely(m_tapeBlockMgr == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstoreNew LogicalTapeBlockMgr fail."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_tapeBlockMgr->Init())) {
        DstorePfreeExt(m_tapeBlockMgr);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Dstore init LogicalTapeBlockMgr fail."));
        return DSTORE_FAIL;
    }

    m_inputTapes = nullptr;
    m_nInputTapes = 0;
    m_nInputRuns = 0;

    m_outputTapes =
        static_cast<LogicalTape **>(DstorePalloc0(static_cast<uint32>(m_maxNumOfTapes) * sizeof(LogicalTape *)));
    if (unlikely(m_outputTapes == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when InitLogicalTapes."));
        return DSTORE_FAIL;
    }
    m_nOutputTapes = 0;
    m_nOutputRuns = 0;

    m_status = TupSortStatus::SORT_ON_TAPE;
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::SelectNewTape()
{
    if (m_nOutputTapes < m_maxNumOfTapes) {
        /* Create a new tape to hold the next run */
        StorageAssert(m_outputTapes[m_nOutputRuns] == nullptr);
        StorageAssert(m_nOutputRuns == m_nOutputTapes);
        m_destTape = DstoreNew(g_dstoreCurrentMemoryContext) LogicalTape();
        if (unlikely(m_destTape == nullptr)) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstoreNew LogicalTape fail."));
            return DSTORE_FAIL;
        }
        m_destTape->SetLogicalTapeBlockMgr(m_tapeBlockMgr);
        m_outputTapes[m_nOutputTapes] = m_destTape;
        m_nOutputTapes++;
        m_nOutputRuns++;
    } else {
        /* We have reached the max number of tapes.  Append to an existing tape. */
        StorageAssert(m_nOutputTapes > 0);
        m_destTape = m_outputTapes[m_nOutputRuns % m_nOutputTapes];
        m_nOutputRuns++;
    }
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::CheckSortOnFinalMerge(bool &finalMerge)
{
    if (m_nInputRuns <= m_nInputTapes) {
        m_tapeBlockMgr->ForgetFreeSpace();
        if (STORAGE_FUNC_FAIL(FillHeapWithFirstTupleOfEachTape())) {
            return DSTORE_FAIL;
        }
        m_status = TupSortStatus::SORT_FINAL_MERGE;
        finalMerge = true;
    }
    return DSTORE_SUCC;
}

/*
 * This implements the Balanced k-Way Merge Algorithm.  All input data has
 * already been written to initial runs on tape (see DumpTuples).
 */
RetStatus TuplesortMgr::MergeRuns()
{
    StorageAssert(m_status == TupSortStatus::SORT_ON_TAPE);
    StorageAssert(m_memTupCount == 0);

    /* We no longer need a large m_memTupArray array.  (We will allocate a smaller one for the heap later.) */
    FreeMem(static_cast<int64>(DstoreGetMemoryChunkSpace(m_memTupArray)));
    DstorePfree(m_memTupArray);
    m_memTupArray = nullptr;

    if (STORAGE_FUNC_FAIL(InitSlabAllocator(m_nOutputTapes + 1))) {
        return DSTORE_FAIL;
    }
    /* Allocate a new 'm_memTupArray' array for the heap.  It will hold one tuple from each input tape. */
    m_memTupArraySize = m_nOutputTapes;
    m_memTupArray =
        (SortTuple *) DstoreMemoryContextAlloc(m_mainContext, static_cast<uint32>(m_nOutputTapes) * sizeof(SortTuple));
    if (unlikely(m_memTupArray == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstoreMemoryContextAlloc fail when MergeRuns."));
        return DSTORE_FAIL;
    }
    UseMem(static_cast<int64>(DstoreGetMemoryChunkSpace(m_memTupArray)));

    /*
     * Use all the remaining memory we have available for tape buffers among all the input tapes.
     * At the beginning of each merge pass, we will divide this memory between the input and output tapes in the pass.
     */
    int64 tapeBufferMem = m_availMem;
    UseMem(tapeBufferMem);

    for (;;) {
        /*
         * Be sure to check for interrupts at least once per round.
         */
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent  */
            return DSTORE_FAIL;
        }
        /*
         * On the first iteration, or if we have read all the runs from the input tapes in a multi-pass merge,
         * it's time to start a new pass. Rewind all the output tapes, and make them inputs for the next pass.
         */
        if (m_nInputRuns == 0) {
            /* Previous pass's outputs become next pass's inputs. */
            m_inputTapes = m_outputTapes;
            m_nInputTapes = m_nOutputTapes;
            m_nInputRuns = m_nOutputRuns;

            /*
             * Reset output tape variables.  The actual LogicalTapes will be
             * created as needed, here we only allocate the array to hold them.
             */
            m_outputTapes =
                static_cast<LogicalTape **>(DstorePalloc0(static_cast<uint32>(m_nInputTapes) * sizeof(LogicalTape *)));
            if (unlikely(m_outputTapes == nullptr)) {
                storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc0 fail when MergeRuns."));
                return DSTORE_FAIL;
            }
            m_nOutputTapes = 0;
            m_nOutputRuns = 0;

            int64 inputBufferSize = MergeReadBufferSize(tapeBufferMem, m_nInputTapes, m_nInputRuns, m_maxNumOfTapes);

            /* Prepare the new input tapes for merge pass. */
            for (int tapenum = 0; tapenum < m_nInputTapes; tapenum++) {
                if (STORAGE_FUNC_FAIL(m_inputTapes[tapenum]->RewindForRead(static_cast<size_t>(inputBufferSize)))) {
                    return DSTORE_FAIL;
                }
            }

            /* If there's just one run left on each input tape. If we don't have to produce a materialized sorted tape,
             * we can stop at this point and do the final merge on-the-fly.
             */
            bool finalMerge = false;
            if (STORAGE_FUNC_FAIL(CheckSortOnFinalMerge(finalMerge))) {
                return DSTORE_FAIL;
            }
            if (finalMerge) {
                return DSTORE_SUCC;
            }
        }

        if (STORAGE_FUNC_FAIL(SelectNewTape())) {
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(MergeOneRun())) {
            return DSTORE_FAIL;
        }
        if (m_nInputRuns == 0 && m_nOutputRuns <= 1) {
            break;
        }
    }

    m_resultTape = m_outputTapes[0];
    if (STORAGE_FUNC_FAIL(m_resultTape->Freeze())) {
        return DSTORE_FAIL;
    }
    /* Close all the now-empty input tapes, to release their read buffers. */
    for (int tapenum = 0; tapenum < m_nInputTapes; tapenum++) {
        m_inputTapes[tapenum]->Destroy();
        delete m_inputTapes[tapenum];
        m_inputTapes[tapenum] = nullptr;
    }
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::MergeOneRun()
{
    if (STORAGE_FUNC_FAIL(FillHeapWithFirstTupleOfEachTape())) {
        return DSTORE_FAIL;
    }
    while (m_memTupCount > 0) {
        SortTuple stup;

        int srcTapeIndex = m_memTupArray[0].srcTapeIndex;
        LogicalTape *srcTape = m_inputTapes[srcTapeIndex];
        if (STORAGE_FUNC_FAIL(WriteTupleOnTape(m_destTape, &m_memTupArray[0]))) {
            return DSTORE_FAIL;
        }
        if (m_memTupArray[0].tuple) {
            ReleaseSlabSlot(m_memTupArray[0].tuple);
        }

        bool hasNext = true;
        if (STORAGE_FUNC_FAIL(ReadNextSortTuple(srcTape, &stup, hasNext))) {
            return DSTORE_FAIL;
        }
        if (hasNext) {
            stup.srcTapeIndex = srcTapeIndex;
            HeapReplaceTop(&stup);
        } else {
            HeapDeleteTop();
            m_nInputRuns--;
        }
    }

    return MarkTapeWriteEnd(m_destTape);
}

RetStatus TuplesortMgr::FillHeapWithFirstTupleOfEachTape()
{
    StorageAssert(m_memTupCount == 0);

    int activeTapes = DstoreMin(m_nInputTapes, m_nInputRuns);

    for (int srcTapeIndex = 0; srcTapeIndex < activeTapes; srcTapeIndex++) {
        SortTuple tup;
        bool hasNext = true;
        if (STORAGE_FUNC_FAIL(ReadNextSortTuple(m_inputTapes[srcTapeIndex], &tup, hasNext))) {
            return DSTORE_FAIL;
        }
        if (hasNext) {
            tup.srcTapeIndex = srcTapeIndex;
            InsertTupleIntoHeap(&tup);
        }
    }
    if (unlikely(thrd->GetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        /* compare function failed comparing */
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::ReadNextSortTuple(LogicalTape *srcTape, SortTuple *stup, bool &hasNext)
{
    unsigned int tuplen = 0;
    if (STORAGE_FUNC_FAIL(srcTape->GetNextTupleLen(tuplen))) {
        return DSTORE_FAIL;
    }
    if (tuplen == 0) {
        hasNext = false;
        return DSTORE_SUCC;
    }
    if (unlikely(tuplen > MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE || tuplen < sizeof(IndexTuple))) {
        storage_set_error(TUPLESORT_ERROR_BOGUS_TUPLE_LENGTH);
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(ReadSortTupleFromTape(stup, srcTape, tuplen))) {
        return DSTORE_FAIL;
    }
    hasNext = true;
    return DSTORE_SUCC;
}

/*
 * sort tuples in m_memTupArray and write them to tape.
 */
RetStatus TuplesortMgr::DumpTuples(bool forceAllTuples)
{
    if (m_memTupCount < m_memTupArraySize && !LackMem() && !forceAllTuples) {
        return DSTORE_SUCC;
    }
    if (m_memTupCount == 0 && m_currentRun > 0) {
        return DSTORE_SUCC;
    }

    StorageAssert(m_status == TupSortStatus::SORT_ON_TAPE);

    if (m_currentRun == INT_MAX) {
        storage_set_error(TUPLESORT_ERROR_TOO_MANY_RUNS_FOR_EXTERNAL_SORT, INT_MAX);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("%s", thrd->GetErrorMessage()));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(SelectNewTape())) {
        return DSTORE_FAIL;
    }
    m_currentRun++;

    if (STORAGE_FUNC_FAIL(SortMemTuples())) {
        return DSTORE_FAIL;
    }

    int memtupwrite = m_memTupCount;
    for (int i = 0; i < memtupwrite; i++) {
        if (STORAGE_FUNC_FAIL(WriteTupleOnTape(m_destTape, &m_memTupArray[i]))) {
            return DSTORE_FAIL;
        }
        m_memTupCount--;
    }
    DstoreMemoryContextReset(m_tupleContext);

    return MarkTapeWriteEnd(m_destTape);
}

RetStatus TuplesortMgr::SortMemTuples()
{
    StorageClearError();
    if (m_memTupCount > 1) {
        DoQuickSortTuples(m_memTupArray, static_cast<size_t>(static_cast<unsigned int>(m_memTupCount)), this);
    }
    if (unlikely(IsSortError(StorageGetErrorCode()))) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void TuplesortMgr::InsertTupleIntoHeap(SortTuple *tuple)
{
    StorageAssert(m_memTupCount < m_memTupArraySize);
    uint64 j = static_cast<uint64>(static_cast<int64>(m_memTupCount++));
    while (j > 0) {
        uint64 i = (j - 1) >> 1;

        if (CompareSortTuple(tuple, &m_memTupArray[i]) >= 0) {
            break;
        }
        m_memTupArray[j] = m_memTupArray[i];
        j = i;
    }
    m_memTupArray[j] = *tuple;
}

void TuplesortMgr::HeapDeleteTop()
{
    SortTuple  *tuple;

    if (--m_memTupCount <= 0) {
        return;
    }

    /* Remove the last tuple in the heap, and re-insert it, by replacing the current top node with it. */
    tuple = &m_memTupArray[m_memTupCount];
    HeapReplaceTop(tuple);
}

void TuplesortMgr::HeapReplaceTop(SortTuple *tuple)
{
    unsigned int i, n;
    StorageAssert(m_memTupCount >= 1);

    n = static_cast<unsigned int>(m_memTupCount);
    i = 0;
    for (;;) {
        unsigned int j = 2 * i + 1;

        if (j >= n) {
            break;
        }
        if (j + 1 < n && CompareSortTuple(&m_memTupArray[j], &m_memTupArray[j + 1]) > 0) {
            j++;
        }
        if (CompareSortTuple(tuple, &m_memTupArray[j]) <= 0) {
            break;
        }
        m_memTupArray[i] = m_memTupArray[j];
        i = j;
    }
    m_memTupArray[i] = *tuple;
}

RetStatus TuplesortMgr::MarkTapeWriteEnd(LogicalTape *tape) const
{
    unsigned int len = 0;
    return tape->Write(static_cast<void *>(&len), sizeof(len));
}

int TuplesortMgr::CompareSortTuple(const SortTuple *a, const SortTuple *b)
{
    SortSupport sortKey = m_sortKeys;
    IndexTuple* tuple1;
    IndexTuple* tuple2;
    int         keysz;
    TupleDesc   tupDes;
    bool        equalHasNull = false;
    int32       compare;
    Datum       datum1, datum2;
    bool        isnull1, isnull2;

    /* Compare the leading sort key */
    compare = ApplySortComparator(a->datum1, a->isnull1, b->datum1, b->isnull1, sortKey);
    if (compare != 0) {
        return compare;
    }

    /* Compare additional sort keys */
    tuple1 = static_cast<IndexTuple*>(a->tuple);
    tuple2 = static_cast<IndexTuple*>(b->tuple);
    keysz = m_nKeys;
    tupDes = m_tupDesc;

    if (unlikely(tuple1 == nullptr && tuple2 == nullptr)) {
        /* Datum comparing */
        return compare;
    }

    /* they are equal, so we only need to examine one nullptr flag */
    if (a->isnull1) {
        equalHasNull = true;
    }
    sortKey++;
    for (int nkey = 2; nkey <= keysz; nkey++, sortKey++) {
        StorageAssert(tuple1 != nullptr && tuple2 != nullptr);
        datum1 = tuple1->GetAttr(nkey, tupDes, &isnull1);
        datum2 = tuple2->GetAttr(nkey, tupDes, &isnull2);

        compare = ApplySortComparator(datum1, isnull1, datum2, isnull2, sortKey);
        if (compare != 0) {
            return compare; /* done when we find unequal attributes */
        }
        /* they are equal, so we only need to examine one nullptr flag */
        if (isnull1) {
            equalHasNull = true;
        }
    }

    if (m_enforceUnique && !equalHasNull) {
        m_uniqueCheckSucc = false;
        StorageAssert(tuple1 != tuple2);
        if (m_duplicateTuple == nullptr) {
            AutoMemCxtSwitch autoMemSwitch{thrd->GetSessionMemoryCtx()};
            m_duplicateTuple = tuple1->Copy();
            if (unlikely(m_duplicateTuple == nullptr && StorageGetErrorCode() == INDEX_ERROR_TUPLE_DAMAGED)) {
                Btree::DumpDamagedTuple(tuple1);
            }
            m_duplicateHeapCtid1 = tuple1->m_link.heapCtid;
            m_duplicateHeapCtid2 = tuple2->m_link.heapCtid;
        }

        storage_set_error(TUPLESORT_ERROR_COULD_NOT_CREATE_UNIQUE_INDEX);
    }

    if (m_tableOidAtt != DSTORE_INVALID_OID) {
        StorageAssert(m_tableOidAtt > 0 && m_tableOidAtt <= m_tupDesc->natts);
        Oid partOid1 = tuple1->GetAttr(m_tableOidAtt, m_tupDesc, &isnull1);
        Oid partOid2 = tuple2->GetAttr(m_tableOidAtt, m_tupDesc, &isnull2);
        StorageAssert(partOid1 != DSTORE_INVALID_OID);
        StorageAssert(partOid2 != DSTORE_INVALID_OID);
        if (partOid1 != partOid2) {
            return partOid1 < partOid2 ? -1 : 1;
        }
    }

    /*
     * If key values are equal, we sort on ItemPointer.  This is required for btree indexes, since heap TID is
     * treated as an implicit last key attribute in order to ensure that all keys in the index are physically unique.
     */
    {
        PageId blk1 = tuple1->GetLowlevelIndexpageLink();
        PageId blk2 = tuple2->GetLowlevelIndexpageLink();
        if (blk1.m_fileId == blk2.m_fileId) {
            if (blk1.m_blockId != blk2.m_blockId) {
                return (blk1.m_blockId < blk2.m_blockId) ? -1 : 1;
            }
        } else {
            return (blk1.m_fileId < blk2.m_fileId) ? -1 : 1;
        }
    }
    {
        OffsetNumber pos1 = tuple1->GetLinkOffset();
        OffsetNumber pos2 = tuple2->GetLinkOffset();
        if (pos1 != pos2) {
            return (pos1 < pos2) ? -1 : 1;
        }
    }

    return 0;
}

RetStatus TuplesortMgr::WriteTupleOnTape(LogicalTape *tape, SortTuple *stup)
{
    IndexTuple*  tuple = static_cast<IndexTuple*>(stup->tuple);
    unsigned int tuplen = 0;
    tuplen = tuple->GetSize() + sizeof(tuplen);
    if (STORAGE_FUNC_FAIL(tape->Write(static_cast<void *>(&tuplen), sizeof(tuplen)))) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(tape->Write(static_cast<void *>(tuple), tuple->GetSize()))) {
        return DSTORE_FAIL;
    }
    if (!m_slabAllocatorUsed) {
        FreeMem(static_cast<int64>(DstoreGetMemoryChunkSpace(tuple)));
        DstorePfree(tuple);
    }
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::ReadSortTupleFromTape(SortTuple *stup, LogicalTape *tape, unsigned int len)
{
    unsigned int tuplen = len - sizeof(unsigned int);
    void *tuple = nullptr;
    if (STORAGE_FUNC_FAIL(AllocSlabSlot(static_cast<void**>(&tuple), tuplen))) {
        return DSTORE_FAIL;
    }
    size_t resultSize = 0;
    if (STORAGE_FUNC_FAIL(tape->Read(tuple, tuplen, resultSize))) {
        return DSTORE_FAIL;
    }
    if (resultSize != static_cast<size_t>(tuplen)) {
        storage_set_error(TUPLESORT_ERROR_UNEXPECTED_END_OF_DATA);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("%s", thrd->GetErrorMessage()));
        return DSTORE_FAIL;
    }
    stup->tuple = tuple;
    stup->datum1 = (static_cast<IndexTuple *>(tuple))->GetAttr(1, m_tupDesc, &stup->isnull1);
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::InitSlabAllocator(int numSlots)
{
    if (numSlots > 0) {
        m_slabMemoryHead =
            static_cast<char *>(DstorePalloc(static_cast<Size>(static_cast<unsigned int>(numSlots)) * SLAB_SLOT_SIZE));
        if (unlikely(m_slabMemoryHead == nullptr)) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when InitSlabAllocator."));
            return DSTORE_FAIL;
        }
        m_slabMemoryTail = m_slabMemoryHead + static_cast<long>(numSlots) * SLAB_SLOT_SIZE;
        m_slabFreeHead = static_cast<SlabSlot *>(static_cast<void *>(m_slabMemoryHead));

        UseMem(numSlots * SLAB_SLOT_SIZE);

        char* p = m_slabMemoryHead;
        for (int i = 0; i < numSlots - 1; i++) {
            (static_cast<SlabSlot *>(static_cast<void *>(p)))->nextfree =
                static_cast<SlabSlot *>(static_cast<void *>(p + SLAB_SLOT_SIZE));
            p += SLAB_SLOT_SIZE;
        }
        (static_cast<SlabSlot *>(static_cast<void *>(p)))->nextfree = nullptr;
    } else {
        m_slabMemoryHead = m_slabMemoryTail = nullptr;
        m_slabFreeHead = nullptr;
    }
    m_slabAllocatorUsed = true;
    return DSTORE_SUCC;
}

bool TuplesortMgr::IsSlabSlot(void *tuple) const
{
    return (static_cast<char *>(tuple) >= m_slabMemoryHead && static_cast<char *>(tuple) < m_slabMemoryTail);
}

RetStatus TuplesortMgr::AllocSlabSlot(void **tuple, Size tuplen)
{
    StorageAssert(m_slabFreeHead);
    if (tuplen > SLAB_SLOT_SIZE || (m_slabFreeHead == nullptr)) {
        *tuple = DstoreMemoryContextAlloc(m_sortContext, tuplen);
        if (unlikely(*tuple == nullptr)) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstoreMemoryContextAlloc fail when AllocSlabSlot."));
            return DSTORE_FAIL;
        }
    } else {
        SlabSlot *buf = m_slabFreeHead;
        /* Reuse this slot */
        m_slabFreeHead = buf->nextfree;

        *tuple = buf;
    }
    return DSTORE_SUCC;
}

void TuplesortMgr::ReleaseSlabSlot(void *tuple)
{
    SlabSlot *buf = static_cast<SlabSlot *>(tuple);
    if (IsSlabSlot(tuple)) {
        buf->nextfree = m_slabFreeHead;
        m_slabFreeHead = buf;
    } else {
        DstorePfree(buf);
    }
}

int TuplesortMgr::ApplySortComparatorHandleNull(bool isNull1, bool isNull2, SortSupport ssup) const
{
    int compare = 0;
    if (isNull1) {
        if (isNull2) {
            compare = 0; /* NULL "=" NULL */
        } else if (ssup->ssupNullsFirst) {
            compare = -1; /* NULL "<" NOT_NULL */
        } else {
            compare = 1; /* NULL ">" NOT_NULL */
        }
    } else if (isNull2) {
        if (ssup->ssupNullsFirst) {
            compare = 1; /* NOT_NULL ">" NULL */
        } else {
            compare = -1; /* NOT_NULL "<" NULL */
        }
    }
    return compare;
}

int TuplesortMgr::ApplySortComparator(Datum datum1, bool isNull1, Datum datum2, bool isNull2, SortSupport ssup) const
{
    if (isNull1 || isNull2) {
        return ApplySortComparatorHandleNull(isNull1, isNull2, ssup);
    }
    int compare = ssup->comparator(datum1, datum2, ssup);
    if (ssup->ssupReverse) {
        InvertCompareResult(&compare);
    }
    return compare;
}

RetStatus TuplesortMgr::InitParallelScanCxt(DstoreMemoryContext parentCxt)
{
    m_mainContext = DstoreMemoryContextIsValid(parentCxt) ?
                    DstoreAllocSetContextCreate(parentCxt, "TupleSort main",
                                                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                ALLOCSET_DEFAULT_MAXSIZE, MemoryContextType::SHARED_CONTEXT) :
                    DstoreAllocSetContextCreate(g_storageInstance->GetMemoryMgr()->
                                                GetGroupContext(MEMORY_CONTEXT_QUERY),
                                                "Tuplesort main",
                                                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                ALLOCSET_DEFAULT_MAXSIZE, MemoryContextType::SHARED_CONTEXT);
    if (STORAGE_VAR_NULL(m_mainContext)) {
        return DSTORE_FAIL;
    }

    m_sortContext = DstoreAllocSetContextCreate(m_mainContext, "TupleSort sort",
                                                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                ALLOCSET_DEFAULT_MAXSIZE, MemoryContextType::SHARED_CONTEXT);
    if (STORAGE_VAR_NULL(m_sortContext)) {
        DstoreMemoryContextDelete(m_mainContext);
        m_mainContext = nullptr;
        return DSTORE_FAIL;
    }
    m_tupleContext = DstoreAllocSetContextCreate(m_sortContext, "Caller tuples",
                                                 ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                 ALLOCSET_DEFAULT_MAXSIZE, MemoryContextType::SHARED_CONTEXT);
    if (STORAGE_VAR_NULL(m_tupleContext)) {
        DstoreMemoryContextDelete(m_sortContext);
        DstoreMemoryContextDelete(m_mainContext);
        m_sortContext = nullptr;
        m_mainContext = nullptr;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::PrepareParallelSortWorker(const IndexInfo &baseInfo, int workMem,
                                                  ScanKey scanKeys, DstoreMemoryContext cxt)
{
    InitParallelScanCxt(cxt);

    m_nKeys = static_cast<int>(baseInfo.indexKeyAttrsNum);
    m_enforceUnique = baseInfo.isUnique;
    m_tupDesc = baseInfo.attributes;
    m_tableOidAtt = baseInfo.tableOidAtt;

    if (STORAGE_FUNC_FAIL(PrepareSortInfo(workMem))) {
        return DSTORE_FAIL;
    }

    UNUSE_PARAM AutoMemCxtSwitch autoSwitch(m_mainContext);

    for (int i = 0; i < m_nKeys; i++) {
        SortSupport sortKey = m_sortKeys + i;
        ScanKey     scanKey = scanKeys + i;

        sortKey->ssupCxt = m_sortContext;
        sortKey->ssupCollation = scanKey->skCollation;
        sortKey->ssupNullsFirst = (scanKey->skFlags & SCANKEY_NULLS_FIRST) != 0;
        sortKey->ssupAttno = scanKey->skAttno;

        StorageAssert(sortKey->ssupAttno != 0);

        StrategyNumber strategy = (scanKey->skFlags & SCANKEY_DESC) != 0 ? SCAN_ORDER_GREATER : SCAN_ORDER_LESS;

        if (STORAGE_FUNC_FAIL(PrepareSortSupportFromIndexInfo(baseInfo, static_cast<int16>(strategy), sortKey))) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::PrepareParallelSortMainThread(IndexInfo &baseInfo, int workMem,
                                                      ScanKey scanKeys, int numWorkers)
{
    RetStatus ret = PrepareTupleSortInfo(baseInfo, workMem, scanKeys);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Fail to initalize memory context in main thread!"));
        return DSTORE_FAIL;
    }
    m_workerTupleSorts = static_cast<TuplesortMgr **>
                         (DstorePalloc0(static_cast<uint32>(numWorkers) * sizeof(TuplesortMgr *)));
    if (unlikely(m_workerTupleSorts == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("DstorePalloc0 fail when allocate the worker tuple sort manager set."));
        return DSTORE_FAIL;
    }
    m_nAggregatedWorkerTupleSorts = 0;

    if (m_memTupArray != nullptr) {
        DstorePfree(m_memTupArray);
        m_memTupArray = nullptr;
        m_memTupCount = 0;
    }
    m_memTupArray =
        (SortTuple *) DstoreMemoryContextAlloc(m_mainContext, static_cast<uint32>(numWorkers) * sizeof(SortTuple));
    if (unlikely(m_memTupArray == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        DstorePfreeExt(m_workerTupleSorts);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstoreMemoryContextAlloc fail when initialize for main thread."));
        return DSTORE_FAIL;
    }
    UseMem(static_cast<int64>(DstoreGetMemoryChunkSpace(m_memTupArray)));
    m_maxAggregate = numWorkers;
    m_nAggregatedWorkerTupleSorts = 0;
    return DSTORE_SUCC;
}


RetStatus TuplesortMgr::Aggregate(TuplesortMgr *tuplesortMgr)
{
    if (m_nAggregatedWorkerTupleSorts == m_maxAggregate) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Max aggregation number exceed!"));
        return DSTORE_FAIL;
    }
    m_workerTupleSorts[m_nAggregatedWorkerTupleSorts++] = tuplesortMgr;
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::GetNextIndexTupleMainThread(IndexTuple **tuple)
{
    DstoreMemoryContext oldContext = DstoreMemoryContextSwitchTo(m_sortContext);
    SortTuple stup;
    stup.tuple = nullptr;
    if (m_memTupCount > 0) {
        stup = m_memTupArray[0];
    }
    (void)DstoreMemoryContextSwitchTo(oldContext);
    *tuple = static_cast<IndexTuple*>(stup.tuple);
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::ReadNextTupleFromWorkers()
{
    DstoreMemoryContext oldContext = DstoreMemoryContextSwitchTo(m_sortContext);
    SortTuple stup;
    if (m_memTupCount > 0) {
        SortTuple newtup;
        stup = m_memTupArray[0];
        bool hasNext = false;
        int workerIndex = stup.srcTapeIndex;
        if (STORAGE_FUNC_FAIL(m_workerTupleSorts[workerIndex]->GetNextSortTuple(&newtup, hasNext))) {
            ErrLog(DSTORE_WARNING, MODULE_INDEX, ErrMsg("Fail to get next sort tuple from worker: %d.", workerIndex));
            (void)DstoreMemoryContextSwitchTo(oldContext);
            return DSTORE_FAIL;
        }

        newtup.srcTapeIndex = workerIndex;

        if (hasNext) {
            HeapReplaceTop(&newtup);
        } else {
            HeapDeleteTop();
        }
    }
    (void)DstoreMemoryContextSwitchTo(oldContext);
    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::PerformSortTupleMainThread()
{
    if (STORAGE_FUNC_FAIL(FillHeapWithFirstTupleOfEachWorker())) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus TuplesortMgr::FillHeapWithFirstTupleOfEachWorker()
{
    StorageAssert(m_memTupCount == 0);
    int activeWorker = m_nAggregatedWorkerTupleSorts;

    for (int i = 0; i < activeWorker; i++) {
        SortTuple tup;
        bool hasNext = false;
        if (STORAGE_FUNC_FAIL(m_workerTupleSorts[i]->GetNextSortTuple(&tup, hasNext))) {
            return DSTORE_FAIL;
        }
        tup.srcTapeIndex = i;
        if (hasNext) {
            InsertTupleIntoHeap(&tup);
            if (unlikely(!UniqueCheckSucc())) {
                return DSTORE_FAIL;
            }
        }
    }
    return DSTORE_SUCC;
}

inline void FillIdxSortTmpFileNameBase(char *target, const PdbId pdbId, uint32 indexOid)
{
    int rc = sprintf_s(target, MAX_SORT_TMP_FILE_BASE_NAME_LEN, INDEX_SORT_TMP_FILE_BASE_PREFIX, pdbId, indexOid);
    storage_securec_check_ss(rc);
}
inline void FillParallelIdxSortTmpFileNameBase(char *target, uint32 pdbId, uint32 indexOid, int parallelNum)
{
    int rc = sprintf_s(target, MAX_SORT_TMP_FILE_BASE_NAME_LEN, INDEX_SORT_TMP_FILE_BASE_PREFIX "p%d_", pdbId, indexOid,
                       parallelNum);
    storage_securec_check_ss(rc);
}

TuplesortMgr *TuplesortMgr::CreateIdxTupleSortMgr(const PdbId pdbId, uint32 indexOid)
{
    char tmpFileNameBase[MAX_SORT_TMP_FILE_BASE_NAME_LEN] = {0};
    FillIdxSortTmpFileNameBase(tmpFileNameBase, pdbId, indexOid);
    return DstoreNew(g_dstoreCurrentMemoryContext) TuplesortMgr(tmpFileNameBase, pdbId);
}

TuplesortMgr *TuplesortMgr::CreateParallelIdxTupleSortMgr(const PdbId pdbId, uint32 indexOid, int parallelThrdNum)
{
    char tmpFileNameBase[MAX_SORT_TMP_FILE_BASE_NAME_LEN] = {0};
    FillParallelIdxSortTmpFileNameBase(tmpFileNameBase, pdbId, indexOid,
                                       parallelThrdNum);
    return DstoreNew(g_dstoreCurrentMemoryContext) TuplesortMgr(tmpFileNameBase, pdbId);
}

/*
 * Delete all temporary index files under gs_pdb.
 * This function should be called when the dstore starts,
 * because it deletes all temporary files instead of individual indexes.
 * Note that even if the deletion fails, there is no impact because temporary files of the same index can be
 * reused.
 */
void TuplesortMgr::DeleteAllIdxSortTmpFile()
{
    DIR *dir;
    struct dirent *entry;
    char basePath[MAXPGPATH];
    int rc =
        snprintf_s(basePath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", g_storageInstance->GetGuc()->dataDir, PDB_BASE_PATH);
    storage_securec_check_ss(rc);
    ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("Try to delete all index sort tmp file under %s.", basePath));
    if ((dir = opendir(basePath)) != NULL) {
        char filePath[MAXPGPATH];
        while ((entry = readdir(dir)) != NULL) {
            /* Ignore"." and ".." directory entries */
            if (entry->d_name[0] == '.')
                continue;

            /* Check whether the file name starts with idxtmp */
            if (std::string(entry->d_name).find(INDEX_BASE_PREFIX) != 0) {
                continue;
            }
            rc = snprintf_s(filePath, MAXPGPATH, MAXPGPATH - 1, "%s%s", basePath, entry->d_name);
            storage_securec_check_ss(rc);
            struct stat fileStat;
            if (stat(filePath, &fileStat) == 0 && S_ISREG(fileStat.st_mode)) {
                /* If it is a normal file, delete it. */
                ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("Try to delete index sort tmp file %s.", filePath));
                (void)unlink(filePath);
            }
        }
        closedir(dir);
    } else {
        ErrLog(DSTORE_WARNING, MODULE_INDEX,
               ErrMsg("Fail to open %s when delete all index sort tmp file.", PDB_BASE_PATH));
    }
}
}
