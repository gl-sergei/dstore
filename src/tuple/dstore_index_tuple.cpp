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
 * dstore_index_tuple.cpp
 *
 * Description: this file provides the facilities to adapt to the external VFS library.
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include "tuple/dstore_index_tuple.h"
#include "index/dstore_btree.h"
#include "tuple/dstore_memheap_tuple.h"
#include "catalog/dstore_typecache.h"
#include "errorcode/dstore_tuple_error_code.h"

namespace DSTORE {

inline void CompareInputCheck(IndexTuple *tuple1, IndexTuple *tuple2, __attribute__((__unused__)) int keyNum1,
                              __attribute__((__unused__)) int keyNum2)
{
    /* Check input */
    if (!tuple1->IsPivot() && tuple2->IsPivot()) {
        /* Situation 1 */
        StorageAssert(keyNum1 >= keyNum2);
    } else if (!tuple1->IsPivot() && !tuple2->IsPivot()) {
        /* Situation 2 */
        StorageAssert(keyNum1 == keyNum2);
    } else {
        StorageAssert(0);
    }
}

/*
 * Compare two tuples, returns:
 *   <0 if tuple1 < tuple2;
 *   =0 if tuple1 = tuple2;
 *   >0 if tuple1 > tuple2.
 *
 * We use this function in two situations:
 * 1. In BtrPage::DoesUndoRecMatchCurrPage, whether the tuple1 should rollback in a btree page,
 *    tuple1 is generated from undo record., tuple2 is the page high key.
 * 2. In BtrPage::UndoBtreeInsert/Delete, tuple1 is generated from undo
 *    record, if tuple1 should rollback in a splitted leaf page, we should find
 *    correct offset using binary search in this page.
 */
int IndexTuple::Compare(IndexTuple *tuple1, IndexTuple *tuple2, IndexInfo *indexInfo)
{
    StorageAssert(indexInfo != nullptr);
    int result = 0;
    TupleDesc tupleDesc = indexInfo->attributes;
    int16 *indoption = indexInfo->indexOption;
    uint16 nkeyatts = indexInfo->indexKeyAttrsNum;
    int keyNum1 = tuple1->GetKeyNum(nkeyatts);
    int keyNum2 = tuple2->GetKeyNum(nkeyatts);
    int minKeyNum = DstoreMin(keyNum1, keyNum2);

    CompareInputCheck(tuple1, tuple2, keyNum1, keyNum2);

    for (int i = 0; i < minKeyNum; ++i) {
        bool isNull1;
        bool isNull2;
        Datum datum1 = tuple1->GetAttr(i + 1, tupleDesc, &isNull1);
        Datum datum2 = tuple2->GetAttr(i + 1, tupleDesc, &isNull2);
        if (isNull1) {
            if (isNull2) {
                result = 0;     /* NULL "=" NULL */
            } else if ((static_cast<uint16>(indoption[i]) & INDEX_OPTION_NULLS_FIRST) != 0) {
                result = -1;    /* NULL "<" NOT_NULL */
            } else {
                result = 1;     /* NULL ">" NOT_NULL */
            }
        } else if (isNull2) {
            if ((static_cast<uint16>(indoption[i]) & INDEX_OPTION_NULLS_FIRST) != 0) {
                result = 1;     /* NOT_NULL ">" NULL */
            } else {
                result = -1;    /* NOT_NULL "<" NULL */
            }
        } else {
            FmgrInfo flinfo;
            Oid typeOid = indexInfo->opcinType[i];
            Oid attColId = tupleDesc->attrs[i]->attcollation;
            FillProcFmgrInfo(indexInfo->m_indexSupportProcInfo, typeOid, i + 1, MAINTAIN_ORDER, flinfo);
            StorageReleasePanic(flinfo.fnOid == DSTORE_INVALID_OID, MODULE_INDEX,
                                ErrMsg("Failed to get compare function for type %u when UNDO.", typeOid));
            result = DatumGetInt32(FunctionCall2Coll(&flinfo, attColId, datum2, datum1));
            if (!(static_cast<uint16>(indoption[i]) & INDEX_OPTION_DESC)) {
                InvertCompareResult(&result);
            }
        }

        if (result != 0) {
            return result;
        }
    }

    /*
     * All non-truncated attributes (other than heap TID) were found to be
     * equal.  Treat truncated attributes as minus infinity. So if tuple
     * key num > highkey key num, tuple > highkey.
     */
    if (keyNum1 > keyNum2) {
        /* Regardless of descend or ascend, here tuple1 is on the right of tuple2, so return 1 */
        return 1;
    }

    StorageAssert(result == 0);

    /* If tuple1 and tuple2 have same key, compare partition oid for global partition index */
    if (indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX)) {
        Oid tableOid1 = tuple1->GetTableOid(indexInfo);
        Oid tableOid2 = tuple2->GetTableOid(indexInfo);

        StorageAssert(tableOid1 != DSTORE_INVALID_OID);
        if (tableOid1 < tableOid2) {
            return -1;
        }

        if (tableOid1 > tableOid2) {
            return 1;
        }
    }

    /* If tuple1 and tuple2 have same key, compare heap ctid */
    ItemPointerData ctid1 = tuple1->GetHeapCtid();
    ItemPointerData ctid2 = tuple2->GetHeapCtid();

    StorageAssert(ctid1 != INVALID_ITEM_POINTER);
    /* In case we do not need heap ctid, eg compare to the high key, same means do not belong to this page */
    if (ctid2 == INVALID_ITEM_POINTER) {
        return 1;
    }

    return ItemPointerData::Compare(&ctid1, &ctid2);
}

IndexTuple *IndexTuple::FormTuple(TupleDesc tupleDescriptor, Datum *values, bool *isnull, uint32 *tupleSize)
{
    if (STORAGE_VAR_NULL(tupleDescriptor) || STORAGE_VAR_NULL(values) || STORAGE_VAR_NULL(isnull)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Invalid input param when form index tuple"));
        return nullptr;
    }
    char *tp = nullptr;          /* tuple pointer */
    IndexTuple* tuple = nullptr; /* return tuple */
    Size size, dataSize, hoff;
    int i;
    bool hasnull = false;
    int attributeNum = tupleDescriptor->natts;

    if (attributeNum > INDEX_MAX_KEY_NUM) {
        storage_set_error(TUPLE_ERROR_TOO_MANY_COLUMNS, attributeNum, INDEX_MAX_KEY_NUM);
    }

    for (i = 0; i < attributeNum; i++) {
        if (isnull[i]) {
            hasnull = true;
            break;
        }
    }

    hoff = IndexTuple::GetDataOffset(hasnull);
    dataSize = ComputeDataSize(tupleDescriptor, values, isnull);
    size = hoff + dataSize;
    if (likely(tupleSize != nullptr)) {
        *tupleSize = static_cast<uint32>(size);
    }
    if (unlikely(size > MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE)) {
        storage_set_error(INDEX_ERROR_FAIL_FOR_HUGE_INDEX_TUPLE, size, MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("tuple size %zu is greater than btree page max available space %u",
                                                  size, MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE));
        return nullptr;
    }

    /* Must use DstorePalloc0 for IndexTuple allocate to set default flags as 0 */
    tp = static_cast<char *>(DstorePalloc0(size));
    if (unlikely(tp == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("FormTuple Failed to alloc index tuple."));
        return nullptr;
    }
    tuple = static_cast<IndexTuple*>(static_cast<void*>(tp));
    if (likely(!hasnull)) {
        DataTuple::AssembleData<false, IndexTuple>(tupleDescriptor, values, isnull, tp, hoff + dataSize);
    } else {
        tuple->SetHasNull();
        DataTuple::AssembleData<true, IndexTuple>(tupleDescriptor, values, isnull, tp, hoff + dataSize);
    }

    tuple->SetSize(static_cast<uint32>(size));
    return tuple;
}

IndexTuple *IndexTuple::FormTuple(HeapTuple *heapTuple, IndexBuildInfo *info, Datum *values,
                                  bool *isnulls, uint32 *tupleSize)
{
    for (uint16 i = 0; i < info->baseInfo.indexAttrsNum; i++) {
        int attnum = info->indexAttrOffset[i];
        if (attnum != 0) {
            /* Plain index column, get the value we need directly from the heap tuple. */
            values[i] = heapTuple->GetAttr(attnum, info->heapAttributes, isnulls + i);
        }
    }
    return FormTuple(info->baseInfo.attributes, values, isnulls, tupleSize);
}

void IndexTuple::CheckAttIsNull(int attnum, bool &slow)
{
    /*
     * there's a null somewhere in the tuple
     *
     * check to see if desired att is null
     */
    if (!HasNull()) {
        return;
    }
    /* XXX "knows" t_bits are just after fixed tuple header! */
    uint8 *bp = GetNullBitmap(); /* ptr to null bitmap in tuple */
    if (unlikely(bp == nullptr)) {
        return;
    }

    /* Now check to see if any preceding bits are null... */
    uint32 byte = static_cast<uint32>(attnum) >> 3;
    uint32 finalbit = static_cast<uint32>(attnum) & 0x07;

    /* check for nulls "before" final bit of last byte */
    if (byte < static_cast<uint32>(GetBitmapLen(INDEX_MAX_KEY_NUM)) &&
        (~bp[byte]) & ((static_cast<uint>(1) << finalbit) - 1)) {
        slow = true;
        return;
    }
    /* check for nulls in any "earlier" bytes */
    for (uint32 i = 0; i < byte; i++) {
        if (bp[i] != 0xFF) {
            slow = true;
            break;
        }
    }
}

Datum IndexTuple::GetAttrNocache(int attnum, TupleDesc desc)
{
    Form_pg_attribute *att = desc->attrs;
    char *tp = nullptr;   /* ptr to data part of tuple */
    bool slow = false;    /* do we have to walk attrs? */
    int off;              /* current offset within data */
    int loboff;

    /*
     * Three cases:
     *   1: No nulls and no variable-width attributes.
     *   2: Has a null or a var-width AFTER att.
     *   3: Has nulls or var-widths BEFORE att.
     */

    /* minus for array offset , very important */
    attnum--;

    CheckAttIsNull(attnum, slow);
    tp = GetValues();

    /*
     * If we get here, there are no nulls up to and including the target
     * attribute.  If we have a cached offset, we can use it.
     */
    if (!slow && att[attnum]->attcacheoff >= 0) {
        return FetchAtt(att[attnum], tp + att[attnum]->attcacheoff);
    }

    /*
     * Otherwise, check for non-fixed-length attrs up to and including
     * target.  If there aren't any, it's safe to cheaply initialize the
     * cached offsets for these attrs.
     */
    if (!slow && HasVariable()) {
        for (int j = 0; j <= attnum; j++) {
            if (att[j]->attlen <= 0) {
                slow = true;
                break;
            }
        }
    }
    CalculateOffsetSlowContext context = {attnum, desc->natts, att, off, tp, false, loboff};
    slow ? CalculateOffsetSlow<IndexTuple>(context) : CalculateOffset<IndexTuple>(attnum, desc->natts, att, off);
    return FetchAtt(att[attnum], tp + off);
}

IndexTuple *IndexTuple::Truncate(TupleDesc desc, int keepNatt)
{
    TupleDesc truncdesc;
    Datum values[INDEX_MAX_KEY_NUM];
    bool isnull[INDEX_MAX_KEY_NUM];
    IndexTuple* truncated;
    uint32 tupleSize = BLCKSZ;

    /* Easy case: no truncation actually required */
    if (keepNatt == desc->natts) {
        return Copy();
    }

    /* Create temporary descriptor to scribble on */
    truncdesc = CopyTupleDesc(desc);
    if (STORAGE_VAR_NULL(truncdesc)) {
        return nullptr;
    }
    truncdesc->natts = keepNatt;

    /* Deform, form copy of tuple with fewer attributes */
    DeformTuple(truncdesc, values, isnull);
    truncated = FormTuple(truncdesc, values, isnull, &tupleSize);
    if (STORAGE_VAR_NULL(truncated)) {
        return nullptr;
    }
    StorageAssert(tupleSize <= MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE);
    truncated->SetHeapCtid(&m_link.heapCtid);
    StorageAssert(truncated->GetSize() <= GetSize());

    /*
     * Cannot leak memory here, TupleDescCopy() doesn't allocate any inner
     * structure, so, plain DstorePfree() should clean all allocated memory
     */
    DstorePfree(truncdesc);

    return truncated;
}

}

namespace TupleInterface {
using namespace DSTORE;
IndexTuple *FormIndexTuple(TupleDesc tupleDescriptor, Datum *values, bool *isNulls)
{
    AutoMemCxtSwitch autoMcxtSwitch{thrd->GetQueryMemoryContext()};
    UNUSE_PARAM uint32 tupleSize;
    IndexTuple *indexTuple = IndexTuple::FormTuple(tupleDescriptor, values, isNulls, &tupleSize);
    StorageAssert(tupleSize <= MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE);
    return indexTuple;
}

Datum GetIndexAttr(IndexTuple *tuple, int attNum, TupleDesc desc, bool *isNull)
{
    return tuple->GetAttr(attNum, desc, isNull);
}

void DeformIndexTuple(IndexTuple *tuple, TupleDesc tupleDesc, Datum *values, bool *isNulls)
{
    return tuple->DeformTuple(tupleDesc, values, isNulls);
}

DSTORE::RetStatus IsIndexTupleValueEqual(IndexTuple *tuple1, IndexTuple *tuple2)
{
    if (unlikely(tuple1->GetSize() != tuple2->GetSize())) {
        return DSTORE_FAIL;
    }
    return memcmp(tuple1->GetValues(), tuple2->GetValues(), tuple1->GetValueSize()) == 0 ? DSTORE_SUCC : DSTORE_FAIL;
}
}
