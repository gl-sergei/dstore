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
 * dstore_btree_incomplete_index_for_ccindex.h.h
 *
 * IDENTIFICATION
 *        dstore/include/index/concurrent/dstore_btree_incomplete_index_for_ccindex.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_INCOMPLETE_INDEX_FOR_CCINDEX_H
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_INCOMPLETE_INDEX_FOR_CCINDEX_H

#include "index/dstore_btree_delete.h"
#include "index/dstore_btree_insert.h"
#include "index/concurrent/dstore_ccindex_btree_build_handler.h"
#include "index/concurrent/dstore_btree_delta_index_for_ccindex.h"

namespace DSTORE {

class IncompleteBtreeDeleteForCcindex final : public BtreeDelete {
public:
    ~IncompleteBtreeDeleteForCcindex() final {};

    IncompleteBtreeDeleteForCcindex() = delete;
    IncompleteBtreeDeleteForCcindex(const IncompleteBtreeDeleteForCcindex &) = delete;
    IncompleteBtreeDeleteForCcindex &operator=(const IncompleteBtreeDeleteForCcindex &) = delete;

    IncompleteBtreeDeleteForCcindex(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKey,
                                    bool forMerge = false);

    /**
     * Merge a deletion record form DeltaDmlTable to a Btree.
     *
     * This function can only be called within a concurrently create index/concurrently reindex transaction that
     * the BtrCcidxStatus (btree concurrently create index status) == WRITE_ONLY_INDEX.
     *
     * In this function we try delete the tuple recorded in DeltaDmlTable from the concurrently built btree structure.
     * The difference with the normal BtreeDelete::DeleteTuple function is that we'll just skip if no to-be-deleted
     * tuple were found and return success.
     *
     * @param handler ccindex build handler. we'll only one CcindexBtrBuildHandler object for each ccindex create.
     * @param values index tuple values deformed from DeltaDmlTable record.
     * @param isnull index tuple isnull flags deformed from DeltaDmlTable record.
     * @param heapCtid heap tuple position that the index tuple pointing to, deformed from DeltaDmlTable record.
     */
    static RetStatus MergeDeltaDeletionToBtree(CcindexBtrBuildHandler *handler,
                                               Datum *values, bool *isnull, ItemPointer heapCtid);

    RetStatus DeleteTuple(Datum *values, bool *isnull, ItemPointer ctid, CommandId cid = INVALID_CID) override;

    bool m_needSkip;
    bool m_needInsertForDel;

private:
    enum class DeleteType : uint8 {
        CONCURRENT_DML_DELETE = 0U,
        DELTA_MERGE_DELETE
    };

    RetStatus FindDeleteLocForMergeDelta(Xid *waitXid);
    RetStatus FindDeleteLocForConcurrentDml(Xid *waitXid);
    RetStatus StepRightForDeleteLocIfNeeded();

    DeleteType m_deleteType;
};

class IncompleteBtreeInsertForCcindex final : public BtreeInsert {
public:
    ~IncompleteBtreeInsertForCcindex() final {};

    IncompleteBtreeInsertForCcindex() = delete;
    IncompleteBtreeInsertForCcindex(const IncompleteBtreeInsertForCcindex &) = delete;
    IncompleteBtreeInsertForCcindex &operator=(const IncompleteBtreeInsertForCcindex &) = delete;

    IncompleteBtreeInsertForCcindex(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKey,
                                    bool forMerge = false);

    /**
     * MergeDeltaInsertionToBtree: Merge an insertion record form DeltaDmlTable to a Btree.
     *
     * This function can only be called within a concurrently create index/concurrently reindex transaction that
     * the BtrCcidxStatus (btree concurrently create index status) == WRITE_ONLY_INDEX.
     *
     * In this function we try insert a tuple from DeltaDmlTable to the concurrently built btree structure.
     * The difference with the normal BtreeInsert::InsertTuple function is that we have to solve the potential
     * heapCtid conflict caused by concurrently DML.
     *
     * @param handler ccindex build handler. we have only one CcindexBtrBuildHandler object for each ccindex create.
     * @param values index tuple values deformed from DeltaDmlTable record.
     * @param isnull index tuple isnull flags deformed from DeltaDmlTable record.
     * @param heapCtid heap tuple position that the index tuple pointing to, deformed from DeltaDmlTable record.
     */
    static RetStatus MergeDeltaInsertionToBtree(CcindexBtrBuildHandler *handler,
                                                Datum *values, bool *isnull, ItemPointer heapCtid);

    /**
     * Insert a record to the Btree if we failed to find the deleting target when DELETION from an index that is
     * under concurrently create/concurrently reindex.
     *
     * This function can be called when delete an index tuple from the btree index that is under the third phase
     * (merging DeltaDmlTable phase) of concurrently create/concurrently reindex(BtrCcidxStatus == WRITE_ONLY_INDEX)
     * while the DML transaction is not the ccindex transaction itself.
     *
     * The concurrent DELETION that happened when ccindex is under merging phase may fail to find its deleting target
     * because of a postponed delta DML merging that a committed insertion may be still in the DeltaDmlTable waiting
     * for merging to the btree, so that we cannot find the record in btree until the merging finished.
     *
     * To deal with the deletion of a postponed merging insertion, we will directly insert the deleted index tuple to
     * the btree structure in this function, and skip the later insertion if heapCtid comflict detected by
     * BtreeInsertForCcindex::InsertTuple.
     *
     * Note 1. we won't do unique check for unique index in this function because it's not a real insertion.
     * We'll have a postponed unique check when merging the committed insertion in the function
     * BtreeInsertForCcindex::InsertTuple just in case the current deletion transaction failed/aborted and make
     * the deletion rollback.
     *
     * Note 2. The deletion MERGE may also fail to find its deleting target if the insertion of the target was
     * also recorded in the DeltaDmlTable, and the final operation -- the deletion we're dealing with -- can
     * be kept. For this case, since the deletion is already committed and visible to all, we can just skip merging.
     *
     * @param btrDel BtreeDelete object of deleting
     */
    static RetStatus InsertTuple4Delete(IncompleteBtreeDeleteForCcindex *btrDel);

    RetStatus InsertTuple(Datum *values, bool *isnull, ItemPointer heapCtid, bool *satisfiesUnique = nullptr,
                          CommandId cid = INVALID_CID) override;

    bool m_needSkip;
    bool m_needEraseIns4DelFlag;

private:
    enum class InsertType : uint8 {
        CONCURRENT_DML_INSERT = 0U,
        DELTA_MERGE_INSERT
    };

    void InitSearchingKeys() override;
    RetStatus CheckConflict() override;

    RetStatus CheckConflictForMergeDelta(Xid *waitXid);
    RetStatus CheckConflictForConcurrentDml(Xid *waitXid);
    RetStatus StepRightForConlictCheckIfNeeded(BufferDesc **rightBuf, BtrPage **currPage, OffsetNumber &checkOff);

    void EraseIns4DelFlag();

    RetStatus InitInsertInfoForConcurrentDelete(IncompleteBtreeDeleteForCcindex *btrDel);
    bool IsLastAccessedPageValid();

    InsertType m_insertType;
};

}
#endif /* SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_DELTA_INDEX_FOR_CCINDEX_H */