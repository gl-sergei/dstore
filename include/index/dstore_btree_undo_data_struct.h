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
 * dstore_btree_undo_data_struct.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_undo_data_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DSTORE_BTREE_UNDO_DATA_STRUCT_H
#define DSTORE_DSTORE_BTREE_UNDO_DATA_STRUCT_H

#include "buffer/dstore_buf.h"
#include "common/memory/dstore_mctx.h"
#include "heap/dstore_heap_undo_struct.h"
#include "index/dstore_index_struct.h"
#include "tuple/dstore_data_tuple.h"
#include "undo/dstore_undo_record.h"

namespace DSTORE {

/*
 * undo record for index insert.
 * m_heapCtid: heap ctid which the index tuple points.
 * m_hasNull: if true, the bitmap exists.
 * m_hasVariable: if true, has variable type.
 * m_rawData: (bitmap) | index key
 */
struct UndoDataBtreeInsert : public UndoData {
    bool m_hasNull;
    bool m_hasVariable;
    bool m_ins4Del;
    uint64 m_heapCtid;
    Xid m_metaCreateXid;
    char m_rawData[];

    inline void InitBtreeInsertUndoData()
    {
        UndoData::Init();
        m_hasNull = false;
        m_hasVariable = false;
    }

    inline int32 GetSize() const
    {
        return rawDataSize + static_cast<int32>(sizeof(UndoDataBtreeInsert));
    }
    inline void AppendBitmap(uint8 *data, uint8 natts)
    {
        /* Append natts and needed bytes, other than INDEX_MAX_KEYS bits */
        UndoData::AppendUndoData(static_cast<char *>(static_cast<void *>((&natts))), sizeof(uint8),
            m_rawData, sizeof(uint8));
        uint16 size = static_cast<uint16>(DataTuple::GetBitmapLen(natts));
        UndoData::AppendUndoData(static_cast<char *>(static_cast<void *>(data)), size, m_rawData, size);
    }
    inline void AppendData(char *data, uint32 size)
    {
        UndoData::AppendUndoData(data, static_cast<uint16>(size), m_rawData, static_cast<uint16>(size));
    }
    inline uint8 *GetData()
    {
        return static_cast<uint8 *>(static_cast<void *>(m_rawData));
    }
    inline uint32 GetDataSize() const
    {
        return rawDataSize;
    }

    inline char *GetValue()
    {
        if (m_hasNull) {
            uint8 natts = *GetData();
            return static_cast<char *>((m_rawData + sizeof(uint8) + DataTuple::GetBitmapLen(natts)));
        }
        return static_cast<char *>((m_rawData));
    }
    inline uint32 GetValueSize()
    {
        if (m_hasNull) {
            uint8 natts = *GetData();
            return rawDataSize - (sizeof(uint8) + DataTuple::GetBitmapLen(natts));
        }
        return rawDataSize;
    }

    inline void SetHeapCtid(ItemPointerData ctid)
    {
        m_heapCtid = ctid.m_placeHolder;
    }
    inline ItemPointerData GetHeapCtid() const
    {
        return ItemPointerData(m_heapCtid);
    }

    inline void SetHasNull()
    {
        m_hasNull = true;
    }

    inline void SetHasVariable(bool flag)
    {
        m_hasVariable = flag;
    }

    inline void SetMetaCreateXid(Xid xid)
    {
        m_metaCreateXid = xid;
    }

    inline void SetInsertForDelete(bool ins4Del)
    {
        m_ins4Del = ins4Del;
    }

    inline Xid GetMetaCreateXid()
    {
        return m_metaCreateXid;
    }

    IndexTuple *GetIndexTuple();
} PACKED;

/*
 * undo record for index delete.
 * m_rawData: IndexTuple
 */
struct UndoDataBtreeDelete : public UndoData {
    Xid m_metaCreateXid;
    char m_rawData[];

    inline uint32 GetSize() const
    {
        return rawDataSize + sizeof(UndoDataBtreeDelete);
    }
    inline void AppendData(char *data, uint32 size)
    {
        UndoData::AppendUndoData(data, static_cast<uint16>(size), m_rawData, static_cast<uint16>(size));
    }
    inline void *GetData()
    {
        return static_cast<void *>((&m_rawData[0]));
    }
    inline uint32 GetDataSize() const
    {
        return rawDataSize;
    }
    inline void SetMetaCreateXid(Xid xid)
    {
        m_metaCreateXid = xid;
    }
    inline Xid GetMetaCreateXid()
    {
        return m_metaCreateXid;
    }
} PACKED;

struct BtrPage;
struct CRContext;

enum class BtreeUndoContextType {
    ROLLBACK = 0,
    SYN_ROLLBACK,
    ASYN_ROLLBACK,
    PAGE_ROLLBACK,
    BACKUP_RESTORE_ROLLBACK,
    CONSTRUCT_CR
};

class BtreeUndoContext : public BaseObject {
public:
    ~BtreeUndoContext() {}

    BtreeUndoContext() = delete;
    BtreeUndoContext &operator=(const BtreeUndoContext &) = delete;

    BtreeUndoContext(PdbId pdbId, BufMgrInterface *bufMgr);
    BtreeUndoContext(PdbId pdbId, PageId btrMetaPageId, IndexInfo *indexInfo, BufMgrInterface *bufMgr,
        Xid btrMetaCreateXid, BtreeUndoContextType type = BtreeUndoContextType::ROLLBACK);
    BtreeUndoContext(const BtreeUndoContext &btrUndoContext, BtreeUndoContextType type);
    BtreeUndoContext(const BtreeUndoContext &btrUndoContext) = delete;

    /* For CR construction that we'll give it up if the undo record is not on the current page. */
    bool DoesUndoRecMatchCurrCrPage();

    /* For asyn rollback, need to check btree meta page, that may reused when drop segment. */
    bool BtreeUndoMetaPageCheck(BufferDesc* bufDesc, UndoRecord *undoRec);

    /* For abort/rollback-for-recovery that we must found the undo record on some page. */
    static BtreeUndoContext *FindUndoRecRelatedPage(BtreeUndoContext *btrUndoContext, PdbId pdbId, BufferDesc *currBuf,
                                                    Xid xid, UndoRecord *undoRec, BufMgrInterface *bufMgr,
                                                    bool &needFreeOutside, UndoRecPtr undoRecPtr, bool isCommitTrx);

    /* For page rollback that we would skip rollback both tuple and td for all-same-key cases */
    RetStatus TryRollbackByUndoRec(UndoRecord &undoRecord, TD &tdOnPage, bool &pageRollbackSkip);
    /* This function would check only current page for the target undorecord related index tuple's existence */
    bool DoesUndoRecMatchCurrPage(bool &pageRollbackSkip);

    RetStatus InitWithBtrPage(BtrPage *page, BufferDesc *bufDesc = nullptr);
    void Destroy();

    RetStatus SetUndoInfo(Xid xid, UndoRecord *undoRec);
    void SetUnfoInfoWithIndexTuple(Xid xid, UndoRecord *undoRec, IndexTuple *undoTuple);
    void ClearUndoRec();

    static inline void SetRollbackAbortedForCr(BtreeUndoContext *btrUndo, bool abortedForCr)
    {
        if (STORAGE_VAR_NULL(btrUndo)) {
            return;
        }
        btrUndo->m_rollbackAbortedForCr = abortedForCr;
    }

    RetStatus GetIndexInfo();

    /* Btree index info */
    PageId          m_btrMetaPageId;
    Xid             m_metaCreateXid;
    IndexInfo       *m_indexInfo;
    bool             m_needFreeIndexInfo;

    /* Working page info */
    BufferDesc      *m_currBuf;  /* should keep nullptr for CR construction */
    BtrPage         *m_currPage;

    /* Undo tuple info */
    OffsetNumber     m_offset;
    IndexTuple      *m_undoTuple;
    bool             m_isDeletionPruned;
    bool             m_isIns4Del;

    /* Transaction info and Undo record */
    Xid              m_xid;
    UndoRecord      *m_undoRec;
    bool             m_forCommittedTxrRollback; /* for deletion undo checking. deletion tuples might be pruned after
                                                 * committed. need to check low key and high key to decide whether to
                                                 * insert the deletion back when true. Otherwise just skip if found
                                                 * no record because an uncommitted deletion would never be pruned */
    bool             m_undoWithNoWal; /* for rollback caused by failed to write undo/wal log when inserting.
                                       * the page must be restore to the original version, ignore any lowkey/hikey
                                       * changing because we have no wal to record it. */
    bool             m_endMatch;
    bool             m_needCheckMetaPage;
    bool             m_doNotRollbackDeleteOnRightPage;
    bool             m_rollbackAbortedForCr;

    /* Instance info */
    BufMgrInterface *m_bufMgr;
    PdbId            m_pdbId;

    /* Undo context type. */
    BtreeUndoContextType m_undoType;
    /* for PAGE_ROLLBACK case
     * If we can't decide if the tuple is really on the page, skip rollback both the tuple and TD */
    bool m_skipRollbackTd;

private:
    /* This function would traverse btree leaf to find the target undorecord related index tuple */
    RetStatus FindUndoRecRelatedPage(UndoRecPtr undoRecPtr);
    /* This function would find the exactly index tuple if there're more than one tuples share the same key */
    bool FindTupleOnPage(bool isDeleted);

    /* For rollback abort/failed transactions that TD would never be reused. We can check transaction status
     * by directly access the current TD slot. */
    bool DoesUndoRecMatchCurrPageForUncommittedRec(bool isDeleted, bool &pageRollbackSkip);
    /* For rollback to consistent point when backup restore that TD may has already been reused. We cannot retrieve
     * transaction status in current TD slot until we rollback TD first to the current transaction. Thus we have
     * to skip transaction status checking for now. */
    bool DoesUndoRecMatchCurrPageForCommittedRec(bool isDeleted);
};

extern IndexTuple *GetIndexTupleFromUndoRec(UndoRecord *undoRec);

}

#endif  // DSTORE_STORAGE_BTREE_UNDO_DATA_STRUCT_H
