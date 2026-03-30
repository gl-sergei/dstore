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
 * dstore_heap_insert.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/heap/dstore_heap_insert.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "heap/dstore_heap_insert.h"
#include "common/log/dstore_log.h"
#include "common/dstore_datatype.h"
#include "errorcode/dstore_heap_error_code.h"
#include "heap/dstore_heap_wal_struct.h"
#include "heap/dstore_heap_undo_struct.h"
#include "heap/dstore_heap_prune.h"
#include "page/dstore_itemptr.h"
#include "wal/dstore_wal_write_context.h"
#include "transaction/dstore_transaction_mgr.h"
#include "logical_replication/dstore_decode_dict.h"

namespace DSTORE {

constexpr int SCALE_TIMES = 2;

HeapInsertHandler::HeapInsertHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                                     bool isLobOperation, bool isUseRingBuf /* = false */)
    : HeapHandler(instance, thread, heapRel, isLobOperation, isUseRingBuf), m_tupIsSplited(false)
{
    m_tupChunks.m_chunkNum = 0;
    m_tupChunks.m_chunksSize = nullptr;
    m_tupChunks.m_chunksData = nullptr;
    if (m_useRingBuf) {
        constexpr int32 retryMax = 1000;
        constexpr long sleepTime = 1000;
        int32 retryCnt = 0;
        m_ringBuf = CreateBufferRing(BufferAccessType::BAS_BULKWRITE);
        while (!m_ringBuf) {
            StorageExit0(retryCnt++ > retryMax, MODULE_HEAP, ErrMsg("No memory for m_ringBuf."));
            GaussUsleep(sleepTime);
            m_ringBuf = CreateBufferRing(BufferAccessType::BAS_BULKWRITE);
        }
    }
}

HeapInsertHandler::~HeapInsertHandler()
{
    if (m_ringBuf != nullptr) {
        DestroyBufferRing(&m_ringBuf);
        StorageReleasePanic(m_ringBuf != nullptr, MODULE_HEAP, ErrMsg("Buffer ring is not null."));
    }
}

RetStatus HeapInsertHandler::Insert(HeapInsertContext *insertContext)
{
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    /* Step 1: Heap begin insert */
    if (STORAGE_FUNC_FAIL(BeginInsert(insertContext))) {
        ClearTupChunks(insertContext->heapTuple);
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("Begin insert failed, ctid({%hu, %u}, %hu)", insertContext->ctid.GetFileId(),
                      insertContext->ctid.GetBlockNum(), insertContext->ctid.GetOffset()));
        return DSTORE_FAIL;
    }

    /* Step 2: Put tuple into buffer page */
    if (likely(!HeapPage::TupBiggerThanPage(insertContext->heapTuple))) {
        if (STORAGE_FUNC_FAIL(InsertSmallTuple(insertContext))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                   ErrMsg("Failed to insert small tuple, ctid({%hu, %u}, %hu)", insertContext->ctid.GetFileId(),
                          insertContext->ctid.GetBlockNum(), insertContext->ctid.GetOffset()));
            return DSTORE_FAIL;
        }
    } else {
        if (STORAGE_FUNC_FAIL(InsertBigTuple(insertContext))) {
            ClearTupChunks(insertContext->heapTuple);
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                   ErrMsg("Failed to insert big tuple, ctid({%hu, %u}, %hu)", insertContext->ctid.GetFileId(),
                          insertContext->ctid.GetBlockNum(), insertContext->ctid.GetOffset()));
            return DSTORE_FAIL;
        }
    }

    /* Step 3: Heap insert end */
    EndInsert(insertContext);
    ClearTupChunks(insertContext->heapTuple);
    ErrLog(DSTORE_DEBUG1, MODULE_HEAP,
           ErrMsg("Insert tuple successfully, ctid({%hu, %u}, %hu), snapshot csn %lu", insertContext->ctid.GetFileId(),
                  insertContext->ctid.GetBlockNum(), insertContext->ctid.GetOffset(), thrd->GetSnapShotCsn()));
    return DSTORE_SUCC;
}

RetStatus HeapInsertHandler::PrepareTuple(HeapTuple *tuple)
{
    StorageAssert(tuple);
    HeapDiskTuple *diskTuple = tuple->GetDiskTuple();
    diskTuple->SetTdId(INVALID_TD_SLOT);
    diskTuple->SetXid(INVALID_XID);
    diskTuple->SetLockerTdId(INVALID_TD_SLOT);
    diskTuple->SetLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_INSERT);
    diskTuple->SetTupleSize(0);
    m_tupIsSplited = false;

    if (unlikely(HeapPage::TupBiggerThanPage(tuple))) {
        RetStatus ret = SplitTupIntoChunks(tuple);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("prepare tuple fail."));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus HeapInsertHandler::SplitTupIntoChunks(HeapTuple *tuple)
{
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    StorageAssert((m_tupChunks.m_chunkNum == 0 && m_tupChunks.m_chunksData == nullptr &&
        m_tupChunks.m_chunksSize == nullptr));
    char *diskTuple = static_cast<char *>(static_cast<void *>(tuple->GetDiskTuple()));
    uint32 diskTupSize = tuple->GetDiskTupleSize();
    uint32 maxTupSpaceSize = HeapPage::MaxDefaultTupleSpace();
    uint32 offset = 0, i = 0;
    StorageAssert(diskTupSize > maxTupSpaceSize);
    uint32 chunkTupSize = 0, chunkDataSize = 0;
    const uint32 linkTupHeaderSize = sizeof(HeapDiskTuple) + LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE;
    /* maxChunkDataSize exclude header of tuple */
    const uint32 maxChunkDataSize = maxTupSpaceSize - linkTupHeaderSize;
    if (unlikely(maxChunkDataSize == 0)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("maxChunkDataSize is 0, maxTupSpaceSize(%u) <= linkTupHeaderSize(%u).",
            maxTupSpaceSize, linkTupHeaderSize));
        return DSTORE_FAIL;
    }
    /* Only count tuple data when calculate how many chunks */
    uint32 numTupChunks = ((diskTupSize - sizeof(HeapDiskTuple)) + maxChunkDataSize - 1) / maxChunkDataSize;
    m_tupChunks.m_chunksData = static_cast<HeapDiskTuple **>(DstorePalloc(sizeof(HeapDiskTuple *) * numTupChunks));
    m_tupChunks.m_chunksSize = static_cast<uint16 *>(DstorePalloc(sizeof(uint16) * numTupChunks));
    if (unlikely(m_tupChunks.m_chunksData == nullptr || m_tupChunks.m_chunksSize == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("DstorePalloc fail chunkData(%u), chunkSize(%u).",
                      static_cast<uint32>(sizeof(HeapDiskTuple *) * numTupChunks),
                      static_cast<uint32>(sizeof(uint16) * numTupChunks)));
        return DSTORE_FAIL;
    }
    for (i = 0; i < numTupChunks; i++) {
        if (diskTupSize - offset >= maxChunkDataSize) {
            chunkTupSize = maxTupSpaceSize;
            chunkDataSize = maxChunkDataSize;
        } else {
            chunkTupSize = (diskTupSize - offset) + linkTupHeaderSize;
            chunkDataSize = diskTupSize - offset;
        }
        m_tupChunks.m_chunksData[i] = static_cast<HeapDiskTuple *>(DstorePalloc0(chunkTupSize));
        if (unlikely(m_tupChunks.m_chunksData[i] == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                ErrMsg("DstorePalloc0 fail size(%u) when split tuple into chunk. Chunk index(%u), chunk count(%u)",
                chunkTupSize, i, numTupChunks));
            return DSTORE_FAIL;
        }
        char *dest = static_cast<char *>(static_cast<void *>(m_tupChunks.m_chunksData[i]));
        if (i == 0) {
            /* We must copy header of tuple if it is first chunk */
            errno_t ret = memcpy_s(dest, sizeof(HeapDiskTuple), diskTuple, sizeof(HeapDiskTuple));
            storage_securec_check(ret, "\0", "\0");
            m_tupChunks.m_chunksData[0]->SetFirstLinkChunk();
            offset += sizeof(HeapDiskTuple);
        } else {
            m_tupChunks.m_chunksData[i]->SetNotFirstLinkChunk();
        }

        errno_t ret = memcpy_s(dest + linkTupHeaderSize, chunkDataSize, diskTuple + offset, chunkDataSize);
        storage_securec_check(ret, "\0", "\0");
        offset += chunkDataSize;
        m_tupChunks.m_chunksSize[i] = static_cast<uint16>(chunkTupSize);
        m_tupChunks.m_chunksData[i]->SetNextChunkCtid(INVALID_ITEM_POINTER);
        m_tupChunks.m_chunkNum++;
    }

    StorageAssert(offset == diskTupSize);
    StorageAssert(m_tupChunks.m_chunkNum == static_cast<uint32>(numTupChunks));
    /* Set the number of chunk in the first tupchunk */
    m_tupChunks.m_chunksData[0]->SetNumTupChunks(static_cast<uint32>(numTupChunks));
    m_tupIsSplited = true;
    return DSTORE_SUCC;
}

RetStatus HeapInsertHandler::BeginInsert(HeapInsertContext *insertContext)
{
    /* Step 1: Prepare the tuple for insertion */
    if (STORAGE_FUNC_FAIL(PrepareTuple(insertContext->heapTuple))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to prepare tuple when begin insert, ctid({%hu, %u}, %hu).",
            insertContext->ctid.GetFileId(), insertContext->ctid.GetBlockNum(), insertContext->ctid.GetOffset()));
        return DSTORE_FAIL;
    }

    /* Step 2: Prepare undo page and transaction slot */
    StorageAssert(m_thrd->GetActiveTransaction() != nullptr);
    if (STORAGE_FUNC_FAIL(m_thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to alloc transaction slot when begin insert."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus HeapInsertHandler::InsertSmallTuple(HeapInsertContext *insertContext)
{
    HeapDiskTuple *heapDiskTup = insertContext->heapTuple->GetDiskTuple();
    uint16 heapDiskTupSize = static_cast<uint16>(insertContext->heapTuple->GetDiskTupleSize());
    StorageAssert(!m_tupIsSplited);

    return InsertSmallDiskTup(insertContext, heapDiskTup, heapDiskTupSize);
}

RetStatus HeapInsertHandler::InsertBigTuple(HeapInsertContext *insertContext)
{
    StorageAssert(m_tupIsSplited);
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    ItemPointerData tupNextChunkCtid = INVALID_ITEM_POINTER;
    for (int32 i = m_tupChunks.m_chunkNum - 1; i >= 0; --i) {
        HeapDiskTuple *tupChunk = m_tupChunks.m_chunksData[i];
        tupChunk->SetNextChunkCtid(tupNextChunkCtid);
        if (STORAGE_FUNC_FAIL(InsertSmallDiskTup(insertContext, tupChunk, m_tupChunks.m_chunksSize[i]))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                   ErrMsg("Failed to insert the chunk when inserting big tuple. chunkId({%hu, %u}, %hu).",
                          insertContext->ctid.GetFileId(), insertContext->ctid.GetBlockNum(),
                          insertContext->ctid.GetOffset()));
            return DSTORE_FAIL;
        }
        tupNextChunkCtid = insertContext->ctid;
    }
    return DSTORE_SUCC;
}

HeapPageSpaceStatus HeapInsertHandler::CheckPageHasEnoughSpace(BufferDesc *bufferDesc, uint32 needFsmSize, TdId tdId)
{
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    StorageAssert(page != nullptr);
    StorageAssert(page->CheckSanity());

    /* if tdId is invalid, directly return, will choose new page. */
    if (tdId == INVALID_TD_SLOT) {
        ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Td slot is invalid when GetBuffer."));
        return HEAP_PAGE_NO_SPACE_INVALID_TD;
    }

    /* if page space is not enough, try prune page and still not enough, return */
    uint32 curPageFreeSpace = page->GetFreeSpaceForInsert();
    if (needFsmSize > curPageFreeSpace) {
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(bufferDesc);
            GenerateAllocTdWal(bufferDesc);
        }
        /* Try to prune page to see whether the free space of page is enough */
        HeapPruneHandler prune(m_instance, m_heapRel, m_thrd, bufferDesc, m_isLob);
        prune.TryPrunePage(static_cast<uint16>(needFsmSize));
        curPageFreeSpace = page->GetFreeSpaceForInsert();
        if (needFsmSize > curPageFreeSpace) {
            return HEAP_PAGE_NO_SPACE_AFTER_PRUNE;
        }
    }

    return HEAP_PAGE_HAS_ENOUGH_SPACE;
}

BufferDesc *HeapInsertHandler::GetBuffer(TdId &newTdId, uint32 size, const PageId &excludePageId)
{
    BufferDesc *bufferDesc;
    PageId targetPageId = GetTableSmgr()->GetLastPageIdForInsert();
    uint32 needFsmSize = TupleNeedPageFreeSize(size);
    uint16 tryFsmFailedTimes = 0;
    const uint16 fsmFailedThreshold = MAX_FSM_SEARCH_RETRY_TIME;

    if (targetPageId != INVALID_PAGE_ID) {
        ErrLog(DSTORE_DEBUG1, MODULE_HEAP,
            ErrMsg("Try to use cached page id (%hu, %u) of tableSmgr, segment meta page id (%hu, %u)",
            targetPageId.m_fileId, targetPageId.m_blockId, GetTableSmgr()->GetSegMetaPageId().m_fileId,
            GetTableSmgr()->GetSegMetaPageId().m_blockId));
    }

    for (;;) {
        /*
         * Be sure to check for interrupts at least once per page.
         */
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent  */
            return INVALID_BUFFER_DESC;
        }

        uint32 spaceInFsm = 0;

        /* Step 1: Get page from FSM if targetPageId is invalid */
        if (targetPageId == INVALID_PAGE_ID) {
            targetPageId = GetTableSmgr()->GetPageFromFSM(needFsmSize, tryFsmFailedTimes, &spaceInFsm);
        }

        /* Step 2: Get a page, either fetch or create one */
        if (!targetPageId.IsValid() || tryFsmFailedTimes >= fsmFailedThreshold) {
            targetPageId = GetTableSmgr()->GetNewPage();
            if (!targetPageId.IsValid()) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP,
                    ErrMsg("Failed to get new page, segment meta page id (%hu, %u), neededSize %u.",
                    GetTableSmgr()->GetSegMetaPageId().m_fileId,
                    GetTableSmgr()->GetSegMetaPageId().m_blockId, needFsmSize));
                /* Cannot extend the relation */
                ErrorCode errCode = StorageGetErrorCode();
                if (errCode == TBS_ERROR_TABLESPACE_USE_UP) {
                    return INVALID_BUFFER_DESC;
                }
                storage_set_error(HEAP_ERROR_GET_BUFFER_FAILED);
                return INVALID_BUFFER_DESC;
            }
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP,
                   ErrMsg("Get new page (%hu, %u) for table, segment meta page id (%hu, %u), neededSize %u",
                          targetPageId.m_fileId, targetPageId.m_blockId, GetTableSmgr()->GetSegMetaPageId().m_fileId,
                          GetTableSmgr()->GetSegMetaPageId().m_blockId, needFsmSize));
        } else if (targetPageId == excludePageId) {
            /* Should not fetch the excluded page */
            targetPageId = INVALID_PAGE_ID;
            tryFsmFailedTimes++;
            continue;
        }
        bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, targetPageId, LW_EXCLUSIVE, BufferPoolReadFlag(), m_ringBuf);
        /* Read buffer failure when node crash or other exception */
        if (unlikely(bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                ErrMsg("Read page(%hu, %u) failed when get buffer for inserting.",
                targetPageId.m_fileId, targetPageId.m_blockId));
            continue;
        }
        CheckBufferedPage(bufferDesc->GetPage(), targetPageId);

        /* Step 3: Alloc td */
        HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
        m_tdcontext.Begin(m_heapRel->m_pdbId, m_instance->GetCsnMgr()->GetRecycleCsnMin(m_heapRel->m_pdbId));
        newTdId = page->AllocTd(m_tdcontext);

        /* Step 4: check whether page space is enough  */
        HeapPageSpaceStatus status = CheckPageHasEnoughSpace(bufferDesc, needFsmSize, newTdId);
        /* Check the result here */
        switch (status) {
            case HEAP_PAGE_HAS_ENOUGH_SPACE: {
                page->SetIsNewPage(false);
                break;
            }
            case HEAP_PAGE_NO_SPACE_AFTER_PRUNE: {
                m_bufMgr->UnlockAndRelease(bufferDesc);
                uint32 curPageFreeSpace = page->GetFreeSpaceForInsert();
                if (GetTableSmgr()->NeedUpdateFsm(spaceInFsm, curPageFreeSpace)) {
                    RetStatus ret = GetTableSmgr()->UpdateFsmAndSearch(page->GetFsmIndex(), curPageFreeSpace,
                        needFsmSize, tryFsmFailedTimes, &targetPageId, &spaceInFsm);
                    if (ret != DSTORE_SUCC) {
                        targetPageId = INVALID_PAGE_ID;
                    }
                } else {
                    targetPageId = INVALID_PAGE_ID;
                }
                tryFsmFailedTimes++;
                continue;
            }
            case HEAP_PAGE_NO_SPACE_INVALID_TD: {
                m_bufMgr->UnlockAndRelease(bufferDesc);
                targetPageId = INVALID_PAGE_ID;
                tryFsmFailedTimes++;
                continue;
            }
            default:
                break;
        }
        break;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_HEAP,
        ErrMsg("Retry fsm failed %hu time, needFsmSize is %u", tryFsmFailedTimes, needFsmSize));

    GetTableSmgr()->SetLastPageIdForInsert(targetPageId);

    return bufferDesc;
}

uint32 HeapInsertHandler::TupleNeedPageFreeSize(uint32 freeSpaceNeeded) const
{
    uint32 maxSpaceAllowed = HeapPage::MaxDefaultTupleSpace();
    StorageReleasePanic(freeSpaceNeeded > maxSpaceAllowed, MODULE_HEAP,
                        ErrMsg("tuple size(%u), max tuple size(%u).", freeSpaceNeeded, maxSpaceAllowed));

    /* calculate the extra space to be saved on the page due to fillfactor */
    uint32 freeSpaceSaved = GetTableSmgr()->GetSaveFreeSpace();
    if (freeSpaceNeeded + freeSpaceSaved > maxSpaceAllowed) {
        ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Insert exceed fillfactor, needsize(%u), savesize(%u, "
            "maxsize(%u)", freeSpaceNeeded, freeSpaceSaved, maxSpaceAllowed));
        freeSpaceNeeded = maxSpaceAllowed;
    } else {
        freeSpaceNeeded += freeSpaceSaved;
    }
    return freeSpaceNeeded;
}

RetStatus HeapInsertHandler::InsertSmallDiskTup(
    HeapInsertContext *insertContext, HeapDiskTuple *diskTup, uint16 diskTupSize)
{
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    StoragePdb *pdb = m_instance->GetPdb(m_heapRel->m_pdbId);
    if (unlikely(pdb == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to get pdb."));
        return DSTORE_FAIL;
    }
    TransactionMgr *txnMgr = pdb->GetTransactionMgr();
    Transaction *transaction = m_thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    Xid xid = transaction->GetCurrentXid();
    uint8 tdId = INVALID_TD_SLOT;
    OffsetNumber offset = INVALID_ITEM_OFFSET_NUMBER;

    /* Step 1: Get proper buffer from insertion and allocate TD */
    m_bufferDesc = GetBuffer(tdId, diskTupSize);
    if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to get buffer. Disk tuple size is %hu.", diskTupSize));
        return DSTORE_FAIL;
    }
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    StorageReleasePanic(page == nullptr, MODULE_HEAP, ErrMsg("Heap page is null when inserting small disk tuple."));
    TD *td = page->GetTd(tdId);

    /* Step 2: Add tuple in page */
    StorageAssert(tdId < page->GetTdCount());
    diskTup->SetTdId(tdId);
    diskTup->SetXid(xid);
    diskTup->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    diskTup->SetLockerTdId(INVALID_TD_SLOT);
    diskTup->SetTupleSize(diskTupSize);
    offset = page->AddTuple(diskTup, diskTupSize, offset);
    StorageReleasePanic(offset == INVALID_ITEM_OFFSET_NUMBER,
        MODULE_HEAP, ErrMsg("Failed to add tuple in page(%hu, %u).", page->GetFileId(), page->GetBlockNum()));

    if (unlikely(!page->HasPrunableTuple() && page->GetFreeSpaceForInsert() < diskTupSize * SCALE_TIMES)) {
        RetStatus ret = GetTableSmgr()->UpdateFSM(page->GetFsmIndex(), page->GetFreeSpaceForInsert());
        StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_HEAP, ErrMsg("UpdateFSM failed."));
    }

    /* We must mark buffer dirty before unlock buffer */
    (void)m_bufMgr->MarkDirty(m_bufferDesc);
    ItemPointerData ctid(m_bufferDesc->GetPageId(), offset);
    insertContext->ctid = ctid;
    UndoType type = GetTableSmgr()->IsGlobalTempTable() ? UNDO_HEAP_INSERT_TMP : UNDO_HEAP_INSERT;
    UndoRecord undoRecord(type, tdId, td, ctid, insertContext->cid);
    /* if extend fail, we have done alloctd and modify data page, need rollback page changes and record alloctd wal */
    if (STORAGE_FUNC_FAIL(txnMgr->ExtendUndoSpaceIfNeeded(xid, undoRecord.GetRecordSize()))) {
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc);
            GenerateAllocTdWal(m_bufferDesc);
        }
        HandleErrorWhenGetUndoBuffer(m_bufferDesc, &undoRecord);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Extend undo space fail when insert tuple."));
        return DSTORE_FAIL;
    }
    /* Step 3: Generate undo record */
    walContext->BeginAtomicWal(xid);
    UndoRecPtr undoRecPtr = InsertUndoAndCheck(&undoRecord);
    if (unlikely(undoRecPtr == INVALID_UNDO_RECORD_PTR)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Insert undo record fail when insert small disk tuple."));
        return DSTORE_FAIL;
    }
    page->SetTd(tdId, xid, undoRecPtr, insertContext->cid);
    /* Step 4: Generate redo record */
    if (likely(NeedWal())) {
        GenerateHeapInsertWal(diskTup, offset, undoRecPtr, insertContext->cid);
    }
    (void)walContext->EndAtomicWal();
    /*
     * Page is in wal write after put wal record , so we can unlock and release buffer
     * When page is wal write, it can't be modified
     * */
    m_bufMgr->UnlockAndRelease(m_bufferDesc);
    m_bufferDesc = NULL;
    return DSTORE_SUCC;
}

void HeapInsertHandler::EndInsert(HeapInsertContext *insertContext)
{
    StorageAssert(insertContext);
    StorageAssert(insertContext->heapTuple);
    StorageAssert(insertContext->ctid != INVALID_ITEM_POINTER);

    insertContext->heapTuple->SetCtid(insertContext->ctid);
}

void HeapInsertHandler::ClearTupChunks(HeapTuple *tuple)
{
    if (likely(!HeapPage::TupBiggerThanPage(tuple))) {
        return;
    }

    for (uint32 i = 0; i < m_tupChunks.m_chunkNum; ++i) {
        if (m_tupChunks.m_chunksData != nullptr) {
            DstorePfreeExt(m_tupChunks.m_chunksData[i]);
        }
    }
    DstorePfreeExt(m_tupChunks.m_chunksData);
    DstorePfreeExt(m_tupChunks.m_chunksSize);
    m_tupChunks.m_chunkNum = 0;
}

RetStatus HeapInsertHandler::BatchInsert(HeapBacthInsertContext *batchContext)
{
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    StorageAssert(m_thrd->GetActiveTransaction() != nullptr);
    if (STORAGE_FUNC_FAIL(m_thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to prepare undo when heap batch insert."));
        return DSTORE_FAIL;
    }

    uint16 nsmalltuples = 0;
    uint32 totalSmallDiskSize = 0;
    HeapInsertContext **smallTupleContexts =
        static_cast<HeapInsertContext **>(DstorePalloc(sizeof(HeapInsertContext *) * batchContext->count));
    if (unlikely(smallTupleContexts == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("DstorePalloc fail size(%u) when heap batch insert.",
                      static_cast<uint32>(sizeof(HeapInsertContext *) * batchContext->count)));
        return DSTORE_FAIL;
    }
    HeapInsertContext *contexts = batchContext->contexts;
    for (int i = 0; i < batchContext->count; i++) {
        HeapInsertContext *context = &contexts[i];
        HeapTuple *heapTuple = context->heapTuple;
        if (STORAGE_FUNC_FAIL(PrepareTuple(heapTuple))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to prepare tuple when heap batch insert."));
            DstorePfree(smallTupleContexts);
            ClearTupChunks(heapTuple);
            return DSTORE_FAIL;
        }
        if (HeapPage::TupBiggerThanPage(heapTuple)) {
            if (STORAGE_FUNC_FAIL(BatchInsertBigTuple(context))) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP,
                       ErrMsg("Failed to insert big tuple when heap batch insert, ctid({%hu, %u}, %hu)",
                              context->ctid.GetFileId(), context->ctid.GetBlockNum(), context->ctid.GetOffset()));
                DstorePfree(smallTupleContexts);
                ClearTupChunks(heapTuple);
                return DSTORE_FAIL;
            }
            EndInsert(context);
            ClearTupChunks(heapTuple);
            continue;
        }
        smallTupleContexts[nsmalltuples] = context;
        nsmalltuples++;
        totalSmallDiskSize += heapTuple->GetDiskTupleSize();
    }
    if (unlikely((nsmalltuples == 0))) {
        DstorePfree(smallTupleContexts);
        return DSTORE_SUCC;
    }

    /* insert small tuples */
    if (STORAGE_FUNC_FAIL(BatchInsertSmallTuples(smallTupleContexts, nsmalltuples, totalSmallDiskSize))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("batch insert small tuples failed, tupleNum(%hu), totalsize(%u).",
                      nsmalltuples, totalSmallDiskSize));
        DstorePfree(smallTupleContexts);
        return DSTORE_FAIL;
    }
    DstorePfree(smallTupleContexts);
    return DSTORE_SUCC;
}

RetStatus HeapInsertHandler::BatchInsertBigTuple(HeapInsertContext *insertContext)
{
    StorageAssert(m_tupIsSplited);
    ItemPointerData tupNextChunkCtid = INVALID_ITEM_POINTER;
    for (int32 i = m_tupChunks.m_chunkNum - 1; i >= 0; --i) {
        HeapDiskTuple *tupChunk = m_tupChunks.m_chunksData[i];
        tupChunk->SetNextChunkCtid(tupNextChunkCtid);
        uint16 diskTupSize = m_tupChunks.m_chunksSize[i];

        if (STORAGE_FUNC_FAIL(InsertSmallDiskTup(insertContext, tupChunk, diskTupSize))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Batch insert small chunk failed, chunkSize(%hu)", diskTupSize));
            return DSTORE_FAIL;
        }

        tupNextChunkCtid = insertContext->ctid;
    }

    return DSTORE_SUCC;
}

uint32 HeapInsertHandler::BatchInsertInOneRangeOffset(
    HeapInsertContext **insertContexts, OffsetNumber &offnum, OffsetNumber endOffnum, TdId tdId)
{
    uint32 saveFreeSpace = GetTableSmgr()->GetSaveFreeSpace();
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    uint32 totalDiskTupleSize = 0;
    uint32 nThisRange = 0;
    while (offnum <= endOffnum) {
        HeapTuple *heapTuple = insertContexts[nThisRange]->heapTuple;
        HeapDiskTuple *diskTup = heapTuple->GetDiskTuple();
        uint16 diskTupSize = static_cast<uint16>(heapTuple->GetDiskTupleSize());
        /* Make sure that the tuple fits in the page. */
        if (page->GetFreeSpaceForInsert() < diskTupSize + saveFreeSpace) {
            break;
        }
        diskTup->SetTdId(tdId);
        diskTup->SetXid(m_thrd->GetActiveTransaction()->GetCurrentXid());
        diskTup->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
        diskTup->SetLockerTdId(INVALID_TD_SLOT);
        diskTup->SetTupleSize(diskTupSize);
        offnum = page->AddTuple(diskTup, diskTupSize, offnum);
        StorageReleasePanic(offnum == INVALID_ITEM_OFFSET_NUMBER,
            MODULE_HEAP,
            ErrMsg("Failed to add tuple in page(%hu, %u).", page->GetFileId(), page->GetBlockNum()));
        ItemPointerData ctid(m_bufferDesc->GetPageId(), offnum);
        /* set ctid for current tuple */
        heapTuple->SetCtid(ctid);
        insertContexts[nThisRange]->ctid = ctid;
        totalDiskTupleSize += diskTupSize;
        nThisRange++;
        offnum++;
    }
    return totalDiskTupleSize;
}

RetStatus HeapInsertHandler::BatchInsertSmallTuples(
    HeapInsertContext **insertContexts, uint16 ntuples, uint32 totalSmallDiskSize)
{
    uint32 saveFreeSpace = GetTableSmgr()->GetSaveFreeSpace();
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    Transaction *transaction = m_thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    Xid xid = transaction->GetCurrentXid();

    const uint32 tupleSpace = HeapPage::MaxDefaultTupleSpace() - GetTableSmgr()->GetSaveFreeSpace();
    uint16 ndone = 0;
    while (ndone < ntuples) {
        AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
        uint8 tdId = INVALID_TD_SLOT;
        HeapFreeOffsetRanges *freeOffsetRanges = nullptr;
        uint32 pageSize = DstoreMin(totalSmallDiskSize, tupleSpace);
        m_bufferDesc = GetBuffer(tdId, pageSize);
        if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to get buffer. Page size is %u.", pageSize));
            return DSTORE_FAIL;
        }
        HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());

        /*
         * Get the unused offset ranges in the page. This is required for
         * deciding the number of undo records to be prepared later.
         */
        freeOffsetRanges = GetUsableOffsetRanges(insertContexts, page, ntuples, ndone, saveFreeSpace);
        if (unlikely(freeOffsetRanges == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("freeOffsetRanges is nullptr."));
            m_bufMgr->UnlockAndRelease(m_bufferDesc);
            return DSTORE_FAIL;
        }
        StorageAssert(freeOffsetRanges->nranges > 0);

        uint16 nrangesInUse = 0;
        uint32 walRecordSize = sizeof(WalRecordHeapBatchInsert);
        uint16 nthispage = 0;
        for (int i = 0; i < freeOffsetRanges->nranges; i++) {
            OffsetNumber offnum = freeOffsetRanges->startOffset[i];
            uint32 insertTupleSize = BatchInsertInOneRangeOffset(
                &insertContexts[ndone + nthispage], offnum, freeOffsetRanges->endOffset[i], tdId);
            walRecordSize += insertTupleSize;
            nthispage += static_cast<uint16>(offnum - freeOffsetRanges->startOffset[i]);
            freeOffsetRanges->endOffset[i] = static_cast<OffsetNumber>(offnum - 1);
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP,
                   ErrMsg("start offset: %d, end offset: %d, offnum: %hu.", freeOffsetRanges->startOffset[i],
                          freeOffsetRanges->endOffset[i], offnum));

            if (offnum == freeOffsetRanges->startOffset[i]) {
                break;
            }
            nrangesInUse++;
        }
        (void)m_bufMgr->MarkDirty(m_bufferDesc);
        TD *td = page->GetTd(tdId);
        ItemPointerData tmpCtid = {m_bufferDesc->GetPageId(), INVALID_ITEM_OFFSET_NUMBER};
        UndoType type = GetTableSmgr()->IsGlobalTempTable() ? UNDO_HEAP_BATCH_INSERT_TMP : UNDO_HEAP_BATCH_INSERT;
        UndoRecord undoRec(type, tdId, td, tmpCtid, insertContexts[ndone]->cid);
        if (STORAGE_FUNC_FAIL(PrepareUndoForBatchInsert(undoRec, freeOffsetRanges, nrangesInUse))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Prepare undo fail when batch insert small tuple."));
            DstorePfree(freeOffsetRanges);
            m_bufMgr->UnlockAndRelease(m_bufferDesc);
            return DSTORE_FAIL;
        }
        DstorePfree(freeOffsetRanges);
        walContext->BeginAtomicWal(xid);
        UndoRecPtr undoRecPtr = InsertUndoAndCheck(&undoRec);
        if (unlikely(undoRecPtr == INVALID_UNDO_RECORD_PTR)) {
            /* No need to release bufferDesc because it has already been released in InsertUndoAndCheck. */
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Insert undo record fail when batch insert small tuple."));
            return DSTORE_FAIL;
        }
        page->SetTd(tdId, xid, undoRecPtr, insertContexts[ndone]->cid);
        if (likely(NeedWal())) {
            GenerateHeapBatchInsertWal(&insertContexts[ndone], walRecordSize, nthispage, td->GetUndoRecPtr());
        }
        (void)walContext->EndAtomicWal();
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        ndone += nthispage;
    }
    return DSTORE_SUCC;
}

RetStatus HeapInsertHandler::PrepareUndoForBatchInsert(
    UndoRecord &undoRec, HeapFreeOffsetRanges *rangeInfo, uint16 nrangesInUse)
{
    StorageAssert(nrangesInUse <= rangeInfo->nranges);

    uint32 size = sizeof(UndoDataHeapBatchInsert) +
                  (sizeof(rangeInfo->startOffset[0]) + sizeof(rangeInfo->endOffset[0])) * nrangesInUse;
    UndoDataHeapBatchInsert *undoData = static_cast<UndoDataHeapBatchInsert *>(DstorePalloc(size));
    StorageReleasePanic(undoData == nullptr, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%u).", size));
    undoData->Init();

    OffsetNumber *startOffset = rangeInfo->startOffset;
    OffsetNumber *endOffset = rangeInfo->endOffset;
    for (uint16 i = 0; i < nrangesInUse; ++i) {
        undoData->Append(static_cast<char *>(static_cast<void *>(startOffset + i)), sizeof(startOffset[0]));
        undoData->Append(static_cast<char *>(static_cast<void *>(endOffset + i)), sizeof(endOffset[0]));
    }

    RetStatus ret = undoRec.Append(static_cast<char *>(static_cast<void *>(undoData)), undoData->GetSize());
    if (STORAGE_FUNC_FAIL(ret)) {
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc);
            GenerateAllocTdWal(m_bufferDesc);
        }
        HandleErrorWhenGetUndoBuffer(m_bufferDesc, &undoRec, undoData);
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("DstorePalloc fail size(%u) when append undodata.", undoData->GetSize()));
        DstorePfree(undoData);
        return DSTORE_FAIL;
    }

    StorageAssert(m_thrd->GetActiveTransaction() != nullptr);
    Xid xid = m_thrd->GetActiveTransaction()->GetCurrentXid();
    TransactionMgr *txnMgr = m_instance->GetPdb(m_heapRel->m_pdbId)->GetTransactionMgr();
    /* if extend fail ,we has done alloctd and modify page, need rollback page changes and record allocTd wal */
    if (STORAGE_FUNC_FAIL(txnMgr->ExtendUndoSpaceIfNeeded(xid, undoRec.GetRecordSize()))) {
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc);
            GenerateAllocTdWal(m_bufferDesc);
        }
        HandleErrorWhenGetUndoBuffer(m_bufferDesc, &undoRec);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Extend undo space fail when batch insert tuple."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

HeapFreeOffsetRanges *HeapInsertHandler::GetUsableOffsetRanges(
    HeapInsertContext **insertContexts, HeapPage *page, uint16 ntuples, uint16 ndone, uint32 saveFreeSpace)
{
    uint16 nThisPage = ndone;
    uint32 usedSpace = 0;
    uint32 availSpace;
    OffsetNumber beginOffset;
    HeapFreeOffsetRanges *freeOffsetRanges;

    OffsetNumber maxOffsetNumber = page->GetMaxOffset();
    beginOffset = maxOffsetNumber + 1;
    availSpace = page->GetFreeSpace<FreeSpaceCondition::RAW>();

    if (page->HasFreeItemId()) {
        freeOffsetRanges = LocateUsableItemIds(insertContexts, page, ntuples, &nThisPage, &usedSpace);
        if (unlikely(freeOffsetRanges == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("freeOffsetRanges is nullptr."));
            return nullptr;
        }
    } else {
        freeOffsetRanges = static_cast<HeapFreeOffsetRanges *>(DstorePalloc0(sizeof(HeapFreeOffsetRanges)));
        if (unlikely(freeOffsetRanges == nullptr)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc0 failed when GetUsableOffsetRanges."));
            return nullptr;
        }
    }

    /*
     * Now, there are no free line pointers. Check whether we can insert
     * another tuple in the page, then we'll insert another range starting
     * from beginOffset to endOffset number. We can decide the actual end
     * offset for this range while inserting tuples in the buffer.
     */
    if ((beginOffset <= page->MaxTupNumPerPage()) && (nThisPage < ntuples)) {
        HeapTuple *heaptup = insertContexts[nThisPage]->heapTuple;
        uint32 neededSpace = usedSpace + sizeof(ItemId) + heaptup->GetDiskTupleSize() + saveFreeSpace;
        /* Check if we can fit this tuple + a new offset in the page */
        if (availSpace >= neededSpace) {
            /*
             * Choose minimum among MaxOffsetNumber and the maximum offsets
             * required for tuples.
             */
            uint16 requiredTuples = static_cast<uint16>(ntuples - nThisPage);
            OffsetNumber endOffset = static_cast<OffsetNumber>(beginOffset + requiredTuples - 1);
            endOffset = DstoreMin(MAX_ITEM_OFFSET_NUMBER, endOffset);

            freeOffsetRanges->nranges++;
            freeOffsetRanges->startOffset[freeOffsetRanges->nranges - 1] = beginOffset;
            freeOffsetRanges->endOffset[freeOffsetRanges->nranges - 1] = endOffset;
        } else if (freeOffsetRanges->nranges == 0) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                   ErrMsg("tuple is too big, please check the table fillfactor: "
                          "needed_size = %u, free_space = %u, save_free_space = %u.",
                          neededSpace, availSpace, saveFreeSpace));
        }
    }
    StorageReleasePanic(freeOffsetRanges->nranges == 0,
        MODULE_HEAP,
        ErrMsg("please check free space: "
               "beginOffset = %hu, num_this_page = %hu, free_space = %u.",
            beginOffset,
            nThisPage,
            availSpace));

    return freeOffsetRanges;
}

HeapFreeOffsetRanges *HeapInsertHandler::LocateUsableItemIds(
    HeapInsertContext **insertContexts, HeapPage *page, uint16 ntuples, uint16 *nthispage, uint32 *usedSpace)
{
    bool inRange = false;
    OffsetNumber offsetNumber;
    OffsetNumber limit;
    uint32 availSpace;
    HeapFreeOffsetRanges *freeOffsetRanges;

    freeOffsetRanges = static_cast<HeapFreeOffsetRanges *>(DstorePalloc0(sizeof(HeapFreeOffsetRanges)));
    if (unlikely(freeOffsetRanges == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc0 failed when LocateUsableItemIds."));
        return nullptr;
    }
    freeOffsetRanges->nranges = 0;

    availSpace = page->GetFreeSpace<FreeSpaceCondition::RAW>();
    limit = page->GetMaxOffset() + 1;

    /*
     * Look for "recyclable" (unused) ItemId.  We check for no storage as
     * well, just to be paranoid --- unused items should never have
     * storage.
     */
    uint32 saveFreeSpace = GetTableSmgr()->GetSaveFreeSpace();
    for (offsetNumber = 1; offsetNumber < limit; offsetNumber++) {
        ItemId *itemId = page->GetItemIdPtr(offsetNumber);
        if (*nthispage >= ntuples) {
            /* No more tuples to insert */
            break;
        }

        if (itemId->IsUnused()) {
            HeapTuple *heapTup = insertContexts[*nthispage]->heapTuple;
            uint32 neededSpace = *usedSpace + heapTup->GetDiskTupleSize() + saveFreeSpace;
            /* Check if we can fit this tuple in the page */
            if (availSpace < neededSpace) {
                /* No more space to insert tuples in this page */
                break;
            }

            (*usedSpace) += heapTup->GetDiskTupleSize();
            (*nthispage)++;

            if (!inRange) {
                /* Start of a new range */
                freeOffsetRanges->nranges++;
                freeOffsetRanges->startOffset[freeOffsetRanges->nranges - 1] = offsetNumber;
                inRange = true;
            }
            freeOffsetRanges->endOffset[freeOffsetRanges->nranges - 1] = offsetNumber;
        } else {
            inRange = false;
        }
    }
    return freeOffsetRanges;
}

void HeapInsertHandler::GenerateHeapInsertWal(HeapDiskTuple *diskTup, OffsetNumber offset, UndoRecPtr undoPtr,
                                              CommandId cid)
{
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_HEAP, ErrMsg("Wal has not atomic begin."));
    walContext->RememberPageNeedWal(m_bufferDesc);

    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = m_bufferDesc->GetFileVersion();
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdcontext);
    uint32 walDataSize = sizeof(WalRecordHeapInsert) + diskTup->GetTupleSize() + allocTdSize;
#ifdef ENABLE_LOGICAL_REPL
    Oid tableOid = GetTableOid();
    if (WalLogicalActive()) {
        walDataSize = walDataSize + sizeof(Oid) + sizeof(CommitSeqNo);
        if (unlikely(IsLogicalDecodeDictNeeded(tableOid))) {
            walDataSize += sizeof(CommandId);
        }
    }
#endif

    /* HeapDiskTuple + AllocTd + (tableOid + snapshotCsn + commandId (if it changed decode dict)) */
    WalRecordHeapInsert *walData = static_cast<WalRecordHeapInsert *>(DstorePalloc(walDataSize));
    while (STORAGE_VAR_NULL(walData)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
            ErrMsg("GenerateHeapInsertWal alloc memory for size %u failed, retry it.", walDataSize));
        GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        walData = static_cast<WalRecordHeapInsert*>(DstorePalloc(walDataSize));
    }
    walData->SetHeader({WAL_HEAP_INSERT, walDataSize, m_bufferDesc->GetPageId(), page->GetWalId(), page->GetPlsn(),
        page->GetGlsn(), glsnChangedFlag, fileVersion}, offset, undoPtr);
    walData->SetData(static_cast<char *>(static_cast<void *>(diskTup)), diskTup->GetTupleSize());
    walData->SetAllocTd(m_tdcontext);
#ifdef ENABLE_LOGICAL_REPL
    if (WalLogicalActive()) {
        uint32 fromPos = diskTup->GetTupleSize() + allocTdSize;
        CommitSeqNo snapshotCsn = m_thrd->GetActiveTransaction()->GetSnapshotCsn();
        CommandId snapshotCid = INVALID_CID;
        if (unlikely(IsLogicalDecodeDictNeeded(tableOid))) {
            snapshotCid = cid;
        }

        walData->SetLogicalDecodeInfo(fromPos, &tableOid, &snapshotCsn, &snapshotCid);
    }
#else
    UNUSED_VARIABLE(cid);
#endif
    walContext->PutNewWalRecord(walData);
    DstorePfree(walData);
}

void HeapInsertHandler::GenerateHeapBatchInsertWal(
    HeapInsertContext **insertContexts, uint32 size, uint16 nthispage, UndoRecPtr undoPtr)
{
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_HEAP, ErrMsg("Wal has not atomic begin."));
    walContext->RememberPageNeedWal(m_bufferDesc);

    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = m_bufferDesc->GetFileVersion();
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdcontext);
    uint32 walDataSize = size + nthispage * sizeof(OffsetNumber) + allocTdSize;

    /* Record WalRecordHeapBatchInsert */
    WalRecordHeapBatchInsert *walData = static_cast<WalRecordHeapBatchInsert *>(walContext->GetTempWalBuf());
    StorageReleasePanic(walDataSize > BLCKSZ, MODULE_HEAP, ErrMsg("The value of dstore palloc exceeds BLCKSZ, "
    "the walDataSize is (%u)", walDataSize));
    walData->SetHeader({WAL_HEAP_BATCH_INSERT, walDataSize, m_bufferDesc->GetPageId(), page->GetWalId(),
        page->GetPlsn(), page->GetGlsn(), glsnChangedFlag, fileVersion}, undoPtr);
    for (int i = 0; i < nthispage; i++) {
        HeapTuple *heapTup = insertContexts[i]->heapTuple;
        HeapDiskTuple *diskTup = heapTup->GetDiskTuple();
        walData->AppendData(heapTup->GetCtid()->GetOffset(),
            static_cast<char *>(static_cast<void *>(diskTup)),
            heapTup->GetDiskTupleSize());
    }
    walData->SetAllocTd(m_tdcontext);

    walContext->PutNewWalRecord(walData);
}

void HeapInsertHandler::RollbackLastRecordWhenConflict(ItemPointerData ctid)
{
    StorageAssert(m_instance->GetPdb(m_heapRel->m_pdbId) != nullptr);
    TransactionMgr *txnMgr = m_instance->GetPdb(m_heapRel->m_pdbId)->GetTransactionMgr();

    /* Step 1: find the page */
    BufferDesc *bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, ctid.GetPageId(), LW_EXCLUSIVE);
    STORAGE_CHECK_BUFFER_PANIC(bufferDesc, MODULE_HEAP, ctid.GetPageId());
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());

    /* Step 2: fetch undo record */
    UndoRecord record;
    TD *td = page->GetTd(page->GetTupleTdId(ctid.GetOffset()));
    UNUSE_PARAM RetStatus status = txnMgr->FetchUndoRecord(td->GetXid(), &record, td->GetUndoRecPtr());
    StorageAssert(STORAGE_FUNC_SUCC(status));
    StorageAssert(record.IsMatchedCtid(ctid));

    /* Step 3: undo */
    RetStatus ret = page->UndoHeap(&record);
    StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_HEAP,
        ErrMsg("Undo failed, page(%hu, %u), offset(%hu).", ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
    (void)m_bufMgr->MarkDirty(bufferDesc);

    /* Step 4: generate wal */
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(thrd->GetCurrentXid());
    UndoZone::GenerateWalForRollback(bufferDesc, record, WAL_UNDO_HEAP);
    m_bufMgr->UnlockAndRelease(bufferDesc);
    (void)walContext->EndAtomicWal();
}
void HeapInsertHandler::SetHeapRelAndBufMgr(StorageRelation heapRel)
{
    StorageAssert(heapRel != nullptr);
    m_heapRel = heapRel;
    m_bufMgr = (heapRel->tableSmgr != nullptr && heapRel->tableSmgr->IsGlobalTempTable()) ? m_thrd->GetTmpLocalBufMgr()
                                                                        : m_instance->GetBufferMgr();
}
}  // namespace DSTORE
