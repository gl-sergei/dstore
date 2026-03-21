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
 * dstore_partition_fsm.cpp
 *
 * IDENTIFICATION
 *        src/fsm/dstore_partition_fsm.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "fsm/dstore_partition_fsm.h"
#include "common/log/dstore_log.h"
#include "framework/dstore_thread.h"
#include "wal/dstore_wal_write_context.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "errorcode/dstore_heap_error_code.h"
#include "errorcode/dstore_tablespace_error_code.h"

namespace DSTORE {

PartitionFreeSpaceMap::PartitionFreeSpaceMap(const PageId segMetaPageId, const PageId fsmMetaPageId,
                                             BufMgrInterface *bufMgr, const TablespaceId tablespaceId, PdbId pdbId)
    : m_pdbId(pdbId),
      m_segMetaPageId(segMetaPageId),
      m_fsmMetaPageId(fsmMetaPageId),
      m_tablespaceId(tablespaceId),
      m_bufMgr(bufMgr)
{}

PageId PartitionFreeSpaceMap::GetPage(PageId heapSegMetaPageId, uint32 space, uint16 retryTime, uint32 *spaceInFsm,
                                      bool *needExtensionTask)
{
    PageId selectPageId;
    /* Search from FSM root */
    uint16 currentLevel = 0;
    PageId currentFsmPageId = INVALID_PAGE_ID;
    BufferDesc *fsmMetaPageBuf = ReadFsmMetaPageBuf(LW_SHARED);
    FreeSpaceMapMetaPage *fsmMetaPage = GetFsmMetaPage(fsmMetaPageBuf);
    StorageReleasePanic(fsmMetaPage == nullptr, MODULE_SEGMENT, ErrMsg("Get fsm meta page failed"));

    /* Read information from Fsm Meta page */
    currentLevel = fsmMetaPage->GetFsmRootLevel();
    currentFsmPageId = fsmMetaPage->currMap[currentLevel];
    UnlockAndReleaseFsmMetaPageBuf(fsmMetaPageBuf);

    /* Decide if we need to register an EXTEND_TASK here */
    *needExtensionTask = NeedExtensionTask(heapSegMetaPageId);

    /* Register an EXTEND_TASK if needed */
    if (*needExtensionTask) {
        ObjSpaceMgrExtendTaskInfo extendTaskInfo(m_pdbId, heapSegMetaPageId, m_tablespaceId, m_segMetaPageId,
            m_fsmMetaPageId);
        ObjSpaceMgr *objSpaceMgr = g_storageInstance->GetPdb(m_pdbId)->GetObjSpaceMgr();

        if (STORAGE_FUNC_FAIL(objSpaceMgr->RegisterObjSpaceMgrTaskIfNeeded(&extendTaskInfo))) {
            /* Something wrong with memory allocation. return INVALID_PAGE_ID now */
            ErrLog(DSTORE_WARNING, MODULE_SEGMENT, ErrMsg("Failed to register extension task for segment (%hu, %u).",
                m_segMetaPageId.m_fileId, m_segMetaPageId.m_blockId));
            return INVALID_PAGE_ID;
        }

        /* Register a RECYCLE_FSM_TASK if needed */
        if (NeedRecycleFsmTask()) {
            ObjSpaceMgrRecycleFsmTaskInfo recycleTaskInfo(m_pdbId, m_tablespaceId, m_segMetaPageId);
            if (STORAGE_FUNC_FAIL(objSpaceMgr->RegisterObjSpaceMgrTaskIfNeeded(&recycleTaskInfo))) {
                /* Something wrong with memory allocation. return INVALID_PAGE_ID now */
                return INVALID_PAGE_ID;
            }
        }
    }

    /* Select a candidate page from the FSM tree */
    selectPageId = GetPageInternal(space, retryTime, currentLevel, currentFsmPageId, spaceInFsm);
    if (selectPageId != INVALID_PAGE_ID) {
        /* Update numUsedPages of this FSM based on if page is a new page */
        if (STORAGE_FUNC_FAIL(UpdateNumUsedPagesIfNeeded(selectPageId))) {
            return INVALID_PAGE_ID;
        }

        ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
               ErrMsg("Get new page (%d, %u) success from non-cache leaf fsm", selectPageId.m_fileId,
                      selectPageId.m_blockId));
    }

    return selectPageId;
}

PageId PartitionFreeSpaceMap::GetPageInternal(uint32 space, uint16 retryTime, uint16 currentLevel,
                                              PageId currentFsmPageId, uint32 *spaceInFsm)
{
    PageId selectPageId;
    BufferDesc *fsmBuf = nullptr;
    FsmPage *fsmPage = nullptr;
    uint16 listId;

    for (;;) {
        /* Read current fsm page */
        fsmBuf = m_bufMgr->Read(m_pdbId, currentFsmPageId, LW_SHARED);
        if (fsmBuf == INVALID_BUFFER_DESC) {
            return INVALID_PAGE_ID;
        }
        fsmPage = static_cast<FsmPage *>(fsmBuf->GetPage());
        const FsmNode *fsmNode = SearchChild(space, fsmPage, retryTime);
        if (fsmNode == nullptr) {
            /* Fail to find pages fit space in FSM */
            ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("FSM has no available page with required space %u "
                "for segment(%hu, %u)", space, m_segMetaPageId.m_fileId, m_segMetaPageId.m_blockId));
            m_bufMgr->UnlockAndRelease(fsmBuf);
            return INVALID_PAGE_ID;
        }
        selectPageId = fsmNode->page;
        listId = fsmNode->listId;
        m_bufMgr->UnlockAndRelease(fsmBuf);

        /* Continue search if level does not reach 0 */
        if (currentLevel > 0) {
            currentLevel -= 1;
            currentFsmPageId = selectPageId;
        } else {
            break;
        }
    }

    if (spaceInFsm != nullptr) {
        *spaceInFsm = FSM_SPACE_LINE[listId];
    }

    return selectPageId;
}

bool PartitionFreeSpaceMap::NeedExtensionTask(PageId heapSegMetaPageId)
{
    /* Step 0: global temp table no need extend in the background */
    if (IsTemporaryTableFsm()) {
        return false;
    }

    /* Step 1: if there is already an Extension task registered for current FSM, return false */
    if (ObjSpaceMgr::IsExtensionTaskRegistered(heapSegMetaPageId, m_tablespaceId, m_segMetaPageId, m_fsmMetaPageId,
                                               m_pdbId)) {
        ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
               ErrMsg("FSM tree {%hu, %u} (segment {%hu, %u}) already have an Extension task registered",
                      m_fsmMetaPageId.m_fileId, m_fsmMetaPageId.m_blockId, m_segMetaPageId.m_fileId,
                      m_segMetaPageId.m_blockId));
        return false;
    }

    /* Step 2: Lock the FsmMetaPage here and retrieve page usage information */
    BufferDesc *fsmMetaBuf = ReadFsmMetaPageBuf(LW_SHARED);
    FreeSpaceMapMetaPage *fsmMetaPage = GetFsmMetaPage(fsmMetaBuf);
    uint64 numUnusedPages = fsmMetaPage->GetNumUnusedPages();
    uint64 numTotalPages = fsmMetaPage->GetNumTotalPages();
    UnlockAndReleaseFsmMetaPageBuf(fsmMetaBuf);

    /* Step 3: if we have enough pages for current FSM or too few pages to register a Extension task, return false */
    if (numUnusedPages < MAX_FSM_EXTEND_COEFFICIENT * PAGES_ADD_TO_FSM_PER_TIME &&
        numTotalPages > MAX_FSM_EXTEND_COEFFICIENT * PAGES_ADD_TO_FSM_PER_TIME) {
        return true;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT, ErrMsg("no need register extension task for segment (%hu, %u), "
        "FSM(%hu, %u)numUnusedPages(%lu)numTotalPages(%lu).", m_segMetaPageId.m_fileId, m_segMetaPageId.m_blockId,
        m_fsmMetaPageId.m_fileId, m_fsmMetaPageId.m_blockId, numUnusedPages, numTotalPages));

    return false;
}

bool PartitionFreeSpaceMap::NeedRecycleFsmTask()
{
    uint32 probOfRecycleFsm = static_cast<uint32>(g_storageInstance->GetGuc()->probOfRecycleFsm);
    uint8 randomNum = 0;
    uint8 maxPercent = 100;

    /* if we are in single node or there is already a RecycleFsm task registered for current Segment, return false */
    if (g_storageInstance->GetType() == StorageInstanceType::SINGLE ||
        ObjSpaceMgr::IsRecycleFsmTaskRegistered(m_tablespaceId, m_segMetaPageId, m_pdbId)) {
        return false;
    }

    /* Global temp table no need recycle fsm */
    if (IsTemporaryTableFsm()) {
        return false;
    }

    /* Now, determine if we need to register the extension task based on probability */
    if (STORAGE_FUNC_FAIL(DstoreGetRandomNum(randomNum))) {
        /* Don't do Recycle task if anything wrong here */
        return false;
    }

    return ((randomNum % maxPercent) < probOfRecycleFsm);
}

RetStatus PartitionFreeSpaceMap::UpdateNumUsedPagesIfNeeded(const PageId &pageId)
{
    /* Check the IsNewPage flag of given page */
    BufferDesc *pageBuf = m_bufMgr->Read(m_pdbId, pageId, LW_SHARED);
    if (pageBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }

    DataPage *dataPage = static_cast<DataPage *>(pageBuf->GetPage());
    bool isNewPage = dataPage->IsNewPage();
    m_bufMgr->UnlockAndRelease(pageBuf);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    /* Update PartitionFreeSpaceMap statistics if the page is a new page */
    if (isNewPage) {
        WalRecordTbsFsmMetaUpdateNumUsedPages fsmMetaUpdateNumUsedPagesWalData;
        BufferDesc *fsmMetaBuf = ReadFsmMetaPageBuf(LW_EXCLUSIVE);
        FreeSpaceMapMetaPage *fsmMetaPage = GetFsmMetaPage(fsmMetaBuf);
        StorageReleasePanic(fsmMetaPage == nullptr, MODULE_SEGMENT,
                            ErrMsg("UpdateNumUsedPagesIfNeeded: Get fsm meta page failed."));
        if (fsmMetaBuf == INVALID_BUFFER_DESC) {
            return DSTORE_FAIL;
        }

        /* In a concurrent scenario, if multiple threads obtain `isNewPage` as true,
         * it could lead to an incorrect increment of `numUsedPages`.
         * The check here is used to ensure that `numUsedPages` is always less than or equal to `numTotalPages`. */
        if ((fsmMetaPage->numUsedPages + 1) <= fsmMetaPage->numTotalPages) {
            fsmMetaPage->numUsedPages += 1;
        }
        ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
               ErrMsg("FSM (%hu, %u) statistic: segment (%hu, %u), numUsedPages %lu, numTotalPages %lu",
                      fsmMetaBuf->GetPageId().m_fileId, fsmMetaBuf->GetPageId().m_blockId, m_segMetaPageId.m_fileId,
                      m_segMetaPageId.m_blockId, fsmMetaPage->numUsedPages, fsmMetaPage->numTotalPages));
        (void)m_bufMgr->MarkDirty(fsmMetaBuf);
        if (NeedWal()) {
            walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
            bool glsnChangedFlag = (fsmMetaPage->GetWalId() != walWriterContext->GetWalId());
            fsmMetaUpdateNumUsedPagesWalData.SetHeader(
                {WAL_TBS_FSM_META_UPDATE_NUM_USED_PAGES, sizeof(fsmMetaUpdateNumUsedPagesWalData),
                 fsmMetaBuf->GetPageId(), fsmMetaPage->GetWalId(), fsmMetaPage->GetPlsn(), fsmMetaPage->GetGlsn(),
                 glsnChangedFlag, fsmMetaBuf->GetFileVersion()});
            fsmMetaUpdateNumUsedPagesWalData.SetData(fsmMetaPage->numUsedPages);
            walWriterContext->RememberPageNeedWal(fsmMetaBuf);
            walWriterContext->PutNewWalRecord(&fsmMetaUpdateNumUsedPagesWalData);
            (void)walWriterContext->EndAtomicWal();
        }
        UnlockAndReleaseFsmMetaPageBuf(fsmMetaBuf);
    }
    return DSTORE_SUCC;
}

RetStatus PartitionFreeSpaceMap::ConditionalUpdateFsmAccessTimestamp()
{
    /* Update FSM timestamp based on random percentage */
    uint8 randomNum = 0;
    uint8 maxPercent = 100;
    if (STORAGE_FUNC_FAIL(DstoreGetRandomNum(randomNum))) {
        /* Don't do Recycle task if anything wrong here */
        return DSTORE_FAIL;
    }

    uint32 probOfUpdateFsmTimestamp = static_cast<uint32>(g_storageInstance->GetGuc()->probOfUpdateFsmTimestamp);
    if ((randomNum % maxPercent) < probOfUpdateFsmTimestamp) {
        BufferDesc *fsmMetaBuf = ReadFsmMetaPageBuf(LW_EXCLUSIVE);
        if (fsmMetaBuf == INVALID_BUFFER_DESC) {
            return DSTORE_FAIL;
        }
        PartitionFreeSpaceMap::UpdateFsmAccessTimestamp(fsmMetaBuf, m_bufMgr);
        UnlockAndReleaseFsmMetaPageBuf(fsmMetaBuf);
    }

    return DSTORE_SUCC;
}

void PartitionFreeSpaceMap::UpdateFsmAccessTimestamp(BufferDesc *fsmMetaPageBuf, BufMgrInterface *bufMgr)
{
    FreeSpaceMapMetaPage *freeSpaceMapMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaPageBuf->GetPage());

    /* Since this is just a timestamp, no need to write XLOG to recovery it */
    TimestampTz timestamp = GetCurrentTimestampInSecond();
    TimestampTz oldTimestamp = freeSpaceMapMetaPage->accessTimestamp;
    if (timestamp != oldTimestamp) {
        freeSpaceMapMetaPage->accessTimestamp = timestamp;
        (void)bufMgr->MarkDirty(fsmMetaPageBuf, false);

        ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
               ErrMsg("FSM (%hu, %u) update access timestamp from %ld to %ld", fsmMetaPageBuf->GetPageId().m_fileId,
                      fsmMetaPageBuf->GetPageId().m_blockId, oldTimestamp, timestamp));
    }
}

void PartitionFreeSpaceMap::SearchPageIdOfChild(FsmPage *fsmPage, uint32 spaceNeeded, PageId *pageId, uint16 retryTimes,
                                                uint32 *spaceInfsm)
{
    if (pageId == nullptr) {
        return;
    }
    if (spaceNeeded != UINT32_MAX) {
        const FsmNode *fsmNode = SearchChild(spaceNeeded, fsmPage, retryTimes);
        if (fsmNode == nullptr) {
            *pageId = INVALID_PAGE_ID;
        } else {
            *pageId = fsmNode->page;
            if (spaceInfsm != nullptr) {
                uint16 listId = fsmNode->listId;
                *spaceInfsm = FSM_SPACE_LINE[listId];
            }
        }
    }
    return;
}

RetStatus PartitionFreeSpaceMap::UpdateSpace(FsmUpdateSpaceContext context)
{
    FsmIndex fsmIndex = context.fsmIndex;
    uint32 space = context.space;
    bool *fsmIsChanged = context.fsmIsChanged;
    uint32 spaceNeeded = context.spaceNeeded;
    uint16 retryTimes = context.retryTimes;
    PageId *pageId = context.pageId;
    BufMgrInterface *bufferMgr = context.bufMgr;

    /* Step 1: Calculate list id of space */
    uint16 newList = GetListId(static_cast<uint16>(space));
    /* Step 2: Read fsm page to get old list id */
    BufferDesc *fsmBuf = bufferMgr->Read(context.pdbId, fsmIndex.page, LW_SHARED);   // Critical-Point
    if (fsmBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }

    /* Return the fsmMetaPageId saved on page if needed */
    FsmPage *fsmPage = static_cast<FsmPage *>(fsmBuf->GetPage());
    StorageReleasePanic(fsmPage == nullptr, MODULE_SEGMENT, ErrMsg("Get fsm page from buffer failed."));
    if (context.fsmMetaPageId != nullptr) {
        *context.fsmMetaPageId = fsmPage->fsmPageHeader.fsmMetaPageId;
        StorageAssert(*context.fsmMetaPageId != INVALID_PAGE_ID);
    }

    /* Step 3: Check new list id and old list id, if equals, return directly */
    uint16 oldList = fsmPage->FsmNodePtr(fsmIndex.slot)->listId;
    if (newList == oldList) {
        SearchPageIdOfChild(fsmPage, spaceNeeded, pageId, retryTimes, nullptr);
        bufferMgr->UnlockAndRelease(fsmBuf);
        *fsmIsChanged = false;
        return DSTORE_SUCC;
    }
    bufferMgr->UnlockAndRelease(fsmBuf);

    /* Step 4: Need to move fsm node, update fsm page */
    *fsmIsChanged = true;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    if (STORAGE_FUNC_FAIL(UpdateFsmPage(context))) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed in atomic wal progress"));
        return DSTORE_FAIL;
    }
    (void)walWriterContext->EndAtomicWal();

    return DSTORE_SUCC;
}

RetStatus PartitionFreeSpaceMap::UpdateFsmPage(FsmUpdateSpaceContext context)
{
    FsmIndex fsmIndex = context.fsmIndex;
    uint16 newList = GetListId(static_cast<uint16>(context.space));
    uint32 spaceNeeded = context.spaceNeeded;
    uint16 retryTimes = context.retryTimes;
    PageId *pageId = context.pageId;
    uint32* spaceInfsm = context.spaceInFsm;
    BufMgrInterface *bufMgr = context.bufMgr;
    uint16 slot = context.fsmIndex.slot;
    PageId fsmPageId = fsmIndex.page;
    BufferDesc *fsmBuf = bufMgr->Read(context.pdbId, fsmPageId, LW_EXCLUSIVE);   // Critical-Point
    if (fsmBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }

    FsmPage *fsmPage = static_cast<FsmPage *>(fsmBuf->GetPage());
    PageId upperFsm = fsmPage->GetUpperFsmPageId();
    uint16 upperSlot = fsmPage->GetUpperSlot();
    FsmIndex upperIndex{upperFsm, upperSlot};
    uint16 curOldMaxListId = fsmPage->GetFsmMaxListId();

    fsmPage->MoveNode(slot, newList);
    uint16 newMaxListId = fsmPage->GetFsmMaxListId();
    SearchPageIdOfChild(fsmPage, spaceNeeded, pageId, retryTimes, spaceInfsm);
    (void)bufMgr->MarkDirty(fsmBuf);

    if (context.needWal) {
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        bool glsnChangedFlag = (fsmPage->GetWalId() != walWriterContext->GetWalId());
        WalRecordTbsMoveFsmSlot walData;
        walData.SetHeader({WAL_TBS_MOVE_FSM_SLOT, sizeof(walData), fsmPageId, fsmPage->GetWalId(), fsmPage->GetPlsn(),
                           fsmPage->GetGlsn(), glsnChangedFlag, fsmBuf->GetFileVersion()});
        walData.SetData(slot, newList);
        walWriterContext->RememberPageNeedWal(fsmBuf);
        walWriterContext->PutNewWalRecord(&walData);
    }
    bufMgr->UnlockAndRelease(fsmBuf);

    /* Check if upper fsm page need to modify */
    if (upperFsm != INVALID_PAGE_ID && curOldMaxListId != newMaxListId) {
        FsmUpdateSpaceContext upperContext = {upperIndex, FSM_SPACE_LINE[newMaxListId], context.fsmIsChanged, 0,
            UINT32_MAX, nullptr, bufMgr, nullptr, nullptr, context.needWal, context.pdbId};
        return UpdateFsmPage(upperContext);
    } else {
        return DSTORE_SUCC;
    }
}

FsmNode *PartitionFreeSpaceMap::SearchChild(uint32 space, FsmPage *currentFsmPage, uint16 retryTime)
{
    /* Calculate min_list and total_slots to fit required space */
    uint16 minFitList = GetListId(static_cast<uint16>(space));
    uint16 minSearchList = DstoreMin(minFitList + retryTime / FSM_SEARCH_UPGRADE_RETRY_TIME, FSM_FREE_LIST_COUNT - 1);

    uint16 totalFitSlot = 0;
    for (uint16 list = minSearchList; list < FSM_FREE_LIST_COUNT; ++list) {
        totalFitSlot += currentFsmPage->FsmListPtr(list)->count;
    }

    if (totalFitSlot == 0) {
        return nullptr; /* Current fsm has no slot fit required space */
    }

    /* Random select one slot which fit required space base on ThreadId */
    uint16 curSearchSeed = (*currentFsmPage->GetSearchSeeds(minSearchList))++;
    uint16 slotIndex = SelectSlotIndex(totalFitSlot, curSearchSeed);
    uint16 selectList;
    for (selectList = minSearchList; selectList < FSM_FREE_LIST_COUNT; ++selectList) {
        if (slotIndex < currentFsmPage->FsmListPtr(selectList)->count) {
            break;
        }
        slotIndex -= currentFsmPage->FsmListPtr(selectList)->count;
    }
    uint16 selectSlot = currentFsmPage->FsmListPtr(selectList)->first;
    while (slotIndex > 0) {
        selectSlot = currentFsmPage->FsmNodePtr(selectSlot)->next;
        slotIndex--;
    }
    return currentFsmPage->FsmNodePtr(selectSlot);
}

uint16 PartitionFreeSpaceMap::SelectSlotIndex(uint16 fitSlotCount, uint16 searchSeed)
{
    return (searchSeed % fitSlotCount);
}

/* To check if we have more than pageCount free pages in the current PartitionFreeSpaceMap */
bool PartitionFreeSpaceMap::HasEnoughUnusedPages(uint64 pageCount)
{
    bool result = false;
    BufferDesc *fsmMetaBuf = ReadFsmMetaPageBuf(LW_SHARED);
    FreeSpaceMapMetaPage *fsmMetaPage = GetFsmMetaPage(fsmMetaBuf);
    result = (fsmMetaPage->GetNumUnusedPages() >= pageCount);
    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
        ErrMsg("HasEnoughUnusedPages: current numTotalPages %lu, current numUnusedPage %lu, required pageCount %lu",
            fsmMetaPage->GetNumTotalPages(), fsmMetaPage->GetNumUnusedPages(), pageCount));

    UnlockAndReleaseFsmMetaPageBuf(fsmMetaBuf);

    return result;
}

uint16 PartitionFreeSpaceMap::GetExtendCoefficient()
{
    uint16 result = MIN_FSM_EXTEND_COEFFICIENT;
    BufferDesc *fsmMetaBuf = ReadFsmMetaPageBuf(LW_SHARED);
    result = GetFsmMetaPage(fsmMetaBuf)->GetExtendCoefficient();
    UnlockAndReleaseFsmMetaPageBuf(fsmMetaBuf);

    return result;
}

RetStatus PartitionFreeSpaceMap::GetFsmStatus(uint16 *freeSlotCount, uint16 *needExtendPageCount)
{
    BufferDesc *fsmMetaBuf = ReadFsmMetaPageBuf(LW_SHARED);  // Critical-Point
    FreeSpaceMapMetaPage *metaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaBuf->GetPage());
    PageId currentFsmMap[HEAP_MAX_MAP_LEVEL];
    for (int i = 0; i < HEAP_MAX_MAP_LEVEL; ++i) {
        currentFsmMap[i] = metaPage->currMap[i];
    }
    UnlockAndReleaseFsmMetaPageBuf(fsmMetaBuf);

    BufferDesc *leafFsmBuf = m_bufMgr->Read(m_pdbId, currentFsmMap[0], LW_SHARED);  // Critical-Point
    if (leafFsmBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }
    FsmPage *leafFsmPage = static_cast<FsmPage *>(leafFsmBuf->GetPage());
    uint16 currentFreeSlotCount = leafFsmPage->GetFreeNodeCount();
    m_bufMgr->UnlockAndRelease(leafFsmBuf);
    if (currentFreeSlotCount > 0) {
        *freeSlotCount = currentFreeSlotCount;
        *needExtendPageCount = 0;
        return DSTORE_SUCC;
    }
    /* Current leaf fsm page has no free slot, calculate page count need by extending fsm */
    uint16 needPageCount = 1;
    for (uint16 level = 1; level < HEAP_MAX_MAP_LEVEL; ++level) {
        if (currentFsmMap[level] == INVALID_PAGE_ID) {
            /* Need to extend a new fsm level */
            needPageCount += 1;
            break;
        }
        BufferDesc *fsmBuf = m_bufMgr->Read(m_pdbId, currentFsmMap[level], LW_SHARED);
        if (fsmBuf == INVALID_BUFFER_DESC) {
            return DSTORE_FAIL;
        }
        FsmPage *fsmPage = static_cast<FsmPage *>(fsmBuf->GetPage());
        uint16 curLevelFreeSlotCount = fsmPage->GetFreeNodeCount();
        m_bufMgr->UnlockAndRelease(fsmBuf);
        /* Current fsm level has free slot to store lower fsm page */
        if (curLevelFreeSlotCount > 0) {
            break;
        }
        /* Current fsm tree is full and cannot extend */
        if (level == HEAP_MAX_MAP_LEVEL - 1) {
            *freeSlotCount = 0;
            return DSTORE_SUCC;
        }
        needPageCount += 1;
    }
    *freeSlotCount = 0;
    *needExtendPageCount = needPageCount;
    return DSTORE_SUCC;
}

RetStatus PartitionFreeSpaceMap::AdjustFsmTree(uint16 needExtendPageCount)
{
    StorageAssert(needExtendPageCount <= HEAP_MAX_MAP_LEVEL);
    /* Step 1: Get new fsm pages from data segment meta page */
    BufferDesc *fsmMetaBuf = ReadFsmMetaPageBuf(LW_EXCLUSIVE);  // Critical-Point
    FreeSpaceMapMetaPage *fsmMetaPage = GetFsmMetaPage(fsmMetaBuf);
    if (!fsmMetaPage->TestType(PageType::FSM_META_PAGE_TYPE) || fsmMetaPage->fsmExtents.last.IsInvalid()) {
        UnlockAndReleaseFsmMetaPageBuf(fsmMetaBuf);
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT, ErrMsg("Adjust fsm tree failed: page has been reused."));
        return DSTORE_FAIL;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;

    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    PageId newFsmPageIdList[HEAP_MAX_MAP_LEVEL];
    for (int i = 0; i < HEAP_MAX_MAP_LEVEL; i++) {
        newFsmPageIdList[i] = INVALID_PAGE_ID;
    }
    StorageAssert(needExtendPageCount <= fsmMetaPage->GetFsmRootLevel() + 2); /* 2 is not magic */
    for (uint16 i = 0; i < needExtendPageCount; ++i) {
        newFsmPageIdList[i] = GetNewFsmPage(fsmMetaPage);
    }
    (void)m_bufMgr->MarkDirty(fsmMetaBuf);
    /* Step 2: Init new fsm pages and add to fsm tree */
    RetStatus ret;
    if (needExtendPageCount == fsmMetaPage->GetFsmRootLevel() + 2) { /* 2 is not magic */
        ret = AdjustFsmTreeWithNewRoot(fsmMetaPage, newFsmPageIdList, needExtendPageCount);
    } else {
        ret = AdjustFsmTreeWithOldRoot(fsmMetaPage, newFsmPageIdList, needExtendPageCount);
    }
    if (ret != DSTORE_SUCC) {
        UnlockAndReleaseFsmMetaPageBuf(fsmMetaBuf);
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Adjust fsm tree failed: extend count %d.", needExtendPageCount));
        return DSTORE_FAIL;
    }
    /* Step 3: Update fsm meta page */
    UpdateFsmTree(needExtendPageCount, newFsmPageIdList, fsmMetaBuf);
    (void)walWriterContext->EndAtomicWal();
    UnlockAndReleaseFsmMetaPageBuf(fsmMetaBuf);
    return DSTORE_SUCC;
}

PageId PartitionFreeSpaceMap::GetNewFsmPage(FreeSpaceMapMetaPage *fsmMetaPage)
{
    if (fsmMetaPage->usedFsmPage != fsmMetaPage->lastFsmPage) {
        fsmMetaPage->usedFsmPage.m_blockId += 1;
    } else {
        StorageAssert(fsmMetaPage->curFsmExtMetaPageId != fsmMetaPage->fsmExtents.last);
        fsmMetaPage->curFsmExtMetaPageId = fsmMetaPage->fsmExtents.last;
        fsmMetaPage->usedFsmPage = {fsmMetaPage->curFsmExtMetaPageId.m_fileId,
                                    fsmMetaPage->curFsmExtMetaPageId.m_blockId + 1};
        fsmMetaPage->lastFsmPage = {fsmMetaPage->curFsmExtMetaPageId.m_fileId,
                                    fsmMetaPage->curFsmExtMetaPageId.m_blockId + static_cast<uint32>(FSM_EXT_SIZE) - 1};
    }

    StorageReleasePanic(fsmMetaPage->usedFsmPage.IsInvalid(), MODULE_SEGMENT,
        ErrMsg("Failed to get new fsm page: usedFsmPage{%u, %u}, lastFsmPage{%u, %u}.",
            fsmMetaPage->usedFsmPage.m_fileId, fsmMetaPage->usedFsmPage.m_blockId,
            fsmMetaPage->lastFsmPage.m_fileId, fsmMetaPage->lastFsmPage.m_blockId));

    return fsmMetaPage->usedFsmPage;
}

RetStatus PartitionFreeSpaceMap::AdjustFsmTreeWithNewRoot(FreeSpaceMapMetaPage *fsmMetaPage, PageId *newFsmPageIdList,
                                                          uint16 pageCount)
{
    StorageReleasePanic(fsmMetaPage->GetFsmRootLevel() >= HEAP_MAX_MAP_LEVEL, MODULE_SEGMENT,
                        ErrMsg("FsmRootLevel OutOfBound"));
    /* Step 1: Add new root fsm page and update old root fsm page upper index */
    PageId oldRootFsmPageId = fsmMetaPage->currMap[fsmMetaPage->GetFsmRootLevel()];
    PageId newRootFsmPageId = newFsmPageIdList[0];
    BufferDesc *oldRootFsmBuf = m_bufMgr->Read(m_pdbId, oldRootFsmPageId, LW_EXCLUSIVE);
    if (oldRootFsmBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }
    uint16 oldRootFsmMaxList = static_cast<FsmPage *>(oldRootFsmBuf->GetPage())->GetFsmMaxListId();
    FsmIndex oldRootUpperIndex = {newRootFsmPageId, 0};
    BufferDesc *newRootFsmBuf =
        m_bufMgr->Read(m_pdbId, newRootFsmPageId, LW_EXCLUSIVE, BufferPoolReadFlag::CreateNewPage());
    if (newRootFsmBuf == INVALID_BUFFER_DESC) {
        m_bufMgr->UnlockAndRelease(oldRootFsmBuf);
        return DSTORE_FAIL;
    }
    UpdateUpperFsmIndex(oldRootFsmBuf, oldRootUpperIndex);
    FsmIndex newRootUpperIndex = {INVALID_PAGE_ID, INVALID_FSM_SLOT_NUM};
    PageId newRootChildPageList[2] = {oldRootFsmPageId, newFsmPageIdList[1]};
    uint16 newRootChildListId[2] = {oldRootFsmMaxList, 0};
    RetStatus ret = InitFsmPage(newRootFsmBuf, newRootUpperIndex, 2, newRootChildPageList, newRootChildListId);
    m_bufMgr->UnlockAndRelease(oldRootFsmBuf);
    m_bufMgr->UnlockAndRelease(newRootFsmBuf);
    if (ret != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }
    /* Step 2: Init remaining new fsm page */
    for (uint16 i = 1; i < pageCount; ++i) {
        BufferDesc *fsmBuf =
            m_bufMgr->Read(m_pdbId, newFsmPageIdList[i], LW_EXCLUSIVE, BufferPoolReadFlag::CreateNewPage());
        if (fsmBuf == INVALID_BUFFER_DESC) {
            return DSTORE_FAIL;
        }
        uint16 upperSlot = 0;
        if (i == 1) {
            upperSlot = 1;
        }
        PageId upperFsmPageId = newFsmPageIdList[i - 1];
        FsmIndex upperFsmIndex = {upperFsmPageId, upperSlot};
        if (i == pageCount - 1) {
            ret = InitFsmPage(fsmBuf, upperFsmIndex, 0);
        } else {
            PageId addPageId = newFsmPageIdList[i + 1];
            uint16 addListId = 0;
            ret = InitFsmPage(fsmBuf, upperFsmIndex, 1, &addPageId, &addListId);
        }
        m_bufMgr->UnlockAndRelease(fsmBuf);
        if (ret != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus PartitionFreeSpaceMap::AdjustFsmTreeWithOldRoot(FreeSpaceMapMetaPage *fsmMetaPage, PageId *newFsmPageIdList,
                                                          uint16 pageCount)
{
    StorageAssert(pageCount < HEAP_MAX_MAP_LEVEL);
    PageId addSlotFsmPageId = fsmMetaPage->currMap[pageCount];
    StorageAssert(addSlotFsmPageId != INVALID_PAGE_ID);
    BufferDesc *fsmBuf = m_bufMgr->Read(m_pdbId, addSlotFsmPageId, LW_EXCLUSIVE);
    if (fsmBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }
    FsmPage *fsmPage = static_cast<FsmPage *>(fsmBuf->GetPage());
    StorageAssert(fsmPage->GetFreeNodeCount() > 0);
    uint16 addSlot = fsmPage->AddNode(newFsmPageIdList[0], 0);
    (void)m_bufMgr->MarkDirty(fsmBuf);
    if (NeedWal()) {
        uint32 walSize = sizeof(WalRecordTbsAddMultiplePagesToFsmSlots) + 1 * sizeof(PageId);
        WalRecordTbsAddMultiplePagesToFsmSlots *walDataPtr =
            (WalRecordTbsAddMultiplePagesToFsmSlots *)DstorePalloc(walSize);
        if (walDataPtr == nullptr) {
            m_bufMgr->UnlockAndRelease(fsmBuf);
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to palloc for walDataPtr."));
            return DSTORE_FAIL;
        }
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        bool glsnChangedFlag = (fsmPage->GetWalId() != walWriterContext->GetWalId());
        walDataPtr->SetHeader({WAL_TBS_ADD_FSM_SLOT, walSize, addSlotFsmPageId, fsmPage->GetWalId(), fsmPage->GetPlsn(),
                               fsmPage->GetGlsn(), glsnChangedFlag, fsmBuf->GetFileVersion()});
        walDataPtr->SetData(1, addSlot, 0, newFsmPageIdList);
        walWriterContext->RememberPageNeedWal(fsmBuf);
        walWriterContext->PutNewWalRecord(walDataPtr);
        DstorePfreeExt(walDataPtr);
    }
    m_bufMgr->UnlockAndRelease(fsmBuf);

    for (uint16 i = 0; i < pageCount; ++i) {
        fsmBuf =
            m_bufMgr->Read(m_pdbId, newFsmPageIdList[i], LW_EXCLUSIVE, BufferPoolReadFlag::CreateNewPage());
        if (fsmBuf == INVALID_BUFFER_DESC) {
            return DSTORE_FAIL;
        }
        FsmIndex upperFsmIndex;
        if (i == 0) {
            upperFsmIndex = {addSlotFsmPageId, addSlot};
        } else {
            upperFsmIndex = {newFsmPageIdList[i - 1], 0};
        }
        RetStatus ret;
        uint16 addedPageCount = 0;
        if (i == pageCount - 1) {
            ret = InitFsmPage(fsmBuf, upperFsmIndex, addedPageCount);
        } else {
            PageId addedPageId = newFsmPageIdList[i + 1];
            uint16 addedListId = 0;
            addedPageCount = 1;
            ret = InitFsmPage(fsmBuf, upperFsmIndex, addedPageCount, &addedPageId, &addedListId);
        }
        m_bufMgr->UnlockAndRelease(fsmBuf);
        if (ret != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

void PartitionFreeSpaceMap::UpdateFsmTree(uint16 needExtendPageCount, PageId *pageIdList, BufferDesc *fsmMetaBuf)
{
    FreeSpaceMapMetaPage *fsmMetaPage = GetFsmMetaPage(fsmMetaBuf);
    StorageAssert(needExtendPageCount <= fsmMetaPage->GetFsmRootLevel() + 2); /* 2 is not magic */
    /* The root node of the tree needs to be adjusted. */
    fsmMetaPage->UpdateFsmTree(needExtendPageCount, pageIdList);
    if (!NeedWal()) {
        return;
    }

    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    bool glsnChangedFlag = (fsmMetaPage->GetWalId() != walWriterContext->GetWalId());
    WalRecordTbsFsmMetaUpdateFsmTree walData;
    walData.SetHeader({WAL_TBS_FSM_META_UPDATE_FSM_TREE, sizeof(walData), m_fsmMetaPageId, fsmMetaPage->GetWalId(),
                       fsmMetaPage->GetPlsn(), fsmMetaPage->GetGlsn(), glsnChangedFlag, fsmMetaBuf->GetFileVersion()});
    walData.SetData(fsmMetaPage);
    walWriterContext->RememberPageNeedWal(fsmMetaBuf);
    walWriterContext->PutNewWalRecord(&walData);
}

void PartitionFreeSpaceMap::UpdateUpperFsmIndex(BufferDesc *fsmDescBuf, const FsmIndex &upperFsmIndex)
{
    FsmPage *fsmPage = static_cast<FsmPage *>(fsmDescBuf->GetPage());
    fsmPage->fsmPageHeader.upperIndex = upperFsmIndex;
    (void)m_bufMgr->MarkDirty(fsmDescBuf);
    if (!NeedWal()) {
        return;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    bool glsnChangedFlag = (fsmPage->GetWalId() != walWriterContext->GetWalId());
    WalRecordTbsUpdateFsmIndex walData;
    walData.SetHeader({WAL_TBS_MODIFY_FSM_INDEX, sizeof(walData), fsmDescBuf->GetPageId(), fsmPage->GetWalId(),
                       fsmPage->GetPlsn(), fsmPage->GetGlsn(), glsnChangedFlag, fsmDescBuf->GetFileVersion()});
    walData.SetData(upperFsmIndex);
    walWriterContext->RememberPageNeedWal(fsmDescBuf);
    walWriterContext->PutNewWalRecord(&walData);
}

RetStatus PartitionFreeSpaceMap::InitFsmPage(BufferDesc *fsmBuf, const FsmIndex &upperFsmIndex, uint16 addPageCount,
                                             PageId *addPageList, uint16 *addListId)
{
    FsmPage *fsmPage = static_cast<FsmPage *>(fsmBuf->GetPage());
    fsmPage->InitFsmPage(fsmBuf->GetPageId(), GetFsmMetaPageId(), upperFsmIndex);

    for (uint16 i = 0; i < addPageCount; ++i) {
        (void)fsmPage->AddNode(addPageList[i], addListId[i]);
    }
    (void)m_bufMgr->MarkDirty(fsmBuf);
    if (!NeedWal()) {
        return DSTORE_SUCC;
    }

    /* Prepare wal data space */
    uint32 walSize = sizeof(WalRecordTbsInitFsmPage) + addPageCount * sizeof(FsmNode);
    WalRecordTbsInitFsmPage *walDataPtr = (WalRecordTbsInitFsmPage *)DstorePalloc(walSize);
    if (walDataPtr == nullptr) {
        return DSTORE_FAIL;
    }

    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    bool glsnChangedFlag = (fsmPage->GetWalId() != walWriterContext->GetWalId());
    walDataPtr->SetHeader({WAL_TBS_INIT_FSM_PAGE, walSize, fsmBuf->GetPageId(), fsmPage->GetWalId(), fsmPage->GetPlsn(),
                           fsmPage->GetGlsn(), glsnChangedFlag, fsmBuf->GetFileVersion()});
    FsmNode *fsmNodeData = reinterpret_cast<FsmNode *>(fsmPage->data);
    walDataPtr->SetData(GetFsmMetaPageId(), upperFsmIndex, addPageCount, fsmNodeData);
    walWriterContext->RememberPageNeedWal(fsmBuf);
    walWriterContext->PutNewWalRecord(walDataPtr);
    DstorePfreeExt(walDataPtr);
    return DSTORE_SUCC;
}

RetStatus PartitionFreeSpaceMap::UpdateSlotFreeSpace(const PageId &fsmPageId, uint16 slotId, uint16 newListId)
{
    BufferDesc *fsmBuf = m_bufMgr->Read(m_pdbId, fsmPageId, LW_EXCLUSIVE);
    if (fsmBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }
    FsmPage *fsmPage = static_cast<FsmPage *>(fsmBuf->GetPage());
    PageId upperFsmPageId = fsmPage->GetUpperFsmPageId();
    uint16 upperSlotId = fsmPage->GetUpperSlot();
    uint16 oldMaxListId = fsmPage->GetFsmMaxListId();
    fsmPage->MoveNode(slotId, newListId);
    uint16 newMaxListId = fsmPage->GetFsmMaxListId();
    (void)m_bufMgr->MarkDirty(fsmBuf);

    if (NeedWal()) {
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        bool glsnChangedFlag = (fsmPage->GetWalId() != walWriterContext->GetWalId());
        WalRecordTbsMoveFsmSlot walData;
        walData.SetHeader({WAL_TBS_MOVE_FSM_SLOT, sizeof(walData), fsmPageId, fsmPage->GetWalId(), fsmPage->GetPlsn(),
                           fsmPage->GetGlsn(), glsnChangedFlag, fsmBuf->GetFileVersion()});
        walData.SetData(slotId, newListId);
        walWriterContext->RememberPageNeedWal(fsmBuf);
        walWriterContext->PutNewWalRecord(&walData);
    }

    m_bufMgr->UnlockAndRelease(fsmBuf);
    /* Check if upper fsm page need to modify */
    if (upperFsmPageId != INVALID_PAGE_ID && oldMaxListId != newMaxListId) {
        return UpdateSlotFreeSpace(upperFsmPageId, upperSlotId, newMaxListId);
    } else {
        return DSTORE_SUCC;
    }
}

RetStatus PartitionFreeSpaceMap::AddMultipleNewPageToFsm(const PageId &leafFsmPageId, uint16 pageCount,
                                                         DSTORE::PageId *pageIdList)
{
    uint16 targetListId = FULL_SPACE_LIST_ID;
    BufferDesc *fsmBuf = m_bufMgr->Read(m_pdbId, leafFsmPageId, LW_EXCLUSIVE);    // Critical-Point
    if (fsmBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }
    FsmPage *fsmPage = static_cast<FsmPage *>(fsmBuf->GetPage());
    PageId upperFsmPageId = fsmPage->GetUpperFsmPageId();
    uint16 upperSlotId = fsmPage->GetUpperSlot();
    uint16 oldMaxListId = fsmPage->GetFsmMaxListId();
    uint16 firstSlotId = fsmPage->AddMultiNode(pageIdList, pageCount, targetListId);
    uint16 newMaxListId = fsmPage->GetFsmMaxListId();
    (void)m_bufMgr->MarkDirty(fsmBuf);

    if (NeedWal()) {
        uint32 walSize = sizeof(WalRecordTbsAddMultiplePagesToFsmSlots) + pageCount * sizeof(PageId);
        WalRecordTbsAddMultiplePagesToFsmSlots *walDataPtr =
            (WalRecordTbsAddMultiplePagesToFsmSlots *)DstorePalloc(walSize);
        if (walDataPtr == nullptr) {
            m_bufMgr->UnlockAndRelease(fsmBuf);
            return DSTORE_FAIL;
        }

        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        bool glsnChangedFlag = (fsmPage->GetWalId() != walWriterContext->GetWalId());
        walDataPtr->SetHeader({WAL_TBS_ADD_FSM_SLOT, walSize, leafFsmPageId, fsmPage->GetWalId(), fsmPage->GetPlsn(),
                               fsmPage->GetGlsn(), glsnChangedFlag, fsmBuf->GetFileVersion()});
        walDataPtr->SetData(pageCount, firstSlotId, targetListId, pageIdList);
        walWriterContext->RememberPageNeedWal(fsmBuf);
        walWriterContext->PutNewWalRecord(walDataPtr);
        DstorePfreeExt(walDataPtr);
    }

    m_bufMgr->UnlockAndRelease(fsmBuf);

    /* Check if upper fsm page need to modify */
    if (upperFsmPageId != INVALID_PAGE_ID && oldMaxListId != newMaxListId) {
        return UpdateSlotFreeSpace(upperFsmPageId, upperSlotId, newMaxListId);
    } else {
        return DSTORE_SUCC;
    }
}

RetStatus PartitionFreeSpaceMap::GetFirstFreeFsmIndex(const PageId &fsmPageId, FsmIndex *firstFreeFsmIndex)
{
    BufferDesc *fsmBuf = m_bufMgr->Read(m_pdbId, fsmPageId, LW_SHARED);
    if (fsmBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }
    FsmPage *fsmPage = static_cast<FsmPage *>(fsmBuf->GetPage());
    StorageAssert(fsmPage->fsmPageHeader.hwm < FSM_MAX_HWM);
    *firstFreeFsmIndex = {fsmPageId, fsmPage->fsmPageHeader.hwm};
    m_bufMgr->UnlockAndRelease(fsmBuf);
    return DSTORE_SUCC;
}

void PartitionFreeSpaceMap::UpdateFsmStatAfterExtend(uint64 pageCount, bool needUpdateExtendCoeff)
{
    /* Step 1: Update the stat in FreeSpaceMapMetaPage */
    BufferDesc *fsmMetaBufDesc = ReadFsmMetaPageBuf(LW_EXCLUSIVE);
    FreeSpaceMapMetaPage *fsmMetaPage = GetFsmMetaPage(fsmMetaBufDesc);
    StorageReleasePanic(fsmMetaPage == nullptr, MODULE_SEGMENT,
                        ErrMsg("UpdateFsmStatAfterExtend: Get fsm meta page failed."));
    fsmMetaPage->numTotalPages += pageCount;
    if (needUpdateExtendCoeff) {
        fsmMetaPage->extendCoefficient = DstoreMin(MAX_FSM_EXTEND_COEFFICIENT, fsmMetaPage->extendCoefficient + 1);
    }
    (void)m_bufMgr->MarkDirty(fsmMetaBufDesc);

    /* Step 2: Write WAL here */
    if (NeedWal()) {
        WalRecordTbsFsmMetaUpdateExtensionStat fsmMetaUpdateExtensionStatWalData;
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        bool glsnChangedFlag = (fsmMetaPage->GetWalId() != walWriterContext->GetWalId());
        fsmMetaUpdateExtensionStatWalData.SetHeader(
            {WAL_TBS_FSM_META_UPDATE_EXTENSION_STAT, sizeof(fsmMetaUpdateExtensionStatWalData), m_fsmMetaPageId,
             fsmMetaPage->GetWalId(), fsmMetaPage->GetPlsn(), fsmMetaPage->GetGlsn(), glsnChangedFlag,
             fsmMetaBufDesc->GetFileVersion()});
        fsmMetaUpdateExtensionStatWalData.SetData(fsmMetaPage->numTotalPages, fsmMetaPage->extendCoefficient);
        walWriterContext->RememberPageNeedWal(fsmMetaBufDesc);
        walWriterContext->PutNewWalRecord(&fsmMetaUpdateExtensionStatWalData);
    }

    UnlockAndReleaseFsmMetaPageBuf(fsmMetaBufDesc);
}

#ifdef UT
RetStatus PartitionFreeSpaceMap::MarkFreeSpaceMapExpired(BufMgrInterface *bufMgr, PageId fsmMetaPageId, PdbId pdbId)
{
    BufferDesc *fsmMetaBuf = bufMgr->Read(pdbId, fsmMetaPageId, LW_EXCLUSIVE);
    if (fsmMetaBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }

    /* Mark the accessTimestamp to be old enough here */
    TimestampTz recycleTimeThreshold =
        static_cast<TimestampTz>(g_storageInstance->GetGuc()->recycleFsmTimeInterval) * STORAGE_SECS_PER_MIN;
    FreeSpaceMapMetaPage *fsmMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaBuf->GetPage());
    StorageAssert(fsmMetaPage->accessTimestamp >= recycleTimeThreshold);
    fsmMetaPage->accessTimestamp -= recycleTimeThreshold;

    (void)bufMgr->MarkDirty(fsmMetaBuf);
    bufMgr->UnlockAndRelease(fsmMetaBuf);

    return DSTORE_SUCC;
}
#endif

FreeSpaceMapNode::FreeSpaceMapNode(PartitionFreeSpaceMap *fsm) : m_fsm(fsm)
{
    DListNodeInit(&m_node);
}

FreeSpaceMapNode::~FreeSpaceMapNode()
{
    if (m_fsm != nullptr) {
        delete m_fsm;
        m_fsm = nullptr;
    }
}

FreeSpaceMapList::FreeSpaceMapList()
{
    DListInit(&m_list);
}

FreeSpaceMapList::~FreeSpaceMapList()
{
    dlist_mutable_iter iter;
    FreeSpaceMapNode *curNode = nullptr;
    dlist_foreach_modify(iter, &m_list)
    {
        curNode = dlist_container(FreeSpaceMapNode, m_node, iter.cur);
        DListDelete(iter.cur);
        delete curNode;
    }
}

FreeSpaceMapNode *FreeSpaceMapList::GetFreeSpaceMapForSpace()
{
    FreeSpaceMapNode *result = nullptr;

    /* Check if we have usable FSM first */
    if (DListIsEmpty(&m_list)) {
        storage_set_error(TBS_ERROR_FREESPACEMAP_ASSIGNMENT_FAILED);
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("FreeSpaceMapList: No usable FSM for current Segment"));
        return result;
    }

    result = dlist_head_element(FreeSpaceMapNode, m_node, &m_list);

    return result;
}

FreeSpaceMapNode *FreeSpaceMapList::SearchFreeSpaceMap(const PageId fsmMetaPageId)
{
    FreeSpaceMapNode *result = nullptr;

    /* Check if we have usable FSM first */
    if (DListIsEmpty(&m_list)) {
        storage_set_error(TBS_ERROR_FREESPACEMAP_ASSIGNMENT_FAILED);
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("FreeSpaceMapList: No usable FSM for current Segment"));
        return result;
    }

    /* If fsmMetaPageId is INVALID_PAGE_ID, just return the first FSM */
    if (fsmMetaPageId == INVALID_PAGE_ID) {
        result = dlist_head_element(FreeSpaceMapNode, m_node, &m_list);
    } else {
        /* Otherwise, find the designated FSM for extension */
        dlist_iter iter;
        FreeSpaceMapNode *curNode = nullptr;
        bool found = false;
        dlist_foreach(iter, &m_list)
        {
            curNode = dlist_container(FreeSpaceMapNode, m_node, iter.cur);
            if (curNode->IsCurrentFsm(fsmMetaPageId)) {
                found = true;
                break;
            }
        }

        if (found) {
            result = curNode;
        } else {
            /* If we cannot find it in the List, that means this FSM is recycled. Just return null here */
            result = nullptr;
        }
    }

    return result;
}

/*
 * SINGLEOPT_TODO: Current usage on FSM/Segment is per-thread level;
 * therefore, No concurrency control is needed. May consider adding it if anything changed
 */
void FreeSpaceMapList::MoveFreeSpaceMapToEnd(FreeSpaceMapNode *fsmNode)
{
    /* Check if the Queue is empty first */
    if (DListIsEmpty(&m_list)) {
        /* We should have at least given fsmNode in the list here */
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT,
               ErrMsg("FreeSpaceMapList is empty while we expect the given FreeSpaceMapNode at least."));
        return;
    }

    /* Move the node if this node is not at the end of the list */
    if (dlist_tail_element(FreeSpaceMapNode, m_node, &m_list) != fsmNode) {
        DListDelete(&fsmNode->m_node);
        DListPushTail(&m_list, &fsmNode->m_node);
    }
}

void FreeSpaceMapList::MoveFreeSpaceMapToFront(FreeSpaceMapNode *fsmNode)
{
    /* Check if the Queue is empty first */
    if (DListIsEmpty(&m_list)) {
        /* We should have at least given fsmNode in the list here */
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT,
               ErrMsg("FreeSpaceMapList is empty while we expect the given FreeSpaceMapNode at least."));
        return;
    }

    /* Move the node if this node is not in the front of the list */
    if (dlist_head_element(FreeSpaceMapNode, m_node, &m_list) != fsmNode) {
        DListDelete(&fsmNode->m_node);
        DListPushHead(&m_list, &fsmNode->m_node);
    }
}

RetStatus FreeSpaceMapList::LoadNewFreeSpaceMap(const PageId segMetaPageId, const PageId fsmMetaPageId,
                                                BufMgrInterface *bufMgr, const TablespaceId tablespaceId, PdbId pdbId)
{
    /* Initialize and add a new node to the list */
    PartitionFreeSpaceMap *fsm = DstoreNew(g_dstoreCurrentMemoryContext)
        PartitionFreeSpaceMap(segMetaPageId, fsmMetaPageId, bufMgr, tablespaceId, pdbId);
    if (fsm == nullptr) {
        ErrLog(
            DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Heap data segment (%hu, %u) failed to initialize PartitionFreeSpaceMap (%hu, %u).",
                   segMetaPageId.m_fileId, segMetaPageId.m_blockId, fsmMetaPageId.m_fileId, fsmMetaPageId.m_blockId));
        return DSTORE_FAIL;
    }

    FreeSpaceMapNode *fsmNode = DstoreNew(g_dstoreCurrentMemoryContext) FreeSpaceMapNode(fsm);
    if (fsmNode == nullptr) {
        delete fsm;
        ErrLog(
            DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Heap data segment (%hu, %u) failed to initialize FreeSpaceMapNode for FSM (%hu, %u).",
                   segMetaPageId.m_fileId, segMetaPageId.m_blockId, fsmMetaPageId.m_fileId, fsmMetaPageId.m_blockId));
        return DSTORE_FAIL;
    }

    DListPushHead(&m_list, &fsmNode->m_node);

    return DSTORE_SUCC;
}

FreeSpaceMapDiagnose::FreeSpaceMapDiagnose(BufMgrInterface *bufMgr, PdbId pdbId, PageId fsmMetaPageId)
    : m_firstFsmRootPageId(INVALID_PAGE_ID), m_numFsmLevels(0), m_curFsmLevel(0), m_slotId(0)
{
    m_bufMgr = bufMgr;
    m_pdbId = pdbId;
    m_fsmMetaPageId = fsmMetaPageId;
    m_cachedFsmPages = nullptr;
    m_pfs = nullptr;
}

bool FreeSpaceMapDiagnose::Init()
{
    BufferDesc *fsmMetaBuf = m_bufMgr->Read(m_pdbId, m_fsmMetaPageId, LW_SHARED);
    if (fsmMetaBuf == INVALID_BUFFER_DESC) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("FSM diagnose read page(%hu, %u) failed.",
            m_fsmMetaPageId.m_fileId, m_fsmMetaPageId.m_blockId));
        storage_set_error(HEAP_ERROR_GET_BUFFER_FAILED);
        return false;
    }
    FreeSpaceMapMetaPage *fsmMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaBuf->GetPage());
    m_firstFsmRootPageId = fsmMetaPage->GetfirstFsmRoot();
    m_numFsmLevels = fsmMetaPage->GetFsmRootLevel();
    m_bufMgr->UnlockAndRelease(fsmMetaBuf);
    return true;
}

void FreeSpaceMapDiagnose::Bind(FsmPage *fsmPageCache, PageFreespace *pfs)
{
    m_cachedFsmPages = fsmPageCache;
    m_pfs = pfs;
}

bool FreeSpaceMapDiagnose::CacheOneFsmPage(PageId fsmPageId, uint8 cacheIdx)
{
    BufferDesc *fsmBuf = m_bufMgr->Read(m_pdbId, fsmPageId, LW_SHARED);
    if (fsmBuf == INVALID_BUFFER_DESC) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("FSM diagnose cache page(%hu, %u) failed.",
            fsmPageId.m_fileId, fsmPageId.m_blockId));
        storage_set_error(HEAP_ERROR_GET_BUFFER_FAILED);
        return false;
    }
    errno_t rc = memcpy_s(static_cast<void*>(&m_cachedFsmPages[cacheIdx]), BLCKSZ,
                          static_cast<void*>(fsmBuf->GetPage()), BLCKSZ);
    StorageReleasePanic(rc != 0, MODULE_SEGMENT, ErrMsg("cache fsm first root page failed!"));

    m_bufMgr->UnlockAndRelease(fsmBuf);
    return true;
}

bool FreeSpaceMapDiagnose::Begin()
{
    m_slotId = 0;
    m_curFsmLevel = 0;

    StorageAssert(m_firstFsmRootPageId != INVALID_PAGE_ID);
    return CacheOneFsmPage(m_firstFsmRootPageId, 0);
}

bool FreeSpaceMapDiagnose::HasNext()
{
    /* the leaf FsmPage has more pages. */
    if (m_slotId < m_cachedFsmPages[m_curFsmLevel].fsmPageHeader.hwm) {
        return true;
    }

    /* still in subtree of the fsmtree */
    if (m_curFsmLevel < m_numFsmLevels) {
        return true;
    }

    /* each level's right-most page has any slot */
    uint8 level = m_numFsmLevels;
    while (level > 0) {
        if (m_cachedFsmPages[level].GetUpperSlot() + 1 < m_cachedFsmPages[level - 1].fsmPageHeader.hwm) {
            return true;
        }
        level--;
    }

    return false;
}

DiagnoseItem *FreeSpaceMapDiagnose::GetNext()
{
    for (;;) {
        if (m_slotId < m_cachedFsmPages[m_curFsmLevel].fsmPageHeader.hwm) {
            FsmNode* fsmNode = m_cachedFsmPages[m_curFsmLevel].FsmNodePtr(m_slotId);
            m_pfs->SetPageId(fsmNode->page);
            m_pfs->SetSpaceline(fsmNode->listId);
            m_slotId++;
            return m_pfs;
        }

        /* dequeue subtree of fsmtree */
        uint8 level = m_curFsmLevel;
        while (level > 0) {
            if (m_cachedFsmPages[level].GetUpperSlot() + 1 != m_cachedFsmPages[level - 1].fsmPageHeader.hwm) {
                break;
            }
            level--;
        }

        /* load new subtree */
        uint16 newSubtreeSlot = 0;
        if (level > 0) {
            /* subtree[0, level] was done, load more a subtree of parent[0, level-1] */
            newSubtreeSlot = m_cachedFsmPages[level].GetUpperSlot() + 1;
        } else if (m_curFsmLevel < m_numFsmLevels) {
            /* subtree[0, curFsmLevel] was done, init for subtree[0,curFsmLevel+1] */
            StorageAssert(m_cachedFsmPages[0].GetUpperSlot() == 0);
            if (!CacheOneFsmPage(m_cachedFsmPages[0].GetUpperFsmPageId(), 0)) {
                return nullptr;
            }
            ++m_curFsmLevel;

            /* load the second subtree of parent[0, curFsmLevel] */
            newSubtreeSlot = 1; /* hwm in upper level can be equal to 1? */
            level = 1;  /* to load fsmpage from */
        } else {
            return nullptr;
        }

        /* load the subtree until leaf */
        while (level <= m_curFsmLevel) {
            if (!CacheOneFsmPage(m_cachedFsmPages[level - 1].FsmNodePtr(newSubtreeSlot)->page, level)) {
                return nullptr;
            }
            newSubtreeSlot = 0;
            level++;
        }

        /* process a new leaf FsmPage */
        m_slotId = 0;
    }
}

void FreeSpaceMapDiagnose::End()
{
}
} /* namespace DSTORE */
