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
 * dstore_relation_space.cpp
 *
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_relation_space.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tablespace/dstore_relation_space.h"
#include "index/dstore_btree.h"
#include "framework/dstore_pdb.h"
#include "common/log/dstore_log.h"
#include "lock/dstore_table_lock_mgr.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_index_page.h"
#include "framework/dstore_thread.h"
#include "framework/dstore_instance.h"
#include "tablespace/dstore_tablespace.h"
#include "table/dstore_table_interface.h"
#include "tablespace/dstore_heap_normal_segment.h"
#include "tablespace/dstore_heap_temp_segment.h"
#include "transaction/dstore_transaction.h"
#include "index/dstore_btree_page_recycle.h"
#include "lock/dstore_lock_interface.h"

namespace DSTORE {

ObjSpaceMgrExtendTaskInfo::ObjSpaceMgrExtendTaskInfo(PdbId pdbId, const PageId heapSegMetaPageId,
    const TablespaceId tablespaceId, const PageId segmentId, const PageId fsmMetaPageId)
    : ObjSpaceMgrTaskInfo(pdbId, EXTEND_TASK, tablespaceId, segmentId),
      m_fsmMetaPageId(fsmMetaPageId),
      m_heapSegMetaPageId(heapSegMetaPageId)
{
}

ObjSpaceMgrExtendTaskInfo::ObjSpaceMgrExtendTaskInfo(ObjSpaceMgrTaskInfo *taskInfo)
    : ObjSpaceMgrTaskInfo(taskInfo), m_fsmMetaPageId(INVALID_PAGE_ID), m_heapSegMetaPageId(INVALID_PAGE_ID)
{
    StorageAssert(taskInfo->GetTaskType() == EXTEND_TASK);
    ObjSpaceMgrExtendTaskInfo *extendInfo = static_cast<ObjSpaceMgrExtendTaskInfo *>(taskInfo);
    m_fsmMetaPageId = extendInfo->m_fsmMetaPageId;
    m_heapSegMetaPageId = extendInfo->m_heapSegMetaPageId;
}

ObjSpaceMgrRecycleBtreeTaskInfo::ObjSpaceMgrRecycleBtreeTaskInfo(PdbId pdbId, TablespaceId tablespaceId,
    const PageId segmentId, const Xid createdXid, IndexInfo *indexInfo, ScanKey scanKey)
    : ObjSpaceMgrTaskInfo(pdbId, RECYCLE_BTREE_TASK, tablespaceId, segmentId),
      m_createdXid(createdXid), m_indexInfo(indexInfo), m_scanKey(scanKey)
{
}

ObjSpaceMgrRecycleBtreeTaskInfo::ObjSpaceMgrRecycleBtreeTaskInfo(ObjSpaceMgrTaskInfo *taskInfo)
    : ObjSpaceMgrTaskInfo(taskInfo)
{
    StorageAssert(GetTaskType() == RECYCLE_BTREE_TASK);
    ObjSpaceMgrRecycleBtreeTaskInfo *recycleBtreeInfo = static_cast<ObjSpaceMgrRecycleBtreeTaskInfo *>(taskInfo);

    /* Do a deep copy of IndexInfo and ScanKey here */
    m_createdXid = recycleBtreeInfo->m_createdXid;
    m_indexInfo = Btree::CopyIndexInfo(recycleBtreeInfo->m_indexInfo);
    if (STORAGE_VAR_NULL(m_indexInfo)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to copy index info."));
        m_scanKey = nullptr;
    } else {
        m_scanKey = Btree::CopyScanKeys(recycleBtreeInfo->m_scanKey, m_indexInfo->indexKeyAttrsNum);
    }
}

ObjSpaceMgrReclaimBtrRecyclePartTaskInfo::ObjSpaceMgrReclaimBtrRecyclePartTaskInfo(PdbId pdbId,
    const TablespaceId tablespaceId, const PageId segmentId, const Xid createdXid)
    : ObjSpaceMgrTaskInfo(pdbId, RECLAIM_BTREE_RECYCLE_PARTITION_TASK, tablespaceId, segmentId),
      m_createdXid(createdXid)
{}

ObjSpaceMgrReclaimBtrRecyclePartTaskInfo::ObjSpaceMgrReclaimBtrRecyclePartTaskInfo(ObjSpaceMgrTaskInfo *taskInfo)
    : ObjSpaceMgrTaskInfo(taskInfo)
{
    StorageAssert(GetTaskType() == RECLAIM_BTREE_RECYCLE_PARTITION_TASK);
}

ObjSpaceMgrRecycleFsmTaskInfo::ObjSpaceMgrRecycleFsmTaskInfo(PdbId pdbId, const TablespaceId tablespaceId,
    const PageId segmentId)
    : ObjSpaceMgrTaskInfo(pdbId, RECYCLE_FSM_TASK, tablespaceId, segmentId)
{
}

ObjSpaceMgrRecycleFsmTaskInfo::ObjSpaceMgrRecycleFsmTaskInfo(ObjSpaceMgrTaskInfo *taskInfo)
    : ObjSpaceMgrTaskInfo(taskInfo)
{
    StorageAssert(GetTaskType() == RECYCLE_FSM_TASK);
}
ObjSpaceMgrExtendIndexTaskInfo::ObjSpaceMgrExtendIndexTaskInfo(PdbId pdbId, const TablespaceId tablespaceId,
                                                               const PageId segmentId, PageId recyclePartitionMeta,
                                                               Xid createdXid)
    : ObjSpaceMgrTaskInfo(pdbId, EXTEND_INDEX_TASK, tablespaceId, segmentId),
      m_recyclePartitionMeta(recyclePartitionMeta),
      m_createdXid(createdXid)
{
}
ObjSpaceMgrExtendIndexTaskInfo::ObjSpaceMgrExtendIndexTaskInfo(ObjSpaceMgrTaskInfo *taskInfo)
    : ObjSpaceMgrTaskInfo(taskInfo)
{
    StorageAssert(taskInfo->GetTaskType() == EXTEND_INDEX_TASK);
    ObjSpaceMgrExtendIndexTaskInfo *extendInfo = static_cast<ObjSpaceMgrExtendIndexTaskInfo *>(taskInfo);
    m_recyclePartitionMeta = extendInfo->m_recyclePartitionMeta;
    m_createdXid = extendInfo->m_createdXid;
}
bool ObjSpaceMgrTask::IsCurrentTask(ObjSpaceMgrTaskInfo *taskInfo)
{
    StorageAssert(taskInfo != nullptr);

    /* return false directly if different basic information */
    if (taskInfo->GetTaskType() != m_taskInfo->GetTaskType() ||
        taskInfo->GetTablespaceId() != m_taskInfo->GetTablespaceId() ||
        taskInfo->GetSegmentId() != m_taskInfo->GetSegmentId()) {
        return false;
    }

    /* Now, check for specific fields based on taskType */
    bool result = true;
    TaskExtraInfo extraInfo;
    TaskExtraInfo currTaskInfo;
    ObjSpaceMgrTask::GetTaskExtraInfo(taskInfo, extraInfo);
    ObjSpaceMgrTask::GetTaskExtraInfo(m_taskInfo, currTaskInfo);
    result = (extraInfo == currTaskInfo);

    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
        ErrMsg("Task is%s registered: type(%hhu), tablespace(%hu) segment(%hu, %u), "
        "pageIdInfo(%hu, %u), xidInfo(%d, %lu)",
        result ? "" : " not",
        static_cast<uint8>(taskInfo->GetTaskType()), static_cast<uint8>(taskInfo->GetTablespaceId()),
        taskInfo->GetSegmentId().m_fileId, taskInfo->GetSegmentId().m_blockId,
        extraInfo.pageIdInfo.m_fileId, extraInfo.pageIdInfo.m_blockId,
        static_cast<int32>(extraInfo.xidInfo.m_zoneId), extraInfo.xidInfo.m_logicSlotId));

    return (extraInfo == currTaskInfo);
}

void ObjSpaceMgrTask::GetTaskExtraInfo(ObjSpaceMgrTaskInfo *taskInfo, TaskExtraInfo &extraInfo)
{
    switch (taskInfo->GetTaskType()) {
        case EXTEND_TASK: {
            /* cast here is safe since we confirmed that two tasks have the same type already */
            static_cast<ObjSpaceMgrExtendTaskInfo *>(taskInfo)->GetTaskExtraInfo(extraInfo);
            break;
        }
        case RECYCLE_FSM_TASK:
            static_cast<ObjSpaceMgrRecycleFsmTaskInfo *>(taskInfo)->GetTaskExtraInfo(extraInfo);

            break;
        case RECYCLE_BTREE_TASK: {
            static_cast<ObjSpaceMgrRecycleBtreeTaskInfo *>(taskInfo)->GetTaskExtraInfo(extraInfo);
            break;
        }
        case RECLAIM_BTREE_RECYCLE_PARTITION_TASK: {
            static_cast<ObjSpaceMgrReclaimBtrRecyclePartTaskInfo *>(taskInfo)->GetTaskExtraInfo(extraInfo);
            break;
        }
        case EXTEND_INDEX_TASK: {
            static_cast<ObjSpaceMgrExtendIndexTaskInfo *>(taskInfo)->GetTaskExtraInfo(extraInfo);
            break;
        }
        case INVALID_TASK_TYPE:
        default: {
            /* should not reach here */
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                ErrMsg("Invalid taskType %hhu", static_cast<uint8>(taskInfo->GetTaskType())));
        }
    }
}

RetStatus ObjSpaceMgrTask::Execute()
{
    RetStatus status = DSTORE_SUCC;
    ObjSpaceMgrTaskType taskType = m_taskInfo->GetTaskType();

    switch (taskType) {
        case EXTEND_TASK: { /* EXTEND_TASK */
            status = ExecuteExtendTask();
            break;
        }
        case RECYCLE_FSM_TASK: { /* RECYCLE_FSM_TASK */
            status = ExecuteRecycleFsmTask();
            break;
        }
        case RECLAIM_BTREE_RECYCLE_PARTITION_TASK: { /* RECLAIM_BTREE_RECYCLE_PARTITION_TASK */
            status = ExecuteColdBtrRecyclePartReclaimTask();
            break;
        }
        case EXTEND_INDEX_TASK: {
            status = ExecuteExtendBtreeTask();
            break;
        }
        case INVALID_TASK_TYPE:
        default: {
            /* should not reach here */
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                ErrMsg("Invalid m_taskType %hhu", static_cast<uint8>(taskType)));
        }
    }

    return status;
}

RetStatus ObjSpaceMgrTask::VerifyExtendTask(ObjSpaceMgrExtendTaskInfo *extendInfo, TableSpace *tablespace) const
{
    PageId fsmMetaPageId = extendInfo->GetFsmMetaPageId();
    BufferDesc *fsmMetaPageBuf = NULL;
    BufMgrInterface *bufMgr = NULL;
    if (!tablespace->IsTempTbs()) {
        bufMgr = g_storageInstance->GetBufferMgr();
    } else {
        bufMgr = thrd->GetTmpLocalBufMgr();
    }
    fsmMetaPageBuf = bufMgr->Read(extendInfo->GetPdbId(), fsmMetaPageId, LW_EXCLUSIVE);
    if (fsmMetaPageBuf == INVALID_BUFFER_DESC) {
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT, ErrMsg("Verify extend task failed, fsm meta page is invalid."));
        return DSTORE_FAIL;
    }
    FreeSpaceMapMetaPage *fsmMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaPageBuf->GetPage());
    if (!fsmMetaPage->TestType(PageType::FSM_META_PAGE_TYPE) || fsmMetaPage->fsmExtents.last.IsInvalid()) {
        bufMgr->UnlockAndRelease(fsmMetaPageBuf);
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT, ErrMsg("Adjust fsm tree failed: page has been reused."));
        return DSTORE_FAIL;
    }
    bufMgr->UnlockAndRelease(fsmMetaPageBuf);

    return DSTORE_SUCC;
}

RetStatus ObjSpaceMgrTask::Doextend(HeapSegment *segment, TableSpace *tablespace) const
{
    ObjSpaceMgrExtendTaskInfo *extendInfo = static_cast<ObjSpaceMgrExtendTaskInfo*>(m_taskInfo);
    PageId segmentId = extendInfo->GetSegmentId();

    if (STORAGE_FUNC_FAIL(segment->InitSegment())) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Failed to initialize segment (%hu, %u) for ObjSpaceMgrTask.",
                segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }

    /* Do the extension here */
    PageId heapSegMetaPageId = extendInfo->GetHeapSegMetaPageId();
    PageId fsmMetaPageId = extendInfo->GetFsmMetaPageId();
    StorageAssert(fsmMetaPageId != INVALID_PAGE_ID);
    LockTag tag;
    tag.SetTableExtensionLockTag(m_taskInfo->GetPdbId(), heapSegMetaPageId);
    LockErrorInfo err = {0};
    TableLockMgr *lockMgr = g_storageInstance->GetTableLockMgr();
    if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_ACCESS_SHARE_LOCK, false, &err))) {    // Critical-Point
        ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("Lock table segment failed."));
        return DSTORE_FAIL;
    }

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    if (STORAGE_VAR_NULL(bufMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to get the bufMgr"));
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        return DSTORE_FAIL;
    }

    /* Check if the segment has only one extent.
     * We don't do extend in background task to avoid to extend after dropped */
    BufferDesc *segMetaBuf = bufMgr->Read(m_taskInfo->GetPdbId(), segmentId, LW_SHARED);
    if (unlikely(segMetaBuf == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Failed to read segMeta(%hu, %u) in pdb: %u when extend background",
                   segmentId.m_fileId, segmentId.m_blockId, m_taskInfo->GetPdbId()));
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        return DSTORE_FAIL;
    }
    SegmentMetaPage *segMetaPage = static_cast<SegmentMetaPage *>(segMetaBuf->GetPage());
    PageId lastExtentPageId = segMetaPage->segmentHeader.extents.last;
    bufMgr->UnlockAndRelease(segMetaBuf);
    if (unlikely(lastExtentPageId == segmentId)) {
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
            ErrMsg("No need extend {Tablespace %hu, Segment (%hu, %u)} background.",
                extendInfo->GetTablespaceId(), segmentId.m_fileId, segmentId.m_blockId));
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(VerifyExtendTask(extendInfo, tablespace))) {
        ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("Verify extend task failed."));
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        return DSTORE_FAIL;
    }
    if (segment->GetNewPage(fsmMetaPageId) == INVALID_PAGE_ID) {
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
            ErrMsg("Failed to do background extension for Heap segment {%hu, %u} PartitionFreeSpaceMap {%hu, %u}",
                segmentId.m_fileId, segmentId.m_blockId, fsmMetaPageId.m_fileId, fsmMetaPageId.m_blockId));
    }

    lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
        ErrMsg("Background extension completed for segment {%hu, %u}",
            segmentId.m_fileId, segmentId.m_blockId));
    return DSTORE_SUCC;
}

RetStatus ObjSpaceMgrTask::ExecuteExtendTask() const
{
    StorageAssert(m_taskInfo->GetTaskType() == EXTEND_TASK);

    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_SMGR)};
    ObjSpaceMgrExtendTaskInfo *extendInfo = static_cast<ObjSpaceMgrExtendTaskInfo*>(m_taskInfo);

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
        ErrMsg("background thread picks up extend task."));

    PdbId pdbId = m_taskInfo->GetPdbId();
    TablespaceId tablespaceId = extendInfo->GetTablespaceId();
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get tablespaceMgr for Extend ObjSpaceMgrTask, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    LockInterface::TablespaceLockContext context;
    context.pdbId = pdbId;
    context.tablespaceId = tablespaceId + TABLESPACE_ID_COUNT;
    context.dontWait = true;
    context.mode = DSTORE::DSTORE_ACCESS_SHARE_LOCK;
    if (STORAGE_FUNC_FAIL(LockInterface::LockTablespace(&context))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Lock tablespace for drop failed, tablespaceId %hu, pdbId %u.", tablespaceId, pdbId));
        return DSTORE_FAIL;
    }

    /* Make sure the tablespace exists. */
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to open tablespace %hu for Extend ObjSpaceMgrTask, pdbId %u.", tablespaceId, pdbId));
        LockInterface::UnlockTablespace(&context);
        return DSTORE_FAIL;
    }
    if (tablespace->GetTbsCtrlItem().used != 1) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to open tablespace %hu due to it is unused, pdbId %u.", tablespaceId, pdbId));
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        LockInterface::UnlockTablespace(&context);
        storage_set_error(TBS_ERROR_TABLESPACE_NOT_EXIST);
        return DSTORE_FAIL;
    }
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);

    /* Obtain HeapSegment object from segmentId and tableSpace */
    PageId segmentId = extendInfo->GetSegmentId();
    if (!tablespace->IsTempTbs()) {
        HeapNormalSegment segment(pdbId, segmentId, tablespaceId,
            g_storageInstance->GetBufferMgr(), g_dstoreCurrentMemoryContext);
        if (STORAGE_FUNC_FAIL(Doextend(&segment, tablespace))) {
            LockInterface::UnlockTablespace(&context);
            return DSTORE_FAIL;
        }
    } else {
        HeapTempSegment segment(pdbId, segmentId, tablespaceId,
            thrd->GetTmpLocalBufMgr(), g_dstoreCurrentMemoryContext);
        if (STORAGE_FUNC_FAIL(Doextend(&segment, tablespace))) {
            LockInterface::UnlockTablespace(&context);
            return DSTORE_FAIL;
        }
    }
    LockInterface::UnlockTablespace(&context);
    return DSTORE_SUCC;
}

RetStatus ObjSpaceMgrTask::ExecuteRecycleBtreeTask() const
{
    StorageAssert(m_taskInfo->GetTaskType() == RECYCLE_BTREE_TASK);

    TimestampTz startTime = GetCurrentTimestampInSecond();

    ObjSpaceMgrRecycleBtreeTaskInfo *recycleBtreeInfo = static_cast<ObjSpaceMgrRecycleBtreeTaskInfo*>(m_taskInfo);
    StorageAssert(recycleBtreeInfo->m_createdXid != INVALID_XID);
    StorageAssert(recycleBtreeInfo->m_indexInfo != nullptr);
    StorageAssert(recycleBtreeInfo->m_scanKey != nullptr);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    if (STORAGE_VAR_NULL(bufMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to get the bufMgr"));
        return DSTORE_FAIL;
    }
    /* Obtain a current transaction and set corresponding snapshot csn */
    Transaction *curTransaction = thrd->GetActiveTransaction();
    if (unlikely(curTransaction == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to get the transaction in ExecuteRecycleBtreeTask"));
        return DSTORE_FAIL;
    }

    /* Start the transaction and obtain a valid snapshotcsn for recycle */
    if (STORAGE_FUNC_FAIL(curTransaction->Start())) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to start the internal transaction in ExecuteRecycleBtreeTask"));
        return DSTORE_FAIL;
    }
    (void)curTransaction->SetSnapshotCsn();

    /* Obtain BtreeStorageMgr based on given information */
    StorageRelationData fakeIndexRel;
    fakeIndexRel.Init();
    PageId segmentId = recycleBtreeInfo->GetSegmentId();

    LockTag tag;
    tag.SetTableExtensionLockTag(m_taskInfo->GetPdbId(), segmentId);
    LockErrorInfo error = {0};
    TableLockMgr *lockMgr = g_storageInstance->GetTableLockMgr();
    if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_ACCESS_SHARE_LOCK, false, &error))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to lock segment (%hu, %u) of RecycleBtree, lock error: %u",
            segmentId.m_fileId, segmentId.m_blockId, error.lockHolder));
        (void)curTransaction->Commit();
        return DSTORE_FAIL;
    }

    /* Check if the segment has only one extent.
     * We don't do extend in background task to avoid to extend after dropped */
    BufferDesc *segMetaBuf = bufMgr->Read(m_taskInfo->GetPdbId(), segmentId, LW_SHARED);
    if (unlikely(segMetaBuf == INVALID_BUFFER_DESC)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        (void)curTransaction->Commit();
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to read segMeta(%hu, %u) in pdb: %u when extend background",
                   segmentId.m_fileId, segmentId.m_blockId, m_taskInfo->GetPdbId()));
        return DSTORE_FAIL;
    }
    SegmentMetaPage *segMetaPage = static_cast<SegmentMetaPage *>(segMetaBuf->GetPage());
    PageId lastExtentPageId = segMetaPage->segmentHeader.extents.last;
    bufMgr->UnlockAndRelease(segMetaBuf);
    if (unlikely(lastExtentPageId == segmentId)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        (void)curTransaction->Commit();
        ErrLog(DSTORE_WARNING, MODULE_INDEX,
            ErrMsg("No need extend Btree {Tablespace %hu, Segment (%hu, %u)} background.",
                recycleBtreeInfo->GetTablespaceId(), segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }

    fakeIndexRel.btreeSmgr = StorageTableInterface::CreateBtreeSmgr(
        m_taskInfo->GetPdbId(), recycleBtreeInfo->GetTablespaceId(), segmentId, DSTORE::INVALID_INDEX_FILLFACTOR);
    fakeIndexRel.m_pdbId = m_taskInfo->GetPdbId();
    if (STORAGE_VAR_NULL(fakeIndexRel.btreeSmgr)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        (void)curTransaction->Commit();
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to initialize BtreeStorageMgr {Tablespace %hu, Segment (%hu, %u)} for ObjSpaceMgrTask",
                recycleBtreeInfo->GetTablespaceId(), segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }

    Xid createdXid = INVALID_XID;
    if (unlikely(!fakeIndexRel.btreeSmgr->IsBtrSmgrValid(bufMgr, &createdXid)) ||
        unlikely(createdXid != recycleBtreeInfo->m_createdXid)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("No need to recycle btree {Tablespace %hu, Segment (%hu, %u)} for ObjSpaceMgrTask, segment changed "
                "created xid(%d, %lu), task xid(%d, %lu)",
                recycleBtreeInfo->GetTablespaceId(), segmentId.m_fileId, segmentId.m_blockId,
                static_cast<int32>(createdXid.m_zoneId), createdXid.m_logicSlotId,
                static_cast<int32>(recycleBtreeInfo->m_createdXid.m_zoneId),
                recycleBtreeInfo->m_createdXid.m_logicSlotId));
        (void)curTransaction->Commit();
        StorageTableInterface::DestroyBtreeSmgr(fakeIndexRel.btreeSmgr);
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("Start recycle btree task: tablespace(%hu) segment(%hu, %u) createdXid(%d, %lu)",
        recycleBtreeInfo->GetTablespaceId(), segmentId.m_fileId, segmentId.m_blockId,
        static_cast<int32>(createdXid.m_zoneId), createdXid.m_logicSlotId));

    /* Try to do the batch page recycle here */
    bool needRetry = false;
    BtreePageRecycle recycle(&fakeIndexRel, startTime);
    if (STORAGE_FUNC_FAIL(recycle.BatchRecycleBtreePage(recycleBtreeInfo->m_indexInfo,
                                                        recycleBtreeInfo->m_scanKey, &needRetry))) {
        ErrLog(DSTORE_WARNING, MODULE_INDEX,
            ErrMsg("Failed to do background unlink for index segment {%hu, %u}",
                segmentId.m_fileId, segmentId.m_blockId));
    }
    lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("Finish recycle btree task: tablespace(%hu) segment(%hu, %u) createdXid(%d, %lu)",
        recycleBtreeInfo->GetTablespaceId(), segmentId.m_fileId, segmentId.m_blockId,
        static_cast<int32>(createdXid.m_zoneId), createdXid.m_logicSlotId));

    StorageTableInterface::DestroyBtreeSmgr(fakeIndexRel.btreeSmgr);

    /* Commit the transaction when we are done with the unlink process */
    if (STORAGE_FUNC_FAIL(curTransaction->Commit())) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to commit the internal transaction in ExecuteRecycleBtreeTask"));
        return DSTORE_FAIL;
    }

    if (needRetry) {
        if (STORAGE_FUNC_FAIL(BtreePageRecycle::TryRegisterRecycleBtreeTask(createdXid, recycleBtreeInfo->m_indexInfo,
                recycleBtreeInfo->m_scanKey, recycleBtreeInfo->GetTablespaceId(), segmentId, m_taskInfo->GetPdbId()))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to register btree Task for SegMetaPageId ({%hu, %u}) with tablespace id (%hu)",
                    segmentId.m_fileId, segmentId.m_blockId, recycleBtreeInfo->GetTablespaceId()));
        }
    }

    return DSTORE_SUCC;
}

RetStatus ObjSpaceMgrTask::ExecuteRecycleFsmTask() const
{
    StorageAssert(m_taskInfo->GetTaskType() == RECYCLE_FSM_TASK);

    PageId segmentId = INVALID_PAGE_ID;
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_SMGR)};
    ObjSpaceMgrRecycleFsmTaskInfo *recycleFsmInfo = static_cast<ObjSpaceMgrRecycleFsmTaskInfo*>(m_taskInfo);
    PdbId pdbId = m_taskInfo->GetPdbId();
    TablespaceId tablespaceId = recycleFsmInfo->GetTablespaceId();
    /* Global temp table no need recycle fsm */
    if (tablespaceId == static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID)) {
        return DSTORE_SUCC;
    }

    /* Obtain TableSpace object from tablespaceId */
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get tablespaceMgr for ObjSpaceMgrTask, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    LockInterface::TablespaceLockContext context;
    context.pdbId = pdbId;
    context.tablespaceId = tablespaceId + TABLESPACE_ID_COUNT;
    context.dontWait = true;
    context.mode = DSTORE::DSTORE_ACCESS_SHARE_LOCK;
    if (STORAGE_FUNC_FAIL(LockInterface::LockTablespace(&context))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Lock tablespace for drop failed, tablespaceId %hu, pdbId %u.", tablespaceId, pdbId));
        return DSTORE_FAIL;
    }
    /* Make sure the tablespace exists. */
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Failed to initialize tablespace %hu for ObjSpaceMgrTask", tablespaceId));
        LockInterface::UnlockTablespace(&context);
        return DSTORE_FAIL;
    }
    if (tablespace->GetTbsCtrlItem().used != 1) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to open tablespace %hu due to it is unused, pdbId %u.", tablespaceId, pdbId));
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        LockInterface::UnlockTablespace(&context);
        storage_set_error(TBS_ERROR_TABLESPACE_NOT_EXIST);
        return DSTORE_FAIL;
    }
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);

    /* Obtain HeapSegment object from segmentId and tableSpace */
    segmentId = recycleFsmInfo->GetSegmentId();
    HeapNormalSegment segment(pdbId, segmentId, tablespaceId,
        g_storageInstance->GetBufferMgr(), g_dstoreCurrentMemoryContext);
    if (STORAGE_FUNC_FAIL(segment.InitSegment())) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Failed to initialize segment (%hu, %u) for Recycle ObjSpaceMgrTask.",
                segmentId.m_fileId, segmentId.m_blockId));
        LockInterface::UnlockTablespace(&context);
        return DSTORE_FAIL;
    }

    /* Do the recycle here */
    if (STORAGE_FUNC_FAIL(segment.RecycleUnusedFsm())) {
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
            ErrMsg("Failed to do background FSM recycle for segment {%hu, %u}",
                segmentId.m_fileId, segmentId.m_blockId));
        LockInterface::UnlockTablespace(&context);
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
        ErrMsg("Background FSM recycle completed for segment {%hu, %u}",
            segmentId.m_fileId, segmentId.m_blockId));

    LockInterface::UnlockTablespace(&context);
    return DSTORE_SUCC;
}

RetStatus ObjSpaceMgrTask::ExecuteColdBtrRecyclePartReclaimTask() const
{
    thrd->RefreshWorkingVersionNum();
    StorageAssert(m_taskInfo->GetTaskType() == RECLAIM_BTREE_RECYCLE_PARTITION_TASK);

    ObjSpaceMgrReclaimBtrRecyclePartTaskInfo *btrRecyclePartInfo =
        static_cast<ObjSpaceMgrReclaimBtrRecyclePartTaskInfo*>(m_taskInfo);
    PdbId pdbId = m_taskInfo->GetPdbId();
    TablespaceId tablespaceId = btrRecyclePartInfo->GetTablespaceId();
    if (tablespaceId == static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID)) {
        /* No need to recycle Global temp table's btree recycle partition */
        return DSTORE_SUCC;
    }

    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_SMGR)};
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get tablespaceMgr for ObjSpaceMgrTask, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    LockInterface::TablespaceLockContext context;
    context.pdbId = pdbId;
    context.tablespaceId = tablespaceId + TABLESPACE_ID_COUNT;
    context.dontWait = true;
    context.mode = DSTORE::DSTORE_ACCESS_SHARE_LOCK;
    if (STORAGE_FUNC_FAIL(LockInterface::LockTablespace(&context))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Lock tablespace for drop failed, tablespaceId %hu, pdbId %u.", tablespaceId, pdbId));
        return DSTORE_FAIL;
    }

    /* Make sure the tablespace exists. */
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Failed to initialize tablespace %hu for ObjSpaceMgrTask, pdbId %u.", tablespaceId, pdbId));
        LockInterface::UnlockTablespace(&context);
        return DSTORE_FAIL;
    }
    if (tablespace->GetTbsCtrlItem().used != 1) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to open tablespace %hu due to it is unused, pdbId %u.", tablespaceId, pdbId));
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        LockInterface::UnlockTablespace(&context);
        storage_set_error(TBS_ERROR_TABLESPACE_NOT_EXIST);
        return DSTORE_FAIL;
    }
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);

    /* Obtain HeapSegment object from segmentId and tableSpace */
    PageId segmentId = btrRecyclePartInfo->GetSegmentId();
    LockTag tag;
    tag.SetTableExtensionLockTag(pdbId, segmentId);
    LockErrorInfo error = {0};
    TableLockMgr *lockMgr = g_storageInstance->GetTableLockMgr();
    if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_ACCESS_SHARE_LOCK, false, &error))) {
        LockInterface::UnlockTablespace(&context);
        return DSTORE_FAIL;
    }

    BtreeStorageMgr *btreeSmgr = StorageTableInterface::CreateBtreeSmgr(
        pdbId, tablespaceId, segmentId, DSTORE::INVALID_INDEX_FILLFACTOR);
    if (STORAGE_VAR_NULL(btreeSmgr)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        LockInterface::UnlockTablespace(&context);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to initialize BtreeSotrageMgr {Tablespace %hu, Segment (%hu, %u)} for ObjSpaceMgrTask",
                tablespaceId, segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }

    Xid createdXid = INVALID_XID;
    if (unlikely(!btreeSmgr->IsBtrSmgrValid(g_storageInstance->GetBufferMgr(), &createdXid)) ||
        unlikely(createdXid != btrRecyclePartInfo->m_createdXid)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        LockInterface::UnlockTablespace(&context);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to recycle cold part {Tablespace %hu, Segment (%hu, %u)} for ObjSpaceMgrTask, "
                "segment changed. created xid(%d, %lu), task xid(%d, %lu)",
                tablespaceId, segmentId.m_fileId, segmentId.m_blockId,
                static_cast<int32>(createdXid.m_zoneId), createdXid.m_logicSlotId,
                static_cast<int32>(btrRecyclePartInfo->m_createdXid.m_zoneId),
                btrRecyclePartInfo->m_createdXid.m_logicSlotId));
        StorageTableInterface::DestroyBtreeSmgr(btreeSmgr);
        return DSTORE_FAIL;
    }

    /* Do the recycle here */
    RetStatus retStatus = btreeSmgr->GetSegment()->TryRecycleColdBtrRecyclePartition(createdXid);
    StorageTableInterface::DestroyBtreeSmgr(btreeSmgr);

    lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
    if (STORAGE_FUNC_FAIL(retStatus)) {
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
            ErrMsg("Failed to do background BtreeRecycle Partition recycle for segment {%hu, %u}",
                segmentId.m_fileId, segmentId.m_blockId));
        LockInterface::UnlockTablespace(&context);
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
        ErrMsg("Background BtreeRecyclePartition recycle completed for segment {%hu, %u}",
               segmentId.m_fileId, segmentId.m_blockId));
    LockInterface::UnlockTablespace(&context);
    return DSTORE_SUCC;
}

RetStatus ObjSpaceMgrTask::ExecuteExtendBtreeTask() const
{
    thrd->RefreshWorkingVersionNum();
    StorageAssert(m_taskInfo->GetTaskType() == EXTEND_INDEX_TASK);

    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_SMGR)};
    ObjSpaceMgrExtendIndexTaskInfo *extendInfo = static_cast<ObjSpaceMgrExtendIndexTaskInfo*>(m_taskInfo);

    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
        ErrMsg("background thread picks up extend index task."));

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    if (STORAGE_VAR_NULL(bufMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to get the bufMgr"));
        return DSTORE_FAIL;
    }

    PdbId pdbId = m_taskInfo->GetPdbId();
    TablespaceId tablespaceId = extendInfo->GetTablespaceId();
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get tablespaceMgr for ExtendBtreeTask, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    /* Add a shared lock to the tablespace to prevent it from being dropped concurrently. */
    LockInterface::TablespaceLockContext context;
    context.pdbId = pdbId;
    context.tablespaceId = tablespaceId + TABLESPACE_ID_COUNT;
    context.dontWait = true;
    context.mode = DSTORE::DSTORE_ACCESS_SHARE_LOCK;
    if (STORAGE_FUNC_FAIL(LockInterface::LockTablespace(&context))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Lock tablespace for drop failed, tablespaceId %hu, pdbId %u.", tablespaceId, pdbId));
        return DSTORE_FAIL;
    }

    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to open tablespace %hu for ExtendBtreeTask, pdbId %u.", tablespaceId, pdbId));
        LockInterface::UnlockTablespace(&context);
        return DSTORE_FAIL;
    }
    /* Make sure that the tablespace exists. */
    if (tablespace->GetTbsCtrlItem().used != 1) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to open tablespace %hu due to it is unused, pdbId %u.", tablespaceId, pdbId));
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        LockInterface::UnlockTablespace(&context);
        storage_set_error(TBS_ERROR_TABLESPACE_NOT_EXIST);
        return DSTORE_FAIL;
    }
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);

    PageId segmentId = extendInfo->GetSegmentId();
    LockTag tag;
    tag.SetTableExtensionLockTag(pdbId, segmentId);
    LockErrorInfo error = {0};
    TableLockMgr *lockMgr = g_storageInstance->GetTableLockMgr();
    if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_ACCESS_SHARE_LOCK, false, &error))) {
        LockInterface::UnlockTablespace(&context);
        return DSTORE_FAIL;
    }

    /* Check if the segment has only one extent.
     * We don't do extend in background task to avoid to extend after dropped */
    BufferDesc *segMetaBuf = bufMgr->Read(m_taskInfo->GetPdbId(), segmentId, LW_SHARED);
    if (unlikely(segMetaBuf == INVALID_BUFFER_DESC)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        LockInterface::UnlockTablespace(&context);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to read segMeta(%hu, %u) in pdb: %u when extend background",
                   segmentId.m_fileId, segmentId.m_blockId, m_taskInfo->GetPdbId()));
        return DSTORE_FAIL;
    }
    SegmentMetaPage *segMetaPage = static_cast<SegmentMetaPage *>(segMetaBuf->GetPage());
    PageId lastExtentPageId = segMetaPage->segmentHeader.extents.last;
    bufMgr->UnlockAndRelease(segMetaBuf);
    if (unlikely(lastExtentPageId == segmentId)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        LockInterface::UnlockTablespace(&context);
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("No need extend Btree {Tablespace %hu, Segment (%hu, %u)} background.",
                extendInfo->GetTablespaceId(), segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }

    BtreeStorageMgr *btreeSmgr = StorageTableInterface::CreateBtreeSmgr(
        pdbId, tablespaceId, segmentId, DSTORE::INVALID_INDEX_FILLFACTOR);
    if (STORAGE_VAR_NULL(btreeSmgr)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        LockInterface::UnlockTablespace(&context);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to initialize BtreeStorageMgr {Tablespace %hu, Segment (%hu, %u)} for ObjSpaceMgrTask",
                tablespaceId, segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }

    Xid createdXid = INVALID_XID;
    if (unlikely(!btreeSmgr->IsBtrSmgrValid(bufMgr, &createdXid)) ||
        unlikely(createdXid != extendInfo->m_createdXid)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
        LockInterface::UnlockTablespace(&context);
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("No need to extend Btree Page {Tablespace %hu, Segment (%hu, %u)} for ObjSpaceMgrTask, "
                      "segment changed. created xid(%d, %lu), task xid(%d, %lu)",
                      tablespaceId, segmentId.m_fileId, segmentId.m_blockId,
                      static_cast<int32>(createdXid.m_zoneId), createdXid.m_logicSlotId,
                      static_cast<int32>(extendInfo->m_createdXid.m_zoneId),
                      extendInfo->m_createdXid.m_logicSlotId));
        StorageTableInterface::DestroyBtreeSmgr(btreeSmgr);
        return DSTORE_FAIL;
    }
    if (btreeSmgr->GetSegment()->GetNewPage(true) == INVALID_PAGE_ID) {
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
               ErrMsg("Failed to do background extension for Index segment {%hu, %u}", segmentId.m_fileId,
                      segmentId.m_blockId));
    }
    lockMgr->Unlock(&tag, DSTORE_ACCESS_SHARE_LOCK);
    StorageTableInterface::DestroyBtreeSmgr(btreeSmgr);
    LockInterface::UnlockTablespace(&context);

    return DSTORE_SUCC;
}

ObjSpaceMgrTaskQueue::ObjSpaceMgrTaskQueue()
    : m_head(nullptr), m_tail(nullptr), m_objSpaceMgrTaskQueueLock{}
{
}

ObjSpaceMgrTaskQueue::~ObjSpaceMgrTaskQueue()
{
    m_head = nullptr;
    m_tail = nullptr;
}

void ObjSpaceMgrTaskQueue::Initialize()
{
    m_head = m_tail = nullptr;
    LWLockInitialize(&m_objSpaceMgrTaskQueueLock, LWLOCK_GROUP_OBJ_SPACE_MGR_TASK_QUEUE);
}

void ObjSpaceMgrTaskQueue::Destroy()
{
    ObjSpaceMgrTask *temp = nullptr;

    /*
     * There is no need to acquire a lock here,
     * since we are destroying the queue
     */
    while (!STORAGE_VAR_NULL(m_head)) {
        temp = m_head;
        m_head = m_head->m_nextTask;
        delete temp;
    }

    m_tail = nullptr;
}

void ObjSpaceMgrTaskQueue::PushTaskIfNeeded(ObjSpaceMgrTask *newTask, bool *registered)
{
    StorageAssert(newTask != nullptr);
    DstoreLWLockAcquire(&m_objSpaceMgrTaskQueueLock, LW_EXCLUSIVE);

    /* Add Task only if it does not exist in the queue */
    bool found = FindTask(newTask->m_taskInfo, false);
    if (!found) {
        if (STORAGE_VAR_NULL(m_head)) {
            m_head = m_tail = newTask;
        } else {
            StorageAssert(m_tail != nullptr);
            m_tail->SetNext(newTask);
            m_tail = newTask;
        }
    }
    LWLockRelease(&m_objSpaceMgrTaskQueueLock);

    /* Update the push result */
    *registered = found;
}

ObjSpaceMgrTask* ObjSpaceMgrTaskQueue::PopNextTask()
{
    ObjSpaceMgrTask *retTask = nullptr;

    DstoreLWLockAcquire(&m_objSpaceMgrTaskQueueLock, LW_EXCLUSIVE);

    /* queue is empty */
    if (STORAGE_VAR_NULL(m_head)) {
        LWLockRelease(&m_objSpaceMgrTaskQueueLock);
        return nullptr;
    }

    if (m_head == m_tail) {
        retTask = m_head;
        m_head = m_tail = nullptr;
    } else {
        retTask = m_head;
        m_head = retTask->m_nextTask;
    }

    LWLockRelease(&m_objSpaceMgrTaskQueueLock);
    return retTask;
}

bool ObjSpaceMgrTaskQueue::FindTask(ObjSpaceMgrTaskInfo *taskInfo, bool needLock)
{
    bool found = false;
    if (needLock) {
        DstoreLWLockAcquire(&m_objSpaceMgrTaskQueueLock, LW_SHARED);
    }

    /* queue is empty */
    if (STORAGE_VAR_NULL(m_head)) {
        if (needLock) {
            LWLockRelease(&m_objSpaceMgrTaskQueueLock);
        }
        return found;
    }

    ObjSpaceMgrTask *curTask = m_head;
    while (!STORAGE_VAR_NULL(curTask)) {
        if (curTask->IsCurrentTask(taskInfo)) {
            found = true;
            break;
        }

        curTask = curTask->m_nextTask;
    }

    if (needLock) {
        LWLockRelease(&m_objSpaceMgrTaskQueueLock);
    }
    return found;
}

ObjSpaceMgr::ObjSpaceMgr(uint32 numObjSpaceMgrTaskQueue)
    : m_initialized(false),
      m_ctx(nullptr),
      m_numObjSpaceMgrTaskQueue(numObjSpaceMgrTaskQueue),
      m_objSpaceMgrTaskQueueList(nullptr)
{}

ObjSpaceMgr::~ObjSpaceMgr()
{
    m_initialized = false;
    m_ctx = nullptr;
    m_numObjSpaceMgrTaskQueue = 0;
    m_objSpaceMgrTaskQueueList = nullptr;
}

RetStatus ObjSpaceMgr::Initialize(DstoreMemoryContext currMemoryContext)
{
    m_ctx = currMemoryContext;
    AutoMemCxtSwitch autoSwitch{m_ctx};
    m_objSpaceMgrTaskQueueList =
        (ObjSpaceMgrTaskQueue *)DstorePalloc(sizeof(ObjSpaceMgrTaskQueue) * m_numObjSpaceMgrTaskQueue);
    if (STORAGE_VAR_NULL(m_objSpaceMgrTaskQueueList)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to alloc m_objSpaceMgrTaskQueueList, m_numObjSpaceMgrTaskQueue %u.",
                      m_numObjSpaceMgrTaskQueue));
        return DSTORE_FAIL;
    }
    for (uint32 i = 0; i < m_numObjSpaceMgrTaskQueue; i++) {
        m_objSpaceMgrTaskQueueList[i].Initialize();
    }
    m_initialized = true;
    return DSTORE_SUCC;
}

void ObjSpaceMgr::Destroy()
{
    if (m_objSpaceMgrTaskQueueList != nullptr) {
        if (m_numObjSpaceMgrTaskQueue == 0) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                   ErrMsg("Failed to destroy m_objSpaceMgrTaskQueueList due to m_numObjSpaceMgrTaskQueue is 0."));
            return;
        }
        for (uint32 i = 0; i < m_numObjSpaceMgrTaskQueue; i++) {
            m_objSpaceMgrTaskQueueList[i].Destroy();
        }
        DstorePfree(m_objSpaceMgrTaskQueueList);
    }
}

bool ObjSpaceMgr::IsObjSpaceMgrInitialized()
{
    return this->m_initialized;
}

ObjSpaceMgrTask *ObjSpaceMgr::AllocateObjSpaceMgrTask(ObjSpaceMgrTaskInfo *taskInfo)
{
    StorageAssert(taskInfo != nullptr);
    ObjSpaceMgrTaskType taskType = taskInfo->GetTaskType();
    ObjSpaceMgrTaskInfo *curTaskInfo = nullptr;
    AutoMemCxtSwitch autoSwitch{m_ctx};
    ObjSpaceMgrTask *task = nullptr;

    /* Copy given task info first */
    switch (taskType) {
        case EXTEND_TASK: {
            ObjSpaceMgrExtendTaskInfo *curExtendInfo = DstoreNew(m_ctx) ObjSpaceMgrExtendTaskInfo(taskInfo);
            curTaskInfo = static_cast<ObjSpaceMgrTaskInfo *>(curExtendInfo);
            break;
        }
        case RECYCLE_BTREE_TASK: {
            ObjSpaceMgrRecycleBtreeTaskInfo *curRecycleBtreeInfo = DstoreNew(m_ctx)
                                                                   ObjSpaceMgrRecycleBtreeTaskInfo(taskInfo);
            /* Handle the scenario that the indexinfo and scankey did not allocate memory successfully */
            if (curRecycleBtreeInfo == nullptr || curRecycleBtreeInfo->m_indexInfo == nullptr ||
                curRecycleBtreeInfo->m_scanKey == nullptr) {
                delete curRecycleBtreeInfo;
                curRecycleBtreeInfo = nullptr;
            }
            curTaskInfo = static_cast<ObjSpaceMgrTaskInfo *>(curRecycleBtreeInfo);
            break;
        }
        case RECLAIM_BTREE_RECYCLE_PARTITION_TASK: {
            ObjSpaceMgrReclaimBtrRecyclePartTaskInfo *curRecycleBtrPartInfo =
                DstoreNew(m_ctx) ObjSpaceMgrReclaimBtrRecyclePartTaskInfo(taskInfo);
            curTaskInfo = static_cast<ObjSpaceMgrTaskInfo *>(curRecycleBtrPartInfo);
            break;
        }
        case RECYCLE_FSM_TASK: {
            ObjSpaceMgrRecycleFsmTaskInfo *curRecycleFsmInfo = DstoreNew(m_ctx) ObjSpaceMgrRecycleFsmTaskInfo(taskInfo);
            curTaskInfo = static_cast<ObjSpaceMgrTaskInfo *>(curRecycleFsmInfo);
            break;
        }
        case EXTEND_INDEX_TASK: {
            ObjSpaceMgrExtendIndexTaskInfo *curExtendIndexInfo =
                DstoreNew(m_ctx) ObjSpaceMgrExtendIndexTaskInfo(taskInfo);
            curTaskInfo = static_cast<ObjSpaceMgrTaskInfo *>(curExtendIndexInfo);
            break;
        }
        case INVALID_TASK_TYPE:
        default: {
            /* should not reach here */
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                ErrMsg("Invalid m_taskType %hhu", static_cast<uint8>(taskType)));
        }
    }
    if (STORAGE_VAR_NULL(curTaskInfo)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Allocate memory for task type: %hhu failed.", static_cast<uint8>(taskType)));
        return nullptr;
    }
    /* Now, create the ObjSpaceMgrTask we want */
    task = DstoreNew(m_ctx) ObjSpaceMgrTask(curTaskInfo);
    if (STORAGE_VAR_NULL(task)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Allocate memory for ObjSpaceMgrTask failed."));
        delete curTaskInfo;
        curTaskInfo = nullptr;
        return nullptr;
    }
    return task;
}

ObjSpaceMgrTaskQueue *ObjSpaceMgr::FindObjSpaceMgrTaskQueue(uint32 id)
{
    if (m_numObjSpaceMgrTaskQueue == 0) {
        return nullptr;
    }

    uint32 index = id % m_numObjSpaceMgrTaskQueue;
    return &m_objSpaceMgrTaskQueueList[index];
}

RetStatus ObjSpaceMgr::RegisterObjSpaceMgrTaskIfNeeded(ObjSpaceMgrTaskInfo *taskInfo)
{
    if (taskInfo == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("taskinfo is null."));
        return DSTORE_FAIL;
    }

    bool registered = false;
    ObjSpaceMgrTask *task = nullptr;
    PageId segmentId = taskInfo->GetSegmentId();

    /* Create task based on given info and try to push it into the queue */
    task = AllocateObjSpaceMgrTask(taskInfo);
    if (STORAGE_VAR_NULL(task)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Failed at allocating new ObjSpaceMgrTask (taskType %hhu) for segment (%hu, %u)",
                static_cast<uint8>(taskInfo->GetTaskType()), segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }
    ObjSpaceMgrTaskQueue *taskQueue = FindObjSpaceMgrTaskQueue(segmentId.m_blockId);
    if (STORAGE_VAR_NULL(taskQueue)) {
        delete task;
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
               ErrMsg("Failed to find ObjSpaceMgrTaskQueue (taskType %hhu) for segment (%hu, %u), "
                      "m_numObjSpaceMgrTaskQueue %u.",
                      static_cast<uint8>(taskInfo->GetTaskType()), segmentId.m_fileId, segmentId.m_blockId,
                      m_numObjSpaceMgrTaskQueue));
        return DSTORE_FAIL;
    }
    taskQueue->PushTaskIfNeeded(task, &registered);

    /* clean up memory if the task is already registered */
    if (registered) {
        delete task;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT, ErrMsg("Registered(%u) a task(%hhu) for segment (%hu, %u), "
        "m_numObjSpaceMgrTaskQueue %u.", registered, static_cast<uint8>(taskInfo->GetTaskType()),
        segmentId.m_fileId, segmentId.m_blockId, m_numObjSpaceMgrTaskQueue));

    return DSTORE_SUCC;
}

bool ObjSpaceMgr::FindObjSpaceMgrTask(ObjSpaceMgrTaskInfo *taskInfo)
{
    PageId segmentId = taskInfo->GetSegmentId();
    ObjSpaceMgrTaskQueue *taskQueue = FindObjSpaceMgrTaskQueue(segmentId.m_blockId);
    if (STORAGE_VAR_NULL(taskQueue)) {
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
               ErrMsg("Failed to find ObjSpaceMgrTaskQueue (taskType %hhu) for segment (%hu, %u), "
                      "m_numObjSpaceMgrTaskQueue %u.",
                      static_cast<uint8>(taskInfo->GetTaskType()), segmentId.m_fileId, segmentId.m_blockId,
                      m_numObjSpaceMgrTaskQueue));
        return false;
    }
    return taskQueue->FindTask(taskInfo);
}

ObjSpaceMgrTask* ObjSpaceMgr::GetObjSpaceMgrTask(uint32 workerId)
{
    ObjSpaceMgrTaskQueue *taskQueue = FindObjSpaceMgrTaskQueue(workerId);
    if (STORAGE_VAR_NULL(taskQueue)) {
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
               ErrMsg("Failed to find ObjSpaceMgrTaskQueue, workerId %u, m_numObjSpaceMgrTaskQueue %u.", workerId,
                      m_numObjSpaceMgrTaskQueue));
        return nullptr;
    }

    ObjSpaceMgrTask *retTask = taskQueue->PopNextTask();
#ifdef DSTORE_USE_ASSERT_CHECKING
    if (!STORAGE_VAR_NULL(retTask)) {
        PageId segmentId = retTask->m_taskInfo->GetSegmentId();
        TaskExtraInfo extraInfo;
        ObjSpaceMgrTask::GetTaskExtraInfo(retTask->m_taskInfo, extraInfo);
        ErrLog(DSTORE_LOG, MODULE_SEGMENT,
            ErrMsg("ObjSpaceMgrTask is popped: type(%hhu) tablespace(%hhu) segment(%hu, %u) "
            "pageIdInfo(%hu, %u), xidInfo(%d, %lu)",
            static_cast<uint8>(retTask->m_taskInfo->GetTaskType()),
            static_cast<uint8>(retTask->m_taskInfo->GetTablespaceId()), segmentId.m_fileId, segmentId.m_blockId,
            extraInfo.pageIdInfo.m_fileId, extraInfo.pageIdInfo.m_blockId,
            static_cast<int32>(extraInfo.xidInfo.m_zoneId), extraInfo.xidInfo.m_logicSlotId));
    }
#endif
    return retTask;
}

bool ObjSpaceMgr::IsExtensionTaskRegistered(PageId heapSegMetaPageId, const TablespaceId tbsId,
                                            const PageId segmentId, const PageId fsmId, PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get pdb, pdbId %u.", pdbId));
        return false;
    }
    ObjSpaceMgr *objSpaceMgr = pdb->GetObjSpaceMgr();
    ObjSpaceMgrExtendTaskInfo extendInfo(pdbId, heapSegMetaPageId, tbsId, segmentId, fsmId);

    return objSpaceMgr->FindObjSpaceMgrTask(&extendInfo);
}

bool ObjSpaceMgr::IsRecycleFsmTaskRegistered(const TablespaceId tbsId, const PageId segmentId, PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get pdb, pdbId %u.", pdbId));
        return false;
    }
    ObjSpaceMgr *objSpaceMgr = pdb->GetObjSpaceMgr();
    ObjSpaceMgrRecycleFsmTaskInfo recycleFsmInfo(pdbId, tbsId, segmentId);

    return objSpaceMgr->FindObjSpaceMgrTask(&recycleFsmInfo);
}

bool ObjSpaceMgr::IsRecycleBtreeTaskRegistered(const TablespaceId tbsId, const PageId segmentId, const Xid createdXid,
                                               PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get pdb, pdbId %u.", pdbId));
        return false;
    }
    ObjSpaceMgr *objSpaceMgr = pdb->GetObjSpaceMgr();
    ObjSpaceMgrRecycleBtreeTaskInfo recycleBtreeInfo(pdbId, tbsId, segmentId, createdXid, nullptr, nullptr);

    return objSpaceMgr->FindObjSpaceMgrTask(&recycleBtreeInfo);
}

bool ObjSpaceMgr::IsReclaimColdBtrRecyclePartTaskRegistered(const TablespaceId tbsId, const PageId segmentId,
                                                            const Xid createdXid, PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get pdb, pdbId %u.", pdbId));
        return false;
    }
    ObjSpaceMgr *objSpaceMgr = pdb->GetObjSpaceMgr();
    ObjSpaceMgrReclaimBtrRecyclePartTaskInfo reclaimBtrPartitionInfo(pdbId, tbsId, segmentId, createdXid);

    return objSpaceMgr->FindObjSpaceMgrTask(&reclaimBtrPartitionInfo);
}

bool ObjSpaceMgr::IsExtensionIndexTaskRegistered(const TablespaceId tbsId, const PageId segmentId,
                                                 PageId recyclePartitionMeta, Xid createdXid, PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get pdb, pdbId %u.", pdbId));
        return false;
    }
    ObjSpaceMgr *objSpaceMgr = pdb->GetObjSpaceMgr();

    ObjSpaceMgrExtendIndexTaskInfo extendInfo(pdbId, tbsId, segmentId, recyclePartitionMeta, createdXid);

    return objSpaceMgr->FindObjSpaceMgrTask(&extendInfo);
}

} /* namespace DSTORE */
