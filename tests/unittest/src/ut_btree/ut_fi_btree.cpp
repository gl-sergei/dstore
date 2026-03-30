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
#include "ut_btree/ut_fi_btree.h"
#include "framework/dstore_pdb.h"
#include "lock/dstore_xact_lock_mgr.h"
#include "index/dstore_index_interface.h"

TEST_F(UTFiBtree, TestRollbackOnePage_level0)
{
    /* Step 1: Build base data */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    int rowNum = 300, i = 1;
    std::vector<IndexTuple *> indexTupVec;
    Transaction *trx = thrd->GetActiveTransaction();
    trx->Start();
    /* Insert (2,2)(4,4)...(600,600) */
    for (i = 1; i <= rowNum; ++i) {
        int16 r1 = (int16)(i * 2);
        int32 r2 = (int32)(i * 2);
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        rowDef.column_int16 = r1;
        rowDef.column_int32 = r2;
        IndexTuple *indexTuple = InsertSpecificIndexTuple(&rowDef, DefaultNullBitMap, true);
        indexTupVec.push_back(indexTuple);
    }
    trx->Commit();

    /* Step 2: Run transaction: delete data and make it as failed transaction */
    trx->Start();
    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit();  /* Just commit 'begin' command */
    trx->SetSnapshotCsn();
    for (i = 1; i <= rowNum; ++i) {
        DeleteIndexTuple(indexTupVec[i-1], true);
    }
    
    /* Release transaction lock in order to make it as failed transaction */
    g_storageInstance->GetXactLockMgr()->Unlock(g_defaultPdbId, trx->GetCurrentXid());
    thrd->DestroyTransactionRuntime();
    (void)thrd->InitTransactionRuntime(g_defaultPdbId, nullptr, nullptr);

    /* Step 3: Run transaction: Insert original data into heap and index, trigger page splitting */
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->SetSnapshotCsn();
    /* Insert (2,2)(4,4)...(600,600) */
    for (i = 1; i <= rowNum; ++i) {
        int16 r1 = (int16)(i * 2);
        int32 r2 = (int32)(i * 2);
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        rowDef.column_int16 = r1;
        rowDef.column_int32 = r2;
        InsertSpecificIndexTuple(&rowDef, DefaultNullBitMap, true);
    }
    trx->Commit();

    /* Step 4: Run transaction: delete data and trigger rollback page */
    trx->Start();
    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit();  /* Just commit 'begin' command */
    trx->SetSnapshotCsn();
    for (i = 1; i <= rowNum; ++i) {
        /* First trigger rollback page */
        DeleteIndexTuple(indexTupVec[i - 1], true);
    }
    trx->UserAbortTransactionBlock();
    /* Rollback transaction */
    thrd->DestroyAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Commit();

    /* Step 5: Run transaction: delete data */
    trx->Start();
    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit();  /* Just commit 'begin' command */

    trx->SetSnapshotCsn();
    for (i = 1; i <= rowNum; ++i) {
        /* 
         * Then delete index tuple should be ok. We need to delete again because no recheck and retry 
         * in DeleteIndexTuple when tuple is chagned 
         */
        RetStatus ret = DeleteIndexTuple(indexTupVec[i - 1], true);
        ASSERT_EQ(ret, DSTORE_SUCC);
        DstorePfreeExt(indexTupVec[i-1]);
    }
    trx->Commit();
    thrd->DestroyAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Commit();
}

/*
 * CrConstruct case 1
 * 1. Start a long transaction1 and write sth to make command id valid (not on the index page).
 * 2. Do some writing in other transactions and commit to have an index page's td slots used up.
 * 3. Go back to the long transaction1 and write on the same index page. This writing will reuse a td slot that having
 *    the previous transaction's committing csn in td greater than the current snapshot csn.
 * 4. Construct a CR page on the same index page in the long transaction1. The tuples writen by current long
 *    transaction1 should be visible on the CR page, while all other tuples writen by step 2 should be rollback and
 *    invisible.
 *
 * CrConstruct case 2
 * Same steps 1~3 with case 1
 * 4. We have comand id = 2 in the long transaction1. Make it back to 1 and construct a CR page on the index page.
 *    NOTHING should be visible on the CR page.
 */
TEST_F(UTFiBtree, TestCrConstruct1_level0)
{
    /* Step 1: Build base data */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();

    /* Step 2: Insert a tuple onto page */
    Transaction *trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->SetSnapshotCsn();
    trx->SetSnapshotCsnForFlashback(COMMITSEQNO_FIRST_NORMAL);
    CommitSeqNo snapshotCsn = trx->GetSnapshotCsn();
    rowDef.column_int16 = (int16)0;
    rowDef.column_int32 = (int32)0;
    /* Make some writing but not on the index page */
    m_utTableHandler->FillTableWithSpecificData(&rowDef, 1, true);
    /* Do not committ here! */

    /* Step 3: Do insertion to use up all td slots on btrPage */
    int rowNum = 6;
    std::vector<IndexTuple *> indexTupVec;
    for (int i = 1; i <= rowNum; ++i) {
        thrd->CreateAutonomousTrx();
        trx = thrd->GetActiveTransaction();
        trx->Start();
        trx->BeginTransactionBlock();
        trx->Commit(); /* Just commit 'begin' command */

        trx->SetSnapshotCsn();
        int16 r1 = (int16)(i * 2);
        int32 r2 = (int32)(i * 2);
        rowDef.column_int16 = r1;
        rowDef.column_int32 = r2;
        IndexTuple *indexTuple = InsertSpecificIndexTuple(&rowDef, DefaultNullBitMap, true);
        indexTupVec.push_back(indexTuple);
        trx->EndTransactionBlock();
        trx->Commit(); 
        thrd->DestroyAutonomousTrx();
    }

    /* Step 4: Delete the tuples */
    for (int i = 1; i <= rowNum; ++i) {
        thrd->CreateAutonomousTrx();
        trx = thrd->GetActiveTransaction();
        trx->Start();
        trx->BeginTransactionBlock();
        trx->Commit(); /* Just commit 'begin' command */
        trx->SetSnapshotCsn();
        /*
         * Then delete index tuple should be ok. We need to delete again because no recheck and retry
         * in DeleteIndexTuple when tuple is chagned
         */
        RetStatus ret = DeleteIndexTuple(indexTupVec[i - 1], true);
        ASSERT_EQ(ret, DSTORE_SUCC);
        DstorePfreeExt(indexTupVec[i - 1]);
        trx->EndTransactionBlock();
        trx->Commit(); 
        thrd->DestroyAutonomousTrx();
    }

    /* Step 5. Prune page */
    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit(); /* Just commit 'begin' command */

    trx->SetSnapshotCsn();
    PageId pageId = m_utTableHandler->GetLastWorkingBtrPageId();
    BufferDesc *pageBuf = Btree::ReadAndCheckBtrPage(pageId, LW_EXCLUSIVE, g_storageInstance->GetBufferMgr(), g_defaultPdbId);
    BtreePagePrune prunePage(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                             m_utTableHandler->GetIndexScanKey(), pageBuf);
    prunePage.Prune();

    BtrPage *page = (BtrPage *)pageBuf->GetPage();
    EXPECT_EQ(page->GetMaxOffset(), 1U); /* The first tuple is kept since recycle min is COMMITSEQNO_FIRST_NORMAL */
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(pageBuf);
    trx->EndTransactionBlock();
    trx->Commit();
    thrd->DestroyAutonomousTrx();

    /* Step 6. Insert some tuples to reuse td slots */
    trx = thrd->GetActiveTransaction();
    EXPECT_EQ(snapshotCsn, trx->GetSnapshotCsn());
    rowDef.column_int16 = (int16)99;
    rowDef.column_int32 = (int32)99;
    InsertSpecificIndexTuple(&rowDef, DefaultNullBitMap, true);

    /* Case 1. Construct CR */
    BtrPage crPage[2];

    CRContext crCtx{g_defaultPdbId, INVALID_CSN, nullptr,
                    nullptr, nullptr, false, false, INVALID_SNAPSHOT, INVALID_XID};
    BtreeUndoContext btrUndoCtx{ g_defaultPdbId, m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId(),
        m_utTableHandler->GetIndexInfo(), g_storageInstance->GetBufferMgr(),
        m_utTableHandler->GetBtreeSmgr()->GetMetaCreateXid() };
    ConsistentReadContext crContext;
    crContext.pdbId = g_defaultPdbId;
    crContext.pageId = pageId;
    crContext.currentXid = thrd->GetActiveTransaction()->GetCurrentXid();
    crContext.snapshot = thrd->GetActiveTransaction()->GetSnapshot();
    crContext.dataPageExtraInfo = static_cast<void *>(&btrUndoCtx);
    crContext.destPage = crPage;
    crContext.crBufDesc = nullptr;
    g_storageInstance->GetBufferMgr()->ConsistentRead(crContext);

    EXPECT_EQ(crPage->GetMaxOffset(), 2U);
    /* The first tuple */
    ItemId *ii = crPage->GetItemIdPtr(1);
    EXPECT_TRUE(ii->IsRangePlaceholder());
    int16 value = DatumGetInt16(*(Datum *)crPage->GetIndexTuple(ii)->GetValues());
    EXPECT_EQ(value, (int16)2);
    /* The second tuple */
    ii = crPage->GetItemIdPtr(2);
    EXPECT_TRUE(ii->IsNormal());
    IndexTuple *tuple = crPage->GetIndexTuple(ii);
    EXPECT_FALSE(tuple->IsDeleted());
    value = DatumGetInt16(*(Datum *)tuple->GetValues());
    EXPECT_EQ(value, (int16)99);

    /* Case 2. Construct CR after make cid smaller */
    /* Insert one more tuple to have command id increased */
    rowDef.column_int16 = (int16)100;
    rowDef.column_int32 = (int32)100;
    InsertSpecificIndexTuple(&rowDef, DefaultNullBitMap, true);

    trx = thrd->GetActiveTransaction();
    Snapshot snapshot = thrd->GetActiveTransaction()->GetSnapshot();
    /* make the last insertion invisible */
    crContext.snapshot->SetCid(snapshot->GetCid() - 1);
    g_storageInstance->GetBufferMgr()->ConsistentRead(crContext);
    /* Still, we can only see the tuple writen by step 6 */
    /* The first tuple */
    ii = crPage->GetItemIdPtr(1);
    EXPECT_TRUE(ii->IsRangePlaceholder());
    value = DatumGetInt16(*(Datum *)crPage->GetIndexTuple(ii)->GetValues());
    EXPECT_EQ(value, (int16)2);
    /* The second tuple */
    ii = crPage->GetItemIdPtr(2);
    EXPECT_TRUE(ii->IsNormal());
    tuple = crPage->GetIndexTuple(ii);
    EXPECT_FALSE(tuple->IsDeleted());
    value = DatumGetInt16(*(Datum *)tuple->GetValues());
    EXPECT_EQ(value, (int16)99);
}

/*
 * CrConstruct case 1
 * 1. The current transaction start with snapshot CSN_1.
 * 2. Transaction A wrote td_1 and then been committed with CSN_2, which is greater than CSN_1.
 * 3. The current transaction re-used td_1 and made some changes on the page.
 * 4. The current transaction start to scan the page, using snapshot CSN_1
 * In this case, the current transaction should not see changes made by Transaction A because the
 * committed csn CSN_2 of Transaction A is greater the the current transaction's snapshot csn CSN_1.
 * Thus we need to check the previous transaction's CSN of TD
 */
TEST_F(UTFiBtree, TestCrConstruct2_level0)
{
    /* Step 1: Build base data */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();

    /* Step 2: Insert a tuple onto page */
    Transaction *trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->SetSnapshotCsn();
    rowDef.column_int16 = (int16)0;
    rowDef.column_int32 = (int32)0;
    /* Make some writing but not on the index page */
    InsertSpecificIndexTuple(&rowDef, DefaultNullBitMap, true);
    /* Do not commit here */
    Xid xid = thrd->GetCurrentXid();

    /* Step 3: Start a new transaction */
    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit(); /* Just commit 'begin' command */

    trx->SetSnapshotCsn();
    rowDef.column_int16 = 1;
    rowDef.column_int32 = 1;
    InsertSpecificIndexTuple(&rowDef, DefaultNullBitMap, true);

    trx->EndTransactionBlock();
    trx->Commit(); 
    thrd->DestroyAutonomousTrx();

    /* Step 4: construct CR */
    BtrPage crPage[2];
    trx = thrd->GetActiveTransaction();
    BtreeUndoContext btrUndoCtx{g_defaultPdbId, m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId(),
        m_utTableHandler->GetIndexInfo(), g_storageInstance->GetBufferMgr(),
        m_utTableHandler->GetBtreeSmgr()->GetMetaCreateXid()};
    ConsistentReadContext crContext;
    crContext.pdbId = g_defaultPdbId;
    crContext.pageId = m_utTableHandler->GetBtreeSmgr()->GetLowestSinglePageIdFromMetaCache();
    crContext.currentXid = trx->GetCurrentXid();
    crContext.snapshot = trx->GetSnapshot();
    crContext.dataPageExtraInfo = static_cast<void *>(&btrUndoCtx);
    crContext.destPage = crPage;
    crContext.crBufDesc = nullptr;
    g_storageInstance->GetBufferMgr()->ConsistentRead(crContext);

    EXPECT_EQ(crPage->GetMaxOffset(), 1U);
    /* Check tuples and TDs */
    IndexTuple *tuple = crPage->GetIndexTuple(crPage->GetLinkAndStatus()->GetFirstDataOffset());
    EXPECT_EQ(tuple->GetTdStatus(), ATTACH_TD_AS_NEW_OWNER);
    TD *td = crPage->GetTd(tuple->GetTdId());
    EXPECT_EQ(td->GetStatus(), TDStatus::OCCUPY_TRX_IN_PROGRESS);
    EXPECT_EQ(td->GetXid(), xid);
    trx->Commit();
}

/*
 * CrConstruct case 3
 * 1. insert and delete the same value until split
 * 2. rollback update once on left page
 * 3. delete the last tuple on left
 * 4. construct cr for right
 * the aborted record should not rollback on right page
 */
TEST_F(UTFiBtree, TestCrConstruct3_level0)
{
    /* Step 1: Build base data */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
    rowDef.column_int16 = (int16)0;
    rowDef.column_int32 = (int32)0;

    /* Step 2: Insert a tuple onto page */
    Transaction *trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->SetSnapshotCsn(); /* to avoid undo recycle */

    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit(); /* Just commit 'begin' command */

    trx->SetSnapshotCsn();
    IndexTuple *tuple1 = InsertIndexTupleOnly(&rowDef);
    PageId leftPage = m_utTableHandler->GetLastWorkingBtrPageId();

    trx->EndTransactionBlock();
    trx->Commit();
    thrd->DestroyAutonomousTrx();

    /* Step 3: Start a new transaction, do update */
    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit(); /* Just commit 'begin' command */

    trx->SetSnapshotCsn();
    bool pageFull = false;
    bool needAbort = false;
    bool finish = false;
    PageId rightPage;
    int counter = 0;
    uint32 tupleSize = tuple1->GetSize();
    BtreePagePayload pagePayload;
    while (true) {
        if (unlikely(needAbort)) {
            counter++;
            trx->EndTransactionBlock();
            trx->Abort();
            thrd->DestroyAutonomousTrx();

            thrd->CreateAutonomousTrx();
            trx = thrd->GetActiveTransaction();
            trx->Start();
            trx->BeginTransactionBlock();
            trx->Commit(); /* Just commit 'begin' command */
            needAbort = false;
        }
        if (unlikely(pageFull)) {
            counter++;
            trx->EndTransactionBlock();
            trx->Commit();
            thrd->DestroyAutonomousTrx();

            thrd->CreateAutonomousTrx();
            trx = thrd->GetActiveTransaction();
            trx->Start();
            trx->BeginTransactionBlock();
            trx->Commit(); /* Just commit 'begin' command */
            needAbort = true;
        }

        DeleteIndexTupleOnly(tuple1);
        DstorePfreeExt(tuple1);
        if (finish) {
            break;
        }
        tuple1 = InsertIndexTupleOnly(&rowDef);

        pagePayload.Init(g_defaultPdbId, leftPage, LW_SHARED, g_storageInstance->GetBufferMgr());
        pageFull =
            pagePayload.GetPage()->GetFreeSpace<FreeSpaceCondition::RAW>() < (tupleSize + sizeof(ItemId)) * 2;
        if (m_utTableHandler->GetBtreeSmgr()->GetLowestSingleLevelFromMetaCache() != 0) {
            rightPage = pagePayload.GetLinkAndStatus()->GetRight();
            finish = true;
        }
        pagePayload.Drop(g_storageInstance->GetBufferMgr());
    }
    trx->EndTransactionBlock();
    trx->Commit();
    thrd->DestroyAutonomousTrx();

    BufferDesc *metaBuf = INVALID_BUFFER_DESC;
    m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(DSTORE::LW_SHARED, &metaBuf);

    pagePayload.Init(g_defaultPdbId, leftPage, LW_SHARED, g_storageInstance->GetBufferMgr());
    char *page = pagePayload.GetPage()->Dump(metaBuf->GetPage(), true);
    ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("base left:\n%s", page));
    DstorePfreeExt(page);
    pagePayload.Drop(g_storageInstance->GetBufferMgr());

    pagePayload.Init(g_defaultPdbId, rightPage, LW_SHARED, g_storageInstance->GetBufferMgr());
    page = pagePayload.GetPage()->Dump(metaBuf->GetPage(), true);
    ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("base right:\n%s", page));
    DstorePfreeExt(page);
    pagePayload.Drop(g_storageInstance->GetBufferMgr());
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(metaBuf);

    /* Step 4: construct CR */
    BtrPage crPage[2];
    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit(); /* Just commit 'begin' command */
    trx->SetSnapshotCsn();
    BtreeUndoContext btrUndoCtx{g_defaultPdbId, m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId(),
        m_utTableHandler->GetIndexInfo(), g_storageInstance->GetBufferMgr(),
        m_utTableHandler->GetBtreeSmgr()->GetMetaCreateXid()};
    ConsistentReadContext crContext;
    crContext.pdbId = g_defaultPdbId;
    crContext.pageId = leftPage;
    crContext.currentXid = trx->GetCurrentXid();
    crContext.snapshot = trx->GetSnapshot();
    crContext.dataPageExtraInfo = static_cast<void *>(&btrUndoCtx);
    crContext.destPage = crPage;
    crContext.crBufDesc = nullptr;
    g_storageInstance->GetBufferMgr()->ConsistentRead(crContext);

    metaBuf = INVALID_BUFFER_DESC;
    m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(DSTORE::LW_SHARED, &metaBuf);
    EXPECT_EQ(((BtrPage *)crContext.destPage)->GetNonDeletedTupleNum(), 0);
    page = ((BtrPage *)crContext.destPage)->Dump(metaBuf->GetPage(), true);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(metaBuf);
    ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("cr left:\n%s", page));
    DstorePfreeExt(page);

    crContext.pageId = rightPage;
    crContext.crBufDesc = nullptr;
    g_storageInstance->GetBufferMgr()->ConsistentRead(crContext);

    metaBuf = INVALID_BUFFER_DESC;
    m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(DSTORE::LW_SHARED, &metaBuf);
    EXPECT_EQ(((BtrPage *)crContext.destPage)->GetNonDeletedTupleNum(), 0);
    page = ((BtrPage *)crContext.destPage)->Dump(metaBuf->GetPage(), true);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(metaBuf);
    ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("cr right\n%s", page));
    DstorePfreeExt(page);

    trx->EndTransactionBlock();
    trx->Commit();
    thrd->DestroyAutonomousTrx();

    thrd->GetActiveTransaction()->Commit();
}

/*
 * CrAfterRecycle case 
 * 1. insert until split
 * 2. save snapshot1
 * 3. delete all
 * 4. insert to trigger prune, then rollback to make page empty
 * 5. try recycle page -- should fail
 * 6. scan using old snapshot1, tuples inserted in step 1 should be visible
 * If page is recycled incorrectly, step 6 would fail for page missing
 */
TEST_F(UTFiBtree, DISABLED_TestCrAfterRecycle_level0)
{
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    /* Step 1: Build base data */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
    rowDef.column_int16 = (int16)0;
    rowDef.column_int32 = (int32)0;

    /* Step 2: Start a transaction, do insert */
    Transaction *trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->SetSnapshotCsn(); /* to avoid undo recycle */

    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit(); /* Just commit 'begin' command */

    trx->SetSnapshotCsn();
    bool pageFull = false;
    bool finish = false;
    PageId rightPage;
    int insertCounter = 0;
    std::vector<IndexTuple *> indexTuples{};
    BtreePagePayload pagePayload;
    PageId leafPageId;
    while (true) {
        insertCounter++;
        rowDef.column_int16 = (int16)insertCounter;
        rowDef.column_int32 = (int32)insertCounter;
        IndexTuple *tuple = InsertIndexTupleOnly(&rowDef);
        indexTuples.push_back(tuple);
        uint32 tupleSize = tuple->GetSize();

        if (finish) {
            break;
        }
        leafPageId = m_utTableHandler->GetBtreeSmgr()->GetRootPageIdFromMetaCache();
        pagePayload.Init(g_defaultPdbId, leafPageId, LW_SHARED, bufMgr);
        EXPECT_TRUE(pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
        bool pageFull =
            pagePayload.GetPage()->GetFreeSpace<FreeSpaceCondition::RAW>() < (tupleSize + sizeof(ItemId));
        pagePayload.Drop(bufMgr);
        if (pageFull) {
            finish = true;
        }
    }
    BufferDesc *metaBuf = INVALID_BUFFER_DESC;
    BtrMeta *meta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(DSTORE::LW_SHARED, &metaBuf);
    EXPECT_EQ(meta->GetRootLevel(), 1);
    bufMgr->UnlockAndRelease(metaBuf);
    trx->EndTransactionBlock();
    trx->Commit();
    thrd->DestroyAutonomousTrx();

    /* Step 3: Save snapshot csn */
    trx = thrd->GetActiveTransaction();
    CommitSeqNo insertVisibleCsn = thrd->GetNextCsn();
    trx->SetTransactionSnapshotCsn(insertVisibleCsn);

    /* Step 4: Delete all tuples */
    trx->SetSnapshotCsn(); /* to avoid undo recycle */
    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit(); /* Just commit 'begin' command */
    for (auto tuple : indexTuples) {
        DeleteIndexTupleOnly(tuple);
    }
    Xid deleteXid = trx->GetCurrentXid();
    trx->EndTransactionBlock();
    trx->Commit();
    thrd->DestroyAutonomousTrx();

    /* Step 5: insert to make page prune then rollback to make page empty */
    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit(); /* Just commit 'begin' command */

    XidStatus deleteXidStatus(deleteXid, trx);
    CommitSeqNo deleteCsn = deleteXidStatus.GetCsn();

    pageFull = false;
    finish = false;
    while (true) {
        rowDef.column_int16 = (int16)0;
        rowDef.column_int32 = (int32)0;
        IndexTuple *tuple = InsertIndexTupleOnly(&rowDef);
        uint32 tupleSize = tuple->GetSize();
        DstorePfreeExt(tuple);

        if (finish) {
            break;
        }
        leafPageId = m_utTableHandler->GetLastWorkingBtrPageId();
        pagePayload.Init(g_defaultPdbId, leafPageId, LW_SHARED, bufMgr);
        EXPECT_TRUE(pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
        bool pageFull =
            pagePayload.GetPage()->GetFreeSpace<FreeSpaceCondition::RAW>() < (tupleSize + sizeof(ItemId));
        pagePayload.Drop(bufMgr);
        if (pageFull) {
            finish = true;
        }
    }
    trx->EndTransactionBlock();
    trx->Abort();
    thrd->DestroyAutonomousTrx();

    g_storageInstance->GetCsnMgr()->UpdateLocalCsnMin();
    /* RecycleMin is smaller than deleteCsn, Thus the page should not be recycled now. */
    EXPECT_LT(g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(g_defaultPdbId), deleteCsn);

    /* Try recycle */
    BufferDesc *leafBuf = bufMgr->Read(g_defaultPdbId, leafPageId, LW_EXCLUSIVE);
    BtreePageUnlink btrUnlink(leafBuf, m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                              m_utTableHandler->GetIndexScanKey());
    EXPECT_FALSE(btrUnlink.IsPageEmptyAndRecyclable());
    EXPECT_EQ(btrUnlink.TryUnlinkPageFromBtree(), DSTORE_FAIL);
    /* Check recycleMin again in case it has been updated during recycle.
     * (actually UT would not update recycleMinCsn automatically, just in case the UT frame changes latter.) */
    EXPECT_LT(g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(g_defaultPdbId), deleteCsn);

    meta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(DSTORE::LW_SHARED, &metaBuf);
    EXPECT_EQ(meta->GetRootLevel(), 1);
    PageId rootPageId = meta->GetRootPageId();
    bufMgr->UnlockAndRelease(metaBuf);

    BufferDesc *rootBuf = bufMgr->Read(g_defaultPdbId, rootPageId, LW_SHARED);
    BtrPage *rootPage = static_cast<BtrPage *>(rootBuf->GetPage());
    EXPECT_EQ(rootPage->GetIndexTuple(1)->GetLowlevelIndexpageLink(), leafPageId);
    EXPECT_EQ(rootPage->GetMaxOffset(), 2);
    bufMgr->UnlockAndRelease(rootBuf);

    trx = thrd->GetActiveTransaction();
    trx->SetTransactionSnapshotCsn(insertVisibleCsn);
    IndexScanHandler *indexScanHandler =
        IndexInterface::ScanBegin(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), 0, 0);
    EXPECT_NE(indexScanHandler, nullptr);
    IndexInterface::IndexScanSetSnapshot(indexScanHandler, trx->GetSnapshot());
    IndexInterface::ScanRescan(indexScanHandler, nullptr);

    bool found;
    bool recheck = false;
    int foundCounter = 0;
    ItemPointerData fakeHeapCtid{{1, 1}, 1};
    while ((IndexInterface::ScanNext(indexScanHandler,
            ScanDirection::FORWARD_SCAN_DIRECTION, &found, &recheck) == DSTORE_SUCC) && found) {
        ItemPointer fetchedCtid = indexScanHandler->GetResultHeapCtid();
        EXPECT_EQ(*fetchedCtid, fakeHeapCtid);
        foundCounter++;
    }
    IndexInterface::ScanEnd(indexScanHandler);
    EXPECT_EQ(foundCounter, insertCounter);

    thrd->GetActiveTransaction()->Commit();
}

TEST_F(UTFiBtree, TestSameWithLastLeftFlag1_level0)
{
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    /* Step 1: Build base data */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
    rowDef.column_int16 = (int16)0;
    rowDef.column_int32 = (int32)0;

    /* Step 2: Start a transaction, do insert */
    Transaction *trx = thrd->GetActiveTransaction();
    trx->Start();

    ItemPointerData fakeHeapCtid2{{1, 1}, 2};
    IndexTuple *tuple = InsertIndexTupleOnly(&rowDef, DefaultNullBitMap, fakeHeapCtid2);
    DstorePfreeExt(tuple);
    ItemPointerData fakeHeapCtid1{{1, 1}, 1};
    tuple = InsertIndexTupleOnly(&rowDef, DefaultNullBitMap, fakeHeapCtid1);

    trx->Commit();

    /* Step 3: Inplace-update */
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->SetSnapshotCsn(); /* to avoid undo recycle */

    DeleteIndexTupleOnly(tuple);

    /* Split when insert, make the left hikey = first right data */
    g_utSplitContext = {1, 1, true};
    FAULT_INJECTION_ACTIVE(DstoreIndexFI::SET_SPLIT_POINT, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(DstoreIndexFI::FORCE_SPLIT, FI_GLOBAL);
    tuple = InsertIndexTupleOnly(&rowDef, DefaultNullBitMap, fakeHeapCtid1);
    FAULT_INJECTION_INACTIVE(DstoreIndexFI::FORCE_SPLIT, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(DstoreIndexFI::SET_SPLIT_POINT, FI_GLOBAL);

    /* Check flags */
    PageId rootPageId = m_utTableHandler->GetBtreeRootPageId();
    BufferDesc *rootPageBuf = bufMgr->Read(m_pdbId, rootPageId, LW_SHARED);
    BtrPage *rootPage = static_cast<BtrPage *>(rootPageBuf->GetPage());
    PageId leftLeafPageId = rootPage->GetIndexTuple(1)->GetLowlevelIndexpageLink();
    PageId rightLeafPageId = rootPage->GetIndexTuple(2)->GetLowlevelIndexpageLink();
    bufMgr->UnlockAndRelease(rootPageBuf);

    BufferDesc *leftLeafBuf = bufMgr->Read(m_pdbId, leftLeafPageId, LW_SHARED);
    BtrPage *leftLeaf = static_cast<BtrPage *>(leftLeafBuf->GetPage());
    IndexTuple *leftHikey = leftLeaf->GetIndexTuple(BTREE_PAGE_HIKEY);
    EXPECT_TRUE(leftHikey->IsPivot());
    EXPECT_TRUE(leftHikey->IsSameWithLastLeft());
    EXPECT_EQ(leftLeaf->GetMaxOffset(), 2);
    EXPECT_EQ(leftLeaf->GetRight(), rightLeafPageId);
    EXPECT_EQ(leftLeaf->GetIndexTuple(BTREE_PAGE_FIRSTKEY)->GetHeapCtid(), fakeHeapCtid1);
    bufMgr->UnlockAndRelease(leftLeafBuf);
    BufferDesc *rightLeafBuf = bufMgr->Read(m_pdbId, rightLeafPageId, LW_SHARED);
    BtrPage *rightLeaf = static_cast<BtrPage *>(rightLeafBuf->GetPage());
    IndexTuple *rightFirstKey = rightLeaf->GetIndexTuple(rightLeaf->GetLinkAndStatus()->GetFirstDataOffset());
    EXPECT_FALSE(rightFirstKey->IsPivot());
    EXPECT_TRUE(rightFirstKey->IsSameWithLastLeft());
    EXPECT_TRUE(rightFirstKey->IsDeleted());
    EXPECT_EQ(rightLeaf->GetMaxOffset(), 2);
    EXPECT_EQ(rightLeaf->GetLeft(), leftLeafPageId);
    bufMgr->UnlockAndRelease(rightLeafBuf);

    /* Step 4: Check CR construction */
    /* Case 1. Same transaction with smaller cid */
    Xid currXid = trx->GetCurrentXid();
    CommandId currCid = trx->GetCurCid();
    trx->SetSnapshotCid(currCid - 1);
    /* Rollback the last insertion only, two visible tuples */
    BtrPage crPage;
    BtreeUndoContext btrUndoCtx{m_pdbId, m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId(),
        m_utTableHandler->GetIndexInfo(), bufMgr, m_utTableHandler->GetBtreeSmgr()->GetMetaCreateXid(),
        BtreeUndoContextType::CONSTRUCT_CR};
    ConsistentReadContext crContext;
    crContext.pdbId = m_pdbId;
    crContext.pageId = leftLeafPageId;
    crContext.currentXid = currXid;
    crContext.snapshot = trx->GetSnapshot();
    crContext.dataPageExtraInfo = static_cast<void *>(&btrUndoCtx);
    crContext.destPage = &crPage;
    crContext.crBufDesc = nullptr;
    g_storageInstance->GetBufferMgr()->ConsistentRead(crContext);
    /* 1 tuple on left: hikey only */
    EXPECT_EQ(crPage.GetNonDeletedTupleNum(), 0);
    EXPECT_EQ(crPage.GetMaxOffset(), 2);
    EXPECT_TRUE(crPage.GetIndexTuple(1)->IsPivot());
    EXPECT_TRUE(crPage.GetItemIdPtr(2)->IsRangePlaceholder());
    EXPECT_EQ(crPage.GetIndexTuple(2)->GetHeapCtid(), fakeHeapCtid1);

    crContext.pageId = rightLeafPageId;
    g_storageInstance->GetBufferMgr()->ConsistentRead(crContext);
    /* 2 tuples on right: heapCtid(1, 1, 1) deleted, heapCtid(1, 1, 2) */
    EXPECT_EQ(crPage.GetNonDeletedTupleNum(), 1);
    EXPECT_EQ(crPage.GetMaxOffset(), 2);
    EXPECT_FALSE(crPage.GetIndexTuple(1)->IsPivot());
    EXPECT_TRUE(crPage.GetIndexTuple(1)->IsDeleted());
    EXPECT_EQ(crPage.GetIndexTuple(1)->GetHeapCtid(), fakeHeapCtid1);
    EXPECT_FALSE(crPage.GetIndexTuple(2)->IsDeleted());
    EXPECT_EQ(crPage.GetIndexTuple(2)->GetHeapCtid(), fakeHeapCtid2);

    trx->SetSnapshotCid(currCid - 2);
    crContext.pageId = leftLeafPageId;
    g_storageInstance->GetBufferMgr()->ConsistentRead(crContext);
    /* 3 tuples on left: hikey, heapCtid(1, 1, 1), heapCtid(1, 1, 1) range holder */
    /* Note: insert rollback to rangeholder, then deletion insert back to tuple. So, range holder is on the right */
    EXPECT_EQ(crPage.GetMaxOffset(), 3);
    EXPECT_TRUE(crPage.GetIndexTuple(1)->IsPivot());
    EXPECT_EQ(crPage.GetIndexTuple(2)->GetHeapCtid(), fakeHeapCtid1);
    EXPECT_FALSE(crPage.GetIndexTuple(2)->IsDeleted());
    EXPECT_EQ(crPage.GetIndexTuple(3)->GetHeapCtid(), fakeHeapCtid1);
    EXPECT_FALSE(crPage.GetIndexTuple(3)->IsDeleted());
    EXPECT_TRUE(crPage.GetItemIdPtr(3)->IsRangePlaceholder());

    crContext.pageId = rightLeafPageId;
    g_storageInstance->GetBufferMgr()->ConsistentRead(crContext);
    /* 2 tuples on right: heapCtid(1, 1, 1) deleted, heapCtid(1, 1, 2) */
    EXPECT_EQ(crPage.GetNonDeletedTupleNum(), 1);
    EXPECT_EQ(crPage.GetMaxOffset(), 2);
    EXPECT_FALSE(crPage.GetIndexTuple(1)->IsPivot());
    EXPECT_TRUE(crPage.GetIndexTuple(1)->IsDeleted());
    EXPECT_EQ(crPage.GetIndexTuple(1)->GetHeapCtid(), fakeHeapCtid1);
    EXPECT_FALSE(crPage.GetIndexTuple(2)->IsDeleted());
    EXPECT_EQ(crPage.GetIndexTuple(2)->GetHeapCtid(), fakeHeapCtid2);

    /* Release transaction lock in order to make it as failed transaction */
    g_storageInstance->GetXactLockMgr()->Unlock(g_defaultPdbId, trx->GetCurrentXid());
    thrd->DestroyTransactionRuntime();
    (void)thrd->InitTransactionRuntime(g_defaultPdbId, nullptr, nullptr);

    /* Step 5. PageRollback */
    trx = thrd->GetActiveTransaction();
    trx->Start();
    FAULT_INJECTION_ACTIVE(DstoreIndexFI::STOP_RETRY_SEARCH_BTREE, FI_GLOBAL);
    EXPECT_EQ(DeleteIndexTupleOnly(tuple), DSTORE_FAIL);
    FAULT_INJECTION_INACTIVE(DstoreIndexFI::STOP_RETRY_SEARCH_BTREE, FI_GLOBAL);
    /* The deletion should trigger page rollback, but only on left */
    leftLeafBuf = bufMgr->Read(m_pdbId, leftLeafPageId, LW_SHARED);
    leftLeaf = static_cast<BtrPage *>(leftLeafBuf->GetPage());
    /* 2 tuples on left: hikey, heapCtid(1, 1, 1) range holder */
    EXPECT_EQ(leftLeaf->GetMaxOffset(), 2);
    EXPECT_TRUE(leftLeaf->GetIndexTuple(1)->IsPivot());
    EXPECT_EQ(leftLeaf->GetIndexTuple(2)->GetHeapCtid(), fakeHeapCtid1);
    EXPECT_FALSE(leftLeaf->GetIndexTuple(2)->IsDeleted());
    EXPECT_TRUE(leftLeaf->GetItemIdPtr(2)->IsRangePlaceholder());
    bufMgr->UnlockAndRelease(leftLeafBuf);
    rightLeafBuf = bufMgr->Read(m_pdbId, rightLeafPageId, LW_SHARED);
    rightLeaf = static_cast<BtrPage *>(rightLeafBuf->GetPage());
    /* 2 tuples on right: heapCtid(1, 1, 1) deleted, heapCtid(1, 1, 2) */
    EXPECT_EQ(rightLeaf->GetMaxOffset(), 2);
    EXPECT_TRUE(rightLeaf->GetIndexTuple(1)->IsSameWithLastLeft());
    EXPECT_TRUE(rightLeaf->GetIndexTuple(1)->IsDeleted());
    EXPECT_EQ(rightLeaf->GetTd(rightLeaf->GetTupleTdId(1))->GetXid(), currXid);
    EXPECT_EQ(rightLeaf->GetIndexTuple(1)->GetHeapCtid(), fakeHeapCtid1);
    EXPECT_FALSE(rightLeaf->GetIndexTuple(2)->IsDeleted());
    EXPECT_EQ(rightLeaf->GetIndexTuple(2)->GetHeapCtid(), fakeHeapCtid2);
    bufMgr->UnlockAndRelease(rightLeafBuf);
    trx->Abort();

    /* Try page rollback again, the deletion should trigger page rollback on right page now, but failed */
    trx = thrd->GetActiveTransaction();
    trx->Start();
    FAULT_INJECTION_ACTIVE(DstoreIndexFI::STOP_RETRY_SEARCH_BTREE, FI_GLOBAL);
    EXPECT_EQ(DeleteIndexTupleOnly(tuple), DSTORE_FAIL);
    FAULT_INJECTION_INACTIVE(DstoreIndexFI::STOP_RETRY_SEARCH_BTREE, FI_GLOBAL);
    rightLeafBuf = bufMgr->Read(m_pdbId, rightLeafPageId, LW_SHARED);
    rightLeaf = static_cast<BtrPage *>(rightLeafBuf->GetPage());
    /* 2 tuples on right: heapCtid(1, 1, 1) deleted, heapCtid(1, 1, 2) */
    EXPECT_EQ(rightLeaf->GetMaxOffset(), 2);
    EXPECT_TRUE(rightLeaf->GetIndexTuple(1)->IsSameWithLastLeft());
    EXPECT_TRUE(rightLeaf->GetIndexTuple(1)->IsDeleted());
    EXPECT_EQ(rightLeaf->GetTd(rightLeaf->GetTupleTdId(1))->GetXid(), currXid);
    EXPECT_EQ(rightLeaf->GetIndexTuple(1)->GetHeapCtid(), fakeHeapCtid1);
    EXPECT_FALSE(rightLeaf->GetIndexTuple(2)->IsDeleted());
    EXPECT_EQ(rightLeaf->GetIndexTuple(2)->GetHeapCtid(), fakeHeapCtid2);
    bufMgr->UnlockAndRelease(rightLeafBuf);
    trx->Abort();

    /* Step 8. rollback failed transation by xid */
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(m_pdbId)->GetTransactionMgr();
    EXPECT_EQ(transactionMgr->RollbackTransactionSlot(currXid), DSTORE_SUCC);

    /* Check result */
    trx = thrd->GetActiveTransaction();
    trx->Start();
    rightLeafBuf = bufMgr->Read(m_pdbId, rightLeafPageId, LW_SHARED);
    rightLeaf = static_cast<BtrPage *>(rightLeafBuf->GetPage());
    IndexTuple *firstRightTuple = rightLeaf->GetIndexTuple(rightLeaf->GetLinkAndStatus()->GetFirstDataOffset());
    EXPECT_FALSE(firstRightTuple->IsDeleted());
    EXPECT_EQ(firstRightTuple->GetTdStatus(), ATTACH_TD_AS_HISTORY_OWNER);
    EXPECT_NE(rightLeaf->GetTd(firstRightTuple->GetTdId())->GetXid(), currXid);
    bufMgr->UnlockAndRelease(rightLeafBuf);
    trx->Commit();
}