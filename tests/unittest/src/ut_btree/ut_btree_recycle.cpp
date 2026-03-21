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
#include "ut_btree/ut_btree.h"
#include "index/dstore_btree_page_unlink.h"
#include "tablespace/dstore_tablespace.h"

void UTRecyclableBtree::RecyclePageAndCheck(int leafNo)
{
    /* Step 1. Empty selected leaf page and put it into recycle queue*/
    PageId emptyPageId = MakePageEmpty(leafNo);

    /* Step 2. Check leaf page */
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    PageId recyclePageId;
    GetBtrStorageMgr()->GetFromRecycleQueue(recyclePageId, csnMgr->GetRecycleCsnMin(INVALID_PDB_ID));
    if (!recyclePageId.IsValid()) {
        return;
    }

    EXPECT_EQ(recyclePageId, emptyPageId);
    BtreePagePayload recyclePayload;
    recyclePayload.Init(g_defaultPdbId, emptyPageId, DSTORE::LW_SHARED, m_bufMgr);
    EXPECT_TRUE(recyclePayload.GetLinkAndStatus()->TestLiveStatus(DSTORE::BtrPageLiveStatus::EMPTY_HAS_PARENT_HAS_SIB));
    EXPECT_FALSE(recyclePayload.GetLinkAndStatus()->IsRightmost());
    recyclePayload.Drop(m_bufMgr);

    /* Step 3. Set global_min_csn larger so that all transactions operated on the page can be considered as done,
     * making the page reusable if empty after deleting. */
    CommitSeqNo nextCsn = INVALID_CSN;
    csnMgr->GetNextCsn(nextCsn, false);
    csnMgr->SetLocalCsnMin(nextCsn);

    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    /* Set snapshot csn larger so that we can recycle the empty page. */
    csnMgr->GetNextCsn(nextCsn, false);
    transaction->SetSnapshotCsnForFlashback(nextCsn);

    /* Step 4. Unlink page from parent */
    recyclePayload.Init(g_defaultPdbId, emptyPageId, DSTORE::LW_EXCLUSIVE, m_bufMgr);
    if (recyclePayload.GetLinkAndStatus()->TestLiveStatus(DSTORE::BtrPageLiveStatus::EMPTY_HAS_PARENT_HAS_SIB)) {
        BtreePageUnlink btrUnlink(recyclePayload.GetBuffDesc(), GetIndexRel(), GetIndexInfo(), GetScanKey());
        EXPECT_EQ(btrUnlink.TryUnlinkPageFromBtree(), DSTORE::DSTORE_SUCC);

        transaction->Commit();
    } else {
        transaction->Abort();
    }
}

BufferDesc *UTRecyclableBtree::SearchForLeaf(int leafNo)
{
    EXPECT_LT(leafNo, m_numLeaves);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    /* Generate a searching key using left page's high key */
    IndexTuple *searchingTarget;
    if (leafNo == 0) {
        searchingTarget = IndexTuple::CreateMinusinfPivotTuple();
    } else {
        BufferDesc *leftBuf = bufMgr->Read(g_defaultPdbId, m_leafPageIds[leafNo - 1], LW_SHARED);
        searchingTarget = static_cast<BtrPage *>(leftBuf->GetPage())->GetIndexTuple(BTREE_PAGE_HIKEY)->Copy();
        bufMgr->UnlockAndRelease(leftBuf);
    }

    UpdateScanKeyWithValues(searchingTarget);

    /* Leaf page will be write-locked again after searching */
    BufferDesc *pageBuf = INVALID_BUFFER_DESC;
    SearchBtree(&pageBuf, true);
    EXPECT_EQ(pageBuf->GetPageId(), m_leafPageIds[leafNo]);

    return pageBuf;
}

PageId UTRecyclableBtree::MakePageEmpty(int leafNo, CommitSeqNo snapshotCsn)
{
    if (leafNo >= m_numLeaves) {
        return INVALID_PAGE_ID;
    }
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    PageId leafPageId = m_leafPageIds[leafNo];
    BtreePagePayload leaf;
    leaf.Init(g_defaultPdbId, leafPageId, LW_EXCLUSIVE, m_bufMgr);

    /* Record page info */
    bool isRoot = leaf.GetLinkAndStatus()->IsRoot();
    bool isRightMost = leaf.GetLinkAndStatus()->IsRightmost();
    bool isSplitComplete = leaf.GetLinkAndStatus()->IsSplitComplete();

    /* Set global_min_csn larger so that all transactions operated on the page can be considered as done, making the
     * page recyclable if empty after deleting. */
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    CommitSeqNo nextCsn = INVALID_CSN;
    csnMgr->GetNextCsn(nextCsn, false);
    csnMgr->SetLocalCsnMin(nextCsn);
    transaction->AllocTransactionSlot();
    /* Set snapshot csn larger so that we can recycle the empty page. */
    csnMgr->GetNextCsn(nextCsn, false);
    transaction->SetSnapshotCsnForFlashback(snapshotCsn == INVALID_CSN ? nextCsn : snapshotCsn);

    for (OffsetNumber delOff = leaf.GetLinkAndStatus()->GetFirstDataOffset(); delOff <= leaf.GetPage()->GetMaxOffset(); delOff++) {
        BtreeDelete btrDel(GetIndexRel(), GetIndexInfo(), GetScanKey());
        EXPECT_EQ(btrDel.DeleteTupleByOffset(leaf.GetBuffDesc(), delOff), DSTORE_SUCC);
    }

    transaction->Commit();

    leaf.Init(g_defaultPdbId, leafPageId, LW_EXCLUSIVE, m_bufMgr);
    for (OffsetNumber delOff = leaf.GetLinkAndStatus()->GetFirstDataOffset(); delOff <= leaf.GetPage()->GetMaxOffset(); delOff++) {
        IndexTuple *delTuple = leaf.GetPage()->GetIndexTuple(delOff);
        delTuple->SetTdStatus(DETACH_TD);
        TD *td = leaf.GetPage()->GetTd(leaf.GetPage()->GetTupleTdId(delOff));
        td->SetStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE);
        td->Reset();
    }
    g_storageInstance->GetBufferMgr()->MarkDirty(leaf.GetBuffDesc());

    /* Set global_min_csn larger so that transaction of deletion can be considered as done making the page reusable
     * if empty. */
    csnMgr->GetNextCsn(nextCsn, false);
    csnMgr->SetLocalCsnMin(nextCsn);

    /* Root or rightmost or incompletely split or non-empty page cannot be recycled, no need to check */
    if (isRoot || isRightMost || !isSplitComplete) {
        leaf.Drop(m_bufMgr);
        return leafPageId;
    }

    /* Check if empty page is recycled */
    EXPECT_EQ(leaf.GetPage()->GetMaxOffset(), leaf.GetLinkAndStatus()->GetFirstDataOffset());
    EXPECT_TRUE(leaf.GetLinkAndStatus()->TestLiveStatus(DSTORE::BtrPageLiveStatus::EMPTY_HAS_PARENT_HAS_SIB));

    BtreePagePrune btreePagePrune(GetIndexRel(), GetIndexInfo(), GetScanKey(), leaf.GetBuffDesc());
    EXPECT_TRUE(btreePagePrune.IsPageEmpty());

    leaf.Drop(m_bufMgr);
    return leafPageId;
}

IndexTuple **UTRecyclableBtree::CreateLeaves(int numLeaves)
{
    m_numLeaves = numLeaves;
    m_leafPageIds = (PageId *)DstorePalloc0(sizeof(PageId) * m_numLeaves);
    m_leafTuples = (IndexTuple **)DstorePalloc0(sizeof(IndexTuple *) * m_numLeaves);
    IndexTuple **upperInternalTuples = (IndexTuple **)DstorePalloc0(sizeof(IndexTuple *) * (m_numLeaves));

    /* Step 1. Define index contents and form leaf tuples
     * ---- We use an incrementing sequence of key attributes to ensure that every insertion could be on the rightmost
     * position to keep the same order of our generated tableDef and the final index in order to make it possible to
     * get a particular leaf page by the very tuple on it. */
    Datum value[2];
    bool isnull[] = {false, false};
    for (int i = 0; i < numLeaves; i++) {
        /* created the index by default template. attribute type is int32 */
        value[0] = Int16GetDatum(i * 100 + 1);
        value[1] = Int32GetDatum(i * 100 + 1);
        m_leafTuples[i] = IndexTuple::FormTuple(GetIndexInfo()->attributes, value, isnull);
        /* Generate fake ctid */
        ItemPointerData ctid = {{1, 1}, 1};
        m_leafTuples[i]->SetHeapCtid(&ctid);
    }

    /* Step 2. Write index tuple on leaf pages. */
    /* ---- A page will have only on index tuple to help make recyclable page easier by delete only on tuple */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BtreeStorageMgr *btrSmgr = m_utTableHandler->GetBtreeSmgr();
    /* Create leaves */
    upperInternalTuples[0] = IndexTuple::CreateMinusinfPivotTuple();
    upperInternalTuples[0]->SetKeyNum(0, false);
    BufferDesc *prevBuf = INVALID_BUFFER_DESC;
    PageId prevPageId = INVALID_PAGE_ID;
    for (int i = 0; i < numLeaves; i++) {
        BtreePagePayload pagePayload;
        btrSmgr->GetNewPage(pagePayload, DEFAULT_TD_COUNT);
        pagePayload.GetLinkAndStatus()->InitPageMeta(btrSmgr->GetBtrMetaPageId(), 0, false);
        if (prevBuf != INVALID_BUFFER_DESC) {
            ((BtrPage *)(prevBuf->GetPage()))->GetLinkAndStatus()->SetRight(pagePayload.GetPageId());
            pagePayload.GetLinkAndStatus()->SetLeft(prevBuf->GetPageId());
            bufMgr->MarkDirty(prevBuf);
            bufMgr->UnlockAndRelease(prevBuf);
        }
        prevBuf = pagePayload.GetBuffDesc();
        /* record new page id */
        m_leafPageIds[i] = pagePayload.GetPageId();
        upperInternalTuples[i]->SetLowlevelIndexpageLink(m_leafPageIds[i]);
        if (i == numLeaves - 1) {
            /* rightmost page has no hikey */
            pagePayload.GetPage()->AddTuple(m_leafTuples[i], BTREE_PAGE_HIKEY);
        } else {
            pagePayload.GetPage()->AddTuple(m_leafTuples[i], BTREE_PAGE_HIKEY);
            IndexTuple *hikey = CreateTruncateInternalTuple(m_leafTuples[i], m_leafTuples[i + 1]);
            pagePayload.GetPage()->AddTuple(hikey, BTREE_PAGE_HIKEY);
            upperInternalTuples[i + 1] = hikey;
        }
    }
    if (prevBuf != INVALID_BUFFER_DESC) {
        bufMgr->MarkDirty(prevBuf);
        bufMgr->UnlockAndRelease(prevBuf);
    }
    return upperInternalTuples;
}

void UTRecyclableBtree::CreateInternalPages(int &numDownlinks, IndexTuple **upperDownlinkTuples, int currLevel, bool isRoot)
{
    if (isRoot) {
        CreateRoot(numDownlinks, upperDownlinkTuples, currLevel);
        DstorePfree(upperDownlinkTuples);
        return;
    }

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BtreeStorageMgr *btrSmgr = m_utTableHandler->GetBtreeSmgr();
    BufferDesc *prevBuf = INVALID_BUFFER_DESC;
    for (int i = 1; i < numDownlinks; i++) {
        BtreePagePayload pagePayload;
        btrSmgr->GetNewPage(pagePayload);
        pagePayload.GetLinkAndStatus()->InitPageMeta(btrSmgr->GetBtrMetaPageId(), currLevel, false);
        if (prevBuf != INVALID_BUFFER_DESC) {
            ((BtrPage *)(prevBuf->GetPage()))->GetLinkAndStatus()->SetRight(pagePayload.GetPageId());
            pagePayload.GetLinkAndStatus()->SetLeft(prevBuf->GetPageId());
            bufMgr->MarkDirty(prevBuf);
            bufMgr->UnlockAndRelease(prevBuf);
        }
        prevBuf = pagePayload.GetBuffDesc();

        /* first data key of internal page should be infinity minus */
        IndexTuple *firstData = IndexTuple::CreateMinusinfPivotTuple();
        firstData->SetKeyNum(0, false);
        firstData->SetLowlevelIndexpageLink(upperDownlinkTuples[i - 1]->GetLowlevelIndexpageLink()) ;
        upperDownlinkTuples[i - 1]->SetLowlevelIndexpageLink(pagePayload.GetPageId());
        if (i == numDownlinks - 1) {
            /* rightmost page has no hikey */
            pagePayload.GetPage()->AddTuple(firstData, BTREE_PAGE_HIKEY);
            pagePayload.GetPage()->AddTuple(upperDownlinkTuples[i], BTREE_PAGE_FIRSTKEY);
        } else {
            /* need hikey */
            pagePayload.GetPage()->AddTuple(upperDownlinkTuples[i], BTREE_PAGE_HIKEY);
            pagePayload.GetPage()->AddTuple(firstData, BTREE_PAGE_FIRSTKEY);
        }
        DstorePfree(firstData);
    }

    if (prevBuf != INVALID_BUFFER_DESC) {
        bufMgr->MarkDirty(prevBuf);
        bufMgr->UnlockAndRelease(prevBuf);
    }
    numDownlinks--;
}

void UTRecyclableBtree::CreateRoot(int &numDownlinks, IndexTuple **upperDownlinkTuples, int rootLevel)
{
    StorageAssert(numDownlinks > 1);
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BtreeStorageMgr *btrSmgr = m_utTableHandler->GetBtreeSmgr();
    BtreePagePayload root;
    btrSmgr->GetNewPage(root);
    PageId rootPageId = root.GetPageId();
    root.GetLinkAndStatus()->InitPageMeta(btrSmgr->GetBtrMetaPageId(), rootLevel, true);
    for (int i = 0; i < numDownlinks; i++) {
        root.GetPage()->AddTuple(upperDownlinkTuples[i], (OffsetNumber)(i + 1));
    }
    GetBtrStorageMgr()->UpdateLowestSinglePageCache(root.buffDesc);
    bufMgr->MarkDirty(root.buffDesc);
    root.Drop(m_bufMgr);

    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = btrSmgr->GetBtrMeta(LW_EXCLUSIVE, &btrMetaBuf);
    btrMeta->SetBtreeMetaInfo(rootPageId, rootPageId, rootLevel, rootLevel);
    bufMgr->MarkDirty(btrMetaBuf);

    GetBtrStorageMgr()->SetMetaCache(btrMeta);
    bufMgr->UnlockAndRelease(btrMetaBuf);
}

/* Create a recyclable btree WITHOUT heap table */
UTRecyclableBtree *UTBtree::GenerateRecyclableSubtree(int numLeaves, int rootLevel)
{
    EXPECT_GE(numLeaves, 2);
    EXPECT_GE(rootLevel, 1);
    EXPECT_LE(rootLevel, numLeaves - 1);

    /* Step 1. Create an empty Btree index */
    bool isUnique = false;
    int indexCols[] = {1, 2};
    int numAttrs = 2;
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    /* Also create an empty RecyclableBtree */
    UTRecyclableBtree *recyclableBtree = DstoreNew(g_dstoreCurrentMemoryContext) UTRecyclableBtree(m_utTableHandler);

    IndexTuple **upperDownlinkTuples = recyclableBtree->CreateLeaves(numLeaves);
    int currLevel = 0;
    int numDownlinks = numLeaves;
    while (currLevel < rootLevel) {
        currLevel++;
        recyclableBtree->CreateInternalPages(numDownlinks, upperDownlinkTuples, currLevel, currLevel == rootLevel);
    }
    EXPECT_EQ(currLevel, rootLevel);

    /* Step 4. Check correctness of Btree structure */
    CheckRecyclableSubtree(numLeaves, rootLevel, recyclableBtree);
    return recyclableBtree;
}

void UTBtree::CheckRecyclableSubtree(int numLeaves, int rootLevel, UTRecyclableBtree *recyclableBtree)
{
    TupleDesc indexTupleDesc = recyclableBtree->GetIndexInfo()->attributes;

    /* Test btree meta page info */
    PageId btrRootPageId = TestBtreeMetaPage(recyclableBtree->GetBtrStorageMgr()->GetBtrMetaPageId());

    /* Test btree root page info */
    TestBtreeRootPage(btrRootPageId, rootLevel);

    std::queue<PageId> parentPages;
    std::queue<PageId> currLevelPages;
    parentPages.push(btrRootPageId);
    int level = rootLevel;
    while (level > 0) {
        currLevelPages = TestPivotTuples(indexTupleDesc, parentPages, level);
        parentPages = currLevelPages;
        level--;
        EXPECT_EQ(currLevelPages.size(), numLeaves - level);
    }
    /* Test all leaf pages */
    EXPECT_EQ(currLevelPages.size(), numLeaves);
    TestLeafTuples(indexTupleDesc, currLevelPages, numLeaves);
}

TEST_F(UTBtree, LeafLiveStatusTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 3;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    /* Case 1. Clear leftmost page */
    /* ---- Step 1. Delete the last item on the leftmost page and check */
    recyclableBtree->MakePageEmpty(0);

    /* Case 2. Clear the middle leaf whose left sibling is already set to EMPTY and check */
    recyclableBtree->MakePageEmpty(1);

    /* Case 3. Delete item on the rightmost leaf and check. The rightmost page can never be recycled. */
    PageId rightPageId = recyclableBtree->MakePageEmpty(2);
    /* ---- Check the rightmost page again */
    /* Assume deletion is done */
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    CommitSeqNo nextCsn = INVALID_CSN;
    csnMgr->GetNextCsn(nextCsn, false);
    csnMgr->SetLocalCsnMin(nextCsn);
    BtreePagePayload rightmostPayload;
    rightmostPayload.Init(g_defaultPdbId, rightPageId, DSTORE::LW_SHARED, g_storageInstance->GetBufferMgr());
    BtreePagePrune btreePagePrune(recyclableBtree->GetIndexRel(), recyclableBtree->GetIndexInfo(),
                                  recyclableBtree->GetScanKey(), rightmostPayload.GetBuffDesc());
    /* Rightmost page will never pass emptiness check even through it's empty */
    EXPECT_FALSE(btreePagePrune.IsPageEmpty());
    EXPECT_TRUE(rightmostPayload.GetLinkAndStatus()->TestLiveStatus(DSTORE::BtrPageLiveStatus::NORMAL_USING));

    rightmostPayload.Drop(g_storageInstance->GetBufferMgr());
    delete recyclableBtree;
}

TEST_F(UTBtree, LeafRecycleTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 4;
    int rootLevel = 2;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    /* Case 1. Recycle and reuse a middle page (neither leftmost nor rightmost). Try the 3rd leaf page here. */
    /* ---- Since we have 4 leaves in total, according to the rule we built this recyclable Btree, the 3rd leaf page has
     * the same parent with the 4th (rightmost) leaf page, meaning only a single leaf can be reused in this case.*/
    recyclableBtree->RecyclePageAndCheck(2);

    /* Case 2. Recycle and reuse another middle page. Thy the 2nd page here. */
    /* ---- The 2nd page has a to-be-empty parent. We are going to delete a subtree in this case. */
    recyclableBtree->RecyclePageAndCheck(1);

    delete recyclableBtree;
}

TEST_F(UTBtree, SubtreeRecycleTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 4;
    int rootLevel = 3;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    /* Case 1. Recycle a "skinny subtree". */
    /* ---- Since we have 4 leaves in total, according to the rule we built this recyclable Btree, the leftmost leaf
     * page belongs to a "skinny subtree" that we can recycle 3 pages from level 0 to level2.*/
    recyclableBtree->RecyclePageAndCheck(0);

    /* Case 2. Non-parent page check */
    /* Get stack before change parent live status, or we'll fail for searching leaf */
    BufferDesc *leafBuf = recyclableBtree->SearchForLeaf(1);

    PageId lowestSinglePageId = recyclableBtree->GetBtrStorageMgr()->GetLowestSinglePageIdFromMetaCache();
    uint32 lowestSingleLevel = recyclableBtree->GetBtrStorageMgr()->GetLowestSingleLevelFromMetaCache();
    EXPECT_EQ(rootLevel, recyclableBtree->GetBtrStorageMgr()->GetRootLevelFromMetaCache());
    EXPECT_EQ(rootLevel, lowestSingleLevel + 1);
    /* Set parent of recyclable subtree unlinked, making subtree root have no parent */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *subtreeRootBuf = bufMgr->Read(g_defaultPdbId, lowestSinglePageId, LW_EXCLUSIVE);
    BtrPageLinkAndStatus *linkStat = static_cast<BtrPage *>(subtreeRootBuf->GetPage())->GetLinkAndStatus();
    linkStat->SetLiveStatus(DSTORE::BtrPageLiveStatus::EMPTY_NO_PARENT_NO_SIB);
    bufMgr->MarkDirty(subtreeRootBuf);
    bufMgr->UnlockAndRelease(subtreeRootBuf);

    /* empty page */
    BtrPage *recyclePage = static_cast<BtrPage *>(leafBuf->GetPage());
    recyclePage->GetIndexTuple(2)->SetDeleted();
    recyclePage->GetLinkAndStatus()->SetLiveStatus(DSTORE::BtrPageLiveStatus::EMPTY_HAS_PARENT_HAS_SIB);
    /* recycle subtree */
    BtreePageUnlink btrUnlink(leafBuf, recyclableBtree->GetIndexRel(),
                              recyclableBtree->GetIndexInfo(),recyclableBtree->GetScanKey());
    recyclableBtree->SetBtrPageUnlinkStack(&btrUnlink);
    EXPECT_EQ(btrUnlink.TryUnlinkPageFromBtree(), DSTORE_FAIL);

    delete recyclableBtree;
}

TEST_F(UTBtree, UnrecyclablePageTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 4;
    int rootLevel = 2;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    /* Case 1. Rightmost page */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    PageId rightPageId = recyclableBtree->GetLeaf(3);
    BufferDesc *recycleBuf = bufMgr->Read(g_defaultPdbId, rightPageId, LW_EXCLUSIVE);
    BtreePageUnlink btrUnlinkRightMost(recycleBuf, recyclableBtree->GetIndexRel(),
                                       recyclableBtree->GetIndexInfo(),recyclableBtree->GetScanKey());
    EXPECT_EQ(btrUnlinkRightMost.TryUnlinkPageFromBtree(), DSTORE_FAIL);

    /* Case 2. Left sibling is splitting */
    PageId leftPageId = recyclableBtree->GetLeaf(0);
    BufferDesc *leftSibBuf = bufMgr->Read(g_defaultPdbId, leftPageId, LW_EXCLUSIVE);
    BtrPageLinkAndStatus *leftSibLinkStat = static_cast<BtrPage *>(leftSibBuf->GetPage())->GetLinkAndStatus();
    leftSibLinkStat->SetSplitStatus(DSTORE::BtrPageSplitStatus::SPLIT_INCOMPLETE);
    bufMgr->MarkDirty(leftSibBuf);
    bufMgr->UnlockAndRelease(leftSibBuf);

    PageId recyclePageId = recyclableBtree->GetLeaf(1);
    recycleBuf = bufMgr->Read(g_defaultPdbId, recyclePageId, LW_EXCLUSIVE);
    BtreePageUnlink btrUnlinkSplittingLeft(recycleBuf, recyclableBtree->GetIndexRel(),
                                           recyclableBtree->GetIndexInfo(),recyclableBtree->GetScanKey());
    EXPECT_EQ(btrUnlinkSplittingLeft.TryUnlinkPageFromBtree(), DSTORE_FAIL);

    /* Case 3. right sib has no parent */
    BufferDesc *rightSibBuf = bufMgr->Read(g_defaultPdbId, rightPageId, LW_EXCLUSIVE);
    BtrPageLinkAndStatus *rightSibLinkStat = static_cast<BtrPage *>(rightSibBuf->GetPage())->GetLinkAndStatus();
    rightSibLinkStat->SetLiveStatus(DSTORE::BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB);
    bufMgr->MarkDirty(rightSibBuf);
    bufMgr->UnlockAndRelease(rightSibBuf);

    recyclePageId = recyclableBtree->GetLeaf(2);
    recycleBuf = bufMgr->Read(g_defaultPdbId, recyclePageId, LW_EXCLUSIVE);
    BtreePageUnlink btrUnlinkNoParentRight(recycleBuf, recyclableBtree->GetIndexRel(),
                                           recyclableBtree->GetIndexInfo(),recyclableBtree->GetScanKey());
    EXPECT_EQ(btrUnlinkNoParentRight.TryUnlinkPageFromBtree(), DSTORE_FAIL);

    delete recyclableBtree;
}

TEST_F(UTBtree, BatchRecycleBtreePageWhenEmptyNotDeadTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 3;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    /* Step 1. Put pages into RecycleQueue, set up page meta and delete tuples */
    PageId emptyPageId = recyclableBtree->MakePageEmpty(1);

    /* Step 2. set TD for deleted tuples to mock failure in BatchRecycleBtreePage, page empty but not dead */
    BtreePagePayload payload;
    payload.Init(g_defaultPdbId, emptyPageId, DSTORE::LW_EXCLUSIVE, g_storageInstance->GetBufferMgr());
    BtrPage *page = payload.GetPage();
    CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);

    OffsetNumber minoff = payload.GetLinkAndStatus()->GetFirstDataOffset();
    OffsetNumber maxoff = page->GetMaxOffset();
    OffsetNumber offnum;
    for (offnum = minoff; offnum <= maxoff; offnum++) {
        IndexTuple *tuple = (IndexTuple *)page->GetRowData(offnum);
        if (tuple->IsDeleted()) {
            TD *td = page->GetTd(page->GetTupleTdId(offnum));
            td->SetCsn(recycleMinCsn + offnum);
            tuple->SetTdStatus(DSTORE::ATTACH_TD_AS_NEW_OWNER);
        }
    }
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(payload.GetBuffDesc());

    /* Step 4. Call BatchRecycleBtreePage, the page is empty but not dead, it should push back into
     * RecycleQueue with revised CSN */
    BtreePageRecycle pageRecycle(recyclableBtree->GetIndexRel());
    pageRecycle.BatchRecycleBtreePage(recyclableBtree->GetIndexInfo(), recyclableBtree->GetScanKey());
    PageId recyclePageId;
    recyclableBtree->GetBtrStorageMgr()->GetFromRecycleQueue(recyclePageId, 100);
    EXPECT_EQ(recyclePageId, emptyPageId);

    delete recyclableBtree;
}

TEST_F(UTBtree, BatchRecycleBtreePageTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 4;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);
    BtreeStorageMgr *btrSmgr = recyclableBtree->GetBtrStorageMgr();
    CommitSeqNo nextCsn = INVALID_CSN;
    g_storageInstance->GetCsnMgr()->GetNextCsn(nextCsn, false);

    /* Unlink middle two pages from siblings*/
    /* Step 1. Put pages into PruneQueue, set up page meta and delete tuples */
    PageId leftPageId = recyclableBtree->MakePageEmpty(1);
    PageId rightPageId = recyclableBtree->MakePageEmpty(2);

    /* Step 2. call BatchRecycleBtreePage and check pages are in FreeQueue */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    BtreePageRecycle pageRecycle(recyclableBtree->GetIndexRel());
    pageRecycle.BatchRecycleBtreePage(recyclableBtree->GetIndexInfo(), recyclableBtree->GetScanKey());
    txn->Commit();
    g_storageInstance->GetCsnMgr()->SetLocalCsnMin(100);

    bool foundLeft = false;
    bool foudRight = false;
    for (int i = 0; i < 10; i++) {
        PageId freePageId = btrSmgr->GetFromFreeQueue();
        if (freePageId == leftPageId) {
            foundLeft = true;
        } else if (freePageId == rightPageId) {
            foudRight = true;
        }
    }
    EXPECT_TRUE(foundLeft);
    EXPECT_TRUE(foudRight);

    delete recyclableBtree;
}

TEST_F(UTBtree, BatchRecycleBtreePageWhenNonRecyclableTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 3;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);
    BtreeStorageMgr *btrSmgr = recyclableBtree->GetBtrStorageMgr();

    /* Step 1. Put page into RecycleQueue, set up page meta and delete tuples */
    PageId emptyPageId = recyclableBtree->MakePageEmpty(1);

    /* Step 2. set the page as not prunable to mock failure scenario in TryRecycleBtreePage */
    BtreePagePayload emptyPage;
    emptyPage.Init(g_defaultPdbId, emptyPageId, LW_EXCLUSIVE, g_storageInstance->GetBufferMgr());
    emptyPage.GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    emptyPage.GetLinkAndStatus()->SetLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB);
    g_storageInstance->GetBufferMgr()->MarkDirty(emptyPage.GetBuffDesc());
    emptyPage.Drop(g_storageInstance->GetBufferMgr());

    /* Step 3. BatchRecycleBtreePage and check page is not in FreeQueue */
    BtreePageRecycle pageRecycle(recyclableBtree->GetIndexRel());
    pageRecycle.BatchRecycleBtreePage(recyclableBtree->GetIndexInfo(), recyclableBtree->GetScanKey());
    g_storageInstance->GetCsnMgr()->SetLocalCsnMin(100);

    bool foundPage = false;
    for (int i = 0; i < 10; i++) {
        PageId freePageId = btrSmgr->GetFromFreeQueue();
        if (freePageId == emptyPageId) {
            foundPage = true;
        }
    }
    EXPECT_FALSE(foundPage);

    delete recyclableBtree;
}

TEST_F(UTBtree, PutIntoRecycleQueueWhenNotNormalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 3;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);
    BtreeStorageMgr *btrSmgr = recyclableBtree->GetBtrStorageMgr();
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();

    /* Step 1. Initialize PageId and set it as not NORMAL_USING to mock failure scenario  */
    PageId pageId = {5121, 10};
    BtreePagePayload emptyPage;
    emptyPage.Init(g_defaultPdbId, pageId, LW_EXCLUSIVE, bufMgr, false);
    emptyPage.GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    emptyPage.GetLinkAndStatus()->SetLiveStatus(BtrPageLiveStatus::EMPTY_HAS_PARENT_HAS_SIB);
    bufMgr->MarkDirty(emptyPage.GetBuffDesc());
    emptyPage.Drop(bufMgr);

    /* Step 2. Call PutIntoRecycleQueueIfEmpty and check page is not in RecycleQueue */
    BufferDesc *pageBuf = bufMgr->Read(g_defaultPdbId, pageId, LW_EXCLUSIVE);
    BtreePageRecycle pageRecycle(recyclableBtree->GetIndexRel());
    pageRecycle.PutIntoRecycleQueueIfEmpty(pageBuf);
    PageId recyclePageId;
    btrSmgr->GetFromRecycleQueue(recyclePageId, 100);
    EXPECT_NE(recyclePageId, pageId);

    delete recyclableBtree;
}

/*
 * We' dont use UTRecyclableBtree tree in this case to test Unlink in a real deletion case
 * Also we'll construnct a page that all tuples have the same key, to test whether we'can
 * still get the correct page when unlink.
 */
TEST_F(UTBtree, UnlinkPageTest_level0)
{
    RetStatus ret = DSTORE_FAIL;
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    /* Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = true;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    /* Insert & delete the same tuple, until the page split */
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    ItemPointerData fakeHeapCtid = {{1, 1}, 1};
    PageId leafPageIds[4];
    int leafCounter = 0;
    int tupleCounter = 0;

    Transaction *txn = thrd->GetActiveTransaction();
    /* Do all insertion and deletion within the same transaction to avoid page prune before splitting */
    txn->Start();
    txn->SetSnapshotCsn();
    CommitSeqNo firstCsn = txn->GetSnapshotCsn();
    /* The rightmost cannot be unlinked. So we need another two pages to test whether
     * unlink works well. */
    while (leafCounter < 4) {
        ret = m_utTableHandler->InsertIndexTupleOnly(&insertRow, DefaultNullBitMap, &fakeHeapCtid);
        EXPECT_EQ(ret, DSTORE_SUCC);
        txn->IncreaseCommandCounter();
        if (unlikely(leafCounter == 0)) {
            /* Will hit only once */
            leafPageIds[leafCounter++] = m_utTableHandler->GetBtreeSmgr()->GetRootPageIdFromMetaCache();
        } else if (m_utTableHandler->GetBtreeSmgr()->GetRootLevelFromMetaCache() != 0) {
            BufferDesc *rootBuf = m_utTableHandler->GetBtreeSmgr()->GetLowestSinglePageDescFromCache();
            EXPECT_NE(rootBuf, INVALID_BUFFER_DESC);
            rootBuf->Pin<false>();
            bufMgr->LockContent(rootBuf, LW_SHARED);
            BtrPage *rootPage = (BtrPage *)rootBuf->GetPage();
            if (rootPage->GetMaxOffset() > leafCounter) {
                leafCounter++;
                for (int i = 0; i < leafCounter; i++) {
                    leafPageIds[i] = rootPage->GetIndexTuple(i + 1)->GetLowlevelIndexpageLink();
                }
            }
            bufMgr->UnlockAndRelease(rootBuf);
        }

        ret = m_utTableHandler->DeleteIndexTupleOnly(&insertRow, DefaultNullBitMap, &fakeHeapCtid);
        EXPECT_EQ(ret, DSTORE_SUCC);
        txn->IncreaseCommandCounter();
        tupleCounter++;
    }
    Xid writingXid = txn->GetCurrentXid();
    txn->Commit();
    XidStatus xidStatus(writingXid, txn);
    CommitSeqNo committedCsn = xidStatus.GetCsn();

    /* Update csn min to make the pages recyclable */
    g_storageInstance->GetCsnMgr()->UpdateLocalCsnMin();
    CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    EXPECT_GT(recycleMinCsn, committedCsn);

    /* Prune one of the pages before unlink to test unlink empty page */
    BufferDesc *pruneBuf = Btree::ReadAndCheckBtrPage(leafPageIds[1], DSTORE::LW_EXCLUSIVE, bufMgr, g_defaultPdbId);
    BtreePagePrune prune(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                         m_utTableHandler->GetIndexScanKey(), pruneBuf);
    ret = prune.Prune();
    EXPECT_EQ(ret, DSTORE_SUCC);
    bufMgr->UnlockAndRelease(pruneBuf);

    /* Test data on page. should be empty now */
    txn->Start();
    txn->SetSnapshotCsn();
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), 0, 0);
    indexScan.InitSnapshot(txn->GetSnapshot());
    indexScan.BeginScan();
    indexScan.ReScan(nullptr);
    int visTupleCount = 0;
    bool found;
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
        visTupleCount++;
    }
    EXPECT_EQ(visTupleCount, 0);

    indexScan.ReScan(nullptr);
    visTupleCount = 0;
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
        visTupleCount++;
    }
    EXPECT_EQ(visTupleCount, 0);

    txn->SetSnapshotCsnForFlashback(firstCsn);
    indexScan.InitSnapshot(txn->GetSnapshot());
    indexScan.ReScan(nullptr);
    visTupleCount = 0;
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
        visTupleCount++;
    }
    EXPECT_EQ(visTupleCount, 0);

    indexScan.ReScan(nullptr);
    visTupleCount = 0;
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
        visTupleCount++;
    }
    EXPECT_EQ(visTupleCount, 0);
    indexScan.EndScan();
    txn->Commit();

    /* Now try unlink pages */
    txn->Start();
    txn->SetSnapshotCsn();
    EXPECT_EQ(thrd->GetActiveTransaction()->AllocTransactionSlot(), DSTORE_SUCC);
    for (int i = leafCounter - 1; i >= 0; i--) {
        BufferDesc *unlinkBuf = Btree::ReadAndCheckBtrPage(leafPageIds[i], DSTORE::LW_EXCLUSIVE, bufMgr, g_defaultPdbId);
        BtreePageUnlink unlink(unlinkBuf, m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                               m_utTableHandler->GetIndexScanKey());
        ret = unlink.TryUnlinkPageFromBtree();
        if (i == leafCounter - 1) {
            /* The rightmost page should not be recycled */
            EXPECT_EQ(ret, DSTORE_FAIL);
        } else {
            /* Other pages should be recycled */
            EXPECT_EQ(ret, DSTORE_SUCC);
        }
    }
    txn->Commit();
}

