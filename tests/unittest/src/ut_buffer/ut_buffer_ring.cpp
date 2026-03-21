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
#include <thread>
#include <random>
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_buffer/ut_buffer_util.h"
#include "ut_buffer/ut_buffer.h"
#include "common/memory/dstore_mctx.h"

using namespace DSTORE;

BufMgr *g_tmpbufPool;
struct TestContext {
    BufMgr* bufferPool;
    FileId fileId;
    BlockNumber maxBlock;
    BufferRing ringBuf;
};

class BufferRingTest : public DSTORETEST {
protected:
    
    void SetUp() override
    {
        g_tmpbufPool = nullptr;
        m_guc.buffer = FAKE_FILES[0].max_block;
        DSTORETEST::SetUp();
        InstallDatabase(&DSTORETEST::m_guc, m_ut_memory_context);
        BufferMgrFakeStorageInstance *bufMgrInstance = DstoreNew(m_ut_memory_context)BufferMgrFakeStorageInstance();
        bufMgrInstance->Startup(&DSTORETEST::m_guc);

        VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
        for (Size i = 0; i < FAKE_SIZE; i++) {
            UtBufferUtils::prepare_fake_file(vfs, FAKE_FILES[i].file_id, FAKE_FILES[i].max_block, FAKE_FILES[i].path);
        }

#ifdef ENABLE_FAULT_INJECTION

        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFRING_TRY_FLUSH_FAIL, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFRING_MAKE_CR_FREE_FAIL, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFRING_BASE_UNABLE_REUSE, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFRING_REUSE_DIRTY_BUF, false, BufferRingTest::WriteNormalBuf),
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFRING_REUSE_BUF_IN_HASHTABLE, false,
                                  BufferRingTest::ReadNormalBuf),
        };
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

#endif
    }
    void TearDown() override
    {
        VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
        for (Size i = 0; i < FAKE_SIZE; i++) {
            UtBufferUtils::remove_fake_file(vfs, FAKE_FILES[i].file_id, FAKE_FILES[i].path);
        }
        
        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->Shutdown();
        delete bufMgrInstance;
        DSTORETEST::TearDown();
    }

    void CreateBufferRingAndCheck(int ringSize, BufferAccessType type, BufferRing &ringBuf)
    {
        BufMgrInterface *bufferPool = g_tmpbufPool;
        StorageAssert(bufferPool != nullptr);
        g_storageInstance->GetGuc()->bulkReadRingSize = ringSize * BLCKSZ / 1024;
        g_storageInstance->GetGuc()->bulkWriteRingSize = ringSize * BLCKSZ / 1024;
        ringBuf = CreateBufferRing(type);
        ASSERT_NE(ringBuf, nullptr);
        ASSERT_EQ(ringBuf->ringSize, ringSize);
        ASSERT_EQ(ringBuf->curPos, -1);
        for (int i = 0; i < ringBuf->ringSize; i++) {
            ASSERT_EQ(ringBuf->bufferDescArray[i], INVALID_BUFFER_DESC);
        }
    }

    static void ReadNormalBuf(const FaultInjectionEntry *entry, PageId* pageId)
    {
        StorageAssert(g_tmpbufPool != nullptr);
        FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFRING_REUSE_BUF_IN_HASHTABLE, FI_GLOBAL);
        BufMgrInterface *bufferPool = g_tmpbufPool;
        BufferDesc *buffer = bufferPool->Read(g_defaultPdbId, *pageId, LW_SHARED);
        bufferPool->UnlockAndRelease(buffer);
    }

    static void WriteNormalBuf(const FaultInjectionEntry *entry, PageId pageId)
    {
        FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFRING_REUSE_DIRTY_BUF, FI_GLOBAL);
        WriteBuffer(pageId);
    }

    static void WriteBuffer(PageId pageId) {
        StorageAssert(g_tmpbufPool != nullptr);
        BufMgrInterface *bufferPool = g_tmpbufPool;
        BufferDesc *buffer = bufferPool->Read(g_defaultPdbId, pageId, LW_EXCLUSIVE);
        bufferPool->MarkDirty(buffer);
        bufferPool->UnlockAndRelease(buffer);
    }



    BufferDesc *BuildCrBuffer(BufferDesc *baseBuf, BufferRing ringBuf) {
        BufMgrInterface *bufferPool = g_tmpbufPool;
        StorageAssert(g_tmpbufPool != nullptr);
        StorageAssert(baseBuf != INVALID_BUFFER_DESC);
        CommitSeqNo pageMaxCsn = 100;
        uint64 latestWriteTime = baseBuf->GetLastModifyTime();
        BufferDesc *crBuffer = bufferPool->ReadOrAllocCr(baseBuf, latestWriteTime, ringBuf);
        StorageAssert(crBuffer != INVALID_BUFFER_DESC);
        bufferPool->FinishCrBuild(crBuffer, pageMaxCsn);
        bufferPool->Release(crBuffer);
        return crBuffer;
    }

    void PrepareBuffer(BlockNumber maxBlock, FileId fileId)
    {
        BufMgrInterface *bufferPool = g_tmpbufPool;
        StorageAssert(bufferPool != nullptr);

        /* BaseBuf1, BaseBuf2, BaseBuf3, BaseBuf4, CrBuf3, CrBuf4, BaseBuf5, BaseBuf6, CrBuf5, CrBuf6, ...... */
        BufferDesc *preBaseBuf[2] = {nullptr};
        for (BlockNumber blockNum = 0; blockNum < maxBlock / 2; blockNum += 2) {
            PageId pageId = {fileId, blockNum};
            BufferDesc *baseBuffer1 = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED);
            ASSERT_NE(baseBuffer1, INVALID_BUFFER_DESC);
            baseBuffer1->controller->lastPageModifyTime = 0;

            pageId = {fileId, blockNum + 1};
            BufferDesc *baseBuffer2 = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED);
            ASSERT_NE(baseBuffer2, INVALID_BUFFER_DESC);
            baseBuffer2->controller->lastPageModifyTime = 0;

            if (preBaseBuf[0] != nullptr) {
                BufferDesc *crBuffer1 = BuildCrBuffer(preBaseBuf[0], nullptr);
                bufferPool->UnlockAndRelease(preBaseBuf[0]);

                BufferDesc *crBuffer2 = BuildCrBuffer(preBaseBuf[1], nullptr);
                bufferPool->UnlockAndRelease(preBaseBuf[1]);
            }
            preBaseBuf[0] = baseBuffer1;
            preBaseBuf[1] = baseBuffer2;
        }
        if (preBaseBuf[0] != nullptr) {
            BufferDesc *crBuffer1 = BuildCrBuffer(preBaseBuf[0], nullptr);
            bufferPool->UnlockAndRelease(preBaseBuf[0]);
            BufferDesc *crBuffer2 = BuildCrBuffer(preBaseBuf[1], nullptr);
            bufferPool->UnlockAndRelease(preBaseBuf[1]);
        }
    }

    void PrepareBuffer2(FileId fileId, BufferDesc **buffers)
    {
        BufMgrInterface *bufferPool = g_tmpbufPool;
        StorageAssert(bufferPool != nullptr);
        /* BaseBuf1, CrBuf1, BaseBuf2(dirty & flush failed), BaseBuf3, CrBuf4, BaseBuf4, BaseBuf5*/
        int index = 0;
        for (BlockNumber blockNum = 0; blockNum < 5; blockNum++) {
            PageId pageId = {fileId, blockNum};
            BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, blockNum == 1 ? LW_EXCLUSIVE : LW_SHARED);
            ASSERT_NE(baseBuffer, INVALID_BUFFER_DESC);
            buffers[index++] = baseBuffer;
            if(blockNum == 0 || blockNum == 3) { /* have cr*/
                buffers[index++] = BuildCrBuffer(baseBuffer, nullptr);
                if(blockNum == 3) {
                    bufferPool->UnlockAndRelease(baseBuffer);
                    baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED);
                }
            }
            if(blockNum == 1) {
                bufferPool->MarkDirty(baseBuffer);
            }
            bufferPool->UnlockAndRelease(baseBuffer);
        }
    }

    void PrepareBuffer3(FileId fileId, BufferDesc **buffers, BufferRing ringBuf)
    {
        BufMgrInterface *bufferPool = g_tmpbufPool;
        StorageAssert(bufferPool != nullptr);
        /* BaseBuf1, CrBuf1, BaseBuf2, BaseBuf3, BaseBuf4*/
        int curPos = 0;
        for (BlockNumber blockNum = 0; blockNum < 4; blockNum++) {
            PageId pageId = {fileId, blockNum};
            BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
            buffers[curPos] = baseBuffer;
            CheckRingBuffer(baseBuffer, curPos++, false, ringBuf);
            if(blockNum == 0) { /* have cr */
                BufferDesc *crBuffer = BuildCrBuffer(baseBuffer, ringBuf);
                buffers[curPos] = crBuffer;
                CheckRingBuffer(crBuffer, curPos++, false, ringBuf);
            }
            bufferPool->UnlockAndRelease(baseBuffer);
        }
    }

    void PrepareBuffer4(FileId fileId, BlockNumber blockCount, BufferDesc **buffers, BufferRing ringBuf)
    {
        BufMgrInterface *bufferPool = g_tmpbufPool;
        StorageAssert(bufferPool != nullptr);
        /* 1. ringBuf == nullptr: Base1, Base2, Base3, Base4, Base5, Base6，Base7, Base8, Base9, Base10, Base11, Base12,
         * Cr13, Base13, Base15, Base16, Base17, Base18, Base19
         * 2. ringBuf != nullptr: Base1, Base2, Base3, Base4, Base5, Base6，Base7, Base8, Base9, Base10, Base11, Base12,
         * Cr12
         */
        int curPos = 0;
        int index = 0;
        BufferDesc *baseBuffer = INVALID_BUFFER_DESC;
        for (BlockNumber blockNum = 0; blockNum < blockCount; blockNum++) {
            PageId pageId = {fileId, blockNum};
            if (ringBuf != nullptr && blockNum == blockCount - 1) {
                baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED);
            } else {
                baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
                CheckRingBuffer(baseBuffer, curPos++, false, ringBuf);
            }
            if (ringBuf == nullptr || (ringBuf != nullptr && blockNum != blockCount - 1)) {
                buffers[index++] = baseBuffer;
            }

            if (ringBuf == nullptr && blockNum == 12) { /* have cr buffer */
                BufferDesc *crBuffer = BuildCrBuffer(baseBuffer, ringBuf);
                buffers[index++] = crBuffer;
                CheckRingBuffer(crBuffer, curPos++, false, ringBuf);
                bufferPool->UnlockAndRelease(baseBuffer);
                baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED);
            } else if (ringBuf != nullptr && blockNum == blockCount - 1) {
                BufferDesc *crBuffer = BuildCrBuffer(baseBuffer, ringBuf);
                buffers[index++] = crBuffer;
                CheckRingBuffer(crBuffer, curPos++, false, ringBuf);
            }

            bufferPool->UnlockAndRelease(baseBuffer);
        }
        
        int count = ringBuf == nullptr ? blockCount + 1: blockCount;
        for (BlockNumber i = 0; i < count; i++) {
            PageId pageId = buffers[i]->GetPageId();
            PageId pageId2 = {fileId, i};
            if (ringBuf == nullptr && i >= 13) {
                pageId2 = {fileId, i - 1};  
            }
            StorageAssert(pageId == pageId2);
        }
    }

    void CheckRingBuffer(BufferDesc* buffer, int curPos, bool isRingFull, BufferRing ringBuf)
    {
        if (ringBuf == nullptr) {
            return;
        }
        ASSERT_NE(buffer, INVALID_BUFFER_DESC);
        ASSERT_TRUE(buffer->lruNode.IsInLruList());

        ASSERT_EQ(ringBuf->curPos, curPos);
        ASSERT_EQ(ringBuf->curWasInRing, isRingFull);
    }

    void CheckRingBufferWasNotInRing(BufferDesc *readBuf, BufferDesc *bufInRing, int curPos, bool isRingFull,
                                     BufferRing ringBuf)
    {
        if (ringBuf == nullptr) {
            return;
        }
        ASSERT_NE(readBuf, INVALID_BUFFER_DESC);
        ASSERT_TRUE(readBuf->lruNode.IsInLruList());

        ASSERT_EQ(ringBuf->curPos, curPos);
        ASSERT_EQ(ringBuf->curWasInRing, isRingFull);

        ASSERT_NE(readBuf, ringBuf->bufferDescArray[curPos]);
        ASSERT_EQ(bufInRing, ringBuf->bufferDescArray[curPos]);
    }
    void CheckRingBufferWasInRing(BufferDesc *readBuf, BufferDesc *bufInRing, int curPos, bool isRingFull,
                                  BufferRing ringBuf)
    {
        if (ringBuf == nullptr) {
            return;
        }
        ASSERT_NE(readBuf, INVALID_BUFFER_DESC);
        ASSERT_TRUE(readBuf->lruNode.IsInLruList());

        ASSERT_EQ(ringBuf->curPos, curPos);
        ASSERT_EQ(ringBuf->curWasInRing, isRingFull);
        ASSERT_EQ(readBuf, ringBuf->bufferDescArray[curPos]);
        ASSERT_EQ(bufInRing, ringBuf->bufferDescArray[curPos]);
    }

    void ReadBaseBufMakeFreeFailed(PageId pageId, int curPos, BufferRing ringBuf, bool isRingFull,
                                   BufferDesc *curBufInRing)
    {
        BufMgrInterface *bufferPool = g_tmpbufPool;
        StorageAssert(bufferPool != nullptr);
        BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
        CheckRingBufferWasInRing(baseBuffer, curBufInRing, curPos, isRingFull, ringBuf);
        ASSERT_EQ(ringBuf->bufferDescArray[curPos]->GetPageId(), baseBuffer->GetPageId());
        ASSERT_EQ(ringBuf->bufferDescArray[curPos], baseBuffer);
        bufferPool->UnlockAndRelease(baseBuffer);
    }

    void ReadBaseBufReuseBaseBufFailed(PageId pageId, int curPos, BufferRing ringBuf, bool isRingFull,
                                       BufferDesc *curBufInRing, BufferDesc *preBufInRing)
    {
        BufMgrInterface *bufferPool = g_tmpbufPool;
        StorageAssert(bufferPool != nullptr);
        BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
        CheckRingBufferWasInRing(baseBuffer, curBufInRing, curPos, isRingFull, ringBuf);
        ASSERT_EQ(ringBuf->bufferDescArray[curPos]->GetPageId(), baseBuffer->GetPageId());
        ASSERT_EQ(ringBuf->bufferDescArray[curPos - 1], preBufInRing);
        bufferPool->UnlockAndRelease(baseBuffer);
    }

    void ReadCrBufReuseBaseBufFailed(BufferDesc *baseBuffer, int curPos, BufferRing ringBuf, bool isRingFull,
                                     BufferDesc *curBufInRing, BufferDesc *preBufInRing)
    {
        BufMgrInterface *bufferPool = g_tmpbufPool;
        StorageAssert(bufferPool != nullptr);
        BufferDesc *crBuffer = BuildCrBuffer(baseBuffer, ringBuf);
        CheckRingBufferWasInRing(crBuffer, curBufInRing, curPos, isRingFull, ringBuf);

        ASSERT_EQ(ringBuf->bufferDescArray[curPos]->GetPageId(), crBuffer->GetPageId());
        ASSERT_EQ(ringBuf->bufferDescArray[curPos - 1], preBufInRing);
        bufferPool->UnlockAndRelease(baseBuffer);
    }

    void ReuseBaseBufFaileBufHasInHashTable(PageId pageId, int curPos, BufferRing ringBuf, bool isRingFull,
                                            BufferDesc *curBufInRing)
    {
        BufMgrInterface *bufferPool = g_tmpbufPool;
        StorageAssert(bufferPool != nullptr);
        FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFRING_REUSE_BUF_IN_HASHTABLE, FI_GLOBAL);
        BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
        CheckRingBufferWasNotInRing(baseBuffer, curBufInRing, curPos, isRingFull, ringBuf);
        bufferPool->UnlockAndRelease(baseBuffer);
    }

    void ReuseCrBufFaileBufHasInHashTable(PageId pageId, int curPos, BufferRing ringBuf, bool isRingFull,
                                          BufferDesc *curBufInRing)
    {
        BufMgrInterface *bufferPool = g_tmpbufPool;
        StorageAssert(bufferPool != nullptr);
        FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFRING_REUSE_BUF_IN_HASHTABLE, FI_GLOBAL);
        BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
        CheckRingBufferWasNotInRing(baseBuffer, curBufInRing, curPos, isRingFull, ringBuf);
        bufferPool->UnlockAndRelease(baseBuffer);
    }

    static void WriterPageThread(TestContext *context)
    {
        BufMgr *bufferPool = context->bufferPool;
        std::default_random_engine rand_engine;
        std::uniform_int_distribution<uint32> distribution(0, context->maxBlock - 1);
        
        BufferMgrFakeStorageInstance *instance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();
        for (BlockNumber i = 0; i < context->maxBlock; i++) {
            BlockNumber blockNumber = distribution(rand_engine);
            PageId pageId = {context->fileId, context->ringBuf == nullptr ? blockNumber : i};
            BufferDesc *buffer =
                bufferPool->Read(g_defaultPdbId, pageId, LW_EXCLUSIVE, BufferPoolReadFlag(), context->ringBuf);
            uint32 *page = (uint32 *)(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header));
            *page = *page + 1;
            bufferPool->MarkDirty(buffer);
            bufferPool->UnlockAndRelease(buffer);
        }
        instance->ThreadUnregisterAndExit();
    }

    static void ReadPageThread(TestContext *context)
    {
        BufMgr *bufferPool = context->bufferPool;
        std::default_random_engine rand_engine;
        std::uniform_int_distribution<uint32> distribution(0, context->maxBlock - 1);

        BufferMgrFakeStorageInstance *instance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();
        for (BlockNumber i = 0; i < context->maxBlock; i++) {
            BlockNumber blockNumber = distribution(rand_engine);
            PageId pageId = {context->fileId, context->ringBuf == nullptr ? blockNumber : i};
            BufferDesc *buffer =
                bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), context->ringBuf);
            bufferPool->UnlockAndRelease(buffer);
        }
        instance->ThreadUnregisterAndExit();
    }
};

/* buffer ring */
TEST_F(BufferRingTest, ConstructAndDestructorTest)
{
    BufMgrInterface *bufferPool = g_storageInstance->GetBufferMgr();

    BufferRing ringBuf = CreateBufferRing(BAS_NORMAL);
    ASSERT_EQ(ringBuf, nullptr);

    ringBuf = CreateBufferRing(BAS_BULKREAD);
    ASSERT_NE(ringBuf, nullptr);
    ASSERT_EQ(ringBuf->ringSize, 4);
    for(int i = 0; i < ringBuf->ringSize; i++) {
        ASSERT_EQ(ringBuf->bufferDescArray[i], INVALID_BUFFER_DESC);
    }
    DestroyBufferRing(&ringBuf);
    ASSERT_EQ(ringBuf, nullptr);

    ringBuf = CreateBufferRing(BAS_BULKWRITE);
    ASSERT_NE(ringBuf, nullptr);
    ASSERT_EQ(ringBuf->ringSize, 4);
    for(int i = 0; i < ringBuf->ringSize; i++) {
        ASSERT_EQ(ringBuf->bufferDescArray[i], INVALID_BUFFER_DESC);
    }
    DestroyBufferRing(&ringBuf);
    ASSERT_EQ(ringBuf, nullptr);

    ringBuf = CreateBufferRing(BAS_VACUUM);
    ASSERT_EQ(ringBuf, nullptr);

    ringBuf = CreateBufferRing(BAS_REPAIR);
    ASSERT_EQ(ringBuf, nullptr);
}
TEST_F(BufferRingTest, ReuseBaseBufSucceededTest)
{
    /* Step1: Preparing some reusable pages. */
    const int bufferPoolSize = 10;
    BufMgr *bufferPool = DstoreNew(m_ut_memory_context) BufMgr(bufferPoolSize, 1);
    bufferPool->Init();
    g_tmpbufPool = bufferPool;
    PrepareBuffer(bufferPoolSize, FAKE_FILES[0].file_id);

    /* Step2: Create a buffer ring object of the BAS_BULKREAD type*/
    const int ringSize = 4;
    BufferRing ringBuf;
    CreateBufferRingAndCheck(ringSize, BufferAccessType::BAS_BULKREAD, ringBuf);

    /* Step3: The ring is not full. Use the LRU to reuse the page.
     * The buffer in the bufferpool before reuse is: BaseBuf1, BaseBuf2, BaseBuf3, BaseBuf4, CrBuf3, CrBuf4, BaseBuf5,
     * BaseBuf6, CrBuf5, CrBuf6,...
     * After reuse, the buffer in the ring is: BaseBuf1, CrBuf2, BaseBuf3, CrBuf4
     */
    BlockNumber blockNum = 0;
    int curPos = 0;
    for (;blockNum < ringSize; blockNum += 2) {
        PageId pageId = {FAKE_FILES[1].file_id, blockNum};
        BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
        CheckRingBuffer(baseBuffer, curPos++, false, ringBuf);
        bufferPool->UnlockAndRelease(baseBuffer);

        pageId = {FAKE_FILES[1].file_id, blockNum + 1};
        baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED);
        ASSERT_NE(baseBuffer, INVALID_BUFFER_DESC);
        ASSERT_FALSE(ringBuf->curWasInRing);
        ASSERT_NE(ringBuf->bufferDescArray[ringBuf->curPos], baseBuffer);

        BufferDesc *crBuffer = BuildCrBuffer(baseBuffer, ringBuf);
        CheckRingBuffer(crBuffer, curPos++, false, ringBuf);
        bufferPool->UnlockAndRelease(baseBuffer);
    }

    /* Step4: Reuse the buf in the ring.
     * The buffers in the current ring is: BaseBuf1, CrBuf2, BaseBuf3, CrBuf4
     * After reuse, the buffer in the ring is: BaseBuf1, BaseBuf2, CrBuf1, CrBuf2
     */
    curPos = 0;
    PageId pageId = {FAKE_FILES[1].file_id, ++blockNum};
    BufferDesc *baseBuffer1 = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
    ASSERT_NE(baseBuffer1, INVALID_BUFFER_DESC);
    baseBuffer1->controller->lastPageModifyTime = 0;
    CheckRingBuffer(baseBuffer1, curPos++, true, ringBuf);

    pageId = {FAKE_FILES[1].file_id, ++blockNum};
    BufferDesc *baseBuffer2 = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
    ASSERT_NE(baseBuffer2, INVALID_BUFFER_DESC);
    baseBuffer2->controller->lastPageModifyTime = 0;
    CheckRingBuffer(baseBuffer2, curPos++, true, ringBuf);

    BufferDesc *crBuffer1 = BuildCrBuffer(baseBuffer1, ringBuf);
    CheckRingBuffer(crBuffer1, curPos++, true, ringBuf);
    bufferPool->UnlockAndRelease(baseBuffer1);

    BufferDesc *crBuffer2 = BuildCrBuffer(baseBuffer2, ringBuf);
    CheckRingBuffer(crBuffer2, curPos++, true, ringBuf);
    bufferPool->UnlockAndRelease(baseBuffer2);

    DestroyBufferRing(&ringBuf);
    bufferPool->Destroy();
    delete bufferPool;
}
TEST_F(BufferRingTest, MakeFreeFailedTest_1)
{
    const int bufferPoolSize = 7;
    BufMgr *bufferPool = DstoreNew(m_ut_memory_context) BufMgr(bufferPoolSize, 1);
    bufferPool->Init();
    g_tmpbufPool = bufferPool;
    BufferDesc *buffers[bufferPoolSize] = {nullptr};
    PrepareBuffer2(FAKE_FILES[0].file_id, &buffers[0]);
    
    /* Step2: Create a buffer ring object of the BAS_BULKREAD type*/
    const int ringSize = 6;
    BufferRing ringBuf;
    CreateBufferRingAndCheck(ringSize, BufferAccessType::BAS_BULKREAD, ringBuf);

    /* Step3: Failed to make free:
     * 1st: because the basic page contains the cr page. 
     * 2nd: because the basic page is dirty and flush failed.
     * 3rd: because the cr page make free failed. */
    int curPos = 0;
    BlockNumber blockNum = 0;
    PageId pageId = {FAKE_FILES[1].file_id, blockNum++};
    ReadBaseBufMakeFreeFailed(pageId, curPos, ringBuf, false, buffers[1]);

    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreBufMgrFI::BUFRING_TRY_FLUSH_FAIL, 0, FI_GLOBAL, 0, 1);
    curPos += 2;
    pageId = {FAKE_FILES[1].file_id, blockNum++};
    ReadBaseBufMakeFreeFailed(pageId, curPos, ringBuf, false, buffers[3]);
    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFRING_TRY_FLUSH_FAIL, FI_GLOBAL);

    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFRING_MAKE_CR_FREE_FAIL, FI_GLOBAL);
    pageId = {FAKE_FILES[1].file_id, blockNum++};
    curPos += 3;
    ReadBaseBufMakeFreeFailed(pageId, curPos, ringBuf, false, buffers[6]);
    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFRING_MAKE_CR_FREE_FAIL, FI_GLOBAL);

    DestroyBufferRing(&ringBuf);
    bufferPool->Destroy();
    delete bufferPool;
}
TEST_F(BufferRingTest, MakeFreeFailedTest_2)
{
    /* Step1: Init buffer pool. */
    const int bufferPoolSize = 10;
    BufMgr *bufferPool = DstoreNew(m_ut_memory_context) BufMgr(bufferPoolSize, 1);
    bufferPool->Init();
    g_tmpbufPool = bufferPool;

    /* Step2: Create a buffer ring object of the BAS_BULKREAD type*/
    const int ringSize = 5;
    BufferRing ringBuf;
    CreateBufferRingAndCheck(ringSize, BufferAccessType::BAS_BULKREAD, ringBuf);

    /* Step3: Prepare buffers in ring.  BaseA（Buf1）, CrA（Buf2）, BaseB（Buf3）, BaseC（Buf4）, BaseD（Buf5） */
    BufferDesc *buffers[bufferPoolSize] = {nullptr};
    PrepareBuffer3(FAKE_FILES[0].file_id, &buffers[0], ringBuf);
    

    /* Step4: Failed to make free:
     * 1st: because the basic page contains the cr page. 
     * 2nd: because the cr page make free failed. 
     * 3rd: because the basic page is dirty and flush failed.
    */
    int curPos = 0;
    BlockNumber blockNum = 0;
    PageId pageId = {FAKE_FILES[1].file_id, blockNum++};
    BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED);
    ASSERT_NE(baseBuffer, INVALID_BUFFER_DESC);
    BufferDesc *crBuffer = BuildCrBuffer(baseBuffer, ringBuf);
    CheckRingBufferWasNotInRing(crBuffer, buffers[0], curPos, true, ringBuf);
    ASSERT_EQ(ringBuf->bufferDescArray[curPos + 1], crBuffer); /* reuse the cr buffer in buffer ring. */
    bufferPool->UnlockAndRelease(baseBuffer);

    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreBufMgrFI::BUFRING_MAKE_CR_FREE_FAIL, 0, FI_GLOBAL, 0, 1);
    curPos += 2;
    pageId = {FAKE_FILES[1].file_id, blockNum++};
    ReadBaseBufMakeFreeFailed(pageId, curPos, ringBuf, true, buffers[2]);
    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFRING_MAKE_CR_FREE_FAIL, FI_GLOBAL);

    curPos += 2;
    WriteBuffer(buffers[curPos - 1]->GetPageId());

    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreBufMgrFI::BUFRING_TRY_FLUSH_FAIL, 0, FI_GLOBAL, 0, 1);
    pageId = {FAKE_FILES[1].file_id, blockNum++};
    baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED);
    crBuffer = BuildCrBuffer(baseBuffer, ringBuf);
    CheckRingBufferWasInRing(crBuffer, buffers[curPos], curPos, true, ringBuf);
    bufferPool->UnlockAndRelease(baseBuffer);
    ASSERT_EQ(ringBuf->bufferDescArray[curPos - 1], INVALID_BUFFER_DESC); /* reuse failed, remove from buffer ring. */
    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFRING_TRY_FLUSH_FAIL, FI_GLOBAL);

    DestroyBufferRing(&ringBuf);
    bufferPool->Destroy();
    delete bufferPool;
}

TEST_F(BufferRingTest, ReuseFailedTest_1)
{
    /* Step1: Prepare buffers: BaseA, BaseB, BaseC, BaseD, BaseE, BaseF, BaseG, BaseH, BaseI, BaseJ, BaseK, CrL, BaseL... */
    const int bufferPoolSize = 13;
    BufMgr *bufferPool = DstoreNew(m_ut_memory_context) BufMgr(bufferPoolSize, 1);
    bufferPool->Init();
    g_tmpbufPool = bufferPool;

    const int blockCount = bufferPool->GetBufMgrSize(); 
    BufferDesc *buffers[blockCount] = {nullptr};
    PrepareBuffer4(FAKE_FILES[0].file_id, blockCount - 1, &buffers[0], nullptr);
    

    /* Step2: Create a ringBuf object. */
    const int ringSize = 13;
    BufferRing ringBuf;
    CreateBufferRingAndCheck(ringSize, BufferAccessType::BAS_BULKREAD, ringBuf);

    /* Step3: Failed to reuse: 
     * 1st: Read base buffer1, reuse base buffer2 failed, base buffer2 is pinned agagin.
     * 2nd: Read base buffer1, reuse base buffer2 failed, base buffer2 is maked dirty agagin.
     * 3rd: Read cr buffer buffer1, reuse base buffer2 failed, base buffer2 is pinned agagin.
     * 4th: Read cr buffer buffer1, reuse base buffer2 failed, base buffer2 is maked dirty agagin.
     */
    const int scenarioSize = 4;
    int curPos = ringBuf->curPos;
    BlockNumber blockNum = 0;
    PageId pageId = {FAKE_FILES[1].file_id, blockNum++};
    curPos += 2;
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFRING_BASE_UNABLE_REUSE, FI_GLOBAL);
    ReadBaseBufReuseBaseBufFailed(pageId, curPos, ringBuf, false, buffers[curPos], INVALID_BUFFER_DESC);

    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFRING_REUSE_DIRTY_BUF, FI_GLOBAL);
    pageId = {FAKE_FILES[1].file_id, blockNum++};
    curPos += 2;
    ReadBaseBufReuseBaseBufFailed(pageId, curPos, ringBuf, false, buffers[curPos], INVALID_BUFFER_DESC);

    pageId = {FAKE_FILES[1].file_id, blockNum++};
    curPos += 3;
    BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFRING_BASE_UNABLE_REUSE, FI_GLOBAL);
    ReadCrBufReuseBaseBufFailed(baseBuffer, curPos, ringBuf, false, buffers[curPos], INVALID_BUFFER_DESC);

    pageId = {FAKE_FILES[1].file_id, blockNum++};
    curPos += 3;
    baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFRING_REUSE_DIRTY_BUF, FI_GLOBAL);
    ReadCrBufReuseBaseBufFailed(baseBuffer, curPos, ringBuf, false, buffers[curPos], INVALID_BUFFER_DESC);

    /* Step4: Failed to reuse, because the basic page has been read into the hash table: 
     * 1st: Read Base buffer1, reuse Base buffer2 failed.
     * 2nd: Read Base buffer1, reuse Cr buffer2 failed. 
     */
    curPos += 1;
    pageId = {FAKE_FILES[1].file_id, blockNum++};
    ReuseBaseBufFaileBufHasInHashTable(pageId, curPos, ringBuf, false, INVALID_BUFFER_DESC);

    curPos += 1;
    pageId = {FAKE_FILES[1].file_id, blockNum++};
    ReuseCrBufFaileBufHasInHashTable(pageId, curPos, ringBuf, false, buffers[curPos + 2]);

    DestroyBufferRing(&ringBuf);
    bufferPool->Destroy();
    delete bufferPool;
}

TEST_F(BufferRingTest, ReuseFailedTest_2)
{
    /* Step1: Init buffer pool. */
    const int bufferPoolSize = 20;
    BufMgr *bufferPool = DstoreNew(m_ut_memory_context) BufMgr(bufferPoolSize, 1);
    bufferPool->Init();
    g_tmpbufPool = bufferPool;

    /* Step2: Create a buffer ring object of the BAS_BULKREAD type*/
    const int ringSize = 13;
    BufferRing ringBuf;
    CreateBufferRingAndCheck(ringSize, BufferAccessType::BAS_BULKREAD, ringBuf);

    /* Step3: Prepare buffers in ring. BaseA(Buf1), BaseB(Buf2), BaseC(Buf3), BaseD(Buf4), BaseE(Buf5), BaseF(Buf6),
     * BaseG(Buf7), BaseH(buf8), BaseI(buf9), BaseJ(buf10), BaseK(buf11), BaseL(buf12), CrM(buf13) */
    BufferDesc *buffers[ringSize] = {nullptr};
    PrepareBuffer4(FAKE_FILES[0].file_id, ringSize, &buffers[0], ringBuf);

    /* Step3: Failed to reuse: 
     * 1st: Read base buffer1, reuse base buffer2 failed, base buffer2 is pinned agagin.
     * 2nd: Read base buffer1, reuse base buffer2 failed, base buffer2 is maked dirty agagin.
     * 3rd: Read cr buffer buffer1, reuse base buffer2 failed, base buffer2 is pinned agagin.
     * 4th: Read cr buffer buffer1, reuse base buffer2 failed, base buffer2 is maked dirty agagin.
     */
    int curPos = -1;
    BlockNumber blockNum = 0;
    PageId pageId = {FAKE_FILES[1].file_id, blockNum++};
    curPos += 2;
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFRING_BASE_UNABLE_REUSE, FI_GLOBAL);
    ReadBaseBufReuseBaseBufFailed(pageId, curPos, ringBuf, true, buffers[curPos], buffers[curPos - 1]);

    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFRING_REUSE_DIRTY_BUF, FI_GLOBAL);
    pageId = {FAKE_FILES[1].file_id, blockNum++};
    curPos += 2;
    ReadBaseBufReuseBaseBufFailed(pageId, curPos, ringBuf, true, buffers[curPos], INVALID_BUFFER_DESC);

    pageId = {FAKE_FILES[1].file_id, blockNum++};
    curPos += 3;
    BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFRING_BASE_UNABLE_REUSE, FI_GLOBAL);
    ReadCrBufReuseBaseBufFailed(baseBuffer, curPos, ringBuf, true, buffers[curPos], buffers[curPos - 1]);

    pageId = {FAKE_FILES[1].file_id, blockNum++};
    curPos += 3;
    baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFRING_REUSE_DIRTY_BUF, FI_GLOBAL);
    ReadCrBufReuseBaseBufFailed(baseBuffer, curPos, ringBuf, true, buffers[curPos], INVALID_BUFFER_DESC);

    /* Step4: Failed to reuse, because the basic page has been read into the hash table: 
     * 1st: Read Base buffer1, reuse base buffer2 failed.
     * 2nd: Read Base buffer1, reuse cr buffer2 failed. 
     */
    curPos += 1;
    pageId = {FAKE_FILES[1].file_id, blockNum++};
    ReuseBaseBufFaileBufHasInHashTable(pageId, curPos, ringBuf, true, buffers[curPos]);

    curPos += 1;
    pageId = {FAKE_FILES[1].file_id, blockNum++};
    ReuseCrBufFaileBufHasInHashTable(pageId, curPos, ringBuf, true, buffers[curPos]);

    DestroyBufferRing(&ringBuf);
    bufferPool->Destroy();
    delete bufferPool;
}

TEST_F(BufferRingTest, ReuseFailed_ResetBufferInRing)
{
    /* Step1: Init buffer pool. */
    const int bufferPoolSize = 200;
    BufMgr *bufferPool = DstoreNew(m_ut_memory_context) BufMgr(bufferPoolSize, 1);
    bufferPool->Init();
    g_tmpbufPool = bufferPool;

    /* Step2: Create a buffer ring object of the BAS_BULKREAD type*/
    const int ringSize = 100;
    BufferRing ringBuf;
    CreateBufferRingAndCheck(ringSize, BufferAccessType::BAS_BULKREAD, ringBuf);

    /* Step3: Prepare buffers in ring. */
    BufferDesc *buffers[ringSize] = {nullptr};
    for (BlockNumber blockNum = 0; blockNum < ringSize; blockNum++) {
        PageId pageId = {FAKE_FILES[0].file_id, blockNum};
        BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
        CheckRingBuffer(baseBuffer, blockNum, false, ringBuf);
        buffers[blockNum] = baseBuffer;
        bufferPool->UnlockContent(baseBuffer);
    }

    PageId pageId = {FAKE_FILES[1].file_id, 0};
    BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED, BufferPoolReadFlag(), ringBuf);
    ASSERT_NE(baseBuffer, INVALID_BUFFER_DESC);
    ASSERT_EQ(ringBuf->curPos, ringSize * 0.1 - 1);
    ASSERT_EQ(ringBuf->bufferDescArray[ringBuf->curPos], baseBuffer);
    ASSERT_NE(ringBuf->bufferDescArray[ringBuf->curPos]->GetPageId().m_fileId, FAKE_FILES[0].file_id);
    bufferPool->UnlockAndRelease(baseBuffer);

    for (BlockNumber blockNum = 0; blockNum < ringSize; blockNum++) {
        bufferPool->Release(buffers[blockNum]);
    }

    DestroyBufferRing(&ringBuf);
    bufferPool->Destroy();
    delete bufferPool;
}


TEST_F(BufferRingTest, MultiThreadReadBufferMixingTest)
{
    /* Step1: Init buffer pool. */
    const int bufferPoolSize = FAKE_FILES[0].max_block  + FAKE_FILES[0].max_block / 2;
    BufMgr *bufferPool = DstoreNew(m_ut_memory_context) BufMgr(bufferPoolSize, 1);
    bufferPool->Init();
    g_tmpbufPool = bufferPool;
    g_tmpbufPool = bufferPool;

    /* Step2: Create a buffer ring object of the BAS_BULKREAD type*/
    const int ringSize = 20;
    const uint32 threadSize = 6;
    std::thread threads[threadSize];
    TestContext *context =
        static_cast<TestContext *>(DstoreMemoryContextAllocZero(m_ut_memory_context, threadSize * sizeof(TestContext)));

    for (uint32 i = 0; i < threadSize; i++) {
        TestContext* ctx = &context[i];
        ctx->bufferPool = bufferPool;
        int fileIndex = i < 3 ? 0 : 1;
        ctx->fileId = FAKE_FILES[fileIndex].file_id;
        if(i % 3 == 0) {
            ctx->maxBlock = FAKE_FILES[i].max_block;
            context[i].ringBuf = nullptr;
        } else {
            ctx->maxBlock = ringSize * 2;
            if (i % 3 == 1) {
                CreateBufferRingAndCheck(ringSize, BufferAccessType::BAS_BULKWRITE, context[i].ringBuf);
            } else {
                CreateBufferRingAndCheck(ringSize, BufferAccessType::BAS_BULKREAD, context[i].ringBuf);
            }
        }
    }

    int index = 0;
    threads[index] = std::thread(WriterPageThread,  &context[index]);
    pthread_setname_np(threads[index++].native_handle(), "write_page");

    threads[index] = std::thread(WriterPageThread,  &context[index]);
    pthread_setname_np(threads[index++].native_handle(), "write_page_use_ring_1");

    threads[index] = std::thread(ReadPageThread,  &context[index]);
    pthread_setname_np(threads[index++].native_handle(), "read_page_use_ring_1");

    threads[index] = std::thread(ReadPageThread,  &context[index]);
    pthread_setname_np(threads[index++].native_handle(), "read_page_1");

    threads[index] = std::thread(WriterPageThread,  &context[index]);
    pthread_setname_np(threads[index++].native_handle(), "write_page_use_ring_2");

    threads[index] = std::thread(ReadPageThread,  &context[index]);
    pthread_setname_np(threads[index++].native_handle(), "read_page_use_ring_2");

    for (uint32 i = 0; i < threadSize; i++) {
        threads[i].join();
    }
    for (uint32 i = 0; i < threadSize; i++) {
        if (context[i].ringBuf != nullptr) {
            DestroyBufferRing(&context[i].ringBuf);
        }
    }

    DstorePfreeExt(context);
    bufferPool->Destroy();
    delete bufferPool;
}