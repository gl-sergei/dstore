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
 * ut_buffer_lockfree.cpp
 *
 * IDENTIFICATION
 *        dstore/tests/src/ut_buffer/ut_buffer_lockfree.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "ut_buffer/ut_buffer_lockfree.h"
#include <thread>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>

DSTORE::PdbId UTBufferLockFree::m_pdbId = 1;
uint16 UTBufferLockFree::m_fileId = 1;

static uint32 GetEnvUint(const char *name, uint32 fallback)
{
    const char *raw = std::getenv(name);
    if (raw == nullptr || raw[0] == '\0') {
        return fallback;
    }
    char *end = nullptr;
    unsigned long v = std::strtoul(raw, &end, 10);
    if (end == raw || v == 0) {
        return fallback;
    }
    return static_cast<uint32>(v);
}

UTBufTask *UTBufferLockFree::InitTask()
{
    UTBufTask *utTask = (UTBufTask *)DstoreMemoryContextAlloc(UTBufferLockFree::m_ut_memory_context, sizeof(UTBufTask));
    utTask->bufNum = 1000;
    utTask->loopCount = 5000;
    utTask->buffers = (BufferDesc *)DstoreMemoryContextAlloc(UTBufferLockFree::m_ut_memory_context, sizeof(BufferDesc) * utTask->bufNum);
    StorageAssert(utTask != nullptr && utTask->buffers != nullptr);
    m_fileId = 1;
    m_pdbId = 1;
    for (uint32 i = 0; i < utTask->bufNum; ++i) {
        utTask->buffers[i].state = 0;
        PageId pageId{ m_fileId, i };
        BufferTag bufTag(m_pdbId, pageId);
        utTask->buffers[i].bufTag = bufTag;
    }
    return utTask;
}

void *UTBufferLockFree::RunTaskPinUnpinBuffer(void *task)
{
    BufferMgrFakeStorageInstance *instance = (BufferMgrFakeStorageInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    UTBufTask *utTask = static_cast<UTBufTask *>(task);

    for (uint32 i = 0; i < utTask->bufNum; ++i) {
        for (uint32 j = 0; j < utTask->loopCount; ++j) {
            utTask->buffers[i].Pin();
            utTask->buffers[i].Unpin();
        }
    }
    instance->ThreadUnregisterAndExit();
    return nullptr;
}

void *UTBufferLockFree::RunTaskLockUnlockHeader(void *task)
{
    BufferMgrFakeStorageInstance *instance = (BufferMgrFakeStorageInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    UTBufTask *utTask = static_cast<UTBufTask *>(task);

     for (uint32 i = 0; i < utTask->bufNum; ++i) {
        for (uint32 j = 0; j < utTask->loopCount; ++j) {
            uint64 state = utTask->buffers[i].LockHdr();
            utTask->buffers[i].PinUnderHdrLocked();
            state |= Buffer::BUF_VALID;
            utTask->buffers[i].UnlockHdr(state);
            utTask->buffers[i].Unpin();
        }
    }
    instance->ThreadUnregisterAndExit();
    return nullptr;
}

void *UTBufferLockFree::RunTaskFastLockHeader(void *task)
{
    BufferMgrFakeStorageInstance *instance = (BufferMgrFakeStorageInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    UTBufTask *utTask = static_cast<UTBufTask *>(task);

    for (uint32 i = 0; i < utTask->bufNum; ++i) {
        for (uint32 j = 0; j < utTask->loopCount; ++j) {
            PageId pageId{m_fileId, i};
            BufferTag bufTag(m_pdbId, pageId);
            if (utTask->buffers[i].FastLockHdrIfReusable(bufTag, false)) {
                utTask->buffers[i].PinUnderHdrLocked();
                uint64 state = utTask->buffers[i].WaitHdrUnlock();
                state |= Buffer::BUF_CONTENT_DIRTY;
                utTask->buffers[i].UnlockHdr(state);
                utTask->buffers[i].Unpin();
            }
        }
    }
    instance->ThreadUnregisterAndExit();
    return nullptr;
}

TEST_F(UTBufferLockFree, TestPinAndUnPinBuffer)
{
    const uint32 WORK_THRD_NUM = 64;
    std::thread workThrd[WORK_THRD_NUM];
    UTBufTask *utTask = InitTask();
    uint32 i = 0;

    for (i = 0; i < WORK_THRD_NUM; ++i) {
        workThrd[i] = std::thread(RunTaskPinUnpinBuffer, static_cast<void *>(utTask));
        pthread_setname_np(workThrd[i].native_handle(), "TestPinAndUnPinBuffer");
    }
    WaitTaskThrdFinish(workThrd, WORK_THRD_NUM);

    for (i = 0; i < utTask->bufNum; ++i) {
        ASSERT_TRUE(utTask->buffers[i].GetRefcount() == 0);
    }
}

TEST_F(UTBufferLockFree, TestLockUnLockHeader)
{
    const uint32 WORK_THRD_NUM = 32;
    std::thread workThrd[WORK_THRD_NUM];
    UTBufTask *utTask = InitTask();
    uint32 i = 0;

    for (i = 0; i < WORK_THRD_NUM; ++i) {
        workThrd[i] = std::thread(RunTaskLockUnlockHeader, static_cast<void *>(utTask));
        pthread_setname_np(workThrd[i].native_handle(), "TestLockUnLockHeader");
    }
    WaitTaskThrdFinish(workThrd, WORK_THRD_NUM);

    for (i = 0; i < utTask->bufNum; ++i) {
        ASSERT_TRUE(utTask->buffers[i].GetRefcount() == 0);
        ASSERT_TRUE(!utTask->buffers[i].IsHdrLocked());
        ASSERT_TRUE(utTask->buffers[i].IsValidPage());
    }
}

TEST_F(UTBufferLockFree, TestMixedPinLockBuffer_TIER1)
{
    const uint32 PIN_WORK_THRD_NUM = 32;
    const uint32 LOCK_WORK_THRD_NUM = 32;
    const uint32 FAST_LOCK_WORK_THRD_NUM = 32;
    const uint32 TOTAL_WORK_THRD_NUM = PIN_WORK_THRD_NUM + LOCK_WORK_THRD_NUM + FAST_LOCK_WORK_THRD_NUM;
    std::thread workThrd[TOTAL_WORK_THRD_NUM];
    UTBufTask *utTask = InitTask();
    uint32 i = 0;

    for (i = 0; i < PIN_WORK_THRD_NUM; ++i) {
        workThrd[i] = std::thread(RunTaskPinUnpinBuffer, static_cast<void *>(utTask));
        pthread_setname_np(workThrd[i].native_handle(), "TestPinAndUnPinBuffer");
    }

    for (i = PIN_WORK_THRD_NUM; i < PIN_WORK_THRD_NUM + LOCK_WORK_THRD_NUM; ++i) {
        workThrd[i] = std::thread(RunTaskLockUnlockHeader, static_cast<void *>(utTask));
        pthread_setname_np(workThrd[i].native_handle(), "TestLockUnLockHeader");
    }

    for (i = PIN_WORK_THRD_NUM + LOCK_WORK_THRD_NUM; i < TOTAL_WORK_THRD_NUM; ++i) {
        workThrd[i] = std::thread(RunTaskFastLockHeader, static_cast<void *>(utTask));
        pthread_setname_np(workThrd[i].native_handle(), "TestFastLockHeader");
    }

    WaitTaskThrdFinish(workThrd, TOTAL_WORK_THRD_NUM);

    for (i = 0; i < utTask->bufNum; ++i) {
        ASSERT_TRUE(utTask->buffers[i].GetRefcount() == 0);
        ASSERT_TRUE(!utTask->buffers[i].IsHdrLocked());
        ASSERT_TRUE(utTask->buffers[i].IsValidPage());
    }
}

/*
 * Hot-buffer pin/unpin microbenchmark.
 *
 * Every worker thread pins and unpins the same single BufferDesc in a tight
 * loop — the pathological case the shared-pin arena is meant to improve.
 *
 * Disabled by default; run explicitly:
 *   UT_BENCH_THREADS=64 UT_BENCH_ITERS=5000000 \
 *     ./dstore_unittest --gtest_also_run_disabled_tests \
 *                       --gtest_filter=*BenchHotPinUnpin*
 */
TEST_F(UTBufferLockFree, DISABLED_BenchHotPinUnpin)
{
    const uint32 workThreads = GetEnvUint("UT_BENCH_THREADS", 64);
    const uint32 iters = GetEnvUint("UT_BENCH_ITERS", 5000000);

    UTBufTask *utTask = (UTBufTask *)DstoreMemoryContextAlloc(
        UTBufferLockFree::m_ut_memory_context, sizeof(UTBufTask));
    utTask->bufNum = 1;
    utTask->loopCount = iters;
    utTask->buffers = (BufferDesc *)DstoreMemoryContextAlloc(
        UTBufferLockFree::m_ut_memory_context, sizeof(BufferDesc));
    StorageAssert(utTask != nullptr && utTask->buffers != nullptr);
    utTask->buffers[0].state = 0;
    PageId pageId{ 1, 0 };
    BufferTag bufTag(1, pageId);
    utTask->buffers[0].bufTag = bufTag;

    std::thread *workThrd = new std::thread[workThreads];
    auto t0 = std::chrono::steady_clock::now();
    for (uint32 i = 0; i < workThreads; ++i) {
        workThrd[i] = std::thread(RunTaskPinUnpinBuffer, static_cast<void *>(utTask));
        pthread_setname_np(workThrd[i].native_handle(), "BenchHotPinUnpin");
    }
    WaitTaskThrdFinish(workThrd, workThreads);
    auto t1 = std::chrono::steady_clock::now();

    double seconds = std::chrono::duration<double>(t1 - t0).count();
    uint64 totalOps = static_cast<uint64>(workThreads) * iters * 2; /* pin + unpin */
    double opsPerSec = totalOps / seconds;
    double nsPerOp = (seconds * 1e9) / static_cast<double>(totalOps);

    std::printf("[BenchHotPinUnpin] threads=%u iters/thread=%u total_ops=%lu "
                "elapsed=%.3fs ops/sec=%.2fM ns/op=%.2f\n",
                workThreads, iters, (unsigned long)totalOps,
                seconds, opsPerSec / 1e6, nsPerOp);
    std::fflush(stdout);

    ASSERT_EQ(utTask->buffers[0].GetRefcount(), 0u);
    delete[] workThrd;
}

/*
 * Btree-lookup-shaped microbenchmark.
 *
 * Simulates crab-walk descent of a 3-level tree: every thread repeatedly
 *
 *   Pin(root)  ->  Pin(L1[c1])  ->  Unpin(root)
 *              ->  Pin(L2[c1,c2])  ->  Unpin(L1[c1])  ->  Unpin(L2[c1,c2])
 *
 * where c1, c2 are thread-local PRNG draws. Root is hammered by every
 * iteration (100% of reads), L1 is diluted by fanoutL1, L2 by
 * fanoutL1 * fanoutL2 — the same heat gradient a real btree sees.
 *
 * Disabled by default; run explicitly:
 *   UT_BENCH_THREADS=64 UT_BENCH_ITERS=1000000 \
 *   UT_BENCH_FANOUT_L1=8 UT_BENCH_FANOUT_L2=16 \
 *     ./unittest --gtest_also_run_disabled_tests \
 *                --gtest_filter=*BenchBtreeLookup*
 */
struct UTBtreeBenchTree
{
    BufferDesc *root;
    BufferDesc *level1;
    BufferDesc *level2;
    uint32 fanoutL1;
    uint32 fanoutL2;
    uint32 itersPerThread;
};

static void *RunTaskBtreeLookup(void *arg)
{
    BufferMgrFakeStorageInstance *instance = (BufferMgrFakeStorageInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    UTBtreeBenchTree *t = static_cast<UTBtreeBenchTree *>(arg);

    std::mt19937 rng(static_cast<uint32>(reinterpret_cast<uintptr_t>(pthread_self())));
    std::uniform_int_distribution<uint32> pickL1(0, t->fanoutL1 - 1);
    std::uniform_int_distribution<uint32> pickL2(0, t->fanoutL2 - 1);

    for (uint32 i = 0; i < t->itersPerThread; ++i) {
        uint32 c1 = pickL1(rng);
        uint32 c2 = pickL2(rng);
        BufferDesc *n1 = &t->level1[c1];
        BufferDesc *n2 = &t->level2[c1 * t->fanoutL2 + c2];

        t->root->Pin();
        n1->Pin();
        t->root->Unpin();

        n2->Pin();
        n1->Unpin();
        n2->Unpin();
    }

    instance->ThreadUnregisterAndExit();
    return nullptr;
}

TEST_F(UTBufferLockFree, DISABLED_BenchBtreeLookup)
{
    const uint32 workThreads = GetEnvUint("UT_BENCH_THREADS", 64);
    const uint32 iters = GetEnvUint("UT_BENCH_ITERS", 1000000);
    const uint32 fanoutL1 = GetEnvUint("UT_BENCH_FANOUT_L1", 8);
    const uint32 fanoutL2 = GetEnvUint("UT_BENCH_FANOUT_L2", 16);

    const uint32 nL1 = fanoutL1;
    const uint32 nL2 = fanoutL1 * fanoutL2;

    UTBtreeBenchTree *tree = (UTBtreeBenchTree *)DstoreMemoryContextAlloc(
        UTBufferLockFree::m_ut_memory_context, sizeof(UTBtreeBenchTree));
    tree->fanoutL1 = fanoutL1;
    tree->fanoutL2 = fanoutL2;
    tree->itersPerThread = iters;
    tree->root = (BufferDesc *)DstoreMemoryContextAlloc(
        UTBufferLockFree::m_ut_memory_context, sizeof(BufferDesc));
    tree->level1 = (BufferDesc *)DstoreMemoryContextAlloc(
        UTBufferLockFree::m_ut_memory_context, sizeof(BufferDesc) * nL1);
    tree->level2 = (BufferDesc *)DstoreMemoryContextAlloc(
        UTBufferLockFree::m_ut_memory_context, sizeof(BufferDesc) * nL2);
    StorageAssert(tree->root != nullptr && tree->level1 != nullptr && tree->level2 != nullptr);

    uint32 pageIdx = 0;
    auto initBuf = [&](BufferDesc *b) {
        b->state = 0;
        PageId pid{ 1, pageIdx++ };
        BufferTag tag(1, pid);
        b->bufTag = tag;
    };
    initBuf(tree->root);
    for (uint32 i = 0; i < nL1; ++i) initBuf(&tree->level1[i]);
    for (uint32 i = 0; i < nL2; ++i) initBuf(&tree->level2[i]);

    std::thread *workThrd = new std::thread[workThreads];
    auto t0 = std::chrono::steady_clock::now();
    for (uint32 i = 0; i < workThreads; ++i) {
        workThrd[i] = std::thread(RunTaskBtreeLookup, static_cast<void *>(tree));
        pthread_setname_np(workThrd[i].native_handle(), "BenchBtreeLookup");
    }
    WaitTaskThrdFinish(workThrd, workThreads);
    auto t1 = std::chrono::steady_clock::now();

    double seconds = std::chrono::duration<double>(t1 - t0).count();
    uint64 opsPerIter = 6; /* 3 pins + 3 unpins */
    uint64 totalOps = static_cast<uint64>(workThreads) * iters * opsPerIter;
    uint64 totalLookups = static_cast<uint64>(workThreads) * iters;
    double opsPerSec = totalOps / seconds;
    double lookupsPerSec = totalLookups / seconds;
    double nsPerOp = (seconds * 1e9) / static_cast<double>(totalOps);

    std::printf("[BenchBtreeLookup] threads=%u iters/thread=%u fanoutL1=%u fanoutL2=%u "
                "lookups=%lu total_ops=%lu elapsed=%.3fs "
                "lookups/sec=%.2fM ops/sec=%.2fM ns/op=%.2f\n",
                workThreads, iters, fanoutL1, fanoutL2,
                (unsigned long)totalLookups, (unsigned long)totalOps, seconds,
                lookupsPerSec / 1e6, opsPerSec / 1e6, nsPerOp);
    std::fflush(stdout);

    /* Drain the arena before reading refcounts. */
    auto drainAssert = [](BufferDesc *b) {
        uint64 hdrState = b->LockHdr();
        uint64 rc = b->GetRefcount();
        b->UnlockHdr(hdrState);
        ASSERT_EQ(rc, 0u);
    };
    drainAssert(tree->root);
    for (uint32 i = 0; i < nL1; ++i) drainAssert(&tree->level1[i]);
    for (uint32 i = 0; i < nL2; ++i) drainAssert(&tree->level2[i]);

    delete[] workThrd;
}
