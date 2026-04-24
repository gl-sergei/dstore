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
