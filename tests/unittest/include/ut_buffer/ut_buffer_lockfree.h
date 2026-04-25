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
 * ut_buffer_lockfree.h
 *
 * IDENTIFICATION
 *        dstore/tests/include/ut_buffer/ut_buffer_lockfree.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef UT_BUFFER_LOCKFREE_H
#define UT_BUFFER_LOCKFREE_H
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "buffer/dstore_buf.h"
#include "buffer/dstore_buf_mgr.h"

struct UTBufTask
{
    uint32 bufNum;
    uint32 loopCount;
    uint32 thinkPauses;
    BufferDesc *buffers;
};

class UTBufferLockFree : public DSTORETEST
{
public:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        InstallDatabase(&DSTORETEST::m_guc, m_ut_memory_context);
        BufferMgrFakeStorageInstance *bufMgrInstance = DstoreNew(m_ut_memory_context) BufferMgrFakeStorageInstance();
        bufMgrInstance->Startup(&DSTORETEST::m_guc);
    }
    void TearDown() override
    {
        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->Shutdown();
        delete bufMgrInstance;
        DSTORETEST::TearDown();
    }

    void WaitTaskThrdFinish(std::thread *thrds, uint32 num)
    {
        for (int i = 0; i < num; i++) {
            thrds[i].join();
        }
    }

    UTBufTask * InitTask();
    static void *RunTaskPinUnpinBuffer(void *task);
    static void *RunTaskLockUnlockHeader(void *task);
    static void *RunTaskFastLockHeader(void *task);

private:
    static uint16 m_fileId;
    static PdbId  m_pdbId;
};

 #endif