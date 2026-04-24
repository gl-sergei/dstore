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
#include <gtest/gtest.h>
#include "ut_mock/ut_mock.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "buffer/dstore_buf_table.h"
#include "buffer/dstore_buf_mgr.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_buffer/ut_bufferpool.h"
#include "framework/dstore_vfs_adapter.h"
#include "vfs/vfs_interface.h"
#include <thread>  /* standard thread support */
/* STL containers: used only to facilitate testing */
#include <bitset>    /* bit manipulation */
#include <set>       /* container to store BufBlock pointers */
#include <map>       /* container to map blocknumbers to BufferDesc pointers */
#include <algorithm> /* for shuffling a vector */
#include <random>    /* for shuffling a vector */

using namespace DSTORE;

/*
 * This is a simple way to disable the whole testcase, we just need to define
 * the UT_BPOOL_DISABLE_TESTSUITE macro before the code below.
 */
#if defined UT_BPOOL_DISABLE_TESTSUITE
#define UTBufferPoolTest DISABLE_BufferPoolTest
#endif

/*
 * The following functions are used globally without an object
 * and may be accessed by anyone. This is especially important for the
 * multi-threading testing where we need to call functions outside of the
 * UTBufferPoolTest class.
 */

/*
 * In order to reuse code and to use ASSERT_* and EXPECT_* macros in these
 * utility functions, we need to pass in the calling line of code (LOC).
 * The following is the ASSERT_ wrapper macro that adds the "calling" line of
 * code to the error message.
 *
 * NOTE: this approach has a drawback of using the  __builtin_FILE() and
 * __builtin_LINE() functions that are only available in GCC. It is still
 * preferable to using googletest's SCOPED_TRACE macro that 1) requires changes
 * in the calling function; and 2) is not that easy to read in the unit test
 * output. The developers may still chose to use SCOPED_TRACE macro. In order
 * to use SCOPED_TRACE, we may use the following macro:
 *     #define SCOPED_CALL(function_call) \
 *     { \
 *     char temp[256]; \
 *     snprintf_s(temp, sizeof(temp), sizeof(temp)-1, "Failure in %s", #function_call); \
 *     SCOPED_TRACE(temp); \
 *     function_call; \
 *     } 0
 * and then call "SCOPED_CALL(validate_buffer(buf));" inside TEST and TEST_F
 * functions.
 */
#define ASSERT_WITH_LOC(assert_expr)                                                   \
    {                                                                                  \
        assert_expr << "   Calling line of code: " << sourcefile << ":" << sourceline; \
    }                                                                                  \
    0
#define EXPECT_WITH_LOC(expect_expr)                                                   \
    {                                                                                  \
        expect_expr << "   Calling line of code: " << sourcefile << ":" << sourceline; \
    }                                                                                  \
    0

/*
 * == Main Bufferpool UT class ==
 */

/*
 * Need to declare following const class static const variables here,
 * otherwise the linker couldn't find their definations.
 */
constexpr char UTBufferPoolTest::BPOOL_UPDATED_PREFIX[];
constexpr char UTBufferPoolTest::BPOOL_DEFAULT_FILEPATH[];
const Size UTBufferPoolTest::BPOOL_MULTITHREAD_NUM_OF_THREADS;
const int UTBufferPoolTest::BPOOL_BITSET_SIZE;

pthread_barrier_t UTBufferPoolTest::g_barriers[BPOOL_MAX_NUM_BARRIERS]{};

/*
 * UTBufferPoolTest member functions.
 */

/*
 * Overloaded setup function.
 */
void UTBufferPoolTest::SetUp()
{
    SetDefaultPdbId(PDB_TEMPLATE1_ID);
    DSTORETEST::SetUp();
    UTBufferPoolTest::g_num_barriers = 0;
    /* prepare_buffer_pool() will be called explicitly to set up BufMgr */
    m_bufmgr = nullptr;
    m_bufferpool_size = 0;
    m_num_blocks_on_disk = 0;
    m_num_lru_partitions = 0;

    /*
     * Setup memory context and a create
     * a backend thread.
     */
    InstallDatabase(&DSTORETEST::m_guc, m_ut_memory_context);
    BufferMgrFakeStorageInstance *bufMgrInstance = DstoreNew(m_ut_memory_context)BufferMgrFakeStorageInstance();
    bufMgrInstance->Startup(&DSTORETEST::m_guc);

    m_vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();

    /* Setup virtual file system */
    FileParameter filePara;
    filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    filePara.flag = IN_PLACE_WRITE_FILE;
    filePara.fileSubType = DATA_FILE_TYPE;
    filePara.rangeSize = (64 << 10);        /* 64KB */
    filePara.maxSize = (uint64) DSTORE_MAX_BLOCK_NUMBER * BLCKSZ;
    filePara.recycleTtl = 0;
    filePara.mode = FILE_READ_AND_WRITE_MODE;
    filePara.isReplayWrite = false;
    int rc = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, UT_DEFAULT_STORESPACE_NAME);
    storage_securec_check(rc, "\0", "\0");

    rc = m_vfs->CreateFile(m_fileId, m_filepath, filePara);
    ASSERT_EQ(rc, 0);
}

/*
 * Overloaded cleanup function.
 */
void UTBufferPoolTest::TearDown()
{
    /*
     * Perform cleanup:
     *   1) delete bufmgr;
     *   2) remove file and exit VFS;
     *   3) deregister backend thread;
     *   4) delete g_storageInstance
     *   5) free memory context
     */
    if (m_bufmgr != nullptr) {
        /* Memory allocated by BufMgr has to be freed explicitly */
        m_bufmgr->Destroy();
        delete m_bufmgr;
        m_bufmgr = nullptr;
    }

    /* Remove file used to store bufferpool on disk */
    m_vfs->RemoveFile(m_fileId, m_filepath);

    BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
    bufMgrInstance->Shutdown();
    delete bufMgrInstance;
    DSTORETEST::TearDown();
}

/*
 * Prepares bufferpool for each test.
 * Each test case can specify how large it requires the bufferpool to be and
 * how many blocks on disk should be created.
 * Each page gets a validation token stored inside which allows to validate
 * these pages later. This token is based on file ID and block number and
 * uniquely identifies the block/buffer.
 */
void UTBufferPoolTest::prepare_buffer_pool(Size bufferpool_size, Size num_blocks_on_disk, Size num_LRU_partitions)
{
    m_bufmgr = DstoreNew(UTBufferPoolTest::m_ut_memory_context) BufMgr(bufferpool_size, num_LRU_partitions);
    ASSERT_NE(m_bufmgr, nullptr);
    m_bufmgr->Init();
    PrepareBlocksOnDisk(num_blocks_on_disk);

    /* Store parameters inside the test class object. */
    m_bufferpool_size = bufferpool_size;
    m_num_blocks_on_disk = num_blocks_on_disk;
    m_num_lru_partitions = num_LRU_partitions;
}

/*
 * Prepares disk for each test.
 * Each test case can specify how many blocks on disk should be created.
 * Each page gets a validation token stored inside which allows to validate
 * these pages later. This token is based on file ID and block number and
 * uniquely identifies the block/buffer.
 */
void UTBufferPoolTest::PrepareBlocksOnDisk(Size numBlocksOnDisk)
{
    int rc = 0;
    rc = m_vfs->Extend(m_fileId, GetOffsetByBlockNo(numBlocksOnDisk));
    ASSERT_EQ(rc, 0);
    /*
     * Write numBlocksOnDisk number of blocks to the virtual file system
     * Each entry is appended with a number in the iteration.
     * The loop then adds the values to the vector and writes it to
     * the file on disk.
     */
    char write_block[BLCKSZ];
    for (BlockNumber i = 0; i < numBlocksOnDisk; i++) {
        /* Make sure page is initialized as empty first */
        errno_t rc = memset_s(write_block, BLCKSZ, 0, BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        rc = memset_s(write_block, sizeof(Page::PageHeader), 1, sizeof(Page::PageHeader));
        storage_securec_check(rc, "\0", "\0");
        Page* page = reinterpret_cast<Page*>(&write_block[0]);
        page->Init(0, PageType::HEAP_PAGE_TYPE, {m_fileId, i});
        std::string temp = generate_token(m_fileId, i);
        strncpy(write_block + sizeof(Page::PageHeader), temp.c_str(), temp.size());
        page->SetChecksum();
        rc = m_vfs->WritePageSync({m_fileId, i}, write_block);
        ASSERT_EQ(rc, 0);
        rc = m_vfs->Fsync(m_fileId);
        ASSERT_EQ(rc, 0);
    }
}

/*
 * Compares data in the bufferpool with the data on disk.
 * Given a bufferpool descriptor, the function checks if the corresponding
 * data is the same on disk.
 */
void UTBufferPoolTest::compare_buffer_with_disk(BufferDesc *buf, const char *sourcefile, int sourceline)
{
    /* Use ASSERT_WITH_LOC to make sure the calling line of code is shown on error */
    ASSERT_WITH_LOC(ASSERT_NE(buf, nullptr));
    PageId page = buf->bufTag.pageId;
    char read_block[BLCKSZ];
    int rc = 0;
    rc = m_vfs->ReadPageSync(page, read_block);
    BufBlock block = (BufBlock)buf->GetPage();
    ASSERT_WITH_LOC(ASSERT_EQ(rc, 0));
    /* Verify that the value read on disk and bufferpool are the same. */
    ASSERT_WITH_LOC(ASSERT_EQ(0, strncmp(read_block, block, strlen(block))));
}

/*
 * Validates buffer using the fact that fileId and blockNumber of the page are
 * stored inside the block on disk.
 */
void UTBufferPoolTest::validate_buffer(BufferDesc *buf, bool is_updated, const char *sourcefile, int sourceline)
{
    /* Use ASSERT_WITH_LOC to make sure the calling line of code is shown on error */
    ASSERT_WITH_LOC(ASSERT_NE(buf, nullptr));
    PageId page = buf->bufTag.pageId;
    std::string temp = generate_token(page.m_fileId, page.m_blockId, is_updated ? BPOOL_UPDATED_PREFIX : nullptr);
    BufBlock block = (BufBlock)buf->GetPage();
    /* Verify that the data inside the page is the expected text based on buffer's PageId. */
    ASSERT_WITH_LOC(ASSERT_EQ(0, strncmp(temp.c_str(), block + sizeof(Page::PageHeader), temp.size())));
}

/*
 * Updates page stored in a BufferDesc in such a way that we may validate it
 * later.
 */
void UTBufferPoolTest::update_page(BufferDesc *buf, const char *sourcefile, int sourceline)
{
    /* Use ASSERT_WITH_LOC to make sure the calling line of code is shown on error */
    ASSERT_WITH_LOC(ASSERT_NE(buf, nullptr));
    PageId page = buf->bufTag.pageId;
    std::string temp = generate_token(page.m_fileId, page.m_blockId, BPOOL_UPDATED_PREFIX);
    BufBlock block = (BufBlock)buf->GetPage();
    errno_t rc = memset_s(block, BLCKSZ, 0, BLCKSZ);
    storage_securec_check(rc, "\0", "\0");
    strncpy(block + sizeof(Page::PageHeader), temp.c_str(), temp.size());
}

/*
 * When should_flags_be_in_state is true, validate that the given flags are
 * set inside the BufferDesc's state.
 * Otherwise, validate if the given flags are NOT in the BufferDesc's state.
 */
void UTBufferPoolTest::check_buffer_state(BufferDesc *buf, uint64 flags, bool should_flags_be_in_state,
                                          const char *sourcefile, int sourceline)
{
    /* Use ASSERT_WITH_LOC to make sure the calling line of code is shown on error */
    ASSERT_WITH_LOC(ASSERT_NE(buf, nullptr));

    uint64 state = GsAtomicReadU64(&buf->state);
    if (should_flags_be_in_state) {
        ASSERT_WITH_LOC(ASSERT_EQ((state & flags), flags));
    } else {
        ASSERT_WITH_LOC(ASSERT_NE((state & flags), flags));
    }
}

/* Generate token from file and block IDs */
inline std::string UTBufferPoolTest::generate_token(const int fileId, const BlockNumber block_number,
                                                    const char *prefix)
{
    std::ostringstream token;
    if (prefix != nullptr) {
        token << prefix;
    }
    /*
     * Note that '.' at the end ensures that strncmp will never give us false
     * positives.
     */
    token << "fileID=" << fileId << ";pageID=" << block_number << ".";
    /* Make sure the offset used to store extra data on page is never exceeded */
    StorageAssert(token.str().size() < BPOOL_PAGE_STRING_END_OFFSET);
    return token.str();
}

/*
 * This utility function will wait for the threads to finish.
 * It takes a list of standard threads. This function is a multi-purpose
 * function and can serve for any multi-threaded function.
 */
void UTBufferPoolTest::wait_for_threads_to_finish(std::thread *threads, Size num_threads)
{
    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }
}

/*
 * Barriers should be re-initialized for every test case that requires them.
 */
void UTBufferPoolTest::init_global_barriers(Size num_threads, Size num_barriers)
{
    /* num_barriers may not exceed our (arbitrary chosen) max value */
    ASSERT_TRUE(num_barriers <= BPOOL_MAX_NUM_BARRIERS);
    /* Sanity checks */
    ASSERT_FALSE(num_barriers == 0);
    ASSERT_FALSE(num_threads == 0);

    g_num_barriers = num_barriers;
    for (int i = 0; i < num_barriers; i++) {
        pthread_barrier_init(&UTBufferPoolTest::g_barriers[i], nullptr, num_threads);
    }
}

/*
 * Performs barrier cleanup after test is finished.
 */
void UTBufferPoolTest::destroy_global_barriers()
{
    ASSERT_TRUE(g_num_barriers != 0);
    for (int i = 0; i < g_num_barriers; i++) {
        pthread_barrier_destroy(&g_barriers[i]);
    }
    g_num_barriers = 0;
}

/*
 * Returns true if the block is in the CANDIDATE list,
 * and returns false otherwise.
 */
bool UTBufferPoolTest::is_in_candidate_list(BufferDesc *buf)
{
    return buf->lruNode.IsInCandidateList();
}

/*
 * Returns true if the block is in the LRU list,
 * and returns false otherwise.
 */
bool UTBufferPoolTest::is_in_lru_list(BufferDesc *buf)
{
    return buf->lruNode.IsInLruList();
}

/*
 * Returns true if the block is in the HOT list,
 * and returns false otherwise.
 */
bool UTBufferPoolTest::is_in_hot_list(BufferDesc *buf)
{
    return buf->lruNode.IsInHotList();
}

/*
 * == SINGLE-THREADED UNIT TESTS ==
 */

/*
 * Goal: test reading one block from disk into bufferpool without eviction.
 * This is a single read version of ReadMultipleBlocks.
 *
 * 1) Setup bufferpool with BPOOL_DEFAULT_BUFFERPOOL_SIZE
 *    blocks in memory and 1 blocks on disk;
 * 2) Read one page from disk to fill out the bufferpool.
 * 3) Read the one page directly from disk and validate that.
 *
 * Test coverage: BufMgr::read().
 */
TEST_F(UTBufferPoolTest, ReadSingleBlock)
{
    prepare_buffer_pool(BPOOL_DEFAULT_BUFFERPOOL_SIZE, 1, BPOOL_DEFAULT_LRU_PARTITIONS);
    BlockNumber blocknum = 0;
    BufferDesc *buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
    ASSERT_NE(buf, nullptr);
    /* Compare buffer in memory with data on disk directly */
    compare_buffer_with_disk(buf);
    /* Validate that buffer contains formatted PageId */
    validate_buffer(buf);
    m_bufmgr->UnlockAndRelease(buf);
}

/*
 * Goal: test reading multiple blocks from disk into bufferpool without eviction.
 *
 * 1) Setup bufferpool with BPOOL_DEFAULT_BUFFERPOOL_SIZE
 *    blocks in memory and BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK
 *    blocks on disk;
 * 2) Read BPOOL_DEFAULT_BUFFERPOOL_SIZE pages
 *    from disk to fill out the bufferpool.
 * 3) Read these BPOOL_DEFAULT_BUFFERPOOL_SIZE
 *    pages directly from disk and validate that.
 *
 * Test coverage: BufMgr::read().
 */
TEST_F(UTBufferPoolTest, ReadMultipleBlocks_TIER1)
{
    prepare_buffer_pool();
    BufferDesc *buf = nullptr;
    /*
     * Go through BPOOL_DEFAULT_BUFFERPOOL_SIZE buffers, enough to fill the bufferpool.
     * Check that the value read from the bufferpool is the same
     * that is on disk.
     */
    for (BlockNumber blocknum = 0; blocknum < m_bufferpool_size; blocknum++) {
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Compare buffer in memory with data on disk directly */
        compare_buffer_with_disk(buf);
        /* Validate that buffer contains formatted PageId */
        validate_buffer(buf);
        m_bufmgr->UnlockAndRelease(buf);
    }
}

/*
 * Goal: test pin, unpin, refcount, lock, unlock and release
 *
 * 1) Setup bufferpool with BPOOL_DEFAULT_BUFFERPOOL_SIZE
 *    blocks in memory and 1 blocks on disk;
 * 2) Read one page from disk to fill out the bufferpool.
 * 3) Assert lock is held and ref count is 1.
 * 4) Unlock and check lock is not held.
 * 5) Release and check ref count is back to 0.
 * 6) Test pin/unpin.
 * 7) Test calling lock and unlock directly on the
 *    content lock and verify it for both LW_SHARED and LW_EXCLUSIVE.
 *
 * Test coverage: BufferDesc::Pin(), BufferDesc::Unpin(), BufMgr::lock()
 *                BufMgr::unlock(), BufMgr::release(),
 *                BufferDesc::GetRefcount()
 */
TEST_F(UTBufferPoolTest, LockPinReleaseRefcount_TIER1)
{
    prepare_buffer_pool(BPOOL_DEFAULT_BUFFERPOOL_SIZE, 1, BPOOL_DEFAULT_LRU_PARTITIONS);
    BlockNumber blockNumber = 0;
    /* Ask for an exclusive lock */
    BufferDesc *buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blockNumber}, LW_EXCLUSIVE);
    ASSERT_NE(buf, nullptr);
    ASSERT_NE(buf, INVALID_BUFFER_DESC);
    /* Assert that lock is held */
    ASSERT_TRUE(LWLockHeldByMeInMode(&buf->contentLwLock, LW_EXCLUSIVE));
    /*
     * Since we are reading and we haven't released the lock
     * or unpined the page, the ref count should be one.
     * */
    ASSERT_EQ(buf->GetRefcount(), 1U);
    m_bufmgr->MarkDirty(buf);
    /*
     * Release the lock.
     */
    m_bufmgr->UnlockContent(buf);
    ASSERT_TRUE(!LWLockHeldByMeInMode(&buf->contentLwLock, LW_EXCLUSIVE));
    /*
     * Unpin the buffer, this should reduce the refcount to zero.
     */
    m_bufmgr->Release(buf);
    ASSERT_EQ(buf->GetRefcount(), 0);
    /*
     * Now let's try with just pin.
     */
    buf->Pin();
    /*
     * Pinning a page should incerement ref count by 1.
     * A pinned buffer is a buffer that has a ref count
     * greater than zero. It is not a specific flag.
     */
    ASSERT_EQ(buf->GetRefcount(), 1U);
    /*
     * We can also check that the bufferpool see's the
     * buffer as pinned. It checks its ref count in a
     * thread secure manner.
     */
    ASSERT_EQ(true, buf->IsPinnedPrivately());

    /*
     * We can explicitly lock the buffer.
     * We try both LW_SHARED and LW_EXCLUSIVE.
     */
    m_bufmgr->LockContent(buf, LW_SHARED);
    ASSERT_TRUE(LWLockHeldByMeInMode(&buf->contentLwLock, LW_SHARED));
    ASSERT_TRUE(!LWLockHeldByMeInMode(&buf->contentLwLock, LW_EXCLUSIVE));
    m_bufmgr->UnlockContent(buf);
    ASSERT_TRUE(!LWLockHeldByMeInMode(&buf->contentLwLock, LW_SHARED));

    /* LW_EXCLUSIVE */
    m_bufmgr->LockContent(buf, LW_EXCLUSIVE);
    ASSERT_TRUE(LWLockHeldByMeInMode(&buf->contentLwLock, LW_EXCLUSIVE));
    ASSERT_TRUE(!LWLockHeldByMeInMode(&buf->contentLwLock, LW_SHARED));
    m_bufmgr->UnlockContent(buf);
    ASSERT_TRUE(!LWLockHeldByMeInMode(&buf->contentLwLock, LW_EXCLUSIVE));

    /*
     * Unpinning the buffer should set ref count back to zero.
     */
    buf->Unpin();
    ASSERT_EQ(false, buf->IsPinnedPrivately());
    ASSERT_EQ(buf->GetRefcount(), 0);
}

/*
 * Goal: test LRU moving blocks to HOT list functionality.
 *
 * 1) create a bufferpool of 10 blocks
 * 2) read all 10 blocks once and make sure all blocks are in LRU list
 * 3) access half of the blocks (blocknum 6-10) multiple times to push those
 *    blocks onto the HOT list.
 * 4) read blocks 6 - 10 once more to check whether the blocks are pushed into
 *    the hot list.
 *
 * Test coverage: LruNode::is_in_lru_list, LruNode::is_in_hot_list
 */
TEST_F(UTBufferPoolTest, LRULists)
{
    /*
     * start_hot_pages_in_bufferpool is the start of the block number in which
     * we will try to get the block and the blocks after into the HOT list. For
     * example, if start_hot_pages_in_bufferpool = 25, and
     * num_slots_in_bufferpool = 30, then we would be putting blocks 26-30 into
     * the HOT list.
     * Here, we divide by 3 because we want the bufferpool to have a size of 10.
     */
    Size num_slots_in_bufferpool = BPOOL_DEFAULT_BUFFERPOOL_SIZE;

    /* We'll make half of the buffers HOT */
    Size start_hot_pages_blocknum = num_slots_in_bufferpool / 2;

    /*
     * It requires BPOOL_LRU_MAX_USAGE (5) reads to move a block from LRU
     * list into HOT list.
     */
    Size num_accesses_to_become_hot = BPOOL_LRU_MAX_USAGE;
    /* Setup the buffer pool and prepare blocks on disk */
    prepare_buffer_pool(num_slots_in_bufferpool, BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK);

    BufferDesc *buf = nullptr;
    /*
     * Read all the pages once to check whether the pages
     * are in the LRU list.
     */
    BufferDesc *buffer_desc[num_slots_in_bufferpool];
    for (BlockNumber blocknum = 0; blocknum < num_slots_in_bufferpool; blocknum++) {
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        ASSERT_TRUE(is_in_lru_list(buf));
        m_bufmgr->UnlockContent(buf);
        buffer_desc[blocknum] = buf;
        /* we can not release buffer here to avoid buffer evicted by another page. */
    }

    /* after finish loading page into buffer, we release all the buffer */
    for (BlockNumber blocknum = 0; blocknum < num_slots_in_bufferpool; blocknum++) {
        m_bufmgr->Release(buffer_desc[blocknum]);
    }

    /*
     * Access blocks we want in the HOT list multiple times so the pages get
     * moved to the HOT list.
     * Here we subtract 2 from the number of accesses because each block is
     * accessed once before and after this nested for loop.
     */
    for (int accesses = 0; accesses < num_accesses_to_become_hot - 2; accesses++) {
        for (BlockNumber blocknum = start_hot_pages_blocknum; blocknum < num_slots_in_bufferpool; blocknum++) {
            buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
            ASSERT_NE(buf, nullptr);
            m_bufmgr->UnlockContent(buf);
            m_bufmgr->Release(buf);
        }
    }

    /*
     * Reads all pages one more time to check if the HOT list pages actually
     * got moved to the HOT list, and the blocks we did not want in the HOT
     * list remained in the LRU list.
     */
    for (BlockNumber blocknum = 0; blocknum < num_slots_in_bufferpool; blocknum++) {
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);

        if (blocknum < start_hot_pages_blocknum) {
            ASSERT_TRUE(is_in_lru_list(buf));
            ASSERT_FALSE(is_in_hot_list(buf));
        } else {
            ASSERT_TRUE(is_in_hot_list(buf));
            ASSERT_FALSE(is_in_lru_list(buf));
        }
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }
}

/*
 * Goal: perform the same tests as in LRULists but on the pre-defined
 * scattered set of blocks on disk. Main benefit is an example on how to
 * traverse the bufferpool table without repeatedly reading blocks in.
 *
 * IMPORTANT: this test shows how to achieve 2 goals:
 *   1) scan bufferpool table without reading all pages and dealing with
 *      eviction, etc.
 *   2) validate scenarios (such as invalidate()) where test needs to access
 *      the specific slot in bufferpool based on its BlockNumber. That's why
 *      std::map is used instead of std::vector.
 * One example of 2) is the case when we want to make sure that specific block
 * has been evicted and is no longer in the bufferpool table.
 *
 * Note that using map is not necessary here (since array of the BufferDesc
 * pointers would work just as well) but using map may come handy for more
 * complex scenarios.
 *
 * Testing algorithm:
 * 1) Create bufferpool with 10 slots.
 * 2) Read all blocks in the bufferpool and save the buffer desc pointers.
 * 3) Decide which blocks will become HOT.
 * 4) Read each "hot" block BPOOL_LRU_MAX_USAGE times.
 * 5) Scan all bufferpool slots to validate that chosen pages are HOT and
 *    others are not.
 *
 * Test coverage: LruNode::is_in_lru_list, LruNode::is_in_hot_list
 */
TEST_F(UTBufferPoolTest, LRUListsScattered)
{
    /* This test needs to use exactly 10 slots */
    const Size num_slots_in_bufferpool = 10;
    std::set<BlockNumber> scattered_blocks{1, 3, 5, 7, 9};

    /*
     * It requires BPOOL_LRU_MAX_USAGE (5) reads to move a block from LRU
     * list into HOT list.
     */
    Size num_accesses_to_become_hot = BPOOL_LRU_MAX_USAGE;
    /* Setup the buffer pool and prepare blocks on disk */
    prepare_buffer_pool(num_slots_in_bufferpool * 2, BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK);

    /* */
    std::map<BlockNumber, BufferDesc *> map_blocknum_to_buffer_desc;

    BufferDesc *buf = nullptr;
    /*
     * Read all the pages once to check whether the pages
     * are in the LRU list.
     */
    for (BlockNumber blocknum = 0; blocknum < num_slots_in_bufferpool; blocknum++) {
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        ASSERT_TRUE(is_in_lru_list(buf));
        /* Insert each buffer into the map */
        map_blocknum_to_buffer_desc[blocknum] = buf;
        m_bufmgr->UnlockContent(buf);
    }

    /* after finish loading page into buffer, we release all the buffer */
    for (BlockNumber blocknum = 0; blocknum < num_slots_in_bufferpool; blocknum++) {
        m_bufmgr->Release(map_blocknum_to_buffer_desc[blocknum]);
    }

    /*
     * Access blocks we want in the HOT list multiple times so the pages get
     * moved to the HOT list.
     * Here we subtract 1 from the number of accesses because each block was
     * accessed once before this for loop.
     */
    for (int accesses = 0; accesses < num_accesses_to_become_hot - 1; accesses++) {
        for (auto blocknum : scattered_blocks) {
            buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
            ASSERT_NE(buf, nullptr);
            m_bufmgr->UnlockContent(buf);
            m_bufmgr->Release(buf);
        }
    }

    /*
     * Scan all buffers in the bufferpool using the map we've created.
     * This allows us to access the bufferpool without reading pages in.
     * Note: the access doesn't require lock since we are running single
     * threaded.
     */
    for (auto buf_slot : map_blocknum_to_buffer_desc) {
        BlockNumber block_num = buf_slot.first;
        buf = map_blocknum_to_buffer_desc[block_num];
        if (scattered_blocks.find(block_num) != scattered_blocks.end()) {
            ASSERT_TRUE(is_in_hot_list(buf));
            ASSERT_FALSE(is_in_lru_list(buf));
        } else {
            ASSERT_TRUE(is_in_lru_list(buf));
            ASSERT_FALSE(is_in_hot_list(buf));
        }
    }
}

/*
 * Goal: validate assumptions made in this tests and detect if they changed in
 * the actual BufMgr implementation.
 *
 * Assumptions to be validated:
 *   1) Buffer becomes hot after it has been read for BPOOL_LRU_MAX_USAGE times;
 *   2) There are bufferpool_size * BUFLRU_DEFAULT_HOT_RATIO HOT pages in the
 *      bufferpool.
 *
 * Testing algorithm:
 * 1) Create bufferpool. Make sure its size is multiple of 10.
 * 2) Read each block BPOOL_LRU_MAX_USAGE times.
 * 3) Check that buffer becomes hot only after BPOOL_LRU_MAX_USAGE times.
 * 4) Read all blocks in again and count how many blocks are HOT, how many are
 *    LRU (cold), and how many are in the candidate list.
 * 4) Validate that we have the expected number of hot and cold pages. And we
 *    have 0 candidates.
 *
 * Test coverage: LruNode::is_in_lru_list, LruNode::is_in_hot_list
 */
TEST_F(UTBufferPoolTest, ValidateHotAssumptions_TIER1)
{
    /*
     * it requires BPOOL_LRU_MAX_USAGE (5) reads to move a block from LRU list into HOT list.
     */
    Size num_accesses_to_become_hot = BPOOL_LRU_MAX_USAGE;

    /* Create large buffer pool with a single LRU partition */
    prepare_buffer_pool(BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK, BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK, 1);
    BufferDesc *buf = nullptr;

    /* Make sure bufferpool size is a multiple of 10 */
    ASSERT_TRUE(m_bufferpool_size % 10 == 0);

    /*
     * Use std::set to store BufBlock pointers. Note that there is no
     * real difference to storing BufferDesc pointers.
     */
    std::set<BufBlock> buf_set;

    /*
     * Access blocks we want in the HOT list multiple times so the pages get
     * moved to the HOT list.
     * Here we subtract 2 from the # of accesses because each block is accessed
     * once before and after this nested for loop.
     */
    for (BlockNumber blocknum = 0; blocknum < m_bufferpool_size; blocknum++) {
        for (int accesses = 0; accesses < num_accesses_to_become_hot; accesses++) {
            buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
            ASSERT_NE(buf, nullptr);
            /* Save the pointer to the data */
            buf_set.insert((BufBlock)buf->GetPage());
            /*
             * Until page is accessed less than (num_accesses_to_become_hot-1)
             * times, it should stay cold.
             */
            if (accesses < num_accesses_to_become_hot - 1) {
                ASSERT_TRUE(is_in_lru_list(buf));
            } else {
                ASSERT_TRUE(is_in_hot_list(buf));
            }
            m_bufmgr->UnlockContent(buf);
            m_bufmgr->Release(buf);
        }
    }

    /*
     * Reads all pages one more time to count that we have exactly the expected
     * number of buffers in the hot, cold (lru), and candidate lists.
     */
    int expected_hot_buffers = m_bufferpool_size * BUFLRU_DEFAULT_HOT_RATIO;
    int expected_lru_buffers = m_bufferpool_size - expected_hot_buffers;
    int expected_candidate_buffers = 0;
    int count_lru_buffers = 0;
    int count_hot_buffers = 0;
    int count_candidate_buffers = 0;
    for (BlockNumber blocknum = 0; blocknum < m_bufferpool_size; blocknum++) {
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Validate that we've seen this data before */
        ASSERT_NE(buf_set.find((BufBlock)buf->GetPage()), buf_set.end());
        if (is_in_lru_list(buf)) {
            count_lru_buffers++;
        } else if (is_in_hot_list(buf)) {
            count_hot_buffers++;
        } else {
            count_candidate_buffers++;
        }
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }
    ASSERT_EQ(count_lru_buffers, expected_lru_buffers);
    ASSERT_EQ(count_hot_buffers, expected_hot_buffers);
    ASSERT_EQ(count_candidate_buffers, expected_candidate_buffers);
}

/*
 * Goal: test updating and flushing the bufferpool buffer.
 *
 * 1) Setup bufferpool with BPOOL_DEFAULT_BUFFERPOOL_SIZE
 *    blocks in memory and BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK
 *    blocks on disk;
 * 2) Read BPOOL_DEFAULT_BUFFERPOOL_SIZE pages
 *    from disk to fill out the bufferpool.
 * 3) Update the page in a way that may be validated later;
 * 4) Mark buffer dirty;
 * 5) Check if that page is marked dirty by checking BUF_DIRTY bit;
 * 6) Flush the buffer to disk;
 * 7) Check that buffer is not marked dirty after flushing;
 * 8) Read again (same as step 2) the same pages. Since pages are in the
 *    bufferpool already, they will be pinned and locked only.
 * 9) Validate that buffers in memory contain updated data.
 * 10) Validate that updates have been persisted on disk by comparing data on
 *    disk with the data in the memory (which are updated as verified by the
 *    previous step).
 *
 * Test coverage: BufMgr::flush(), BufMgr::mark_dirty().
 */
TEST_F(UTBufferPoolTest, UpdateAndFlushBuffer_TIER1)
{
    prepare_buffer_pool();
    BufferDesc *buf = nullptr;

    /* Read check */
    for (BlockNumber blocknum = 0; blocknum < m_bufferpool_size; blocknum++) {
        /* Read in pages */
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_EXCLUSIVE);
        ASSERT_NE(buf, nullptr);
        /* Check that buffer is NOT marked dirty */
        check_buffer_state(buf, Buffer::BUF_CONTENT_DIRTY, false);

        /* Update the pages */
        update_page(buf);

        m_bufmgr->MarkDirty(buf);
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
        /* Check that buffer is marked dirty */
        check_buffer_state(buf, Buffer::BUF_CONTENT_DIRTY, true);
    }

    /* Flush buffers to disk */
    for (BlockNumber blocknum = 0; blocknum < m_bufferpool_size; blocknum++) {
        /* Lock and pin page that is already in the bufferpool */
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);

        /* Validate that buffer contains updated data */
        validate_buffer(buf, BPOOL_PAGE_UPDATED);
        m_bufmgr->UnlockAndRelease(buf);

        BufferTag bufTag = {g_defaultPdbId, {m_fileId, blocknum}};
        ASSERT_TRUE(STORAGE_FUNC_SUCC(m_bufmgr->Flush(bufTag)));
        /*
         * Check that buffer is NOT marked dirty after the flush().
         * BUF_DIRTY bit will be cleared by terminate_io() inside flush().
         * BUF_JUST_DIRTIED will be cleared explicitly by flush().
         */
        check_buffer_state(buf, (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY), false);
    }

    /* Validate that updates have been persisted to disk */
    for (BlockNumber blocknum = 0; blocknum < m_bufferpool_size; blocknum++) {
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Validate that disk contains the updated version */
        compare_buffer_with_disk(buf);
        m_bufmgr->UnlockAndRelease(buf);
    }
}

/*
 * Goal: test Buffer Eviction while reading pages from disk.
 *
 * 1) Setup bufferpool with BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK  (30 blocks) on disk,
 *    BPOOL_DEFAULT_BUFFERPOOL_SIZE (10 slots) in memory.
 * 2) Read in the first 10 pages.
 * 3) Validate that all 10 pages are correct blocks by using validate_buffer()
 * 4) Unlock and Release all 10 pages.
 * 5) Read in second 10 pages.
 * 6) Validate that all 10 pages are new blocks by using validate_buffer()
 * 7) Unlock and Release all 10 pages.
 *
 * Test coverage: BufMgr::read(), BufMgr::unlock(), BufMgr::release()
 */
TEST_F(UTBufferPoolTest, BufferEviction_TIER1)
{
    prepare_buffer_pool();
    BufferDesc *buf = nullptr;

    /* Read the first group of 10 blocks */
    for (BlockNumber blocknum = 0; blocknum < 10; blocknum++) {
        /* Read blocknum page in */
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Validate the buffer */
        validate_buffer(buf);
        /* Unlock and Release */
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* Read the second group of 10 blocks */
    for (BlockNumber blocknum = 10; blocknum < 20; blocknum++) {
        /* Read blocknum page in */
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Validate the buffer */
        validate_buffer(buf);
        /* Unlock and Release */
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* Read the third group of 10 blocks */
    for (BlockNumber blocknum = 20; blocknum < m_num_blocks_on_disk; blocknum++) {
        /* Read blocknum page in */
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Validate the buffer */
        validate_buffer(buf);
        /* Unlock and Release */
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }
}

/*
 * FIXME: test is disabled as invalidation is not yet fully implemented.
 *
 * Goal: test buffer invalidation performed by BufMgr::invalidate(). Make sure
 * that invalidated buffer is reused when the new page is loaded.
 * In addition, update page in memory which should trigger page flush as a part
 * of the buffer invalidation.
 *
 * 1) Setup bufferpool with BPOOL_DEFAULT_BUFFERPOOL_SIZE (30 blocks) on disk,
 *    BPOOL_DEFAULT_BUFFERPOOL_SIZE (10 slots) in memory.
 * 2) Read all disk pages in so that free list would be empty.
 * 3) Choose a specific block number to be invalidated.
 * 4) Find its BufferDesc in the bufferpool and call BufMgr::invalidate() on it.
 * 5) Read a new page: invalidation should have created a candidate buffer
 *    that may be reused.
 * 6) Repeat steps 3-5 but this time update the page and mark it dirty prior to
 *    the invalidation. In this way, page will be flushed during invalidation.
 * 7) Compare block on disk and in memory: should be the same.
 *
 * Test coverage: BufMgr::invalidate(), LruNode::is_in_candidate_list().
 */
TEST_F(UTBufferPoolTest, DISABLED_BufferInvalidation)
{
    prepare_buffer_pool(BPOOL_DEFAULT_BUFFERPOOL_SIZE, BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK, 1);
    BufferDesc *buf = nullptr;

    BlockNumber first_block = 0;
    BlockNumber last_block = BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK;
    /* Use vector to store buffer descriptors */
    std::vector<BufferDesc *> buffers;

    /* Read all disk pages in so that free list would be empty */
    for (BlockNumber blocknum = first_block; blocknum < last_block; blocknum++) {
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        buffers.push_back(buf);
        ASSERT_NE(buf, nullptr);
        /* Validate every page */
        validate_buffer(buf);
        /* Unlock and Release */
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* Make sure that there are no free pages anymore */
    for (BlockNumber blocknum = first_block; blocknum < last_block; blocknum++) {
        /* Read in pages */
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Validate pages */
        validate_buffer(buf);
        /* this will show that all 10 pages were in the bufferpool */
        ASSERT_EQ(buf, buffers[blocknum - first_block]);
        /* Unlock and Release */
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* Choose blocknumber of the block/page to be invalidated */
    BlockNumber block_to_invalidate = last_block - 1;
    /* Get a BufferDesc to the block to be invalidated */
    BufferDesc *invalidated_buf = m_bufmgr->
        Read(g_defaultPdbId, {m_fileId, block_to_invalidate}, LW_SHARED);
    ASSERT_NE(invalidated_buf, nullptr);
    validate_buffer(invalidated_buf);
    m_bufmgr->UnlockContent(invalidated_buf);
    m_bufmgr->Release(invalidated_buf);

    /* Header should be locked when invalidating buffer */
    invalidated_buf->LockHdr();
    /* Invalidate the buffer we've chosen above */
    ASSERT_TRUE(STORAGE_FUNC_SUCC(m_bufmgr->Invalidate(invalidated_buf)));
    ASSERT_TRUE(invalidated_buf->bufTag.IsInvalid());
    invalidated_buf->UnlockHdr(GsAtomicReadU64(&invalidated_buf->state));

    /* Now read the new page in */
    buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, block_to_invalidate}, LW_SHARED);
    ASSERT_NE(buf, nullptr);
    /*
     * Make sure that the previously invalidated buffer has been re-used:
     *    1) validate new buffer (make sure this is block_to_invalidate);
     *    2) make sure that new buffer is the one we've invalidated.
     */
    validate_buffer(buf);
    ASSERT_EQ(buf->bufBlock, invalidated_buf->bufBlock);

    /* Buffer Invalidation with flush */
    block_to_invalidate = first_block;
    invalidated_buf = m_bufmgr->
        Read(g_defaultPdbId, {m_fileId, block_to_invalidate}, LW_EXCLUSIVE);
    ASSERT_NE(invalidated_buf, nullptr);
    validate_buffer(invalidated_buf);
    update_page(invalidated_buf);
    m_bufmgr->MarkDirty(invalidated_buf);
    m_bufmgr->UnlockContent(invalidated_buf);
    m_bufmgr->Release(invalidated_buf);

    /* Header should be locked when invalidating buffer */
    invalidated_buf->LockHdr();
    /* Invalidate the buffer we've chosen above */
    ASSERT_TRUE(STORAGE_FUNC_SUCC(m_bufmgr->Invalidate(invalidated_buf)));
    /*
     * FIXME: IMPORTANT: the above invalidate() hangs in BufferDesc::Pin() which
     * calls wait_hdr_unlock.
     */
    invalidated_buf->UnlockHdr(GsAtomicReadU64(&invalidated_buf->state));
    ASSERT_TRUE(invalidated_buf->bufTag.IsInvalid());

    /* Now read the new page in */
    buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, block_to_invalidate}, LW_SHARED);
    ASSERT_NE(buf, nullptr);
    /* Validate that page is updated */
    validate_buffer(buf, BPOOL_PAGE_UPDATED);
    /* Validate that page has been flushed to disk */
    compare_buffer_with_disk(buf);
}

const uint32 LRU_MAX_USAGE = 5u;
TEST_F(UTBufferPoolTest, BufferInvalidationUsingGivenPdbId_TIER1)
{
    /* buffer pool:10 blocks lru_partition:1 */
    prepare_buffer_pool(BPOOL_DEFAULT_BUFFERPOOL_SIZE, BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK, 1);
    /* Use vector to store buffer descriptors */
    std::vector<BufferDesc *> buffers;
    BufferDesc *buf = nullptr;
    /* block 0-8 are in lrulist */
    for (BlockNumber blocknum = 0; blocknum < 9; blocknum++) {
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Validate every page */
        validate_buffer(buf);
        buffers.push_back(buf);
        /* Unlock and Release */
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* block 9(cr buffer) is in lrulist */
    buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, 0}, LW_SHARED);
    buf->controller->lastPageModifyTime.store(0, std::memory_order_release);
    uint64 latestWriteTime = buf->GetLastModifyTime();
    BufferDesc *crBuffer = m_bufmgr->ReadOrAllocCr(buf, latestWriteTime);
    ASSERT_NE(crBuffer, INVALID_BUFFER_DESC);
    CommitSeqNo pageMaxCsn = 100;
    m_bufmgr->FinishCrBuild(crBuffer, pageMaxCsn);
    buffers.push_back(crBuffer);
    m_bufmgr->Release(crBuffer);
    m_bufmgr->UnlockAndRelease(buf);
    ASSERT_EQ(m_bufmgr->GetBufLruList(0)->GetCandidateList()->Length(), 0);

    /* block 0 is in hotlist */
    for (uint32 i = 0; i < LRU_MAX_USAGE; i++) {
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, 0}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        m_bufmgr->UnlockAndRelease(buf);
    }
    ASSERT_EQ(m_bufmgr->GetBufLruList(0)->GetHotList()->Length(), 1);
    ASSERT_EQ(m_bufmgr->GetBufLruList(0)->GetCandidateList()->Length(), 0);

    m_bufmgr->InvalidateUsingGivenPdbId(g_defaultPdbId);
    ASSERT_EQ(m_bufmgr->GetBufLruList(0)->GetCandidateList()->Length(), 10);
    for (BlockNumber blocknum = 0; blocknum < 10; blocknum++) {
        ASSERT_TRUE(buffers[blocknum]->bufTag.IsInvalid());
        ASSERT_EQ(GsAtomicReadU64(&(buffers[blocknum]->state)), 0);
    }
}

/*
 * Goal: test Buffer Eviction with Pinned Buffers
 *
 * 1) Setup bufferpool with BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK (30 blocks) on disk,
 *    BPOOL_DEFAULT_BUFFERPOOL_SIZE (10 slots) in memory. Use 1 LRU partition.
 * 2) Read in the first 10 pages.
 * 3) Unlock all 10 pages but keep them pinned.
 * 4) Choose some buffer and release it. Now we have 9 buffers pinned and 1 unpinned.
 * 6) Read a single page (new) from disk.
 * 7) Validate that that page is in and that old page is out.
 *
 * Test coverage: BufMgr::p_try_to_reuse_buffer() and BufMgr::p_get_candidate_buffer().
 */
TEST_F(UTBufferPoolTest, BufferEvictionWithPinnedBuffers)
{
    Size num_slots_in_bufferpool = BPOOL_DEFAULT_BUFFERPOOL_SIZE;
    /* Important: use a single LRU partition */
    prepare_buffer_pool(num_slots_in_bufferpool, BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK, 1);
    BufferDesc *buf = nullptr;
    BufferDesc *buf_need_release[num_slots_in_bufferpool];

    /*
     * Read the first group num_slots_in_bufferpool blocks on disk to fill
     * the bufferpool completely.
     */
    for (BlockNumber blocknum = 0; blocknum < num_slots_in_bufferpool; blocknum++) {
        /* Read in pages */
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Unlock buffer but keep it pinned */
        m_bufmgr->UnlockContent(buf);
        buf_need_release[blocknum] = buf;
    }

    /* Record the old page */
    BufferDesc *old_buf = buf;
    /* Release this page */
    m_bufmgr->Release(buf);

    /* This line shows that the page is indeed unpinned. */
    ASSERT_EQ(buf->GetRefcount(), 0);

    /* Read the new page in: we'll use blocknum = num_slots_in_bufferpool */
    buf = m_bufmgr->
        Read(g_defaultPdbId, {m_fileId, (BlockNumber)num_slots_in_bufferpool}, LW_SHARED);
    ASSERT_NE(buf, nullptr);
    buf_need_release[num_slots_in_bufferpool - 1] = buf;
    /* Validate buffer: it should contain new page */
    validate_buffer(buf);

    m_bufmgr->UnlockContent(buf);
    for (uint32 i = 0; i < num_slots_in_bufferpool; i++) {
        m_bufmgr->Release(buf_need_release[i]);
    }
}

/*
 * Goal: test Buffer Eviction with Hot Pages
 *
 * 1) Setup bufferpool with BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK (30 blocks) on disk,
 *    BPOOL_DEFAULT_BUFFERPOOL_SIZE (10 slots) in memory.
 * 2) Read in the first 10 pages.
 * 3) Unlock and release 10 pages.
 * 4) Choose 1 page and access it enought times to make it HOT.
 * 6) Validate that page is HOT.
 * 7) Validate that the HOT page is still there and is still HOT.
 *
 * Test coverage: BufMgr::p_try_to_reuse_buffer() and BufMgr::p_get_candidate_buffer().
 */
TEST_F(UTBufferPoolTest, BufferEvictionWithHotPages)
{
    Size num_slots_in_bufferpool = BPOOL_DEFAULT_BUFFERPOOL_SIZE;
    prepare_buffer_pool(); /* using all defaults */
    BufferDesc *buf = nullptr;

    /* Read the first 10 blocks on disk */
    for (BlockNumber blocknum = 0; blocknum < num_slots_in_bufferpool; blocknum++) {
        /* Read in pages */
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Unlock and release */
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* Access one page enough (BPOOL_LRU_MAX_USAGE) times to make it HOT */
    Size num_accesses_to_become_hot = BPOOL_LRU_MAX_USAGE;
    Size hot_blocknum = num_slots_in_bufferpool - 1;
    for (int accesses = 0; accesses < num_accesses_to_become_hot; accesses++) {
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, (BlockNumber)hot_blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* Record Hot Page */
    BufferDesc *hot_buf = buf;
    /* Validate that the page is HOT */
    ASSERT_TRUE(is_in_hot_list(buf));
    /* Read the second 10 blocks on disk */
    for (BlockNumber blocknum = num_slots_in_bufferpool; blocknum < 2 * num_slots_in_bufferpool; blocknum++) {
        /* Read in pages */
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Unlock and Release */
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* Validate that the HOT page is still there and is still HOT. Acquire content lock to mute GetPage() check. */
    DstoreLWLockAcquire(&hot_buf->contentLwLock, LW_SHARED);
    validate_buffer(hot_buf);
    LWLockRelease(&hot_buf->contentLwLock);
    ASSERT_TRUE(is_in_hot_list(hot_buf));
}

/*
 * Goal: test buffer eviction with update, to validate that the updated pages
 * will get flushed to disk on eviction.
 *
 * 1) Setup bufferpool with BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK (30 blocks) on disk,
 *    BPOOL_DEFAULT_BUFFERPOOL_SIZE (10 slots) in memory.
 * 2) Read in all the blocks on disk one-by-one, update each page using update_page().
 * 3) Validate that each block is correct by using validate_buffer()
 * 4) Mark each buffer dirty, unlock and release the buffer.
 * 5) Run loop over all 30 blocks on disk and ensure that updated pages were
 *    flushed to disk during page eviction.
 *
 * Test coverage: BufMgr::p_try_to_reuse_buffer() and BufMgr::p_get_candidate_buffer().
 */
TEST_F(UTBufferPoolTest, BufferEvictionWithUpdate)
{
    /* Setup bufferpool with all default settings */
    prepare_buffer_pool();
    BufferDesc *buf = nullptr;

    /*
     * Read all pages on disk one-by-one and update each.
     */
    for (BlockNumber blocknum = 0; blocknum < m_num_blocks_on_disk; blocknum++) {
        /* Read in each page */
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_EXCLUSIVE);
        ASSERT_NE(buf, nullptr);

        /* Update the page */
        update_page(buf);

        m_bufmgr->MarkDirty(buf);
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);

        /* Validate page in memory */
        DstoreLWLockAcquire(&buf->contentLwLock, LW_SHARED);
        validate_buffer(buf, BPOOL_PAGE_UPDATED);
        LWLockRelease(&buf->contentLwLock);
    }

    /* Validate and compare with buffer for all blocks on disk */
    for (BlockNumber blocknum = 0; blocknum < m_num_blocks_on_disk; blocknum++) {
        /* Read in pages */
        buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
        ASSERT_NE(buf, nullptr);

        /*
         * Before we compare the buffer with disk, we should make sure that
         * the buffer is evicted from the buffer pool at least once, so that the
         * buffer content has been flushed to disk. Since we group buffers by
         * different lru lists, some buffers may not be evicted, so that their
         * content is not flushed. Therefore, we should check the buffer state
         * to see if the buffer is dirty. If the buffer is not dirty, it means
         * the buffer flushed and we can check buffer content with disk. If
         * buffer is still dirty, it means the buffer content exists only in the
         * buffer pool, and it's not been flushed, and therefore it's different
         * from disk.
         */
        if (GsAtomicReadU64(&buf->state) & Buffer::BUF_CONTENT_DIRTY) {
            validate_buffer(buf, BPOOL_PAGE_UPDATED);
        } else {
            /* Validate pages */
            validate_buffer(buf, BPOOL_PAGE_UPDATED);
            compare_buffer_with_disk(buf);
        }
        /*
         * We should add unlock + release after we validate buffer. If not, all
         * available buffers in the buffer pool will be locked and it will loop
         * infinitely because no available buffer can be found.
         */
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }
}

/*
 *  == MULTI-THREADED TEST CASES ==
 */

/*
 * Used by ConcurrentRead test case. Each read thread will have this function
 * passed to it. It then looks up its buffer and performs a read on it. Since
 * all threads will be doing this concurrently, the use of barriers is
 * required. The boolean check_ref_count is used to ensure that only one thread
 * checks the ref_count. The variable ref_count_before_release should equal to
 * the number of threads once every thread has read. This test uses 3 barriers:
 * 1) the first barrier, "read_barrier", insures that before checking the
 *    ref_count, all threads have read (and pinned) the page;
 * 2) the second barrier, "ref_count_before_release", ensures that the
 *    ref_count is read before releasing a buffer.
 * 3) The third barrier, "refcount_after_release_barrier", is used to ensure
 *    that all threads have released the buffer before asserting that the
 *    ref_count is 0.
 */
void UTBufferPoolTest::multithread_read_test(const TestThreadContext &thd_ctx, bool check_ref_count)
{
    BufMgr *bufmgr = thd_ctx.bufmgr;
    FileId fileId = thd_ctx.fileId;

    /* Create a backend thread in order to access the buffer pool. */
    create_thread_and_register();

    BlockNumber blockNumber = 0;
    BufferDesc *buf;
    buf = bufmgr->Read(g_defaultPdbId, {fileId, blockNumber}, LW_SHARED);
    ASSERT_NE(buf, nullptr);

    /* Wait for all threads to finish reading */
    pthread_barrier_wait(&g_barriers[0]); /* read barrier */

    /*
     * We expect the ref count before each thread releasing
     * the buffer to be equal to the number of threads and zero
     * after releasing the buffer. The thread that is assigned
     * to check the ref_count is checking if the ref_count is
     * equal to the number of threads, as we have barriers in place
     * so that no thread releases the buffer before the ref_count
     * is read.
     */
    if (check_ref_count) {
        /* Once BUF_MAY_DEFER is set, concurrent pins publish into the
         * shared-pin arena instead of state.refcount, so GetRefcount()
         * alone undercounts the true pin count. LockHdr() drains the arena
         * (via ApplyDeferredPins) and folds those pins back into
         * state.refcount, giving a trustworthy value to read.
         *
         * Snapshot the refcount while holding the header lock, then release
         * it before asserting — ASSERT_EQ returns from the function on
         * failure, which would strand the spinlock held. */
        uint64 hdrState = buf->LockHdr();
        uint64 refcount = buf->GetRefcount();
        buf->UnlockHdr(hdrState);
        ASSERT_EQ(refcount, BPOOL_MULTITHREAD_NUM_OF_THREADS);
    }

    /*
     * Wait for the ref count to be set before unlocking and unpinning.
     * This is "refcount_before_release_barrier".
     */
    pthread_barrier_wait(&g_barriers[1]);
    bufmgr->UnlockContent(buf);
    bufmgr->Release(buf);

    /* Wait for all threads to release: refcount_after_release_barrier */
    pthread_barrier_wait(&g_barriers[2]);

    /*
     * Now that every thread has released the shared lock, let's
     * obtain the ref count again, the value should be zero.
     */
    if (check_ref_count) {
        ASSERT_EQ(buf->GetRefcount(), 0);
    }

    /* De-register the backend thread to avoid leaks */
    unregister_thread();

    return;
}

/*
 * Goal: To test that multiple reads increases the
 *     buffer reference count. Releasing and unlocking
 *     the buffer from all threads should then decrement it
 *     back to zero.
 *
 * 1. Setup bufferpool.
 * 2. Start 10 threads that read bufferpool page and stop
 *    after that (pthread_barrier_wait).
 * 3. Once they continue, one of them (using special flag passed in)
 *    will check that ref_count of the page is BPOOL_MULTITHREAD_NUM_OF_THREADS.
 *    Another barrier is reached and all threads may continue.
 * 4. All threads release their pages. --> need to release to reduce the ref count
 * 5. Main thread checks that ref_count is back to 0.
 *
 * Test Coverage: Concurrent read, buffer_desc->GetRefcount, and functions related
 *                to incrementing and decrementing the reference counter.
 *
 *
 */
TEST_F(UTBufferPoolTest, ConcurrentRead_TIER1)
{
    const Size num_barriers = 3; /* unique for this test */
    /* Initialize the barriers */
    init_global_barriers(BPOOL_MULTITHREAD_NUM_OF_THREADS, num_barriers);

    /* Prepare the bufferpool with all the default settings. */
    prepare_buffer_pool();

    /* Declare BPOOL_MULTITHREAD_NUM_OF_THREADS and contexts. */
    std::thread threads[BPOOL_MULTITHREAD_NUM_OF_THREADS];
    TestThreadContext thd_ctx[BPOOL_MULTITHREAD_NUM_OF_THREADS];

    /*
     * Create BPOOL_MULTITHREAD_NUM_OF_THREADS threads and assign them
     * the function pointer along with their respected thread context. One
     * the threads will need to set the ref counts. In this case, it will
     * the last one in the iteration.
     */
    for (Size i = 0; i < BPOOL_MULTITHREAD_NUM_OF_THREADS; i++) {
        TestThreadContext ctx = thd_ctx[i];
        ctx.bufmgr = m_bufmgr;
        ctx.fileId = m_fileId;
        if (i != BPOOL_MULTITHREAD_NUM_OF_THREADS - 1) {
            threads[i] = std::thread(multithread_read_test, ctx, false);
        } else { /* We need one thread to obtain the ref_count */
            threads[i] = std::thread(multithread_read_test, ctx, true);
        }
    }

    /* We need to wait for all threads to gracefully finish */
    wait_for_threads_to_finish(threads, BPOOL_MULTITHREAD_NUM_OF_THREADS);

    /* Destroy the barriers */
    destroy_global_barriers();
}

/*
 * Used by ConcurrentWriteWithEviction test case.
 * The function updates the integer on each page by adding one to it.
 * After all threads finish, it checks whether integer is equal to number of
 * threads.
 */
void UTBufferPoolTest::multithread_write(const TestThreadContext &thd_ctx, int thread_id, int number_of_pages_on_disk)
{
    BufMgr *bufmgr = thd_ctx.bufmgr;
    FileId fileId = thd_ctx.fileId;

    /* Create a backend thread in order to access the buffer pool. */
    create_thread_and_register();
    BufferDesc *buf = nullptr;

    /* Each thread will update the integer (increase the integer by 1). */
    for (BlockNumber i = 0; i < number_of_pages_on_disk; i++) {
        buf = bufmgr->Read(g_defaultPdbId, {fileId, i}, LW_EXCLUSIVE);
        BufBlock block = (BufBlock)buf->GetPage();
        /* Obtain the page through the buffertag so we can update it. */
        PageId page = buf->bufTag.pageId;

        /* add one to the stored counter */
        uint *counter = (uint *)(block + BPOOL_PAGE_STRING_END_OFFSET);
        *counter = *counter + 1;

        /* Mark page as dirty */
        bufmgr->MarkDirty(buf);
        bufmgr->UnlockContent(buf);
        bufmgr->Release(buf);
    }

    /* De-register the backend thread to avoid leaks */
    unregister_thread();
    return;
}

/*
 * Goal: To test that multiple write with update multiple pages in each thread.
 *
 * 1. Setup bufferpool.
 * 2. Choose a page and store integer in it (set at 0 initially).
 * 3. Start 10 threads.
 * 4. Each thread updates data on the page. It simply reads our integer from
 *    the page and increases it by 1. Page is released after that.
 * 5. Once all threads are done, main thread reads the page and validates that integer is 10.
 *
 * Test Coverage: BufMgr::read().
 */
TEST_F(UTBufferPoolTest, ConcurrentWriteWithEviction_TIER1)
{
    const int number_of_threads = 24;
    /* Setup bufferpool and blocks on disk. */
    prepare_buffer_pool();

    /*
     * For each block on disk, store integer counter (0 initially) at some
     * offset.
     */
    for (BlockNumber i = 0; i < m_num_blocks_on_disk; i++) {
        BufferDesc *buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, i}, LW_EXCLUSIVE);
        BufBlock block = (BufBlock)buf->GetPage();
        /* add one to the stored counter */
        uint *counter = (uint *)(block + BPOOL_PAGE_STRING_END_OFFSET);
        *counter = 0;

        /* Mark page as dirty */
        m_bufmgr->MarkDirty(buf);
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* Declare number_of_threads and contexts.*/
    std::thread threads[number_of_threads];
    TestThreadContext thd_ctx[number_of_threads];

    /*
     * Create number_of_threads threads and assign them
     * the function pointer along with their respected thread context. One
     * the threads will need to set the ref counts. In this case, it will
     * the last one in the iteration.
     */
    for (Size i = 0; i < number_of_threads; i++) {
        TestThreadContext ctx = thd_ctx[i];
        ctx.bufmgr = m_bufmgr;
        ctx.fileId = m_fileId;
        threads[i] = std::thread(multithread_write, ctx, i, m_num_blocks_on_disk);
    }

    /* We need to wait for all threads to gracefully finish */
    wait_for_threads_to_finish(threads, number_of_threads);

    /*
     * For each block on disk, check that counter is equal to the number of
     * threads.
     */
    for (BlockNumber i = 0; i < m_num_blocks_on_disk; i++) {
        BufferDesc *buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, i}, LW_EXCLUSIVE);
        BufBlock block = (BufBlock)buf->GetPage();
        /* add one to the stored counter */
        uint *counter = (uint *)(block + BPOOL_PAGE_STRING_END_OFFSET);
        ASSERT_EQ(*counter, number_of_threads);

        /* Mark page as dirty */
        m_bufmgr->MarkDirty(buf);
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }
}

/*
 * Used by RandomBufferpoolMod test case.
 * For each round, the function increments P1 by random_num_modify_page and
 * decrements P2 by random_num_modify_page. Then the last thread of each round checks if the sum of all pages
 * in the bufferpool is still the same.
 */
void UTBufferPoolTest::multithread_randomMod(const TestThreadContext &thd_ctx, int thread_id, uint number_of_rounds,
                                             uint bufferpool_size, bool check_sum,
                                             std::vector<RandomNumbers> *rand_tracker)
{
    /* Initiate variables for the helper function (ex to help generate random numbers, tracking them etc.) */
    int barrier_count = 0;

    uint random_num_modify_page;
    BlockNumber random_P1;
    BlockNumber random_P2;

    uint sum_of_pages;
    std::vector<uint> actual_page_value;
    std::vector<BlockNumber> pagenumbers;

    BufMgr *bufmgr = thd_ctx.bufmgr;
    FileId fileId = thd_ctx.fileId;

    /* Create a backend thread in order to access the buffer pool. */
    create_thread_and_register();
    BufferDesc *buf = nullptr;
    uint *counter;

    /* To calculate P1, P2 later, we use a vector to make sure P1 and P2 are different, storing all pages
     * then shuffling the vector to get different pages each round
     */
    for (BlockNumber i = 0; i < bufferpool_size; i++) {
        pagenumbers.push_back(i);
    }

    /* Each round modifies new random pages, incrementing and decrementing the same value for two
     * different pages. At the end of the round, the last thread checks if the sum of pages is still the same.
     */
    for (int i = 0; i < number_of_rounds; i++) {
        sum_of_pages = 0;
        random_num_modify_page = rand() % 100 + 1;

        /* Shuffle the vector to obtain P1, P2 (this ensures P1, P2 are different) */
        std::random_shuffle(pagenumbers.begin(), pagenumbers.end());
        random_P1 = pagenumbers[0];
        random_P2 = pagenumbers[1];

        /* Each thread will increment the value in P1 by random_num_modify_page . */
        buf = bufmgr->Read(g_defaultPdbId, {fileId, random_P1}, LW_EXCLUSIVE);
        BufBlock block = (BufBlock)buf->GetPage();
        /* add random_num_modify_page to the stored counter */
        counter = (uint *)(block + BPOOL_PAGE_STRING_END_OFFSET);
        *counter = *counter + random_num_modify_page;

        /* Mark page as dirty */
        bufmgr->MarkDirty(buf);
        bufmgr->UnlockContent(buf);
        bufmgr->Release(buf);

        /* Each thread will decrement the value in P2 by random_num_modify_page . */
        buf = bufmgr->Read(g_defaultPdbId, {fileId, random_P2}, LW_EXCLUSIVE);
        block = (BufBlock)buf->GetPage();

        /* subtract random_num_modify_page to the stored counter */
        counter = (uint *)(block + BPOOL_PAGE_STRING_END_OFFSET);
        int decrement = *counter - random_num_modify_page;

        /* If the value of the new page is less than 0, we get a new page to decrement by random_num_modify_page */
        if (decrement < 0) {
            int page_counter = 2;
            while (decrement < 0) {
                /* Unlock and release */
                bufmgr->UnlockContent(buf);
                bufmgr->Release(buf);

                random_P2 = pagenumbers[page_counter];
                buf = bufmgr->Read(g_defaultPdbId, {fileId, random_P2}, LW_EXCLUSIVE);
                block = (BufBlock)buf->GetPage();

                /* Obtaining the value of the page */
                counter = (uint *)(block + BPOOL_PAGE_STRING_END_OFFSET);
                decrement = *counter - random_num_modify_page;

                ++page_counter;
            }
        }
        /* Set the page value to a non-negative number */
        *counter = decrement;

        /* Mark page as dirty */
        bufmgr->MarkDirty(buf);
        bufmgr->UnlockContent(buf);
        bufmgr->Release(buf);

        /* Keep track of the random numbers in rand_tracker to help with future debugging */
        rand_tracker->at(thread_id).P1 = random_P1;
        rand_tracker->at(thread_id).P2 = random_P2;
        rand_tracker->at(thread_id).random_num = random_num_modify_page;

        /* Wait for threads to finish modifying page value */
        pthread_barrier_wait(&g_barriers[barrier_count]);
        barrier_count++;

        /* Check sum of pages in bufferpool using the the last thread. */
        if (check_sum) {
            for (BlockNumber i = 0; i < bufferpool_size; i++) {
                buf = bufmgr->Read(g_defaultPdbId, {fileId, i}, LW_SHARED);
                block = (BufBlock)buf->GetPage();

                /* Obtaining the value of the page */
                counter = (uint *)(block + BPOOL_PAGE_STRING_END_OFFSET);
                sum_of_pages = sum_of_pages + *counter;

                /* Store the actual page values of the bufferpool to compare later */
                actual_page_value.push_back(*counter);

                /* Unlock and release */
                bufmgr->UnlockContent(buf);
                bufmgr->Release(buf);
            }
            EXPECT_EQ(sum_of_pages, bufferpool_size * 1000);

            if (sum_of_pages != bufferpool_size * 1000) {
                /* Initialize a vector to store value of all bufferpool pages after modifying
                 * the pages values */
                std::vector<uint> expected_page_value;
                for (int i = 0; i < bufferpool_size; i++) {
                    expected_page_value.push_back(1000);
                }

                for (int i = 0; i < rand_tracker->size(); i++) {
                    expected_page_value[rand_tracker->at(i).P1] += rand_tracker->at(i).random_num;
                    expected_page_value[rand_tracker->at(i).P2] -= rand_tracker->at(i).random_num;
                }

                /* Compare expected_page_value with actual_page_value */
                for (int i = 0; i < bufferpool_size; i++) {
                    ASSERT_EQ(expected_page_value[i], actual_page_value[i]);
                }
            }
        }

        /* Clear the values in rand_tracker for the next round */
        rand_tracker->at(thread_id).P1 = 0;
        rand_tracker->at(thread_id).P2 = 0;
        rand_tracker->at(thread_id).random_num = 0;

        /* Wait for threads to finish summing the pages */
        pthread_barrier_wait(&g_barriers[barrier_count]);
        barrier_count++;
    }

    /* De-register the backend thread to avoid leaks */
    unregister_thread();
    return;
}

/*
 * Goal: To test concurrent random multiple reads and writes to random pages of the bufferpool
 *
 * 1. Setup bufferpool. num_blocks_on_disk pages on disk, bufferpool size is bufferpool_size
 * 2. Each page stores an integer in it (set at 1000 initially). Start number_of_threads
 * 3. For each round: (number of rounds are configurable, set round = 5).
 *      a. m = random_num_modify_page page, P1, P2 are random pages.
 *      b. P1 increments data_content by m, then unlocks and releases it.
 *      c. P2 decrements data_content by m, then unlocks and releases it.
 *      d. Set up barrier, main thread checks the sum of pages in bufferpool is unchanged.
 * 4. Flush all pages into disk, check the sum of pages in disk is unchanged.
 *
 * Test Coverage:
 */
void UTBufferPoolTest::RandomBufferpoolMod(Size num_blocks_on_disk, Size bufferpool_size, const uint number_of_threads)
{
    /* To generate random numbers */
    srand(time(NULL));

    /* Initiate variables used throughout the test case. */
    const int number_of_rounds = 5;
    const Size num_barriers = number_of_rounds * 2;
    uint sum_of_pages = 0;

    /* Initialize the barriers */
    init_global_barriers(number_of_threads, num_barriers);

    /*
     * For each block on disk, store integer counter (1000 initially) at some
     * offset.
     */
    for (BlockNumber i = 0; i < num_blocks_on_disk; i++) {
        BufferDesc *buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, i}, LW_EXCLUSIVE);
        BufBlock block = (BufBlock)buf->GetPage();
        /* Setting the stored counter to 1000 */
        uint *counter = (uint *)(block + BPOOL_PAGE_STRING_END_OFFSET);
        *counter = 1000;

        /* Mark page as dirty */
        m_bufmgr->MarkDirty(buf);
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* Declare number_of_threads and contexts.*/
    std::thread threads[number_of_threads];
    TestThreadContext thd_ctx[number_of_threads];

    /* Declare and initialize a vector to keep track of the random numbers for each thread
     * Each thread modifies its own slot (from 0 to number_of_threads -1) in the rand_tracker
     * by storing its random
     */
    std::vector<RandomNumbers> rand_tracker;
    for (int i = 0; i < number_of_threads; i++) {
        RandomNumbers r;
        rand_tracker.push_back(r);
    }

    /*
     * Create number_of_threads threads and assign them
     * the function pointer along with their respected thread context. One
     * the threads will need to calculate sum of pages in bufferpool. In this case, it will
     * the last one in the iteration.
     */
    for (Size i = 0; i < number_of_threads; i++) {
        TestThreadContext ctx = thd_ctx[i];
        ctx.bufmgr = m_bufmgr;
        ctx.fileId = m_fileId;
        if (i == number_of_threads - 1) {
            threads[i] =
                std::thread(multithread_randomMod, ctx, i, number_of_rounds, bufferpool_size, true, &rand_tracker);
        } else {
            threads[i] =
                std::thread(multithread_randomMod, ctx, i, number_of_rounds, bufferpool_size, false, &rand_tracker);
        }
    }

    /* Wait for all threads to gracefully finish */
    wait_for_threads_to_finish(threads, number_of_threads);

    /* Destroy the barriers */
    destroy_global_barriers();

    /* Flush all pages into disk */
    for (BlockNumber i = 0; i < bufferpool_size; i++) {
        BufferTag bufTag = {g_defaultPdbId, {m_fileId, i}};
        m_bufmgr->Flush(bufTag);
    }

    /* Check sum of pages in disk */
    for (BlockNumber i = 0; i < num_blocks_on_disk; i++) {
        BufferDesc *buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, i}, LW_SHARED);
        BufBlock block = (BufBlock)buf->GetPage();
        /* Obtaining the value of the page */
        uint *counter = (uint *)(block + BPOOL_PAGE_STRING_END_OFFSET);
        sum_of_pages = sum_of_pages + *counter;

        /* Unlock and release */
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }
    ASSERT_EQ(sum_of_pages, num_blocks_on_disk * 1000);
}

/*
 * This test is for RandomBufferpoolMod, we call the function 5 times with various configurations
 * See RandomBufferpoolMod for detailed comments.
 */
TEST_F(UTBufferPoolTest, RandomBufferpoolModTest_TIER1)
{
    uint num_blocks_on_disk = 1000;
    uint bufferpool_size = 300;
#ifdef ENABLE_THREAD_CHECK
    uint number_of_threads = 5;
#else
    uint number_of_threads = 20;
#endif

    /* Setup bufferpool and blocks on disk. */
    prepare_buffer_pool(bufferpool_size, num_blocks_on_disk, BPOOL_DEFAULT_LRU_PARTITIONS);

    /* Call function 1st time */
    RandomBufferpoolMod(num_blocks_on_disk, bufferpool_size, number_of_threads);
    m_bufmgr->Destroy();
    delete m_bufmgr;

    /* Function called 2nd time */
    bufferpool_size = 300;
#ifdef ENABLE_THREAD_CHECK
    number_of_threads = 5;
#else
    number_of_threads = 99;
#endif

    m_bufmgr = DstoreNew(UTBufferPoolTest::m_ut_memory_context) BufMgr(bufferpool_size, BPOOL_DEFAULT_LRU_PARTITIONS);
    ASSERT_NE(m_bufmgr, nullptr);
    m_bufmgr->Init();

    RandomBufferpoolMod(num_blocks_on_disk, bufferpool_size, number_of_threads);
    m_bufmgr->Destroy();
    delete m_bufmgr;

    /* Function called again */
    bufferpool_size = 600;
#ifdef ENABLE_THREAD_CHECK
    number_of_threads = 5;
#else
    number_of_threads = 40;
#endif

    m_bufmgr = DstoreNew(UTBufferPoolTest::m_ut_memory_context) BufMgr(bufferpool_size, BPOOL_DEFAULT_LRU_PARTITIONS);
    ASSERT_NE(m_bufmgr, nullptr);
    m_bufmgr->Init();

    RandomBufferpoolMod(num_blocks_on_disk, bufferpool_size, number_of_threads);
    m_bufmgr->Destroy();
    delete m_bufmgr;

    /* Function called again */
    bufferpool_size = 600;
#ifdef ENABLE_THREAD_CHECK
    number_of_threads = 5;
#else
    number_of_threads = 99;
#endif

    m_bufmgr = DstoreNew(UTBufferPoolTest::m_ut_memory_context) BufMgr(bufferpool_size, BPOOL_DEFAULT_LRU_PARTITIONS);
    ASSERT_NE(m_bufmgr, nullptr);
    m_bufmgr->Init();

    RandomBufferpoolMod(num_blocks_on_disk, bufferpool_size, number_of_threads);
    m_bufmgr->Destroy();
    delete m_bufmgr;

    /* Function called again */
    bufferpool_size = 900;
#ifdef ENABLE_THREAD_CHECK
    number_of_threads = 5;
#else
    number_of_threads = 99;
#endif

    m_bufmgr = DstoreNew(UTBufferPoolTest::m_ut_memory_context) BufMgr(bufferpool_size, BPOOL_DEFAULT_LRU_PARTITIONS);
    ASSERT_NE(m_bufmgr, nullptr);
    m_bufmgr->Init();

    RandomBufferpoolMod(num_blocks_on_disk, bufferpool_size, number_of_threads);
}

/*
 * Used by RandomBufferpoolAccess test case. This function is the main thread
 * function bound to each thread in RandomBufferpoolAccess.
 * Each thread has an thread_id associated with it. The thread_id corresponds
 * to its position in the thread array from thread_id from
 * RandomBufferpoolAccess. Each thread has unique position thread_bit_idx
 * assigned to it in the bitmask. Access to pages will be done in a random
 * order to mimic TPC-C behavior.
 * The function goes through every page in on disk and does the following:
 *   1) checks that the thread's own bit on the page is set to zero. Page is
 *      unlocked and released after that.
 *   2) The thread's bit is then set to 1 and page is updated. page is then
 *      marked dirty and again unlocked and released.
 *   3) Finally, thread verifies (by reading the page again) that its bit is
 *      set to 1.
 * Note that the bufferpool will need to evict and handle concurrent
 * reads/writes to the same pages.
 */
void UTBufferPoolTest::multithread_read_write_bitmask(const TestThreadContext &thd_ctx, uint thread_bit_idx,
                                                      uint number_of_pages_on_disk)
{
    BufMgr *bufmgr = thd_ctx.bufmgr;
    FileId fileId = thd_ctx.fileId;

    /* Create a backend thread in order to access the buffer pool. */
    create_thread_and_register();
    BufferDesc *buf = nullptr;

    /*
     * Blocks will be accessed in the random order to mimic TPC-C behaviour.
     * in order to do that, std::random_shuffle() will be done on a vector
     * containing block numbers for all blocks on disk.
     */
    std::vector<BlockNumber> blocknumbers;
    for (int i = 0; i < number_of_pages_on_disk; ++i) {
        blocknumbers.push_back(i);
    }
    /* Shuffle the vector using std::shuffle */
    std::random_shuffle(blocknumbers.begin(), blocknumbers.end());

    /*
     * Each thread is assigned a bit in the bitmask, thread_bit_idx.
     * The first thread will get thread_bit_idx 0, the 2nd will get 1,
     * and so on.
     *
     * Bits for threads are stored in the array of the 64-bit integers
     * inside the page. In order to access bit thread_bit_idx, we need
     * to determine which integer (bucket) to use in the array and then
     * we need to determine which bit in this integer/bucket we should
     * use. The bucket will be (thread_bit_idx / BPOOL_BITSET_SIZE)
     * and the bit position inside the bucket (64-bit integer) will be
     * (thread_bit_idx % BPOOL_BITSET_SIZE).
     * As an example if we have 11 buckets and 500 threads, thread
     * with thread_bit_idx = 64 will fall under bucket 1, (starting buckets
     * from 0), at offset 0 in the bucket.
     *
     * We first start off with calculating the bucket/offset for each thread. The
     * rest of the function will use this value. Note that, when accessing the page,
     * the first (BPOOL_PAGE_STRING_END_OFFSET - 1) bits are reserved for text
     * generated by the generate_token() function.
     */

    /* Find which bitset bucket we belong to */
    uint bitset_idx = (thread_bit_idx) / BPOOL_BITSET_SIZE;
    /* Find the relative position within the bitset bucket */
    uint relative_bit_idx = thread_bit_idx % BPOOL_BITSET_SIZE;
    /* Start position of bitset as a uint64 on the page */
    uint block_offset = bitset_idx * BPOOL_BITSET_SIZE + BPOOL_PAGE_STRING_END_OFFSET;

    /*
     * Read each page (every available page, including the ones on disk) and
     * ensure that the corresponding thread's bit is set to zero. Note that
     * we going through the pages by the order in which we shuffled.
     */
    for (auto blocknum : blocknumbers) {
        buf = bufmgr->Read(g_defaultPdbId, {fileId, blocknum}, LW_SHARED);
        BufBlock block = (BufBlock)buf->GetPage();
        /* Copy the corresponding bit set from the page and verify relative bit id */
        uint64 *bitmask_bucket;
        bitmask_bucket = (uint64 *)(block + block_offset);
        std::bitset<BPOOL_BITSET_SIZE> bit_set_in_page = std::bitset<BPOOL_BITSET_SIZE>(*bitmask_bucket);
        ASSERT_EQ(bit_set_in_page[relative_bit_idx], 0);

        /*
         * We need to release the page in order for the bufferpool to safely
         * evict pages.
         */
        bufmgr->UnlockContent(buf);
        bufmgr->Release(buf);

        /*
         * Each thread will update the bitset by flipping
         * the bit in its thread_bit_idx location. Flipping should
         * flip a 0 to a 1.
         */
        buf = bufmgr->Read(g_defaultPdbId, {fileId, blocknum}, LW_EXCLUSIVE);
        block = (BufBlock)buf->GetPage();
        /* Obtain the page through the buffertag so we can update it. */
        PageId page = buf->bufTag.pageId;
        /* Read bitset from the page and flip the bit at position relative_bit_idx */
        bitmask_bucket = (uint64 *)(block + block_offset);
        std::bitset<BPOOL_BITSET_SIZE> bitmask_in_page = std::bitset<BPOOL_BITSET_SIZE>(*bitmask_bucket);
        bitmask_in_page.flip(relative_bit_idx);
        /* We need this check since we are copying the bitset as a uint64 */
        ASSERT_EQ(BPOOL_BITSET_SIZE, 64);
        /* Copy bitset to page at offset BPOOL_PAGE_STRING_END_OFFSET. */
        *((uint64 *)(block + block_offset)) = bitmask_in_page.to_ullong();

        /* Mark page as dirty, unlock and release. */
        bufmgr->MarkDirty(buf);
        bufmgr->UnlockContent(buf);
        bufmgr->Release(buf);

        /*
         * Read the page again and ensure that the corresponding thread's
         * bit is set to one.
         */
        buf = bufmgr->Read(g_defaultPdbId, {fileId, blocknum}, LW_SHARED);
        block = (BufBlock)buf->GetPage();
        bitmask_bucket = (uint64 *)(block + block_offset);
        bit_set_in_page = std::bitset<BPOOL_BITSET_SIZE>(*bitmask_bucket);
        ASSERT_EQ(bit_set_in_page[relative_bit_idx], 1);
        /* Unlock and release. */
        bufmgr->UnlockContent(buf);
        bufmgr->Release(buf);
    }

    /* De-register the backend thread to avoid leaks */
    unregister_thread();

    return;
}

/*
 * Goal: To test that multiple concurrent reads and writes to
 *       multiple pages happen without an issue. This test tries to
 *       simulate the concurrent workload with bufferpool that is not
 *       large enough to hold all the pages in memory. This workload
 *       has twice as many reads as writes. This workload accesses pages
 *       randomly, similar to what TPC-C does.
 *
 * 1. Setup bufferpool with 1000 pages on disk and 300 bufferpool size.
 * 2. Initiate number_of_threads threads.
 * 3. Write the bitmasks to all 1000 pages on disk.
 *    This corresponds to read of the page.
 * 4. Start each thread with multithread_read_write_bitmask.
 *    Assign one bit to each thread in the bitmask.
 *    Note that the next few steps will be done concurrently
 *    by multiple threads, they will be reading and writing to the
 *    same pages.
 * 5. Each thread will check that their corresponding bit
 *    in the bitmask is set to zero.
 * 6. Each thread will set its corresponding bit to 1. This corresponds
 *    to the page write and is done with exclusive lock.
 *
 * 7. Each thread will verify that the its corresponding
 *    bit is set to 1.
 *
 * Test Coverage: BufMgr::read() (including flush and eviction).
 */
void UTBufferPoolTest::RandomBufferpoolAccess(Size num_blocks_on_disk, Size bufferpool_size,
                                              const uint number_of_threads)
{
    /* Initiate variables used throughout the test case. */
    const uint lru_partitions = BPOOL_DEFAULT_LRU_PARTITIONS;
    /*
     * Since each thread will be assigned a dedicated bit, number of threads
     * should not exceed the number of bits stored inside a page.
     */
    ASSERT_TRUE(number_of_threads <= BPOOL_MAX_NUMBER_BITS);
    /*
     * Setup bufferpool and pages on disk.
     * Note that generate_token() is called
     */
    prepare_buffer_pool(bufferpool_size, num_blocks_on_disk, lru_partitions);

    /* For each of copying on each page, we use an array of bitmasks */
    std::bitset<BPOOL_BITSET_SIZE> array_of_bitmasks[BPOOL_MAX_NUMBER_BITMASKS];
    /*
     * For each page on disk, add a BPOOL_BITSET_SIZE
     * bitmask and initialize all its bits to zero.
     * Since we might have more than 64 threads, and
     * 64 is the maximum size of a primitive integer, we
     * need to use an array of bitsets. Each bitset will
     * be of size 64. When reading/writing, one needs to
     * calculate the which position within array_of_bitmasks
     * the bit in question falls under.
     *
     */
    for (BlockNumber i = 0; i < num_blocks_on_disk; i++) {
        BufferDesc *buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, i}, LW_EXCLUSIVE);
        Page *block = buf->GetPage();

        /* Initialize bitset with all bits zeros and write to block. */
        for (uint j = 0; j < BPOOL_MAX_NUMBER_BITMASKS; j++) {
            uint block_offset = j * BPOOL_BITSET_SIZE;
            /* Ensure that all bits are set to off (0) */
            array_of_bitmasks[j].reset();
            /* Copy bitset to page at offset BPOOL_PAGE_STRING_END_OFFSET. */
            *((uint64 *)(block + BPOOL_PAGE_STRING_END_OFFSET + block_offset)) = array_of_bitmasks[j].to_ullong();
        }

        /* Mark page as dirty, unlock and release. */
        m_bufmgr->MarkDirty(buf);
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* Declare number_of_threads and contexts.*/
    std::thread threads[number_of_threads];
    TestThreadContext thd_ctx[number_of_threads];

    /* Create number_of_threads threads and assign them
     * the function pointer along with their respected thread context. One
     * the threads will need to set the ref counts. In this case, it will
     * the last one in the iteration.*/
    for (Size i = 0; i < number_of_threads; i++) {
        TestThreadContext ctx = thd_ctx[i];
        ctx.bufmgr = m_bufmgr;
        ctx.fileId = m_fileId;
        threads[i] = std::thread(multithread_read_write_bitmask, ctx, i, num_blocks_on_disk);
    }

    /* We need to wait for all threads to gracefully finish */
    wait_for_threads_to_finish(threads, number_of_threads);

    /*
     * We now need to check that all page bitset's are set to 1.
     * The number of bits set to 1 must equal the number of threads.
     * Since all but the last bucket needs to be filled, where the
     * last bucket may or may not be empty, we can check the bitset
     * for fully set buckets as a whole with the std::bitset any function.
     * For the last bucket, we need to check every bit of the remaining
     * bits that must be set to 1.
     * For example, if we have 129 threads, 129 bits must've been set to 1.
     * 0--> 63 bit position is in bucket zero, hence
     * we just check bucket 0 with any and read it from the page as a whole.
     * 64 --> 127 will be in bucket 1 and again we can read it as a whole.
     * bits 128 --> 129 (129th and 130th position since we started from 0),
     * will be in bucket 2, therefore we need to explicitly check for bucket 2,
     * position 0 and 1.
     */

    /* Find how many buckets in the bitset array are fully set. */
    uint number_of_used_bitset_buckets = number_of_threads / BPOOL_BITSET_SIZE;
    /* For the last element, find out how many bits are set. */
    uint remainder_bitset_array_size = number_of_threads % BPOOL_BITSET_SIZE;
    /* Loop over all pages on disk. */
    for (BlockNumber i = 0; i < num_blocks_on_disk; i++) {
        BufferDesc *buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, i}, LW_SHARED);
        BufBlock block = (BufBlock)buf->GetPage();

        for (uint i = 0; i <= number_of_used_bitset_buckets; i++) {
            uint block_offset_start = i * BPOOL_BITSET_SIZE + BPOOL_PAGE_STRING_END_OFFSET;
            uint64 *bitmask_bucket;
            bitmask_bucket = (uint64 *)(block + block_offset_start);
            std::bitset<BPOOL_BITSET_SIZE> bit_set_in_page = std::bitset<BPOOL_BITSET_SIZE>(*bitmask_bucket);
            if (i != number_of_used_bitset_buckets) {
                ASSERT_TRUE(bit_set_in_page.all());
            }
            /* If at the last bucket and the bucket is not whole, then check each position. */
            else if (i == number_of_used_bitset_buckets && remainder_bitset_array_size > 0) {
                for (uint offset = 0; offset < remainder_bitset_array_size; offset++) {
                    ASSERT_EQ(bit_set_in_page[offset], 1);
                }
            }
        }

        /* Unlock and release. */
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    return;
}

/*
 * This test for RandomBufferpoolAccess is quick enough to be run every time,
 * as a part of the default test suite.
 * See RandomBufferpoolAccess for detailed comments.
 */
TEST_F(UTBufferPoolTest, DISABLED_RandomBufferpoolAccessQuick)
{
    const uint num_blocks_on_disk = 1000;
    const uint bufferpool_size = 300;
    const uint number_of_threads = 5;

    RandomBufferpoolAccess(num_blocks_on_disk, bufferpool_size, number_of_threads);
}

/*
 * This RandomBufferpoolAccess test uses many threads and should be run only on demand
 * using "--gtest_also_run_disabled_tests" option.
 * See RandomBufferpoolAccess for detailed comments.
 * This testcase takes around 106 seconds in our tests.
 */
TEST_F(UTBufferPoolTest, DISABLED_RandomBufferpoolAccessStressTest)
{
    const uint num_blocks_on_disk = 1000;
    const uint bufferpool_size = 300;
    const uint number_of_threads = 500;

    RandomBufferpoolAccess(num_blocks_on_disk, bufferpool_size, number_of_threads);
}

/*
 * Used by the HotPagesConcurrent test case. This function
 * serves as the the main thread function for the test case.
 * Each thread will read number_of_pages_to_hit pages and will
 * wait for all threads to finish reading as well. This will
 * ensure that the pages are moved the hot lru. It will then
 * verify that the pages it read are indeed hot.
 */
void UTBufferPoolTest::multithread_hot_pages(const TestThreadContext &thd_ctx, const int number_of_pages_to_hit,
                                             std::set<BufferDesc *> *buf_set)
{
    BufMgr *bufmgr = thd_ctx.bufmgr;
    FileId fileId = thd_ctx.fileId;

    /* Create a backend thread in order to access the buffer pool. */
    create_thread_and_register();

    for (BlockNumber blockNumber = 0; blockNumber < number_of_pages_to_hit; blockNumber++) {
        BufferDesc *buf = bufmgr->
            Read(g_defaultPdbId, {(uint16)fileId, blockNumber}, LW_EXCLUSIVE);
        ASSERT_NE(buf, nullptr);
        /* Save the pointer to the data */
        buf_set->insert(buf);
        /* Update the page */
        update_page(buf);
        bufmgr->MarkDirty(buf);
        bufmgr->UnlockContent(buf);
        bufmgr->Release(buf);
    }

    /* De-register the backend thread to avoid leaks */
    unregister_thread();

    return;
}

/*
 * Goal: To test that multiple threads hitting the same
 *       pages triggers the pages to move to the hot lru.
 *
 * 1. Setup bufferpool.
 * 2. Start at BPOOL_MULTITHREAD_NUM_OF_THREADS > BPOOL_LRU_MAX_USAGE
 *    threads. We require it the inequality to ensure that pages are moved
 *    to the hot lru.
 * 3. Each thread will read number_of_pages_to_hit pages. The first
 *    thread will add them to a set so we know that it's been visited.
 * 4. After ensuring all threads have read (using a barrier), verify
 *    that all the pages we read were are in the hot list. There should
 *    not be any other pages, therefore if a page isn't in the set and
 *    somehow we read it, then there is an issue.
 * 5. Remove the read page from the set. At the end, the set should have
 *    size zero, if not, then there is a problem.
 *
 * Test Coverage: BufMgr::read(), LruNode::is_in_hot_list()
 */
TEST_F(UTBufferPoolTest, DISABLED_HotPagesConcurrent)
{
    /* Set how many pages to check */
    const uint number_of_pages_to_hit = 5;

    /* Prepare the bufferpool with all default parameters. */
    prepare_buffer_pool();

    /*
     * In order to ensure that pages move to the hot lru,
     * the number of threads needs to be greater than
     * BPOOL_LRU_MAX_USAGE.
     */
    ASSERT_GT(BPOOL_MULTITHREAD_NUM_OF_THREADS, BPOOL_LRU_MAX_USAGE);

    /* Declare BPOOL_MULTITHREAD_NUM_OF_THREADS and contexts.*/
    std::thread threads[BPOOL_MULTITHREAD_NUM_OF_THREADS];
    TestThreadContext thd_ctx[BPOOL_MULTITHREAD_NUM_OF_THREADS];

    /* Set to store BufferDesc pointers */
    std::set<BufferDesc *> buf_set;

    /*
     * Create BPOOL_MULTITHREAD_NUM_OF_THREADS threads and assign them
     * the function pointer along witdowh their respected thread context. One
     * the threads will need to set the ref counts. In this case, it will
     * the last one in the iteration.
     */
    for (Size i = 0; i < BPOOL_MULTITHREAD_NUM_OF_THREADS; i++) {
        TestThreadContext ctx = thd_ctx[i];
        ctx.bufmgr = m_bufmgr;
        ctx.fileId = m_fileId;
        threads[i] = std::thread(UTBufferPoolTest::multithread_hot_pages, ctx, number_of_pages_to_hit, &buf_set);
    }

    /* We need to wait for all threads to gracefully finish */
    wait_for_threads_to_finish(threads, BPOOL_MULTITHREAD_NUM_OF_THREADS);
    /*
     * Once all the threads are done, we need to
     * check that the buffers read/updated are in
     * the hot lru and remove them from our set.
     */
    for (BlockNumber blockNumber = 0; blockNumber < number_of_pages_to_hit; blockNumber++) {
        BufferDesc *buf = m_bufmgr->
            Read(g_defaultPdbId, {(uint16)m_fileId, blockNumber}, LW_SHARED);
        ASSERT_NE(buf, nullptr);
        /* Validate that we've seen this data before */
        ASSERT_NE(buf_set.find(buf), buf_set.end());
        /* Validate that the buffer is in hot lru */
        ASSERT_TRUE(is_in_hot_list(buf));
        /* Remove it from the list once we hit it */
        buf_set.erase(buf);
        m_bufmgr->UnlockContent(buf);
        m_bufmgr->Release(buf);
    }

    /* Verify that all pages were removed */
    ASSERT_EQ(buf_set.size(), 0);
}

/*
 * Goal: test checksum of a singleblock
 *
 * 1) Setup bufferpool with BPOOL_DEFAULT_BUFFERPOOL_SIZE
 *    blocks in memory and 1 blocks on disk;
 * 2) Read one page from disk to fill out the bufferpool.
 * 3) Read the one page directly from disk and validate checksum.
 *
 * Test coverage: BufMgr::read().
 */
TEST_F(UTBufferPoolTest, ReadSingleBlockCheckCrc)
{
    prepare_buffer_pool(BPOOL_DEFAULT_BUFFERPOOL_SIZE, 1, BPOOL_DEFAULT_LRU_PARTITIONS);
    BlockNumber blocknum = 0;
    BufferDesc *buf = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
    ASSERT_NE(buf, nullptr);
    /* Compare buffer in memory with data on disk directly */
    compare_buffer_with_disk(buf);
    /* Validate that buffer contains formatted PageId */
    validate_buffer(buf);
    Page *page = buf->GetPage();
    ASSERT_EQ(page->CheckPageCrcMatch(), true);

    page->SetGlsn(1);
    int rc = m_vfs->WritePageSync({m_fileId, 0}, page);
    ASSERT_EQ(rc, 0);
    rc = m_vfs->Fsync(m_fileId);
    ASSERT_EQ(rc, 0);
    m_bufmgr->UnlockAndRelease(buf);

    BufferDesc *buf2 = m_bufmgr->Read(g_defaultPdbId, {m_fileId, blocknum}, LW_SHARED);
    ASSERT_NE(buf2, nullptr);
    /* Compare buffer in memory with data on disk directly */
    compare_buffer_with_disk(buf2);
    /* Validate that buffer contains formatted PageId */
    validate_buffer(buf2);
    Page *page2 = buf->GetPage();
    ASSERT_EQ(page2->CheckPageCrcMatch(), false);
    m_bufmgr->UnlockAndRelease(buf2);
}