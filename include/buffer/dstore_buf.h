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

#ifndef DSTORE_BUF_H
#define DSTORE_BUF_H

#include <atomic>
#include <sys/time.h>
#include "common/log/dstore_log.h"
#include "page/dstore_page.h"
#include "lock/dstore_lwlock.h"
#include "framework/dstore_thread.h"

namespace DSTORE {

namespace Buffer {
/*
 * Buffer state is a single 64-bit variable where following data is combined.
 *
 * - [0-17]  bits refcount
 * - [18-63] bits buffer flags
 *
 * Combining these values allows to perform some operations without locking
 * the buffer header, by modifying them together with a CAS loop.
 *
 * The definition of buffer state components is below.
 *
 * Note: BUF_TAG_VALID essentially means that there is a buffer hashtable
 * entry associated with the buffer's bufTag.
 */
enum BufFlagBit : uint8 {
    /* left shifting 18 - 45 bits are reserved for single node BufMgr flags */
    BUF_LOCKED_BIT = 32,
    BUF_CONTENT_DIRTY_BIT = 33,
    BUF_VALID_BIT = 34,
    BUF_TAG_VALID_BIT = 35,
    BUF_IO_IN_PROGRESS_BIT = 36,
    BUF_IO_ERROR_BIT = 37,
    BUF_HINT_DIRTY_BIT = 38,
    BUF_CR_PAGE_BIT = 40,
    BUF_IS_WRITING_WAL_BIT = 41,
    BUF_OWNED_BY_ME_BIT = 46,
    BUF_READ_AUTHORITY_BIT = 47,
    BUF_GRANT_READ_AUTH_IN_PROGRESS_BIT = 48,
    BUF_PAST_IMAGE_BIT = 49,
    BUF_PO_SHARE_NO_READ_COPY_BIT = 50,
    BUF_REPLAY_IN_PROGRESS_BIT = 51,
    BUF_READ_AUTHORITY_LOCKLESS_BIT = 52,
};

constexpr uint32 BUF_REFCOUNT_BIT_NUM = 32;
constexpr uint64 BUF_REFCOUNT_ONE = 1;
constexpr uint64 BUF_REFCOUNT_MASK = ((1ULL << 32) - 1);
constexpr uint64 BUF_FLAG_MASK = ((1ULL << 52) - 1);
;

/* buffer header is locked */
constexpr uint64 BUF_LOCKED = (1ULL << BUF_LOCKED_BIT);
/* the content in the page needs writing */
constexpr uint64 BUF_CONTENT_DIRTY = (1ULL << BUF_CONTENT_DIRTY_BIT);
/* data is valid */
constexpr uint64 BUF_VALID = (1ULL << BUF_VALID_BIT);
/* bufTag is assigned */
constexpr uint64 BUF_TAG_VALID = (1ULL << BUF_TAG_VALID_BIT);
/* read or write in progress */
constexpr uint64 BUF_IO_IN_PROGRESS = (1ULL << BUF_IO_IN_PROGRESS_BIT);
/* previous I/O failed */
constexpr uint64 BUF_IO_ERROR = (1ULL << BUF_IO_ERROR_BIT);
/*
 * the hint in the page is dirtied since last write start.
 * the caller may have only share-lock instead of exclusive-lock on the buffer to update hint flag.
 * NOTE: it can not guarantee that the change is always flushed to disk (due to race condition),
 * so it cannot be used for important changes.
 */
constexpr uint64 BUF_HINT_DIRTY = (1ULL << BUF_HINT_DIRTY_BIT);
/* indicate buffer cache CR page */
constexpr uint64 BUF_CR_PAGE = (1ULL << BUF_CR_PAGE_BIT);
/* indicate the page is in wal write */
constexpr uint64 BUF_IS_WRITING_WAL = (1ULL << BUF_IS_WRITING_WAL_BIT);
/* pls mod GetBufSingleNodeFlagString and vars below if new flag is added */

constexpr uint64 BUF_READ_AUTHORITY_LOCKLESS = (1ULL << BUF_READ_AUTHORITY_LOCKLESS_BIT);

/*
 * Only refcount (low 32 bits) can't be clear, the high 32 bits should be clear
 */
constexpr uint64 BUF_FLAG_RESET_MASK = 0x0FFFFFFFF;

/* combine all flags which single BufMgr will use */
constexpr uint64 BUF_ALL_SINGLE_FLAGS =
    (BUF_LOCKED | BUF_CONTENT_DIRTY | BUF_VALID | BUF_TAG_VALID | BUF_IO_IN_PROGRESS | BUF_IO_ERROR | BUF_HINT_DIRTY |
     BUF_CR_PAGE | BUF_IS_WRITING_WAL);
static_assert((BUF_ALL_SINGLE_FLAGS & BUF_FLAG_MASK) == BUF_ALL_SINGLE_FLAGS, "some flags is out of flag bits range.");
constexpr uint64 BUF_ALL_SINGLE_FLAGS_ARRAY[] = {BUF_LOCKED,         BUF_CONTENT_DIRTY, BUF_VALID,      BUF_TAG_VALID,
                                                 BUF_IO_IN_PROGRESS, BUF_IO_ERROR,      BUF_HINT_DIRTY, BUF_CR_PAGE,
                                                 BUF_IS_WRITING_WAL};
constexpr uint8 BUF_ALL_SINGLE_FLAGS_NUM =
    (sizeof(BUF_ALL_SINGLE_FLAGS_ARRAY) / sizeof((BUF_ALL_SINGLE_FLAGS_ARRAY)[0]));
constexpr uint8 BUF_FLAG_MAX_PRINT_ONE_ROW = 8;

/* Am I the owner of the page? */
constexpr uint64 BUF_OWNED_BY_ME = 1ULL << static_cast<int>(BUF_OWNED_BY_ME_BIT);
/* PO: the page has granted read authority to others;
non-PO: has been granted read authority. */
constexpr uint64 BUF_READ_AUTHORITY = 1ULL << static_cast<int>(BUF_READ_AUTHORITY_BIT);
/* One worker thread is trying to grant read authority to this page. */
constexpr uint64 BUF_GRANT_READ_AUTH_IN_PROGRESS = 1ULL << static_cast<int>(BUF_GRANT_READ_AUTH_IN_PROGRESS_BIT);
/* Indicate if the page is a past image. */
constexpr uint64 BUF_PAST_IMAGE = 1ULL << static_cast<int>(BUF_PAST_IMAGE_BIT);
/* PO hasn't shared any read copy so it can still directly write to this page. */
constexpr uint64 BUF_PO_SHARE_NO_READ_COPY = 1ULL << static_cast<int>(BUF_PO_SHARE_NO_READ_COPY_BIT);
/* Indicate the buffer is under WAL replay locally or somewhere else in the cluster. */
constexpr uint64 BUF_REPLAY_IN_PROGRESS = 1ULL << static_cast<int>(BUF_REPLAY_IN_PROGRESS_BIT);

/* combine all flags which multi BufMgr will use */
constexpr uint64 BUF_ALL_MULTI_FLAGS = (BUF_OWNED_BY_ME | BUF_READ_AUTHORITY | BUF_GRANT_READ_AUTH_IN_PROGRESS |
                                        BUF_PAST_IMAGE | BUF_PO_SHARE_NO_READ_COPY | BUF_REPLAY_IN_PROGRESS);
constexpr uint64 BUF_ALL_MULTI_FLAGS_ARRAY[] = {
    BUF_OWNED_BY_ME, BUF_READ_AUTHORITY,        BUF_GRANT_READ_AUTH_IN_PROGRESS,
    BUF_PAST_IMAGE,  BUF_PO_SHARE_NO_READ_COPY, BUF_REPLAY_IN_PROGRESS};
constexpr uint8 BUF_ALL_MULTI_FLAGS_NUM = (sizeof(BUF_ALL_MULTI_FLAGS_ARRAY) / sizeof((BUF_ALL_MULTI_FLAGS_ARRAY)[0]));

} /* namespace Buffer */

/* Request Types */
enum BufRpcMessageType : uint8 {
    BUF_RPC_MESSAGE_INIT = 0,
    BUF_RPC_RESPONSE_ERROR,

    BUF_RPC_REQUEST_PAGE_TO_PD_OWNER,              /* sent from RN to PD when RN != PD */
    BUF_RPC_RELEASE_PAGE_TO_PD_OWNER,              /* sent from RN to PD when RN != PD */
    BUF_RPC_REQUEST_PAGE_TO_PAGE_OWNER,            /* sent from PD to PO when RN != PD != PO */
    BUF_RPC_REVOKE_READ_AUTH_BUFFER,               /* sent from PD when the read granted pages should be invalidated
                                                      across the cluster */
    BUF_RPC_REQUEST_BATCH_CREATE_PD_ENTRY,
    BUF_RPC_RESPONSE_BATCH_CREATE_PD_ENTRY,
    BUF_RPC_REQUEST_FLUSH_PAGE_TO_PD_OWNER,        /* sent from RN to PD to flush the buffer */
    BUF_RPC_REQUEST_FLUSH_PAGE_TO_PO_OWNER,        /* sent from RN to PO to flush the buffer */
    BUF_RPC_RETURN_PAGE_TO_PD_OWNER,               /* sent from PO to PD to return page */

    /* 11 - 20 */
    BUF_RPC_RETURN_NO_PAGE_TO_PD_OWNER,            /* sent from PO to PD to let PD load the page */

    BUF_RPC_RESPONSE_PAGE_REPLIED,                 /* 8K page replied from PO back to RN when RN != PO */
    BUF_RPC_RESPONSE_PAGE_REPLIED_LATCH_RELEASED,  /* PD->RN response with 8K page and global latch released */

    BUF_RPC_RESPONSE_PAGE_OWNER,                   /* Return Page Owner Node ID from PD back to RN. */
    BUF_RPC_RESPONSE_RELEASE_PAGE_TO_PD_OWNER,     /* replied from PD back to RN when it release Page & Bucket lock
                                                      in PD */
    BUF_RPC_RESPONSE_PO_FLUSH_PAGE,                /* replied from PO back to RN when the buffer is flushed by PO */
    BUF_RPC_RESPONSE_OBSOLETE_PAGE,                /* replied from PD back to RN when the buffer is obsolete */

    BUF_RPC_RESPONSE_REVOKE_READ_AUTH_BUFFER,      /* replied from each node to PD that broadcast */
    BUF_RPC_REQUEST_PAGE_COPY_FOR_RECOVERY,        /* sent to each node of the cluster for a copy of its page */

    BUF_RPC_GRANT_PD_READ_AUTH_LOCK,               /* Lock PD latch exclusively for granting PD read authority. */
    /* 21 - 30 */
    BUF_RPC_GRANT_PD_READ_AUTH_UNLOCK,             /* Unlock PD latch at the end of granting PD read authority. */

    BUF_RPC_REQUEST_SET_BUFFER_INACCESSIBLE,       /* Broadcast to set buffer inaccessible before removing PD
                                                      entry. */
    BUF_RPC_RESPONSE_SET_BUFFER_INACCESSIBLE,      /* Response set buffer inaccessible succeeded. */

    /* Message types below do not contain valid buffer tags in the request/response structure. */
    BUF_RPC_RELOCATE_PD,                           /* for both add and remove node, send pd entries to the
                                                      expected owner. */
    BUF_RPC_RELOCATE_PD_SUCCEED,

    BUF_RPC_REQUEST_INVALID_BUCKETS,               /* sent from recovering node to other nodes in the cluster */
    BUF_RPC_RESPONSE_INVALID_BUCKETS,              /* replied back from other nodes in the cluster to the recovering
                                                      node */

    BUF_RPC_REQUEST_SET_BUCKETS_RIP,               /* sent from recovering node to other nodes in the cluster */
    BUF_RPC_RESPONSE_SET_BUCKETS_RIP,              /* replied back from other nodes in the cluster to the recovering
                                                      node */
    /* 31 - 40 */
    BUF_RPC_REQUEST_BUFFERPOOL_SCAN,               /* sent from recovering node to other nodes in the cluster */
    BUF_RPC_RESPONSE_BUFFERPOOL_SCAN,              /* replied back from other nodes in the cluster to the recovering
                                                      node */

    BUF_RPC_REQUEST_STORE_PD_RECOVERY_INFO,        /* sent from a healthy node to other nodes in the cluster for
                                                      recovery */
    BUF_RPC_RESPONSE_STORE_PD_RECOVERY_INFO,       /* replied from other nodes in the cluster to the sender node */

    BUF_RPC_REQUEST_RECOVERY_BATCH_INSERT,         /* sent from recovering node to other nodes in the cluster */
    BUF_RPC_RESPONSE_RECOVERY_BATCH_INSERT,        /* replied back from other nodes in the cluster to the recovering
                                                      node */

    BUF_RPC_REQUEST_SET_BUCKETS_VALID,             /* sent from recovering node to other nodes in the cluster */
    BUF_RPC_RESPONSE_SET_BUCKETS_VALID,            /* replied back from other nodes in the cluster to the recovering
                                                      node */

    BUF_RPC_RETURN_OWNERSHIP_TO_PD_FOR_CHECKPOINT, /* Issued by a checkpoint command and broadcasted to all nodes */
    BUF_RPC_RESPONSE_OWNERSHIP_FOR_CHECKPOINT_RETURNED,
    /* 41 - 50 */
    BUF_RPC_FLUSH_ALL_BUFFERS_FOR_CHECKPOINT,      /* Issued by a checkpoint command and broadcasted to all nodes */
    BUF_RPC_RESPONSE_ALL_BUFFERS_FOR_CHECKPOINT_FLUSHED,
    BUF_RPC_STORE_WAL_STREAMS_PLSN_FOR_CHECKPOINT, /* Issued by a checkpoint command and broadcasted to all nodes */
    BUF_RPC_RESPONSE_STORED_PLSNS_FOR_CHECKPOINT,
    BUF_RPC_UPDATE_CONTROL_FILES_FOR_CHECKPOINT,   /* Issued by a checkpoint command and broadcasted to all nodes */
    BUF_RPC_RESPONSE_CONTROL_FILES_UPDATED,
    BUF_RPC_BATCH_UPDATE_DIRTY_PAGE_TO_PD_OWNER,   /* sent from PO to PD for batch update dirty pages when PO != PD */
    BUF_RPC_RESPONSE_PAGE_BATCH_UPDATED,           /* replied from PD back to PO/RN batch page update completed */

    BUF_RPC_BATCH_RETURN_OWNERSHIP_TO_PD_OWNER,    /* sent from PO to PD for batch return page ownership */
    BUF_RPC_RESPONSE_OWNERSHIP_RETURNED,           /* replied from PD back to PO/RN batch after ownership returned */
    /* 51 - 60 */
    BUF_RPC_REQUEST_REMOVE_PD_ENTRY,               /* sent from RN to PD to evict the PD entry of the designated
                                                      buffer */
    BUF_RPC_RESPONSE_REMOVE_PD_ENTRY,
    BUF_RPC_PD_REQUEST_PO_EVICT,                   /* sent from PD to PO in order to evict buffers on PO when PD wants
                                                      to shrink */
    BUF_RPC_REQUEST_INVALIDATE_PAGE_BATCH,         /* Sent from PD to other nodes after modified page updated in PD */
    BUF_RPC_RESPONSE_INVALIDATE_PAGE_BATCH,

    BUF_RPC_RESPONSE_PAGE_NOT_REPLIED,             /* a valid PI page could not be replied */

    BUF_RPC_REQUEST_ABANDON_PAGE_OWNER,            /* sent from a stopping node to abandon all page ownership */
    BUF_RPC_RESPONSE_ABANDON_PAGE_OWNER,           /* replied back abandon page ownership result */

    BUF_RPC_BATCH_FLUSH_TO_PD_OWNER,               /* batch flush dirty pages to PD */
    BUF_RPC_RESPONSE_BATCH_FLUSH_TO_PD_OWNER,
    /* 61 - 70 */
    BUF_RPC_REQUEST_PD_ENTRY_LOCKS_GRANTED,         /* page directory owner node grants pd entry locks to RN */
    BUF_RPC_RESPONSE_PD_ENTRY_LOCKS_GRANTED,          /* wake up request node reponse to page directory owner */

    BUF_RPC_REQUEST_CONSTRUCT_CR_PAGE_TO_PD_OWNER,
    BUF_RPC_REQUEST_CONSTRUCT_CR_PAGE_TO_PO_OWNER,
    BUF_RPC_RESPONSE_CONSTRUCT_CR_PAGE,

    BUF_RPC_REQUEST_INVALIDATE_BUFFER_TO_PD_OWNER,
    BUF_RPC_REQUEST_INVALIDATE_BUFFER_TO_PO_OWNER,
    BUF_RPC_RESPONSE_PO_INVALIDATE_BUFFER,
    BUF_RPC_RELEASE_AND_EVICT_PDENTRY_TO_PD_OWNER,
    BUF_RPC_REQUEST_INVALIDATE_BYFILEID_TO_PO_OWNER,
    /* 71 - 80 */
    BUF_RPC_RESPONSE_PO_INVALIDATE_BYFILEID,
    BUF_RPC_REQUEST_CLEAN_CRASHED_PO,
    BUF_RPC_RESPONSE_CLEAN_CRASHED_PO_SUCCEED,
    BUF_RPC_RESPONSE_CLEAN_CRASHED_PO_FAILED,
    BUF_RPC_REQUEST_BECOME_PAGE_OWNER,
    BUF_RPC_RESPONSE_BECOME_PAGE_OWNER,
    BUF_RPC_REQUEST_ABANDON_PAGE_OWNER_BY_IDS,
    BUF_RPC_REQUEST_BUFFER_INFO_TO_PD_OWNER,
    BUF_RPC_RESPONSE_BUFFER_INFO_TO_PD_OWNER,
    BUF_RPC_REQUEST_BUFFER_INFO,
    BUF_RPC_RESPONSE_BUFFER_INFO,
    BUF_RPC_RESPONSE_BUFFER_INFO_NOT_FOUND,
    BUF_RPC_ACQUIRE_AIO_STATS,
    BUF_RPC_ACQUIRE_SLOT_STATS,
    /* 81 - 90 */
    BUF_RPC_ACQUIRE_PAGEWRITER_STATS,
    BUF_RPC_RESPONSE_AIO_STATS,
    BUF_RPC_RESPONSE_SLOT_STATS,
    BUF_RPC_RESPONSE_PAGEWRITER_STATS,

    BUF_RPC_REQUEST_MSG_MAX_COUNT                  /* the message count, this should be the last enum. */
};

#ifdef __aarch64__
constexpr uint32 BUFFER_DESC_SIZE = 256;
#else
constexpr uint32 BUFFER_DESC_SIZE = 256;
#endif

#ifdef UT
const uint64 TIMESTAMP_THRESHOLD_IN_CR = 1; /* second */
#else
const uint64 TIMESTAMP_THRESHOLD_IN_CR = 20; /* second */
#endif

using BufBlock = char *;
struct BufferDesc;
/*
 * Buffer identifiers.
 *
 * Point to the BufferDesc, NULL is invalid.
 */
#define INVALID_BUFFER_DESC (nullptr)

/* Return true if the buffer is local (not visible to other backends). */
extern bool BufferIsLocal(BufferDesc *bufferDesc);

/*
 * BufferTag indicate the disk block which the buffer is cached
 */
struct BufferTag {
    PageId pageId; /* indicate which block to read or write */
    int16 padding;
    PdbId pdbId;   /* indicate which container database the page is belong to */

    BufferTag() noexcept : pageId{INVALID_PAGE_ID}, padding{0}, pdbId{INVALID_PDB_ID}
    {}

    BufferTag(PdbId id, const PageId cachedPageId) noexcept : pageId{cachedPageId}, padding{0}, pdbId{id}
    {}

    BufferTag(const BufferTag &bufTag) noexcept : pageId{bufTag.pageId}, padding{0}, pdbId{bufTag.pdbId}
    {}

    BufferTag &operator=(const BufferTag &bufTag) noexcept
    {
        if (this == &bufTag) {
            return *this;
        }
        this->pageId = bufTag.pageId;
        this->padding = 0;
        this->pdbId = bufTag.pdbId;
        return *this;
    }

    inline bool operator==(const BufferTag &bufTag) const
    {
        return pageId == bufTag.pageId && pdbId == bufTag.pdbId;
    }

    inline bool operator!=(const BufferTag &bufTag) const
    {
        return pageId.m_fileId != bufTag.pageId.m_fileId || pageId.m_blockId != bufTag.pageId.m_blockId ||
               pdbId != bufTag.pdbId;
    }

    bool operator<(const BufferTag &bufTag) const
    {
        return pdbId < bufTag.pdbId || (pdbId == bufTag.pdbId && pageId < bufTag.pageId);
    }

    bool IsInvalid() const
    {
        return pageId == INVALID_PAGE_ID;
    }

    void SetInvalid()
    {
        pageId = INVALID_PAGE_ID;
    }

    static inline bool Match(const BufferTag *a, const BufferTag *b)
    {
        return a->pageId.m_blockId == b->pageId.m_blockId && a->pageId.m_fileId == b->pageId.m_fileId &&
               a->pdbId == b->pdbId;
    }

    StringLog ToString() const
    {
        AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_BUFFER)};
        StringLog dumpInfo;
        dumpInfo.stringData.append("(%hhu, %hu, %u)", pdbId, pageId.m_fileId, pageId.m_blockId);
        return dumpInfo;
    }
};

/*
 * size of BufferTag = PageId(6) + padding(2) + PdbId(4)
 */
static_assert(sizeof(BufferTag) == 12, "BufferTag must be packed.");
const BufferTag INVALID_BUFFER_TAG = {INVALID_PDB_ID, INVALID_PAGE_ID};
constexpr int MAX_ARRAY_SIZE = 10;

/*
 * This structure is used by each thread to record the page currently in use
 * to prevent the page from being invalidated. Under normal circumstances,
 * this is done via Pin() in the buffer descriptor.
 */
struct BufferTagArray {
    BufferTag bufTags[MAX_ARRAY_SIZE];
    DstoreSpinLock spinLock;

    void Initialize();
    void Add(const BufferTag &bufTag);
    void Remove(const BufferTag &bufTag);
    bool IsExist(const BufferTag &bufTag);
};

constexpr uint8 STAGING_BUFFER_BIT = 7;
constexpr uint16 BUF_IS_STAGING = (1 << STAGING_BUFFER_BIT);


/*
 * The info about associate CR buffer.
 * When the buffer cache base page, it has an array which the size is CR_LIST_SIZE, and it records the associated CR
 * buffer.
 * When the buffer cache CR page, it has a pointer which point to the base page buffer and the offset of the cr slot in
 * the base page buffer.
 */
union CRInfo {
    /* For base buffer */
    struct {
        BufferDesc *crBuffer;
        CommitSeqNo crPageMaxCsn;
        bool isUsable;
    };

    /* For cr buffer */
    struct {
        BufferDesc *baseBufferDesc;
    };

    /*
     * Initialize CRInfo as base buffer in default.
     */
    void InitCRInfo()
    {
        crBuffer = INVALID_BUFFER_DESC;
        crPageMaxCsn = INVALID_CSN;
        isUsable = false;
    }

    /*
     * Remove the specified CR buffer in the CRInfo.
     *
     * NOTE: the buffer should cache the base page.
     */
    void RemoveCrBuffer()
    {
        StorageAssert(crBuffer != INVALID_BUFFER_DESC);
        StorageAssert(!isUsable);
        crBuffer = INVALID_BUFFER_DESC;
        crPageMaxCsn = INVALID_CSN;
    }

    void SetCrPageMaxCsn(CommitSeqNo pageMaxCsn)
    {
        StorageAssert(crBuffer != INVALID_BUFFER_DESC);
        crPageMaxCsn = pageMaxCsn;
    }

    bool IsCrMatched(CommitSeqNo snapshotCsn) const
    {
        StorageAssert(isUsable && crBuffer != INVALID_BUFFER_DESC && crPageMaxCsn != INVALID_CSN);
        return snapshotCsn > crPageMaxCsn;
    }
};

static_assert(std::is_standard_layout<CRInfo>::value, "CRInfo must be standard layout");

enum LruNodeType : uint8 { LN_CANDIDATE = 0, LN_LRU = 1, LN_HOT = 2, LN_PENDING = 3, LN_TO_BE_INVALIDATED = 4 };

struct LruNode {
    dlist_node m_list_node;
    void *m_value;
    uint32 lruIndex;
    std::atomic<LruNodeType> m_type;
    std::atomic<uint8> m_usage;

    void InitNode(void *val)
    {
        DListNodeInit(&m_list_node);
        m_value = val;
        lruIndex = 0;
        m_type = LN_PENDING;
        m_usage = 0;
    }

    template <typename T>
    T *GetValue()
    {
        return static_cast<T *>(m_value);
    }

    uint8 IncUsage()
    {
        uint8 state = m_usage.fetch_add(1u, std::memory_order_acq_rel);
        return (state + 1);
    }

    uint8 GetUsage() const
    {
        return (m_usage.load(std::memory_order_acquire));
    }

    void ResetUsage()
    {
        m_usage.store(0u, std::memory_order_release);
    }

    bool IsInPendingState() const
    {
        return m_type == LN_PENDING;
    }

    bool IsInHotList() const
    {
        return m_type == LN_HOT;
    }

    bool IsInLruList() const
    {
        return m_type == LN_LRU;
    }

    bool IsInCandidateList() const
    {
        return m_type == LN_CANDIDATE;
    }

    bool IsInInvalidationList() const
    {
        return m_type == LN_TO_BE_INVALIDATED;
    }
};

static_assert(std::is_pod<LruNode>::value, "LruNode must be POD");

/**
 * BufferDescController is in charge of BufferDesc's internal concurrency control. We didn't put the content lock over
 * here, as it will be used by other modules, and should better be in tandem with BufferDesc so that CPU won't need
 * another memory load to fetch it.
 */
struct BufferDescController {
    LWLockPadded ioInProgressLwlock; /* to wait for I/O to complete */
    LWLockPadded crAssignLwlock;     /* cr slot assign LWlock */
    std::atomic<uint64> lastPageModifyTime;       /* Last time of modifying the page */

public:
    void InitController();

    LWLock *GetIoInProgressLwLock()
    {
        return &ioInProgressLwlock.lock;
    }

    LWLock *GetCrAssignLwLock()
    {
        return &crAssignLwlock.lock;
    }

    /*
     * Update the lastWriteTimestamp to the current time.
     *
     * Note: this method should be invoked under the protection of the buffer content lock.
     */
    inline void UpdateLastModifyTimeToNow()
    {
        uint64 now = static_cast<uint64>(time(nullptr));
        StorageAssert(lastPageModifyTime.load(std::memory_order_acquire) <= now);
        lastPageModifyTime.store(now, std::memory_order_release);
    }
};

static_assert(std::is_pod<BufferDescController>::value, "BufferDescController must be a POD");

struct PageVersion {
    uint64 glsn;
    uint64 plsn;

    PageVersion() noexcept : glsn{INVALID_WAL_GLSN}, plsn{INVALID_PLSN} {}

    PageVersion(const PageVersion &pageVersion)
        : glsn{pageVersion.glsn}, plsn{pageVersion.plsn} {}

    PageVersion(const uint64 &pageGlsn, const uint64 &pagePlsn)
        : glsn{pageGlsn}, plsn{pagePlsn} {}

    PageVersion& operator=(const PageVersion &pageVersion)
    {
        if (this == &pageVersion) {
            return *this;
        }
        glsn = pageVersion.glsn;
        plsn = pageVersion.plsn;
        return *this;
    }

    /*
     * A lsn is considered invalid as long as either the glsn or plsn is invalid.
     * When a page is created, the glsn/plsn are both 0.
     */
    bool IsValidPageVersion() const
    {
        return ((glsn != INVALID_WAL_GLSN) && (plsn != INVALID_PLSN || (plsn == 0 && glsn == 0)));
    }

    void SetInvalidPageVersion()
    {
        glsn = INVALID_WAL_GLSN;
        plsn = INVALID_PLSN;
    }

    bool IsPageVersionAllZero() const
    {
        return (glsn == 0 && plsn == 0);
    }

    bool operator>(const PageVersion &pageVersion) const
    {
        StorageAssert(pageVersion.IsValidPageVersion() && IsValidPageVersion());
        return (glsn > pageVersion.glsn || (glsn == pageVersion.glsn && plsn > pageVersion.plsn));
    }

    bool operator>=(const PageVersion &pageVersion) const
    {
        StorageAssert(pageVersion.IsValidPageVersion() && IsValidPageVersion());
        return (glsn > pageVersion.glsn || (glsn == pageVersion.glsn && plsn >= pageVersion.plsn));
    }

    bool operator<(const PageVersion &pageVersion) const
    {
        StorageAssert(pageVersion.IsValidPageVersion() && IsValidPageVersion());
        return (glsn < pageVersion.glsn || (glsn == pageVersion.glsn && plsn < pageVersion.plsn));
    }

    bool operator<=(const PageVersion &pageVersion) const
    {
        StorageAssert(pageVersion.IsValidPageVersion() && IsValidPageVersion());
        return (glsn < pageVersion.glsn || (glsn == pageVersion.glsn && plsn <= pageVersion.plsn));
    }

    bool operator==(const PageVersion &pageVersion) const
    {
        return ((glsn == pageVersion.glsn) && (plsn == pageVersion.plsn));
    }

    bool operator!=(const PageVersion &pageVersion) const
    {
        return ((glsn != pageVersion.glsn) || (plsn != pageVersion.plsn));
    }

    StringLog ToString() const
    {
        AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_BUFFER)};
        StringLog dumpInfo;
        dumpInfo.stringData.append("{(%lu, %lu)}", glsn, plsn);
        return dumpInfo;
    }
};

const PageVersion INVALID_PAGE_LSN = {INVALID_WAL_GLSN, INVALID_PLSN};

constexpr uint32 BUFFER_DESC_FORMAT_SIZE = 128;
constexpr uint32 DIRTY_PAGE_QUEUE_MAX_SIZE = 5;
constexpr uint32 DEFAULT_BGPAGEWRITER_SLOT_ID = 0;
/*
 * BufferDesc is a very hot struct, we have to pad it to cache line size. CAVEAT: DO NOT USE bufBlock DIRECTLY!
 */
struct BufferDesc : public BaseObject {
    BufBlock bufBlock; /* point to the buffer block */
    BufferTag bufTag;  /* bufTag of page contained in buffer */
    BufferDescController *controller;
    gs_atomic_uint64 state; /* state of the buffer, containing flags, refcount */
    LruNode lruNode;        /* lru node which will be linked in BufLruList */
    CRInfo crInfo;          /* Used to record associate CR buffer */
    LWLock contentLwLock;   /* to lock access to buffer contents */

    std::atomic<BufferDesc *> nextDirtyPagePtr[DIRTY_PAGE_QUEUE_MAX_SIZE]; /* next dirty page ptr in dirty page list */
    std::atomic<uint64> recoveryPlsn[DIRTY_PAGE_QUEUE_MAX_SIZE];           /* min plsn in recovery */
    PageVersion pageVersionOnDisk;              /* the glsn and plsn flush to disk. Used to check missing dirty */
    uint64 fileVersion;                         /* version of file */

    void InitBufferDesc(BufBlock block, BufferDescController *ctrler);
    void UpdatePageVersion(Page *newPage);
public:
    static inline int DirtyPagePointerSortCompare(const void *a, const void *b)
    {
        const BufferDesc *item1 = *(static_cast<const BufferDesc *const *>((a)));
        const BufferDesc *item2 = *(static_cast<const BufferDesc *const *>((b)));

        if (item1->bufTag.pdbId < item2->bufTag.pdbId) {
            return -1;
        } else if (item1->bufTag.pdbId > item2->bufTag.pdbId) {
            return 1;
        }

        if (item1->bufTag.pageId.m_fileId < item2->bufTag.pageId.m_fileId) {
            return -1;
        } else if (item1->bufTag.pageId.m_fileId > item2->bufTag.pageId.m_fileId) {
            return 1;
        }

        if (item1->bufTag.pageId.m_blockId < item2->bufTag.pageId.m_blockId) {
            return -1;
        } else if (item1->bufTag.pageId.m_blockId > item2->bufTag.pageId.m_blockId) {
            return 1;
        }
        return 0;
    }

    /*
     * Get the buffer content and reinterpret cast.
     *
     * NOTE: this method should be invoked under the protection of buffer content lock.
     */
    inline Page *GetPage(UNUSE_PARAM bool needCheck = true)
    {
        void *privateBuf = thrd->GetPrivateBuffer(this->bufTag);
        StorageAssert(!needCheck || (GsAtomicReadU64(&state) &
            (Buffer::BUF_IO_IN_PROGRESS | Buffer::BUF_CR_PAGE | Buffer::BUF_IS_WRITING_WAL | Buffer::BUF_OWNED_BY_ME |
            Buffer::BUF_READ_AUTHORITY | Buffer::BUF_PAST_IMAGE | Buffer::BUF_REPLAY_IN_PROGRESS)) ||
            LWLockHeldByMe(&this->contentLwLock) || (privateBuf != nullptr) ||
            (GsAtomicReadU64(&state) & Buffer::BUF_VALID));

        if (privateBuf == nullptr) {
            return reinterpret_cast<Page *>(bufBlock);
        }
        return static_cast<Page *>(privateBuf);
    }

    /*
     * Get the buffer page id.
     *
     * NOTE: this method should be invoked when the buffer is pinned.
     */
    inline PageId GetPageId() const
    {
        return bufTag.pageId;
    }

    /*
     * Get the buffer bufTag.
     *
     * NOTE: this method should be invoked when the buffer is pinned.
     */
    inline BufferTag GetBufferTag() const
    {
        return bufTag;
    }

    inline bool IsValidPage()
    {
        uint64 bufState = WaitHdrUnlock();
        return ((bufState & Buffer::BUF_VALID) == Buffer::BUF_VALID);
    }

    /*
     * Get the container database id.
     *
     * NOTE: this method should be invoked when the buffer is pinned.
     */
    inline PdbId GetPdbId() const
    {
        return bufTag.pdbId;
    }

    /*
     * Check if the CR page in the buffer is valid.
     *
     * NOTE:this method should be invoked when the buffer is pinned.
     */
    inline bool IsCrValid()
    {
        uint64 bufState = this->WaitHdrUnlock();

        return (bufState & (Buffer::BUF_VALID | Buffer::BUF_CR_PAGE)) == (Buffer::BUF_VALID | Buffer::BUF_CR_PAGE);
    }

    /*
     * Check if the buffer cache the CR page.
     *
     * NOTE: this method should be invoked when the buffer is pinned.
     */
    inline bool IsCrPage()
    {
        uint64 bufState = this->WaitHdrUnlock();
        return (bufState & Buffer::BUF_CR_PAGE) ? true : false;
    }

    inline void SetCrPage()
    {
        uint64 bufState = LockHdr();
        bufState |= Buffer::BUF_CR_PAGE;
        UnlockHdr(bufState);
    }

    inline BufferDesc *GetCrBuffer()
    {
        StorageAssert(!(GsAtomicReadU64(&state) & Buffer::BUF_CR_PAGE));
        StorageAssert(IsCrAssignLocked());
        return crInfo.crBuffer;
    }

    /*
     * Get associated the buffer which cache the base page.
     *
     * NOTE: this method should be called when the buffer cache a CR page and the buffer should be pinned.
     */
    inline BufferDesc *GetCrBaseBuffer()
    {
        StorageAssert(state & Buffer::BUF_CR_PAGE);
        StorageAssert(IsPinnedPrivately());
        return crInfo.baseBufferDesc;
    }
    inline void SetBaseBuffer(BufferDesc *baseBuf)
    {
        crInfo.baseBufferDesc = baseBuf;
    }
    inline void SetCrBuffer(BufferDesc *crBuf)
    {
        crInfo.crBuffer = crBuf;
    }

    inline void SetCrUnusable()
    {
        crInfo.isUsable = false;
    }

    inline void SetCrUsable()
    {
        crInfo.isUsable = true;
    }

    inline bool IsCrUsable() const
    {
        return crInfo.isUsable;
    }

    inline void UpdateLastModifyTimeToNow() const
    {
        controller->UpdateLastModifyTimeToNow();
    }

    inline uint64 GetLastModifyTime() const
    {
        return controller->lastPageModifyTime.load(std::memory_order_acquire);
    }

    inline void AcquireCrAssignLwlock(LWLockMode mode)
    {
        DstoreLWLockAcquireByMode(controller->GetCrAssignLwLock(), mode)
    }

    inline bool TryAcquireCrAssignLwlock(LWLockMode mode)
    {
        return DstoreLWLockConditionalAcquire(controller->GetCrAssignLwLock(), mode);
    }

    inline void ReleaseCrAssignLwlock()
    {
        StorageAssert(LWLockHeldByMe(controller->GetCrAssignLwLock()));
        LWLockRelease(controller->GetCrAssignLwLock());
    }

    inline bool IsCrAssignLocked()
    {
        return LWLockHeldByMe(controller->GetCrAssignLwLock());
    }

    inline bool IsCrAssignLocked(LWLockMode mode)
    {
        return LWLockHeldByMeInMode(controller->GetCrAssignLwLock(), mode);
    }

    inline bool IsHeldContentLockByMe()
    {
        return LWLockHeldByMe(&this->contentLwLock);
    }

    inline bool IsContentLocked(LWLockMode mode)
    {
        return LWLockHeldByMeInMode(&this->contentLwLock, mode);
    }

    /*
     * Indicate if the buffer has CR buffer in the CRList.
     *
     * NOTE: the buffer must cache base page and {is_cr_page} and {is_cr_valid} are both false.
     */
    inline bool HasCrBuffer()
    {
        StorageAssert(IsCrAssignLocked());
        uint64 bufState = GsAtomicReadU64(static_cast<volatile uint64 *>(&state));
        bool isBaseBuf = (bufState & Buffer::BUF_CR_PAGE) ? false : true;
        return isBaseBuf && (crInfo.crBuffer != INVALID_BUFFER_DESC);
    }

    /*
     * Get the buffer state when no one hold the buffer header lock
     */
    inline uint64 GetState(bool waitLock = true)
    {
        if (waitLock) {
            return this->WaitHdrUnlock();
        } else {
            return GsAtomicReadU64(&state);
        }
    }

    inline bool IsPageOwner()
    {
        uint64 bufState = this->GetState(false);
        return (bufState & Buffer::BUF_OWNED_BY_ME) != 0;
    }

    inline bool HasReadAuth()
    {
        uint64 bufState = this->GetState(false);
        return (bufState & Buffer::BUF_READ_AUTHORITY) != 0;
    }

    inline bool IsPageDirty()
    {
        uint64 bufState = this->GetState();
        return ((bufState & Buffer::BUF_CONTENT_DIRTY) || (bufState & Buffer::BUF_HINT_DIRTY));
    }

    inline uint64 GetFileVersion()
    {
        if (fileVersion == INVALID_FILE_VERSION) {
            ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("Invalidate file version."));
        }
        return fileVersion;
    }

    inline void SetFileVersion(uint64 version)
    {
        fileVersion = version;
    }

    /*
     * Reset CRInfo as base buffer.
     *
     * NOTE: When the buffer cache base page, the CRInfo should be reset as base buffer.
     */
    void ResetAsBaseBuffer()
    {
        crInfo.crBuffer = INVALID_BUFFER_DESC;
        crInfo.crPageMaxCsn = INVALID_CSN;
        crInfo.isUsable = false;
    }

    /*
     * Reset CRInfo as CR buffer.
     *
     * NOTE: When the buffer cache CR page, the CRInfo should be reset as CR buffer.
     */
    void ResetAsCrBuffer()
    {
        crInfo.baseBufferDesc = INVALID_BUFFER_DESC;
    }

    void SetValid()
    {
        uint64 bufState = LockHdr();
        bufState |= Buffer::BUF_VALID;
        UnlockHdr(bufState);
    }

    void SetInvalid()
    {
        uint64 bufState = LockHdr();
        bufState &= ~(Buffer::BUF_VALID);
        UnlockHdr(bufState);
    }

    bool IsInDirtyPageQueue(const int64 slotId) const;

    void SetPageIsWritingWal();
    void SetPageEndWriteWal();
    void WaitIfIsWritingWal();
    void WaitIfIoInProgress();
    void InvalidateCrPage();

    void PrintBufferDesc(char *str, Size maxSize) const;
    char *PrintBufferDesc();
    void PrintBufferState(uint64 bufferState, StringInfoData *dumpInfo);

    uint64 WaitHdrUnlock();
    bool FastLockHdrIfReusable(const BufferTag &inBufTag, bool justValidTag,
        bool ignoreDirtyPage = false);
    uint64 LockHdr();
    void UnlockHdr(uint64 flags);
    bool IsHdrLocked();
    template <bool isGlobalTempTable = false>
    void Pin();
    void PinUnderHdrLocked();
    template <bool isGlobalTempTable = false>
    void Unpin();
    bool IsPinnedPrivately();
    uint64 GetRefcount();
    void ClearDirtyState(uint64 flags);
    void PinForAio();
    void UnpinForAio();

private:
    void SharedPin();
    void SharedUnpin();
    const char *GetBufSingleNodeFlagString(uint64 bufFlagBit) const;
    const char *GetBufMultiNodeFlagString(uint64 bufFlagBit) const;
    void PrintBufSingleFlagByState(uint64 bufferState, StringInfoData *dumpInfo, uint8 *flagCnt);
} __attribute__((aligned(DSTORE_CACHELINE_SIZE)));

class BufDescVector : public BaseObject {
public:
    BufDescVector() : m_size(0), m_capacity(BUFFER_DESC_VECTOR_DEFAULT_CAPACITY), m_bufDescs(nullptr), m_memCtx(nullptr)
    {}

    ~BufDescVector() {}

    RetStatus Init(DstoreMemoryContext memCtx)
    {
        m_memCtx = memCtx;
        AutoMemCxtSwitch autoMemCxtSwitch(memCtx);
        m_bufDescs = static_cast<BufferDesc **>(DstorePalloc(sizeof(BufferDesc *) * m_capacity));
        if (STORAGE_VAR_NULL(m_bufDescs)) {
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("OOM! capacity: %u", m_capacity));
            return DSTORE_FAIL;
        }
        return DSTORE_SUCC;
    }

    void Destroy()
    {
        DstorePfreeExt(m_bufDescs);
    }

    inline uint32 Size() const
    {
        return m_size;
    }

    RetStatus Push(BufferDesc *bufDesc)
    {
        AutoMemCxtSwitch autoMemCxtSwitch(m_memCtx);
        if (unlikely(m_size == m_capacity)) {
            m_capacity = m_capacity << 1;
            BufferDesc **bufDescs = static_cast<BufferDesc **>(DstorePalloc(sizeof(BufferDesc *) * m_capacity));
            if (bufDescs == nullptr) {
                ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("OOM! capacity: %u", m_capacity));
                return DSTORE_FAIL;
            }
            errno_t rc = memcpy_s(bufDescs, sizeof(BufferDesc *) * m_size, m_bufDescs, sizeof(BufferDesc *) * m_size);
            storage_securec_check(rc, "\0", "\0");
            DstorePfree(m_bufDescs);
            m_bufDescs = bufDescs;
        }

        m_bufDescs[m_size++] = bufDesc;
        return DSTORE_SUCC;
    }

    void Clear()
    {
        m_size = 0;
    }

    BufferDesc *operator[](uint32 idx)
    {
        StorageReleasePanic(idx >= m_size, MODULE_BUFMGR, ErrMsg("Out of range, idx: %u, size: %u", idx, m_size));
        return m_bufDescs[idx];
    }

private:
    uint32 m_size;
    uint32 m_capacity;
    BufferDesc **m_bufDescs;
    DstoreMemoryContext m_memCtx;
    static constexpr uint8 BUFFER_DESC_VECTOR_DEFAULT_CAPACITY = 8;
};

struct BufDescPrintInfo {
    uint16 fileId;
    uint32 blockId;
    PdbId pdbId;
    uint64 lastPageModifyTime;
    uint32 lruIndex;
    uint8 lruNodeType;
    uint8 lruNodeUsage;
    int64 lruNodeUnevictCnt;
    uint64 recoveryPlsn;
    uint64 refCount;
    bool isLocked;
    bool isContentDirty;
    bool isValid;
    bool isTagValid;
    bool ioInProgress;
    bool ioError;
    bool isHintDirty;
    bool isCrPage;
    bool isWritingWal;
    bool isOwnedByMe;
    bool hasReadAuthority;
    bool isRAInProgress;
    bool isPastImage;
    bool isPoHasReadCopy;
    bool isReplayInProgress;
    uint64 glsnOnDisk;
    uint64 plsnOnDisk;
    uint64 fileVersion;

public:
    void Init(BufferDesc *bufferDesc)
    {
        fileId = bufferDesc->bufTag.pageId.m_fileId;
        blockId = bufferDesc->bufTag.pageId.m_blockId;
        pdbId = bufferDesc->bufTag.pdbId;
        lastPageModifyTime = bufferDesc->controller->lastPageModifyTime.load(std::memory_order_acquire);
        lruIndex = bufferDesc->lruNode.lruIndex;
        lruNodeType = static_cast<uint8>(bufferDesc->lruNode.m_type);
        lruNodeUsage = bufferDesc->lruNode.m_usage;
        lruNodeUnevictCnt = 0;
        /* recoveryPlsn is array for standby, we print element in index 0 simply */
        recoveryPlsn = bufferDesc->recoveryPlsn[DEFAULT_BGPAGEWRITER_SLOT_ID].load();
        refCount = bufferDesc->GetRefcount();
        isLocked = bufferDesc->state & Buffer::BUF_LOCKED ? true : false;
        isContentDirty = bufferDesc->state & Buffer::BUF_CONTENT_DIRTY ? true : false;
        isValid = bufferDesc->state & Buffer::BUF_VALID ? true : false;
        isTagValid = bufferDesc->state & Buffer::BUF_TAG_VALID ? true : false;
        ioInProgress = bufferDesc->state & Buffer::BUF_IO_IN_PROGRESS ? true : false;
        ioError = bufferDesc->state & Buffer::BUF_IO_ERROR ? true : false;
        isHintDirty = bufferDesc->state & Buffer::BUF_HINT_DIRTY ? true : false;
        isCrPage = bufferDesc->state & Buffer::BUF_CR_PAGE ? true : false;
        isWritingWal = bufferDesc->state & Buffer::BUF_IS_WRITING_WAL ? true : false;
        isOwnedByMe = bufferDesc->state & Buffer::BUF_OWNED_BY_ME ? true : false;
        hasReadAuthority = bufferDesc->state & Buffer::BUF_READ_AUTHORITY ? true : false;
        isRAInProgress = bufferDesc->state & Buffer::BUF_GRANT_READ_AUTH_IN_PROGRESS ? true : false;
        isPastImage = bufferDesc->state & Buffer::BUF_PAST_IMAGE ? true : false;
        isPoHasReadCopy = bufferDesc->state & Buffer::BUF_PO_SHARE_NO_READ_COPY ? true : false;
        isReplayInProgress = bufferDesc->state & Buffer::BUF_REPLAY_IN_PROGRESS ? true : false;
        glsnOnDisk = bufferDesc->pageVersionOnDisk.glsn;
        plsnOnDisk = bufferDesc->pageVersionOnDisk.plsn;
        fileVersion = bufferDesc->fileVersion;
    }
};

union PointerToAddress {
    uint64 address;
    void *pointer;
};

static_assert(sizeof(BufferDesc) == BUFFER_DESC_SIZE && sizeof(BufferDesc) % DSTORE_CACHELINE_SIZE == 0,
              "BufferDesc size doesn't align to cache line.");

struct PrivateBufferEntry {
    BufferTag bufTag;
    void *addr;
};

enum BufferAccessType : uint8 {
    BAS_NORMAL,    /* Normal random access */
    BAS_BULKREAD,  /* Large read-only scan (hint bit updates are
                    * ok) */
    BAS_BULKWRITE, /* Large multi-block write (e.g. COPY IN) */
    BAS_VACUUM,    /* VACUUM */
    BAS_REPAIR     /* repair file */
};

struct BufferRingData {
    BufferAccessType accessType;
    int ringSize;
    int curPos;
    int flushRate; /* Reserved field, which is not used currently. */
    bool curWasInRing;
    BufferDesc *bufferDescArray[DSTORE_FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE SIZE ARRAY */
    void Init(BufferAccessType type, int size)
    {
        accessType = type;
        ringSize = size;
        curPos = -1;
        curWasInRing = false;
    }
    
    BufferDesc *GetFreeBufFromRing();
    void AddFreeBufToRing(BufferDesc* bufDesc);
    void RemoveBufInRing(BufferDesc* bufDesc);
};

/*
 * Buffer access strategy objects.
 */
using BufferRing = struct BufferRingData *;

extern BufferRing CreateBufferRing(BufferAccessType strategyType);
extern void DestroyBufferRing(BufferRing *strategyObj);

} /* namespace DSTORE */

#endif
