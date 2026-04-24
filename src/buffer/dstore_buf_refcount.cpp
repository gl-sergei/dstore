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

#include "buffer/dstore_buf_refcount.h"
#include "securec.h"
#include "common/algorithm/dstore_string_info.h"
#include "common/log/dstore_log.h"
#include "buffer/dstore_buf_mgr.h"
#include "buffer/dstore_buf.h"
#include "framework/dstore_instance.h"
#include "page/dstore_page.h"

namespace DSTORE {

BufPrivateRefCount::BufPrivateRefCount()
    : m_private_refcount_array{},
      m_private_refcount_hash(nullptr),
      m_private_refcount_overflowed(0),
      m_private_refcount_clock(0)
{
    for (uint32 i = 0; i < Buffer::SHARED_PIN_NUM_PARTITIONS; ++i) {
        m_lastDeferredSlots[i] = -1;
    }
}

PrivateRefCountEntry *BufPrivateRefCount::PGetPrivateRefcountEntryFast(BufferDesc *bufferDesc,
                                                                       PrivateRefCountEntry *&freeEntry)
{
    PrivateRefCountEntry *res = nullptr;
    uint32 i;

    StorageAssert(bufferDesc != INVALID_BUFFER_DESC);

    /*
     * First search for references in the array, that'll be sufficient in the
     * majority of cases.
     */
    for (i = 0; i < REFCOUNT_ARRAY_ENTRIES; i++) {
        res = &m_private_refcount_array[i];

        if (res->buffer == bufferDesc) {
            return res;
        }

        /* Remember where to put a new refcount, should it become necessary. */
        if (freeEntry == nullptr && res->buffer == INVALID_BUFFER_DESC) {
            freeEntry = res;
        }
    }
    return nullptr;
}

/*
 * Move an entry from the refcount array to refcount hashtable based on refcount clock.
 */
PrivateRefCountEntry *BufPrivateRefCount::MoveEntryToHashTable(bool found)
{
    PrivateRefCountEntry *arrayEnt = nullptr;
    PrivateRefCountEntry *hashEnt = nullptr;
    int32 retryCnt = 0;
    constexpr int32 retryMax = 10000;

    /* select victim slot */
    arrayEnt = &m_private_refcount_array[m_private_refcount_clock++ % REFCOUNT_ARRAY_ENTRIES];
    StorageReleasePanic((arrayEnt == nullptr || arrayEnt->buffer == INVALID_BUFFER_DESC), MODULE_BUFMGR,
        ErrMsg("MoveEntryToHashTable arrayEnt null (%u).", m_private_refcount_clock));

RETRY:
    /* enter victim array entry into hashtable */
    hashEnt = static_cast<PrivateRefCountEntry *>(
        hash_search(m_private_refcount_hash, static_cast<void *>(&arrayEnt->buffer), HASH_ENTER, &found));
    StorageAssert(!found);
    if (unlikely(hashEnt == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("alloc hash for arrayEnt fail!"));
        StorageExit0(retryCnt++ > retryMax, MODULE_BUFMGR,
            ErrMsg("No memory MoveEntryToHashTable hashEntry."));
        GaussUsleep(1000);  /* sleep 1000us */
        goto RETRY;
    }

    hashEnt->refcount = arrayEnt->refcount;
    hashEnt->arenaSlotIdx = arrayEnt->arenaSlotIdx;
    return arrayEnt;
}

PrivateRefCountEntry *BufPrivateRefCount::PGetPrivateRefcountEntrySlow(BufferDesc *bufferDesc, bool create,
                                                                       bool doMove,
                                                                       PrivateRefCountEntry *freeEntry)
{
    StorageAssert(!create || doMove);
    StorageAssert(bufferDesc != INVALID_BUFFER_DESC);

    /*
     * By here we know that the buffer, if already pinned, isn't residing in
     * the array.
     */
    PrivateRefCountEntry *res = nullptr;
    bool found = false;

    /*
     * Look up the buffer in the hashtable if we've previously overflowed into
     * it.
     */
    if (m_private_refcount_overflowed > 0) {
        res = static_cast<PrivateRefCountEntry *>(
            hash_search(m_private_refcount_hash, static_cast<void *>(&bufferDesc), HASH_FIND, &found));
    }

    if (!found) {
        if (!create) {
            /* Neither array nor hash have an entry and no new entry is needed */
            return nullptr;
        } else if (freeEntry != nullptr) {
            /* add entry into the free array slot */
            freeEntry->buffer = bufferDesc;
            freeEntry->refcount = 0;
            freeEntry->arenaSlotIdx = -1;

            return freeEntry;
        } else {
            /*
             * Move entry from the current clock position in the array into the
             * hashtable. Use that slot.
             */
            PrivateRefCountEntry *arrayEnt = MoveEntryToHashTable(found);

            /* fill the now free array slot */
            arrayEnt->buffer = bufferDesc;
            arrayEnt->refcount = 0;
            arrayEnt->arenaSlotIdx = -1;

            m_private_refcount_overflowed++;

            return arrayEnt;
        }
    } else {
        if (!doMove) {
            return res;
        } else if (found && freeEntry != nullptr) {
            /* move buffer from hashtable into the free array slot
             *
             * fill array slot
             */
            if (STORAGE_VAR_NULL(res)) {
                ErrLog(DSTORE_PANIC, MODULE_BUFFER, ErrMsg("Res is nullptr."));
            }
            freeEntry->buffer = bufferDesc;
            freeEntry->refcount = res->refcount;
            freeEntry->arenaSlotIdx = res->arenaSlotIdx;

            /* delete from hashtable */
            (void)hash_search(m_private_refcount_hash, static_cast<void *>(&bufferDesc), HASH_REMOVE, &found);
            StorageAssert(found);
            StorageAssert(m_private_refcount_overflowed > 0);
            m_private_refcount_overflowed--;

            return freeEntry;
        } else {
            /*
             * Swap the entry in the hash table with the one in the array at the
             * current clock position.
             */
            PrivateRefCountEntry *arrayEnt = MoveEntryToHashTable(found);

            /* fill now free array entry with previously searched entry */
            if (res != nullptr) {
                arrayEnt->buffer = res->buffer;
                arrayEnt->refcount = res->refcount;
                arrayEnt->arenaSlotIdx = res->arenaSlotIdx;
            }

            /* and remove the old entry */
            (void)hash_search(m_private_refcount_hash, static_cast<void *>(&arrayEnt->buffer), HASH_REMOVE, &found);
            StorageAssert(found);

            /* PrivateRefCountOverflowed stays the same -1 + +1 = 0 */
            return arrayEnt;
        }
    }
}

void BufPrivateRefCount::ForgetPrivateRefcountEntry(PrivateRefCountEntry *ref)
{
    StorageAssert(ref->refcount == 0);
    /* SharedUnpin must have already cleared our arena slot. */
    StorageAssert(ref->arenaSlotIdx < 0);

    if (ref >= &m_private_refcount_array[0] &&
        ref < &m_private_refcount_array[REFCOUNT_ARRAY_ENTRIES]) {
        ref->buffer = INVALID_BUFFER_DESC;
        ref->arenaSlotIdx = -1;
    } else {
        bool found = false;
        BufferDesc* bufferDesc = ref->buffer;
        (void)hash_search(m_private_refcount_hash, static_cast<void *>(&bufferDesc), HASH_REMOVE, &found);
        StorageAssert(found);
        StorageAssert(m_private_refcount_overflowed > 0);
        m_private_refcount_overflowed--;
    }
}

void BufPrivateRefCount::Initialize()
{
    HASHCTL ctl{};

    errno_t rc = memset_s(m_private_refcount_array, sizeof(m_private_refcount_array), 0,
        sizeof(m_private_refcount_array));
    storage_securec_check(rc, "\0", "\0");
    /* memset_s zeroed arenaSlotIdx, but 0 is a valid arena slot. -1 means
     * "this (thread, buffer) has no arena pin". */
    for (Size i = 0; i < REFCOUNT_ARRAY_ENTRIES; ++i) {
        m_private_refcount_array[i].arenaSlotIdx = -1;
    }

    ctl.keysize = sizeof(BufferDesc*);
    ctl.entrysize = sizeof(PrivateRefCountEntry);
    ctl.hash = buf_hash;

    const int elemCtn = 100;
    m_private_refcount_hash = hash_create("PrivateRefCount", elemCtn, &ctl, HASH_ELEM | HASH_FUNCTION);
}

void BufPrivateRefCount::Destroy() noexcept
{
    (void)CheckForBufferPinLeaks();
    hash_destroy(m_private_refcount_hash);
}


PrivateRefCountEntry *BufPrivateRefCount::GetPrivateRefcount(BufferDesc *bufferDesc, bool create, bool doMove)
{
    PrivateRefCountEntry *ref = nullptr;
    PrivateRefCountEntry *freeEntry = nullptr;

    ref = PGetPrivateRefcountEntryFast(bufferDesc, freeEntry);
    if (ref == nullptr) {
        /* take a shortcut */
        if (freeEntry != nullptr && m_private_refcount_overflowed == 0 && create) {
            /* add entry into the free array slot */
            freeEntry->buffer = bufferDesc;
            freeEntry->refcount = 0;
            freeEntry->arenaSlotIdx = -1;

            return freeEntry;
        }

        ref = PGetPrivateRefcountEntrySlow(bufferDesc, create, doMove, freeEntry);
#ifndef UT
        if (m_private_refcount_overflowed > 0) {
            ErrLog(DSTORE_DEBUG1, MODULE_BUFFER,
                ErrMsg("Use buffer slow(num > 8), check potential buffer pin leak"));
        }
#endif
    }

    return ref;
}

uint32 BufPrivateRefCount::CheckForBufferPinLeaks()
{
    uint32 leakCount = 0;
    uint32 retryCnt = 0;
    constexpr uint32 retryMax = 1000;
    constexpr long sleepTime = 1000;

    StringInfo leakWarning = StringInfoData::make();
    while (leakWarning == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("CheckForBufferPinLeaks leakWarning is nullptr."));
        StorageExit0(retryCnt++ > retryMax, MODULE_BUFMGR, ErrMsg("No memory to make leakWarning string."));
        GaussUsleep(sleepTime);
        leakWarning = StringInfoData::make();
    }

    for (Size i = 0; i < REFCOUNT_ARRAY_ENTRIES; i++) {
        PrivateRefCountEntry *ref = &m_private_refcount_array[i];
        if (ref->buffer != INVALID_BUFFER_DESC) {
            const BufferTag &bufTag = ref->buffer->GetBufferTag();
            retryCnt = 0;
            while (STORAGE_FUNC_FAIL(leakWarning->append(" leak %s bufTag:(%hhu, %hu, %u), state:%lu, pin:%d %lu.",
                ref->buffer->IsCrPage() ? "CR" : "Base",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                ref->buffer->GetState() & Buffer::BUF_FLAG_MASK,
                ref->refcount, ref->buffer->GetRefcount()))) {
                ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("CheckForBufferPinLeaks append leak from array fail."));
                StorageExit0(retryCnt++ > retryMax, MODULE_BUFMGR,
                    ErrMsg("No memory to make append leak info from array."));
                GaussUsleep(sleepTime);
            }
            leakCount++;
#ifndef UT
            for (int32 cnt = 0; cnt < ref->refcount; cnt++) {
                ref->buffer->Unpin();
            }
#endif
        }
    }

    if (m_private_refcount_overflowed) {
        PrivateRefCountEntry *ref;
        HASH_SEQ_STATUS hstat;
        hash_seq_init(&hstat, m_private_refcount_hash);
        while ((ref = (PrivateRefCountEntry *)hash_seq_search(&hstat)) != nullptr) {
            const BufferTag &bufTag = ref->buffer->GetBufferTag();
            retryCnt = 0;
            while (STORAGE_FUNC_FAIL(leakWarning->append(" leak %s bufTag:(%hhu, %hu, %u), state:%lu, pin:%d %lu.",
                ref->buffer->IsCrPage() ? "CR" : "Base",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                ref->buffer->GetState() & Buffer::BUF_FLAG_MASK,
                ref->refcount, ref->buffer->GetRefcount()))) {
                ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("CheckForBufferPinLeaks append leak from hash fail."));
                StorageExit0(retryCnt++ > retryMax, MODULE_BUFMGR,
                    ErrMsg("No memory to make append leak info from hash."));
                GaussUsleep(sleepTime);
            }
            leakCount++;
#ifndef UT
            for (int32 cnt = 0; cnt < ref->refcount; cnt++) {
                ref->buffer->Unpin();
            }
#endif
        }
    }

    if (leakCount > 0) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
            ErrMsg("buffer leak_count: %u.%s", leakCount, leakWarning->data));
    }
    DstorePfreeExt(leakWarning->data);
    DstorePfreeExt(leakWarning);

    return leakCount;
}

void BufPrivateRefCount::PReleaseBufferDuringAbort()
{
    BufMgr *bufMgr = dynamic_cast<BufMgr *>(g_storageInstance->GetBufferMgr());
    for (Size i = 0; i < REFCOUNT_ARRAY_ENTRIES; i++) {
        PrivateRefCountEntry *ref = &m_private_refcount_array[i];
        if (ref->buffer != INVALID_BUFFER_DESC) {
            /* We may still holding lock due to Error. Unlock it here */
            if (ref->buffer->IsHeldContentLockByMe()) {
                bufMgr->UnlockContent(ref->buffer);
            }
            bufMgr->Release(ref->buffer);
        }
    }

    if (m_private_refcount_overflowed) {
        PrivateRefCountEntry *ref;
        HASH_SEQ_STATUS hstat;
        hash_seq_init(&hstat, m_private_refcount_hash);
        while ((ref = (PrivateRefCountEntry *)hash_seq_search(&hstat)) != nullptr) {
            if (ref->buffer->IsHeldContentLockByMe()) {
                bufMgr->UnlockContent(ref->buffer);
            }
            bufMgr->Release(ref->buffer);
        }
    }
}

}
