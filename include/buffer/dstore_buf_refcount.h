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

#ifndef DSTORE_BUF_REFCOUNT_H
#define DSTORE_BUF_REFCOUNT_H

#include "buffer/dstore_buf.h"
#include "common/algorithm/dstore_hsearch.h"

namespace DSTORE {

struct PrivateRefCountEntry {
    BufferDesc *buffer;
    int32 refcount;
    /* Index of the arena slot this (thread, buffer) pin occupies, or -1 if
     * this thread's pin lives in state.refcount (CAS path). Set by
     * PinInSharedArena, cleared by UnpinInSharedArena or by a reconciler
     * drain via ApplyDeferredPins. */
    int32 arenaSlotIdx;

    PrivateRefCountEntry()
    {
        buffer = INVALID_BUFFER_DESC;
        refcount = 0;
        arenaSlotIdx = -1;
    }
};

constexpr Size REFCOUNT_ARRAY_ENTRIES = 8;

class BufPrivateRefCount : public BaseObject {
public:
    BufPrivateRefCount();
    ~BufPrivateRefCount() = default;

    void Initialize();

    void Destroy() noexcept;

    /*
     * Find private refcount
     *
     * If set create as true, then it will find a free PrivateRefCountEntry or evict an existing PrivateRefCountEntry
     * from PrivateRefCountEntryArray when buffer not find in the HashTable.
     * If set doMove as true, then it will evict an existing PrivateRefCountEntry from PrivateRefCountEntryArray when
     * find buffer in the HashTable.
     */
    PrivateRefCountEntry *GetPrivateRefcount(BufferDesc *bufferDesc, bool create = true, bool doMove = true);

    /*
     * Reset a refcount entry
     */
    void ForgetPrivateRefcountEntry(PrivateRefCountEntry *ref);

    /*
     * scan buffer private refcount to check if it has buffer that is not released.
     */
    uint32 CheckForBufferPinLeaks();

    /*
     * scan buffer private refcount during abort to release these buffers
     */
    void PReleaseBufferDuringAbort();

    /*
     * Move an entry from the refcount array to refcount hashtable based on refcount clock.
     */
    PrivateRefCountEntry *MoveEntryToHashTable(bool found);

    /*
     * Shared-pin arena per-thread hints. Each entry remembers which slot in the
     * arena this thread last used for the given partition, so PinInSharedArena
     * can start probing from there and UnpinInSharedArena can usually hit on
     * the first probe. -1 means "no hint yet".
     */
    int GetLastDeferredSlot(uint32 bucket) const
    {
        return m_lastDeferredSlots[bucket];
    }

    void SetLastDeferredSlot(uint32 bucket, int slot)
    {
        m_lastDeferredSlots[bucket] = slot;
    }

private:
   /*
    * Backend-Private refcount management:
    *
    * Each buffer also has a private refcount that keeps track of the number of
    * times the buffer is pinned in the current process.  This is so that the
    * shared refcount needs to be modified only once if a buffer is pinned more
    * than once by a individual backend.  It's also used to check that no buffers
    * are still pinned at the end of transactions and when exiting.
    *
    *
    * To avoid - as we used to - requiring an array with g_storageInstance.attr.attr_storage.NBuffers entries to keep
    * track of local buffers we use a small sequentially searched array
    * (PrivateRefCountArray) and a overflow hash table (PrivateRefCountHash) to
    * keep track of backend local pins.
    *
    * Until no more than REFCOUNT_ARRAY_ENTRIES buffers are pinned at once, all
    * refcounts are kept track of in the array; after that, new array entries
    * displace old ones into the hash table. That way a frequently used entry
    * can't get "stuck" in the hashtable while infrequent ones clog the array.
    *
    * Note that in most scenarios the number of pinned buffers will not exceed
    * REFCOUNT_ARRAY_ENTRIES.
    */

    struct PrivateRefCountEntry m_private_refcount_array[REFCOUNT_ARRAY_ENTRIES];
    HTAB* m_private_refcount_hash;
    int32 m_private_refcount_overflowed;
    uint32 m_private_refcount_clock;

    /* Per-partition hint for last-used arena slot; see GetLastDeferredSlot. */
    int m_lastDeferredSlots[Buffer::SHARED_PIN_NUM_PARTITIONS];

    /*
     * GetPrivateRefCountEntryFast
     *    find PrivateRefCountEntry fast.
     *    If not find, then return nullptr and set freeEntry point to a free PrivateRefCountEntry if any.
     */
    PrivateRefCountEntry *PGetPrivateRefcountEntryFast(BufferDesc *bufferDesc, PrivateRefCountEntry *&freeEntry);

    /*
     * GetPrivateRefCountEntrySlow
     *    find PrivateRefCountEntry from HashTable.
     *    If set create, then it will find a free PrivateRefCountEntry or evict a exist PrivateRefCountEntry from
     * PrivateRefCountEntryArray when buffer not find in the HashTable.
     *    If set doMove, then it will evict a exist PrivateRefCountEntry from PrivateRefCountEntryArray when find
     * buffer in the HashTable.
     */
    PrivateRefCountEntry *PGetPrivateRefcountEntrySlow(BufferDesc *bufferDesc, bool create, bool doMove,
                                                            PrivateRefCountEntry *freeEntry);
};


} /* namespace DSTORE */

#endif /* STORAGE_BUF_REFCOUNT_H */
