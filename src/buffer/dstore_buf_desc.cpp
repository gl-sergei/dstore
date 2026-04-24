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
 * dstore_buf_desc.cpp
 *      This file implements the functionality of buffer descriptor.
 *
 * IDENTIFICATION
 *      src/buffer/dstore_buf_desc.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/dstore_datatype.h"
#include "common/log/dstore_log.h"
#include "buffer/dstore_buf_mgr.h"
#include "buffer/dstore_buf_refcount.h"
#include "framework/dstore_instance.h"
#include "wal/dstore_wal.h"

namespace DSTORE {

namespace Buffer {

SharedPinArena *g_sharedPinArena = nullptr;

void InitSharedPinArena()
{
    if (g_sharedPinArena != nullptr) {
        return;
    }
    AutoMemCxtSwitch autoSwitch{g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_BUFFER)};
    g_sharedPinArena = DstoreNew(g_dstoreCurrentMemoryContext) SharedPinArena();
    if (STORAGE_VAR_NULL(g_sharedPinArena)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("alloc memory for shared pin arena failed"));
        return;
    }
    /* Zero-init: an empty slot is 0 (a nullptr BufferDesc). */
    for (uint32 p = 0; p < SHARED_PIN_NUM_PARTITIONS; ++p) {
        for (uint32 s = 0; s < SHARED_PIN_SLOTS_PER_PARTITION * SHARED_PIN_SLOT_SPACING; ++s) {
            GsAtomicWriteU64(&g_sharedPinArena->pins[p][s], 0);
        }
    }
}

void DestroySharedPinArena()
{
    if (g_sharedPinArena == nullptr) {
        return;
    }
    delete g_sharedPinArena;
    g_sharedPinArena = nullptr;
}

/* Map a BufferDesc pointer to one of SHARED_PIN_NUM_PARTITIONS partitions.
 * BufferDescs are memchunk-contiguous; a plain pointer-modulo would cluster
 * adjacent hot buffers. We shift off cacheline bits and mix with a
 * splittable-random multiplier to scramble adjacency. NUM_PARTITIONS must be
 * a power of two. */
static inline uint32 DeferredSlotBucket(const BufferDesc *buf)
{
    static_assert((SHARED_PIN_NUM_PARTITIONS & (SHARED_PIN_NUM_PARTITIONS - 1)) == 0,
                  "SHARED_PIN_NUM_PARTITIONS must be a power of two");
    uint64 h = (reinterpret_cast<uintptr_t>(buf) >> 6) * 0x9E3779B97F4A7C15ULL;
    return static_cast<uint32>((h >> 32) & (SHARED_PIN_NUM_PARTITIONS - 1));
}

static inline gs_atomic_uint64 *DeferredSlotPtr(uint32 bucket, uint32 slot)
{
    return &g_sharedPinArena->pins[bucket][slot * SHARED_PIN_SLOT_SPACING];
}

/* Try to publish a deferred pin for buf into this partition. Returns true on
 * success, false if no empty slot could be found (caller must fall back to
 * CAS). On success, records the slot index on entry->arenaSlotIdx so the
 * matching UnpinInSharedArena knows exactly which slot to clear.
 *
 * entry must belong to (this thread, buf). On entry, entry->arenaSlotIdx must
 * be -1 (no prior un-drained arena pin for this (thread, buf)). */
static bool PinInSharedArena(BufferDesc *buf, PrivateRefCountEntry *entry)
{
    if (STORAGE_VAR_NULL(g_sharedPinArena) || entry == nullptr) {
        return false;
    }
    StorageAssert(entry->arenaSlotIdx < 0);
    BufPrivateRefCount *privateRefCount = thrd->GetBufferPrivateRefCount();
    if (STORAGE_VAR_NULL(privateRefCount)) {
        return false;
    }

    const uint32 bucket = DeferredSlotBucket(buf);
    int hint = privateRefCount->GetLastDeferredSlot(bucket);
    if (hint < 0) {
        /* First attempt in this partition for this thread: scatter threads so
         * they don't all start probing at slot 0. */
        hint = static_cast<int>(thrd->GetThreadId() & (SHARED_PIN_SLOTS_PER_PARTITION - 1));
    }

    const uint64 bufU64 = reinterpret_cast<uint64>(buf);
    for (uint32 i = 0; i < SHARED_PIN_SLOTS_PER_PARTITION; ++i) {
        const uint32 idx = (static_cast<uint32>(hint) ^ i) & (SHARED_PIN_SLOTS_PER_PARTITION - 1);
        uint64 expected = 0;
        if (GsAtomicCompareExchangeU64(DeferredSlotPtr(bucket, idx), &expected, bufU64)) {
            privateRefCount->SetLastDeferredSlot(bucket, static_cast<int>(idx));
            entry->arenaSlotIdx = static_cast<int32>(idx);
            return true;
        }
    }
    return false;
}

/* Clear the deferred pin for buf from the slot the matching PinInSharedArena
 * claimed for this (thread, buf). Returns true on success, false if the slot
 * no longer points to buf (drained by ApplyDeferredPins) — caller must then
 * CAS-decrement state.refcount instead.
 *
 * Preconditions: entry must belong to (this thread, buf), entry->arenaSlotIdx
 * must be >= 0 (an arena pin exists). On success, resets arenaSlotIdx to -1. */
static bool UnpinInSharedArena(BufferDesc *buf, PrivateRefCountEntry *entry)
{
    if (STORAGE_VAR_NULL(g_sharedPinArena) || entry == nullptr) {
        return false;
    }
    if (entry->arenaSlotIdx < 0) {
        return false;
    }

    const uint32 bucket = DeferredSlotBucket(buf);
    const uint64 bufU64 = reinterpret_cast<uint64>(buf);
    gs_atomic_uint64 *slot = DeferredSlotPtr(bucket, static_cast<uint32>(entry->arenaSlotIdx));
    uint64 expected = bufU64;
    if (GsAtomicCompareExchangeU64(slot, &expected, 0)) {
        entry->arenaSlotIdx = -1;
        return true;
    }
    /* Slot no longer holds buf: ApplyDeferredPins drained it and folded our
     * pin into state.refcount. The slot is not ours to touch anymore. */
    entry->arenaSlotIdx = -1;
    return false;
}

/* Must be called with BUF_LOCKED set on buf. Scans buf's partition, clears
 * every slot that points to buf, and folds the count into the refcount (low
 * 32 bits of state). Clears BUF_MAY_DEFER. Returns the post-reconcile state.
 *
 * Caller invariant: refcount field in the returned state is trustworthy for
 * decisions like "can we evict this buffer?". */
HOTFUNCTION uint64 ApplyDeferredPins(BufferDesc *buf)
{
    uint64 bufState = GsAtomicReadU64(&buf->state);
    StorageAssert((bufState & BUF_LOCKED) != 0);

    if ((bufState & BUF_MAY_DEFER) == 0) {
        return bufState;
    }
    if (STORAGE_VAR_NULL(g_sharedPinArena)) {
        /* Arena not allocated — nothing to reconcile; just clear the flag. */
        while (true) {
            uint64 expected = bufState;
            uint64 newState = expected & ~BUF_MAY_DEFER;
            if (GsAtomicCompareExchangeU64(&buf->state, &expected, newState)) {
                return newState;
            }
            bufState = expected;
        }
    }

    const uint32 bucket = DeferredSlotBucket(buf);
    const uint64 bufU64 = reinterpret_cast<uint64>(buf);
    uint64 reclaimed = 0;

    /* A slot may contain buf multiple times if several threads deferred-pinned
     * it; the inner while loop handles that. */
    for (uint32 i = 0; i < SHARED_PIN_SLOTS_PER_PARTITION; ++i) {
        gs_atomic_uint64 *slot = DeferredSlotPtr(bucket, i);
        while (GsAtomicReadU64(slot) == bufU64) {
            uint64 expected = bufU64;
            if (GsAtomicCompareExchangeU64(slot, &expected, 0)) {
                reclaimed++;
            } else if (expected != bufU64) {
                /* Slot was taken by someone else — stop scanning this slot. */
                break;
            }
        }
    }

    /* Fold reclaimed into state.refcount + clear BUF_MAY_DEFER. BUF_LOCKED
     * stops other pin/unpin CAS from committing, so this CAS should normally
     * succeed first try; we still loop for safety. */
    while (true) {
        uint64 expected = bufState;
        uint64 newState = (expected + reclaimed * BUF_REFCOUNT_ONE) & ~BUF_MAY_DEFER;
        if (GsAtomicCompareExchangeU64(&buf->state, &expected, newState)) {
            return newState;
        }
        bufState = expected;
    }
}

} /* namespace Buffer */

void BufferDescController::InitController()
{
    LWLockInitialize(&ioInProgressLwlock.lock, LWLOCK_GROUP_BUF_DESC_IO_IN_PROGRESS);
    LWLockInitialize(&crAssignLwlock.lock, LWLOCK_GROUP_BUF_DESC_CR_ASSIGN);
    lastPageModifyTime.store(0, std::memory_order_release);
}

void BufferDesc::InitBufferDesc(BufBlock block, BufferDescController *ctrler)
{
    ASSERT_POINTER_ALIGNMENT(block, ALIGNOF_BUFFER);
    bufTag = INVALID_BUFFER_TAG;
    TsAnnotateRWLockCreate(&state);
    GsAtomicInitU64(&state, 0);

    lruNode.InitNode(this);
    crInfo.InitCRInfo();
    bufBlock = block;
    controller = ctrler;
    LWLockInitialize(&contentLwLock, LWLOCK_GROUP_BUF_DESC_CONTENT);

    for (uint32 i = 0; i < DIRTY_PAGE_QUEUE_MAX_SIZE; i++) {
        nextDirtyPagePtr[i] = INVALID_BUFFER_DESC;
        recoveryPlsn[i].store(INVALID_PLSN, std::memory_order_release);
    }

    pageVersionOnDisk = INVALID_PAGE_LSN;
    fileVersion = INVALID_FILE_VERSION;
}

void BufferDesc::UpdatePageVersion(Page *newPage)
{
    PageVersion newPageVersion = {newPage->GetGlsn(), newPage->GetPlsn()};
    if (newPageVersion == INVALID_PAGE_LSN || newPageVersion.IsPageVersionAllZero()) {
        return;
    }

    if (pageVersionOnDisk == INVALID_PAGE_LSN || pageVersionOnDisk < newPageVersion) {
        pageVersionOnDisk = newPageVersion;
        return;
    }
    StorageReleasePanic(pageVersionOnDisk > newPageVersion, MODULE_BUFMGR, ErrMsg("[AntiCache]page(%hu, %u) "
        "newPageVersion(%lu, %lu) < oldPageVerion(%lu, %lu)", newPage->GetFileId(), newPage->GetBlockNum(),
        newPageVersion.glsn, newPageVersion.plsn, pageVersionOnDisk.glsn, pageVersionOnDisk.plsn));
}
/*
 * Get reference count.
 * Note: this method should be called under BUF_LOCKED protect (calling LockHdr() before calling this function).
 */
uint64 BufferDesc::GetRefcount()
{
    uint64 currentState = GsAtomicReadU64(&state);
    return (currentState & Buffer::BUF_REFCOUNT_MASK);
}

void BufferDesc::ClearDirtyState(uint64 flags)
{
    uint64 bufState = LockHdr();
    StorageReleasePanic(bufState & Buffer::BUF_IS_WRITING_WAL, MODULE_BUFMGR,
        ErrMsg("Clear dirty state has writing wal flag, bufTag:(%hhu, %hu, %u) lsnInfo(walId %lu glsn %lu plsn %lu) "
        "state(%lu), recoveryPlsn(%lu)", GetPdbId(), GetPageId().m_fileId, GetPageId().m_blockId,
        GetPage()->GetWalId(), GetPage()->GetGlsn(), GetPage()->GetPlsn(), bufState,
        recoveryPlsn[DEFAULT_BGWRITER_SLOT_ID].load(std::memory_order_acquire)));
    bufState &= ~(Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY | flags);
    UnlockHdr(bufState);
}

/*
 * Lock buffer header - set BUF_LOCKED in buffer state.
 */
HOTFUNCTION uint64 BufferDesc::LockHdr()
{
    uint64 oldState = 0;
    SpinDelayStatus delayStatus = InitSpinDelay(__FILE__, __LINE__);

    for (;;) {
        /* set BUF_LOCKED flag */
        oldState = GsAtomicFetchOrU64(&state, Buffer::BUF_LOCKED);
        /* if it wasn't set before we're OK */
        if (!(oldState & Buffer::BUF_LOCKED)) {
            break;
        }
        PerformSpinDelay(&delayStatus, contentLwLock.spinsPerDelay);
    }

    AdjustSpinsPerDelay(&delayStatus, contentLwLock.spinsPerDelay);
    TsAnnotateRWLockAcquired(&state, 1);
    /* Drain any deferred pins from the shared-pin arena into state.refcount
     * so the returned state is trustworthy for refcount-sensitive decisions.
     * No-op (cheap read of BUF_MAY_DEFER) when the buffer isn't deferred. */
    return Buffer::ApplyDeferredPins(this);
}

HOTFUNCTION void BufferDesc::UnlockHdr(uint64 flags)
{
    TsAnnotateRWLockReleased(&state, 1);
    uint32 tmpState = (flags >> Buffer::BUF_REFCOUNT_BIT_NUM) & ((~Buffer::BUF_LOCKED) >> Buffer::BUF_REFCOUNT_BIT_NUM);
    GsAtomicWriteU32(((volatile uint32 *)&state) + 1, tmpState);
}

HOTFUNCTION uint64 BufferDesc::WaitHdrUnlock()
{
    uint64 bufState;
    SpinDelayStatus delayStatus = InitSpinDelay(__FILE__, __LINE__);

    bufState = GsAtomicReadU64(static_cast<volatile uint64*>(&state));
    while (bufState & Buffer::BUF_LOCKED) {
        PerformSpinDelay(&delayStatus, contentLwLock.spinsPerDelay);
        bufState = GsAtomicReadU64(static_cast<volatile uint64*>(&state));
    }

    return bufState;
}

bool BufferDesc::FastLockHdrIfReusable(const BufferTag &inBufTag, bool justValidTag, bool ignoreDirtyPage)
{
    const uint64 oldState = GsAtomicFetchOrU64(&this->state, Buffer::BUF_LOCKED);
    if ((oldState & Buffer::BUF_LOCKED) != 0) {
        /*
         * When it's locked by others, we assume it's most likely not reusable for us. Therefore, we bravely give it
         * up and try other buffers on the LRU list.
         */
        return false;
    }

    /* Drain deferred pins from the arena before trusting GetRefcount(). If we
     * skipped this, a buffer with arena-only pins would look refcount==0 here
     * and be wrongly reused. */
    const uint64 reconciledState = Buffer::ApplyDeferredPins(this);

    bool isBufTagReusable = justValidTag ? (!bufTag.IsInvalid()) : (bufTag.IsInvalid() || bufTag != inBufTag);
    if ((reconciledState & Buffer::BUF_REFCOUNT_MASK) == 0 && isBufTagReusable &&
        ((oldState & Buffer::BUF_IS_WRITING_WAL) == 0) &&
        !(ignoreDirtyPage && (oldState & (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY)))) {
            /* Return with the header locked. */
            TsAnnotateRWLockAcquired(&state, 1);
            return true;
    }

    /* We still fail to reuse it, unlock the header and bail. */
    __sync_fetch_and_and(&state, ~Buffer::BUF_LOCKED);
    return false;
}

HOTFUNCTION bool BufferDesc::IsHdrLocked()
{
    uint64 bufferState = GsAtomicReadU64(&state);
    return static_cast<bool>(bufferState & Buffer::BUF_LOCKED);
}

template<bool isGlobalTempTable>
HOTFUNCTION void BufferDesc::Pin()
{
    if (isGlobalTempTable) {
        state += Buffer::BUF_REFCOUNT_ONE;
        StorageReleasePanic(((state & Buffer::BUF_REFCOUNT_MASK) == Buffer::BUF_REFCOUNT_MASK), MODULE_BUFMGR,
                            ErrMsg("refcount in temp buffer desc is overflow, state:%lu", state));
        return;
    }
    BufPrivateRefCount *privateRefCount = thrd->GetBufferPrivateRefCount();
    if (STORAGE_VAR_NULL(privateRefCount)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("PrivateRefCount is nullptr."));
    }

    /* When the secondly and thirdly parameter all both true, the ret value must not be NULL. */
    PrivateRefCountEntry *privateRef = privateRefCount->GetPrivateRefcount(this);
    StorageAssert(privateRef != nullptr);

    if (privateRef->refcount == 0) {
        SharedPin(privateRef);
    }

    if (unlikely(privateRef->refcount > 0xffff)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
            ErrMsg("the pin of buffer bufTag:(%hhu, %hu, %u) may leak, the refcount is %d",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                privateRef->refcount));
    }
    privateRef->refcount++;
}

/*
 * Pin Shared buffer among multi-threads.
 * Pin() is based on it when one thread has use it pin the first time,
 * because it involves race on Shared buffer modifing with other threads.
 * note: dont use it directly.
 *
 * Shared-pin arena fast path: once the refcount has crossed BUF_DEFER_THRESHOLD
 * the buffer is tagged with BUF_MAY_DEFER. Subsequent pins publish themselves
 * into the global SharedPinArena (each slot on its own cacheline) instead of
 * CAS-mutating this buffer's state. Arena pins are invisible in state.refcount
 * until a header-lock holder calls Buffer::ApplyDeferredPins.
 */
HOTFUNCTION void BufferDesc::SharedPin(PrivateRefCountEntry *entry)
{
    uint64 bufState;
    for (;;) {
        bufState = GsAtomicReadU64(&state);

        if (bufState & Buffer::BUF_LOCKED) {
            bufState = WaitHdrUnlock();
            continue;
        }

        /* Hot path: publish pin into the arena instead of mutating state.
         * entry == nullptr (PinForAio) forces CAS path. */
        if (entry != nullptr && (bufState & Buffer::BUF_MAY_DEFER) != 0 &&
            Buffer::PinInSharedArena(this, entry)) {
            return;
        }

        /* Cold path: CAS-bump refcount. If the new refcount reaches the
         * defer threshold, opportunistically set BUF_MAY_DEFER in the same
         * CAS so future pinners see the flag. */
        uint64 newState = bufState + Buffer::BUF_REFCOUNT_ONE;
        if ((newState & Buffer::BUF_REFCOUNT_MASK) >= Buffer::BUF_DEFER_THRESHOLD) {
            newState |= Buffer::BUF_MAY_DEFER;
        }
        if (GsAtomicCompareExchangeU64(&state, &bufState, newState)) {
            StorageAssert((bufState & Buffer::BUF_REFCOUNT_MASK) != Buffer::BUF_REFCOUNT_MASK);
            return;
        }
        /* CAS failed: bufState now has current state; retry from the top
         * (which will re-check BUF_LOCKED). */
    }
}

HOTFUNCTION void BufferDesc::PinForAio()
{
    /* AIO path does not maintain a per-thread PrivateRefCountEntry, so it
     * cannot track an arena slot. Force the CAS path by passing nullptr. */
    SharedPin(nullptr);
}

HOTFUNCTION void BufferDesc::PinUnderHdrLocked()
{
    BufPrivateRefCount *privateRefCount = thrd->GetBufferPrivateRefCount();
    StorageAssert(privateRefCount != nullptr);
    UNUSE_PARAM uint64 bufState = GsAtomicReadU64(&state);
    StorageAssert((bufState & Buffer::BUF_LOCKED) != 0);

    /* When the secondly and thirdly parameter all both true, the ret value must not be NULL. */
    PrivateRefCountEntry *privateRef = privateRefCount->GetPrivateRefcount(this);
    StorageAssert(privateRef != nullptr);

    if (unlikely(privateRef->refcount > 0xffff)) {
        ErrLog(DSTORE_ERROR, (MODULE_BUFMGR),
            ErrMsg("the pin of buffer bufTag:(%hhu, %hu, %u) may leak, the refcount is %d",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                privateRef->refcount));
    }

    if (privateRef->refcount == 0) {
        StorageAssert((bufState & Buffer::BUF_REFCOUNT_MASK) != Buffer::BUF_REFCOUNT_MASK);
        bufState = GsAtomicFetchAddU64(&state, Buffer::BUF_REFCOUNT_ONE);
        if (unlikely((bufState & Buffer::BUF_REFCOUNT_MASK) > 0xffffff)) {
            ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
                ErrMsg("the pin of buffer bufTag:(%hhu, %hu, %u) may leak, the refcount is %lu",
                    bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                    (bufState & Buffer::BUF_REFCOUNT_MASK)));
        }
        StorageReleasePanic(((bufState & Buffer::BUF_REFCOUNT_MASK) == Buffer::BUF_REFCOUNT_MASK),
            MODULE_BUFMGR, ErrMsg("refcount in buffer desc is overflow, state:%lu", bufState));
    }
    privateRef->refcount++;
}

template<bool isGlobalTempTable>
HOTFUNCTION void BufferDesc::Unpin()
{
    if (isGlobalTempTable) {
        state -= Buffer::BUF_REFCOUNT_ONE;
        StorageReleasePanic(((state & Buffer::BUF_REFCOUNT_MASK) == Buffer::BUF_REFCOUNT_MASK), MODULE_BUFMGR,
                            ErrMsg("double unpin in temp buffer desc, state:%lu", state));
        return;
    }
    BufPrivateRefCount *privateRefCount = thrd->GetBufferPrivateRefCount();
    StorageAssert(privateRefCount != nullptr);

    /* if error happend in GetPrivateRefCountEntry , can not do UnlockBufHdrNew */
    PrivateRefCountEntry *privateRef = privateRefCount->GetPrivateRefcount(this, false, false);
    if (privateRef == nullptr || privateRef->refcount <= 0) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("privateRef is null or refcount <= 0."));
        return;
    }

    privateRef->refcount--;
    StorageReleasePanic(privateRef->refcount < 0, MODULE_BUFMGR,
        ErrMsg("refcount in buffer desc (%hhu, %hu, %u) is overflow, private refcount:%d state:%lu",
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, privateRef->refcount, state));
    if (privateRef->refcount == 0) {
        /* I'd better not still hold any locks on the buffer */
        StorageAssert(!LWLockHeldByMe(&contentLwLock));
        StorageAssert(!LWLockHeldByMe(controller->GetIoInProgressLwLock()));
        SharedUnpin(privateRef);
        privateRefCount->ForgetPrivateRefcountEntry(privateRef);
    }
}

/* better not to use it directly. the same as SharedPin */
HOTFUNCTION void BufferDesc::SharedUnpin(PrivateRefCountEntry *entry)
{
#ifndef ENABLE_THREAD_CHECK
    /* Sanity check: at least one of three signals must indicate a live pin
     * for this buffer. (a) refcount > 0 means some thread's pin is in state;
     * (b) BUF_MAY_DEFER set means arena-published pins may exist; (c) our
     * own entry holds an arena slot. Read state once to avoid a TOCTOU race
     * between the refcount check and the flag check. */
    {
        const uint64 snap = GsAtomicReadU64(&state);
        StorageAssert((snap & Buffer::BUF_REFCOUNT_MASK) > 0 ||
                      (snap & Buffer::BUF_MAY_DEFER) != 0 ||
                      (entry != nullptr && entry->arenaSlotIdx >= 0));
    }
#endif

    /* If this (thread, buffer) pin lives in the arena, try to clear its slot
     * first. When the racing drainer already folded our slot into state, the
     * slot no longer matches and UnpinInSharedArena returns false — we then
     * fall through to CAS-decrement state.refcount. The check is keyed on
     * entry->arenaSlotIdx (not on BUF_MAY_DEFER, which the drainer may have
     * already cleared while our entry still points at the arena slot). */
    if (entry != nullptr && entry->arenaSlotIdx >= 0) {
        if (Buffer::UnpinInSharedArena(this, entry)) {
            return;
        }
        /* UnpinInSharedArena reset arenaSlotIdx to -1; fall through to CAS. */
    }

    uint64 bufState;
    for (;;) {
        bufState = GsAtomicReadU64(&state);

        if (bufState & Buffer::BUF_LOCKED) {
            bufState = WaitHdrUnlock();
            continue;
        }

        /* Cold path: CAS-decrement refcount. Do not clear BUF_MAY_DEFER here;
         * that flag is cleared by ApplyDeferredPins once the arena has been
         * drained under the header lock. */
        uint64 newState = bufState - Buffer::BUF_REFCOUNT_ONE;
        if (GsAtomicCompareExchangeU64(&state, &bufState, newState)) {
            StorageReleasePanic(((bufState & Buffer::BUF_REFCOUNT_MASK) == 0), MODULE_BUFMGR,
                ErrMsg("refcount in buffer desc underflow, state:%lu", bufState));
            return;
        }
    }
}

HOTFUNCTION void BufferDesc::UnpinForAio()
{
    /* Paired with PinForAio: no entry, forced CAS-only path. */
    SharedUnpin(nullptr);
}

/*
 * Returns true iff the buffer is pinned privately
 * (also checks for valid buffer number).
 *
 * NOTE: what we check here is that *this* backend holds a pin on
 * the buffer. We do not care whether some other backend also does.
 */
bool BufferDesc::IsPinnedPrivately()
{
    BufPrivateRefCount *privateRefCount = thrd->GetBufferPrivateRefCount();
    StorageAssert(privateRefCount != nullptr);

    PrivateRefCountEntry *ref = privateRefCount->GetPrivateRefcount(this, false, true);
    return ((ref != nullptr) && (ref->refcount > 0));
}

bool BufferDesc::IsInDirtyPageQueue(const int64 slotId) const
{
    return recoveryPlsn[slotId].load(std::memory_order_acquire) != INVALID_PLSN;
}

/* Note: acquires wal write LWLock to be released by SetPageEndWriteWal(). */
HOTFUNCTION void BufferDesc::SetPageIsWritingWal()
{
    uint64 oldBufState;
    uint64 bufState;
    StorageReleaseBufferCheckPanic(!LWLockHeldByMeInMode(&contentLwLock, LW_EXCLUSIVE), MODULE_BUFMGR, bufTag,
        "BufferDesc is not locked by me");
    for (;;) {
        oldBufState = WaitHdrUnlock();
        bufState = oldBufState;
        StorageAssert((bufState & Buffer::BUF_IS_WRITING_WAL) != Buffer::BUF_IS_WRITING_WAL);
        StorageReleaseBufferCheckPanic(g_storageInstance->GetType() == StorageInstanceType::DISTRIBUTE_COMPUTE &&
            (bufState & Buffer::BUF_OWNED_BY_ME) == 0, MODULE_BUFMGR, bufTag, "Page not owned when start write wal");
        bufState |= Buffer::BUF_IS_WRITING_WAL;
        if (GsAtomicCompareExchangeU64(&state, &oldBufState, bufState)) {
            break;
        }
    }
}

HOTFUNCTION void BufferDesc::SetPageEndWriteWal()
{
    uint64 oldBufState;
    uint64 bufState;
    for (;;) {
        oldBufState = WaitHdrUnlock();
        bufState = oldBufState;
        StorageAssert((bufState & Buffer::BUF_IS_WRITING_WAL) == Buffer::BUF_IS_WRITING_WAL);
        StorageReleaseBufferCheckPanic(g_storageInstance->GetType() == StorageInstanceType::DISTRIBUTE_COMPUTE &&
            (bufState & Buffer::BUF_OWNED_BY_ME) == 0, MODULE_BUFMGR, bufTag, "Page not owned when end write wal");
        StorageReleaseBufferCheckPanic((bufState & Buffer::BUF_CONTENT_DIRTY) == 0, MODULE_BUFMGR, bufTag,
            "Page not dirty when end write wal");
        bufState &= ~Buffer::BUF_IS_WRITING_WAL;
        if (GsAtomicCompareExchangeU64(&state, &oldBufState, bufState)) {
            break;
        }
    }
}

/*
 * The caller of the function must hold the page lock
 */
HOTFUNCTION void BufferDesc::WaitIfIsWritingWal()
{
    uint64 bufState;
    StorageAssert(LWLockHeldByMe(&contentLwLock));
    /*
     * Note that we must have acquired content lock here, otherwise we could get a spurious buffer state and mistakenly
     * think there is no wal write underway.
     */
    bufState = GetState(false);
    while (bufState & Buffer::BUF_IS_WRITING_WAL) {
        GaussUsleep(1);
        bufState = GetState(false);
    }
}

void BufferDesc::WaitIfIoInProgress()
{
    uint64 bufState = GetState(false);
    while ((bufState & Buffer::BUF_IO_IN_PROGRESS) != 0U) {
        GaussUsleep(1);
        bufState = GetState(false);
    }
}

void BufferDesc::InvalidateCrPage()
{
    PageType pageType = this->GetPage()->GetType();
    if (pageType == PageType::HEAP_PAGE_TYPE || pageType == PageType::INDEX_PAGE_TYPE) {
        AcquireCrAssignLwlock(LW_EXCLUSIVE);
        SetCrUnusable();
        ReleaseCrAssignLwlock();
    }
}
void BufferDesc::PrintBufferDesc(char *str, Size maxSize) const
{
    BufferTag localBufTag = this->bufTag;
    errno_t rc = sprintf_s(str, maxSize, "(bufTag:(%hhu, %hu, %u) State:%lx)",
        localBufTag.pdbId, localBufTag.pageId.m_fileId, localBufTag.pageId.m_blockId, this->state);
    storage_securec_check_ss(rc);
}

char *BufferDesc::PrintBufferDesc()
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for bufferDesc dump info."));
        return nullptr;
    }
    dumpInfo.append("Buffer:%p bufTag:(%hhu, %hu, %u)\n", this,
        bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId);

    uint64 bufferState = GetState(false);
    PrivateRefCountEntry *entry = thrd->GetBufferPrivateRefCount()->GetPrivateRefcount(this, false, false);
    dumpInfo.append("state: 0x%016lx pin share:%lu local:%d\n", bufferState, bufferState & Buffer::BUF_REFCOUNT_MASK,
                    entry == nullptr ? -1 : entry->refcount);
    PrintBufferState(bufferState, &dumpInfo);
    dumpInfo.append("\nlru:\n");
    dumpInfo.append("    queue index:%u type:%d usage:%d\n",
                    lruNode.lruIndex, static_cast<int>(lruNode.m_type.load()), lruNode.m_usage.load());

    if (bufferState & Buffer::BUF_CR_PAGE) {
        dumpInfo.append("It is a CR bufferdesc, base bufferdesc :%p \n", crInfo.baseBufferDesc);
    } else {
        const char *str = crInfo.isUsable ? "cr usable" : "cr unusable";
        dumpInfo.append("It is a base bufferdesc, CR buffer desc: %p, %s", crInfo.crBuffer, str);
    }

    dumpInfo.append("buf block:%p\n", bufBlock);

    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("%s", dumpInfo.data));

    return dumpInfo.data;
}

void BufferDesc::PrintBufferState(uint64 bufferState, StringInfoData *dumpInfo)
{
    dumpInfo->append("bufState: ");
    uint8 flagCnt = 0;
    PrintBufSingleFlagByState(bufferState, dumpInfo, &flagCnt);
}

const char *BufferDesc::GetBufSingleNodeFlagString(uint64 bufFlagBit) const
{
    /* omit useless namespace prefix in printing */
    switch (bufFlagBit) {
        case Buffer::BUF_LOCKED:
            return "BUF_LOCKED";
        case Buffer::BUF_CONTENT_DIRTY:
            return "BUF_CONTENT_DIRTY";
        case Buffer::BUF_VALID:
            return "BUF_VALID";
        case Buffer::BUF_TAG_VALID:
            return "BUF_TAG_VALID";
        case Buffer::BUF_IO_IN_PROGRESS:
            return "BUF_IO_IN_PROGRESS";
        case Buffer::BUF_IO_ERROR:
            return "BUF_IO_ERROR";
        case Buffer::BUF_HINT_DIRTY:
            return "BUF_HINT_DIRTY";
        case Buffer::BUF_CR_PAGE:
            return "BUF_CR_PAGE";
        case Buffer::BUF_IS_WRITING_WAL:
            return "BUF_IS_WRITING_WAL";
        default:
            break;
    }
    return "INVALID_BUF_FLAG_EN";
}

void BufferDesc::PrintBufSingleFlagByState(uint64 bufferState, StringInfoData *dumpInfo, uint8 *flagCnt)
{
    if (!(bufferState & Buffer::BUF_ALL_SINGLE_FLAGS)) {
        return;
    }

    uint8 flagCntTmp = *flagCnt;
    for (uint8 idx = 0; idx < Buffer::BUF_ALL_SINGLE_FLAGS_NUM; idx++) {
        if (!(bufferState & Buffer::BUF_ALL_SINGLE_FLAGS_ARRAY[idx])) {
            continue;
        }
        if (flagCntTmp % Buffer::BUF_FLAG_MAX_PRINT_ONE_ROW == 0 && flagCntTmp != 0) {
            dumpInfo->append("\n");
        }
        dumpInfo->append("%s|", GetBufSingleNodeFlagString(Buffer::BUF_ALL_SINGLE_FLAGS_ARRAY[idx]));
        flagCntTmp++;
    }
    *flagCnt = flagCntTmp;
}

template void BufferDesc::Pin<true>();
template void BufferDesc::Pin<false>();
template void BufferDesc::Unpin<true>();
template void BufferDesc::Unpin<false>();

} /* namespace DSTORE */
