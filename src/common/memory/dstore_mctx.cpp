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
 * dstore_mctx.cpp
 *
 *
 * IDENTIFICATION
 *        src/common/memory/dstore_mctx.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/memory/dstore_mctx.h"
#include "common/memory/dstore_memory_allocator_generic.h"
#include "common/error/dstore_error.h"
#include "errorcode/dstore_common_error_code.h"
#include "common/memory/dstore_memory_allocator_stack.h"
#include "common/dstore_common_utils.h"
#include "framework/dstore_thread.h"

namespace DSTORE {
/* g_dstoreCurrentMemoryContext, Default memory context for allocations. */
THR_LOCAL DstoreMemoryContext g_dstoreCurrentMemoryContext = nullptr;

void MemoryContextDeleteChildren(DstoreMemoryContext context);

#ifdef MEMORY_CONTEXT_CHECKING
void DstoreMemoryContextCheck(DstoreMemoryContext context, bool onlyCheckThreadConsistent)
{
    if (onlyCheckThreadConsistent == false) {
        (context->allocSet)->check();
    }

    ThreadId id = context->type == MemoryContextType::THREAD_CONTEXT ? context->thread_id : 0;
    for (DstoreMemoryContext child = context->firstChild; child != nullptr; child = child->nextChild) {
        if (child->type == MemoryContextType::THREAD_CONTEXT) {
            StorageAssert((id == 0) || (id == child->thread_id));
        }
        DstoreMemoryContextCheck(child, onlyCheckThreadConsistent);
    }
}

void CheckMemoryContextSize(__attribute__((__unused__)) DstoreMemoryContext context,
                            __attribute__((__unused__)) Size size)
{
    /* todo:
     * when session level memory context control is enabled (u_sess->attr.attr_memory.enable_memory_context_control)
     * compare m_total_space & m_max_space_size in specific condition
     * (while condition is depending on session information)
     */
}
#endif

/*
 * SetMemoryContextParent
 *      Change a context to belong to a new parent (or no parent).
 * Callers often assume that this function cannot fail, so don't put any
 * elog(ERROR) calls in it.
 */
void SetMemoryContextParent(DstoreMemoryContext context, DstoreMemoryContext newParent)
{
    StorageAssert(DstoreMemoryContextIsValid(context));
    StorageAssert(context != newParent);

    if (newParent != nullptr) {
        if (context->session_id != newParent->session_id) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("We can not set memory context parent with different "
                "session number"));
            StorageAssert(0);
        }
    }

    /* Delink from existing parent, if any */
    if (context->parent) {
        DstoreMemoryContext parent = context->parent;

        if (context->prevChild != nullptr) {
            context->prevChild->nextChild = context->nextChild;
        } else {
            StorageAssert(parent->firstChild == context);
            parent->firstChild = context->nextChild;
        }

        if (context->nextChild != nullptr) {
            context->nextChild->prevChild = context->prevChild;
        }
    }

    /* And relink */
    if (newParent) {
        StorageAssert(DstoreMemoryContextIsValid(newParent));
        context->parent = newParent;
        context->prevChild = nullptr;
        context->nextChild = newParent->firstChild;
        if (newParent->firstChild != nullptr) {
            newParent->firstChild->prevChild = context;
        }
        newParent->firstChild = context;
    } else {
        context->parent = nullptr;
        context->prevChild = nullptr;
        context->nextChild = nullptr;
    }
}

void DeleteInternalMemoryContext(DstoreMemoryContext context, bool parentLocked)
{
    StorageAssert(DstoreMemoryContextIsValid(context));
    /* We had better not be deleting t_thrd.top_mem_cxt ... */
    /* And not g_dstoreCurrentMemoryContext, either */
    StorageAssert(context != g_dstoreCurrentMemoryContext);

    MemoryContextDeleteChildren(context);
    DstoreMemoryContext parent = context->parent;
    /*
     * If the parent context is shared and is already locked by the caller,
     * no need to relock again. In fact, that's not the right thing to do
     * since it will lead to a self-deadlock
     */
    if (parent && !parentLocked) {
        DstoreAllocSetContext::LockMemoryContextIfShared(parent);
    }
    /*
     * We delink the context from its parent before deleting it, so that if
     * there's an error we won't have deleted/busted contexts still attached
     * to the context tree.  Better a leak than a crash.
     */
    SetMemoryContextParent(context, nullptr);

    if (parent && !parentLocked) {
        DstoreAllocSetContext::UnlockMemoryContextIfShared(parent);
    }

    (context->allocSet)->clear();

    delete (context->allocSet);
}

/*
 * ChunkGetAllocSetContext
 *      Given a currently-allocated chunk, determine the alloc_set_context it belongs to.
 */
static inline DstoreAllocSetContext *ChunkGetAllocSetContext(void *pointer)
{
    DstoreAllocSetContext *allocSet;

    AllocChunk chunk = AllocPtrGetChunk(pointer);
    allocSet = static_cast<DstoreAllocSetContext *>(chunk->alloc_set);

    StorageAssert(DstorePointerIsValid(allocSet));
    StorageAssert(DstoreMemoryContextIsValid(allocSet->GetMemoryContext()));
    return allocSet;
}

/* public interfaces */
/*
 * AllocSetContextDestroy
 *      Destroy memory context along with its DstoreAllocSetContext
 *......Only used for deleting instance's top memory context
 */
void DstoreMemoryContextDestroyTop(DstoreMemoryContext context)
{
    StorageAssert(context != nullptr);
    StorageAssert(context->parent == nullptr);
#ifdef MEMORY_CONTEXT_CHECKING
    DstoreMemoryContextCheck(context);
#endif

    DstoreMemoryContext oldContext = DstoreMemoryContextSwitchTo(context);
    while (context->firstChild != nullptr) {
        DstoreMemoryContextDelete(context->firstChild);
    }
    DstoreAllocSetContext *allocSet = context->allocSet;
    StorageAssert(allocSet->is_root_context());
    allocSet->clear();
    free(allocSet);
    (void)DstoreMemoryContextSwitchTo(oldContext);
}

/*
 * MemoryContextAlloc
 *      Allocate space within the specified context.
 */
void *DstoreMemoryContextAllocDebug(DstoreMemoryContext context, Size size, const char *file, int line)
{
    if (!AllocSizeIsValid(size)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("invalid memory allocate request size %lu",
            static_cast<unsigned long>(size)));
        return nullptr;
    }

    StorageAssert(DstoreMemoryContextIsValid(context));

    void *space = (context->allocSet)->allocate(0, size, file, line);
    if (unlikely(space == nullptr && thrd != nullptr && thrd->error != nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed on request of size %lu bytes in %s:%d.",
            static_cast<unsigned long>(size), file, line));
        return nullptr;
    }
    return space;
}

void *DstoreMemoryContextAllocHugeMemoryDebug(DstoreMemoryContext context, Size size, const char *file, int line)
{
    if (!AllocHugeSizeIsValid(size)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("invalid memory allocate request size %lu",
            static_cast<unsigned long>(size)));
        return nullptr;
    }

    StorageAssert(DstoreMemoryContextIsValid(context));

    void *space = (context->allocSet)->allocate(0, size, file, line);
    if (unlikely(space == nullptr && thrd != nullptr && thrd->error != nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed on request of size %lu bytes in %s:%d.",
            static_cast<unsigned long>(size), file, line));
        return nullptr;
    }
    return space;
}

/*
 * MemoryContextStrdup
 *      Like strdup(), but allocate from the specified context
 */
char* DstoreMemoryContextStrdupDebug(DstoreMemoryContext context, const char* string, const char* file, int line)
{
    char* nstr = nullptr;
    Size len;
    errno_t rc;

    if (string == nullptr) {
        return nullptr; /* nothing to do */
    }
    len = strlen(string) + 1;

    nstr = (char*)DstoreMemoryContextAllocDebug(context, len, file, line);
    if (STORAGE_VAR_NULL(nstr)) {
        return nullptr;
    }

    rc = memcpy_s(nstr, len, string, len);
    storage_securec_check(rc, "\0", "\0");

    return nstr;
}

/*
 * MemoryContextAllocZero
 *      Like MemoryContextAlloc, but clears allocated memory
 */
void *DstoreMemoryContextAllocDebugZero(DstoreMemoryContext context, Size size, const char *file, int line)
{
    if (!AllocSizeIsValid(size)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("invalid memory allocate request size %lu",
            static_cast<unsigned long>(size)));
        return nullptr;
    }

    StorageAssert(DstoreMemoryContextIsValid(context));

    void *ret = (context->allocSet)->allocate0(0, size, file, line);
    if (unlikely(ret == nullptr && thrd != nullptr && thrd->error != nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed on request of size %lu bytes in %s:%d.",
            static_cast<unsigned long>(size), file, line));
        return nullptr;
    }
    return ret;
}

/*
 * DstoreMemoryContextSwitchTo
 *      Returns the current context; installs the given context.
 */
DstoreMemoryContext DstoreMemoryContextSwitchTo(DstoreMemoryContext context)
{
    DstoreMemoryContext old = g_dstoreCurrentMemoryContext;
    g_dstoreCurrentMemoryContext = context;
    return old;
}

/*
 * DstoreMemoryContextDelete
 *      clear a context and its descendants, and release all space allocated therein.
 *
 * The type-specific delete routine removes all subsidiary storage
 * for the context, but we have to delete the context node itself,
 * as well as recurse to get the children. We must also delink the
 * node from its parent, if it has one.
 */
void DstoreMemoryContextDelete(DstoreMemoryContext context) noexcept
{
    StorageAssert(DstoreMemoryContextIsValid(context));

    UNUSE_PARAM DstoreMemoryContext parent = context->parent;
    DstoreMemoryContext topnode = context->topnode;
    bool flag = (topnode != context);
    if (flag && parent->type == MemoryContextType::SHARED_CONTEXT) {
        pthread_rwlock_wrlock(&topnode->tree_lock);
    }
    DeleteInternalMemoryContext(context, false);
    if (flag && parent->type == MemoryContextType::SHARED_CONTEXT) {
        pthread_rwlock_unlock(&topnode->tree_lock);
    }
}

void MemoryContextDeleteChildren(DstoreMemoryContext context)
{
    StorageAssert(DstoreMemoryContextIsValid(context));

    UNUSE_PARAM DstoreMemoryContext parent = context->parent;
    /* We had better not be deleting DstoreTopMemoryContext */
    StorageAssert(parent != nullptr);

    DstoreAllocSetContext::LockMemoryContextIfShared(context);

    while (context->firstChild) {
        DeleteInternalMemoryContext(context->firstChild, true);
    }

    DstoreAllocSetContext::UnlockMemoryContextIfShared(context);
}

void DstoreMemoryContextResetAndDeleteChildren(DstoreMemoryContext context)
{
    StorageAssert(DstoreMemoryContextIsValid(context));
#ifdef MEMORY_CONTEXT_CHECKING
    DstoreMemoryContextCheck(context);
#endif
    MemoryContextDeleteChildren(context);
    DstoreMemoryContextReset(context);
}
/*
 * DstoreMemoryContextIsValid
 *      True iff memory context is valid.
 */
bool DstoreMemoryContextIsValid(DstoreMemoryContext context)
{
    if (context != nullptr &&
        context->type > MemoryContextType::INVALID_CONTEXT && context->type < MemoryContextType::MAX_CONTEXT) {
        return true;
    }
    return false;
}

/*
 * DstoreGetMemoryChunkSpace
 *      Given a currently-allocated chunk, determine the total space
 *      it occupies (including all memory-allocation overhead).
 *
 * This is useful for measuring the total space occupied by a set of
 * allocated chunks.
 */
Size DstoreGetMemoryChunkSpace(void *pointer)
{
    /*
     * Try to detect bogus pointers handed to us, poorly though we can.
     * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
     * allocated chunk.
     */
    StorageAssert(DstorePointerIsValid(pointer));
    StorageAssert(pointer == reinterpret_cast<void *>(MAXALIGN(pointer)));

    DstoreAllocSetContext *allocSet = ChunkGetAllocSetContext(pointer);
    return allocSet->get_chunk_space(pointer);
}

/*
 * DstoreMemoryContextReset
 *      Release all space allocated within a context and its descendants,
 *      but don't delete the contexts themselves.
 *
 * The type-specific reset routine handles the context itself, but we
 * have to do the recursion for the children.
 */
void DstoreMemoryContextReset(DstoreMemoryContext context)
{
    StorageAssert(DstoreMemoryContextIsValid(context));

    DstoreAllocSetContext::LockMemoryContextIfShared(context);

    /*
     * Release all space allocated within a context's descendants,
     * but don't delete the contexts themselves. The named context
     * itself is not touched.
    */
    for (DstoreMemoryContext child = context->firstChild; child != nullptr; child = child->nextChild) {
        DstoreMemoryContextReset(child);
    }

    DstoreAllocSetContext::UnlockMemoryContextIfShared(context);

    /* Nothing to do if no pallocs since startup or last reset */
    if (!context->is_reset) {
        (context->allocSet)->reset();
        context->is_reset = true;
    }
}

void DstoreAllocSetCheckPointer(UNUSE_PARAM void* pointer)
{
    StorageAssert(DstorePointerIsValid(pointer));
#ifdef MEMORY_CONTEXT_CHECKING
    ChunkGetAllocSetContext(pointer)->check_data_pointer(pointer);
#endif
}

/* DstoreAllocSetContextCreate
 *      create a new AllocSet context.
 */
DstoreMemoryContext DstoreAllocSetContextCreate(DstoreMemoryContext parent, const char *name, Size minContextSize,
    Size initBlockSize, Size maxBlockSize, MemoryContextType contextType, Size maxSize)
{
#ifndef UT
    /* Only UT have the following situation */
    /* The following situation is forbidden, parent context is not shared, while current context is shared. */
    if (parent != nullptr && !(parent->type == MemoryContextType::SHARED_CONTEXT) &&
        contextType == MemoryContextType::SHARED_CONTEXT) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed while creating shared memory context %s from standard "
            "context %s.", name, parent->name));
        return nullptr;
    }
#endif

    switch (contextType) {
        case MemoryContextType::THREAD_CONTEXT:
        case MemoryContextType::SESSION_CONTEXT:
        case MemoryContextType::SHARED_CONTEXT:
            return GenericAllocSetContext::Create(parent, name, minContextSize, initBlockSize, maxBlockSize,
                                                  contextType, maxSize);
        case MemoryContextType::STACK_CONTEXT:
            return StackAllocSetContext::Create(parent, name, minContextSize, initBlockSize, maxBlockSize,
                                                contextType, maxSize);
        case MemoryContextType::MEMALIGN_CONTEXT:
        case MemoryContextType::MEMALIGN_SHRCTX:
            /* TODO: create an instance of AlignAllocSetContext */
        case MemoryContextType::INVALID_CONTEXT:
        case MemoryContextType::MAX_CONTEXT:
        default:
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("unrecognized context type %d",
                static_cast<int>(contextType)));
            break;
    }
    return nullptr;
}

void *DstorePallocExtendedDebug(Size size, uint32 flags, const char *file, int line)
{
    if (!AllocSizeIsValid(size)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("invalid memory allocate request size %lu",
            static_cast<unsigned long>(size)));
        return nullptr;
    }

    StorageAssert(DstoreMemoryContextIsValid(g_dstoreCurrentMemoryContext));

    void *ret;
    if ((flags & MCXT_ALLOC_ZERO) != 0) {
        ret = (g_dstoreCurrentMemoryContext->allocSet)->allocate0(0, size, file, line);
    } else {
        ret = (g_dstoreCurrentMemoryContext->allocSet)->allocate(0, size, file, line);
    }

    if (ret == nullptr) {
        /* If flag has not MCXT_ALLOC_NO_OOM, we must set_error ERROR here */
        if (unlikely((flags & MCXT_ALLOC_NO_OOM) != 0 && thrd != nullptr && thrd->error != nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed on request of size %lu bytes in %s:%d.",
                static_cast<unsigned long>(size), file, line));
            return nullptr;
        }
        return nullptr;
    }

    /* check if the session used memory is beyond the limitation */
    /* todo: MemoryContextCheckSessionMemory */

    return ret;
}
void *DstoreRepallocDebug(void *pointer, Size size, const char *file, int line)
{
    StorageAssert(DstorePointerIsValid(pointer));
    if (!AllocSizeIsValid(size)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("invalid memory allocate request size %lu",
            static_cast<unsigned long>(size)));
        return nullptr;
    }

    DstoreAllocSetContext *allocSetContext = ChunkGetAllocSetContext(pointer);
    UNUSE_PARAM DstoreMemoryContext context = allocSetContext->GetMemoryContext();

    /* is_reset must be false already */
    StorageAssert(!context->is_reset);

    void *ret = allocSetContext->reallocate(pointer, 0, size, file, line);
    if (unlikely(ret == nullptr && thrd != nullptr && thrd->error != nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed on request of size %lu bytes under queryid in %s:%d.",
            static_cast<unsigned long>(size), file, line));
        return nullptr;
    }

#ifdef MEMORY_CONTEXT_CHECKING
    /* check if the memory context is out of control */
    CheckMemoryContextSize(context, size);
#endif
    /* check if the session used memory is beyond the limitation */
    /* TODO: MemoryContextCheckSessionMemory */
    return ret;
}

void DstorePfree(void *pointer) noexcept
{
    StorageAssert(DstorePointerIsValid(pointer));

    /* BaseObject's operator new routes through DstorePallocAligned so the object
     * meets __STDCPP_DEFAULT_NEW_ALIGNMENT__. DstorePfreeExt callers on such
     * objects land here with the aligned-header prefix still in front of the
     * pointer — detect the magic and dispatch to the aligned free path. */
    if (DstoreIsAlignedMem(pointer)) {
        DstorePfreeAlignedImpl(pointer);
        return;
    }

    DstoreAllocSetContext *allocSetContext = ChunkGetAllocSetContext(pointer);
    UNUSE_PARAM DstoreMemoryContext context = allocSetContext->GetMemoryContext();

    StorageAssert(DstoreMemoryContextIsValid(context));

    allocSetContext->free_pointer(pointer);
}

const char* StorageMemoryMgr::m_memory_context_name[STORAGE_MEMORY_TYPE_COUNT] = {
    "StorageInstanceTopMemoryContext",
    "StorageSessionTopMemoryContext",
    "StorageThreadTopMemoryContext"
};

RetStatus StorageMemoryMgr::Init(DstoreMemoryContext top, StorageMemoryMgrType type)
{
    bool isSharedMem = (type == STORAGE_INSTANCE_MEMORY);
    m_type = type;
    m_topContext = top;
    g_dstoreCurrentMemoryContext = m_topContext;
    m_group = DstoreNew(m_topContext) MemoryContextGroup();
    if (STORAGE_VAR_NULL(m_group)) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_group->Init(m_topContext, isSharedMem))) {
        DstorePfreeExt(m_group);
        return DSTORE_FAIL;
    }
    (void)DstoreMemoryContextSwitchTo(GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    return DSTORE_SUCC;
}

void StorageMemoryMgr::reset_group_context(MemoryGroupType type)
{
    DstoreMemoryContextResetAndDeleteChildren(GetGroupContext(type));
}

void StorageMemoryMgr::destroy(StorageMemoryMgr* mgr)
{
    StorageAssert(mgr != nullptr);
    DstoreMemoryContextDestroyTop(mgr->GetRoot());
    g_dstoreCurrentMemoryContext = nullptr;
}

StorageMemoryMgr *StorageMemoryMgr::create_memory_mgr(StorageMemoryMgrType type)
{
    bool is_shared = (type == STORAGE_INSTANCE_MEMORY);
    DstoreMemoryContext topContext =
        DstoreAllocSetContextCreate(nullptr, m_memory_context_name[type], ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
        is_shared ? MemoryContextType::SHARED_CONTEXT : MemoryContextType::THREAD_CONTEXT);
    if (STORAGE_VAR_NULL(topContext)) {
        return nullptr;
    }
    StorageMemoryMgr *mgr = DstoreNew(topContext) StorageMemoryMgr();
    if (STORAGE_VAR_NULL(mgr)) {
        DstoreMemoryContextDestroyTop(topContext);
        return nullptr;
    }
    if (STORAGE_FUNC_FAIL(mgr->Init(topContext, type))) {
        DstorePfree(mgr);
        DstoreMemoryContextDestroyTop(topContext);
        return nullptr;
    }
    return mgr;
}

const uint64 ALIGNED_MAGIC_NUM = 0xFEFEFEFEFEFEDEAD;
inline unsigned long long dstore_get_aligned_magic_offset()
{
    unsigned long long offset = 0;
    void *pos = (void *)0x100;
    AllocChunk chunk = AllocPtrGetChunk(pos);
    offset = (unsigned long long)pos - (unsigned long long)chunk;
    return offset;
}

inline unsigned long long dstore_get_aligned_header_len()
{
    return (dstore_get_aligned_magic_offset() + sizeof(unsigned char *));
}

void *DstorePallocAligned(size_t size, size_t alignSize, DstoreMemoryContext ctx)
{
    DstoreMemoryContext memContext;
    if (ctx == nullptr) {
        memContext = g_dstoreCurrentMemoryContext;
    } else {
        memContext = ctx;
    }
    unsigned char *rawData = static_cast<unsigned char *>(DstoreMemoryContextAlloc(
        memContext, size + alignSize + dstore_get_aligned_header_len()));
    if (unlikely(rawData == nullptr)) {
        return nullptr;
    }
    return DstoreHandleAligned(rawData, alignSize);
}

void *DstorePallocAlignedHugeMemory(size_t size, size_t alignSize, DstoreMemoryContext ctx)
{
    DstoreMemoryContext memContext;
    if (ctx == nullptr) {
        memContext = g_dstoreCurrentMemoryContext;
    } else {
        memContext = ctx;
    }
    unsigned char *rawData = static_cast<unsigned char *>(DstoreMemoryContextAllocHugeSize(
        memContext, size + alignSize + dstore_get_aligned_header_len()));
    if (unlikely(rawData == nullptr)) {
        return nullptr;
    }
    return DstoreHandleAligned(rawData, alignSize);
}

void DstorePfreeAligned(void *ptr) noexcept
{
    DstorePfreeAlignedImpl(ptr);
}

void *DstoreHandleAligned(unsigned char *rawData, size_t alignSize)
{
    unsigned char **alignedData = reinterpret_cast<unsigned char **>(TYPEALIGN(
        alignSize, reinterpret_cast<unsigned long long>(rawData) + dstore_get_aligned_header_len()));
    unsigned char** magicNum = reinterpret_cast<unsigned char**>(
        (reinterpret_cast<unsigned long long>(alignedData) - dstore_get_aligned_magic_offset()));
    *(uint64*)magicNum = ALIGNED_MAGIC_NUM;
    magicNum[-1] = rawData;
    StorageAssert(reinterpret_cast<unsigned long long>(rawData) <=
        reinterpret_cast<unsigned long long>((&magicNum[-1])));
    return alignedData;
}

void DstorePfreeAlignedImpl(void *ptr)
{
    if (unlikely(ptr == nullptr)) {
        return;
    }
    unsigned char** magicNum = (unsigned char**)((void *)((char *)ptr - dstore_get_aligned_magic_offset()));
    StorageAssert(magicNum != nullptr);
    unsigned char* rawData = magicNum[-1];
    DstorePfreeExt(rawData);
}

bool DstoreIsAlignedMem(void *p)
{
    unsigned char** magicNum = (unsigned char**)((void *)((char *)p - dstore_get_aligned_magic_offset()));
    if (*(uint64*)magicNum == ALIGNED_MAGIC_NUM) {
        return true;
    }
    return false;
}

/* remove it later */
RetStatus CreateMemoryContextForTool(const char *name)
{
    g_dstoreCurrentMemoryContext = DstoreAllocSetContextCreate(nullptr, name, ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, MemoryContextType::THREAD_CONTEXT);
    if (STORAGE_VAR_NULL(g_dstoreCurrentMemoryContext)) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

AutoMemCxtSwitch::AutoMemCxtSwitch(DstoreMemoryContext memContext) : m_oldMemContext(nullptr), m_oldTop(0)
{
    if (memContext == nullptr) {
        m_oldMemContext = g_dstoreCurrentMemoryContext;
    } else {
        m_oldMemContext = DstoreMemoryContextSwitchTo(memContext);
    }

    if (memContext != nullptr && memContext->type == MemoryContextType::STACK_CONTEXT) {
        m_oldTop = (StackAllocSet(memContext->allocSet))->GetStackTop();
    }
}

AutoMemCxtSwitch::~AutoMemCxtSwitch()
{
    if (g_dstoreCurrentMemoryContext != nullptr &&
        g_dstoreCurrentMemoryContext->type == MemoryContextType::STACK_CONTEXT) {
        (StackAllocSet(g_dstoreCurrentMemoryContext->allocSet))->Restore(m_oldTop);
    }
    (void)DstoreMemoryContextSwitchTo(m_oldMemContext);
    m_oldMemContext = nullptr;
}

void AutoMemCxtSwitch::RestoreStackTop()
{
    StorageAssert(g_dstoreCurrentMemoryContext != nullptr &&
                  g_dstoreCurrentMemoryContext->type == MemoryContextType::STACK_CONTEXT);
    (StackAllocSet(g_dstoreCurrentMemoryContext->allocSet))->Restore(m_oldTop);
}

/* DstoreMemcpySafelyForHugeSize
 *      memcpy_s for huge size.
 *
 * memcpy_s will check input count, if count > SECUREC_MEM_MAX_LEN, memcpy_s will report error
 * if need to memcpy_s a huge size buffer, copy it segmentally
 * max_size is for ut, recommend not use it.
 */
void DstoreMemcpySafelyForHugeSize(void *dest, size_t destSize, const void *src, size_t count, size_t maxSize)
{
    int rc = 0;
    size_t memMaxLen = maxSize == 0 ? SECUREC_MEM_MAX_LEN : maxSize;
    if (unlikely(count > memMaxLen)) {
        ErrLog(DSTORE_LOG, MODULE_COMMON, ErrMsg("DstoreMemcpySafelyForHugeSize destSize %zu, count %zu, "
            "memMaxLen %zu", destSize, count, memMaxLen));
        size_t remaining = count;
        size_t copied = 0;

        while (remaining > 0) {
            size_t chunk_size = remaining > memMaxLen ? memMaxLen : remaining;
            rc = memcpy_s((char*)dest + copied, destSize - copied, (const char*)src + copied, chunk_size);
            storage_securec_check(rc, "\0", "\0");
            copied += chunk_size;
            remaining -= chunk_size;
        }
    } else {
        rc = memcpy_s(dest, destSize, src, count);
        storage_securec_check(rc, "\0", "\0");
    }
}

}  /* namespace DSTORE */
