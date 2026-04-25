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
 * dstore_mctx.h
 *
 *
 * IDENTIFICATION
 *        include/common/memory/dstore_mctx.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_MCXT_H
#define DSTORE_MCXT_H
#include <new>
#include "common/dstore_datatype.h"
#include "common/algorithm/dstore_ilist.h"

namespace DSTORE {


#ifdef DSTORE_USE_ASSERT_CHECKING
#define MEMORY_CONTEXT_CHECKING
#endif

#define MaxAllocSize ((Size)0x3fffffff) /* 1 gigabyte - 1 */
#define MaxAllocHugeSize ((Size)-1 >> 1) /* SIZE_MAX / 2 */
#define MaxContextNameLen 64
/*
 * Flags for MemoryContextAllocExtended.
 */
#define MCXT_ALLOC_HUGE 0x01   /* allow huge allocation (> 1 GB) */
#define MCXT_ALLOC_NO_OOM 0x02 /* no failure if out-of-memory */
#define MCXT_ALLOC_ZERO 0x04   /* zero allocated memory */

/*
 * Recommended default allocate parameters, suitable for "ordinary" contexts
 * that might hold quite a lot of data.
 */
#define ALLOCSET_DEFAULT_MINSIZE 0
#define ALLOCSET_DEFAULT_INITSIZE (8 * 1024)
#define ALLOCSET_DEFAULT_MAXSIZE (8 * 1024 * 1024)
#define ALLOCSET_DEFAULT_SIZES ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE

/*
 * Recommended alloc parameters for "small" contexts that are never expected
 * to contain much data (for example, a context to contain a query plan).
 */
#define ALLOCSET_SMALL_MINSIZE	 0
#define ALLOCSET_SMALL_INITSIZE  (1 * 1024)
#define ALLOCSET_SMALL_MAXSIZE	 (8 * 1024)
#define ALLOCSET_SMALL_SIZES \
        ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE

enum class MemoryContextType : uint8 {
    INVALID_CONTEXT = 0,    /* invalid type */
    THREAD_CONTEXT,         /* thread level context, only can be used by current thread */
    SESSION_CONTEXT,        /* session level context, used by different threads but no concurrency */
    STACK_CONTEXT,          /* a simple context, do not support free_pointer single pointer */
    SHARED_CONTEXT,         /* shared context used by different threads */
    MEMALIGN_CONTEXT,       /* the context only used to allocate the aligned memory */
    MEMALIGN_SHRCTX,        /* the shared context only used to allocate the aligned memory */
    MAX_CONTEXT,
};

class DstoreAllocSetContext;
using DstoreMemoryContext = DstoreMemoryContextData*;
struct DstoreMemoryContextData {
    MemoryContextType type;         /* identifies exact kind of context */
    DstoreAllocSetContext *allocSet;      /* allocate set the memory context belongs to */
    DstoreMemoryContext parent;           /* NULL if no parent (toplevel context) */
    DstoreMemoryContext topnode;          /* Root node that points to the tree */
    DstoreMemoryContext firstChild;       /* head of linked list of children */
    DstoreMemoryContext prevChild;        /* previous child of same parent */
    DstoreMemoryContext nextChild;        /* next child of same parent */
    char name[MaxContextNameLen];         /* context name (just for debugging) */
    pthread_rwlock_t lock;          /* lock to protect members if the context is shared */
    pthread_rwlock_t tree_lock;     /* If the tree is in the share state, it is used to lock the entire tree */
    bool is_shared;                 /* context is shared by threads */
    bool is_reset;                  /* T = no space alloced since last reset */
    int level;                      /* context level */
    uint64 session_id;              /* session id of context owner */
    ThreadId thread_id;             /* thread id of context owner */
    slist_node cell;                /* cell to pointer to this context */
};

struct DstoreMemoryAllocDebugInfo {
    const char *file;
    int line;
    uint32 magicNum;
};

/* TODO: The following 5 variables should be moved into ThreadContext later */
extern THR_LOCAL DstoreMemoryContext g_dstoreCurrentMemoryContext;

#define DEFAULT_MEMORY_CONTEXT_MAX_SIZE 0                  /* 0 MB as default value for AllocSetContextCreat function */
#define SHARED_MEMORY_CONTEXT_MAX_SIZE (100 * 1024 * 1024) /* 100 MB */
#define SELF_GENRIC_MEMCTX_LIMITATION (10 * 1024 * 1024)   /* 10MB */

extern DstoreMemoryContext DstoreAllocSetContextCreate(DstoreMemoryContext parent, const char *name,
    Size minContextCize, Size initBlockCize, Size maxBlockSize,
    MemoryContextType contextType = MemoryContextType::THREAD_CONTEXT,
    Size maxSize = DEFAULT_MEMORY_CONTEXT_MAX_SIZE);
extern void DstoreMemoryContextDelete(DstoreMemoryContext context) noexcept;
extern DstoreMemoryContext DstoreMemoryContextSwitchTo(DstoreMemoryContext context);
extern void DstoreMemoryContextReset(DstoreMemoryContext context);
extern void DstoreMemoryContextResetAndDeleteChildren(DstoreMemoryContext context);
extern void DstoreMemoryContextDestroyTop(DstoreMemoryContext context);
#ifdef MEMORY_CONTEXT_CHECKING
extern void DstoreMemoryContextCheck(DstoreMemoryContext context, bool onlyCheckThreadConsistent = true);
#endif
extern void DstoreAllocSetCheckPointer(void* pointer);
extern Size DstoreGetMemoryChunkSpace(void *pointer);
extern void DstorePfree(void *pointer) noexcept;
extern void *DstorePallocExtendedDebug(Size size, uint32 flags, const char *file, int line);
extern void *DstoreRepallocDebug(void *pointer, Size size, const char *file, int line);
extern bool DstoreMemoryContextIsValid(DstoreMemoryContext context);
extern char *DstoreMemoryContextStrdupDebug(DstoreMemoryContext context, const char* string,
                                            const char* file, int line);
extern void *DstoreMemoryContextAllocDebug(DstoreMemoryContext context, Size size, const char *file, int line);
extern void *DstoreMemoryContextAllocHugeMemoryDebug(DstoreMemoryContext context, Size size,
                                                     const char *file, int line);
extern void *DstoreMemoryContextAllocDebugZero(DstoreMemoryContext context, Size size, const char *file, int line);
extern bool DstoreIsAlignedMem(void *p);
extern void *DstorePallocAligned(size_t size, size_t alignSize, DstoreMemoryContext ctx = nullptr);
extern void *DstorePallocAlignedHugeMemory(size_t size, size_t alignSize, DstoreMemoryContext ctx = nullptr);
extern void *DstoreHandleAligned(unsigned char *rawData, size_t alignSize);
extern void DstorePfreeAligned(void *ptr) noexcept;
extern void DstorePfreeAlignedImpl(void *ptr);
extern void DstoreMemcpySafelyForHugeSize(void *dest, size_t destSize, const void *src, size_t count,
                                          size_t maxSize = 0);
/**
 * These functions are used for some threads that doesn't support memory control,
 * like when process failure recovery, if the thread run out of memory, we can't report error
 * to CMC and the recovery will stop, end up with unknown state. Without memory control,
 * recovery still somehow works because cgroup will kill the process if oom, and CMC will start
 * another failure recovery on one of the living nodes.
 */
extern void DstoreSetMemoryOutOfControl();
extern void DstoreSetMemoryInControl();

#define DstorePallocExtended(size, flags) DstorePallocExtendedDebug(size, flags, __FILE__, __LINE__)
#define DstoreRepalloc(pointer, size) DstoreRepallocDebug(pointer, size, __FILE__, __LINE__)
#define DstoreMemoryContextAllocZero(context, size) DstoreMemoryContextAllocDebugZero(context, size, __FILE__, __LINE__)
#define DstorePalloc0(size) DstoreMemoryContextAllocZero(g_dstoreCurrentMemoryContext, (size))
#define DstorePalloc(size) DstoreMemoryContextAlloc(g_dstoreCurrentMemoryContext, (size))
#define Dstorepstrdup(str) DstoreMemoryContextStrdup(g_dstoreCurrentMemoryContext, (str))
#define DstoreMemoryContextStrdup(context, size) DstoreMemoryContextStrdupDebug(context, size, __FILE__, __LINE__)
#define DstoreMemoryContextAlloc(context, size) DstoreMemoryContextAllocDebug(context, size, __FILE__, __LINE__)
#define DstoreMemoryContextAllocHugeSize(context, size) \
    DstoreMemoryContextAllocHugeMemoryDebug(context, size, __FILE__, __LINE__)

#define DstorePfreeExt(ptr)           \
    do {                                \
        if ((ptr) != nullptr) {         \
            DstorePfree(static_cast<void *>(ptr));       \
            ptr = nullptr;              \
        }                               \
    } while (0)

#define DstoreNew(pmc) new((pmc), __FILE__, __LINE__, std::nothrow)

inline bool DstoreIsAlignedAddr(void *ptr, size_t alignSize)
{
    return reinterpret_cast<size_t>(ptr) % alignSize == 0;
}

/* BaseObject is a basic class
 *      All other class should inherit from BaseObject class which
 * override operator new/delete.
 */
class BaseObject {
public:
    BaseObject() = default;
    ~BaseObject() = default;
    DISALLOW_MOVE(BaseObject);

    /*
     * boot strap only set true during top memory context initialize in process/thread/session start up.
     * */
    static void *operator new(Size size, DstoreMemoryContext parentMctx, const char *file, int line,
        UNUSE_PARAM const std::nothrow_t& nothrow = std::nothrow, bool bootstrap = false) noexcept
    {
        if (likely(bootstrap == false)) {
            /* Must match __STDCPP_DEFAULT_NEW_ALIGNMENT__ (16 on x86_64) so the compiler
             * can emit aligned SIMD moves inside member constructors. */
            return DstorePallocAligned(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__, parentMctx);
        } else {
            return malloc(size);
        }
    }

    static void *operator new[](Size size, DstoreMemoryContext parentMctx, const char *file, int line,
        UNUSE_PARAM const std::nothrow_t& nothrow = std::nothrow) noexcept
    {
        return DstorePallocAligned(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__, parentMctx);
    }

    static void *operator new(UNUSE_PARAM size_t size, void *start, UNUSE_PARAM const char *file, UNUSE_PARAM int line,
        UNUSE_PARAM const std::nothrow_t& nothrow = std::nothrow) noexcept
    {
        return start;
    }

    static void operator delete(void *p)
    {
        if (unlikely(p == nullptr)) {
            return;
        }
        if (DstoreIsAlignedMem(p)) {
            DstorePfreeAligned(p);
        } else {
            DstorePfree(p);
        }
    }

    static void operator delete[](void *p)
    {
        DstorePfree(p);
    }

protected:
    BaseObject(const BaseObject &) = default;
    BaseObject &operator=(const BaseObject &) = default;
};

enum MemoryGroupType : uint8 {
    /* becareful this group will have the same life span as instance or thread,
     *  so misuse will cause memory blow unexpected */
    MEMORY_CONTEXT_LONGLIVE = 0,
    MEMORY_CONTEXT_SMGR,
    MEMORY_CONTEXT_ERROR,
    MEMORY_CONTEXT_BUFFER,
    MEMORY_CONTEXT_TRANSACTION,
    MEMORY_CONTEXT_LOCK,
    MEMORY_CONTEXT_QUERY, /* it will be destroyed by per query base */
    MEMORY_CONTEXT_STACK,
    MEMORY_CONTEXT_MAX
};

class MemoryContextGroup : public BaseObject {
public:
    MemoryContextGroup() : m_memoryContexts{}
    {}

    ~MemoryContextGroup() = default;

    void RollBackInit(uint8 index)
    {
        for (uint8 i = 0; i < index; i++) {
            DstoreMemoryContextDelete(m_memoryContexts[i]);
            m_memoryContexts[i] = nullptr;
        }
    }
    RetStatus Init(DstoreMemoryContext parent, bool isShared = false)
    {
        for (uint8 i = 0; i < static_cast<uint8>(MEMORY_CONTEXT_MAX); i++) {
            MemoryContextType mcxtType = MemoryContextType::MAX_CONTEXT;
            if (i == static_cast<uint8>(MEMORY_CONTEXT_STACK)) {
                mcxtType = MemoryContextType::STACK_CONTEXT;
            } else {
                mcxtType = isShared ? MemoryContextType::SHARED_CONTEXT : MemoryContextType::THREAD_CONTEXT;
            }
            m_memoryContexts[i] = DstoreAllocSetContextCreate(parent,
                                                              m_memory_context_name[i],
                                                              ALLOCSET_DEFAULT_MINSIZE,
                                                              ALLOCSET_DEFAULT_INITSIZE,
                                                              ALLOCSET_DEFAULT_MAXSIZE,
                                                              mcxtType);
            if (STORAGE_VAR_NULL(m_memoryContexts[i])) {
                RollBackInit(i);
                return DSTORE_FAIL;
            }
        }
        return DSTORE_SUCC;
    }

    DstoreMemoryContext GetMemCxtGroup(MemoryGroupType type)
    {
        return m_memoryContexts[type];
    }

private:
    const char *m_memory_context_name[MEMORY_CONTEXT_MAX] = {
        "LongLiveMemoryContext",
        "StorageManagerMemoryContext",
        "ErrorMemoryContext",
        "BufferMgrMemoryContext",
        "TransactionMemoryContext",
        "LockMgrMemoryContext",
        "PerQueryMemoryContext",
        "StackMemoryContext"
    };

    DstoreMemoryContext m_memoryContexts[MEMORY_CONTEXT_MAX];
};

enum StorageMemoryMgrType {
    STORAGE_INSTANCE_MEMORY = 0,
    STORAGE_SESSION_MEMORY,
    STORAGE_THREAD_MEMORY,
    STORAGE_MEMORY_TYPE_COUNT
};

/**
 * Base Class for Storage Memory library memory control
 */
class StorageMemoryMgr : public BaseObject {
public:
    RetStatus Init(DstoreMemoryContext top, StorageMemoryMgrType type);

    inline DstoreMemoryContext GetGroupContext(MemoryGroupType type)
    {
        return m_group->GetMemCxtGroup(type);
    }

    /* delete all data in this group ,but keep the group context self */
    void reset_group_context(MemoryGroupType type);

    inline DstoreMemoryContext GetRoot()
    {
        return m_topContext;
    }

    static StorageMemoryMgr*  create_memory_mgr(StorageMemoryMgrType type);
    // clean all the memory.
    static void destroy(StorageMemoryMgr* mgr);
    DstoreMemoryContext         m_topContext;
private:
    static const char* m_memory_context_name[STORAGE_MEMORY_TYPE_COUNT];

    MemoryContextGroup         *m_group;
    StorageMemoryMgrType        m_type;
};

class AutoMemCxtSwitch {
public:
    explicit AutoMemCxtSwitch(DstoreMemoryContext memContext);
    ~AutoMemCxtSwitch();

    /*
     * Restore the stack top to the state when AutoMemCxtSwitch was created
     * can only be used for AutoMemCxtSwitch(MEMORY_CONTEXT_STACK)
     */
    void RestoreStackTop();

private:
    DstoreMemoryContext m_oldMemContext;
    uint32 m_oldTop;
};

}
#endif
