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
 * Description: Implement the blocking lock-free scheduler
 */
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <linux/futex.h>
#include "defines/err_code.h"
#include "defines/utils_errorcode.h"
#include "types/data_types.h"
#include "schedule/scheduler.h"
#include "schedule/scheduler_internal.h"
#include "schedule/blocking_lockfree_scheduler.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "thread_pool"

#define MIN_TASK_QUEUE_SIZE 1000
#define MAX_TASK_QUEUE_SIZE 10000000
#define QUEUE_TERMINTE      ((uint32_t)1)
/* consumer wait bit */
#define CONSUMER_BIT ((uint32_t)1 << 1)
/* productor wait bit */
#define PRODUCER_BIT        ((uint32_t)1 << 2)
#define BIT_MASK            ((uint64_t)(QUEUE_TERMINTE | CONSUMER_BIT | PRODUCER_BIT))
#define TASK_ENTRY_BIT_MASK (~BIT_MASK)

struct TaskEntry {
    uint64_t enqueueTime;
    ThreadPoolTask task;
};

static inline int *TaskEntryToFutexAddr(TaskEntry **slot)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return (int *)slot;
#else
    /* futex address is the lower 4byte on 8byte slot. on big endian mode, lower byte is in high address */
    return (int *)slot + 1;
#endif
}

static inline int Futex(int *uaddr, int futexOp, int val, const struct timespec *timeout, int val3)
{
    return (int)syscall(SYS_futex, uaddr, futexOp, val, timeout, NULL, val3);
}

static inline int SimpleFutexWait(int *futexp, int expect, const struct timespec *timeout, uint32_t waitbit)
{
    if (Futex(futexp, FUTEX_WAIT_BITSET | FUTEX_PRIVATE_FLAG, expect, timeout, (int)waitbit) == 0) {
        /* success */
        return 0;
    }
    if (errno == EAGAIN) {
        /* The value pointed to by uaddr was not equal to the expected value val at the time of the call */
        return 0;
    }
    /* if EINTR occurred (operation was interrupted by a signal) or ETIMEDOUT or other errors, return to caller */
    return errno;
}

static inline int SimpleFutexWake(int *futexp, int waiters, uint32_t waitbit)
{
    int ret = Futex(futexp, FUTEX_WAKE_BITSET | FUTEX_PRIVATE_FLAG, waiters, NULL, (int)waitbit);
    if (ret == -1) {
        /* return -1 case (from linux man 2 futex page):
           1. required futexp argument did not point to a valid user-space address
           2. the kernel detected an inconsistency between the user-space state at uaddr and the kernel
              state--that is, it detected a waiter which waits in FUTEX_LOCK_PI on uaddr */
        return -errno;
    }
    /* returns the number of waiters that were woken up. */
    return ret;
}

static inline void *SchedulerMemAlloc(MemAllocator *allocator, uint64_t size)
{
    if ((allocator != NULL) && (allocator->alloc != NULL)) {
        return allocator->alloc(allocator, size);
    }
    return malloc(size);
}

static inline void SchedulerMemFree(MemAllocator *allocator, void *ptr)
{
    if ((allocator != NULL) && (allocator->free != NULL)) {
        allocator->free(allocator, ptr);
    } else {
        free(ptr);
    }
}

static inline void *Run(TaskEntry *self)
{
    ASSERT((self->task.routine != NULL) || (self->task.run != NULL));
    if (self->task.routine) {
        return self->task.routine(self->task.args);
    } else if (self->task.run) {
        return self->task.run(&self->task.args);
    }
    return NULL;
}

static inline void Finalize(TaskEntry *self)
{
    if (self->task.finalize) {
        self->task.finalize(self->task.args);
    }
}

static TaskEntry *GetTaskFromQueue(BLFScheduler *self)
{
    /* get queue slot */
    uint32_t gIndex = __atomic_fetch_add(&self->consumeIndex, 1, __ATOMIC_RELAXED);
    uint32_t index = gIndex % self->taskQueueSize;
    TaskEntry **slot = &self->queue[index].entry;
    int *volatile futexp = TaskEntryToFutexAddr(slot);
    for (;;) {
        uintptr_t entry = (uintptr_t)__atomic_load_n(slot, __ATOMIC_RELAXED);
        if (((entry & QUEUE_TERMINTE) != 0) || (self->state != SCHEDULING)) {
            /* scheduler is terminate */
            break;
        }
        if ((entry & TASK_ENTRY_BIT_MASK) == 0) {
            /* this slot have not task, wait */
            if (((entry & CONSUMER_BIT) == 0) &&
                (!__atomic_compare_exchange_n(slot, &entry, (TaskEntry *)(uintptr_t)(entry | CONSUMER_BIT), true,
                                              __ATOMIC_RELAXED, __ATOMIC_RELAXED))) {
                /* before consumer wait need mark the CONSUMER_BIT, if CONSUMER_BIT not marked and atomic cas mark
                   CONSUMER_BIT fail (maybe this slot had product a task at this moment), retry */
                continue;
            }
            int ret = SimpleFutexWait(futexp, (int)(uint32_t)(entry | CONSUMER_BIT), NULL, CONSUMER_BIT);
            if ((ret != 0) && (ret != EINTR)) {
                ErrLog(ERROR, ErrMsg("Get task failed, wait %d", ret));
            }
            continue;
        }
        /* this slot have been product, consume it */
        if (!__atomic_compare_exchange_n(slot, &entry, (TaskEntry *)(uintptr_t)(entry & (BIT_MASK & ~PRODUCER_BIT)),
                                         true, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
            continue;
        }
        if ((entry & PRODUCER_BIT) != 0) {
            /* already get task, need wakeup producer, if have a producer thread wait on this slot
               for want to put a task in this slot. this case may happen
               For example, because consumption is too slow, all slots are filled with unscheduled tasks. */
            int ret = SimpleFutexWake(futexp, 1, PRODUCER_BIT);
            if (unlikely(ret < 0)) {
                ErrLog(ERROR, ErrMsg("wake producer failed, SimpleFutexWake %d", ret));
            }
        }
        TaskEntry *task = (TaskEntry *)(entry & TASK_ENTRY_BIT_MASK);
        return task;
    }

    return NULL;
}

static ErrorCode BLFSchedulerSubmitTaskImpl(Scheduler *super, ThreadPoolTask *task, SYMBOL_UNUSED TaskId *taskId,
                                            SYMBOL_UNUSED bool highPriority)
{
    BLFScheduler *self = DOWN_TYPE_CAST(super, BLFScheduler);

    TaskEntry *taskEntry = (TaskEntry *)SchedulerMemAlloc(self->allocator, sizeof(TaskEntry));
    if (taskEntry == NULL) {
        return UTILS_ERROR_THREADPOOL_NOMEMORY;
    }
    errno_t rc = memset_s(taskEntry, sizeof(*taskEntry), 0, sizeof(*taskEntry));
    if (unlikely(rc != EOK)) {
        SchedulerMemFree(self->allocator, taskEntry);
        ErrLog(ERROR, ErrMsg("BLFSchedulerSubmitTaskImpl failed, can't memset_s, rc is %d!", rc));
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }
    /* get queue slot */
    uint32_t pIndex = __atomic_fetch_add(&self->produceIndex, 1, __ATOMIC_RELAXED);
    uint32_t index = pIndex % self->taskQueueSize;
    TaskEntry **slot = &self->queue[index].entry;
    int *futexp = TaskEntryToFutexAddr(slot);
    taskEntry->enqueueTime = GetMonotonicTimeUs();
    taskEntry->task = *task;

    for (;;) {
        uintptr_t entry = (uintptr_t)__atomic_load_n(slot, __ATOMIC_RELAXED);
        if (((entry & QUEUE_TERMINTE) != 0) || (self->state != SCHEDULING)) {
            /* scheduler is terminate */
            break;
        }
        if ((entry & TASK_ENTRY_BIT_MASK) != 0) {
            /* this slot have not be consumed, wait */
            /* submit task wait queue, maybe the queue is full or work thread abnormal. */
            if (((entry & PRODUCER_BIT) == 0) &&
                (!__atomic_compare_exchange_n(slot, &entry, (TaskEntry *)(uintptr_t)(entry | PRODUCER_BIT), true,
                                              __ATOMIC_RELAXED, __ATOMIC_RELAXED))) {
                /* before producer wait need mark the PRODUCER_BIT, if PRODUCER_BIT not marked and atomic cas mark
                   PRODUCER_BIT fail (maybe this slot became empty at this moment), retry */
                continue;
            }
            int ret = SimpleFutexWait(futexp, (int)(uint32_t)(entry | PRODUCER_BIT), NULL, PRODUCER_BIT);
            if ((ret != 0) && (ret != EINTR)) {
                ErrLog(ERROR, ErrMsg("Schedule task failed, wait %d", ret));
            }
            continue;
        }
        /* this slot is empty */
        if (!__atomic_compare_exchange_n(
                slot, &entry, (TaskEntry *)(uintptr_t)((uintptr_t)taskEntry | (entry & (BIT_MASK & ~CONSUMER_BIT))),
                true, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
            continue;
        }
        /* ok, suceess produce a task, if any consumer had wait this slot, wake up it */
        if ((entry & CONSUMER_BIT) != 0) {
            int ret = SimpleFutexWake(futexp, 1, CONSUMER_BIT);
            if (unlikely(ret < 0)) {
                ErrLog(ERROR, ErrMsg("wake consumer failed, SimpleFutexWake %d", ret));
            }
        }
        return ERROR_SYS_OK;
    }
    SchedulerMemFree(self->allocator, taskEntry);
    return ERROR_SYS_OK;
}

static int32_t BLFSchedulerTaskImpl(Scheduler *super, ScheduleCallback *callback)
{
    BLFScheduler *self = DOWN_TYPE_CAST(super, BLFScheduler);
    TaskEntry *taskEntry = GetTaskFromQueue(self);
    if (taskEntry == NULL) {
        return SCHEDULE_STATE_EXIT;
    }

    if (self->state != SCHEDULING) {
        SchedulerMemFree(self->allocator, taskEntry);
        return SCHEDULE_STATE_EXIT;
    }
    /* before run */
    SetMaxValueSafety(&self->lastExecTaskTime, taskEntry->enqueueTime);
    UpdateSchedStats(callback, taskEntry->enqueueTime);
    TaskPreRun(callback,
               taskEntry->task.routine == NULL ? (void *)taskEntry->task.run : (void *)taskEntry->task.routine,
               &taskEntry->task, taskEntry->task.args);

    /* call the task routine function */
    int ret = Run(taskEntry) == (void *)(intptr_t)SCHEDULE_STATE_SHRINK ? SCHEDULE_STATE_SHRINK : SCHEDULE_STATE_OK;
    /* after run */
    TaskPostRun(callback);
    /* call the task finalize function */
    Finalize(taskEntry);

    SchedulerMemFree(self->allocator, taskEntry);
    return ret;
}

static void BLFSchedulerBroadcast(BLFScheduler *self)
{
    for (uint32_t i = 0; i < self->taskQueueSize; i++) {
        uintptr_t old = __atomic_fetch_add((uintptr_t *)&self->queue[i].entry, QUEUE_TERMINTE, __ATOMIC_RELAXED);
        if ((old & (CONSUMER_BIT | PRODUCER_BIT)) != 0) {
            int *futexp = TaskEntryToFutexAddr(&self->queue[i].entry);
            /* wakeup all waiters waited in this futex */
            (void)SimpleFutexWake(futexp, INT32_MAX, CONSUMER_BIT | PRODUCER_BIT);
        }
    }
}

static void BLFSchedulerDestroyImpl(Scheduler *super)
{
    BLFScheduler *self = DOWN_TYPE_CAST(super, BLFScheduler);
    self->state = DESTROYED;
    __atomic_thread_fence(__ATOMIC_RELEASE); /* write barrier */
    BLFSchedulerBroadcast(self);
}

/*
 * ------------------------------------------------
 * Define class of BLFScheduler
 * ------------------------------------------------
 */
static ErrorCode BLFSchedulerInit(BLFScheduler *super, BLFSchedulerInitParams *initParams)
{
    BLFScheduler *self = (BLFScheduler *)super;
    if (initParams == NULL) {
        ErrLog(ERROR, ErrMsg("New BLFScheduler failed, the initParams is NULL."));
        return UTILS_ERROR_THREADPOOL_NULLPARAMETER;
    }
    uint32_t taskQueueSize = initParams->taskQueueSize;
    self->allocator = initParams->allocator;
    self->state = SCHEDULING;
    self->taskQueueSize = Min(Max(MIN_TASK_QUEUE_SIZE, taskQueueSize), MAX_TASK_QUEUE_SIZE); // [1000, 10000000]
    self->produceIndex = 0;
    self->consumeIndex = 0;
    self->lastExecTaskTime = 0;
    self->queue = (BLFTaskQueue *)SchedulerMemAlloc(self->allocator, taskQueueSize * sizeof(BLFTaskQueue));
    if (self->queue == NULL) {
        ErrLog(ERROR, ErrMsg("New BLFScheduler failed, can't alloc memory!"));
        return UTILS_ERROR_THREADPOOL_NOMEMORY;
    }
    errno_t rc = memset_s(self->queue, taskQueueSize * sizeof(BLFTaskQueue), 0, taskQueueSize * sizeof(BLFTaskQueue));
    if (unlikely(rc != EOK)) {
        SchedulerMemFree(self->allocator, self->queue);
        ErrLog(ERROR, ErrMsg("New BLFScheduler failed, can't memset_s, rc is %d!", rc));
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }

    ErrLog(INFO, ErrMsg("Init BLF scheduler done."));
    return ERROR_SYS_OK;
}

static void BLFSchedulerFinalize(BLFScheduler *scheduler)
{
    BLFScheduler *self = (BLFScheduler *)scheduler;
    SchedulerMemFree(self->allocator, self->queue);
}

static void BLFSchedulerWakeUpWorkerImpl(Scheduler *super)
{
    ASSERT(super != NULL);
    BLFScheduler *self = DOWN_TYPE_CAST(super, BLFScheduler);
    BLFSchedulerBroadcast(self);
}

static uint64_t BLFScheduleGetEarliestImpl(Scheduler *super)
{
    BLFScheduler *self = DOWN_TYPE_CAST(super, BLFScheduler);
    if ((self->produceIndex > self->consumeIndex) && (self->consumeIndex != 0)) {
        return self->lastExecTaskTime;
    }
    return UINT64_MAX;
}

static ErrorCode BLFSchedulerCancelTaskImpl(SYMBOL_UNUSED Scheduler *super, SYMBOL_UNUSED TaskId taskId)
{
    ASSERT(super != NULL);
    ErrLog(INFO, ErrMsg("Cancel task not support in BLF scheduler yet."));
    return UTILS_ERROR_OPERATION_NOT_SUPPORT;
}

static void BLFSchedulerOpsInit(BLFScheduler *self)
{
    GET_FOPS(Scheduler)->getEarliestTaskSubmitTime = BLFScheduleGetEarliestImpl;
    GET_FOPS(Scheduler)->scheduleTask = BLFSchedulerTaskImpl;
    GET_FOPS(Scheduler)->submitTask = BLFSchedulerSubmitTaskImpl;
    GET_FOPS(Scheduler)->wakeUpWorker = BLFSchedulerWakeUpWorkerImpl;
    GET_FOPS(Scheduler)->destroy = BLFSchedulerDestroyImpl;
    GET_FOPS(Scheduler)->cancelTask = BLFSchedulerCancelTaskImpl;
}

DEFINE_NEW_TYPED_CLASS(BLFScheduler, Scheduler)
