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

#ifndef UT_DEBUG_POINT_API_H
#define UT_DEBUG_POINT_API_H

#include "common/instrument/trace/dstore_trace.h"
#include "common/dstore_datatype.h"
#include "errorcode/dstore_error_struct.h"
#include "port/dstore_port.h"
#include <mutex>
#include <pthread.h>  /* rwLockAttribute */
#include <atomic>

/*
 *     DEBUG POINT API  /  CONCURRENCY TESTING INFRASTRUCTURE
 *
 * DebugPoint API allows us to implement the Concurrency Testing Infrastructure
 * using the existing trace functionality. This is debugger-like approach that
 * is piggybacking on the trace calls inside the engine.
 *
 * To use the DebugPoint API in the UT, the following steps are needed:
 * 1) Include these header files where you use DebugPoint API:
 *        #include "ut_utilities/ut_debug_point_api.h"
 *        #include "common/instrument/trace/dstore_lock_trace.h"
 * 2) Initialize debug point API inside your TEST_F class SetUp() function:
 *        \* Create shared memory segment to use with the concurrency testing infrastructure. *\
 *        DebugPointMgr::GetDebugPointMgr()->Initialize();
 * 3) Initialize trace inside your test. Add the following at the beginning of the testcase:
 *        Trace::Initialize(&(CheckDebugPointCallback));
 *    and the following at the end:
 *        Trace::Initialize(nullptr);
 * 4) Use DebugPoint API to inject an action into the code.
 *    Please see ut_concurrency_testing_api.cpp for examples. Examples are
 *    shown here as well.
 *
 * DebugPoint API USER GUIDE.
 *
 * In order to test concurrency, we already have tools in our UT which allow us
 * to synchronize threads (using barriers) and nodes (using SyncPoint() between
 * virtual nodes). The problem is when we need to take an action (suspend
 * execution, for example) inside some engine function in the middle of
 * execution.
 * Usually we would use debugger to do that and DebugPoint API simulates the
 * debugger by using trace points (existing or the new ones). One may think of
 * the debug points as though they are debugger's breakpoints.
 *
 * Benefits of this approach:
 * 1) trace has its own value for debugging problems;
 * 2) it's almost free to add trace point inside the engine code;
 * 3) infrastructure may be delivered to the engine code and no need in double
 *    maintenance or any changes to the engine code;
 * 4) this approach may work on the production code and not only for UT;
 * 5) allows high level of control and complex scenarios.
 *
 * The DebugPoint API may be used for the following testing scenarios:
 * 1) To suspend the database engine inside any trace point (existing or newly
 *    added). This would allow us to do complex concurency testing scenarios
 *    where database instance is frozen in time in the state we need.
 *      * This suspension may be conditional. It may be controlled using the
 *        nodeId of the node where trace function is called. Or using the data
 *        that are being traced by that trace call.
 *      * API allows to resume suspended threads conditionally as well: using
 *        the specified list of nodes. Threads may be suspended for all or some
 *        nodes but they may be resumed only for nodes we need.
 * 2) To perform the error and fault injection. CHANGE_DATA action may be used
 *    to change the traced data using debug point. For instance, RetStatus of
 *    the function call may be changed to FAIL to test error handling inside
 *    the engine functions.
 * 3) To perform complex codepath validations. By setting a debug point and
 *    then checking how many threads have hit it using COUNT debug action, we
 *    may validate that scenario goes through the expected codepath.
 * 4) To support barriers for the engine code using the BARRIER debug action.
 *    Debug point API barriers may be setup for any (existing or newly added)
 *    trace point. Once the number of threads suspended in the debug point
 *    reaches the specified threshold, execution is resumed. In addition, these
 *    barriers synchronize threads between multiple nodes.
 * 2) To crash the engine at any trace point which would allow us to run
 *    complex crash recovery scenarios.
 * Notice that this may be done that without debugger and without affecting
 * engine performance.
 *
 * DebugPoint API (aka Concurrency Testing Infrastructure) uses the following
 * main components:
 *   1) SetupDebugPoint() function that stores the debug point meta-data in the
 *      control block stored in the shared memory. That data will be used by
 *      the Trace API via the CheckDebugPoint callback function.
 *      It sets the trace point ID (traceId, traceType, and probePoint) at
 *      which the specified action should be taken. It also sets the additional
 *      options that allow user to control when the action will be taken, e.g.
 *      target VnodeId, number of times the debug point has to be skipped, and
 *      so on.
 *   2) storage_trace_(entry|exit|data) functions in the engine code.
 *      These calls should either exist in the code or maybe added to it.
 *   3) CheckDebugPointCallback function that is set as follows:
 *          Trace::Initialize(&(CheckDebugPointCallback));
 *      and which will trigger action specified by the SetupDebugPoint() once
 *      the debug point is encountered by the trace function.
 *   4) ResumeDebugPoint() function that will resume execution for all threads
 *      suspended in the specified debug point.
 *        * Important: ResumeDebugPoint() needs to be executed in a thread that
 *          is not suspended and so you may need to create a separate thread to
 *          issue the ResumeDebugPoint() call.
 *        * ResumeDebugPoint() has an overloaded version that allows to resume
 *          threads on the specified list of nodes (one or more).
 *            * Example:
 *                  ResumeNodeList list = { 1, VNode2 };
 *                  ResumeDebugPoint(&debugPointId, &list);
 *              For more nodes: ResumeNodeList list = { 2, { VNode2, VNode3 } };
 *        * Important: ResumeDebugPoint() will wait until all threads are
 *          resumed (either on all nodes or on the specified list).
 *
 * Debug point API uses unique debug point Id to control how and when the specified
 * actions are taken. DebugPointid has 5 fields (traceType, traceId (function
 * Id), probeId, dataLen, data pointer) but not all of them are used based on
 * the type of the trace point:
 *   * TRACE_ENTRY and TRACE_EXIT only need traceType and traceId fields;
 *   * TRACE_DATA needs probeId field at least. If we want to filter using the
 *     data trace point is tracing, add dataLen and data pointer as well.
 *   * Setting dataLen to 0 means that TRACE_DATA point will be triggered for
 *     any storage_trace_data() call with the specified probeId irregardless of
 *     data its traces.
 * Note that inclusion of trace data in the DebugPointId allows us to specify
 * different actions for the same TRACE_DATA point based on the traced data. It
 * also allows to suspend and resume debug points based on their traced data.
 * Use designated initializers when defining debug point ids, e.g.
 *   DebugPointId debugPointId = {.targetTraceType = TRACE_ENTRY, .targetTraceId = MYFUNC_TRACE_ID};
 *   DebugPointId debugPointId = {.targetTraceType = TRACE_EXIT, .targetTraceId = MYFUNC_TRACE_ID};
 *   DebugPointId debugPointId = {.targetTraceType = TRACE_DATA, .targetTraceId = MYFUNC_TRACE_ID,
 *                                .targetProbeId = 1};
 *   int mydata = 7;
 *   DebugPointId debugPointId = {.targetTraceType = TRACE_DATA, .targetTraceId = MYFUNC_TRACE_ID,
 *                                .targetProbeId = 1, .targetDataLen = sizeof(mydata),
 *                                .targetData = &mydata};
 * That way all other fields will be value-initialized to 0 or nullptr.
 *
 * Important notes:
 *   * There could be only one action specified per debug point.
 *   * Trace is currently disabled on the release builds. On the other hand,
 *     this makes it easier to use this for UT as there is no perf impact.
 *
 * Debug point API supports the following debug actions:
 *   * DEBUG_ACTION_SUSPEND - suspend thread when debug point is reached.
 *      * Requires user to call ResumeDebugPoint() in a different thread to
 *        continue execution. All suspended threads will be resumed.
 *      * Use GetNumSuspended() to determine how many threads are suspended.
 *   * DEBUG_ACTION_BARRIER - threads are suspended in debug point until the
 *     specified barrier (set expectedSuspended in DebugOptions) is reached.
 *      * Works between threads on different VNodes (unlike pthread_barrier).
 *      * Doesn't require call to ResumeDebugPoint().
 *   * DEBUG_ACTION_COUNT   - counter is incremented every time thread reaches
 *     the trace point (debug point is a rule applied to a trace point).
 *      * Useful for the unit test codepath validation.
 *      * Use GetHitCount() to determine how many times trace point has been
 *        reached.
 *   * DEBUG_ACTION_CHANGE_DATA - the storage_trace_data() call is intercepted
 *     and its data is changed to the desired value.
 *      * May be used to change RetStatus of any engine function to test error
 *        injection in the engine code. For that, trace the return value using
 *        storage_trace_data().
 *      * For error injection, set debugOptions.errorCode as well.
 *      * Only integer data may be changed (which includes RetStatus).
 *   * DEBUG_ACTION_TRAP    - trigger SIGSEGV when trace point is reached.
 *      * Useful for crash recovery testing.
 *      * Use for manual testing for now as SIGSEGV crashes the unittest
 *        executable.
 * See the corresponding unit tests for examples of usage.
 *
 * In addition to DebugPointId, the DebugPoint API uses DebugPointOptions to
 * control the way debug actions are performed. For the basic functionality
 * defaults are enough and one needs only to declare them (defaults will be used):
 *     DebugPointOptions debugPointOptions;
 * Important options are:
 *   targetVNodeId array - actions will be taken only for threads running on
 *                         one of the target VNodeIds.
 *   expectedSuspended - used by BARRIER action, same as count for pthread_barrier().
 *   expectedNumPass - first expectedNumPass hits of debug point will be skipped.
 *                     Used to control which call to trace function is triggered.
 *
 * EXAMPLES OF USAGE: (see ut_concurrency_testing_api.cpp for more examples)
 * To trigger the COUNT debug action only on 2 nodes, use targetVNodeId[] array:
 *      DebugPointId debugPointId = {.targetTraceType = TRACE_DATA,
 *          .targetTraceId = TRACE_ID_MyFunc, .targetProbeId = 1};
 *      DebugPointOptions debugPointOptions;
 *      debugPointOptions.targetVNodeId[0] = VNode2;
 *      debugPointOptions.targetVNodeId[1] = VNode3;
 *      DebugPointMgr::GetDebugPointMgr()->SetupDebugPoint(&debugPointId, &debugPointOptions, DEBUG_ACTION_COUNT);
 *
 * To inject the failure in the DSTORE engine code, use the following approach.
 * In the engine code:
 *     ret = ComponentFunc(args);
 *     storage_trace_data(1, TRACE_ID_ComponentFunc, TRC_DATA_FMT_DFLT,
 *         reinterpret_cast<const char *>(&ret), sizeof(ret));
 * In the UT code, set debug point for the CHANGE_DATA action:
 *      DebugPointOptions debugPointOptions;
 *      --- This test will change the return value to failure.
 *      debugPointOptions.newIntValue = DSTORE_FAIL;
 *      --- errorCode should be set as well.
 *      debugPointOptions.errorCode = LOCK_ERROR_BUCKET_RELOCATION_FAILURE;
 *      DebugPointMgr::GetDebugPointMgr()->SetupDebugPoint(&debugPointId, &debugPointOptions,
 *          DEBUG_ACTION_CHANGE_DATA);
 */

namespace DSTORE {

/*
 * This value represents the number of trace points we can have.
 * The value is arbitrary and can be increased/decreased.
 */
static constexpr int UT_DEBUG_POINTS_NUM_MAX = 128;
/* Invalid debug point will have traceId set to 0. TraceId is never 0. */
static constexpr int UT_DEBUG_POINT_INVALID_SLOT = 0;
/* Maximum size of data that may be stored inside debug point slot. */
static constexpr int UT_DEBUG_POINT_MAX_DATA_SIZE_BYTES = 128;
/* sleep interval used while debug point is suspended (0.5s) */
static const long UT_DEBUG_POINT_SLEEP_WHILE_SUSPENDED_MS (500000L);
/* Error code used to indicate that debug point hasn't been found in the debug point array. */
static const int UT_DEBUG_POINT_NOT_FOUND = -1;
/*
 * Max number of nodes that may be specified for the debug point API. It's used
 * when resuming the suspended threads or when setting the target nodes for the
 * debug action.
 */
static const int UT_DEBUG_POINT_MAX_NUM_NODES = 10;

/* Actions that may be taken by threads hitting the debug points. */
typedef enum DebugAction {
    DEBUG_ACTION_SUSPEND,
    DEBUG_ACTION_COUNT,
    DEBUG_ACTION_BARRIER,
    DEBUG_ACTION_TRAP,
    DEBUG_ACTION_CHANGE_DATA,
} DebugAction;

/*
 * Debug points data contain: debug point ID (traceType, traceId, probeId),
 * debug point meta-data (target VnodeId, debugAction, etc) and different counters
 * (number of suspended threads, keepSuspended flag, etc).
 */
struct DebugPointData {
    TraceType traceType;
    uint32 traceId;
    uint32 probeId;
    uint32 vNodeId[UT_DEBUG_POINT_MAX_NUM_NODES];
    DebugAction debugAction;
    uint32 expectedNumSuspended;
    uint32 expectedNumPass;
    std::atomic<int> curNumSuspended;
    std::atomic<int> keepSuspended;
    uint32 curNumPass;
    uint32 useExtraChecks;
    uint32 numResumeNodes;
    uint32 resumeNodeIds[UT_DEBUG_POINT_MAX_NUM_NODES];
    std::atomic<int> numSuspendedPerNode[UT_DEBUG_POINT_MAX_NUM_NODES];
    uint32 traceDataLen;
    uint8  traceData[UT_DEBUG_POINT_MAX_DATA_SIZE_BYTES];
    int    newIntValue;
    ErrorCode errorCode;
};

/* Debug point control block used by the concurrency testing infrastructure. */
struct DebugPointControlBlock {
    pthread_rwlock_t rwLock;
    /* Data used by infrastructure. */
    /* Last "being used" slot/index in debugPoints array. */
    int lastUsedDebugPointIndex;
    /* Array of debug points and related counters (meta-data) */
    DebugPointData debugPoints[UT_DEBUG_POINTS_NUM_MAX];
};

/*
 * Info (5-tuple) that uniquely defines the debug point: debugPointId.
 * TRACE_ENTRY and TRACE_EXIT debug points need only traceType and traceId and
 * other fields are ignored.
 * TRACE_DATA points need probeId as well as dataLen and data pointer.
 * Note that if dataLen is set to 0, then both dataLen and data pointer will be
 * ignored and debug point will be matched only using probeId.
 * Use aggregate initialization when defining DebugPointId. In this way all
 * the fields that are not given an explicit initializer, will be value
 * initialized (to 0 or nullptr).
 */
struct DebugPointId {
    TraceType targetTraceType;
    uint32 targetTraceId;
    uint32 targetProbeId;
    uint32 targetDataLen;
    void *targetData;
};

/* Options that control behaviour of the debug point API. */
struct DebugPointOptions {
    /*
     * Actions will be taken only for threads running on one of the target
     * VNodes provided the target node (at least one) is set.
     * By default, all target nodes are set to -1 and will be skipped.
     */
    int targetVNodeId[UT_DEBUG_POINT_MAX_NUM_NODES] = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 };
    /* If not 0, will be used to implement barrier. */
    int expectedSuspended = 0;
    /* Skip first expectedNumPass hits. Only if curNumPass > expectedNumPass will action be excuted. */
    int expectedNumPass = 0;
    /* Sets the value used by the CHANGE_DATA action. */
    int newIntValue = 0;
    /* Sets the error code for the CHANGE_DATA action. */
    ErrorCode errorCode = STORAGE_OK;
};

/*
 * Data struct to specify list of nodes to use when resuming suspended debug
 * point. This is an optional parameter to ResumeDebugPoint().
 */
struct ResumeNodeList {
    uint32 numNodes;
    uint32 nodeIds[UT_DEBUG_POINT_MAX_NUM_NODES];
};

/*
 * CheckDebugPointCallback is used by the storage_trace API inside the dstore
 * engine. This function is kept outside the DebugPointMgr class to avoid
 * declaring it static which makes DebugPointMgr easier to maintain.
 */
void CheckDebugPointCallback(const TraceType type, const uint32_t probe, const uint32_t rec_id,
    const TraceDataFmt fmt_type, const char* data, size_t data_len);

/*
 * Singleton class which implements DebugPoint API (aka Concurrency Testing Infrastructure).
 * Please read API overview at the beginning of this file.
 */
class DebugPointMgr {
public:
    /* Ensure this is a singleton: disallow copy and move */
    DISALLOW_COPY_AND_MOVE(DebugPointMgr);
    /* Any access to this class should go through GetDebugPointMgr()-> */
    static DebugPointMgr *GetDebugPointMgr();

    void Initialize();
    void Destroy();
    void ResetSharedMemory();
    /* Main concurrency infrastructure driver to setup the debug points. */
    void SetupDebugPoint(DebugPointId *debugPointId, DebugPointOptions *debugPointOptions, DebugAction action);
    /* Called by storage_trace_xxx functions to take actions inside debug points. */
    void CheckDebugPoint(const TraceType type, const uint32_t probe, const uint32_t rec_id,
        const TraceDataFmt fmt_type, const char* data, size_t data_len);
    /*
     * ResumeDebugPoint resumes execution on all threads and all nodes that
     * were suspended (hit the debug point). Use targetVnodeId to suspend
     * threads for a particular node.
     */
    void ResumeDebugPoint(DebugPointId *debugPointId);
    void ResumeDebugPoint(DebugPointId *debugPointId, ResumeNodeList *nodeList);
    /* Function to get the number of suspended threads given the debug point. */
    int GetNumSuspended(DebugPointId *debugPointId);
    /* Function to determine whether the debug point has suspended threads. */
    bool IsDebugPointSuspended(DebugPointId *debugPointId);
    /* Function to get the number of times the debug point has been reached. */
    int GetHitCount(DebugPointId *debugPointId);
    /* Function to determine whether the debug point is hit. */
    bool IsDebugPointHit(DebugPointId *debugPointId);
    /* After resume, debug point becomes inactive. This activates it again. */
    void ResetDebugPoint(DebugPointId *debugPointId);
    /* Removes debug point completely. Slot is not reused (for now). */
    void RemoveDebugPoint(DebugPointId *debugPointId);
    /* Helper function to validate that debug point is set in control block. */
    bool IsDebugPointSet(DebugPointId *debugPointId);
    /* Debug function to print all debug points set in the control block. */
    void PrintDebugPoints();
    /* Debug function to use in TEST_F functions to dump extra diagnostics. */
    bool UseVerboseDiagnostics();

protected:
    DebugPointMgr() {}
    ~DebugPointMgr() {}

private:
    /* Private helper functions */
    int GetDebugPointIndex(DebugPointId *debugPointId);
    void SuspendDebugPoint(int dpIndex, bool isBarrier = false);
    void CleanDebugPoint(int index);
    void ResumeIfBarrierReached(int index);
    void CreateDebugPointCB();
    void DestroyDebugPointCB();

    /* Data used to make this class a singleton: should be static */
    static DebugPointMgr *m_instance;
    static std::mutex m_mutex;

    /* Parameters related to shared memory */
    pthread_rwlockattr_t m_rwLockAttribute;
    struct DebugPointControlBlock *m_debugPointCB;

    /* Used to control diagnostic (verbose if true) */
    bool m_useVerboseDiagnostics;
};

/* Use this in the UT functions to dump the extra diagnostics. */
inline bool DebugPointMgr::UseVerboseDiagnostics()
{
    return m_useVerboseDiagnostics;
}

}  /* namespace DSTORE */
#endif
