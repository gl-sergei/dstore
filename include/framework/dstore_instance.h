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

#ifndef DSTORE_INSTANCE_H
#define DSTORE_INSTANCE_H

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "securec.h"
#include "common/algorithm/dstore_hsearch.h"
#include "common/memory/dstore_mctx.h"
#include "common/instrument/dstore_stat.h"
#include "framework/dstore_instance_interface.h"
#include "common/dstore_datatype.h"
#include "framework/dstore_thread.h"
#include "framework/dstore_pdb.h"
#include "framework/dstore_thread_cpu_autobinder.h"
#include "page/dstore_page_struct.h"
#include "lock/dstore_lock_interface.h"
#include "diagnose/dstore_backup_restore_function.h"

#ifdef ENABLE_LCOV
extern "C" void __gcov_flush();
#endif

namespace DSTORE {
const int32 INVALID_CPU_ID = -1;
const int32 MAX_THREAD_NAME_LEN = 16;

struct PdbInfoCache {
    PdbInfo pdbInfoCache;
    bool cacheValide;
};

class AutoPdbCxtSwitch {
public:
    explicit AutoPdbCxtSwitch(PdbId oldId, PdbId newId) : oldPdbId(oldId), newPdbId(newId)
    {
        thrd->SetXactPdbId(newPdbId);
    }
    ~AutoPdbCxtSwitch()
    {
        thrd->SetXactPdbId(oldPdbId);
    }
    PdbId oldPdbId;
    PdbId newPdbId;
};

enum class InstanceState : uint8 {
    NOT_ACTIVE,
    ACTIVE,
};

class StorageInstance : public StorageInstanceInterface {
public:
    /* StorageInstanceInterface required interfaces */
    FUNCTION_DELETE_LATER void Initialize(StorageGUC *guc, bool bootStrap = false) override;
    FUNCTION_DELETE_LATER void Destroy() override;
    RetStatus Bootstrap(StorageGUC *guc) override;
    void BootstrapDestroy() override;
    void BootstrapResDestroy() override;
    RetStatus StartupInstance(StorageGUC *guc) override;
    void StopInstance() override;
    void ShutdownInstance() override;

    RetStatus OpenPDB(PdbInfo *pdbInfo, bool keepPrevPdbContext) override;
    RetStatus ClosePDB(const char *pdbName, bool immediate = false) override;

    virtual void StartBgThreads();
    virtual void StopBgThreads();
    bool IsStoppingBgThreads() const;

    RetStatus CreateThreadAndRegister(PdbId pdbId = INVALID_PDB_ID, bool bootstrap = false,
                                      const char *threadName = "worker", bool needCommBuffer = true,
                                      ThreadMemoryLevel level = ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY) override;
    void UnregisterThread(bool bootstrap = false) override;
    /* Add a thread to the instance so that changes on that thread are visible to other threads. */
    void AddVisibleThread(ThreadContextInterface *thread, const PdbId, const char *threadName = nullptr,
                          volatile uint32_t *InterruptHoldoffCount = nullptr) override;
    void RemoveVisibleThread(ThreadContextInterface *thread) override;
    RetStatus CreateMemMgr() override;
    RetStatus UpdateCpuRes(uint32_t minCpuCount, uint32_t maxCpuCount, uint32_t numaCount,
                      NumaCpuInfo *numaCpuInfos) override;
    void GetCurrentComputeNodes(NodeId **nodeIdArr, uint32_t &nodeCnt) override;
    bool IsInPdbInfocache(const char *pdbName, PdbId *pdbidx);
    RetStatus InvalidatePdbInfoCache(PdbId pdbId);
    RetStatus UpdatePdbRoleModeInPdbInfoCache(PdbId pdbId, PdbRoleMode roleMode);
    bool IsParallelBuildThread(pthread_t thread) override;

    StorageInstance();
    ~StorageInstance() override = default;
    DISALLOW_COPY_AND_MOVE(StorageInstance);

    virtual class BufMgrInterface     *GetBufferMgr();
    virtual class LockMgr             *GetLockMgr();
    virtual class TableLockMgr        *GetTableLockMgr();
    virtual class XactLockMgr         *GetXactLockMgr();

    virtual struct StorageGUC         *GetGuc();
    virtual class StorageMemoryMgr    *GetMemoryMgr();
    virtual class ThreadCoreMgr       *GetThreadCoreMgr();
    virtual class StoragePdb          *GetPdb(PdbId pdbId);
    virtual class CacheHashManager    *GetCacheHashMgr();
    virtual class ThreadCpuAutoBinder *GetThreadCpuAutoBinder();
    virtual class StorageStat         *GetStat();

    Oid GetDefaultTablespaceOid() override;

    RetStatus AllocPdb(PdbInfo *pdbInfo, const char* vfsName = nullptr) override;
    RetStatus CreatePDB(PdbInfo *pdbInfo) override;
    RetStatus DropPDB(const char* pdbName) override;
    void StopAllPdbBgThreads(PdbId pdbId) override;
    RetStatus StopAllPdbBgThreadsPhase1(PdbId pdbId) override;
    RetStatus StopAllPdbBgThreadsPhase2(PdbId pdbId) override;
    RetStatus SendPDBStopBgThreadsPhase1(PdbId pdbId) override;
    RetStatus SendPDBStopBgThreadsPhase2(PdbId pdbId) override;
    RetStatus SendPDBStopSqlThreads(PdbId pdbId, int8_t switchOn, bool immediate, bool force = false) override;
    RetStatus SendPDBReset(PdbId pdbId) override;
    RetStatus SendPDBInvalidateBuffer(PdbId pdbId) override;
    RetStatus SendPDBResetTime(PdbId pdbId) override;
    PdbId GetPdbIdByName(const char *pdbName) override;
    PdbId GetVisablePdbIdByName(const char *pdbName) override;
    PdbId GetPdbIdByUuid(const char *pdbUuid) override;
    RetStatus GetPdbSize(Oid pdbId, int64 &pdbSize) override;
    uint64_t GetPdbTerm(Oid pdbId) override;
    bool IsWalMgrInit(const PdbId &pdbId) override;
    RetStatus GetPdbInfoById(PdbId pdbId, PdbInfo *pdbInfo) override;
    void UpdatePdbInfoById(PdbId pdbId, PdbInfo *pdbInfo) override;
    RetStatus GetPdbBriefInfoByName(const char *pdbName, PdbBriefInfo *pdbBriefInfo) override;
    RetStatus GetVisablePdbBriefInfoByName(const char *pdbName, PdbBriefInfo *pdbBriefInfo) override;
    RetStatus GetPdbBriefInfoById(PdbId pdbId, PdbBriefInfo *pdbBriefInfo) override;
    RetStatus GetPdbBriefInfoAll(PdbBriefInfo *pdbIdInfos, uint32 *pdbCount) override;
    RetStatus GetPdbBriefInfoVisable(PdbBriefInfo *pdbIdInfos, uint32 *pdbCount) override;
    RetStatus UpdatePdbStatusById(PdbId pdbId, PdbStatus pdbStatus) override;
    RetStatus InitPdbByName(const char *pdbName, bool keepPrevPdbContext) override;
    void InitWorkingVersionNum(const uint32_t *workingGrandVersionNum) override;
    uint32_t GetWorkingVersionNum();
    void FreePdbObj(const PdbId pdbId);
    virtual RetStatus LockAndRefreshPdb(const PdbId pdbId, const LockMode mode = DSTORE_ACCESS_SHARE_LOCK,
                                        uint32_t *bitmap = nullptr);
    virtual void UnlockAfterRefreshPdb(const PdbId pdbId, const LockMode mode = DSTORE_ACCESS_SHARE_LOCK);

    template <LWLockMode mode>
    void LockPdbObj(const PdbId pdbId)
    {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("LockPdbObj %d.", pdbId));
        DstoreLWLockAcquire(&m_pdbLwLock[StoragePdb::GetPdbIndex(pdbId)], mode);
    }
    void UnLockPdbObj(const PdbId pdbId);
 
    uint64_t GlobalPdbTermAddFetchU64()
    {
        return GsAtomicAddFetchU64(&m_pdbGlobalTerm, 1);
    }
    
    template <LWLockMode mode>
    void LockPdbInit(const PdbId pdbId)
    {
        DstoreLWLockAcquire(&m_pdbInitLock[StoragePdb::GetPdbIndex(pdbId)], mode);
    }
    void UnLockPdbInit(const PdbId pdbId);

    template <LWLockMode mode>
    void LockPdbStatus(const PdbId pdbId)
    {
        DstoreLWLockAcquire(&m_pdbStatusLock[StoragePdb::GetPdbIndex(pdbId)], mode);
    }
    void UnLockPdbStatus(const PdbId pdbId);

    StoragePdb *GetPdbByName(const char *pdbName);
    void StoreMaxAppendPlsnOfPdbs(PdbId pdbId, PdbsPlsnRecords &plsnRecords);
    RetStatus CreateCheckpointForPdbs(const PdbsPlsnRecords &plsnRecords);

    virtual RetStatus CreatePdbReplica(StoragePdb *newPdb);

    RetStatus GetClusterRecoveryStatus(ClusterRecoveryStatus &status) override;

    bool IsInit() override
    {
        return static_cast<InstanceState>(m_instanceState.load(std::memory_order_acquire)) == InstanceState::ACTIVE;
    }

    bool IsBootstrapping() const
    {
        return m_bootstrapping;
    }

    bool IsStopBgThread()
    {
        return m_stopBgThread;
    }

    StorageInstanceType GetType() const override
    {
        return m_instanceType;
    }

    bool IsInstanceShutdown()
    {
        return m_isInstanceShutdown;
    }

    static char *GetPdbPath(const char *dataDir, PdbId pdbId);
    static char *GetPdbWalPath(const char *dataDir, PdbId pdbId);

    TablespaceMgr *GetTablespaceMgrByPdbId(PdbId pdbId);

    MemOutputParam *MemnodeRemoteCall(NodeId nodeId, MemInputParam *param) override;

    RetStatus UpdateAidClusterInfo(const AidClusterInfo *aidClusterInfo) override;
    void GetAidClusterInfo(AidClusterInfo *&aidClusterInfo) override;
    void RegisterCrmmRouteInfoCallback(CrmmRouteUpdateCallback callback) override;
    void RegisterCrossClusterCommCallback(CrossClusterCommCallback callback) override;
    void RegisterWaitForAllSessionDisconnectCallback(WaitForAllSessionDisconnectCallback callback) override;
    void RegisterWaitForAllSQLThreadStopCallback(WaitForAllSQLThreadStopCallback callback) override;

    void RegisterReportWaitEventCallback(ReportWaitEventCallback callback) override;
    void RegisterReportWaitEventFailedCallback(ReportWaitEventCallback callback) override;
    void RegisterReportWaitStatusCallback(ReportWaitStatusCallback callback) override;
    void RegisterReportXactTimestampCallback(ReportXactTimestampCallback callback) override;
    void RegisterReportCurrentTopXidCallback(ReportCurrentTopXidCallback callback) override;
    void RegisterReportCurrentXidCallback(ReportCurrentXidCallback callback) override;
    void RegisterReportXlogLenCallback(ReportXlogLenCallback callback) override;
    void RegisterReportXactInfoCallback(ReportXactInfoCallback callback) override;
    void RegisterReportCountBufferReadCallback(ReportCountBufferReadCallback callback) override;
    void RegisterReportCountBufferHitCallback(ReportCountBufferHitCallback callback) override;
    void RegisterReportCountBufferCallback(ReportCountBufferCallback callback) override;
    void RegisterReportLockStatCallback(ReportLockStatCallback callback) override;
    void RegisterReportLWLockStatCallback(ReportLWLockStatCallback callback) override;
    void RegisterReportSendBgwriterCallback(ReportSendBgwriterCallback callback) override;
    void RegisterReportBufferReadTimeCallback(ReportBufferReadTimeCallback callback) override;
    void RegisterReportBufferWriteTimeCallback(ReportBufferWriteTimeCallback callback) override;
    void RegisterReportDeadLockTagCallback(ReportDeadLockTagCallback callback) override;
    void RegisterReportSendTabstatCallback(ReportSendTabstatCallback callback) override;
    void RegisterReportDataIOTimeRecordCallback(ReportDataIOTimeRecordCallback callback) override;

    bool CanAcceptNewConnection() override
    {
        return static_cast<InstanceState>(m_instanceState.load(std::memory_order_acquire)) == InstanceState::ACTIVE;
    }

    void StopAcceptNewConnection() override
    {
        StorageReleasePanic(true, MODULE_FRAMEWORK, ErrMsg("Interface not supported."));
    }

    bool IsReadyToStopSQLThread() override
    {
        return true;
    }

    void Wakeup(ThreadId threadId) override;

    void DestroyPerfCounter();
    StorageMemoryMgr       *m_memoryMgr;
    ThreadCpuAutoBinder    *m_threadCpuAutoBinder;
    class BufMgrInterface  *m_bufMgr;
    class LockMgr          *m_lockMgr;
    class TableLockMgr     *m_tableLockMgr;
    class XactLockMgr      *m_xactLockMgr;
    struct StorageGUC      *m_guc;
    class CacheHashManager *m_cacheHashMgr;
    class ThreadCoreMgr     m_thrdCoreMgr;
    class CsnMgr           *m_csnMgr;
    class StorageStat       m_stat;
    /* Global storage instance version */
    volatile uint32_t *m_working_version_num;
    /* SQL callback to init and release SQL engine thread context */
    ThreadInitCallback      m_threadInitCallback = nullptr;
    ThreadReleaseCallback   m_threadReleaseCallback = nullptr;

    LockWaitTimeoutMSCallback m_lockWaitTimeoutCallBack = nullptr;
    IndexGetFuncCb m_indexCallback{nullptr};

    virtual RetStatus InitCsnMgr();
    virtual class CsnMgr *GetCsnMgr();
    void UpdateCsnMinThreadMain();
    void SetBackupCheckPoint(PdbId pdbId, BackupWalInfo *checkPoint)
    {
        StorageReleasePanic(!StoragePdb::IsValidPdbId(pdbId), MODULE_FRAMEWORK,
                            ErrMsg("pdbId is invalid."));
        m_checkPoint[pdbId - PDB_START_ID] = checkPoint;
        GS_MEMORY_BARRIER();
    }
    BackupWalInfo *GetBackupCheckPoint(PdbId pdbId)
    {
        if (!StoragePdb::IsValidPdbId(pdbId) || m_checkPoint[pdbId - PDB_START_ID] == nullptr) {
            StorageReleasePanic(true, MODULE_FRAMEWORK,
                            ErrMsg("pdbId is invalid or m_checkPoint is nullptr."));
        }
        return m_checkPoint[pdbId - PDB_START_ID];
    }
    void ClearBackupRestoreCheckPoint(PdbId pdbId, bool needFree = false)
    {
        StorageReleasePanic(!StoragePdb::IsValidPdbId(pdbId), MODULE_FRAMEWORK,
                            ErrMsg("pdbId is invalid."));
        if (needFree) {
            DstorePfreeExt(m_checkPoint[pdbId - PDB_START_ID]);
        } else {
            m_checkPoint[pdbId - PDB_START_ID] = nullptr;
        }
        GS_MEMORY_BARRIER();
    }
    bool IsInBackupRestore(PdbId pdbId)
    {
        StorageReleasePanic(!StoragePdb::IsValidPdbId(pdbId), MODULE_FRAMEWORK,
                            ErrMsg("pdbId is invalid."));
        if (m_consistencyPoint[pdbId - PDB_START_ID] != nullptr) {
            return true;
        }
        return false;
    }

    void SetBackupRestoreConsistencyPoint(PdbId pdbId, BackupConsistencyPoint *consistencyPoint)
    {
        StorageReleasePanic(!StoragePdb::IsValidPdbId(pdbId), MODULE_FRAMEWORK,
                            ErrMsg("pdbId is invalid."));
        m_consistencyPoint[pdbId - PDB_START_ID] = consistencyPoint;
        GS_MEMORY_BARRIER();
    }
    BackupConsistencyPoint *GetBackupConsistencyPoint(PdbId pdbId)
    {
        StorageReleasePanic(!StoragePdb::IsValidPdbId(pdbId), MODULE_FRAMEWORK,
                            ErrMsg("pdbId is invalid."));
        return m_consistencyPoint[pdbId - PDB_START_ID];
    }
    void ClearBackupRestoreConsistencyPoint(PdbId pdbId, bool needFree = false)
    {
        StorageReleasePanic(!StoragePdb::IsValidPdbId(pdbId), MODULE_FRAMEWORK,
                            ErrMsg("pdbId is invalid."));
        if (needFree) {
            DstorePfreeExt(m_consistencyPoint[pdbId - PDB_START_ID]);
        } else {
            m_consistencyPoint[pdbId - PDB_START_ID] = nullptr;
        }
        GS_MEMORY_BARRIER();
    }

    bool IsPdbRollbackEnd(PdbId pdbId);
    bool IsBackupRestoreRollbackToConsistencyPointEnd(PdbId pdbId = INVALID_PDB_ID);
    void RegisterDstoreInternalThreadCallback(ThreadInitCallback initCallback,
                                              ThreadReleaseCallback releaseCallback) override;
    /* InitSQLThreadContext will start a sql&dstore thread ctx and start new transaction. */
    bool InitSQLThreadContext(SQLThrdInitCtx *context);
    void ReleaseSQLThreadContext(SQLThrdInitCtx *context);
    void RegisterIndexCallbacks(IndexGetFuncCb *indexCb) override;
    void RegisterGetLockWaitTimeoutCallback(LockWaitTimeoutMSCallback callback) override;

    void BindAllThreads() override;

#ifdef UT
    inline RetStatus SetPdbStatusOpened(PdbId pdbId)
    {
        StoragePdb *pdb = GetPdb(g_defaultPdbId);
        if (LockInterface::LockPdb(pdbId, DSTORE_ACCESS_EXCLUSIVE_LOCK) != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
        ControlFile *controlFile = pdb->GetControlFile();
        controlFile->SetOpenedFlag(pdbId);
        LockInterface::UnlockPdb(pdbId, DSTORE_ACCESS_EXCLUSIVE_LOCK);
        return DSTORE_SUCC;
    }
    inline bool CheckPdbInfoCacheValid(PdbId pdbId) { return m_pdbInfoCache[pdbId].cacheValide; };
#endif

protected:
    RetStatus ThreadCoreMgrInit();

    virtual void DestroyCsnMgr();
    virtual RetStatus GucInit(StorageGUC *guc);
    virtual void GucDestroy();

    RetStatus InitAllLockMgrs();
    void InitPdbLwLocks();
    RetStatus InitPdbSlots();
    void InitPdbInfoCache();

    virtual class StoragePdb *CreatePdbObj(PdbId pdbId);
    /* If the pdb has already exists in the control file, we need to
     *  enter the uuid in the control file as the parameter.
     */
    virtual class StoragePdb *CreatePdbObj(PdbId pdbId, const char* pdbUuid);
    FUNCTION_DELETE_LATER virtual void CreateTemplatePdb();
    FUNCTION_DELETE_LATER virtual void InitAllPdb();
    FUNCTION_DELETE_LATER virtual void DestroyAllPdb();

    virtual void CreateTemplatePDB();
    virtual void CloseTemplatePDB();

    virtual void OpenOnePdb(PdbId pdbId, bool withSingleMode);
    virtual void CloseAllPdb(bool createCheckpoint);

    virtual RetStatus BufMgrInit();
    virtual void BufMgrDestroy();

    virtual RetStatus InitLockMgr();
    virtual void DestroyLockMgr();

    virtual RetStatus InitTableLockMgr();
    virtual void DestroyTableLockMgr();

    virtual RetStatus InitXactLockMgr();
    virtual void DestroyXactLockMgr();

    RetStatus InitCpuRes(NumaInfo *numaInfos);
    void DestroyCpuRes();

    void ResourcesCleanUp();

    virtual RetStatus TypecacheMgrInit(IndexGetFuncCb *indexCb = nullptr);
    virtual void TypecacheMgrDestroy();

    class StoragePdb       *m_pdb[MAX_PDB_COUNT];
    BackupConsistencyPoint *m_consistencyPoint[MAX_PDB_COUNT];
    BackupWalInfo          *m_checkPoint[MAX_PDB_COUNT];
    std::atomic<uint8>      m_instanceState;
    bool                    m_bootstrapping;
    StorageInstanceType     m_instanceType;
    std::atomic<bool>       m_stopBgThread;
    class std::thread      *m_updateCsnMinThread;
    LWLock m_pdbLwLock[MAX_PDB_COUNT];
    /* ONLY for locking the process of initializing pdb */
    LWLock m_pdbInitLock[MAX_PDB_COUNT];
    LWLock m_pdbStatusLock[MAX_PDB_COUNT];
    pthread_rwlock_t m_pdbInfoCacheRwlock = PTHREAD_RWLOCK_INITIALIZER;
    PdbInfoCache m_pdbInfoCache[PDB_MAX_ID + 1];
    bool                    m_isInstanceShutdown;
    gs_atomic_uint64 m_pdbGlobalTerm;

    virtual RetStatus SetPdbToSlot(StoragePdb *newPdb);
    RetStatus SwitchContextToTargetPdb(PdbId pdbId, bool destroyWalContext,
                                       AtomicWalWriterContext *walWriterContext = nullptr);
    void SwitchWalWriterContextToTargetPdb(PdbId pdbId, bool destroyWalContext,
                                           AtomicWalWriterContext *walWriterContext = nullptr);
    virtual void UpdateCsnMinThreadLoop();
    RetStatus CopyVfsAndFiles(StoragePdb *newPdb, PdbInfo *pdbInfo, ControlPdbInfoPageItemData *pdbInfoPageItemData,
                          const char *existVfsName = nullptr);
    RetStatus DoInnerlyOpenPdb(StoragePdb *newPdb, ControlPdbInfoPageItemData *pdbInfoPageItemData);
    RetStatus UpdateSysTable(PdbId oldPdbId, PdbId pdbId, PdbInfo *pdbInfo);
    RetStatus DoDropPdbForce(StoragePdb *pdb, const char* vfsName);

    RetStatus InitPerfCounter();
    void FetchBriefInfoFromCtrlItem(PdbBriefInfo *pdbBriefInfo, ControlPdbInfoPageItemData *pdbItem);
    void FetchPdbInfoFromCtrlItem(PdbInfo *pdbInfo, ControlPdbInfoPageItemData *pdbItem);
    RetStatus RefreshPdbStatus(PdbId);
} ALIGNED(DSTORE_CACHELINE_SIZE);

extern StorageInstance *g_storageInstance;
extern bool g_enableAsyncIoFlush;

#define USE_PAGE_STORE (g_storageInstance->GetGuc()->tenantConfig->storageConfig.type == \
    StorageType::PAGESTORE)
/*
 * default open aio at pagestore, now local disk aio flush page too slow so that
 * g_enableAsyncIoFlush default false
 */
#define USE_VFS_AIO (g_enableAsyncIoFlush || USE_PAGE_STORE)
#define USE_VFS_LOCAL_AIO (g_enableAsyncIoFlush && !USE_PAGE_STORE)
#define USE_VFS_PAGESTORE_AIO USE_PAGE_STORE
} /* namespace DSTORE */
#endif
