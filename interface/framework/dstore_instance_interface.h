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
 * dstore_instance_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/framework/dstore_instance_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_INSTANCE_INTERFACE_H
#define DSTORE_INSTANCE_INTERFACE_H

#include "common/dstore_common_utils.h"
#include "catalog/dstore_catalog_struct.h"
#include "diagnose/dstore_memnode_diagnose.h"
#include "systable/systable_callback_param.h"
#include "systable/sys_database.h"
#include "framework/dstore_stat_interface.h"
#include "framework/dstore_thread_interface.h"
#include "index/dstore_index_struct.h"
#include "pdb/dstore_pdb_interface.h"

namespace DSTORE {

#pragma GCC visibility push(default)

constexpr PdbId PDB_DDL_LOCK_ID = 0xFFFFFFFE;
constexpr PdbId PDB_MAX_ID = 32;    /* supported max pdb id */
constexpr PdbId FIRST_USER_PDB_ID = 17;
constexpr PdbId PDB_TEMPLATE1_ID = 1;
constexpr PdbId PDB_TEMPLATE0_ID = 2;
constexpr PdbId PDB_ROOT_ID = 3;
constexpr PdbId PDB_TEMPLATEA_ID = 4;
constexpr PdbId PDB_TEMPLATEM_ID = 5;  /* not use, reserved */
constexpr PdbId PDB_START_ID = PDB_TEMPLATE1_ID;
constexpr uint8_t FORMATTED_UUID_VALID_LEN = 36; /* 32byte uuid + 4byte '-' */
constexpr uint8_t FORMATTED_UUID_ARR_LEN = 64; /* 32byte uuid + 4byte '-' + 1byte '\0' + 27byte reserve */
constexpr char PDB_DEFAULT_UUID[FORMATTED_UUID_ARR_LEN] = {0};
constexpr PdbId MAX_PDB_COUNT = (PDB_MAX_ID - PDB_START_ID) + 1;
constexpr PdbId TENANT_PDB_COUNT = PDB_MAX_ID - FIRST_USER_PDB_ID + 1;
constexpr uint32_t MAX_WAL_STREAM_SEMAS_NUM = 6;
const NodeId INVALID_NODE_ID = UINT32_MAX;

inline bool IsTemplate(PdbId pdbId)
{
    if (pdbId == PDB_TEMPLATE1_ID || pdbId == PDB_TEMPLATE0_ID || pdbId == PDB_TEMPLATEA_ID) {
        return true;
    }
    return false;
}

constexpr uint8_t BASE_DIR_LEN = 10;
constexpr char BASE_DIR[BASE_DIR_LEN] = "gs_pdb";
constexpr uint16_t MAX_CPU_NUM_PER_NUMA = 256;
constexpr uint16_t MAX_DSTORE_CPU_LIST_LEN = 256;
constexpr uint16_t DSTORE_MAX_TENANT_CLUSTER_COUNT = 5;

constexpr unsigned int MAXSPACECOUNT = 10;
constexpr unsigned int MAXTYPELEN = 16;
constexpr unsigned int MAXLIBPATH = 1024;
constexpr unsigned int MAXNAMELEN = 128;
constexpr unsigned int MAXIPLEN = 1024;
class ThreadContextInterface;

constexpr uint64_t BUFFER_TRACE_SWITCH                  = (1LL << 1);
constexpr uint64_t WAL_TRACE_SWITCH                     = (1LL << 2);
constexpr uint64_t DEADLOCK_TRACE_SWITCH                = (1LL << 3);
constexpr uint64_t TABLELOCK_STATS_TRACE_SWITCH         = (1LL << 4);
constexpr uint64_t BTREE_STATISTIC_INFO_MIN_TRACE_LEVEL = (1LL << 5);

constexpr int DSTORE_VFS_NAME_MAX_LEN = 256;
constexpr int DSTORE_FILE_PATH_MAX_LEN = 1024;
constexpr int DSTORE_VFS_LIB_ATTR_LEN = 4096;
constexpr int DSTORE_CLUSTER_NAME_MAX_LEN = 256;
constexpr int DSTORE_TENANT_NAME_MAX_LEN = 256;
constexpr int DSTORE_LOG_PATH_MAX_LEN = 1024;
constexpr int DSTORE_MAX_DB_VERSION_LEN = 128;
constexpr int DSTORE_MAX_TLS_NAME_LEN = 1024;
constexpr int MAX_GUC_INT_LEN = 64; /* GUC config integer max len */
constexpr uint32_t DEFAULT_TIME_LINE_ID = 100;

enum class CsnMode {
    DEFAULT = 0,
    BROADCAST,
    GLOBAL_CLOCK
};

enum class PerfLevel { PERF_CLOSE = 0, PERF_DEBUG, RELEASE, OFF = 0xFF };

struct NumaCpuInfo {
    uint32_t numaId;    /* numa node id which cpu reference to */
    char cpuList[MAX_DSTORE_CPU_LIST_LEN];
};

struct NumaInfo {
    uint32_t numaCount;
    NumaCpuInfo *numaCpuInfos;
};

struct AidClusterInfo {
    uint32_t clusterCount;
    uint32_t clusterIds[DSTORE_MAX_TENANT_CLUSTER_COUNT];
};

struct StoreSpaceConfig {
    char storeSpaceName[DSTORE_VFS_NAME_MAX_LEN];
    int maxSpaceSize;
    char type[MAXTYPELEN];
};

enum class StorageType {
    INVALID_TYPE,
    TENANT_ISOLATION,
    PAGESTORE,
    LOCAL,
};

struct StorageConfig {
    StorageType type;
    char clientLibPath[DSTORE_FILE_PATH_MAX_LEN];
    char serverAddresses[DSTORE_VFS_LIB_ATTR_LEN];
    char serverProtocolType[MAXTYPELEN];
    char rootpdbVfsName[DSTORE_VFS_NAME_MAX_LEN];
    char template0VfsName[DSTORE_VFS_NAME_MAX_LEN];
    char template1VfsName[DSTORE_VFS_NAME_MAX_LEN];
    char votingVfsName[DSTORE_VFS_NAME_MAX_LEN];
    char runlogVfsName[DSTORE_VFS_NAME_MAX_LEN];
};
struct LocalConfig {
    char localIp[DSTORE_VFS_LIB_ATTR_LEN];
    int localPort;
};
struct CommunicationConfig {
    int clusterId;
    LocalConfig localConfig;
    char commConfigStr[DSTORE_VFS_LIB_ATTR_LEN];
    int dstoreCommThreadMin;
    int dstoreCommThreadMax;
    uint32_t authType;
};

struct VotingConfig {
    char votingFilePath[DSTORE_FILE_PATH_MAX_LEN];
};

struct LogConfig {
    char logPath[DSTORE_LOG_PATH_MAX_LEN];
    int logLevel;
};

struct SslConfig {
    char caFile[DSTORE_MAX_TLS_NAME_LEN];
    char keyFile[DSTORE_MAX_TLS_NAME_LEN];
    char crlFile[DSTORE_MAX_TLS_NAME_LEN];
    char certFile[DSTORE_MAX_TLS_NAME_LEN];
    char cipher[DSTORE_MAX_TLS_NAME_LEN];
    uint32_t certNotifyTime;
};

struct SecurityConfig {
    char securityRootKeyPath[DSTORE_LOG_PATH_MAX_LEN];
    char cipherKey[MAXNAMELEN];
    char commSharedKey[MAXNAMELEN];
    SslConfig connectSsl; /* security authentication for connecting to external components of GaussDB */
    SslConfig rpcSsl; /* security authentication for connecting to components inner GaussDB */
};

struct TenantConfig {
    char clusterName[DSTORE_CLUSTER_NAME_MAX_LEN];
    char tenantName[DSTORE_TENANT_NAME_MAX_LEN];
    int tenantId;
    uint32_t nodeId;
    StorageConfig storageConfig;
    CommunicationConfig communicationConfig;
    int storeSpaceCnt;
    StoreSpaceConfig storeSpaces[MAXSPACECOUNT];
    VotingConfig votingConfig;
    LogConfig logConfig;
    SecurityConfig securityConfig;
    bool isEmbeddedShareMode = false;
};

struct StorageGUC {
    NodeId selfNodeId;       /* TODO: we may directly get selfNodeId from cluster manager, or change type to NodeId */
    int buffer;              /* buffer size */
    int bufferLruPartition;  /* the number of buffer lru parition */
    Timestamp checkpointTimeout; /* seconds [1, 3600], default is 60s */
    int defaultIsolationLevel; /* default isolation level, see enum TrxIsolationType */
    int maintenanceWorkMem;      /* maintenance work memory, used for ddl(just for build index now), kb */
    char *dataDir;              /* PG_DATA directory */
    uint32_t ncores;                  /* max_thread_num */
    int logMinMessages;        /* log min messages */
    int foldPeriod;            /* seconds */
    int foldThreshold;         /* the log exceeded threshold will be fold in one period */
    int foldLevel;             /* less than or equal to foldLevel will fold */
    CommitSeqNo csnAssignmentIncrement;
    char *moduleLoggingConfigure;
    uint32_t lockHashTableSize;                   /* size of lock hash table */
    uint32_t lockTablePartitionNum;               /* number of partitioned lock used by lock hash table */
    bool enableLazyLock;                          /* enable transaction level lazy lock to speed up weak lock */
    char *vfsTenantIsolationConfigPath;                          /* VFS config path */
    int updateCsnMinInterval;                  /* global minimal csn updating interval */
    uint16_t numObjSpaceMgrWorkers;               /* The number of background threads that performs extension */
    int minFreePagePercentageThreshold1;          /* The first threshold percentage of free pages for extension */
    int minFreePagePercentageThreshold2;          /* The second threshold percentage of free pages for extension */
    int probOfExtensionThreshold;                 /* The probability of extension threshold */
    int walLevel;                                 /* wal level decide the detail of wal */
    uint32_t recoveryWorkerNum;
    bool synchronousCommit;                      /* dstore wal added */
    int walStreamCount;                        /* dstore wal stream count */
    int walFileNumber;                         /* dstore wal added */
    int64_t walFileSize;                       /* dstore wal added */
    int walBuffers;                             /* dstore wal added */
    int walFlushTimeout;                         /* dstore wal added */
    int64_t walReadBufferSize;                  /* dstore wal added */
    int64_t walRedoBufferSize;                  /* dstore wal added */
    int walwriterCpuBind;                      /* dstore wal added */
    int walKeepSegments;                        /* dstore wal added */
    char* redoBindCpuAttr;                       /* dstore wal added */
    uint64_t bgWalWriterMinBytes;                /* Minimum bytes of WAL to be flushed by each WAL writer thread. */
    uint64_t walEachWriteLenghthLimit;                   /* The limit of communication wal size. */
    uint8_t numaNodeNum;
    bool disableBtreePageRecycle;                 /* Disable btree page unlink. */
    int deadlockTimeInterval;                     /* The time to wait on a lock before checking for deadlock. */
    int recycleFsmTimeInterval;            /* The time in minutes to recycle a FSM from the last time it is accessed */
    int probOfUpdateFsmTimestamp;          /* The probability of a thread updating the access timestamp of given FSM */
    int probOfRecycleFsm;                  /* The probability of check potential recyclable FSMs */
    int probOfRecycleBtree;                 /* The probability of recycling vacant pages in Btree */
    bool enableOutOfLineLob;                      /* Enable creation of the LOB extent for dstore */
    NumaInfo *numaInfo;                  /* numa resources for setting CPU affinity */
    int tacGracePeriod;                  /* max time in seconds the server will retain snapshot data
                                            while waiting for TAC client to reconnect */
    int bgDiskWriterSlaveNum;                     /* Background disk page writer slave thread number. */
    int bgPageWriterSleepMilliSecond;             /* Background writer sleep time between rounds. */
    uint32_t walThrottlingSize;                   /* Threshold of the WAL that trigger flow control */
    int maxIoCapacityKb;                          /* The I/O upper limit of batch flush dirty page every second. */

    int bulkReadRingSize;                       /* Size of bulk read buffer ring. */
    int bulkWriteRingSize;                      /* Size of bulk write buffer ring. */

    uint32_t distLockNumBuckets;  /* Size of bucket array for consistency hash table. */
    uint32_t distLockMaxRingSize; /* An upper bound of the size of the node ring. */
    CsnMode csnMode;              /* 0: DEFAULT, 1: CENTRALIZED, 2: BROADCAST */
    PerfLevel perfCounterLevel;   /* 0: CLOSE, 1: DEBUG, 2: RELEASE, 255: OFF */
    uint8_t perfCounterInterval;
    uint16_t ctrlPlanePort;       /* RPC communicator control plane port. */
    uint8_t rdmaGidIndex;         /* RPC communicator rdma gid index. */
    uint8_t rdmaIbPort;           /* RPC communicator rdma ib port. */
    int pdReadAuthResetPeriod;    /* the maximum period in seconds of the page pattern checks in PageDirectory */
    int csnThreadBindCpu;         /* NIC affinitive CPUs, usually belong to a particular NUMA */
    char *commConfigStr;          /* The configuration string for the communication module. */
    char *memberView;             /* Initial cluster member info, for stand-alone mode, it shall always be null. */
    char *commProtocolTypeStr;    /* The protocol type for the communication module. */
    int  commProtocolType;
    int globalClockAdjustWaitTimeUs;
    int globalClockSyncIntervalMs;
    bool gclockOverlapWaitTimeOptimization;
    uint32_t defaultHeartbeatTimeoutInterval;   /* Default timeout for DR MAX_AVAILABILITY sync mode */
    uint32_t defaultWalSizeThreshold;           /* Default threshold for DR MAX_AVAILABILITY sync mode */
    int commThreadMin;
    int commThreadMax;
    int clusterId;                /* cluster id defined by cluster manager. */
    bool enableQuickStartUp;
    TenantConfig* tenantConfig;
    int bgMemWriterSlaveNum;      /* Background memory page writer slave thread number. */
    int rnSleepTimeoutSeconds;    /* RN timeout seconds when it is asynchronously waiting for PD entry latch. */
    bool enableRemoteCrConstruction; /* Enable remote cr page construction, default to be false. */
    bool enableTrackActivities;
    uint32_t walKeepTimeAfterRecovery;
    int pageVersionCheckMode;       /* AntiCache : check pageVersion mode: off memory persistence */
    bool enablePageMissDirtyCheck;  /* AntiCache : whether check missing dirtry : off on */
    int maxPageVersionNum;          /* AntiCache : the max number of antiEntry */
    int pageVersionPartitionNum;    /* AntiCache : the partition number of anticache hashtable */
    bool enableStmtTrack;
    bool enableTrackIOTiming;
    double candidateSafePercent;
    uint32_t lruScanDepth;
};

enum class StorageInstanceType : uint8_t {
    SINGLE,
    DISTRIBUTE_COMPUTE,
    DISTRIBUTE_MEMORY
};

/* Identifies the pdb role mode */
enum class PdbRoleMode : uint8_t {
    /* Pdb role mode is being modified, so it is invalid temporarily, should wait util it is valid */
    PDB_INVALID = 0,
    /* Primary pdb can be read and wrote */
    PDB_PRIMARY,
    /* Standby pdb can be read only (not supported currently) */
    PDB_STANDBY,
    /* Online pdb can be read only, it is used for manually see pdb replica data in special scenarios */
    PDB_ONLINE,
    /* Offline pdb is not working and no wal sync with primary pdb */
    PDB_OFFLINE
};

/* String representations for PdbRoleMode enum values */
constexpr const char* g_pdbRoleModeStr[] = {
    "PDB_INVALID",    /* [0] PDB is temporarily invalid (in modification) */
    "PDB_PRIMARY",    /* [1] Primary PDB (read/write) */
    "PDB_STANDBY",    /* [2] Standby PDB (read-only, currently unsupported) */
    "PDB_ONLINE",     /* [3] Online PDB (read-only, for replica viewing) */
    "PDB_OFFLINE"     /* [4] Offline PDB (inactive, no WAL sync) */
};

/**
 * pdb switch status in switchover.
 * NORMAL means not in any switchover/failover/alterPdbStatus process.
 */
enum class PdbSwitchStatus : uint8_t {
    NORMAL = 0,          /* primary, standby */
    WAIT_PRIMARY_DEMOTE, /* standby */
    BEGIN_DEMOTE,        /* primary */
    BEGIN_PROMOTE,       /* standby */
    DEMOTE_FAIL,         /* primary */
    PROMOTE_FAIL,        /* standby */
    WAL_FORKING          /* standby */
};

/* String representations for PdbRoleMode enum values */
constexpr const char* g_pdbSwtichStatusStr[] = {
    "NORMAL",
    "WAIT_PRIMARY_DEMOTE",
    "BEGIN_DEMOTE",
    "BEGIN_PROMOTE",
    "DEMOTE_FAIL",
    "PROMOTE_FAIL",
    "WAL_FORKING"
};

/* Identifies the wal sync mode between standby pdb and primary pdb */
enum class PdbSyncMode : uint8_t {
    INVALID_SYNC_MODE = 0,
    /* Sync wal in asynchronous mode */
    MAX_PERFORMANCE_MODE,
    /* Sync wal in synchronous mode */
    MAX_RELIABILITY_MODE,
    /*
     * If the primary-standby connection is normal, sync wal in synchronous mode.
     * Otherwise, switch to asynchnorouns mode until the connection is back to normal
     * and the primary-standby log difference is small
     */
    MAX_AVAILABILITY_MODE
};

enum class PdbStatus : uint8_t {
    PDB_STATUS_UNCREATED,        /* pdb id is not allocated, but pdb is still being created */
    PDB_STATUS_CREATING,         /* pdb is being creatted After the creation is complete, the state automatically
                                    changes to OPEN.If the creation fails, the status is residual and needs to be
                                    cleared. */
    PDB_STATUS_OPENED_READ_WRITE, /* pdb is open, Read and write supported. Resources may not be loaded. */
    PDB_STATUS_CLOSING,          /* pdb is closing, Read and write are not supported.
                                    The PDB must be closed successfully. */
    PDB_STATUS_CLOSED,           /* pdb id is closed, No resource is loaded. Read and write are not supported. */
    PDB_STATUS_DROPPING,         /* pdb is being deleted. If an exception occurs, residual data can be deleted. */
    PDB_STATUS_NEED_RELOAD,      /* Indicates that the PDB information needs to be reloaded from the control file. */
};

/* String representations for PdbStatus enum values */
constexpr const char* g_pdbStatusStr[] = {
    "PDB_STATUS_UNCREATED",
    "PDB_STATUS_CREATING",
    "PDB_STATUS_OPENED_READ_WRITE",
    "PDB_STATUS_CLOSING",
    "PDB_STATUS_CLOSED",
    "PDB_STATUS_DROPPING",
    "PDB_STATUS_NEED_RELOAD",
};

struct PdbBriefInfo {
    PdbId pdbId;
    char pdbName[DB_NAME_LEN];
    char pdbUuid[FORMATTED_UUID_ARR_LEN];
    PdbStatus pdbStatus;
    Oid grpRespoolOid;                /* group respool oid of pdb */
    PdbRoleMode pdbRoleMode;
    Oid dbaId;       /* pdb dba user oid */
    PdbSwitchStatus pdbSwitchStatus;
};

struct PdbInfo {
    union {
        PdbBriefInfo briefInfo;
        struct {
            PdbId pdbId;
            char pdbName[DB_NAME_LEN];
            char pdbUuid[FORMATTED_UUID_ARR_LEN];
            PdbStatus pdbStatus;
            Oid grpRespoolOid;                /* group respool oid of pdb */
            PdbRoleMode pdbRoleMode;
            Oid dbaId;       /* pdb dba user oid */
            PdbSwitchStatus pdbSwitchStatus;
        };
    };
    PdbId templateId;
    HeapTuple *sysDatabaseTuple;
};

static_assert(offsetof(PdbInfo, briefInfo) == 0, "PdbBriefInfo must be the first var.");

struct SharedInvalTableSpaceCacheMsg {
    int8_t id;
    DSTORE::PdbId pdbId;
    uint16_t tablespaceId;
    uint16_t fileId;
};

enum class MetaDataMsgType : uint8_t {
    METADATAINVALIDATOR_RPC_INVALIDATION_REQUEST = 0,
    METADATAINVALIDATOR_RPC_CHECK_INVALIDATION,
    METADATAINVALIDATOR_RPC_TABLESPACE_CACHE
};

/**
 * Indicate if there are some nodes in the cluster is recovering
 */
enum class ClusterRecoveryStatus : uint8_t {
    CRS_INVALID = 0,    /* Invalid status, collect recovery status fail, can not determine the specific stauts. */
    CRS_RECOVERING = 1, /* Some nodes in the cluster is recovering */
    CRS_RUNNING = 2     /* None of the nodes is recovering */
};

struct AdbInfo {
    char adbName[DB_NAME_LEN];
};

struct AidClusterGsscmsCommInfo {
    char gsscmsAddr[DSTORE_VFS_LIB_ATTR_LEN];
    char clusterName[DSTORE_CLUSTER_NAME_MAX_LEN];
    uint32_t protocolType;
    uint32_t authMethod;
};

using CrmmRouteUpdateCallback = ErrorCode(*)(const struct RouteInfoUpdateMsg *);
using CrossClusterCommCallback = ErrorCode(*)(const struct PdbCrossClusterCommMsg *);
using WaitForAllSessionDisconnectCallback = bool(*)(void);
using WaitForAllSQLThreadStopCallback = bool(*)(void);
using LockWaitTimeoutMSCallback = int(*)(void);

class StorageInstanceInterface {
public:
    virtual ~StorageInstanceInterface() = default;
    virtual void Initialize(StorageGUC *guc, bool bootStrap = false) = 0;
    virtual void Destroy() = 0;
    virtual bool IsInit() = 0;
    virtual RetStatus Bootstrap(StorageGUC *guc) = 0;
    virtual void BootstrapDestroy() = 0;
    virtual void BootstrapResDestroy() = 0;
    virtual RetStatus AllocPdb(PdbInfo *pdbInfo, const char* vfsName = nullptr) = 0;
    virtual RetStatus CreatePDB(PdbInfo *pdbInfo) = 0;
    virtual RetStatus OpenPDB(PdbInfo *pdbInfo, bool keepPrevPdbContext) = 0;
    virtual RetStatus ClosePDB(const char* pdbName, bool immediate = false) = 0;
    virtual RetStatus DropPDB(const char* pdbName) = 0;
    virtual void StopAllPdbBgThreads(PdbId pdbId) = 0;
    virtual RetStatus StopAllPdbBgThreadsPhase1(PdbId pdbId) = 0;
    virtual RetStatus StopAllPdbBgThreadsPhase2(PdbId pdbId) = 0;
    virtual RetStatus SendPDBStopSqlThreads(PdbId pdbId, int8_t switchOn, bool immediate, bool force = false) = 0;
    virtual RetStatus SendPDBStopBgThreadsPhase1(PdbId pdbId) = 0;
    virtual RetStatus SendPDBStopBgThreadsPhase2(PdbId pdbId) = 0;
    virtual RetStatus SendPDBReset(PdbId pdbId) = 0;
    virtual RetStatus SendPDBInvalidateBuffer(PdbId pdbId) = 0;
    virtual RetStatus SendPDBResetTime(PdbId pdbId) = 0;
    virtual PdbId GetPdbIdByName(const char *pdbName) = 0;
    virtual PdbId GetVisablePdbIdByName(const char *pdbName) = 0;
    virtual PdbId GetPdbIdByUuid(const char *pdbUuid) = 0;
    virtual RetStatus GetPdbSize(Oid pdbId, int64_t &pdbSize) = 0;
    virtual uint64_t GetPdbTerm(Oid pdbId) = 0;
    virtual bool IsWalMgrInit(const PdbId &pdbId) = 0;
    virtual RetStatus GetPdbInfoById(PdbId pdbId, PdbInfo *pdbInfo) = 0;
    virtual void UpdatePdbInfoById(PdbId pdbId, PdbInfo *pdbInfo) = 0;
    virtual RetStatus GetPdbBriefInfoByName(const char *pdbName, PdbBriefInfo *pdbBriefInfo) = 0;
    virtual RetStatus GetVisablePdbBriefInfoByName(const char *pdbName, PdbBriefInfo *pdbBriefInfo) = 0;
    virtual RetStatus GetPdbBriefInfoById(PdbId pdbId, PdbBriefInfo *pdbBriefInfo) = 0;
    virtual RetStatus GetPdbBriefInfoAll(PdbBriefInfo *pdbIdInfos, uint32_t *pdbCount) = 0;
    virtual RetStatus GetPdbBriefInfoVisable(PdbBriefInfo *pdbIdInfos, uint32_t *pdbCount) = 0;
    virtual RetStatus UpdatePdbStatusById(PdbId pdbId, PdbStatus pdbStatus) = 0;
    virtual RetStatus InitPdbByName(const char *pdbName, bool keepPrevPdbContext) = 0;
    virtual Oid GetDefaultTablespaceOid() = 0;
    virtual RetStatus StartupInstance(StorageGUC *guc) = 0;
    virtual void StopInstance() = 0;
    virtual void ShutdownInstance() = 0;
    virtual RetStatus CreateThreadAndRegister(PdbId pdbId = INVALID_PDB_ID, bool bootstrap = false,
                                              const char *threadName = "worker", bool needCommBuffer = true,
                                              ThreadMemoryLevel level = ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY) = 0;
    virtual void UnregisterThread(bool bootstrap = false) = 0;
    /* Add a thread to the instance so that changes on that thread are visible to other threads. */
    virtual void AddVisibleThread(ThreadContextInterface *thrd, const PdbId, const char *threadName = nullptr,
                                  volatile uint32_t *InterruptHoldoffCount = nullptr) = 0;
    virtual void RemoveVisibleThread(ThreadContextInterface *thrd) = 0;
    virtual RetStatus CreateMemMgr() = 0;
    virtual bool IsParallelBuildThread(pthread_t thread) = 0;
    virtual MemOutputParam *MemnodeRemoteCall(NodeId nodeId, MemInputParam *param) = 0;
    virtual bool CanAcceptNewConnection() = 0;
    virtual void StopAcceptNewConnection() = 0;
    virtual bool IsReadyToStopSQLThread() = 0;
    virtual void Wakeup(ThreadId threadId) = 0;
    virtual StorageInstanceType GetType() const = 0;
    virtual RetStatus UpdateCpuRes(uint32_t minCpuCount, uint32_t maxCpuCount, uint32_t numaCount,
                                   NumaCpuInfo *numaCpuInfos) = 0;
    virtual RetStatus UpdateAidClusterInfo(const AidClusterInfo *aidClusterInfo) = 0;
    virtual void GetAidClusterInfo(AidClusterInfo *&aidClusterInfo) = 0;
    
    virtual void RegisterCrmmRouteInfoCallback(CrmmRouteUpdateCallback callback) = 0;
    virtual void RegisterCrossClusterCommCallback(CrossClusterCommCallback callback) = 0;
    virtual void RegisterWaitForAllSessionDisconnectCallback(WaitForAllSessionDisconnectCallback callback) = 0;
    virtual void RegisterWaitForAllSQLThreadStopCallback(WaitForAllSQLThreadStopCallback callback) = 0;
    virtual RetStatus GetClusterRecoveryStatus(ClusterRecoveryStatus &status) = 0;

    /* For stat */
    virtual void RegisterReportWaitEventCallback(ReportWaitEventCallback callback) = 0;
    virtual void RegisterReportWaitEventFailedCallback(ReportWaitEventCallback callback) = 0;
    virtual void RegisterReportWaitStatusCallback(ReportWaitStatusCallback callback) = 0;
    virtual void RegisterReportXactTimestampCallback(ReportXactTimestampCallback callback) = 0;
    virtual void RegisterReportCurrentTopXidCallback(ReportCurrentTopXidCallback callback) = 0;
    virtual void RegisterReportCurrentXidCallback(ReportCurrentXidCallback callback) = 0;
    virtual void RegisterReportXlogLenCallback(ReportXlogLenCallback callback) = 0;
    virtual void RegisterReportXactInfoCallback(ReportXactInfoCallback callback) = 0;
    virtual void RegisterReportCountBufferReadCallback(ReportCountBufferReadCallback callback) = 0;
    virtual void RegisterReportCountBufferHitCallback(ReportCountBufferHitCallback callback) = 0;
    virtual void RegisterReportCountBufferCallback(ReportCountBufferCallback callback) = 0;
    virtual void RegisterReportLockStatCallback(ReportLockStatCallback callback) = 0;
    virtual void RegisterReportLWLockStatCallback(ReportLWLockStatCallback callback) = 0;
    virtual void RegisterReportSendBgwriterCallback(ReportSendBgwriterCallback callback) = 0;
    virtual void RegisterReportBufferReadTimeCallback(ReportBufferReadTimeCallback callback) = 0;
    virtual void RegisterReportBufferWriteTimeCallback(ReportBufferWriteTimeCallback callback) = 0;
    virtual void RegisterReportDeadLockTagCallback(ReportDeadLockTagCallback callback) = 0;
    virtual void RegisterReportSendTabstatCallback(ReportSendTabstatCallback callback) = 0;

    /* For lock */
    virtual void RegisterGetLockWaitTimeoutCallback(LockWaitTimeoutMSCallback callback) = 0;

    virtual void RegisterReportDataIOTimeRecordCallback(ReportDataIOTimeRecordCallback callback) = 0;
    /*
     * Note:
     *  [nodeIdArr] is a array pointer, its size will be set to [nodeCnt],
     *  caller must use DSTORE::DestroyObject to release it after used up
     *  if [nodeIdArr] is null, [nodeCnt]'s value is unbelievable.
     */
    virtual void GetCurrentComputeNodes(NodeId **nodeIdArr, uint32_t &nodeCnt) = 0;

    virtual bool IsPdbRollbackEnd(PdbId pdbId) = 0;
    virtual bool IsBackupRestoreRollbackToConsistencyPointEnd(PdbId pdbId = INVALID_PDB_ID) = 0;

    virtual void RegisterIndexCallbacks(IndexGetFuncCb *indexCb) = 0;
    virtual void RegisterDstoreInternalThreadCallback(ThreadInitCallback initCallback,
                                                      ThreadReleaseCallback releaseCallback) = 0;

    virtual void InitWorkingVersionNum(const uint32_t *workingGrandVersionNum) = 0;
    virtual uint32_t GetWorkingVersionNum() = 0;

    virtual void BindAllThreads() = 0;

    static StorageInstanceInterface *Create(StorageInstanceType type);
    static void DestoryInstance();
};
extern int64_t g_traceSwitch;
extern int g_gssFlushThreadNum;
extern int g_gssAsyncBatchPageNum;
extern bool g_enableAsyncIoFlush;
extern int g_gssAsyncFlushInterval;
extern int g_gssOlcAlgorithm;
extern uint32_t g_defaultPdbId;

RetStatus TestDestNodeAlive(uint32_t dstNode);

/* For DDL Synchronization */
void RegisterMetaDataInvByOidCallback(MetaDataInvByOidCallback callback);
void InvalidateMetaDataByOid(const Oid oid);
RetStatus InvalidateMetaDataByMsg(char *invalMsg, int numMsg, size_t msgLen, bool isLocalInv,
    DSTORE::MetaDataMsgType type);
RetStatus GetMetaDataInvalidationTask(char **invalMsg, int *numMsg, uint64_t *taskId);
RetStatus CompleteMetaDataInvalidationTask(uint64_t taskId);

/* For Pdb management */
using PdbStopSqlThreadCallback = RetStatus (*)(PdbId pdbId, int8_t switchOn, bool isImmdiate, bool isForce);
using PdbDestroySqlCacheCallback = RetStatus (*)(PdbId pdbId);
using PdbResetTimeCallback = RetStatus (*)(PdbId pdbId);

void RegisterPdbStopSqlThreadCallback(PdbStopSqlThreadCallback callback);
void RegisterPdbDestroySqlCacheCallback(PdbDestroySqlCacheCallback callback);
void RegisterPdbResetTimeCallback(PdbResetTimeCallback callback);

/* For Background Extension */
const int MAX_TASK_QUEUE_BG_THRD_NUM = 10;

void DestroyObject(void **ptr);
void InitSignalMask();
void CreateTemplateTablespace(PdbId pdbId);
void CreateUndoMapSegment(PdbId pdbId);

char *GetDataDir(PdbId pdbId);
char *GetDatabasePathByVfsName(const char *vfsName);
void SetDefaultPdbId(PdbId pdbId);

void MemCallBackinitInterface(void *reserve, void *release);

#pragma GCC visibility pop
} /* namespace DSTORE */
#endif
