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
 * dstore_instance.cpp
 *    StorageInstance is a class inherit from StorageInstanceInterface, which is a
 *        pure abstract class.
 *    StorageInstance provides all functions to access each single node managers,
 *        and functions to register/unregister a thread to instance.
 *    g_storageInstance is an extern global variable for user to get managers from
 *        storage instance.
 *
 * IDENTIFICATION
 *        storage/src/framework/dstore_instnace.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <climits>
#include <csignal>
#include "buffer/dstore_buf_mgr.h"
#include "buffer/dstore_bg_page_writer_mgr.h"
#include "buffer/dstore_buf_perf_unit.h"
#include "catalog/dstore_typecache.h"
#include "catalog/dstore_catalog_perf_unit.h"
#include "pdbreplica/dstore_pdbreplica_perf_unit.h"
#include "control/dstore_control_perf_unit.h"
#include "common/log/dstore_log.h"
#include "common/algorithm/dstore_tuplesort.h"
#include "vfs/vfs_interface.h"
#include "framework/dstore_vfs_adapter.h"
#include "systable/dstore_rootdb_utility.h"
#include "systable/sys_database.h"
#include "port/dstore_port.h"
#include "lock/dstore_lock_mgr.h"
#include "lock/dstore_table_lock_mgr.h"
#include "lock/dstore_xact_lock_mgr.h"
#include "lock/dstore_lock_interface.h"
#include "lock/dstore_lock_perf_unit.h"
#include "framework/dstore_instance_interface.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_interface.h"
#include "perfcounter/dstore_perf_scheduler.h"
#include "perfcounter/dstore_perf_catalog.h"
#include "transaction/dstore_transaction_perf_unit.h"
#include "wal/dstore_wal_perf_unit.h"
#include "tablespace/dstore_table_space_perfunit.h"
#include "framework/dstore_framework_perf_unit.h"
#include "recovery/dstore_recovery_perf_unit.h"
#include "heap/dstore_heap_perf_unit.h"
#include "index/dstore_btree_perf_unit.h"
#include "systable/dstore_systable_perf_unit.h"
#include "errorcode/dstore_pdb_error_code.h"
#include "errorcode/dstore_framework_error_code.h"
#include "framework/dstore_external_perf_unit.h"
#include "framework/dstore_instance.h"
#include "lock/dstore_lock_datatype.h"
#include "buffer/dstore_buf.h"

namespace DSTORE {

StorageInstance *g_storageInstance = nullptr;
int64 g_traceSwitch = 0;
PdbId g_defaultPdbId = PDB_ROOT_ID;
const int32 HASH_INIT_PARTITION_COUNT = 16;
const int32 HASH_INIT_ELEMENT_COUNT = 256;

void StorageInstance::InitPdbInfoCache()
{
    (void)pthread_rwlock_init(&m_pdbInfoCacheRwlock, nullptr);
    errno_t rc = memset_s(m_pdbInfoCache, sizeof(PdbInfoCache) * (PDB_MAX_ID + 1), 0,
                          sizeof(PdbInfoCache) * (PDB_MAX_ID + 1));
    storage_securec_check(rc, "\0", "\0");
}

bool StorageInstance::IsInPdbInfocache(const char *pdbName, PdbId *pdbidx)
{
    for (PdbId pdbid = 0; pdbid <= PDB_MAX_ID; pdbid++) {
        PdbInfoCache *pdbCache = &m_pdbInfoCache[pdbid];
        if ((pdbCache->cacheValide) && (pdbCache->pdbInfoCache.pdbId <= PDB_MAX_ID)) {
            if (strcmp(pdbName, pdbCache->pdbInfoCache.pdbName) == 0) {
                *pdbidx = pdbid;
                return true;
            }
        }
    }
    return false;
}

RetStatus StorageInstance::InvalidatePdbInfoCache(PdbId pdbId)
{
    if (!StoragePdb::IsValidPdbId(pdbId)) {
        storage_set_error(CONTROL_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Invalidate pdbInfoCache input error , pdbId %hhu", pdbId));
        return DSTORE_FAIL;
    }

    (void)pthread_rwlock_wrlock(&m_pdbInfoCacheRwlock);
    PdbInfoCache *pdbCache = &m_pdbInfoCache[pdbId];
    pdbCache->cacheValide = false;
    pdbCache->pdbInfoCache.pdbId = 0;
    (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);
    return DSTORE_SUCC;
}

RetStatus StorageInstance::UpdatePdbRoleModeInPdbInfoCache(PdbId pdbId, PdbRoleMode roleMode)
{
    if (!StoragePdb::IsValidPdbId(pdbId)) {
        storage_set_error(CONTROL_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Update pdbInfoCache input error, pdbId %hhu", pdbId));
        return DSTORE_FAIL;
    }

    (void)pthread_rwlock_wrlock(&m_pdbInfoCacheRwlock);
    PdbInfoCache *pdbCache = &m_pdbInfoCache[pdbId];
    if (pdbCache->cacheValide) {
        pdbCache->pdbInfoCache.pdbRoleMode = roleMode;
    }
    (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);
    return DSTORE_SUCC;
}

bool StorageInstance::IsParallelBuildThread(pthread_t thread)
{
    if (thread == INVALID_THREAD_ID) {
        return false;
    }
    char threadname[MAX_THREAD_NAME_LEN];
    if (pthread_getname_np(thread, threadname, MAX_THREAD_NAME_LEN) == 0 &&
        strcmp(threadname, "ParallelBuilder") == 0) {
        return true;
    }
    return false;
}

void StorageInstance::FetchBriefInfoFromCtrlItem(PdbBriefInfo *pdbBriefInfo, ControlPdbInfoPageItemData *pdbItem)
{
    if ((STORAGE_VAR_NULL(pdbBriefInfo)) || (STORAGE_VAR_NULL(pdbItem))) {
        StorageReleasePanic(true, MODULE_CONTROL, ErrMsg("Get pdb brief info by page item failed, invalid parameter."));
        return;
    }
    pdbBriefInfo->pdbId = pdbItem->pdbId;
    errno_t rc = strncpy_s(pdbBriefInfo->pdbName, DB_NAME_LEN, pdbItem->pdbName, strlen(pdbItem->pdbName));
    storage_securec_check(rc, "\0", "\0");
    rc = strncpy_s(pdbBriefInfo->pdbUuid, FORMATTED_UUID_ARR_LEN, pdbItem->pdbUuid, strlen(pdbItem->pdbUuid));
    storage_securec_check(rc, "\0", "\0");
    pdbBriefInfo->pdbStatus = pdbItem->pdbStatus;
    pdbBriefInfo->pdbSwitchStatus = pdbItem->pdbSwitchStatus;
    pdbBriefInfo->grpRespoolOid = pdbItem->grpRespoolOid;
    pdbBriefInfo->pdbRoleMode = pdbItem->pdbRoleMode;
    pdbBriefInfo->dbaId = pdbItem->dbaId;
}

void StorageInstance::FetchPdbInfoFromCtrlItem(PdbInfo *pdbInfo, ControlPdbInfoPageItemData *pdbItem)
{
    if ((STORAGE_VAR_NULL(pdbInfo)) || (STORAGE_VAR_NULL(pdbItem))) {
        StorageReleasePanic(true, MODULE_CONTROL, ErrMsg("Get pdb info by page item failed, invalid parameter."));
        return;
    }

    errno_t rc = strncpy_s(pdbInfo->pdbName, DB_NAME_LEN, pdbItem->pdbName, strlen(pdbItem->pdbName));
    storage_securec_check(rc, "\0", "\0");
    pdbInfo->templateId = 0;
    rc = strncpy_s(pdbInfo->pdbUuid, FORMATTED_UUID_ARR_LEN, pdbItem->pdbUuid, strlen(pdbItem->pdbUuid));
    storage_securec_check(rc, "\0", "\0");
    pdbInfo->pdbRoleMode = pdbItem->pdbRoleMode;
    pdbInfo->grpRespoolOid = pdbItem->grpRespoolOid;
    pdbInfo->pdbStatus = pdbItem->pdbStatus;
    pdbInfo->dbaId = pdbItem->dbaId;
    pdbInfo->sysDatabaseTuple = NULL;
    pdbInfo->pdbSwitchStatus = pdbItem->pdbSwitchStatus;
}

void SetDefaultPdbId(PdbId pdbId)
{
    g_defaultPdbId = pdbId;
}

BufMgrInterface *StorageInstance::GetBufferMgr()
{
    return m_bufMgr;
}

StorageGUC *StorageInstance::GetGuc()
{
    return m_guc;
}

StorageMemoryMgr *StorageInstance::GetMemoryMgr()
{
    return m_memoryMgr;
}

StoragePdb *StorageInstance::GetPdb(PdbId pdbId)
{
    if (pdbId >= PDB_START_ID && pdbId <= PDB_MAX_ID) {
        return m_pdb[StoragePdb::GetPdbIndex(pdbId)];
    }
    return nullptr;
}

RetStatus StorageInstance::RefreshPdbStatus(PdbId pdbId)
{
    if (!StoragePdb::IsValidPdbId(pdbId)) {
        return DSTORE_FAIL;
    }
    
    /* reload pdb info from rootpdb's ctrl file, be careful with the Locking sequence.
       Locks of control files cannot be added within the LWLock range, otherwise,
       a deadlock will occur. */
    PdbId pdbIndex = StoragePdb::GetPdbIndex(pdbId);
    do {
        switch (m_pdb[pdbIndex]->GetPdbStatus()) {
            case PdbStatus::PDB_STATUS_OPENED_READ_WRITE:
            case PdbStatus::PDB_STATUS_CLOSED:
            case PdbStatus::PDB_STATUS_DROPPING:
            case PdbStatus::PDB_STATUS_CREATING:
            case PdbStatus::PDB_STATUS_CLOSING:
            case PdbStatus::PDB_STATUS_UNCREATED:
                return DSTORE_SUCC;
            case PdbStatus::PDB_STATUS_NEED_RELOAD:
                break;
            default:
                ErrLog(DSTORE_PANIC, MODULE_BUFMGR,
                       ErrMsg("Unknown pdb status type: %hhu", static_cast<uint8>(m_pdb[pdbIndex]->GetPdbStatus())));
        }
        StoragePdb *rootPdb = g_storageInstance->GetPdb(g_defaultPdbId);
        if (STORAGE_VAR_NULL(rootPdb) || STORAGE_VAR_NULL(rootPdb->GetControlFile())) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("rootPdb or controlFile is nullptr."));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(rootPdb->GetControlFile()->ReadPdbCxtFromCtrlFile(pdbId))) {
            ErrLog(DSTORE_LOG, MODULE_PDB, ErrMsg("[Refresh Pdb Status]: PDB %u status change failed.", pdbId));
            break;
        }
        ErrLog(DSTORE_LOG, MODULE_PDB,
               ErrMsg("[Refresh Pdb Status]: PDB %u status change to [%hhu].", pdbId,
                      static_cast<uint8>(m_pdb[pdbIndex]->GetPdbStatus())));
    } while (true);

    return DSTORE_FAIL;
}

ThreadCoreMgr *StorageInstance::GetThreadCoreMgr()
{
    return &m_thrdCoreMgr;
}

CacheHashManager *StorageInstance::GetCacheHashMgr()
{
    return m_cacheHashMgr;
}

RetStatus StorageInstance::CreateMemMgr()
{
    if (m_memoryMgr != nullptr) {
        return DSTORE_FAIL;
    }
    m_memoryMgr = StorageMemoryMgr::create_memory_mgr(STORAGE_INSTANCE_MEMORY);
    if (STORAGE_VAR_NULL(m_memoryMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Out of memory when init mem mgr."));
        storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus StorageInstance::UpdateCpuRes(uint32_t minCpuCount, uint32_t maxCpuCount,
                                        uint32_t numaCount, NumaCpuInfo *numaCpuInfos)
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    return m_threadCpuAutoBinder->UpdateCpuRes(minCpuCount, maxCpuCount, numaCount, numaCpuInfos);
}

RetStatus StorageInstance::InitCpuRes(NumaInfo *numaInfos)
{
    bool enableAutoBinder = true;
    bool isRedoBindCpu;
    uint32 cpus[WAL_MAX_BIND_CPU_SIZE];
    uint32 bindCpuCount;
    WalRedoManager::ParseRedoBindCpuInfo(&isRedoBindCpu, cpus, &bindCpuCount);
    if (m_guc->walwriterCpuBind != INVALID_CPU_ID || m_guc->csnThreadBindCpu != INVALID_CPU_ID || isRedoBindCpu) {
        enableAutoBinder = false;
    }
    if (m_threadCpuAutoBinder == nullptr) {
        m_threadCpuAutoBinder = DstoreNew(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)) ThreadCpuAutoBinder();
    }
    if (STORAGE_VAR_NULL(m_threadCpuAutoBinder)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Out of memory when init thread cpu auto binder."));
        storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    if (numaInfos == nullptr || numaInfos->numaCount == 0 || numaInfos->numaCpuInfos == nullptr) {
        m_threadCpuAutoBinder->Initialize(enableAutoBinder);
        return DSTORE_SUCC;
    }
    m_threadCpuAutoBinder->Initialize(enableAutoBinder, numaInfos->numaCount, numaInfos->numaCpuInfos);
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]init cpu resouce success!"));
    return DSTORE_SUCC;
}

ThreadCpuAutoBinder *StorageInstance::GetThreadCpuAutoBinder()
{
    return m_threadCpuAutoBinder;
}

StorageInstance::StorageInstance()
    : m_memoryMgr(nullptr),
      m_threadCpuAutoBinder(nullptr),
      m_bufMgr(nullptr),
      m_lockMgr(nullptr),
      m_tableLockMgr(nullptr),
      m_xactLockMgr(nullptr),
      m_guc(nullptr),
      m_cacheHashMgr(nullptr),
      m_thrdCoreMgr(),
      m_csnMgr(nullptr),
      m_stat(),
      m_working_version_num(nullptr),
      m_instanceState(static_cast<uint8>(InstanceState::NOT_ACTIVE)),
      m_bootstrapping(false),
      m_instanceType(StorageInstanceType::SINGLE),
      m_stopBgThread(false),
      m_updateCsnMinThread(nullptr),
      m_pdbLwLock{},
      m_isInstanceShutdown(false),
      m_pdbGlobalTerm(0)
{
    for (auto &pdb : m_pdb) {
        pdb = nullptr;
    }
    for (auto &checkPoint : m_checkPoint) {
        checkPoint = nullptr;
    }
    for (auto &consistencyPoint : m_consistencyPoint) {
        consistencyPoint = nullptr;
    }
}

void StorageInstance::Initialize(StorageGUC *guc, bool bootStrap)
{
    StorageReleasePanic(true, MODULE_FRAMEWORK, ErrMsg("abandon interface!"));
    StorageReleasePanic(m_memoryMgr == nullptr, MODULE_FRAMEWORK, ErrMsg("m_memoryMgr is nullptr!"));
    StorageReleasePanic(m_instanceState != static_cast<uint8>(InstanceState::NOT_ACTIVE),
                        MODULE_FRAMEWORK, ErrMsg("m_memoryMgr is nullptr!"));
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    (void)GucInit(guc);
    (void)TypecacheMgrInit();
    (void)ThreadCoreMgrInit();
    InitCpuRes(guc->numaInfo);
    (void)BufMgrInit();
    (void)InitAllLockMgrs();
    InitPdbLwLocks();
    (void)InitPdbSlots();
    InitPdbInfoCache();
    if (bootStrap) {
        CreateTemplatePdb();  // Need remove this after fix UT setup
    }
    InitAllPdb();
    m_instanceState = static_cast<uint8>(InstanceState::ACTIVE);
    m_bootstrapping = bootStrap;
}

void StorageInstance::Destroy()
{
    StorageReleasePanic(true, MODULE_FRAMEWORK, ErrMsg("abandon interface!"));
    m_instanceState = static_cast<uint8>(InstanceState::NOT_ACTIVE);
    StorageInstance::StopBgThreads();

    DestroyAllPdb();
    DestroyCsnMgr();
    DestroyTableLockMgr();
    DestroyXactLockMgr();
    DestroyLockMgr();
    BufMgrDestroy();
    m_thrdCoreMgr.Destroy();
    DestroyCpuRes();
    TypecacheMgrDestroy();
    GucDestroy();
    StorageMemoryMgr::destroy(m_memoryMgr);
    m_memoryMgr = nullptr;
}

RetStatus StorageInstance::Bootstrap(StorageGUC *guc)
{
    RetStatus ret;
    if (m_memoryMgr == nullptr || m_instanceState != static_cast<uint8>(InstanceState::NOT_ACTIVE)) {
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    ret = GucInit(guc);
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    ret = TypecacheMgrInit();
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    ret = ThreadCoreMgrInit();
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    ret = InitCpuRes(guc->numaInfo);
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    ret = BufMgrInit();
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    ret = InitAllLockMgrs();
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    InitPdbLwLocks();
    ret = InitPdbSlots();
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    InitPdbInfoCache();

    CreateTemplatePDB();
    m_bootstrapping = true;
    m_instanceState = static_cast<uint8>(InstanceState::ACTIVE);
    return DSTORE_SUCC;
}

void StorageInstance::BootstrapDestroy()
{
    m_instanceState = static_cast<uint8>(InstanceState::NOT_ACTIVE);
    StorageInstance::StopBgThreads();
    CloseTemplatePDB();
    BufMgrDestroy();
    DestroyTableLockMgr();
    DestroyXactLockMgr();
    DestroyLockMgr();
    DestroyCsnMgr();
    m_bootstrapping = false;
}

void StorageInstance::BootstrapResDestroy()
{
    m_instanceState = static_cast<uint8>(InstanceState::NOT_ACTIVE);
    m_thrdCoreMgr.Destroy();
    DestroyCpuRes();
    TypecacheMgrDestroy();
    GucDestroy();
    StorageMemoryMgr::destroy(m_memoryMgr);
    m_memoryMgr = nullptr;
    m_bootstrapping = false;
}

RetStatus StorageInstance::StartupInstance(StorageGUC *guc)
{
    RetStatus ret;
    if (m_memoryMgr == nullptr || m_instanceState != static_cast<uint8>(InstanceState::NOT_ACTIVE)) {
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    ret = GucInit(guc);
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    ret = TypecacheMgrInit(&m_indexCallback);
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    ret = ThreadCoreMgrInit();
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    ret = InitCpuRes(guc->numaInfo);
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

#ifndef UT
    ret = InitPerfCounter();
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

#endif

    ret = BufMgrInit();
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    g_storageInstance->AddVisibleThread(thrd, g_defaultPdbId);
    ret = InitAllLockMgrs();
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    InitPdbLwLocks();
    ret = InitPdbSlots();
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    InitPdbInfoCache();
    OpenOnePdb(g_defaultPdbId, false);
    TuplesortMgr::DeleteAllIdxSortTmpFile();
    m_bootstrapping = false;
    m_instanceState = static_cast<uint8>(InstanceState::ACTIVE);
    return DSTORE_SUCC;
}

void StorageInstance::StopInstance()
{
    StorageReleasePanic(true, MODULE_FRAMEWORK, ErrMsg("interface not supported for single node."));
}

void StorageInstance::ShutdownInstance()
{
    m_instanceState = static_cast<uint8>(InstanceState::NOT_ACTIVE);
    StorageInstance::StopBgThreads();
#ifndef UT
    DestroyPerfCounter();
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    (void)g_storageInstance->GetBufferMgr()->FlushAll(false, false, INVALID_PDB_ID);
}
#endif
    CloseAllPdb(true);
    BufMgrDestroy();
    DestroyCsnMgr();
    DestroyTableLockMgr();
    DestroyXactLockMgr();
    DestroyLockMgr();
    if (likely(thrd != nullptr)) {
        thrd->DestroyTransactionRuntime();
        UnregisterThread();
    }
    ResourcesCleanUp();
}

StoragePdb *StorageInstance::CreatePdbObj(PdbId pdbId)
{
    StoragePdb *pdb = DstoreNew(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)) StoragePdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Out of memory when create pdb obj."));
        return nullptr;
    }

    if (STORAGE_FUNC_FAIL(SetPdbToSlot(pdb))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to set pdb to slot."));
        delete pdb;
        return nullptr;
    }
    return pdb;
}

StoragePdb *StorageInstance::CreatePdbObj(PdbId pdbId, const char* pdbUuid)
{
    StoragePdb *pdb = DstoreNew(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)) StoragePdb(pdbId, pdbUuid);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Out of memory when create pdb obj."));
        return nullptr;
    }
    if (STORAGE_FUNC_FAIL(SetPdbToSlot(pdb))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to set pdb to slot."));
        delete pdb;
        return nullptr;
    }
    return pdb;
}

FUNCTION_DELETE_LATER
void StorageInstance::CreateTemplatePdb()
{
    m_pdb[StoragePdb::GetPdbIndex(g_defaultPdbId)]->Create();
}

void StorageInstance::CreateTemplatePDB()
{
    m_pdb[StoragePdb::GetPdbIndex(g_defaultPdbId)]->CreatePdb(true);
    if (g_defaultPdbId == PDB_TEMPLATE1_ID || g_defaultPdbId == PDB_TEMPLATEA_ID ||
        g_defaultPdbId == PDB_TEMPLATEM_ID) {
        RootDBUtility::PreAllocPdbIdWhenBootstrap(g_defaultPdbId);
    }
}

PdbId StorageInstance::GetPdbIdByName(const char *pdbName)
{
    PdbBriefInfo info;
    if (STORAGE_VAR_NULL(GetPdb(g_defaultPdbId)) || !GetPdb(g_defaultPdbId)->IsInit()) {
        return INVALID_PDB_ID;
    }
    if (STORAGE_FUNC_FAIL(GetPdbBriefInfoByName(pdbName, &info))) {
        return INVALID_PDB_ID;
    }
    return info.pdbId;
}

PdbId StorageInstance::GetVisablePdbIdByName(const char *pdbName)
{
    PdbBriefInfo info;
    if (STORAGE_VAR_NULL(GetPdb(g_defaultPdbId)) || !GetPdb(g_defaultPdbId)->IsInit()) {
        return INVALID_PDB_ID;
    }
    if (STORAGE_FUNC_FAIL(GetVisablePdbBriefInfoByName(pdbName, &info))) {
        return INVALID_PDB_ID;
    }
    return info.pdbId;
}

RetStatus StorageInstance::GetPdbSize(Oid pdbId, int64 &pdbSize)
{
    StoragePdb *pdb = GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("Unable to get pdb from pdbid while calculating pdb size."));
        return DSTORE_FAIL;
    }
    if (pdb->m_vfs == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("Attempted to get file size but target PDB has null VFS."));
        return DSTORE_FAIL;
    }

    return pdb->GetSizeVfs(pdbSize);
}

uint64_t StorageInstance::GetPdbTerm(Oid pdbId)
{
    StoragePdb *pdb = GetPdb(pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("The pdb doesn't exist."));
    return pdb->GetPdbTerm();
}

PdbId StorageInstance::GetPdbIdByUuid(const char *pdbUuid)
{
    StoragePdb *rootPdb = GetPdb(g_defaultPdbId);
    StorageReleasePanic(rootPdb == nullptr, MODULE_FRAMEWORK, ErrMsg("Root pdb doesn't exist."));
    StorageReleasePanic(!rootPdb->IsInit(), MODULE_FRAMEWORK, ErrMsg("Root pdb is not inited."));
    PdbId pdbId = INVALID_PDB_ID;
    if (STORAGE_FUNC_FAIL(rootPdb->m_controlFile->GetAvailablePdbIdByUuid(pdbUuid, &pdbId))) {
        return INVALID_PDB_ID;
    }
    return pdbId;
}

bool StorageInstance::IsWalMgrInit(const PdbId &pdbId)
{
    if (pdbId < PDB_TEMPLATE1_ID || pdbId > PDB_MAX_ID) {
        return false;
    }

    /* In the expansion scenario, the rootpdb has not been initialized, and the new pdb needs to be opened. */
    if (pdbId != g_defaultPdbId) {
        StoragePdb *rootPdb = StorageInstance::GetPdb(g_defaultPdbId);
        StorageReleasePanic(rootPdb == nullptr, MODULE_FRAMEWORK, ErrMsg("Root pdb doesn't exist."));
        while (!rootPdb->IsWalMgrInit()) {
            const uint32 sleepTime = 1000;
            GaussUsleep(sleepTime);
        }
    }

    StoragePdb *pdb = StorageInstance::GetPdb(pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("Pdb %u doesn't exist.", pdbId));
    if (!pdb->IsWalMgrInit()) {
        return false;
    }
    return true;
}

RetStatus StorageInstance::GetPdbInfoById(PdbId pdbId, PdbInfo *pdbInfo)
{
    errno_t rc;
    PdbInfoCache *pdbCache;

    if (STORAGE_VAR_NULL(pdbInfo) || !StoragePdb::IsValidPdbId(pdbId)) {
        storage_set_error(CONTROL_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Invalid input, pdbId %hhu", pdbId));
        return DSTORE_FAIL;
    }
    (void)RefreshPdbStatus(pdbId);

    (void)pthread_rwlock_rdlock(&m_pdbInfoCacheRwlock);
    pdbCache = &m_pdbInfoCache[pdbId];
    if ((pdbCache->cacheValide) && (pdbCache->pdbInfoCache.pdbId == pdbId)) {
        rc = memcpy_s(pdbInfo, sizeof(PdbInfo), &pdbCache->pdbInfoCache, sizeof(PdbInfo));
        storage_securec_check(rc, "\0", "\0");
        (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);
        return DSTORE_SUCC;
    }

    if (pdbCache->pdbInfoCache.pdbId > PDB_MAX_ID) {
        (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);
        return DSTORE_FAIL;
    }
    (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);

    StoragePdb *rootPdb = GetPdb(g_defaultPdbId);
    StorageReleasePanic((rootPdb == nullptr) || (!rootPdb->IsInit()), MODULE_FRAMEWORK,
        ErrMsg("Root pdb doesn't exist or is not inited."));
    RetStatus ret = rootPdb->m_controlFile->GetPdbInfoById(pdbId, pdbInfo);
    (void)pthread_rwlock_wrlock(&m_pdbInfoCacheRwlock);
    if (ret == DSTORE_SUCC) {
        /* recheck pdbid, to prevent it from being obtained by other threads. */
        if ((!pdbCache->cacheValide) || (pdbCache->pdbInfoCache.pdbId != pdbId)) {
            pdbCache = &m_pdbInfoCache[pdbId];
            rc = memcpy_s(&pdbCache->pdbInfoCache, sizeof(PdbInfo), pdbInfo, sizeof(PdbInfo));
            storage_securec_check(rc, "\0", "\0");
            pdbCache->pdbInfoCache.pdbId = pdbId;
            pdbCache->cacheValide = true;
        }
    } else {
        /* Set pdbId as invalid which over the PDB_MAX_ID, reset it when invalidate pdb. */
        pdbCache->pdbInfoCache.pdbId = PDB_MAX_ID + 1;
    }
    (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);

    return ret;
}

void StorageInstance::UpdatePdbInfoById(PdbId pdbId, PdbInfo *pdbInfo)
{
    errno_t rc;

    if (STORAGE_VAR_NULL(pdbInfo) || !StoragePdb::IsValidPdbId(pdbId)) {
        StorageReleasePanic(true, MODULE_SYSTABLE, ErrMsg("UpdatePdbInfoById fail. Invalid input, pdbId %hhu", pdbId));
        return;
    }

    StoragePdb *pdb = GetPdb(g_defaultPdbId);
    StorageReleasePanic((pdb == nullptr) || (!pdb->IsInit()), MODULE_FRAMEWORK,
        ErrMsg("Root pdb doesn't exist or is not inited."));
    ControlFile *controlFile = pdb->GetControlFile();
    StorageReleasePanic(controlFile == nullptr, MODULE_FRAMEWORK, ErrMsg("Root pdb control file doesn't exist."));

    const ControlPdbInfoPageItemData *oldPdbInfo = controlFile->GetPdbInfoById(pdbId);
    StorageReleasePanic(oldPdbInfo == nullptr, MODULE_SYSTABLE, ErrMsg("pdbId %hhu not create!", pdbId));
    ControlPdbInfoPageItemData newPdbInfo = *oldPdbInfo;
    rc = strncpy_s(newPdbInfo.pdbName, PDB_NAME_LEN, pdbInfo->pdbName, strlen(pdbInfo->pdbName));
    storage_securec_check(rc, "\0", "\0");
    newPdbInfo.grpRespoolOid = pdbInfo->grpRespoolOid;
    RetStatus ret = controlFile->UpdatePdbItemData(pdbId, &newPdbInfo, sizeof(ControlPdbInfoPageItemData));
    if (ret == DSTORE_SUCC) {
        (void)InvalidatePdbInfoCache(pdbId);
    }

    StorageReleasePanic(ret != DSTORE_SUCC, MODULE_SYSTABLE, ErrMsg("Update pdb id failed."));
}

RetStatus StorageInstance::GetPdbBriefInfoByName(const char *pdbName, PdbBriefInfo *pdbBriefInfo)
{
    PdbId pdbId = 0;
    PdbInfoCache *pdbCache = nullptr;
    errno_t rc;

    if ((STORAGE_VAR_NULL(pdbName)) || (STORAGE_VAR_NULL(pdbBriefInfo))) {
        storage_set_error(CONTROL_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get pdb brief info failed, invalid parameter."));
        return DSTORE_FAIL;
    }

    /* Try to get pdb brief info from cache. */
    (void)pthread_rwlock_rdlock(&m_pdbInfoCacheRwlock);
    if (IsInPdbInfocache(pdbName, &pdbId)) {
        pdbCache = &m_pdbInfoCache[pdbId];
        rc = memcpy_s(pdbBriefInfo, sizeof(PdbBriefInfo), &pdbCache->pdbInfoCache.briefInfo, sizeof(PdbBriefInfo));
        storage_securec_check(rc, "\0", "\0");
        (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);
        return DSTORE_SUCC;
    }
    (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);

    /* If not found in cache, get from control file. */
    StoragePdb *rootPdb = GetPdb(g_defaultPdbId);
    StorageReleasePanic((rootPdb == nullptr) || (!rootPdb->IsInit()), MODULE_FRAMEWORK,
        ErrMsg("Root pdb doesn't exist or is not inited."));
    ControlPdbInfoPageItemData *pdbItem = rootPdb->m_controlFile->GetAvailablePdbInfo(pdbName);
    if (pdbItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get pdb brief info by name from control file failed."));
        return DSTORE_FAIL;
    }

    FetchBriefInfoFromCtrlItem(pdbBriefInfo, pdbItem);

    /* refresh pdbCache */
    (void)pthread_rwlock_wrlock(&m_pdbInfoCacheRwlock);
    /* recheck pdbid, to prevent it from being obtained by other threads. */
    if (!IsInPdbInfocache(pdbName, &pdbId)) {
        pdbCache = &m_pdbInfoCache[pdbBriefInfo->pdbId];
        rc = memcpy_s(&pdbCache->pdbInfoCache.briefInfo, sizeof(PdbBriefInfo), pdbBriefInfo, sizeof(PdbBriefInfo));
        storage_securec_check(rc, "\0", "\0");

        FetchPdbInfoFromCtrlItem(&pdbCache->pdbInfoCache, pdbItem);
        pdbCache->cacheValide = true;
    }

    (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);

    (void)RefreshPdbStatus(pdbBriefInfo->pdbId);
    return DSTORE_SUCC;
}

RetStatus StorageInstance::GetVisablePdbBriefInfoByName(const char *pdbName, PdbBriefInfo *pdbBriefInfo)
{
    PdbId pdbId = 0;
    PdbInfoCache *pdbCache = nullptr;
    errno_t rc;

    if ((STORAGE_VAR_NULL(pdbName)) || (STORAGE_VAR_NULL(pdbBriefInfo))) {
        storage_set_error(CONTROL_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get pdb brief info failed, invalid parameter."));
        return DSTORE_FAIL;
    }

    /* Try to get pdb brief info from cache. */
    (void)pthread_rwlock_rdlock(&m_pdbInfoCacheRwlock);
    if (IsInPdbInfocache(pdbName, &pdbId)) {
        pdbCache = &m_pdbInfoCache[pdbId];
        rc = memcpy_s(pdbBriefInfo, sizeof(PdbBriefInfo), &pdbCache->pdbInfoCache.briefInfo, sizeof(PdbBriefInfo));
        storage_securec_check(rc, "\0", "\0");
        (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);
        return DSTORE_SUCC;
    }
    (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);

    /* If not found in cache, get from control file. */
    StoragePdb *rootPdb = GetPdb(g_defaultPdbId);
    StorageReleasePanic((rootPdb == nullptr) || (!rootPdb->IsInit()), MODULE_FRAMEWORK,
        ErrMsg("Root pdb doesn't exist or is not inited."));
    ControlPdbInfoPageItemData *pdbItem = rootPdb->m_controlFile->GetPdbInfo(pdbName);
    if (pdbItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get pdb brief info by name from control file failed."));
        return DSTORE_FAIL;
    }

    FetchBriefInfoFromCtrlItem(pdbBriefInfo, pdbItem);

    /* refresh pdbCache */
    (void)pthread_rwlock_wrlock(&m_pdbInfoCacheRwlock);
    /* recheck pdbid, to prevent it from being obtained by other threads. */
    if (!IsInPdbInfocache(pdbName, &pdbId)) {
        pdbCache = &m_pdbInfoCache[pdbBriefInfo->pdbId];
        rc = memcpy_s(&pdbCache->pdbInfoCache.briefInfo, sizeof(PdbBriefInfo), pdbBriefInfo, sizeof(PdbBriefInfo));
        storage_securec_check(rc, "\0", "\0");

        FetchPdbInfoFromCtrlItem(&pdbCache->pdbInfoCache, pdbItem);
        pdbCache->cacheValide = true;
    }

    (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);

    (void)RefreshPdbStatus(pdbBriefInfo->pdbId);
    return DSTORE_SUCC;
}

RetStatus StorageInstance::GetPdbBriefInfoById(PdbId pdbId, PdbBriefInfo *pdbBriefInfo)
{
    errno_t rc;

    if ((!StoragePdb::IsValidPdbId(pdbId)) || (STORAGE_VAR_NULL(pdbBriefInfo))) {
        storage_set_error(CONTROL_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Invalid input, pdbId %hhu", pdbId));
        return DSTORE_FAIL;
    }

    (void)RefreshPdbStatus(pdbId);

    (void)pthread_rwlock_rdlock(&m_pdbInfoCacheRwlock);
    PdbInfoCache *pdbCache = &m_pdbInfoCache[pdbId];
    if ((pdbCache->cacheValide) && (pdbCache->pdbInfoCache.pdbId == pdbId)) {
        rc = memcpy_s(pdbBriefInfo, sizeof(PdbBriefInfo), &pdbCache->pdbInfoCache.briefInfo, sizeof(PdbBriefInfo));
        storage_securec_check(rc, "\0", "\0");
        (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);
        return DSTORE_SUCC;
    }
    (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);

    StoragePdb *rootPdb = GetPdb(g_defaultPdbId);
    StorageReleasePanic((rootPdb == nullptr) || (!rootPdb->IsInit()), MODULE_FRAMEWORK,
        ErrMsg("Root pdb doesn't exist or is not inited."));
    ControlPdbInfoPageItemData *pdbItem = rootPdb->m_controlFile->GetAvailablePdbInfo(pdbId);
    if (pdbItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get pdb brief info by id from control file failed."));

        (void)pthread_rwlock_wrlock(&m_pdbInfoCacheRwlock);
        /* Set pdbId as invalid which over the PDB_MAX_ID, reset it when invalidate pdb. */
        pdbCache->pdbInfoCache.pdbId = PDB_MAX_ID + 1;
        (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);

        return DSTORE_FAIL;
    }

    FetchBriefInfoFromCtrlItem(pdbBriefInfo, pdbItem);

    (void)pthread_rwlock_wrlock(&m_pdbInfoCacheRwlock);
    /* recheck pdbid, to prevent it from being obtained by other threads. */
    if ((!pdbCache->cacheValide) || (pdbCache->pdbInfoCache.pdbId != pdbId)) {
        rc = memcpy_s(&pdbCache->pdbInfoCache.briefInfo, sizeof(PdbBriefInfo), pdbBriefInfo, sizeof(PdbBriefInfo));
        storage_securec_check(rc, "\0", "\0");

        FetchPdbInfoFromCtrlItem(&pdbCache->pdbInfoCache, pdbItem);
        pdbCache->cacheValide = true;
    }
    (void)pthread_rwlock_unlock(&m_pdbInfoCacheRwlock);

    return DSTORE_SUCC;
}

RetStatus StorageInstance::GetPdbBriefInfoAll(PdbBriefInfo *pdbIdInfos, uint32_t *pdbCount)
{
    StoragePdb *rootPdb = GetPdb(g_defaultPdbId);
    if (STORAGE_VAR_NULL(rootPdb) || !rootPdb->IsInit()) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("rootPdb is nullptr or not init"));
        return DSTORE_FAIL;
    }
    if (STORAGE_VAR_NULL(pdbIdInfos) || STORAGE_VAR_NULL(pdbCount)) {
        storage_set_error(CONTROL_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get pdbBriefInfo all invalid input"));
        return DSTORE_FAIL;
    }

    if (rootPdb->m_controlFile->GetVisiablePdbInfoAll(pdbIdInfos, pdbCount) == DSTORE_FAIL) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus StorageInstance::GetPdbBriefInfoVisable(PdbBriefInfo *pdbIdInfos, uint32_t *pdbCount)
{
    StoragePdb *rootPdb = GetPdb(g_defaultPdbId);
    StorageReleasePanic(rootPdb == nullptr, MODULE_FRAMEWORK, ErrMsg("Root pdb doesn't exist."));

    if (STORAGE_VAR_NULL(pdbIdInfos) || STORAGE_VAR_NULL(pdbCount)) {
        storage_set_error(CONTROL_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get pdbBriefInfo all invalid input"));
        return DSTORE_FAIL;
    }

    StorageReleasePanic(rootPdb->m_controlFile == nullptr, MODULE_FRAMEWORK,
        ErrMsg("Root pdb control file doesn't exist."));
    if (rootPdb->m_controlFile->GetVisiablePdbInfoAll(pdbIdInfos, pdbCount) == DSTORE_FAIL) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus StorageInstance::UpdatePdbStatusById(PdbId pdbId, PdbStatus pdbStatus)
{
    StoragePdb *pdb = GetPdb(g_defaultPdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("Root pdb doesn't exist."));
    ControlFile *controlFile = pdb->GetControlFile();
    StorageReleasePanic(controlFile == nullptr, MODULE_FRAMEWORK, ErrMsg("Root pdb control file doesn't exist."));

    ControlPdbInfoPageItemData *pdbInfo = controlFile->GetPdbInfoById(pdbId);
    StorageReleasePanic(pdbInfo == nullptr, MODULE_SYSTABLE, ErrMsg("pdbId %hhu not create!", pdbId));
    pdbInfo->pdbStatus = pdbStatus;
    RetStatus ret = controlFile->UpdatePdbItemData(pdbId, pdbInfo, sizeof(ControlPdbInfoPageItemData));
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_PDB, ErrMsg("Update pdb %u status to %d failed.",
            pdbId, static_cast<uint8>(pdbInfo->pdbStatus)));
        return ret;
    }
    if (STORAGE_FUNC_FAIL(InvalidatePdbInfoCache(pdbId))) {
        ErrLog(DSTORE_ERROR, MODULE_PDB, ErrMsg("Update pdb %u info cache failed.", pdbId));
        return DSTORE_FAIL;
    }

    // update newPdb->m_pdbStatus
    LockPdbObj<LW_EXCLUSIVE>(pdbId);
    StoragePdb *newPdb = GetPdb(pdbId);
    newPdb->SetPdbStatus(pdbStatus);
    UnLockPdbObj(pdbId);
    return DSTORE_SUCC;
}

RetStatus StorageInstance::AllocPdb(PdbInfo *pdbInfo, const char* vfsName)
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));

    /* Step1: Allocate pdb id for new pdb.
     * a) When in gs_initdb, template1 and RootCDB are allocated fixed pdb id.
     * b) User pdb is allocated pdb id from the control file of RootCDB.
     */
    PdbId &pdbId = pdbInfo->pdbId;
    if (strcmp(pdbInfo->pdbName, PDB_ROOT_NAME) == 0) {
        pdbId = PDB_ROOT_ID;
    } else if (strcmp(pdbInfo->pdbName, PDB_TEMPLATE0_NAME) == 0) {
        pdbId = PDB_TEMPLATE0_ID;
    } else {
        StoragePdb *rootPdb = GetPdb(g_defaultPdbId);
        if (STORAGE_VAR_NULL(rootPdb)) {
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(rootPdb->m_controlFile->AllocPdbId(pdbInfo->pdbName, pdbId, false, vfsName))) {
            return DSTORE_FAIL;
        }
    }

    LockPdbObj<LW_EXCLUSIVE>(pdbId);
    StoragePdb *newPdb = GetPdb(pdbId);
    if (STORAGE_VAR_NULL(newPdb)) {
        UnLockPdbObj(pdbId);
        return DSTORE_FAIL;
    }
    newPdb->SetPdbStatus(PdbStatus::PDB_STATUS_CREATING);
    UnLockPdbObj(pdbId);
    return DSTORE_SUCC;
}

RetStatus StorageInstance::CreatePDB(PdbInfo *pdbInfo)
{
    /* create standby pdb is not required for single node. */
    if (pdbInfo->pdbRoleMode == PdbRoleMode::PDB_STANDBY) {
        return DSTORE_FAIL;
    }
    if (pdbInfo->pdbId == INVALID_PDB_ID) {
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    PdbId lastPdbId = thrd->GetXactPdbId();

    PdbId pdbId = pdbInfo->pdbId;
    /* Step2: Create pdb object and add it to pdb array in instance. */
    StoragePdb *newPdb = GetPdb(pdbId);
    StorageReleasePanic(newPdb == nullptr, MODULE_FRAMEWORK, ErrMsg("Pdb %u doesn't exist.", pdbId));
    errno_t rc = memcpy_s(newPdb->m_pdbName, DB_NAME_LEN, pdbInfo->pdbName, DB_NAME_LEN);
    storage_securec_check(rc, "\0", "\0");
    newPdb->SetPdbRoleMode(pdbInfo->pdbRoleMode);
    if (STORAGE_FUNC_FAIL(SetPdbToSlot(newPdb))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to set pdb to slot."));
        return DSTORE_FAIL;
    }

    /* Step3: Create VFS and default files for the new PDB */
    if (STORAGE_FUNC_FAIL(newPdb->CreateVfsAndFiles(pdbId))) {
        return DSTORE_FAIL;
    }

    /* Step4: Copy files from template and update PENDING status to control file.
     * a) Copy control file, data files and wal files form template vfs.
     * b) Build control file pdb page and set PENDING to cdb id status, then flush page to disk.
     */

    ControlPdbInfoPageItemData pdbInfoPageItemData;
    if (STORAGE_FUNC_FAIL(CopyVfsAndFiles(newPdb, pdbInfo, &pdbInfoPageItemData))) {
        return DSTORE_FAIL;
    }

#ifndef UT
    if (STORAGE_FUNC_FAIL(DoInnerlyOpenPdb(newPdb, &pdbInfoPageItemData))) {
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(UpdateSysTable(lastPdbId, pdbId, pdbInfo))) {
        return DSTORE_FAIL;
    }
#endif

    if (pdbId >= FIRST_USER_PDB_ID) {
        if (STORAGE_FUNC_FAIL(GetPdb(PDB_ROOT_ID)->GetControlFile()->UpdateTotalPdbCount(true))) {
            return DSTORE_FAIL;
        }
    }

    if (STORAGE_FUNC_FAIL(SwitchContextToTargetPdb(lastPdbId, true, nullptr))) {
        return DSTORE_FAIL;
    }

    if (LockInterface::LockPdb(pdbId, DSTORE_ACCESS_EXCLUSIVE_LOCK) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }

    /* When creating template0, we write template1 control file which will be copied to rootpdb. */
    RetStatus ret = DSTORE_SUCC;
    if (pdbId == PDB_ROOT_ID) {
        pdbInfoPageItemData.pdbStatus = PdbStatus::PDB_STATUS_OPENED_READ_WRITE;
        ret = GetPdb(PDB_ROOT_ID)->GetControlFile()->UpdatePdbItemData(pdbId, &pdbInfoPageItemData,
            sizeof(ControlPdbInfoPageItemData));
    } else if (pdbId >= FIRST_USER_PDB_ID) {
        ret = GetPdb(PDB_ROOT_ID)->GetControlFile()->UpdatePdbItemData(pdbId, &pdbInfoPageItemData,
            sizeof(ControlPdbInfoPageItemData));
    } else {
        ret = GetPdb(g_defaultPdbId)->GetControlFile()->UpdatePdbItemData(pdbId, &pdbInfoPageItemData,
            sizeof(ControlPdbInfoPageItemData));
    }
    LockInterface::UnlockPdb(pdbId, DSTORE_ACCESS_EXCLUSIVE_LOCK);
    return ret;
}

RetStatus StorageInstance::InitPdbByName(const char *pdbName, bool keepPrevPdbContext)
{
    PdbInfo pdbInfo;
    errno_t rc = memset_s(&pdbInfo, sizeof(pdbInfo), 0, sizeof(pdbInfo));
    storage_securec_check(rc, "\0", "\0");
    pdbInfo.pdbRoleMode = PdbRoleMode::PDB_PRIMARY;
    rc = strcpy_s(pdbInfo.pdbName, DB_NAME_LEN, pdbName);
    storage_securec_check(rc, "\0", "\0");
    return OpenPDB(&pdbInfo, keepPrevPdbContext);
}

void StorageInstance::InitWorkingVersionNum(const uint32_t *workingGrandVersionNum)
{
    StorageReleasePanic(workingGrandVersionNum == nullptr, MODULE_FRAMEWORK,
                        ErrMsg("workingGrandVersionNum is nullptr"));
    m_working_version_num = const_cast<uint32_t*>(workingGrandVersionNum);
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
        ErrMsg("InitWorkingVersionNum set workingGrandVersionNum to %u.", *m_working_version_num));
}

uint32_t StorageInstance::GetWorkingVersionNum()
{
    return GsAtomicReadU32(m_working_version_num);
}

RetStatus StorageInstance::DropPDB(const char* pdbName)
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    StoragePdb *pdb = nullptr;

    /* step1: Get RootDB */
    StoragePdb *rootPdb = GetPdb(g_defaultPdbId);  /* FIX ME: we use tempdb as rootPdb, will replace it later */
    StorageReleasePanic(rootPdb == nullptr, MODULE_FRAMEWORK, ErrMsg("Root pdb doesn't exist."));
    StorageReleasePanic(rootPdb->m_controlFile == nullptr, MODULE_FRAMEWORK,
        ErrMsg("Root pdb control file doesn't exist."));

    ControlPdbInfoPageItemData *itemData = rootPdb->m_controlFile->GetDropablePdbInfo(pdbName);
    if (itemData == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to get pdbInfo for %s from root_pdb", pdbName));
        return DSTORE_FAIL;
    }
    if (unlikely(itemData->pdbId == INVALID_PDB_ID)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed to find PDB: %s in controlFile, maybe not exist", pdbName));
        return DSTORE_FAIL;
    }
    if (unlikely(itemData->pdbId < FIRST_USER_PDB_ID)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Cannot delete PDB %s, which is reserved by the system.", pdbName));
        return DSTORE_FAIL;
    }

    if (LockInterface::LockPdb(itemData->pdbId, DSTORE_ACCESS_EXCLUSIVE_LOCK) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }
    /* update pdbStatus on root control file */
    if (STORAGE_FUNC_FAIL(rootPdb->m_controlFile->SetDeleteFlag(itemData->pdbId))) {
        LockInterface::UnlockPdb(itemData->pdbId, DSTORE_ACCESS_EXCLUSIVE_LOCK);
        return DSTORE_FAIL;
    }
    LockInterface::UnlockPdb(itemData->pdbId, DSTORE_ACCESS_EXCLUSIVE_LOCK);

    (void)InvalidatePdbInfoCache(itemData->pdbId);
    pdb = GetPdb(itemData->pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("Pdb %u doesn't exist.", itemData->pdbId));

    LockPdbObj<LW_EXCLUSIVE>(itemData->pdbId);
    pdb->SetPdbStatus(PdbStatus::PDB_STATUS_DROPPING);
    UnLockPdbObj(itemData->pdbId);
    if (STORAGE_VAR_NULL(pdb->m_vfs)) {
        if (STORAGE_FUNC_FAIL(pdb->MountExistingVFS(itemData->vfsName))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to mount vfs when drop pdb %s", pdbName));
            return DSTORE_FAIL;
        }
    }
    if (STORAGE_VAR_NULL(pdb->m_controlFile) || !pdb->IsInit()) {
        if (STORAGE_FUNC_FAIL(pdb->InitControlFile())) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Pdb InitControlfile fail."));
            return DSTORE_FAIL;
        }
    }

    /* Single node : use InvalidateUsingGivenPdbId to invalid single node's buffer */
    if (STORAGE_FUNC_FAIL(g_storageInstance->GetBufferMgr()->InvalidateUsingGivenPdbId(itemData->pdbId))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Invalid pdb %u buffer fail.", itemData->pdbId));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(DoDropPdbForce(pdb, itemData->vfsName))) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

void StorageInstance::StopAllPdbBgThreads(UNUSE_PARAM PdbId pdbId)
{
    StoragePdb *pdb = GetPdb(pdbId);
    if (pdb == nullptr || !pdb->IsInit()) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("PDB(%u) is not exist in memory or not initialized, no bg thread.", pdbId));
        return;
    }
    pdb->StopBgThread();
}

RetStatus StorageInstance::StopAllPdbBgThreadsPhase1(UNUSE_PARAM PdbId pdbId)
{
    return DSTORE_SUCC;
}

RetStatus StorageInstance::StopAllPdbBgThreadsPhase2(UNUSE_PARAM PdbId pdbId)
{
    return DSTORE_SUCC;
}

RetStatus StorageInstance::SendPDBResetTime(PdbId pdbId)
{
    (void)pdbId;
    return DSTORE_SUCC;
}


RetStatus StorageInstance::SendPDBStopSqlThreads(PdbId pdbId, int8_t switchOn, bool immediate, bool force)
{
    (void)pdbId;
    (void)switchOn;
    (void)immediate;
    (void)force;
    return DSTORE_SUCC;
}

RetStatus StorageInstance::SendPDBStopBgThreadsPhase1(PdbId pdbId)
{
    (void)pdbId;
    return DSTORE_SUCC;
}

RetStatus StorageInstance::SendPDBStopBgThreadsPhase2(PdbId pdbId)
{
    (void)pdbId;
    return DSTORE_SUCC;
}

RetStatus StorageInstance::SendPDBReset(PdbId pdbId)
{
    (void)pdbId;
    return DSTORE_SUCC;
}

RetStatus StorageInstance::SendPDBInvalidateBuffer(PdbId pdbId)
{
    (void)pdbId;
    return DSTORE_SUCC;
}

void StorageInstance::CloseTemplatePDB()
{
    if (m_pdb[StoragePdb::GetPdbIndex(g_defaultPdbId)] == nullptr ||
        g_storageInstance->GetGuc()->tenantConfig == nullptr) {
        StorageReleasePanic(true, MODULE_FRAMEWORK,
                            ErrMsg("rootPdb is nullptr or tenantConfig is nullptr."));
    }
    const char *vfsName = g_storageInstance->GetGuc()->tenantConfig->storageConfig.template1VfsName;
    m_pdb[StoragePdb::GetPdbIndex(g_defaultPdbId)]->ResetPdb(vfsName, false);
}

Oid StorageInstance::GetDefaultTablespaceOid()
{
    return static_cast<Oid>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
}

RetStatus StorageInstance::OpenPDB(PdbInfo *pdbInfo, bool keepPrevPdbContext)
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    StoragePdb *rootPdb = GetPdb(g_defaultPdbId);
    if (STORAGE_VAR_NULL(rootPdb) || STORAGE_VAR_NULL(rootPdb->GetControlFile())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("rootPdb or controlFile is nullptr."));
        return DSTORE_FAIL;
    }
    ControlPdbInfoPageItemData *pdbItem = rootPdb->GetControlFile()
            ->GetAvailablePdbInfo(pdbInfo->pdbName);
    if (pdbItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("Open invalid pdb: %s.", pdbInfo->pdbName));
        return DSTORE_FAIL;
    }

    PdbId lastPdbId = thrd->GetXactPdbId();
    LockPdbObj<LW_EXCLUSIVE>(pdbItem->pdbId);
    StoragePdb *pdb = GetPdb(pdbItem->pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("pdb is nullptr."));
        UnLockPdbObj(pdbItem->pdbId);
        return DSTORE_FAIL;
    }
    if (!pdb->IsInit()) {
        errno_t rc = memcpy_s(pdb->m_pdbName, DB_NAME_LEN, pdbInfo->pdbName, DB_NAME_LEN);
        storage_securec_check(rc, "\0", "\0");
        if (STORAGE_FUNC_FAIL(SetPdbToSlot(pdb))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to set pdb to slot."));
            UnLockPdbObj(pdbItem->pdbId);
            return DSTORE_FAIL;
        }
        if (pdb->IsInit()) {
            UnLockPdbObj(pdbItem->pdbId);
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("PDB(%s) already available.", pdbInfo->pdbName));
            return DSTORE_SUCC;
        }
        thrd->SetXactPdbId(pdbItem->pdbId);
        if (STORAGE_FUNC_FAIL(pdb->OpenPdb(static_cast<void *>(pdbItem)))) {
            UnLockPdbObj(pdbItem->pdbId);
            thrd->SetXactPdbId(lastPdbId);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to switch to PDB: %s", pdbInfo->pdbName));
            return DSTORE_FAIL;
        }
        thrd->SetXactPdbId(keepPrevPdbContext ? lastPdbId : pdbItem->pdbId);
    }
    UnLockPdbObj(pdbItem->pdbId);
    pdb->CleanTempFile(true);
#ifdef UT
    (void)thrd->InitTransactionRuntime(keepPrevPdbContext ? lastPdbId : pdbItem->pdbId, nullptr, nullptr);
#endif
    return DSTORE_SUCC;
}

RetStatus StorageInstance::ClosePDB(const char* pdbName, UNUSE_PARAM bool immediate)
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));

    ControlPdbInfoPageItemData *pdbItem = GetPdb(g_defaultPdbId)->GetControlFile()->GetPdbInfo(pdbName);
    StorageReleasePanic(pdbItem == nullptr, MODULE_FRAMEWORK, ErrMsg("Cannot find pdb item: %s.", pdbName));
    PdbId pdbId = pdbItem->pdbId;
    if (unlikely(pdbId == INVALID_PDB_ID)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("Non-existing pdb: %s.", pdbName));
        return DSTORE_FAIL;
    }

    if (pdbItem->AlreadyClosed()) {
        return DSTORE_SUCC;
    }

    StoragePdb *pdb = GetPdb(pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("pdb %u is nullptr.", pdbId));

    LockPdbObj<LW_EXCLUSIVE>(pdbId);
    if (!pdb->IsInit()) {
        pdb->SetVFSName(pdbItem->vfsName);
    }
    UnLockPdbObj(pdbId);

    if (g_storageInstance->GetType() == StorageInstanceType::SINGLE) {
        /* Once the interface for flushing pages in single pdb is provided, replace FlusAll(). */
        /* Flush all dirty pages before closing pdb. */
        RetStatus ret = g_storageInstance->GetBufferMgr()->FlushAll(false, false, pdbId);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Flush all dirty pages failed,  pdbId: %u.", pdbId));
            return DSTORE_FAIL;
        }
        ret = g_storageInstance->GetBufferMgr()->InvalidateUsingGivenPdbId(pdb->GetPdbId());
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Invalidate buffer in this node failed, pdbId: %u.", pdbId));
            return DSTORE_FAIL;
        }
    }

    // PDB_TODO:
    pdb->CleanTempFile();
    if (STORAGE_FUNC_FAIL(pdb->ClosePdb())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("Fail to close Pdb: %s.", pdbName));
        return DSTORE_FAIL;
    }
    /* Attention: how to release new thread? */
    return DSTORE_SUCC;
}

void StorageInstance::OpenOnePdb(PdbId pdbId, UNUSE_PARAM bool withSingleMode)
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    /* we only open one database(templateDb) when startup instance. */
    StorageReleasePanic(m_pdb[StoragePdb::GetPdbIndex(pdbId)] == nullptr, MODULE_FRAMEWORK,
        ErrMsg("pdb %u is nullptr.", pdbId));

    if (pdbId == PDB_ROOT_ID) {
        errno_t rc = strncpy_s(m_pdb[StoragePdb::GetPdbIndex(pdbId)]->m_pdbName,
                               PDB_NAME_LEN, PDB_ROOT_NAME, strlen(PDB_ROOT_NAME));
        storage_securec_check(rc, "\0", "\0");
    }
    StorageReleasePanic(m_pdb[StoragePdb::GetPdbIndex(pdbId)]->IsInit(), MODULE_FRAMEWORK,
                            ErrMsg("pdb has been init already"));
    PdbId lastPdbId = thrd->GetXactPdbId();
    /* When enabling the PDB, ensure that the value of m_pdbid on the thread is the PDB to be enabled. */
    thrd->SetXactPdbId(pdbId);
    m_pdb[StoragePdb::GetPdbIndex(pdbId)]->InitPdb(true);
    if (g_defaultPdbId == PDB_ROOT_ID && IsTemplate(pdbId)) {
        /* open template for createpdb */
        thrd->SetXactPdbId(lastPdbId);
        return;
    }
    thrd->SetXactPdbId(pdbId);
    m_pdb[StoragePdb::GetPdbIndex(pdbId)]->CleanTempFile(true);
    StorageInstance::StartBgThreads();
}

void StorageInstance::UpdateCsnMinThreadMain()
{
    InitSignalMask();
    RetStatus ret = CreateThreadAndRegister(INVALID_PDB_ID, false, "CSNMinUpdator", true,
                                            ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_FRAMEWORK, ErrMsg("csn min thread register fail."));

    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("CSNMinUpdator start."));
    UpdateCsnMinThreadLoop();
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("CSNMinUpdator stop."));

    UnregisterThread();
}

void StorageInstance::CloseAllPdb(bool isSingleNode)
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    m_isInstanceShutdown = true;
    for (PdbId pdbId = PDB_START_ID; pdbId <= PDB_MAX_ID; pdbId++) {
        ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("[DS_SHUTDOWN]start to close pdb %hhu.", pdbId));
        PdbId index = static_cast<PdbId>(pdbId - PDB_START_ID);
        if (m_pdb[index] == nullptr || !m_pdb[index]->IsInit()) {
            continue;
        }
        if (isSingleNode) {
            m_pdb[index]->CleanTempFile();
        }
        m_pdb[index]->ClosePdb(isSingleNode);
    }
    (void)::ExitVfsModule();
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DS_SHUTDOWN]close all pdb success!"));
}

void StorageInstance::InitAllPdb()
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    /* we only open one database(templateDb) when startup instance. */
    StorageReleasePanic(m_pdb[StoragePdb::GetPdbIndex(g_defaultPdbId)] == nullptr,
        MODULE_FRAMEWORK, ErrMsg("Root pdb is nullptr."));
    StorageReleasePanic(m_pdb[StoragePdb::GetPdbIndex(g_defaultPdbId)]->IsInit(),
        MODULE_FRAMEWORK, ErrMsg("Root pdb has been init already."));
    thrd->SetXactPdbId(g_defaultPdbId);
    m_pdb[StoragePdb::GetPdbIndex(g_defaultPdbId)]->Initialize(true);
    StorageInstance::StartBgThreads();
}

void StorageInstance::DestroyAllPdb()
{
    for (PdbId pdbId = PDB_START_ID; pdbId <= PDB_MAX_ID; pdbId++) {
        PdbId index = static_cast<PdbId>(pdbId - PDB_START_ID);
        if (m_pdb[index] == nullptr) {
            continue;
        }
        m_pdb[index]->Destroy();
        FreePdbObj(pdbId);
    }
}

char *StorageInstance::GetPdbPath(const char *dataDir, PdbId pdbId)
{
    int rc;
    char *path = static_cast<char *>(DstorePalloc0(MAXPGPATH));
    if (STORAGE_VAR_NULL(path)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc for pdb path."));
        return nullptr;
    }
    if (STORAGE_VAR_NULL(g_storageInstance->GetGuc()->tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("guc tenantConfig is nullptr."));
        DstorePfreeExt(path);
        return nullptr;
    }
    if (g_storageInstance->GetGuc()->tenantConfig->storageConfig.type == StorageType::PAGESTORE) {
        rc = sprintf_s(path, MAXPGPATH, "%s", dataDir);
    } else {
        StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
        if (STORAGE_VAR_NULL(pdb)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("pdb is nullptr."));
            DstorePfreeExt(path);
            return nullptr;
        }
        if (pdb->GetVFSName() != nullptr) {
            const char *newVfsName = pdb->GetVFSName();
            rc = sprintf_s(path, MAXPGPATH, "%s/%s/%s", dataDir, BASE_DIR, newVfsName);
        } else {
            char newVfsName[MAX_CONFIG_NAME_LENGTH] = {};
            StoragePdb::GenerateVfsName(pdbId, newVfsName, MAX_CONFIG_NAME_LENGTH);
            rc = sprintf_s(path, MAXPGPATH, "%s/%s/%s", dataDir, BASE_DIR, newVfsName);
        }
    }
    storage_securec_check_ss(rc);
    return path;
}

char *StorageInstance::GetPdbWalPath(const char *dataDir, PdbId pdbId)
{
    (void)dataDir;
    (void)pdbId;
    int rc;
    char *path = static_cast<char *>(DstorePalloc0(MAXPGPATH));
    if (STORAGE_VAR_NULL(path)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc for wal path."));
        return nullptr;
    }
    rc = sprintf_s(path, MAXPGPATH, "wal");
    storage_securec_check_ss(rc);
    return path;
}

TablespaceMgr *StorageInstance::GetTablespaceMgrByPdbId(PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to get pdb, pdbId %u.", pdbId));
        return nullptr;
    }

    return pdb->GetTablespaceMgr();
}

RetStatus StorageInstance::ThreadCoreMgrInit()
{
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]thread core mgr init start, ncores %d, numaNodeNum %d.",
        m_guc->ncores, m_guc->numaNodeNum));
    RetStatus ret = m_thrdCoreMgr.Init(m_guc->ncores, static_cast<uint32>(m_guc->numaNodeNum));
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]thread core mgr init fail!"));
        return ret;
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]thread core mgr init success!"));
    return ret;
}

RetStatus StorageInstance::GucInit(StorageGUC *guc)
{
    if (m_guc != nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("m_guc has been init already."));
        return DSTORE_FAIL;
    }
    if (guc->tenantConfig == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("guc tenantConfig is nullptr."));
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    m_guc = static_cast<StorageGUC *>(DstorePalloc0(sizeof(StorageGUC)));
    if (STORAGE_VAR_NULL(m_guc)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc for guc."));
        storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    *m_guc = *guc;
    m_guc->dataDir = (char *)DstorePalloc0(MAXPGPATH);
    if (STORAGE_VAR_NULL(m_guc->dataDir)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc for guc data dir."));
        DstorePfreeExt(m_guc);
        storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    int rc = memcpy_s(m_guc->dataDir, MAXPGPATH, guc->dataDir, strlen(guc->dataDir));
    storage_securec_check(rc, "\0", "\0");
    m_guc->moduleLoggingConfigure = static_cast<char *>(DstorePalloc0(DSTORE_MODULE_BITMAP_SIZE));
    if (STORAGE_VAR_NULL(m_guc->moduleLoggingConfigure)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc for guc module logging config."));
        DstorePfreeExt(m_guc->dataDir);
        DstorePfreeExt(m_guc);
        storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    m_guc->tenantConfig = static_cast<TenantConfig*>(DstorePalloc0(sizeof(TenantConfig)));
    if (STORAGE_VAR_NULL(m_guc->tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc for guc tenant config."));
        DstorePfreeExt(m_guc->moduleLoggingConfigure);
        DstorePfreeExt(m_guc->dataDir);
        DstorePfreeExt(m_guc);
        storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    rc = memcpy_s(m_guc->tenantConfig, sizeof(TenantConfig), guc->tenantConfig, sizeof(TenantConfig));
    storage_securec_check(rc, "\0", "\0");
    /* TODO: turn off for default */
    ModuleLoggingInit(true);
    return DSTORE_SUCC;
}

void StorageInstance::GetCurrentComputeNodes(NodeId **nodeIdArr, uint32_t &nodeCnt)
{
    nodeCnt = 1;
    {
        AutoMemCxtSwitch autoSwitch{m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)};
        *nodeIdArr = static_cast<NodeId *>(DstorePalloc0(sizeof(NodeId) * nodeCnt));
    }
    if (unlikely(*nodeIdArr == nullptr)) {
        nodeCnt = 0;
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Allocate memory for GetCurrentComputeNodes error"));
        return;
    }
    (*nodeIdArr)[0] = 1;
}

void StorageInstance::GucDestroy()
{
    if (likely(m_guc != nullptr)) {
        DstorePfreeExt(m_guc->moduleLoggingConfigure);
        DstorePfreeExt(m_guc->dataDir);
        if (m_guc->tenantConfig) {
            DstorePfreeExt(m_guc->tenantConfig);
        }
        DstorePfreeExt(m_guc);
        m_guc = nullptr;
    }
}

RetStatus StorageInstance::InitAllLockMgrs()
{
    RetStatus ret = InitLockMgr();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]init lock mgr fail."));
        return ret;
    }

    ret = InitXactLockMgr();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]init xact lock mgr fail."));
        return ret;
    }

    ret = InitTableLockMgr();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]init table lock mgr fail."));
        return ret;
    }

    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]init all lock mgr success!"));
    return DSTORE_SUCC;
}

void StorageInstance::InitPdbLwLocks()
{
    InitVfsHandleLock();
    InitVfsClientHandleLock();
    for (uint32_t i = 0; i < MAX_PDB_COUNT; i++) {
        LWLockInitialize(&m_pdbLwLock[i], LWLOCK_GROUP_INIT_PDB);
    }
    for (uint32_t i = 0; i < MAX_PDB_COUNT; i++) {
        LWLockInitialize(&m_pdbInitLock[i], LWLOCK_GROUP_INIT_PDB);
    }
    for (uint32_t i = 0; i < MAX_PDB_COUNT; i++) {
        LWLockInitialize(&m_pdbStatusLock[i], LWLOCK_GROUP_INIT_PDB);
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]init all lwlocks success!"));
}

RetStatus StorageInstance::InitPdbSlots()
{
    for (PdbId pdbId = PDB_START_ID; pdbId <= PDB_MAX_ID; ++pdbId) {
        StoragePdb *pdb = CreatePdbObj(pdbId);
        if (STORAGE_VAR_NULL(pdb)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("create pdb object for pdb %u fail.", pdbId));
            storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

CsnMgr *StorageInstance::GetCsnMgr()
{
    return m_csnMgr;
}

RetStatus StorageInstance::InitCsnMgr()
{
    if (m_csnMgr == nullptr) {
        m_csnMgr = DstoreNew(g_dstoreCurrentMemoryContext) CsnMgr(g_storageInstance->GetThreadCoreMgr(),
            g_storageInstance->GetPdb(g_defaultPdbId)->GetControlFile(),
            g_storageInstance->GetGuc()->csnAssignmentIncrement);
        if (STORAGE_VAR_NULL(m_csnMgr)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]failed to alloc memory for csn mgr."));
            storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
        RetStatus ret = m_csnMgr->Initialize();
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]init csn mgr fail."));
            delete m_csnMgr;
            m_csnMgr = nullptr;
            return ret;
        }
    }
    return DSTORE_SUCC;
}

void StorageInstance::DestroyCsnMgr()
{
    if (likely(m_csnMgr != nullptr)) {
        m_csnMgr->Destroy();
        delete m_csnMgr;
    }
    m_csnMgr = nullptr;
}

RetStatus StorageInstance::InitPerfCounter()
{
    if (!PerfCatalog::GetInstance().Init()) {
        ErrLog(DSTORE_ERROR,
               MODULE_FRAMEWORK, ErrMsg("the pagestore server init perfcatalog failed"));
        return DSTORE_FAIL;
    }
    if (!PerfCounterScheduler::GetInstance().Init()) {
        ErrLog(DSTORE_ERROR,
               MODULE_FRAMEWORK, ErrMsg("the pagestore server init perfcatalog's scheduler failed"));
        return DSTORE_FAIL;
    }

    uint32 perfSchedulerInterval = 1;
    if (PerfCounterScheduler::GetInstance().CreateTask(
        PerfCatalog::RunDump, perfSchedulerInterval * static_cast<uint32>(MILLISECONDS_ONE_SECOND)) == 0) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to create perfcatalog's scheduler task"));
        return DSTORE_FAIL;
    }

    (void)PerfGlobalLevel::GetInstance().SetPerfLevel(g_storageInstance->GetGuc()->perfCounterLevel);

    if (!TransactionPerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init transaction perf unit"));
        return DSTORE_FAIL;
    }
    if (!WalPerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init wal perf unit"));
        return DSTORE_FAIL;
    }
    if (!BufPerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init buffer perf unit"));
        return DSTORE_FAIL;
    }
    BufPerfUnit::GetInstance().CreateMsgPerfItems("BufRpcMessageType",
        static_cast<uint8>(BUF_RPC_REQUEST_MSG_MAX_COUNT));
    if (!RecoveryPerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init recovery perf unit"));
        return DSTORE_FAIL;
    }
    RecoveryPerfUnit::GetInstance().CreateMsgPerfItems("RecoveryRpcMessageType",
        static_cast<uint8>(RPC_RECOVERY_MSG_MAX));
    if (!FrameworkPerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init framework perf unit"));
        return DSTORE_FAIL;
    }
    if (!CatalogPerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init framework perf unit"));
        return DSTORE_FAIL;
    }
    CatalogPerfUnit::GetInstance().CreateMsgPerfItems("MetaDataMsgType",
        static_cast<uint8>(MetaDataMsgType::METADATAINVALIDATOR_RPC_TABLESPACE_CACHE) + 1);
    if (!PdbReplicaPerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init framework perf unit"));
        return DSTORE_FAIL;
    }
    if (!ControlPerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init framework perf unit"));
        return DSTORE_FAIL;
    }
    ControlPerfUnit::GetInstance().CreateMsgPerfItems("CtrlFileMessageType",
                                                      static_cast<uint8>(CTRL_FILE_REQUEST_PDB_SYNC_MODE_RELOAD) + 1);
    if (!LockPerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init framework perf unit"));
        return DSTORE_FAIL;
    }
    LockPerfUnit::GetInstance().CreateMsgPerfItems("LockMessageType",
        static_cast<uint8>(LOCK_MSG_MAX));
    if (!TableSpacePerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init table space perf unit"));
        return DSTORE_FAIL;
    }
    if (!HeapPerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init heap perf unit"));
        return DSTORE_FAIL;
    }
    if (!BtreePerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init btree perf unit"));
        return DSTORE_FAIL;
    }
    if (!SystablePerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init systable perf unit"));
        return DSTORE_FAIL;
    }
    if (!ExternalPerfUnit::GetInstance().Init(g_dstoreCurrentMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init perf counter unit"));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void StorageInstance::DestroyPerfCounter()
{
    PerfCounterScheduler::GetInstance().Destroy();
    (void)TableSpacePerfUnit::GetInstance().Destroy();
    (void)LockPerfUnit::GetInstance().Destroy();
    (void)ControlPerfUnit::GetInstance().Destroy();
    (void)PdbReplicaPerfUnit::GetInstance().Destroy();
    (void)CatalogPerfUnit::GetInstance().Destroy();
    (void)FrameworkPerfUnit::GetInstance().Destroy();
    (void)RecoveryPerfUnit::GetInstance().Destroy();
    (void)BufPerfUnit::GetInstance().Destroy();
    (void)WalPerfUnit::GetInstance().Destroy();
    (void)TransactionPerfUnit::GetInstance().Destroy();
    (void)HeapPerfUnit::GetInstance().Destroy();
    (void)BtreePerfUnit::GetInstance().Destroy();
    (void)SystablePerfUnit::GetInstance().Destroy();
    (void)ExternalPerfUnit::GetInstance().Destroy();
    PerfCatalog::GetInstance().Destroy();
}

void StorageInstance::UpdateCsnMinThreadLoop()
{
    long csnMinUpdateInterval = g_storageInstance->GetGuc()->updateCsnMinInterval;
    while (!m_stopBgThread) {
        GaussUsleep(csnMinUpdateInterval);
        if (m_instanceState == static_cast<uint8>(InstanceState::ACTIVE)) {
            GetCsnMgr()->UpdateLocalCsnMin();
        }
    }
}

RetStatus StorageInstance::BufMgrInit()
{
    if (m_bufMgr != nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("m_bufMgr has been init already."));
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_BUFFER));
    /* lru size should be 1/2 thread number */
    m_bufMgr = DstoreNew(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_BUFFER))
        BufMgr(m_guc->buffer, m_guc->bufferLruPartition);
    if (STORAGE_VAR_NULL(m_bufMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to alloc memory for buffer mgr."));
        storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_bufMgr->Init())) {
        delete m_bufMgr;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void StorageInstance::BufMgrDestroy()
{
    if (likely(m_bufMgr != nullptr)) {
        m_bufMgr->Destroy();
        delete m_bufMgr;
        m_bufMgr = nullptr;
    }
}

RetStatus StorageInstance::InitLockMgr()
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));
    m_lockMgr = DstoreNew(g_dstoreCurrentMemoryContext) LockMgr();
    if (STORAGE_VAR_NULL(m_lockMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to alloc memory for lock mgr."));
        storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    uint32 hashTableSize = g_storageInstance->GetGuc()->lockHashTableSize;
    uint32 partitionNum = g_storageInstance->GetGuc()->lockTablePartitionNum;
    RetStatus ret = m_lockMgr->Initialize(hashTableSize, partitionNum);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to initialize lock mgr."));
        delete m_lockMgr;
        m_lockMgr = nullptr;
        return DSTORE_FAIL;
    }
    return ret;
}

void StorageInstance::DestroyLockMgr()
{
    if (likely(m_lockMgr != nullptr)) {
        m_lockMgr->Destroy();
        delete m_lockMgr;
        m_lockMgr = nullptr;
    }
}

RetStatus StorageInstance::InitTableLockMgr()
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));
    m_tableLockMgr = DstoreNew(g_dstoreCurrentMemoryContext) TableLockMgr();
    if (STORAGE_VAR_NULL(m_tableLockMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to alloc memory for table lock mgr."));
        storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    uint32 hashTableSize = g_storageInstance->GetGuc()->lockHashTableSize;
    uint32 partitionNum = g_storageInstance->GetGuc()->lockTablePartitionNum;
    RetStatus ret = m_tableLockMgr->Initialize(hashTableSize, partitionNum);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to initialize table lock mgr."));
        delete m_tableLockMgr;
        m_tableLockMgr = nullptr;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void StorageInstance::DestroyTableLockMgr()
{
    if (likely(m_tableLockMgr != nullptr)) {
        m_tableLockMgr->Destroy();
        delete m_tableLockMgr;
        m_tableLockMgr = nullptr;
    }
}

RetStatus StorageInstance::InitXactLockMgr()
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));
    m_xactLockMgr = DstoreNew(g_dstoreCurrentMemoryContext) XactLockMgr();
    if (STORAGE_VAR_NULL(m_xactLockMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to alloc memory for xact lock mgr."));
        storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    uint32 hashTableSize = g_storageInstance->GetGuc()->lockHashTableSize;
    uint32 partitionNum = g_storageInstance->GetGuc()->lockTablePartitionNum;
    RetStatus ret = m_xactLockMgr->Initialize(hashTableSize, partitionNum);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to initialize xact lock mgr."));
        delete m_xactLockMgr;
        m_xactLockMgr = nullptr;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void StorageInstance::DestroyXactLockMgr()
{
    if (likely(m_xactLockMgr != nullptr)) {
        m_xactLockMgr->Destroy();
        delete m_xactLockMgr;
        m_xactLockMgr = nullptr;
    }
}

void StorageInstance::DestroyCpuRes()
{
    if (likely(m_threadCpuAutoBinder != nullptr)) {
        m_threadCpuAutoBinder->Destroy();
        delete m_threadCpuAutoBinder;
        m_threadCpuAutoBinder = nullptr;
    }
}

LockMgr *StorageInstance::GetLockMgr()
{
    return m_lockMgr;
}

TableLockMgr *StorageInstance::GetTableLockMgr()
{
    return m_tableLockMgr;
}

XactLockMgr *StorageInstance::GetXactLockMgr()
{
    return m_xactLockMgr;
}

StorageStat *StorageInstance::GetStat()
{
    return &m_stat;
}

void StorageInstance::ResourcesCleanUp()
{
    DestroyCpuRes();
    m_thrdCoreMgr.Destroy();
    TypecacheMgrDestroy();
    GucDestroy();
    if (likely(m_memoryMgr != nullptr)) {
        StorageMemoryMgr::destroy(m_memoryMgr);
        m_memoryMgr = nullptr;
    }
}

RetStatus StorageInstance::TypecacheMgrInit(IndexGetFuncCb *indexCb)
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    if (m_cacheHashMgr != nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("m_cacheHashMgr must be nullptr before init."));
        return DSTORE_FAIL;
    }
    m_cacheHashMgr = DstoreNew(g_dstoreCurrentMemoryContext) CacheHashManager();
    if (STORAGE_VAR_NULL(m_cacheHashMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to alloc memory for cache hash mgr."));
        storage_set_error(FRAMEWORK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    m_cacheHashMgr->Init(g_dstoreCurrentMemoryContext);
    m_cacheHashMgr->CacheGetFuncCb(indexCb);
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DS_INIT]typecache mgr init success!"));
    return DSTORE_SUCC;
}

void StorageInstance::TypecacheMgrDestroy()
{
    if (likely(m_cacheHashMgr != nullptr)) {
        m_cacheHashMgr->Destory();
        delete m_cacheHashMgr;
        m_cacheHashMgr = nullptr;
    }
}

RetStatus StorageInstance::CreateThreadAndRegister(PdbId pdbId, bool bootstrap, const char *threadName,
                                                   bool needCommBuffer, ThreadMemoryLevel level)
{
#ifdef ENABLE_LCOV
    __gcov_flush();
#endif
    /* Thread name len must < 16 for pthread lib. */
    if (threadName != nullptr && strlen(threadName) >= THREAD_NAME_MAX_LENGTH) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
               ErrMsg("threadName %s is longger than THREAD_NAME_MAX_LENGTH.", threadName));
        return DSTORE_FAIL;
    }
    LatencyStat::Timer timer(&FrameworkPerfUnit::GetInstance().m_createThreadAndRegister);
    if (thrd != nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_COMMON,
               ErrMsg("ThreadId %lu [%s] already exists!! start time is [%ld], will free it and restart a new thrd.",
                      thrd->threadCore.core->pid, thrd->threadCore.core->threadName, thrd->threadCore.core->startTime));
        g_storageInstance->RemoveVisibleThread(thrd);
        DSTORE::ThreadContextInterface::DestroyCurrentThreadContext();
    }
    (void)ThreadContextInterface::Create();
    if (STORAGE_VAR_NULL(thrd)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to create thread context for thread %s", threadName));
        return DSTORE_FAIL;
    }
    thrd->SetThreadMemLevel(level);
    if (STORAGE_FUNC_FAIL(thrd->InitializeBasic())) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to initialize basic thread context."));
        ThreadContextInterface::DestroyCurrentThreadContext();
        return DSTORE_FAIL;
    }
    if (!bootstrap) {
        if (STORAGE_FUNC_FAIL(thrd->InitStorageContext(pdbId))) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to initialize sotrage context."));
            ThreadContextInterface::DestroyCurrentThreadContext();
            return DSTORE_FAIL;
        }
        thrd->SetNeedCommBuffer(needCommBuffer);
        this->AddVisibleThread(thrd, pdbId, threadName);
    }
    static_cast<void>(pthread_setname_np(pthread_self(), threadName));
    return DSTORE_SUCC;
}

void StorageInstance::UnregisterThread(bool bootstrap)
{
#ifdef ENABLE_LCOV
    __gcov_flush();
#endif
    if (thrd != nullptr) {
        if (!bootstrap) {
            this->RemoveVisibleThread(thrd);
        }
        ThreadContextInterface::DestroyCurrentThreadContext();
    } else {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("[thread] thread is null and still try to unregister."));
    }
}

constexpr uint32 SLEEP_MICROSECONDS = 100;
void StorageInstance::AddVisibleThread(ThreadContextInterface *thread, const PdbId pdbId, const char *threadName,
                                       volatile uint32_t *InterruptHoldoffCount)
{
    if (threadName != nullptr && strlen(threadName) >= THREAD_NAME_MAX_LENGTH) {
        StorageReleasePanic(true, MODULE_FRAMEWORK,
                            ErrMsg("threadName is more than THREAD_NAME_MAX_LENGTH"));
    }
    if (threadName != nullptr) {
        (void)pthread_setname_np(pthread_self(), threadName);
    }

    ThreadContext *threadContext = static_cast<ThreadContext *>(thread);
    while (STORAGE_FUNC_FAIL(
        m_thrdCoreMgr.ThreadBindCoreSlot(pdbId, threadContext, threadContext->threadCore, InterruptHoldoffCount))) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("Waiting for thread bind core slot, Pdb id is: %d",
            pdbId));
        GaussUsleep(SLEEP_MICROSECONDS);
    }
    threadContext->threadCore.core->SetThreadName(threadName);
    m_thrdCoreMgr.InsertSortArray(threadContext->threadCore);
    threadContext->threadCore.core->SetStartTime(GetCurrentTimestamp());
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
           ErrMsg("[thread] thread %s:%lu added at %lu.", threadContext->threadCore.core->threadName,
                  threadContext->threadCore.core->pid, threadContext->threadCore.core->startTime));
}

void StorageInstance::RemoveVisibleThread(ThreadContextInterface *thread)
{
    ThreadContext *threadContext = static_cast<ThreadContext *>(thread);
    if (!STORAGE_VAR_NULL(threadContext->threadCore.core)) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
            ErrMsg("[thread] thread %s:%lu removed, started at %lu.", threadContext->threadCore.core->threadName,
                    threadContext->threadCore.core->pid, threadContext->threadCore.core->startTime));
    }
    CheckLwLockLeak();
    if (unlikely(((static_cast<ThreadContext *>(thread))->threadCore).core == nullptr)) {
        return;
    }
    m_thrdCoreMgr.RemoveSortArray(((static_cast<ThreadContext *>(thread))->threadCore));
    m_thrdCoreMgr.ThreadUnbindCoreSlot((static_cast<ThreadContext *>(thread))->threadCore);
}

MemOutputParam *StorageInstance::MemnodeRemoteCall(UNUSE_PARAM NodeId nodeId, UNUSE_PARAM MemInputParam *param)
{
    StringInfoData string;
    if (unlikely(!string.init())) {
        return nullptr;
    }
    if (STORAGE_FUNC_FAIL(string.append("Not support at single node mode.\n"))) {
        DstorePfreeExt(string.data);
        return nullptr;
    }

    MemOutputParam *output = static_cast<MemOutputParam *>(DstorePalloc(sizeof(MemOutputParam)));
    if (STORAGE_VAR_NULL(output)) {
        DstorePfreeExt(string.data);
        return nullptr;
    }
    output->error = -1;
    output->errInfo = string.data;
    return output;
}

RetStatus StorageInstance::UpdateAidClusterInfo(const AidClusterInfo *aidClusterInfo)
{
    (void)aidClusterInfo;
    ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("aidClusterInfo not used in single-node setup."));
    return DSTORE_FAIL;
}

void StorageInstance::GetAidClusterInfo(AidClusterInfo *&aidClusterInfo)
{
    (void)aidClusterInfo;
    ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("aidClusterInfo not used in single-node setup."));
}

void StorageInstance::RegisterCrmmRouteInfoCallback(CrmmRouteUpdateCallback callback)
{
    (void)callback;
    ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("RegisterCrmmRouteInfoCallback not used in single-node setup."));
}

void StorageInstance::RegisterCrossClusterCommCallback(CrossClusterCommCallback callback)
{
    (void)callback;
    ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("RegisterCrossClusterCommCallback not used in single-node setup."));
}

void StorageInstance::RegisterWaitForAllSessionDisconnectCallback(WaitForAllSessionDisconnectCallback callback)
{
    (void)callback;
    ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK,
           ErrMsg("RegisterWaitForAllSessionDisconnectCallback not used in single-node setup."));
}

void StorageInstance::RegisterWaitForAllSQLThreadStopCallback(WaitForAllSQLThreadStopCallback callback)
{
    (void)callback;
    ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK,
           ErrMsg("RegisterWaitForAllSQLThreadStopCallback not used in single-node setup."));
}

RetStatus StorageInstance::GetClusterRecoveryStatus(ClusterRecoveryStatus &status)
{
    status = ClusterRecoveryStatus::CRS_INVALID;
    ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK,
           ErrMsg("GetClusterRecoveryStatus not used in single-node setup."));
    return DSTORE_FAIL;
}

void StorageInstance::Wakeup(ThreadId threadId)
{
    ThreadCoreMgr::ThreadIterator iter(&m_thrdCoreMgr);
    ThreadCore *core = nullptr;
    while ((core = iter.GetNextThreadCore()) != nullptr) {
        if (core->pid == threadId) {
            core->Wakeup();
        }
    }
}

RetStatus StorageInstance::SetPdbToSlot(StoragePdb *newPdb)
{
    PdbId pdbId = newPdb->GetPdbId();
    if (pdbId <= INVALID_PDB_ID || pdbId > PDB_MAX_ID) {
        return DSTORE_FAIL;
    }
    m_pdb[StoragePdb::GetPdbIndex(pdbId)] = newPdb;
    return DSTORE_SUCC;
}

void StorageInstance::StartBgThreads()
{
#ifndef UT
    m_updateCsnMinThread = new (std::nothrow) std::thread(&StorageInstance::UpdateCsnMinThreadMain, this);
    if (STORAGE_VAR_NULL(m_updateCsnMinThread)) {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("Failed to start csn min thread."));
    }
#endif
}

void StorageInstance::StopBgThreads()
{
    m_stopBgThread = true;
    if (m_updateCsnMinThread != nullptr) {
        m_updateCsnMinThread->join();
        delete m_updateCsnMinThread;
        m_updateCsnMinThread = nullptr;
    }
}

bool StorageInstance::IsStoppingBgThreads() const
{
    return m_stopBgThread;
}

RetStatus StorageInstance::CopyVfsAndFiles(StoragePdb *newPdb, PdbInfo *pdbInfo,
                                           ControlPdbInfoPageItemData *pdbInfoPageItemData, const char *existVfsName)
{
    if (STORAGE_VAR_NULL(newPdb)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("newPdb is null."));
        return DSTORE_FAIL;
    }

    if (STORAGE_VAR_NULL(pdbInfo)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("pdbInfo is null."));
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));

    if (pdbInfo->pdbRoleMode != PdbRoleMode::PDB_STANDBY) {
        /* Open template pdb before copy files from it. */
        if (!IsTemplate(pdbInfo->templateId)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("pdbInfo is not belong to templatePdb."));
            return DSTORE_FAIL;
        }
        StoragePdb *templatePdb = GetPdb(pdbInfo->templateId);
        if (STORAGE_VAR_NULL(templatePdb)) {
            OpenOnePdb(pdbInfo->templateId, true);
            templatePdb = GetPdb(pdbInfo->templateId);
            if (STORAGE_VAR_NULL(templatePdb)) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("templatePdb is nullptr."));
                return DSTORE_FAIL;
            }
        }
        if (IsTemplate(g_defaultPdbId)) {
            templatePdb->TryMarkSelfWalStreamDropping();
        }

        if (existVfsName != nullptr) {
            newPdb->SetVFSName(existVfsName);
        }

        if (existVfsName == nullptr) {
            if (STORAGE_FUNC_FAIL(newPdb->CopyFiles(templatePdb))) {
                return DSTORE_FAIL;
            }
        }
    }

    PdbId newPdbId = newPdb->GetPdbId();
    UuidGenerate(newPdb->GetPdbUuid());
    const char *vfsName = newPdb->GetVFSName();
    char newPdbVfsName[MAX_CONFIG_NAME_LENGTH] = {0 };
    error_t rc = memcpy_s(newPdbVfsName, MAX_CONFIG_NAME_LENGTH, vfsName, strlen(vfsName));
    storage_securec_check(rc, "\0", "\0");

    /* Build control file pdb page and set PDB_STATUS_CREATING status. */
    RetStatus ret = ControlPdbInfoPageItemData::Init(pdbInfoPageItemData, PDB_TEMPLATE1_ID, PDB_DEFAULT_UUID,
                                                     PdbStatus::PDB_STATUS_UNCREATED, PDB_TEMPLATE1_NAME, "");
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }
    pdbInfoPageItemData->pdbId = newPdbId;
    rc = memcpy_s(pdbInfoPageItemData->pdbUuid, FORMATTED_UUID_ARR_LEN, newPdb->GetPdbUuid(), FORMATTED_UUID_ARR_LEN);
    storage_securec_check(rc, "\0", "\0");
    pdbInfoPageItemData->pdbStatus = PdbStatus::PDB_STATUS_CREATING;
    rc = memcpy_s(pdbInfoPageItemData->vfsName, MAX_CONFIG_NAME_LENGTH, newPdbVfsName, MAX_CONFIG_NAME_LENGTH);
    storage_securec_check(rc, "\0", "\0");
    if (pdbInfo != nullptr) {
        rc = memset_s(pdbInfoPageItemData->pdbName, DB_NAME_LEN, 0, DB_NAME_LEN);
        storage_securec_check(rc, "\0", "\0");
        rc = memcpy_s(pdbInfoPageItemData->pdbName, DB_NAME_LEN, pdbInfo->pdbName, strlen(pdbInfo->pdbName));
        storage_securec_check(rc, "\0", "\0");
        pdbInfoPageItemData->pdbRoleMode = pdbInfo->pdbRoleMode;
        pdbInfoPageItemData->pdbReplicaStatus = PdbReplicaRebuildProgress::NO_REPLICA_REBUILD;
        pdbInfoPageItemData->pdbSwitchStatus = PdbSwitchStatus::NORMAL;
        pdbInfoPageItemData->grpRespoolOid = pdbInfo->grpRespoolOid;
        pdbInfoPageItemData->dbaId = pdbInfo->dbaId;
    }

    if (existVfsName != nullptr) {
        pdbInfoPageItemData->isInRestoreFromBackup = true;
    }

    ret = DSTORE_SUCC;
    if (newPdbId != PDB_ROOT_ID) {
        StoragePdb *rootPdb = GetPdb(g_defaultPdbId);
        if (STORAGE_VAR_NULL(rootPdb) || STORAGE_VAR_NULL(rootPdb->m_controlFile)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("rootPdb or controlFile is null."));
            return DSTORE_FAIL;
        }
        if (LockInterface::LockPdb(newPdbId, DSTORE_ACCESS_EXCLUSIVE_LOCK) != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
        ret = rootPdb->m_controlFile->UpdatePdbItemData(newPdbId, pdbInfoPageItemData,
            sizeof(ControlPdbInfoPageItemData));
        LockInterface::UnlockPdb(newPdbId, DSTORE_ACCESS_EXCLUSIVE_LOCK);
    }
    return ret;
}

RetStatus StorageInstance::CreatePdbReplica(UNUSE_PARAM StoragePdb *newPdb)
{
    return DSTORE_FAIL;
}

StoragePdb *StorageInstance::GetPdbByName(const char *pdbName)
{
    for (PdbId pdbId = 0; pdbId < MAX_PDB_COUNT; ++pdbId) {
        if (m_pdb[pdbId] == nullptr) {
            continue;
        }
        if (strcmp(m_pdb[pdbId]->m_pdbName, pdbName) == 0) {
            return m_pdb[pdbId];
        }
    }
    return nullptr;
}

void StorageInstance::StoreMaxAppendPlsnOfPdbs(PdbId pdbId, PdbsPlsnRecords &plsnRecords)
{
    /* pdbId == INVALID_PDB_ID' means record all PDBs */
    if (unlikely(pdbId != INVALID_PDB_ID && !StoragePdb::IsValidPdbId(pdbId))) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("Record Pdb max append plsn failed, invalid PdbId:%hhu.", pdbId));
        return;
    }

    for (PdbId id = PDB_START_ID; id <= PDB_MAX_ID; ++id) {
        if (pdbId == INVALID_PDB_ID || id == pdbId) {
            StoragePdb *pdb = GetPdb(id);
            StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("pdb is nullptr."));
            if (pdb->IsInit()) {
                pdb->StoreMaxAppendPlsnOfWalStreams(plsnRecords);
            }
            if (id == pdbId) {
                break;
            }
        }
    }
}

RetStatus StorageInstance::CreateCheckpointForPdbs(const PdbsPlsnRecords &plsnRecords)
{
    RetStatus ret = DSTORE_SUCC;
    for (PdbId pdbId = PDB_START_ID; pdbId <= PDB_MAX_ID; ++pdbId) {
        if (STORAGE_FUNC_FAIL(g_storageInstance->LockAndRefreshPdb(pdbId))) {
            StorageReleasePanic(true, MODULE_PDB, ErrMsg("Refresh pdb status failed when create checkpoint."));
        }
        StoragePdb *pdb = GetPdb(pdbId);
        StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("pdb is nullptr."));
        if (!pdb->IsInit()) {
            g_storageInstance->UnlockAfterRefreshPdb(pdbId);
            continue;
        }
        if (STORAGE_FUNC_FAIL(pdb->CreateCheckpoint(plsnRecords))) {
            ret = DSTORE_FAIL;
        }
        g_storageInstance->UnlockAfterRefreshPdb(pdbId);
    }
    return ret;
}

void StorageInstance::SwitchWalWriterContextToTargetPdb(PdbId pdbId, bool destroyWalContext,
                                                        AtomicWalWriterContext *walWriterContext)
{
    thrd->SetXactPdbId(pdbId);
    if (destroyWalContext) {
        delete thrd->m_walWriterContext;
        thrd->m_walWriterContext = walWriterContext;
    }

    if (STORAGE_VAR_NULL(walWriterContext)) {
        thrd->m_walWriterContext = nullptr;
        (void)thrd->InitStorageContext(pdbId);
    }
}

RetStatus StorageInstance::SwitchContextToTargetPdb(PdbId pdbId, bool destroyWalContext,
                                                    AtomicWalWriterContext *walWriterContext)
{
    Transaction *curTransaction = thrd->GetActiveTransaction();
    StorageReleasePanic(curTransaction == nullptr, MODULE_FRAMEWORK, ErrMsg("transaction should not be null."));
    bool inTransaction = curTransaction->InTransaction();
    bool inTransactionBlock = curTransaction->InTransactionBlock();
    if (inTransaction) {
        if (inTransactionBlock) {
            if (STORAGE_FUNC_FAIL(curTransaction->EndTransactionBlock())) {
                return DSTORE_FAIL;
            }
        }
        if (STORAGE_FUNC_FAIL(curTransaction->Commit())) {
            return DSTORE_FAIL;
        }
    }

    thrd->DestroyTransactionRuntime();
    thrd->SetXactPdbId(pdbId);
    if (destroyWalContext) {
        delete thrd->m_walWriterContext;
        thrd->m_walWriterContext = walWriterContext;
    }
    if (STORAGE_VAR_NULL(walWriterContext)) {
        thrd->m_walWriterContext = nullptr;
        if (STORAGE_FUNC_FAIL(thrd->InitStorageContext(pdbId))) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to initialize sotrage context."));
            return DSTORE_FAIL;
        }
    }
    /* Init tranaction may write undo recycle wal, we need init tranaction run time after set wal context pdbId. */
    if (STORAGE_FUNC_FAIL(thrd->InitTransactionRuntime(pdbId, nullptr, nullptr, true))) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to init transaction runtime."));
        return DSTORE_FAIL;
    }
    if (inTransaction) {
        curTransaction = thrd->GetActiveTransaction();
        if (STORAGE_FUNC_FAIL(curTransaction->Start())) {
            return DSTORE_FAIL;
        }
        if (inTransactionBlock) {
            if (STORAGE_FUNC_FAIL(curTransaction->BeginTransactionBlock())) {
                return DSTORE_FAIL;
            }
        }
    }
    return DSTORE_SUCC;
}

void StorageInstance::FreePdbObj(const PdbId pdbId)
{
    if (GetPdb(pdbId) == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("Free pdb with pdbId %d failed. Pdb does not exist in storage instance.", pdbId));
        return;
    }
    delete m_pdb[pdbId - PDB_START_ID];
    m_pdb[pdbId - PDB_START_ID] = nullptr;
}

void StorageInstance::UnLockPdbObj(const PdbId pdbId)
{
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("UnLockPdbObj %d.", pdbId));
    LWLockRelease(&m_pdbLwLock[StoragePdb::GetPdbIndex(pdbId)]);
}

void StorageInstance::UnLockPdbInit(const PdbId pdbId)
{
    LWLockRelease(&m_pdbInitLock[StoragePdb::GetPdbIndex(pdbId)]);
}

void StorageInstance::UnLockPdbStatus(const PdbId pdbId)
{
    LWLockRelease(&m_pdbStatusLock[StoragePdb::GetPdbIndex(pdbId)]);
}

RetStatus StorageInstance::DoDropPdbForce(StoragePdb *pdb, const char* vfsName)
{
    StoragePdb *rootPdb = GetPdb(g_defaultPdbId);
    if (STORAGE_VAR_NULL(rootPdb)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("rootPdb is nullptr."));
        return DSTORE_FAIL;
    }

    /* Drop VFS and others in old PDB */
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DROP PDB] Resetting PDB."));
    pdb->ResetPdb(vfsName, true);

    /* Free pdbId to rootPdb, and clear pdb info page item data */
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DROP PDB] Clearing info page item data."));
    if (pdb->GetPdbId() != g_defaultPdbId) {
        ControlPdbInfoPageItemData pdbInfoPageItemData;
        RetStatus ret = ControlPdbInfoPageItemData::Init(&pdbInfoPageItemData, pdb->GetPdbId(), PDB_DEFAULT_UUID,
                                                         PdbStatus::PDB_STATUS_UNCREATED, "", "");
        if (STORAGE_FUNC_SUCC(ret)) {
            if (LockInterface::LockPdb(pdb->GetPdbId(), DSTORE_ACCESS_EXCLUSIVE_LOCK) != DSTORE_SUCC) {
                return DSTORE_FAIL;
            }
            ret = rootPdb->m_controlFile->UpdatePdbItemData(pdb->GetPdbId(), &pdbInfoPageItemData,
                                                            sizeof(ControlPdbInfoPageItemData));
            LockInterface::UnlockPdb(pdb->GetPdbId(), DSTORE_ACCESS_EXCLUSIVE_LOCK);
        }
        if (STORAGE_FUNC_FAIL(ret)) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus StorageInstance::DoInnerlyOpenPdb(StoragePdb *newPdb, ControlPdbInfoPageItemData *pdbInfoPageItemData)
{
    AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));

    if (STORAGE_VAR_NULL(pdbInfoPageItemData)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DoInnerlyOpenPdb]pdb info page invalid"));
        return DSTORE_FAIL;
    }

    StoragePdb *pdb = GetPdb(pdbInfoPageItemData->pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DoInnerlyOpenPdb]pdb is nullptr"));
        return DSTORE_FAIL;
    }
    LockPdbObj<LW_EXCLUSIVE>(pdbInfoPageItemData->pdbId);
    if (!pdb->IsInit()) {
        newPdb = pdb;
        errno_t rc = memcpy_s(newPdb->m_pdbName, DB_NAME_LEN, pdbInfoPageItemData->pdbName, DB_NAME_LEN);
        storage_securec_check(rc, "\0", "\0");
        newPdb->SetPdbUuid(pdbInfoPageItemData->pdbUuid);
        newPdb->SetVFSName(pdbInfoPageItemData->vfsName);
        if (STORAGE_FUNC_FAIL(SetPdbToSlot(newPdb))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to set pdb to slot."));
            UnLockPdbObj(pdbInfoPageItemData->pdbId);
            return DSTORE_FAIL;
        }
    }
    thrd->SetXactPdbId(pdbInfoPageItemData->pdbId);

    if (STORAGE_FUNC_FAIL(newPdb->OpenPdb(static_cast<void *>(pdbInfoPageItemData)))) {
        UnLockPdbObj(pdbInfoPageItemData->pdbId);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("[DoInnerlyOpenPdb]Open pdb %s failed", pdbInfoPageItemData->pdbName));
        return DSTORE_FAIL;
    }
    UnLockPdbObj(pdbInfoPageItemData->pdbId);
#ifdef UT
    (void)thrd->InitTransactionRuntime(pdbInfoPageItemData->pdbId, nullptr, nullptr);
#endif
    return DSTORE_SUCC;
}

RetStatus StorageInstance::UpdateSysTable(PdbId currPdbId, PdbId targetPdbId, PdbInfo *pdbInfo)
{
    /* before update pg_database in new pdb, switch context from rootpdb to newPdb */
    AtomicWalWriterContext *oldWalContext = thrd->m_walWriterContext;
    if (STORAGE_VAR_NULL(oldWalContext)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("oldWalWriterContext is nullptr."));
        return DSTORE_FAIL;
    }
    thrd->SetXactPdbId(currPdbId);
    if (STORAGE_FUNC_FAIL(SwitchContextToTargetPdb(targetPdbId, false))) {
        return DSTORE_FAIL;
    }
    StoragePdb *newPdb = GetPdb(targetPdbId);
    if (newPdb == nullptr || !newPdb->IsInit()) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[CreatePdb]fetch new pdb from memory failed"));
        if (STORAGE_FUNC_FAIL(SwitchContextToTargetPdb(currPdbId, true, oldWalContext))) {
            StorageReleasePanic(true, MODULE_FRAMEWORK,
                ErrMsg("[CreatePdb]failed to switch to old context, current pdbId is %u, "
                "target pdbId is %u", targetPdbId, currPdbId));
        }
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(newPdb->UpdateDatabaseSysTable(pdbInfo->sysDatabaseTuple))) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[CreatePdb]Update database systable failed"));
        if (STORAGE_FUNC_FAIL(SwitchContextToTargetPdb(currPdbId, true, oldWalContext))) {
            StorageReleasePanic(true, MODULE_FRAMEWORK,
                ErrMsg("[CreatePdb]failed to switch to old context, current pdbId is %u, "
                "target pdbId is %u", targetPdbId, currPdbId));
        }
        return DSTORE_FAIL;
    }
    /* switch context from newPdb back to rootpdb */
    if (STORAGE_FUNC_FAIL(SwitchContextToTargetPdb(currPdbId, true, oldWalContext))) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void StorageInstance::RegisterReportWaitEventCallback(ReportWaitEventCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportWaitEventCallback(callback);
}

void StorageInstance::RegisterReportWaitEventFailedCallback(ReportWaitEventCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportWaitEventFailedCallback(callback);
}

void StorageInstance::RegisterReportWaitStatusCallback(ReportWaitStatusCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportWaitStatusCallback(callback);
}

bool StorageInstance::IsPdbRollbackEnd(PdbId pdbId)
{
    StoragePdb *pdb = GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb) || !pdb->IsInit() || pdb->IsRollbackEnd() == false) {
        return false;
    }

    return true;
}

bool StorageInstance::IsBackupRestoreRollbackToConsistencyPointEnd(PdbId pdbId)
{
    if (pdbId != INVALID_PDB_ID) {
        if (GetBackupConsistencyPoint(pdbId) != nullptr) {
            return false;
        }
        return true;
    }
    for (PdbId id = PDB_START_ID; id < PDB_MAX_ID; id++) {
        if (GetBackupConsistencyPoint(id) != nullptr) {
            return false;
        }
    }
    return true;
}

void StorageInstance::RegisterReportXactTimestampCallback(ReportXactTimestampCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportXactTimestampCallback(callback);
}

void StorageInstance::RegisterReportCurrentTopXidCallback(ReportCurrentTopXidCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportCurrentTopXidCallback(callback);
}

void StorageInstance::RegisterReportCurrentXidCallback(ReportCurrentXidCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportCurrentXidCallback(callback);
}

void StorageInstance::RegisterReportXlogLenCallback(ReportXlogLenCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportXlogLenCallback(callback);
}

void StorageInstance::RegisterReportXactInfoCallback(ReportXactInfoCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportXactInfoCallback(callback);
}

void StorageInstance::RegisterReportCountBufferReadCallback(ReportCountBufferReadCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportCountBufferReadCallback(callback);
}

void StorageInstance::RegisterReportCountBufferHitCallback(ReportCountBufferHitCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportCountBufferHitCallback(callback);
}

void StorageInstance::RegisterReportCountBufferCallback(ReportCountBufferCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportCountBufferCallback(callback);
}

void StorageInstance::RegisterReportLockStatCallback(ReportLockStatCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportLockStatCallback(callback);
}
 
void StorageInstance::RegisterReportLWLockStatCallback(ReportLWLockStatCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportLWLockStatCallback(callback);
}
void StorageInstance::RegisterDstoreInternalThreadCallback(ThreadInitCallback initCallback,
                                                           ThreadReleaseCallback releaseCallback)
{
    m_threadInitCallback = initCallback;
    m_threadReleaseCallback = releaseCallback;
}

void StorageInstance::BindAllThreads()
{
    return;
}

bool StorageInstance::InitSQLThreadContext(SQLThrdInitCtx *context)
{
    StorageAssert(context);
    if (context == nullptr) {
        return false;
    }
#ifdef UT
    return true;
#endif
    StorageAssert(m_threadInitCallback);
    if (m_threadInitCallback == nullptr) {
        return false;
    }
    return m_threadInitCallback(context);
}

/* !!!NOTE: the dstore thrd will be freed by DstorePfreeAligned,
 * so if create new thrd, must use dstoreAlloc[Aligned] instead of DstoreNew */
void StorageInstance::ReleaseSQLThreadContext(SQLThrdInitCtx *context)
{
    StorageAssert(context);
    if (context == nullptr) {
        return;
    }
#ifdef UT
    return;
#endif
    StorageAssert(m_threadReleaseCallback);
    if (m_threadReleaseCallback == nullptr) {
        return;
    }
    m_threadReleaseCallback(context);
}

void StorageInstance::RegisterIndexCallbacks(IndexGetFuncCb *indexCb)
{
    if (STORAGE_VAR_NULL(indexCb)) {
        return;
    }
    m_indexCallback = *indexCb;
}

void StorageInstance::RegisterGetLockWaitTimeoutCallback(LockWaitTimeoutMSCallback callback)
{
    m_lockWaitTimeoutCallBack = callback;
}

void StorageInstance::RegisterReportSendBgwriterCallback(ReportSendBgwriterCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportSendBgwriterCallback(callback);
}

void StorageInstance::RegisterReportBufferReadTimeCallback(ReportBufferReadTimeCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportBufferReadTimeCallback(callback);
}

void StorageInstance::RegisterReportBufferWriteTimeCallback(ReportBufferWriteTimeCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportBufferWriteTimeCallback(callback);
}

void StorageInstance::RegisterReportDeadLockTagCallback(ReportDeadLockTagCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportDeadLockTagCallback(callback);
}

void StorageInstance::RegisterReportSendTabstatCallback(ReportSendTabstatCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportSendTabstatCallback(callback);
}

void StorageInstance::RegisterReportDataIOTimeRecordCallback(ReportDataIOTimeRecordCallback callback)
{
    StorageReleasePanic(g_storageInstance->GetStat() == nullptr, MODULE_FRAMEWORK, ErrMsg("stat is nullptr."));
    return g_storageInstance->GetStat()->RegisterReportDataIOTimeRecordCallback(callback);
}

RetStatus StorageInstance::LockAndRefreshPdb(UNUSE_PARAM const PdbId pdbId, UNUSE_PARAM const LockMode mode,
                                             UNUSE_PARAM uint32_t *bitmap)
{
    return DSTORE_SUCC;
}

void StorageInstance::UnlockAfterRefreshPdb(UNUSE_PARAM const PdbId pdbId, UNUSE_PARAM const LockMode mode)
{
}

} /* namespace DSTORE */
