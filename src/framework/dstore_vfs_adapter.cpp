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
 * dstore_vfs_adapter.h
 *
 * Description: this file defineds the behaviors how does storage engine communicate
 * with the external VFS library.
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include <csignal>
#include <string>
#include "framework/dstore_instance.h"
#include "config/dstore_vfs_config.h"
#include "errorcode/dstore_framework_error_code.h"
#include "common/log/dstore_log.h"
#include "framework/dstore_vfs_interface.h"
#include "framework/dstore_vfs_adapter.h"

namespace DSTORE {

constexpr int VFS_WRITE_RETRY_TIME_US = 100;
constexpr int AIO_CALLBACK_RECV_THREADNUM = 2;
constexpr int WARNING_LOG_INTEVAL_TIMES = 1000;
constexpr int VFS_CONTROL_MAX_COUNT = 10;

VFSAdapter::VFSAdapter(PdbId pdbId)
    : m_pdbId{pdbId},
      m_fileDescHash{nullptr},
      m_staticVfs{nullptr},
      m_hasDestroyed{false},
      m_storeType{StorageType::INVALID_TYPE},
      m_fdMappingMutexlock{nullptr},
      m_mcxt{nullptr},
      m_hasOpenLocalAio(false)
{
    /* Nullify all the file descriptors, including the invalid file id. */
    for (uint32 i = 0; i < MAX_VFS_FILE_NUMBER; i++) {
        m_fileDescArray[i] = nullptr;
    }
    InitializeFdHash();
    for (int i = 0; i < NUM_FD_ARRAY_MUTEX; i++) {
        (void)pthread_rwlock_init(&(m_fdMutexArray[i]), nullptr);
    }
}

VFSAdapter::~VFSAdapter()
{
    DestroyFdHash();
    for (int i = 0; i < NUM_FD_ARRAY_MUTEX; i++) {
        (void)pthread_rwlock_destroy(&(m_fdMutexArray[i]));
    }
}

RetStatus VFSAdapter::Bootstrap()
{
    const char *vfsName = nullptr;
    char templateaVfsName[MAX_CONFIG_NAME_LENGTH] = {};
    /* There must be no one has ever initialized it. */
    if (m_staticVfs != nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("VFS is not nullptr."));
        return DSTORE_FAIL;
    }

    ::ErrorCode ret = ::InitVfsModule(nullptr);
    /*
     * VFS module may be initialized by communication module already for dumping their logs, so we simply ignore
     * VFS_ERROR_VFS_MODULE_ALREADY_INIT here.
     */
    if (ret != 0 && ret != VFS_ERROR_VFS_MODULE_ALREADY_INIT) {
        storage_set_error(VFS_ERROR_FAILED_TO_INIT_VFS_MODULE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to InitVfsModule"));
        return DSTORE_FAIL;
    }

    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    if (STORAGE_VAR_NULL(tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Vfs tenantConfig is nullptr."));
        return DSTORE_FAIL;
    }
    if (g_defaultPdbId == PDB_TEMPLATE1_ID) {
        vfsName = tenantConfig->storageConfig.template1VfsName;
    } else if (g_defaultPdbId == PDB_TEMPLATEA_ID) {
        StoragePdb::GenerateVfsName(PDB_TEMPLATEA_ID, templateaVfsName, MAX_CONFIG_NAME_LENGTH);
        vfsName = templateaVfsName;
    } else {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Invalid value of g_defaultPdbId (%d)", g_defaultPdbId));
        return DSTORE_FAIL;
    }
    StorageType storageType = tenantConfig->storageConfig.type;
    if (storageType == StorageType::PAGESTORE) {
        if (LoadVfsLibrary(tenantConfig) == nullptr) {
            return DSTORE_FAIL;
        }
#ifndef UT
        if (STORAGE_FUNC_FAIL(IoFencingVFSCollection::GetSingleton()->SetIoFencingWhiteList(vfsName))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to set iofence vfs. vfsName = %s", vfsName));
            return DSTORE_FAIL;
        }
#endif
        ret = MountVfs(GetDefaultVfsClientHandle(), tenantConfig->tenantName, vfsName, &m_staticVfs);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to mount VFS, vfsRet(%lld)", ret));
            return DSTORE_FAIL;
        }
        if (static_cast<int>(tenantConfig->storageConfig.type) < static_cast<int>(StorageType::LOCAL)) {
            m_storeType = static_cast<StorageType>(tenantConfig->storageConfig.type);
        }
    } else if (tenantConfig->storageConfig.type == StorageType::TENANT_ISOLATION) {
        ret = Open(vfsName);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to Open vfs. vfsName = %s", vfsName));
            return DSTORE_FAIL;
        }
    } else {
        ret = ::GetStaticLocalVfsInstance(&m_staticVfs);
        if (unlikely(ret != 0)) {
            storage_set_error(VFS_ERROR_FAILED_TO_GET_LIBRARY);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to get static linux vfs library. VFS_ERROR = %lld", ret));
            return DSTORE_FAIL;
        }
        m_storeType = StorageType::LOCAL;
    }
    OpenLocalAioIfNeed();
    m_hasDestroyed = false;
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::CreateVfs(const char *datadir, const char *vfsName, const uint64 ioFenceFlag)
{
    (void)datadir;
    ::ErrorCode ret = ::InitVfsModule(nullptr);
    if (ret != 0 && ret != VFS_ERROR_VFS_MODULE_ALREADY_INIT) {
        storage_set_error(VFS_ERROR_FAILED_TO_INIT_VFS_MODULE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to InitVfsModule"));
        return DSTORE_FAIL;
    }
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    if (STORAGE_VAR_NULL(tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("VFS tenantConfig is nullptr."));
        return DSTORE_FAIL;
    }
    m_storeType = static_cast<StorageType>(tenantConfig->storageConfig.type);
    if (m_storeType == StorageType::PAGESTORE) {
        if (GetVfsLibHandle() == nullptr) {
            RetStatus status = DynamicLinkVFS(static_cast<void *>(tenantConfig), g_storageInstance->GetType(), 0);
            if (STORAGE_FUNC_FAIL(status)) {
                return DSTORE_FAIL;
            }
        }
        if (STORAGE_FUNC_FAIL(CreateVFS(&m_staticVfs, vfsName, ioFenceFlag))) {
            return DSTORE_FAIL;
        }
        bool enableIoFencing = (ioFenceFlag == VFS_ENABLE_IO_FENCE_FLAG);
        RetStatus rc = IoFencingVFSCollection::GetSingleton()->AddActiveVFS(vfsName, enableIoFencing);
        if (STORAGE_FUNC_FAIL(rc)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to AddActiveVFS"));
            return DSTORE_FAIL;
        }
        if (g_storageInstance->GetGuc()->memberView == nullptr) {
            uint64 nodeId = static_cast<uint64>(tenantConfig->nodeId);
            uint64 nodeTimelineId = NODE_TIMELINE_ID_DEFAULT;
            rc = IoFencingVFSCollection::GetSingleton()->SetIoFencingWhiteList(&nodeId, &nodeTimelineId, 1, 1);
            if (STORAGE_FUNC_FAIL(rc)) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                    ErrMsg("Failed to SetIoFencingWhiteList"));
            }
            return DSTORE_FAIL;
        }

        rc = IoFencingVFSCollection::GetSingleton()->TriggerIoFencing();
        if (STORAGE_FUNC_FAIL(rc)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to io fencing."));
            return DSTORE_FAIL;
        }
    } else if (m_storeType == StorageType::TENANT_ISOLATION) {
        if (GetVfsLibHandle() == nullptr) {
            RetStatus status =
                DynamicLinkVFS(static_cast<void *>(tenantConfig), g_storageInstance->GetType(), 0, datadir);
            if (STORAGE_FUNC_FAIL(status)) {
                return DSTORE_FAIL;
            }
        }
        if (STORAGE_FUNC_FAIL(CreateVFS(&m_staticVfs, vfsName))) {
            return DSTORE_FAIL;
        }
    } else {
        ret = ::GetStaticLocalVfsInstance(&m_staticVfs);
        if (unlikely(ret != 0)) {
            storage_set_error(VFS_ERROR_FAILED_TO_GET_LIBRARY);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to get static linux vfs library. VFS_ERROR = %lld", ret));
            return DSTORE_FAIL;
        }
    }
    OpenLocalAioIfNeed();
    return DSTORE_SUCC;
}

bool VFSAdapter::HasMounted() const
{
    return (m_staticVfs != nullptr);
}

RetStatus VFSAdapter::Open(const char *vfsName)
{
    if (unlikely(vfsName == nullptr)) {
        storage_set_error(VFS_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("Invalid vfs name"));
        return DSTORE_FAIL;
    }
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    if (STORAGE_VAR_NULL(tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("VFS tenantConfig is nullptr."));
        return DSTORE_FAIL;
    }
    m_storeType = static_cast<StorageType>(tenantConfig->storageConfig.type);
    if (GetVfsLibHandle() == nullptr) {
        DstoreLWLockAcquire(GetVfsHandleLock(), LW_EXCLUSIVE);
        if (likely(GetVfsLibHandle() == nullptr)) {
            char *path = (char *)DstorePalloc0(MAXPGPATH);
            if (path == nullptr) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to alloc memory in VFSAdapter::Open"));
                return DSTORE_FAIL;
            }
            int rc = sprintf_s(path, MAXPGPATH, "%s/%s", g_storageInstance->GetGuc()->dataDir, BASE_DIR);
            storage_securec_check_ss(rc);
            RetStatus ret = DynamicLinkVFS(static_cast<void *>(tenantConfig), g_storageInstance->GetType(), 0, path);
            DstorePfreeExt(path);
            if (unlikely(ret != DSTORE_SUCC)) {
                LWLockRelease(GetVfsHandleLock());
                storage_set_error(VFS_ERROR_FAILED_TO_GET_LIBRARY);
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to Init vfs library"));
                return DSTORE_FAIL;
            }
        }
        LWLockRelease(GetVfsHandleLock());
    }

    uint64 ioFenceFlag = VFS_ENABLE_IO_FENCE_FLAG;
    /* Mount vfs with name and get vfs handle */
    if (STORAGE_FUNC_FAIL(OpenVFS(tenantConfig->tenantName, vfsName, &m_staticVfs, ioFenceFlag))) {
        storage_set_error(VFS_ERROR_FAILED_TO_OPEN_VFS);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to open VFS."));
        return DSTORE_FAIL;
    }
    OpenLocalAioIfNeed();
    return DSTORE_SUCC;
}

/* currently, only storageType PAGESTORE and LOCAL use this interface,
 * storageType TENANT_ISOLATION use Open(char *vfsName) */
RetStatus VFSAdapter::Initialize(const char *vfsName, uint32 clientId)
{
    if (m_staticVfs != nullptr) {
        m_hasDestroyed = false;
        return DSTORE_SUCC;
    }
    RetStatus retStatus = ModuleInitialize();
    if (STORAGE_FUNC_FAIL(retStatus)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("ModuleInitialize fail!"));
        return DSTORE_FAIL;
    }
    auto tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    if (STORAGE_VAR_NULL(tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("VFS tenantConfig is nullptr."));
        return DSTORE_FAIL;
    }
    m_storeType = tenantConfig->storageConfig.type;
    if (tenantConfig->storageConfig.type == StorageType::PAGESTORE) {
        if (GetVfsLibHandle() == nullptr) {
            RetStatus ret = LibInitialize(tenantConfig, g_storageInstance->GetType(), clientId);
            if (unlikely(ret != DSTORE_SUCC)) {
                storage_set_error(VFS_ERROR_FAILED_TO_GET_LIBRARY);
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to Init vfs library"));
                return DSTORE_FAIL;
            }
        }
        
        uint64 ioFenceFlag = VFS_ENABLE_IO_FENCE_FLAG;
        if (STORAGE_FUNC_FAIL(OpenVFS(tenantConfig->tenantName, vfsName, &m_staticVfs, ioFenceFlag))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to OpenVFS."));
            return DSTORE_FAIL;
        }
    } else if (tenantConfig->storageConfig.type == StorageType::TENANT_ISOLATION) {
        RetStatus ret = Open(vfsName);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to Open vfs. vfsName = %s", vfsName));
            return DSTORE_FAIL;
        }
    } else {
        ErrorCode ret = ::GetStaticLocalVfsInstance(&m_staticVfs);
        if (ret != 0) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to get static linux vfs library. VFS_ERROR = %lld", ret));
            return DSTORE_FAIL;
        }
    }
    m_hasDestroyed = false;
    OpenLocalAioIfNeed();
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::ModuleInitialize()
{
    ::ErrorCode ret = ::InitVfsModule(nullptr);
    if (ret != 0 && ret != VFS_ERROR_VFS_MODULE_ALREADY_INIT) {
        storage_set_error(VFS_ERROR_FAILED_TO_INIT_VFS_MODULE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to InitVfsModule"));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::LibInitialize(void *tenantConfig, StorageInstanceType type, uint32 clientId)
{
    if (GetVfsLibHandle() != nullptr) {
        return DSTORE_SUCC;
    }
    TenantConfig *tenantCfg = static_cast<TenantConfig *>(tenantConfig);
    if (unlikely(tenantCfg == nullptr)) {
        (void)fprintf(stderr, "Dynamic link VFS tenant cfg ptr is invalid!\n");
        return DSTORE_FAIL;
    }
    StorageType storageType = tenantCfg->storageConfig.type;
    if (storageType == StorageType::TENANT_ISOLATION) {
        (void)fprintf(stderr, "Dynamic link VFS TENANT_ISOLATION ignore.\n");
        return DSTORE_SUCC;
    }

    RetStatus status = DynamicLinkVFS(tenantConfig, type, clientId);
    if (STORAGE_FUNC_FAIL(status)) {
        (void)fprintf(stderr, "Failed to get dynamic vfs library.\n");
    }
    return status;
}

VfsLibHandle *VFSAdapter::GetVfsLib()
{
    return static_cast<VfsLibHandle *>(GetVfsLibHandle());
}

RetStatus VFSAdapter::CloseAllFiles()
{
    /* Remember zero is the invalid file id. */
    for (uint32 i = 1; i < MAX_VFS_FILE_NUMBER; i++) {
        Close(static_cast<FileId>(i));
    }
    /*
     * Check whether the ref count of entry in fd hash is not 0. If there exists a file still opened, the caller of vfs
     * adapter interfaces is to blame.
     */
    if (m_fileDescHash == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("VFSAdapter[pdbId: %u]: fileDesc hash table is nullptr.", m_pdbId));
        return DSTORE_FAIL;
    }
    HASH_SEQ_STATUS scan;
    hash_seq_init(&scan, m_fileDescHash);
    VFSAdapterFdHashEntry *entry;
    while ((entry = static_cast<VFSAdapterFdHashEntry *>(hash_seq_search(&scan))) != nullptr) {
#ifndef UT
        if (entry->refCount > 0) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("CloseAllFiles[pdbId: %u]: file(%s) still opened when fd hash destroying, refCount: %u",
                          m_pdbId, entry->pathName, entry->refCount));
            return DSTORE_FAIL;
        }
#else
        (void)CloseFile(entry->fileDesc);
#endif
    }
    return DSTORE_SUCC;
}

struct FakeVirtualFileSystem {
    void *vfsHandle;
    void *ops;
    bool isStaticVfs;
};

RetStatus VFSAdapter::Destroy(const char *vfsName, bool dropData)
{
    if (m_hasDestroyed) {
        return DSTORE_SUCC;
    }
    if (STORAGE_FUNC_FAIL(CloseAllFiles())) {
        return DSTORE_FAIL;
    }
    m_hasDestroyed = true;

    /* m_staticVfs init failed. */
    if (m_staticVfs == nullptr) {
        return DSTORE_SUCC;
    }

    (void)static_cast<FakeVirtualFileSystem *>(static_cast<void *>(m_staticVfs))->vfsHandle;
    (void)static_cast<FakeVirtualFileSystem *>(static_cast<void *>(m_staticVfs))->ops;
    if (!(static_cast<FakeVirtualFileSystem *>(static_cast<void *>(m_staticVfs))->isStaticVfs || vfsName == nullptr)) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DynamicUnlinkVFS] vfsName is %s.", vfsName));
        DynamicUnlinkVFS(m_staticVfs, vfsName, dropData);
    }
    m_staticVfs = nullptr;
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::DestroyDataForce(const char *vfsName, PdbId pdbId)
{
    if (vfsName == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DestroyDataForce] vfsName is null."));
        return DSTORE_FAIL;
    }
    return DropVfsDataForce(vfsName, pdbId);
}

void VFSAdapter::InitializeFdHash()
{
    /* Create a shared DstoreMemoryContext for hash table */
    m_mcxt = DstoreAllocSetContextCreate(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE),
                                         "VFSAdaptorFdHashMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
                                         ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                         MemoryContextType::SHARED_CONTEXT);
    StorageReleasePanic(
        STORAGE_VAR_NULL(m_mcxt), MODULE_FRAMEWORK,
        ErrMsg("VFSAdapter[pdbId: %u]: Failed to create memory context for VFSAdaptorFdHashMemoryContext", m_pdbId));

    /* Initialize partitionally mapping locks */
    Size hashMappingLockSize = NUM_FD_HASH_PARTITIONS * sizeof(Mutex);
    m_fdMappingMutexlock = (Mutex *)DstorePallocAligned(hashMappingLockSize, DSTORE_CACHELINE_SIZE);
    StorageReleasePanic(m_fdMappingMutexlock == nullptr, MODULE_FRAMEWORK,
                        ErrMsg("VFSAdapter[pdbId: %u]: alloc memory for fdMappingMutexlock fail!", m_pdbId));
    for (int i = 0; i < NUM_FD_HASH_PARTITIONS; i++) {
        MutexInit(&m_fdMappingMutexlock[i]);
    }

    /* Create shared hash table */
    HASHCTL ctl;
    ctl.keysize = MAXPGPATH;                       /* Hash key is file pathName */
    ctl.entrysize = sizeof(VFSAdapterFdHashEntry); /* Hash value fd hash entry */
    ctl.hash = string_hash;
    ctl.num_partitions = NUM_FD_HASH_PARTITIONS;
    ctl.dsize = hash_select_dirsize(NUM_FD_HASH_INIT_ENTRY);
    ctl.hcxt = m_mcxt;

    m_fileDescHash =
        hash_create("vfs adaptor hash for file descriptor", NUM_FD_HASH_INIT_ENTRY, &ctl,
                    HASH_ELEM | HASH_FUNCTION | HASH_PARTITION | HASH_DIRSIZE | HASH_CONTEXT | HASH_SHRCTX);
    StorageReleasePanic(STORAGE_VAR_NULL(m_fileDescHash), MODULE_FRAMEWORK,
                        ErrMsg("VFSAdapter[pdbId: %u]: Failed to create fileDesc hash", m_pdbId));
}

void VFSAdapter::DestroyFdHash()
{
    if (m_fileDescHash != nullptr) {
        hash_destroy(m_fileDescHash);
        m_fileDescHash = nullptr;
        ErrLog(DSTORE_DEBUG1, MODULE_FRAMEWORK, ErrMsg("VFSAdapter[pdbId: %u]: fileDesc hash destroyed", m_pdbId));
    }
    if (m_mcxt != nullptr) {
        DstoreMemoryContextDelete(m_mcxt);
        m_mcxt = nullptr;
    }
    if (m_fdMappingMutexlock != nullptr) {
        for (int i = 0; i < NUM_FD_HASH_PARTITIONS; i++) {
            MutexDestroy(&m_fdMappingMutexlock[i]);
        }
        DstorePfreeAligned(m_fdMappingMutexlock);
        m_fdMappingMutexlock = nullptr;
    }
}

void VFSAdapter::LockHashPartition(const char *pathName)
{
    uint32 hashCode = get_hash_value(m_fileDescHash, static_cast<const void *>(pathName));
    Mutex *mutex = m_fdMappingMutexlock + (hashCode % NUM_FD_HASH_PARTITIONS);
    MutexLock(mutex);
}

void VFSAdapter::UnLockHashPartition(const char *pathName)
{
    uint32 hashCode = get_hash_value(m_fileDescHash, static_cast<const void *>(pathName));
    Mutex *mutex = m_fdMappingMutexlock + (hashCode % NUM_FD_HASH_PARTITIONS);
    MutexUnlock(mutex);
}

void VFSAdapter::LockAllHashPartition()
{
    for (int i = 0; i < NUM_FD_HASH_PARTITIONS; i++) {
        Mutex *mutex = &m_fdMappingMutexlock[i];
        MutexLock(mutex);
    }
}

void VFSAdapter::UnLockAllHashPartition()
{
    for (int i = 0; i < NUM_FD_HASH_PARTITIONS; i++) {
        Mutex *mutex = &m_fdMappingMutexlock[i];
        MutexUnlock(mutex);
    }
}

RetStatus VFSAdapter::CreateFile(FileId fileId, const char *pathName, ::FileParameter const filePara)
{
    if (GetFileDescriptor(fileId) != nullptr) {
        storage_set_error(VFS_ERROR_FILE_ALREADY_CREATED_OR_OPEN);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("The file(fileId = %hu) has already been created or opened.", fileId));
        return DSTORE_FAIL;
    }
    /* Make sure it has never created the file */
    if (FileExists(fileId, pathName)) {
        storage_set_error(VFS_WARNING_FILE_ALREADY_EXISTS);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("Creating file failed, filePath(%s) already exists.", pathName));
        return DSTORE_FAIL;
    }

    int mutexIndex = fileId % NUM_FD_ARRAY_MUTEX;
    if (pthread_rwlock_wrlock(&(m_fdMutexArray[mutexIndex])) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Acquire file lock failed."));
        return DSTORE_FAIL;
    }
    if (m_fileDescArray[fileId] != nullptr) {
        storage_set_error(VFS_ERROR_FILE_ALREADY_CREATED_OR_OPEN);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("The file(fileId = %hu) has already been created or opened.", fileId));
        (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
        return DSTORE_FAIL;
    }
    /* The filePara only makes sense when using PageStore, it means nothing if it's for a local vfs. */
    ::ErrorCode vfsRet;
    ::FileDescriptor *fileDesc;
    if (m_storeType == StorageType::PAGESTORE) {
        char fileName[MAXPGPATH] = {0};
        RetStatus ret = GetFileNameFromFileId(fileId, fileName);
        if (STORAGE_FUNC_FAIL(ret)) {
            (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
            storage_set_error(VFS_ERROR_FAILED_TO_GET_FILENAME);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("CreateFile: Get fileName from fileId(%hu) failed.", fileId));
            return DSTORE_FAIL;
        }
        vfsRet = ::Create(m_staticVfs, fileName, filePara, &fileDesc);
    } else {
        vfsRet = ::Create(m_staticVfs, pathName, filePara, &fileDesc);
    }
    if (vfsRet != 0) {
        (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
        storage_set_error(VFS_WARNING_FAILED_TO_CREATE_FILE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Creating file failed, filePath(%s), vfsRet(%lld).", pathName, vfsRet));
        return DSTORE_FAIL;
    }
    m_fileDescArray[fileId] = fileDesc;
    (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::RemoveFile(FileId fileId, const char *pathName)
{
    /* Make sure the file has been created. */
    if (!FileExists(fileId, pathName)) {
        storage_set_error(VFS_WARNING_FILE_ALREADY_EXISTS);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("Removing file failed, filePath(%s) does not exist.", pathName));
        return DSTORE_FAIL;
    }

    if (GetFileDescriptor(fileId) != nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_CLOSED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("The file(fileId = %hu) to be removed has not been closed.", fileId));
        Close(fileId);
    }

    ::ErrorCode vfsRet;
    if (m_storeType == StorageType::PAGESTORE) {
        char fileName[MAXPGPATH];
        RetStatus ret = GetFileNameFromFileId(fileId, fileName);
        if (STORAGE_FUNC_FAIL(ret)) {
            storage_set_error(VFS_ERROR_FAILED_TO_GET_FILENAME);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("RemoveFile: Get fileName from fileId(%hu) failed.", fileId));
            return DSTORE_FAIL;
        }
        vfsRet = ::Remove(m_staticVfs, fileName);
    } else {
        vfsRet = ::Remove(m_staticVfs, pathName);
    }
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_REMOVE_FILE);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("Failed to remove the file(fileId = %hu), vfsRet(%lld).", fileId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

bool VFSAdapter::FileExists(FileId fileId, const char *pathName)
{
    if (m_storeType == StorageType::PAGESTORE) {
        char fileName[MAXPGPATH];
        RetStatus ret = GetFileNameFromFileId(fileId, fileName);
        StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_FRAMEWORK,
                            ErrMsg("FileExists[pdbId: %u]: Get fileName from fileId(%hu) failed.", m_pdbId, fileId));
        return FileExists(fileName);
    } else {
        return FileExists(pathName);
    }
}

bool VFSAdapter::FileExists(const char *pathName)
{
    bool fileExists;
    ::ErrorCode vfsRet = ::FileIsExist(m_staticVfs, pathName, &fileExists);
    StorageReleasePanic(
        (vfsRet != 0), MODULE_FRAMEWORK,
        ErrMsg("FileExists[pdbId: %u]: Checking file existence in vfs failed, filePath(%s), vfsRet(%lld).", m_pdbId,
               pathName, vfsRet));
    return fileExists;
}

RetStatus VFSAdapter::OpenFile(FileId fileId, const char *pathName, int flags)
{
    ::FileDescriptor *fileDesc = GetFileDescriptor(fileId);
    if (fileDesc != nullptr) {
        return DSTORE_SUCC;
    }

    int mutexIndex = fileId % NUM_FD_ARRAY_MUTEX;
    if (pthread_rwlock_wrlock(&(m_fdMutexArray[mutexIndex])) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Acquire file lock failed."));
        return DSTORE_FAIL;
    }
    if (m_fileDescArray[fileId] != nullptr) {
        (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
        return DSTORE_SUCC;
    }

    ::ErrorCode vfsRet;
    if (m_storeType == StorageType::PAGESTORE) {
        char fileName[MAXPGPATH];
        RetStatus ret = GetFileNameFromFileId(fileId, fileName);
        if (STORAGE_FUNC_FAIL(ret)) {
            (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
            storage_set_error(VFS_ERROR_FAILED_TO_GET_FILENAME);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("OpenFile: Get fileName from fileId(%hu) failed.", fileId));
            return DSTORE_FAIL;
        }
        vfsRet = ::Open(m_staticVfs, fileName, flags, &fileDesc);
    } else {
        vfsRet = ::Open(m_staticVfs, pathName, flags, &fileDesc);
    }
    if (vfsRet != 0) {
        (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
        storage_set_error(VFS_FATAL_FAILED_TO_OPEN_FILE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Open file failed, filePath(%s), vfsRet(%lld).", pathName, vfsRet));
        return DSTORE_FAIL;
    }
    m_fileDescArray[fileId] = fileDesc;
    (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::Close(FileId fileId)
{
    ::FileDescriptor *fileDesc = GetFileDescriptor(fileId);
    if (fileDesc == nullptr) {
        return DSTORE_SUCC;
    }

    int mutexIndex = fileId % NUM_FD_ARRAY_MUTEX;
    if (pthread_rwlock_wrlock(&(m_fdMutexArray[mutexIndex])) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Acquire file lock failed."));
        return DSTORE_FAIL;
    }
    if (m_fileDescArray[fileId] == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Can not find FileDescriptor, fileId(%hu).", fileId));
        (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
        return DSTORE_FAIL;
    }
    ::ErrorCode vfsRet = ::Close(fileDesc);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_CLOSE_FILE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Closing file failure, fileId(%hu, vfsRet(%lld).", fileId, vfsRet));
        (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
        return DSTORE_FAIL;
    }
    m_fileDescArray[fileId] = nullptr;
    (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::ReadPageSync(const PageId pageId, void *outBuffer)
{
    ::FileDescriptor *fileDesc = GetFileDescriptor(pageId.m_fileId);
    if (fileDesc == nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_OPENED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("Reading file failure, fileId(%hu) not opened.", pageId.m_fileId));
        return DSTORE_FAIL;
    }

    uint64 count = BLCKSZ;
    int64 offset = GetOffsetByBlockNo(pageId.m_blockId);
    int64 readBytes;

    ::ErrorCode vfsRet = ::Pread(fileDesc, outBuffer, count, offset, &readBytes);
    if (vfsRet != 0 || readBytes != BLCKSZ) {
        storage_set_error(VFS_WARNING_FAILED_TO_PREAD_FILE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Pread failed pdbId(%hhu) fileId(%hu), blockId(%u) vfsRet(%lld).", m_pdbId, pageId.m_fileId,
                      pageId.m_blockId, vfsRet));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus VFSAdapter::ReadPageSync(FileDescriptor *fileDesc, BlockNumber blockNumber, void *outBuffer)
{
    if (unlikely(fileDesc == nullptr)) {
        storage_set_error(VFS_WARNING_FILE_NOT_OPENED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("Reading file failure, file not opened."));
        return DSTORE_FAIL;
    }

    uint64 count = BLCKSZ;
    int64 offset = GetOffsetByBlockNo(blockNumber);
    int64 readBytes;

    ::ErrorCode vfsRet = ::Pread(fileDesc, outBuffer, count, offset, &readBytes);
    if (vfsRet != 0 || readBytes != BLCKSZ) {
        storage_set_error(VFS_WARNING_FAILED_TO_PREAD_FILE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Pread failed pdbId(%hhu), blockId(%u) vfsRet(%lld).", m_pdbId, blockNumber, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::Pread(FileId fileId, void *buf, uint64 count, int64 offset, int64 *readSize)
{
    ::FileDescriptor *fileDesc = GetFileDescriptor(fileId);
    if (fileDesc == nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_OPENED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("Reading file failure, fileId(%hu) not opened.", fileId));
        return DSTORE_FAIL;
    }

    ::ErrorCode vfsRet = ::Pread(fileDesc, buf, count, offset, readSize);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_PREAD_FILE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Pread failure, fileId(%hu), vfsRet(%lld).", fileId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::WritePageSync(const PageId pageId, const void *inBuffer)
{
    ::FileDescriptor *fileDesc = GetFileDescriptor(pageId.m_fileId);
    if (fileDesc == nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_OPENED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("Writing file failure, fileId(%hu) not opened.", pageId.m_fileId));
        return DSTORE_FAIL;
    }

    return PwriteSync(fileDesc, inBuffer, BLCKSZ, GetOffsetByBlockNo(pageId.m_blockId));
}

RetStatus VFSAdapter::WritePageSync(const FileDescriptor *fileDesc,  BlockNumber blockNumber, const void *inBuffer)
{
    return PwriteSync(const_cast<FileDescriptor *>(fileDesc), inBuffer, BLCKSZ, GetOffsetByBlockNo(blockNumber));
}

RetStatus VFSAdapter::PwriteSync(FileId fileId, const void *inBuffer, uint64 count, int64 offset)
{
    return PwriteSync(GetFileDescriptor(fileId), inBuffer, count, offset);
}

RetStatus VFSAdapter::WritePageAsync(const PageId pageId, const void *inBuffer, const AsyncIoContext *aioContext)
{
    if (USE_VFS_LOCAL_AIO) {
        ASSERT_POINTER_ALIGNMENT(inBuffer, AIO_MAX_REQNUM);
    }
    int64 offset = GetOffsetByBlockNo(pageId.m_blockId);
    RetStatus ret = PwriteAsync(GetFileDescriptor(pageId.m_fileId), inBuffer, BLCKSZ, offset, aioContext);

    ErrLog(DSTORE_DEBUG1, MODULE_FRAMEWORK,
           ErrMsg("PwriteAsync success, PageId(%hu, %u).", pageId.m_fileId, pageId.m_blockId));
    return ret;
}

RetStatus VFSAdapter::PwriteAsync(FileId fileId, const void *buf, uint64 count, int64 offset,
                                  const AsyncIoContext *aioContext)
{
    return PwriteAsync(GetFileDescriptor(fileId), buf, count, offset, aioContext);
}

::FileDescriptor *VFSAdapter::GetFileDescriptor(FileId fileId)
{
    if (m_staticVfs == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("VFS is nullptr."));
        return nullptr;
    }
    int mutexIndex = fileId % NUM_FD_ARRAY_MUTEX;
    int ret = pthread_rwlock_rdlock(&(m_fdMutexArray[mutexIndex]));
    StorageReleasePanic((ret != EOK), MODULE_FRAMEWORK, ErrMsg("Acquire file lock failed.ret: %d", ret));
    ::FileDescriptor *fileDesc = m_fileDescArray[fileId];
    (void)pthread_rwlock_unlock(&(m_fdMutexArray[mutexIndex]));
    return fileDesc;
}

RetStatus VFSAdapter::Fsync(FileId fileId)
{
    ::FileDescriptor *fileDesc = GetFileDescriptor(fileId);
    if (fileDesc == nullptr) {
        storage_set_error(VFS_WARNING_FSYNC_FILE_NOT_OPENED);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Fsync failure, fileId(%hu) not opened.", fileId));
        return DSTORE_FAIL;
    }
    ::ErrorCode vfsRet = ::Fsync(fileDesc);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_FSYNC);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("FSync failure, fileId(%hu), vfsRet(%lld).", fileId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

int64 VFSAdapter::GetSize(FileId fileId)
{
    ::FileDescriptor *fileDesc = GetFileDescriptor(fileId);
    if (fileDesc == nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_OPENED);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Getting file size failure, fileId(%hu) not opened.", fileId));
        return -1;
    }
    int64 fileSize;
    ::ErrorCode vfsRet = ::GetSize(fileDesc, &fileSize);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_GET_SIZE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed to get the file size, fileId(%hu), vfsRet(%lld).", fileId, vfsRet));
        return -1;
    }
    return fileSize;
}

RetStatus VFSAdapter::Extend(FileId fileId, int64 length)
{
    ::FileDescriptor *fileDesc = GetFileDescriptor(fileId);
    if (fileDesc == nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_OPENED);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("VFS Extend failure, fileId(%hu) not opened.", fileId));
        return DSTORE_FAIL;
    }
    ::ErrorCode vfsRet = ::Extend(fileDesc, length);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_EXTEND);
        ErrLog(GetSize(fileId) >= length ? DSTORE_WARNING : DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("Extend failure, fileId(%hu), vfsRet(%lld).", fileId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::Truncate(FileId fileId, int64 length)
{
    ::FileDescriptor *fileDesc = GetFileDescriptor(fileId);
    if (fileDesc == nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_OPENED);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("VFS Truncate failure, fileId(%hu) not opened.", fileId));
        return DSTORE_FAIL;
    }
    ::ErrorCode vfsRet = ::Truncate(fileDesc, length);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_TRUNCATE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Truncate failure, fileId(%hu, vfsRet(%lld).", fileId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::Rewind(FileId fileId)
{
    ::FileDescriptor *fileDesc = GetFileDescriptor(fileId);
    if (fileDesc == nullptr) {
        storage_set_error(VFS_WARNING_FSYNC_FILE_NOT_OPENED);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Rewind failure, fileId(%hu) not opened.", fileId));
        return DSTORE_FAIL;
    }
    ::ErrorCode vfsRet = ::Rewind(fileDesc);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_FSYNC);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Rewind failure, fileId(%hu, vfsRet(%lld).", fileId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::RenameFile(FileId srcFileId, FileId destFileId, const char *srcPathName, const char *destPathName)
{
    if (GetFileDescriptor(srcFileId) != nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_CLOSED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("The file(fileId = %hu) to be removed has not been closed.", srcFileId));
        Close(srcFileId);
    }

    ::ErrorCode vfsRet;
    RetStatus result;
    if (m_storeType == StorageType::PAGESTORE) {
        char srcfileName[MAXPGPATH];
        char destfileName[MAXPGPATH];
        result = GetFileNameFromFileId(srcFileId, srcfileName);
        if (STORAGE_FUNC_FAIL(result)) {
            storage_set_error(VFS_ERROR_FAILED_TO_GET_FILENAME);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("RenameFile: Get SrcfileName from fileId(%hu) failed.", srcFileId));
            return DSTORE_FAIL;
        }
        result = GetFileNameFromFileId(destFileId, destfileName);
        if (STORAGE_FUNC_FAIL(result)) {
            storage_set_error(VFS_ERROR_FAILED_TO_GET_FILENAME);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("RenameFile: Get DestfileName from fileId(%hu) failed.", destFileId));
            return DSTORE_FAIL;
        }
        vfsRet = ::RenameFile(m_staticVfs, srcfileName, destfileName);
    } else {
        vfsRet = ::RenameFile(m_staticVfs, srcPathName, destPathName);
    }

    if (unlikely(vfsRet != 0)) {
        storage_set_error(VFS_ERROR_FAILED_TO_RENAME);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("RenameFile failure, fileId(%hu, vfsRet(%lld) destFileId %hu.", srcFileId, vfsRet, destFileId));
        result = DSTORE_FAIL;
    } else {
        result = DSTORE_SUCC;
    }
    return result;
}

IoFencingVFSCollection IoFencingVFSCollection::m_self;

IoFencingVFSCollection::IoFencingVFSCollection() noexcept : m_ioFencingCallback(nullptr), m_totalActiveVFSCnt(0)
{
    errno_t rc = memset_s(m_clusterActiveVFS, sizeof(m_clusterActiveVFS), 0, sizeof(m_clusterActiveVFS));
    storage_securec_check(rc, "\0", "\0");
    (void)pthread_rwlock_init(&m_mutex, nullptr);
}

IoFencingVFSCollection::~IoFencingVFSCollection()
{
    (void)pthread_rwlock_destroy(&m_mutex);
    m_ioFencingCallback = nullptr;
}

RetStatus IoFencingVFSCollection::AddActiveVFS(const char *vfsName, bool enableIoFencing, uint32 clusterId)
{
    if (pthread_rwlock_wrlock(&m_mutex) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[IO_FENCE] Acquire pthread lock failed."));
        return DSTORE_FAIL;
    }

    /* check cluster id if exsit. */
    uint32 clusterSlot = MAX_CLUSTER_COUNT;
    bool isClusterExist = false;
    for (uint32 slot = 0; slot < MAX_CLUSTER_COUNT; slot++) {
        if (!m_clusterActiveVFS[slot].isValid) {
            clusterSlot = slot;
            continue;
        }
        if (m_clusterActiveVFS[slot].clusterId != clusterId) {
            continue;
        }
        isClusterExist = true;
        clusterSlot = slot;
        /* check duplicate vfs name. */
        for (int i = 0; i < maxVfsNumPerInstance; i++) {
            if (m_clusterActiveVFS[clusterSlot].activeVFS[i].isValid &&
                strcmp(m_clusterActiveVFS[clusterSlot].activeVFS[i].vfsName, vfsName) == 0) {
                storage_set_error(VFS_WARNING_DUPLICATE_ADD_ACTIVE);
                (void)pthread_rwlock_unlock(&m_mutex);
                ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg(
                    "[IO_FENCE] duplicate vfs name %s, cluster id %u, cannot Add ActiveVFS again.",
                    vfsName, clusterId));
                return DSTORE_FAIL;
            }
        }
        break;
    }

    if (!isClusterExist) {
        if (clusterSlot == MAX_CLUSTER_COUNT) {
            storage_set_error(VFS_WARNING_NO_AVAILABLE_SLOT);
            (void)pthread_rwlock_unlock(&m_mutex);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
                "[IO_FENCE] no available slot for active vfs %s, cluster id %u, cannot add ActiveVFS.",
                vfsName, clusterId));
            return DSTORE_FAIL;
        } else {
            m_clusterActiveVFS[clusterSlot].clusterId = clusterId;
            m_clusterActiveVFS[clusterSlot].isValid = true;
            m_clusterActiveVFS[clusterSlot].activeVFSCnt = 0;
        }
    }

    RetStatus ret = DSTORE_FAIL;
    for (int i = 0; i < maxVfsNumPerInstance; i++) {
        if (!m_clusterActiveVFS[clusterSlot].activeVFS[i].isValid) {
            errno_t rc = strcpy_s(m_clusterActiveVFS[clusterSlot].activeVFS[i].vfsName,
                sizeof(m_clusterActiveVFS[clusterSlot].activeVFS[i].vfsName), vfsName);
            storage_securec_check(rc, "\0", "\0");
            m_clusterActiveVFS[clusterSlot].activeVFS[i].isValid = true;
            m_clusterActiveVFS[clusterSlot].activeVFS[i].enableIoFencing = enableIoFencing;
            m_totalActiveVFSCnt = enableIoFencing ? (m_totalActiveVFSCnt + 1) : m_totalActiveVFSCnt;
            m_clusterActiveVFS[clusterSlot].activeVFSCnt = enableIoFencing ?
                (m_clusterActiveVFS[clusterSlot].activeVFSCnt + 1) : m_clusterActiveVFS[clusterSlot].activeVFSCnt;
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
                "[AddActiveVFS] add active vfs success, vfs name %s, cluster id %u.", vfsName, clusterId));
            ret = DSTORE_SUCC;
            break;
        }
    }

    if (ret != DSTORE_SUCC) {
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[IO_FENCE] ActiveVFS %s cluster id %u add active VFS failed.", vfsName, clusterId));
        return DSTORE_FAIL;
    }
    (void)pthread_rwlock_unlock(&m_mutex);
    return ret;
}

RetStatus IoFencingVFSCollection::RemoveActiveVFS(const char *vfsName, uint32 clusterId)
{
    if (vfsName == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[IO_FENCE] vfs name is null."));
        return DSTORE_FAIL;
    }
    if (pthread_rwlock_wrlock(&m_mutex) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Acquire pthread lock failed."));
        return DSTORE_FAIL;
    }

    RetStatus ret = DSTORE_FAIL;
    bool isClusterExist = false;
    for (uint32 slot = 0; slot < MAX_CLUSTER_COUNT; slot++) {
        if (!m_clusterActiveVFS[slot].isValid || m_clusterActiveVFS[slot].clusterId != clusterId) {
            continue;
        }
        isClusterExist = true;
        for (int i = 0; i < maxVfsNumPerInstance; i++) {
            if (m_clusterActiveVFS[slot].activeVFS[i].isValid &&
                (memcmp(m_clusterActiveVFS[slot].activeVFS[i].vfsName, vfsName, strlen(vfsName)) == 0)) {
                m_clusterActiveVFS[slot].activeVFS[i].isValid = false;
                m_totalActiveVFSCnt = m_clusterActiveVFS[slot].activeVFS[i].enableIoFencing ?
                    (m_totalActiveVFSCnt - 1) : m_totalActiveVFSCnt;
                m_clusterActiveVFS[slot].activeVFSCnt = m_clusterActiveVFS[slot].activeVFS[i].enableIoFencing ?
                    (m_clusterActiveVFS[slot].activeVFSCnt - 1) : m_clusterActiveVFS[slot].activeVFSCnt;
                m_clusterActiveVFS[slot].activeVFS[i].enableIoFencing = false;
                ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
                    "[RemoveActiveVFS] remove active vfs success, vfs name %s, cluster id %u.", vfsName, clusterId));
                ret = DSTORE_SUCC;
                break;
            }
        }
    }

    if (!isClusterExist) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[IO_FENCE] cluster id %u not exsit for active vfs %s, cannot remove ActiveVFS.",
            clusterId, vfsName));
        (void)pthread_rwlock_unlock(&m_mutex);
        return ret;
    }

    if (ret == DSTORE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[IO_FENCE] vfs %s not exist for cluster id %u, cannot remove ActiveVFS.",
            vfsName, clusterId));
    }
    (void)pthread_rwlock_unlock(&m_mutex);
    return ret;
}

RetStatus IoFencingVFSCollection::GetActiveDrVfsIoFenceErr(const char *vfsName, uint32 clusterId, ErrorCode &err)
{
    if (vfsName == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[IO_FENCE] vfs name is null."));
        return DSTORE_FAIL;
    }
    if (pthread_rwlock_rdlock(&m_mutex) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Acquire pthread lock failed."));
        return DSTORE_FAIL;
    }

    RetStatus ret = DSTORE_FAIL;
    bool isClusterExist = false;
    for (uint32 slot = 0; slot < MAX_CLUSTER_COUNT; slot++) {
        if (!m_clusterActiveVFS[slot].isValid || m_clusterActiveVFS[slot].clusterId != clusterId) {
            continue;
        }
        isClusterExist = true;
        for (int i = 0; i < maxVfsNumPerInstance; i++) {
            if (m_clusterActiveVFS[slot].activeVFS[i].isValid &&
                (memcmp(m_clusterActiveVFS[slot].activeVFS[i].vfsName, vfsName, strlen(vfsName)) == 0)) {
                err = m_clusterActiveVFS[slot].activeVFS[i].drIoFenceTriggerErr.load(std::memory_order_acquire);
                ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
                    "[IO_FENCE] Get active vfs io fence err success, vfs name %s, cluster id %u.", vfsName, clusterId));
                ret = DSTORE_SUCC;
                break;
            }
        }
    }

    if (!isClusterExist) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[IO_FENCE] cluster id %u not exsit for active vfs %s, cannot get err.",
            clusterId, vfsName));
        (void)pthread_rwlock_unlock(&m_mutex);
        return ret;
    }

    if (ret == DSTORE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[IO_FENCE] vfs %s not exist for cluster id %u, cannot get err.",
            vfsName, clusterId));
    }
    (void)pthread_rwlock_unlock(&m_mutex);
    return ret;
}

void IoFencingVFSCollection::PrintIoFencingResult(const uint64 *nodeIds, uint32 nodeCnt, uint64 term, ErrorCode err,
    const VfsControlInfo *controlInfo, const int activeVFSCnt, uint32 clusterId) const
{
    ErrLevel errLevel = (err != ERROR_SYS_OK) ? DSTORE_ERROR : DSTORE_LOG;
    const uint32 maxSize = 30000;
    char *str = static_cast<char *>(malloc(maxSize));
    uint32 offset = 0;
    if (unlikely(str == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[IO_FENCE] Malloc memory failed when Print IoFencingResult, cluster id %u.", clusterId));
        return;
    }

    int rc = sprintf_s(str, maxSize,
        "[IO_FENCE] Set IO fencing white list by VfsControl vfsRet(%lld), cluster id %u, Term %lu, "
        "NodeList: ", err, clusterId, term);
    storage_securec_check_ss(rc);
    offset += static_cast<uint32>(rc);

    for (uint32 i = 0; i < nodeCnt; i++) {
        if (offset >= maxSize) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg(
                "[IO_FENCE] the value offset(%u) is invalid, cluster id %u.", clusterId, offset));
            return;
        }
        rc = sprintf_s(str + offset, maxSize - offset, " %llu", nodeIds[i]);
        storage_securec_check_ss(rc);
        offset += static_cast<uint64>(rc);
    }

    ErrLog(errLevel, MODULE_FRAMEWORK, ErrMsg("%s.", str));
    free(str);

    for (int i = 0; i < activeVFSCnt; i++) {
        if (controlInfo[i].retCode != ERROR_SYS_OK) {
            ErrLog(errLevel, MODULE_FRAMEWORK, ErrMsg(
                "[IO_FENCE] Io fencing vfs:%s, vfsRet(%lld), cluster id %u.",
                controlInfo[i].vfsName, err, clusterId));
        }
    }
}

RetStatus IoFencingVFSCollection::SetIoFencingWhiteList(const uint64 *nodeIds, const uint64 *nodeTimelineIds,
                                                        uint32 nodeCnt, uint64 term)
{
    RetStatus ret = DSTORE_SUCC;

    if (pthread_rwlock_rdlock(&m_mutex) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        (void)fprintf(stderr, "Acquire pthread lock failed.\n");
        return DSTORE_FAIL;
    }

    if (m_totalActiveVFSCnt == 0) {
        (void)pthread_rwlock_unlock(&m_mutex);
        return DSTORE_SUCC;
    }

    uint32 clusterId = INVALID_CLUSTER_ID; /* default means the vfs belongs to self cluster */
    /* 1: get cluster's active vfs. */
    ClusterActiveVFS* clusterActiveVFS = nullptr;
    for (uint32 i = 0; i < MAX_CLUSTER_COUNT; i++) {
        if (!m_clusterActiveVFS[i].isValid) {
            continue;
        }
        if (m_clusterActiveVFS[i].clusterId == clusterId) {
            clusterActiveVFS = &m_clusterActiveVFS[i];
            break;
        }
    }
    if (STORAGE_VAR_NULL(clusterActiveVFS)) {
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
               ErrMsg("[IO_FENCE] get cluster(%u) not exist.", clusterId));
        return DSTORE_SUCC;
    }
    if (clusterActiveVFS->activeVFSCnt == 0) {
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
               ErrMsg("[IO_FENCE] get cluster(%u)'s active vfs zero.", clusterId));
        return DSTORE_SUCC;
    }

    uint32 infoSize = sizeof(VfsControlInfo) * static_cast<unsigned int>(clusterActiveVFS->activeVFSCnt);
    VfsControlInfo *controlInfo = static_cast<VfsControlInfo *>(malloc(infoSize));
    if (controlInfo == nullptr) {
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[IO_FENCE] Malloc memory failed in IoFencingVFSCollection::SetIoFencingWhiteList"));
        return DSTORE_FAIL;
    }
    if (unlikely(memset_s(controlInfo, infoSize, 0, infoSize) != EOK)) {
        free(controlInfo);
        (void)pthread_rwlock_unlock(&m_mutex);
        return DSTORE_FAIL;
    }

    int index = 0;
    term += 1;
    for (int i = 0; i < maxVfsNumPerInstance; i++) {
        if (!clusterActiveVFS->activeVFS[i].isValid || !clusterActiveVFS->activeVFS[i].enableIoFencing) {
            continue;
        }
        if (index == clusterActiveVFS->activeVFSCnt) {
            free(controlInfo);
            (void)pthread_rwlock_unlock(&m_mutex);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("[IO_FENCE] The index of cluster's active vfs out of bounds."));
            return DSTORE_FAIL;
        }
        controlInfo[index].retCode = ERROR_SYS_OK;
        controlInfo[index].info.ioFence.clientIdCount = nodeCnt;
        controlInfo[index].info.ioFence.term = term;
        errno_t rc = strcpy_s(controlInfo[index].vfsName, sizeof(controlInfo[index].vfsName),
            clusterActiveVFS->activeVFS[i].vfsName);
        storage_securec_check(rc, "\0", "\0");
        rc = memcpy_s(controlInfo[index].info.ioFence.clientIds, sizeof(controlInfo[index].info.ioFence.clientIds),
                      nodeIds, sizeof(uint64) * nodeCnt);
        storage_securec_check(rc, "\0", "\0");
        rc = memcpy_s(controlInfo[index].info.ioFence.clientTimeLineIds,
                      sizeof(controlInfo[index].info.ioFence.clientTimeLineIds),
                      nodeTimelineIds, sizeof(uint64) * nodeCnt);
        storage_securec_check(rc, "\0", "\0");
        index++;
    }
    if (index != clusterActiveVFS->activeVFSCnt) {
        free(controlInfo);
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[IO_FENCE] Some vfs in the cluster are invalid or do not allow iofencing."));
        return DSTORE_FAIL;
    }
    int start = 0;
    ErrorCode err;
    VfsClientHandle *vfsClientHandle = GetDefaultVfsClientHandle();
    do {
        int send_ctl_cnt = VFS_CONTROL_MAX_COUNT;
        if (start + VFS_CONTROL_MAX_COUNT > clusterActiveVFS->activeVFSCnt) {
            send_ctl_cnt = clusterActiveVFS->activeVFSCnt - start;
        }
        err = ::VfsControl(vfsClientHandle, GetVfsTenantName(), SET_VFS_IO_FENCE_WHITELIST,
                           &controlInfo[start], static_cast<uint32_t>(send_ctl_cnt));
        if (err != ERROR_SYS_OK) {
            ret = DSTORE_FAIL;
            break;
        }
        start += VFS_CONTROL_MAX_COUNT;
    } while (start < clusterActiveVFS->activeVFSCnt);

    PrintIoFencingResult(nodeIds, nodeCnt, term, err, controlInfo, clusterActiveVFS->activeVFSCnt, clusterId);

    free(controlInfo);
    (void)pthread_rwlock_unlock(&m_mutex);
    return ret;
}

RetStatus IoFencingVFSCollection::SetDrIoFencingWhiteList(const uint64 *nodeIds, const uint64 *nodeTimelineIds,
                                                        uint32 nodeCnt, uint64 term, uint32 clusterId)
{
    RetStatus ret = DSTORE_SUCC;
    StorageReleasePanic((clusterId == INVALID_CLUSTER_ID), MODULE_FRAMEWORK,
        ErrMsg("[IO_FENCE] Set dr io fencing white list with illegal cluster id."));

    if (pthread_rwlock_rdlock(&m_mutex) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        (void)fprintf(stderr, "Acquire pthread lock failed.\n");
        return DSTORE_FAIL;
    }

    if (m_totalActiveVFSCnt == 0) {
        (void)pthread_rwlock_unlock(&m_mutex);
        return DSTORE_SUCC;
    }

    /* 1: get cluster's active vfs. */
    ClusterActiveVFS* clusterActiveVFS = nullptr;
    for (uint32 i = 0; i < MAX_CLUSTER_COUNT; i++) {
        if (!m_clusterActiveVFS[i].isValid) {
            continue;
        }
        if (m_clusterActiveVFS[i].clusterId == clusterId) {
            clusterActiveVFS = &m_clusterActiveVFS[i];
            break;
        }
    }
    if (STORAGE_VAR_NULL(clusterActiveVFS)) {
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
               ErrMsg("[IO_FENCE] get cluster(%u) not exist.", clusterId));
        return DSTORE_SUCC;
    }
    if (clusterActiveVFS->activeVFSCnt == 0) {
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
               ErrMsg("[IO_FENCE] get cluster(%u)'s active vfs zero.", clusterId));
        return DSTORE_SUCC;
    }

    uint32 infoSize = sizeof(VfsControlInfo) * static_cast<unsigned int>(clusterActiveVFS->activeVFSCnt);
    VfsControlInfo *controlInfo = static_cast<VfsControlInfo *>(malloc(infoSize));
    if (controlInfo == nullptr) {
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[IO_FENCE] Malloc memory failed in IoFencingVFSCollection::SetIoFencingWhiteList"));
        return DSTORE_FAIL;
    }
    if (unlikely(memset_s(controlInfo, infoSize, 0, infoSize) != EOK)) {
        free(controlInfo);
        (void)pthread_rwlock_unlock(&m_mutex);
        return DSTORE_FAIL;
    }

    int index = 0;
    TenantConfig *tenantCfg = g_storageInstance->GetGuc()->tenantConfig;
    if (STORAGE_VAR_NULL(tenantCfg)) {
        free(controlInfo);
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("Guc tenantConfig is nullptr!"));
        return DSTORE_FAIL;
    }

    VfsClientHandle *vfsClientHandle = GetVfsClientHandle(clusterId, false);
    if (vfsClientHandle == nullptr) {
        free(controlInfo);
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
            "[IO_FENCE] get existed vfs client handle failed, aid cluster id %u when set iofencing white list.",
            clusterId));
        return DSTORE_SUCC; /* dr vfs client not created, just return success */
    }

    term += 1;
    for (int i = 0; i < maxVfsNumPerInstance; i++) {
        if (!clusterActiveVFS->activeVFS[i].isValid || !clusterActiveVFS->activeVFS[i].enableIoFencing) {
            continue;
        }
        if (index == clusterActiveVFS->activeVFSCnt) {
            free(controlInfo);
            (void)pthread_rwlock_unlock(&m_mutex);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("[IO_FENCE] The index of cluster's active vfs out of bounds."));
            return DSTORE_FAIL;
        }

        controlInfo[index].retCode = ERROR_SYS_OK;
        controlInfo[index].info.ioFence.clientIdCount = nodeCnt;
        controlInfo[index].info.ioFence.term = term;
        errno_t rc = strcpy_s(controlInfo[index].vfsName, sizeof(controlInfo[index].vfsName),
            clusterActiveVFS->activeVFS[i].vfsName);
        storage_securec_check(rc, "\0", "\0");
        rc = memcpy_s(controlInfo[index].info.ioFence.clientIds, sizeof(controlInfo[index].info.ioFence.clientIds),
                      nodeIds, sizeof(uint64) * nodeCnt);
        storage_securec_check(rc, "\0", "\0");
        rc = memcpy_s(controlInfo[index].info.ioFence.clientTimeLineIds,
                      sizeof(controlInfo[index].info.ioFence.clientTimeLineIds),
                      nodeTimelineIds, sizeof(uint64) * nodeCnt);
        storage_securec_check(rc, "\0", "\0");
        /* The owner name is required in the cross-cluster scenario. */
        rc = strcpy_s(controlInfo[index].info.ioFence.ownerName, sizeof(controlInfo[index].info.ioFence.ownerName),
            tenantCfg->clusterName);
        storage_securec_check(rc, "\0", "\0");

        ErrorCode err = ::VfsControl(vfsClientHandle, GetVfsTenantName(), SET_VFS_IO_FENCE_WHITELIST,
            &controlInfo[index], 1);
         /*
          * set err for invoker.
          * dr vfs err not affect each other.
          */
        clusterActiveVFS->activeVFS[i].drIoFenceTriggerErr.store(err, std::memory_order_release);
        PrintIoFencingResult(nodeIds, nodeCnt, term, err, &controlInfo[index], 1, clusterId);

        index++;
    }
    if (index != clusterActiveVFS->activeVFSCnt) {
        free(controlInfo);
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[IO_FENCE] Some vfs in the cluster are invalid or do not allow iofencing."));
        return DSTORE_FAIL;
    }

    free(controlInfo);
    (void)pthread_rwlock_unlock(&m_mutex);
    return ret;
}

RetStatus IoFencingVFSCollection::SetIoFencingWhiteList(const char *vfsName, const uint64 *nodeIds,
                                                        const uint64 *nodeTimelineIds,  uint32 nodeCnt, uint64 term)
{
    RetStatus ret = DSTORE_SUCC;

    if (pthread_rwlock_rdlock(&m_mutex) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        (void)fprintf(stderr, "Acquire pthread lock failed.\n");
        return DSTORE_FAIL;
    }

    VfsControlInfo *controlInfo = static_cast<VfsControlInfo *>(malloc(sizeof(VfsControlInfo)));
    if (controlInfo == nullptr) {
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Malloc memory failed in IoFencingVFSCollection::SetIoFencingWhiteList"));
        return DSTORE_FAIL;
    }

    if (unlikely(memset_s(controlInfo, sizeof(VfsControlInfo), 0, sizeof(VfsControlInfo)) != EOK)) {
        free(controlInfo);
        (void)pthread_rwlock_unlock(&m_mutex);
        return DSTORE_FAIL;
    }

    controlInfo->retCode = ERROR_SYS_OK;
    controlInfo->info.ioFence.clientIdCount = 1;
    controlInfo->info.ioFence.term = term + 1;
    errno_t rc = strcpy_s(controlInfo->vfsName, sizeof(controlInfo->vfsName), vfsName);
    storage_securec_check(rc, "\0", "\0");
    rc = memcpy_s(controlInfo->info.ioFence.clientIds, sizeof(controlInfo->info.ioFence.clientIds), nodeIds,
                  sizeof(uint64) * nodeCnt);
    storage_securec_check(rc, "\0", "\0");
    rc = memcpy_s(controlInfo->info.ioFence.clientTimeLineIds, sizeof(controlInfo->info.ioFence.clientTimeLineIds),
                  nodeTimelineIds, sizeof(uint64) * nodeCnt);
    storage_securec_check(rc, "\0", "\0");
    ErrorCode err = ::VfsControl(GetDefaultVfsClientHandle(), GetVfsTenantName(), SET_VFS_IO_FENCE_WHITELIST,
        controlInfo, 1);
    PrintIoFencingResult(nodeIds, nodeCnt, term, err, controlInfo, 1);
    free(controlInfo);
    (void)pthread_rwlock_unlock(&m_mutex);
    return ret;
}

RetStatus IoFencingVFSCollection::GetIoFencingWhiteList(const char *vfsName, uint64 *nodeIds, uint32 *nodeCnt,
                                                        uint64 *term, uint32 clusterId) const
{
    VfsControlInfo controlInfo = {};

    errno_t rc = strcpy_s(controlInfo.vfsName, VFS_NAME_MAX_LEN, vfsName);
    storage_securec_check(rc, "\0", "\0");

    /* default means the vfs belongs to self cluster */
    VfsClientHandle *vfsClientHandle = GetDefaultVfsClientHandle();
    bool isSelfVfs = (clusterId == INVALID_CLUSTER_ID);
    if (!isSelfVfs) {
        TenantConfig *tenantCfg = g_storageInstance->GetGuc()->tenantConfig;
        if (STORAGE_VAR_NULL(tenantCfg)) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("Guc tenantConfig is nullptr!"));
            return DSTORE_FAIL;
        }
        vfsClientHandle = GetVfsClientHandle(clusterId, false);
        if (vfsClientHandle == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
                "[IO_FENCE] get existed vfs client handle failed, aid cluster id %u when get iofencing white list.",
                clusterId));
            return DSTORE_FAIL;
        }
        rc = strcpy_s(controlInfo.info.ioFence.ownerName, OWNER_NAME_MAX_LEN, tenantCfg->clusterName);
        storage_securec_check(rc, "\0", "\0");
    }

    ErrorCode err =
        ::VfsControl(vfsClientHandle, GetVfsTenantName(), GET_VFS_IO_FENCE_WHITELIST, &controlInfo, 1);
    if (err != ERROR_SYS_OK) {
        (void)fprintf(stderr, "Get vfs whitelist failure, vfsRet(%lld), cluster id(%u).\n", err, clusterId);
        return DSTORE_FAIL;
    }
    if (nodeCnt) {
        *nodeCnt = controlInfo.info.ioFence.clientIdCount;
    }
    *term = controlInfo.info.ioFence.term;
    if (nodeIds) {
        if (nodeCnt != nullptr && *nodeCnt == 0) {
            return DSTORE_SUCC;
        } else {
            size_t dataLen = sizeof(uint64) * controlInfo.info.ioFence.clientIdCount;
            error_t rcs = memcpy_s(nodeIds, dataLen, controlInfo.info.ioFence.clientIds, dataLen);
            storage_securec_check(rcs, "\0", "\0");
        }
    }
    return DSTORE_SUCC;
}

RetStatus IoFencingVFSCollection::GetIoFencingWhiteList(const char *vfsName, uint64 *nodeIds, uint32 *nodeCnt,
    uint64 *term, const char* ownerName) const
{
    StorageReleasePanic(ownerName == nullptr, MODULE_FRAMEWORK, ErrMsg("[IO_FENCE] ownerName is nullptr!"));
    VfsControlInfo controlInfo = {};

    errno_t rc = strcpy_s(controlInfo.vfsName, VFS_NAME_MAX_LEN, vfsName);
    storage_securec_check(rc, "\0", "\0");

    /* default means the vfs belongs to self cluster */
    VfsClientHandle *vfsClientHandle = GetDefaultVfsClientHandle();
    rc = strcpy_s(controlInfo.info.ioFence.ownerName, OWNER_NAME_MAX_LEN, ownerName);
    storage_securec_check(rc, "\0", "\0");

    ErrorCode err =
        ::VfsControl(vfsClientHandle, GetVfsTenantName(), GET_VFS_IO_FENCE_WHITELIST, &controlInfo, 1);
    if (err != ERROR_SYS_OK) {
        (void)fprintf(stderr, "Get vfs whitelist failure, vfsRet(%lld), ownerName(%s).\n", err, ownerName);
        return DSTORE_FAIL;
    }
    if (nodeCnt) {
        *nodeCnt = controlInfo.info.ioFence.clientIdCount;
    }
    *term = controlInfo.info.ioFence.term;
    if (nodeIds) {
        if (nodeCnt != nullptr && *nodeCnt == 0) {
            return DSTORE_SUCC;
        } else {
            size_t dataLen = sizeof(uint64) * controlInfo.info.ioFence.clientIdCount;
            error_t rcs = memcpy_s(nodeIds, dataLen, controlInfo.info.ioFence.clientIds, dataLen);
            storage_securec_check(rcs, "\0", "\0");
        }
    }
    return DSTORE_SUCC;
}

RetStatus IoFencingVFSCollection::SetIoFencingWhiteList(const char *vfsName)
{
    uint64 term = 0;
    if (unlikely(GetIoFencingWhiteList(vfsName, nullptr, nullptr, &term) != DSTORE_SUCC)) {
        storage_set_error(VFS_ERROR_INVALID_PARAMETER);
        (void)fprintf(stderr, "Invalid parameter for creating a VFS.\n");
        return DSTORE_FAIL;
    }
    ++term;
    uint64 vfsLibClientId = 0;
    uint64 clientTimelineId = NODE_TIMELINE_ID_DEFAULT;   // default value
    ::ErrorCode retError = GetVfsLibClientId(GetDefaultVfsClientHandle(), &vfsLibClientId);
    if (unlikely(retError)) {
        (void)fprintf(stderr, "Failed to get vfs lib client id, error code is %lld.\n", retError);
        return DSTORE_FAIL;
    }
    return SetIoFencingWhiteList(vfsName, &vfsLibClientId, &clientTimelineId, 1, term);
}

RetStatus IoFencingVFSCollection::ClearIoFencingWhiteList(const char *vfsName, const char *ownerName)
{
    StorageReleasePanic(vfsName == nullptr, MODULE_FRAMEWORK, ErrMsg("[IO_FENCE] vfsName is nullptr!"));
    StorageReleasePanic(ownerName == nullptr, MODULE_FRAMEWORK, ErrMsg("[IO_FENCE] ownerName is nullptr!"));
    if (pthread_rwlock_wrlock(&m_mutex) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        (void)fprintf(stderr, "Acquire pthread lock failed.\n");
        return DSTORE_FAIL;
    }

    VfsControlInfo controlInfo = {};
    VfsClientHandle *vfsClientHandle = GetDefaultVfsClientHandle();

    errno_t rc = strcpy_s(controlInfo.vfsName, VFS_NAME_MAX_LEN, vfsName);
    storage_securec_check(rc, "\0", "\0");
    controlInfo.retCode = ERROR_SYS_OK;
    controlInfo.info.ioFence.clientIdCount = 0;
    controlInfo.info.ioFence.term = DR_FENCING_CLEAR_TERM;
    rc = memset_s(controlInfo.info.ioFence.clientIds, sizeof(controlInfo.info.ioFence.clientIds), 0,
        sizeof(controlInfo.info.ioFence.clientIds));
    storage_securec_check(rc, "\0", "\0");
    rc = memset_s(controlInfo.info.ioFence.clientTimeLineIds, sizeof(controlInfo.info.ioFence.clientTimeLineIds),
        0, sizeof(controlInfo.info.ioFence.clientTimeLineIds));
    storage_securec_check(rc, "\0", "\0");
    rc = strcpy_s(controlInfo.info.ioFence.ownerName, OWNER_NAME_MAX_LEN, ownerName);
    storage_securec_check(rc, "\0", "\0");

    ErrorCode err =
        ::VfsControl(vfsClientHandle, GetVfsTenantName(), SET_VFS_IO_FENCE_WHITELIST, &controlInfo, 1);
    if (err != ERROR_SYS_OK) {
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[IO_FENCE] Clear vfs (%s) io fence white list failure, ownerName(%s), vfsRet (%lld).",
            vfsName, ownerName, err));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
        "[IO_FENCE] Clear vfs (%s), ownerName(%s) io fence white list success.", vfsName, ownerName));
    (void)pthread_rwlock_unlock(&m_mutex);
    return DSTORE_SUCC;
}

void IoFencingVFSCollection::SetTriggerIoFencingCallback(IoFencingVFSCollection::IoFencingCallback callback)
{
    m_ioFencingCallback = callback;
}

RetStatus IoFencingVFSCollection::TriggerIoFencing()
{
    if (m_ioFencingCallback == nullptr) {
        return DSTORE_SUCC;
    }
    static constexpr int maxRetryTime = 100;
    ::ErrorCode vfsRet = ERROR_SYS_OK;

    for (int i = 0; i < maxRetryTime; i++) {
        vfsRet = m_ioFencingCallback();
        if (vfsRet != ERROR_SYS_OK) {
            /* If someone else is locking the voting disk file, IoFencing may return an error,
             * so we retry and treat it as an unrecoverable failure if it still fails for a long time. */
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                   ErrMsg("Trigger io fencing failure %d times, vfsRet(%lld).", i, vfsRet));
        } else {
            break;
        }
    }

    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_IO_FENCING);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus IoFencingVFSCollection::AddIoFenceOwner(const char *vfsName, const char *ownerName)
{
    if (pthread_rwlock_wrlock(&m_mutex) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("[IO_FENCE] Acquire pthread lock failed, vfs name %s.", vfsName));
        return DSTORE_FAIL;
    }

    uint64 term = 0;
    if (unlikely(GetIoFenceOwners(vfsName, nullptr, nullptr, &term) != DSTORE_SUCC)) {
        (void)pthread_rwlock_unlock(&m_mutex);
        storage_set_error(VFS_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("[IO_FENCE] Invalid parameter for add io fencing owner, vfs name %s.", vfsName));
        return DSTORE_FAIL;
    }
    term += 1;

    VfsControlInfo *controlInfo = static_cast<VfsControlInfo *>(malloc(sizeof(VfsControlInfo)));
    if (controlInfo == nullptr) {
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("[IO_FENCE] Malloc memory failed in Add IoFenceOwner"));
        return DSTORE_FAIL;
    }

    if (unlikely(memset_s(controlInfo, sizeof(VfsControlInfo), 0, sizeof(VfsControlInfo)) != EOK)) {
        free(controlInfo);
        (void)pthread_rwlock_unlock(&m_mutex);
        return DSTORE_FAIL;
    }

    controlInfo->retCode = ERROR_SYS_OK;
    controlInfo->info.ioFenceOwner.ownerCount = 1;
    controlInfo->info.ioFenceOwner.term = term;
    errno_t rc = strcpy_s(controlInfo->vfsName, sizeof(controlInfo->vfsName), vfsName);
    storage_securec_check(rc, "\0", "\0");
    rc = strcpy_s(controlInfo->info.ioFenceOwner.ownerName[0], sizeof(controlInfo->info.ioFenceOwner.ownerName[0]),
        ownerName);
    storage_securec_check(rc, "\0", "\0");
    ErrorCode err = ::VfsControl(GetDefaultVfsClientHandle(), GetVfsTenantName(), ADD_VFS_IO_FENCE_OWNER,
        controlInfo, 1);
    free(controlInfo);
    (void)pthread_rwlock_unlock(&m_mutex);
    if (err != ERROR_SYS_OK) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,  ErrMsg(
            "[IO_FENCE] Fail to Add IoFenceOwner %s for vfs %s, term %lu. vfsRet(%lld).",
            ownerName, vfsName, term, err));
        return DSTORE_FAIL;
    } else {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
            "[IO_FENCE] Success to Add IoFenceOwner %s for vfs %s, term %lu.", ownerName, vfsName, term));
    }
    return DSTORE_SUCC;
}

RetStatus IoFencingVFSCollection::DelIoFenceOwner(const char *vfsName, const char *ownerName)
{
    if (pthread_rwlock_wrlock(&m_mutex) != EOK) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("[IO_FENCE] Acquire pthread lock failed, vfs name %s.", vfsName));
        return DSTORE_FAIL;
    }

    uint64 term = 0;
    if (unlikely(GetIoFenceOwners(vfsName, nullptr, nullptr, &term) != DSTORE_SUCC)) {
        (void)pthread_rwlock_unlock(&m_mutex);
        storage_set_error(VFS_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("[IO_FENCE] Invalid parameter for add io fencing owner, vfs name %s.", vfsName));
        return DSTORE_FAIL;
    }
    term += 1;

    VfsControlInfo *controlInfo = static_cast<VfsControlInfo *>(malloc(sizeof(VfsControlInfo)));
    if (controlInfo == nullptr) {
        (void)pthread_rwlock_unlock(&m_mutex);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[IO_FENCE] Malloc memory failed in Del IoFenceOwner"));
        return DSTORE_FAIL;
    }

    if (unlikely(memset_s(controlInfo, sizeof(VfsControlInfo), 0, sizeof(VfsControlInfo)) != EOK)) {
        free(controlInfo);
        (void)pthread_rwlock_unlock(&m_mutex);
        return DSTORE_FAIL;
    }

    controlInfo->retCode = ERROR_SYS_OK;
    controlInfo->info.ioFenceOwner.ownerCount = 1;
    controlInfo->info.ioFenceOwner.term = term;
    errno_t rc = strcpy_s(controlInfo->vfsName, sizeof(controlInfo->vfsName), vfsName);
    storage_securec_check(rc, "\0", "\0");
    rc = strcpy_s(controlInfo->info.ioFenceOwner.ownerName[0], sizeof(controlInfo->info.ioFenceOwner.ownerName[0]),
        ownerName);
    storage_securec_check(rc, "\0", "\0");
    ErrorCode err = ::VfsControl(GetDefaultVfsClientHandle(), GetVfsTenantName(), DEL_VFS_IO_FENCE_OWNER,
        controlInfo, 1);
    free(controlInfo);
    (void)pthread_rwlock_unlock(&m_mutex);
    if (err != ERROR_SYS_OK) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,  ErrMsg(
            "[IO_FENCE] Fail to Del IoFenceOwner %s for vfs %s, term %lu. vfsRet(%lld).",
            ownerName, vfsName, term, err));
        return DSTORE_FAIL;
    } else {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
            "[IO_FENCE] Success to Del IoFenceOwner %s for vfs %s, term %lu.", ownerName, vfsName, term));
    }
    return DSTORE_SUCC;
}

RetStatus IoFencingVFSCollection::GetIoFenceOwners(const char *vfsName, char (*ownerNames)[CLUSTER_NAME_MAX_LEN],
    uint32 *ownerCnt, uint64 *term)
{
    VfsControlInfo controlInfo = {};

    errno_t rc = strcpy_s(controlInfo.vfsName, VFS_NAME_MAX_LEN, vfsName);
    storage_securec_check(rc, "\0", "\0");

    ErrorCode err =
        ::VfsControl(GetDefaultVfsClientHandle(), GetVfsTenantName(), GET_VFS_IO_FENCE_OWNER, &controlInfo, 1);
    if (err != ERROR_SYS_OK) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("[IO_FENCE] Get vfs io fence owner failure, vfsRet(%lld).", err));
        return DSTORE_FAIL;
    }
    if (ownerCnt) {
        *ownerCnt = controlInfo.info.ioFenceOwner.ownerCount;
    }
    if (term) {
        *term = controlInfo.info.ioFenceOwner.term;
    }
    if (ownerNames) {
        if (controlInfo.info.ioFenceOwner.ownerCount == 0) {
            return DSTORE_SUCC;
        } else {
            for (uint32 i = 0; i < controlInfo.info.ioFenceOwner.ownerCount; i++) {
                ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
                    "[IO_FENCE] Get vfs io fence owner %s for vfs %s.", controlInfo.info.ioFenceOwner.ownerName[i],
                    vfsName));
                rc = strcpy_s(ownerNames[i], sizeof(ownerNames[i]), controlInfo.info.ioFenceOwner.ownerName[i]);
                storage_securec_check(rc, "\0", "\0");
            }
        }
    }
    return DSTORE_SUCC;
}

ErrorCode VFSAdapter::CreateSnapshotInVfs(const char *vfsName, const char *snapshotName, int64_t flags) const
{
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    if (STORAGE_VAR_NULL(tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to obtain the tenantConfig value."));
        return DSTORE_FAIL;
    }
    char *vfsTenantName = tenantConfig->tenantName;

    if (tenantConfig->storageConfig.type == StorageType::PAGESTORE) {
        if (STORAGE_VAR_NULL(vfsName)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("VFS name is nullptr."));
            return DSTORE_FAIL;
        }
        if (STORAGE_VAR_NULL(snapshotName)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Snapshot name is nullptr."));
            return DSTORE_FAIL;
        }

        ::ErrorCode retError =
            ::CreateSnapshot(GetDefaultVfsClientHandle(), vfsTenantName, vfsName, snapshotName, flags);
        if (retError != 0) {
            storage_set_error(VFS_ERROR_FAILED_TO_CREATE_SNAPSHOT);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to create VFS snapshot, error code = %lld", retError));
            return retError;
        }
        return DSTORE_SUCC;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::OpenSnapshotFile(const FileOpenParam &openPara, FileDescriptor **fd)
{
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    if (STORAGE_VAR_NULL(tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to obtain the tenantConfig value."));
        return DSTORE_FAIL;
    }
    if (openPara.preSnapshotName != nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Previous snapshot name of VFS is not nullptr."));
        return DSTORE_FAIL;
    }
    if (tenantConfig->storageConfig.type == StorageType::PAGESTORE) {
        if (openPara.snapshotName == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Snapshot name of VFS is nullptr."));
            return DSTORE_FAIL;
        }
        ::ErrorCode retError = ::OpenSnapshot(m_staticVfs, openPara, fd);
        if (retError) {
            storage_set_error(VFS_ERROR_FAILED_TO_OPEN_SNAPSHOT);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to open VFS snapshot from ps, fileName is %s,"
                          "error code = %lld",
                          openPara.filePath, retError));
            return DSTORE_FAIL;
        }
    } else {
        if (openPara.filePath == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("File path is nullptr."));
            return DSTORE_FAIL;
        }
        ::ErrorCode retError = ::Open(m_staticVfs, openPara.filePath, openPara.flags, fd);
        if (retError) {
            storage_set_error(VFS_ERROR_FAILED_TO_OPEN_SNAPSHOT);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to open VFS snapshot from local disk,"
                          "fileName is %s, error code = %lld",
                          openPara.filePath, retError));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

ErrorCode VFSAdapter::DropSnapshotInVfs(const char *vfsName, const char *snapshotName) const
{
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    if (STORAGE_VAR_NULL(tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to obtain the tenantConfig value."));
        return DSTORE_FAIL;
    }
    char *vfsTenantName = tenantConfig->tenantName;

    if (tenantConfig->storageConfig.type == StorageType::PAGESTORE) {
        if (snapshotName == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Snapshot name of VFS is nullptr."));
            return DSTORE_FAIL;
        }
        ::ErrorCode retError = ::DropSnapshot(GetDefaultVfsClientHandle(), vfsTenantName, vfsName, snapshotName);
        if (retError) {
            storage_set_error(VFS_ERROR_FAILED_TO_DROP_SNAPSHOT);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to drop VFS snapshot, error code = %lld", retError));
            return retError;
        }
        return DSTORE_SUCC;
    }
    return DSTORE_SUCC;
}

ErrorCode VFSAdapter::RollbackSnapshotInVfs(const char *vfsName, const char *snapshotName)
{
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    if (STORAGE_VAR_NULL(tenantConfig)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to obtain the tenantConfig value."));
        return DSTORE_FAIL;
    }
    char *vfsTenantName = tenantConfig->tenantName;

    if (tenantConfig->storageConfig.type == StorageType::PAGESTORE) {
        if (snapshotName == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Snapshot name of VFS is nullptr."));
            return DSTORE_FAIL;
        }
        ::ErrorCode retError = ::RollbackSnapshot(GetDefaultVfsClientHandle(), vfsTenantName, vfsName, snapshotName);
        if (retError) {
            storage_set_error(VFS_ERROR_FAILED_TO_ROLLBACK_SNAPSHOT);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to rollback VFS snapshot, error code = %lld", retError));
            return retError;
        }
        return DSTORE_SUCC;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::GetFileNameFromFileId(FileId fileId, char *fileName)
{
    if (fileId == INVALID_VFS_FILE_ID) {
        storage_set_error(VFS_ERROR_FAILED_TO_GET_FILENAME);
        return DSTORE_FAIL;
    }
    int rc = snprintf_s(fileName, MAXPGPATH, MAXPGPATH - 1, "%hu", fileId);
    storage_securec_check_ss(rc);
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::GetFileIdFromFileName(const char *fileName, FileId &fileId)
{
    if (STORAGE_VAR_NULL(fileName)) {
        storage_set_error(VFS_ERROR_INVALID_PARAMETER);
        return DSTORE_FAIL;
    }
    int val = std::stoi(fileName);
    if (val <= std::numeric_limits<uint16_t>::min() || val >= std::numeric_limits<uint16_t>::max()) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Get file id failed from file name."));
        return DSTORE_FAIL;
    }
    fileId = static_cast<FileId>(val);
    if (fileId == INVALID_VFS_FILE_ID) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

VFSAdapterFdHashEntry *VFSAdapter::GetFileHashEntry(const char *pathName)
{
    if (m_staticVfs == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("VFS is nullptr."));
        return nullptr;
    }
    if (STORAGE_VAR_NULL(pathName)) {
        storage_set_error(VFS_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("GetHashFileDescriptor[pdbId: %u]: pathName is empty", m_pdbId));
        return nullptr;
    }

    bool found = false;
    VFSAdapterFdHashEntry *entry = static_cast<VFSAdapterFdHashEntry *>(
        hash_search(m_fileDescHash, static_cast<const void *>(pathName), HASH_FIND, &found));
    if (unlikely(!found || entry == nullptr)) {
        storage_set_error(VFS_WARNING_HASH_ENTRY_FIND_FAILED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("GetHashFileDescriptor[pdbId: %u]: Find no entry in fileDesc hash, pathname: %s.", m_pdbId,
                      pathName));
        return nullptr;
    }
    if (entry->refCount <= 0 || entry->fileDesc == nullptr) {
        storage_set_error(VFS_WARNING_HASH_ENTRY_FIND_FAILED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("GetHashFileDescriptor[pdbId: %u]: Find no fd in fileDesc hash entry.", m_pdbId));
        return nullptr;
    }

    return entry;
}

RetStatus VFSAdapter::CreateFile(const char *pathName, const ::FileParameter filePara, FileDescriptor **fd)
{
    if (STORAGE_VAR_NULL(pathName)) {
        storage_set_error(VFS_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("CreateFile[pdbId: %u]: pathName is empty", m_pdbId));
        return DSTORE_FAIL;
    }
    LockHashPartition(pathName);

    /* Step1: Make sure the file does not physically exist in vfs. */
    if (FileExists(pathName)) {
        storage_set_error(VFS_ERROR_FILE_ALREADY_CREATED_OR_OPEN);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("CreateFile[pdbId: %u]: Creating file failed, filePath(%s) already exists.", m_pdbId, pathName));
        UnLockHashPartition(pathName);
        return DSTORE_FAIL;
    }

    /* Step2: Find file in fd hash. */
    if (GetFileHashEntry(pathName) != nullptr) {
        storage_set_error(VFS_ERROR_FILE_ALREADY_CREATED_OR_OPEN);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("CreateFile[pdbId: %u]: File(%s) has already been created or opened.", m_pdbId, pathName));
        UnLockHashPartition(pathName);
        return DSTORE_FAIL;
    }

    /* Step3: Physically creating file in vfs. */
    ::FileDescriptor *fileDesc;
    ::ErrorCode vfsRet = ::Create(m_staticVfs, pathName, filePara, &fileDesc);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_CREATE_FILE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("CreateFile[pdbId: %u]: Creating file in vfs failed, filePath(%s), vfsRet(%lld).", m_pdbId,
                      pathName, vfsRet));
        UnLockHashPartition(pathName);
        return DSTORE_FAIL;
    }

    /* Step4: Insert new entry into fd hash.
     * If the given fd is a nullptr, don't return opened fd and close it in vfs
     * If insert failed, rollback ::Create with ::Remove */
    if (fd == nullptr) {
        vfsRet = ::Close(fileDesc);
        if (vfsRet != 0) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("CreateFile[pdbId: %u]: File(%s) given handler ptr is null, closing file failed, vfsRet(%lld).",
                    m_pdbId, pathName, vfsRet));
            return DSTORE_FAIL;
        }
        UnLockHashPartition(pathName);
        return DSTORE_SUCC;
    }
    bool found = false;
    VFSAdapterFdHashEntry *entry = static_cast<VFSAdapterFdHashEntry *>(
        hash_search(m_fileDescHash, static_cast<const char *>(pathName), HASH_ENTER, &found));
    if (found || entry == nullptr) {
        storage_set_error(VFS_WARNING_HASH_ENTRY_FIND_FAILED);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("CreateFile[pdbId: %u]: File(%s) enter fd hash failed after file created.", m_pdbId, pathName));
        return DSTORE_FAIL;
    }
    errno_t rc = strcpy_s(entry->pathName, MAXPGPATH, pathName);
    storage_securec_check(rc, "\0", "\0");
    entry->refCount = 1;
    entry->fileDesc = fileDesc;
    *fd = fileDesc;

    UnLockHashPartition(pathName);
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::RemoveFile(const char *pathName)
{
    if (STORAGE_VAR_NULL(pathName)) {
        storage_set_error(VFS_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("RemoveFile[pdbId: %u]: pathName is empty", m_pdbId));
        return DSTORE_FAIL;
    }
    LockHashPartition(pathName);

    /* Make sure the file has been created. */
    if (!FileExists(pathName)) {
        storage_set_error(VFS_WARNING_FILE_NOT_EXISTS);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("RemoveFile[pdbId: %u]: The file(%s) does not exist in vfs.", m_pdbId, pathName));
        UnLockHashPartition(pathName);
        return DSTORE_FAIL;
    }

    VFSAdapterFdHashEntry *entry = GetFileHashEntry(pathName);
    if (entry != nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_CLOSED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("RemoveFile[pdbId: %u]: The file(%s) to be removed has not been closed, ref count: %u.", m_pdbId,
                      pathName, entry->refCount));
        UnLockHashPartition(pathName);
        return DSTORE_FAIL;
    }

    ::ErrorCode vfsRet = ::Remove(m_staticVfs, pathName);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_REMOVE_FILE);
        ErrLog(
            DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("RemoveFile[pdbId: %u]: Failed to remove the file(%s), vfsRet(%lld).", m_pdbId, pathName, vfsRet));
        UnLockHashPartition(pathName);
        return DSTORE_FAIL;
    }

    UnLockHashPartition(pathName);
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::CloseFile(FileDescriptor *fd)
{
    if (fd == nullptr) {
        storage_set_error(VFS_WARNING_FILE_DESCRIPTOR_EMPTY);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("CloseFile[pdbId: %u]: Writing file failed, fd is empty.", m_pdbId));
        return DSTORE_FAIL;
    }

    LockAllHashPartition();

    if (m_fileDescHash == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("VFSAdapter[pdbId: %u]: fileDesc hash table is nullptr.", m_pdbId));
        return DSTORE_FAIL;
    }
    HASH_SEQ_STATUS scan;
    hash_seq_init(&scan, m_fileDescHash);
    VFSAdapterFdHashEntry *entry;
    bool found = false;
    while ((entry = static_cast<VFSAdapterFdHashEntry *>(hash_seq_search(&scan))) != nullptr) {
        if (entry->fileDesc == fd) {
            found = true;
            break;
        }
    }
    if (entry == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("VFSAdapter[pdbId: %u]: fileDesc hash entry is nullptr.", m_pdbId));
        return DSTORE_FAIL;
    }
    if (!found) {
        storage_set_error(VFS_WARNING_FAILED_TO_CLOSE_FILE);
        ErrLog(
            DSTORE_WARNING, MODULE_FRAMEWORK,
            ErrMsg("CloseFile[pdbId: %u]: The file(%s) to be close does not exist in hash.", m_pdbId, entry->pathName));
        UnLockAllHashPartition();
        return DSTORE_FAIL;
    }

    if (entry->refCount > 1) {
        entry->refCount--;
        UnLockAllHashPartition();
        return DSTORE_SUCC;
    }

    ::ErrorCode vfsRet = ::Close(entry->fileDesc);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_CLOSE_FILE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("CloseFile[pdbId: %u]: Closing file(%s) in vfs failed, vfsRet(%lld)", m_pdbId, entry->pathName,
                      vfsRet));
        UnLockAllHashPartition();
        return DSTORE_FAIL;
    }

    (void)hash_search(m_fileDescHash, static_cast<const void *>(entry->pathName), HASH_REMOVE, nullptr);
    UnLockAllHashPartition();

    return DSTORE_SUCC;
}

RetStatus VFSAdapter::OpenFile(const char *pathName, int flags, FileDescriptor **fd)
{
    if (fd == nullptr) {
        storage_set_error(VFS_WARNING_FILE_DESCRIPTOR_EMPTY);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("Fsync[pdbId: %u]: Open file failed, fd is empty.", m_pdbId));
        return DSTORE_FAIL;
    }
    if (STORAGE_VAR_NULL(pathName)) {
        storage_set_error(VFS_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("OpenFile[pdbId: %u]: pathName is empty", m_pdbId));
        return DSTORE_FAIL;
    }
    LockHashPartition(pathName);

    /* Step1: If exist in fd hash, fetch and increase ref count. */
    VFSAdapterFdHashEntry *entry = GetFileHashEntry(pathName);
    if (entry != nullptr) {
        entry->refCount++;
        *fd = entry->fileDesc;
        UnLockHashPartition(pathName);
        return DSTORE_SUCC;
    }

    /* Step2: Physically open file in vfs. */
    ::FileDescriptor *fileDesc;
    ::ErrorCode vfsRet = ::Open(m_staticVfs, pathName, flags, &fileDesc);
    if (vfsRet != 0) {
        if (vfsRet == VFS_ERROR_FILE_NOT_EXIST) {
            storage_set_error(VFS_WARNING_FILE_NOT_EXISTS);
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                ErrMsg("OpenFile[pdbId: %u]: File(%s) does not exist, vfsRet(%lld)", m_pdbId, pathName, vfsRet));
        } else {
            storage_set_error(VFS_FATAL_FAILED_TO_OPEN_FILE);
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                ErrMsg("OpenFile[pdbId: %u]: Open file(%s) in vfs failed, vfsRet(%lld)", m_pdbId, pathName, vfsRet));
        }
        UnLockHashPartition(pathName);
        return DSTORE_FAIL;
    }

    /* Step3: Insert new entry into hash */
    bool found = false;
    entry = static_cast<VFSAdapterFdHashEntry *>(
        hash_search(m_fileDescHash, static_cast<const void *>(pathName), HASH_ENTER, &found));
    if (found || entry == nullptr) {
        storage_set_error(VFS_ERROR_HASH_ENTRY_INSERT_FAILED);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("OpenFile[pdbId: %u]: Insert entry failed, filePath(%s).", m_pdbId, pathName));
        return DSTORE_FAIL;
    }
    errno_t rc = strcpy_s(entry->pathName, MAXPGPATH, pathName);
    storage_securec_check(rc, "\0", "\0");
    entry->refCount = 1;
    entry->fileDesc = fileDesc;
    *fd = fileDesc;

    UnLockHashPartition(pathName);
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::RenameFile(const char *srcPathName, const char *destPathName)
{
    if (STORAGE_VAR_NULL(srcPathName)) {
        storage_set_error(VFS_ERROR_INVALID_PARAMETER);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("RenameFile[pdbId: %u]: srcPathName is empty", m_pdbId));
        return DSTORE_FAIL;
    }
    LockHashPartition(srcPathName);

    VFSAdapterFdHashEntry *entry = GetFileHashEntry(srcPathName);
    if (entry != nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_CLOSED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("RenameFile[pdbId: %u]: The file(%s) to be removed has not been closed, ref count: %u.", m_pdbId,
                      srcPathName, entry->refCount));
        UnLockHashPartition(srcPathName);
        return DSTORE_FAIL;
    }

    ::ErrorCode vfsRet = ::RenameFile(m_staticVfs, srcPathName, destPathName);
    if (unlikely(vfsRet != 0)) {
        storage_set_error(VFS_ERROR_FAILED_TO_RENAME);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("RenameFile[pdbId: %u]: Rename file failed, srcFile(%s), destFile(%s), vfsRet(%lld).", m_pdbId,
                      srcPathName, destPathName, vfsRet));
        UnLockHashPartition(srcPathName);
        return DSTORE_FAIL;
    }

    UnLockHashPartition(srcPathName);

    return DSTORE_SUCC;
}

int64 VFSAdapter::GetSize(FileDescriptor *fd)
{
    if (fd == nullptr) {
        storage_set_error(VFS_WARNING_FILE_DESCRIPTOR_EMPTY);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("GetSize[pdbId: %u]: Getting file size failed, fd is empty.", m_pdbId));
        return -1;
    }
    int64 fileSize;
    ::ErrorCode vfsRet = ::GetSize(fd, &fileSize);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_GET_SIZE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("GetSize[pdbId: %u]: Failed to get the file size, vfsRet(%lld).", m_pdbId, vfsRet));
        return -1;
    }
    return fileSize;
}

RetStatus VFSAdapter::Rewind(FileDescriptor *fd)
{
    if (fd == nullptr) {
        storage_set_error(VFS_WARNING_FILE_DESCRIPTOR_EMPTY);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("Rewind[pdbId: %u]: Rewind failed, fd is empty.", m_pdbId));
        return DSTORE_FAIL;
    }
    ::ErrorCode vfsRet = ::Rewind(fd);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_FSYNC);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Rewind[pdbId: %u]: Rewind failed, vfsRet(%lld).", m_pdbId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::Pread(FileDescriptor *fd, void *buf, uint64 count, int64 offset, int64 *readSize)
{
    if (fd == nullptr) {
        storage_set_error(VFS_WARNING_FILE_DESCRIPTOR_EMPTY);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("Pread[pdbId: %u]: Reading file failed, fd is empty.", m_pdbId));
        return DSTORE_FAIL;
    }

    ::ErrorCode vfsRet = ::Pread(fd, buf, count, offset, readSize);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_PREAD_FILE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Pread[pdbId: %u]: Pread failed, vfsRet(%lld).", m_pdbId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::Fsync(FileDescriptor *fd)
{
    if (fd == nullptr) {
        storage_set_error(VFS_WARNING_FILE_DESCRIPTOR_EMPTY);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("Fsync[pdbId: %u]: Fsync failed, fd is empty.", m_pdbId));
        return DSTORE_FAIL;
    }
    ::ErrorCode vfsRet = ::Fsync(fd);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_FSYNC);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Fsync[pdbId: %u]: FSync failed, vfsRet(%lld).", m_pdbId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::Truncate(FileDescriptor *fd, int64 length)
{
    if (fd == nullptr) {
        storage_set_error(VFS_WARNING_FILE_DESCRIPTOR_EMPTY);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("Truncate[pdbId: %u]: VFS Truncate failed, fd is empty.", m_pdbId));
        return DSTORE_FAIL;
    }
    ::ErrorCode vfsRet = ::Truncate(fd, length);
    if (vfsRet != 0) {
        storage_set_error(VFS_WARNING_FAILED_TO_TRUNCATE);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Truncate[pdbId: %u]: Truncate failed, vfsRet(%lld).", m_pdbId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::Extend(FileDescriptor *fd, int64 length)
{
    if (unlikely(fd == nullptr)) {
        storage_set_error(VFS_WARNING_FILE_DESCRIPTOR_EMPTY);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("Extend[pdbId: %u]: VFS Extend failed, fd is empty.", m_pdbId));
        return DSTORE_FAIL;
    }
    ::ErrorCode vfsRet = ::Extend(fd, length);
    if (unlikely(vfsRet != 0)) {
        storage_set_error(VFS_WARNING_FAILED_TO_EXTEND);
        ErrLog(GetSize(fd) >= length ? DSTORE_WARNING : DSTORE_PANIC, MODULE_FRAMEWORK,
            ErrMsg("Extend failure, vfsRet(%lld).", vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::PreadAsync(FileDescriptor *fd, void *buf, uint64_t count, int64_t offset,
                                 const AsyncIoContext *aioContext)
{
    if (fd == nullptr) {
        storage_set_error(VFS_WARNING_FILE_DESCRIPTOR_EMPTY);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("PreadAsync[pdbId: %u]: PreadAsync file failed, fd is empty.", m_pdbId));
        return DSTORE_FAIL;
    }

    ::ErrorCode vfsRet = ::PreadAsync(fd, buf, count, offset, aioContext);
    if (vfsRet != 0) {
        storage_set_error(VFS_ERROR_FAILED_TO_PREAD_ASYNC);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("PreadAsync[pdbId: %u]: PreadAsync failed, vfsRet(%lld).", m_pdbId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus VFSAdapter::PwriteSync(FileDescriptor *fd, const void *inBuffer, uint64 count, int64 offset)
{
    int64 writeSize;
    ::ErrorCode vfsRet;
    RetStatus ret = DSTORE_SUCC;
    uint32 retryCnt = 0;
    if (unlikely(fd == nullptr)) {
        storage_set_error(VFS_WARNING_FILE_NOT_OPENED);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("PwriteSync[pdbId: %u]: Writing file failed, fd is empty.", m_pdbId));
        return DSTORE_FAIL;
    }

    if (m_storeType != StorageType::PAGESTORE) {
        /* Local static linux library. */
        vfsRet = ::PwriteAsync(fd, inBuffer, count, offset, nullptr);
        if (unlikely(vfsRet != 0)) {
            ret = DSTORE_FAIL;
            storage_set_error(VFS_ERROR_FAILED_TO_PWRITE_ASYNC);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("PwriteAsync fail, vfs error %lld.", vfsRet));
        }
        return ret;
    }
RETRY:
    writeSize = 0;
    vfsRet = ::PwriteSync(fd, inBuffer, count, offset, &writeSize);
    if (likely(vfsRet == 0)) {
        if (likely(writeSize == static_cast<int64>(count))) {
            ErrLog(DSTORE_DEBUG2, MODULE_FRAMEWORK,
                   ErrMsg("PwriteSync success with writeSize %ld (expected size %lu) at write offset %ld", writeSize,
                          count, offset));
            ret = DSTORE_SUCC;
        } else {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                   ErrMsg("PwriteSync partially success with writeSize %ld (expected size %lu) at write offset %ld",
                          writeSize, count, offset));
            /* update write information and retry unwritten data */
            count -= static_cast<uint64>(writeSize);
            offset += writeSize;
            char *newBuffer = static_cast<char *>(const_cast<void *>(inBuffer)) + writeSize;
            inBuffer = const_cast<const void *>(static_cast<void *>(newBuffer));
            goto RETRY;
        }
    } else if (unlikely(vfsRet == VFS_ERROR_NEED_RETRY_AGAIN || vfsRet == VFS_ERROR_REQUEST_OVERLOAD)) {
        retryCnt++;
        if ((retryCnt % WARNING_LOG_INTEVAL_TIMES) == 0) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                   ErrMsg("PwriteSync get retry error %lld and will retry, now already retryCnt %u", vfsRet, retryCnt));
        }
        GaussUsleep(VFS_WRITE_RETRY_TIME_US);
        goto RETRY;
    } else if (unlikely(vfsRet == VFS_ERROR_IO_FENCING_REFUSE)) {
        STORAGE_PROCESS_FORCE_EXIT(
            true, "PwriteSync meets %lld(IO fencing refuse) so we will exit gaussdb process.", vfsRet);
    } else {
        storage_set_error(VFS_ERROR_FAILED_TO_PWRITE_SYNC);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("PwriteSync need panic for getting fatal vfs error %lld, already writeSize %ld at offset %ld"
                      " expected write size %lu.",
                      vfsRet, writeSize, offset, count));
        ret = DSTORE_FAIL;
    }
    return ret;
}

RetStatus VFSAdapter::PwriteAsync(FileDescriptor *fd, const void *buf, uint64 count, int64 offset,
                                  const AsyncIoContext *aioContext)
{
    uint32 retryCnt = 0;
    if (fd == nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_OPENED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("PwriteAsync[pdbId: %u]: Writing file failed, fd is empty.", m_pdbId));
        return DSTORE_FAIL;
    }

RETRY:
    ::ErrorCode vfsRet = ::PwriteAsync(fd, buf, count, offset, aioContext);
    if (likely(vfsRet == 0)) {
        return DSTORE_SUCC;
    }

    if (vfsRet == VFS_ERROR_AIO_WRITE_SUBMIT_FAIL || vfsRet == VFS_ERROR_REQUEST_OVERLOAD ||
        vfsRet == VFS_ERROR_NEED_RETRY_AGAIN) {
        /* aio queue reach max num, need retry and it is just useful at local adio but not pagestore aio */
        retryCnt++;
        if ((retryCnt % WARNING_LOG_INTEVAL_TIMES) == 0) {
            ErrLog(
                DSTORE_WARNING, MODULE_FRAMEWORK,
                ErrMsg("PwriteAsync get retry error %lld and will retry, now already retryCnt %u", vfsRet, retryCnt));
        }
        GaussUsleep(VFS_WRITE_RETRY_TIME_US);
        goto RETRY;
    }

    if (vfsRet != 0) {
        storage_set_error(VFS_ERROR_FAILED_TO_PWRITE_ASYNC);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("PwriteAsync[pdbId: %u]: Pwrite failed, vfsRet(%lld).", m_pdbId, vfsRet));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

/* if use local aio, need flush buffer 512Byte aligned and file open with FILE_ASYNC_IO_FLAG */
void VFSAdapter::OpenLocalAioIfNeed()
{
    if (!USE_VFS_LOCAL_AIO) {
        return;
    }
    if (m_hasOpenLocalAio) {
        ErrLog(DSTORE_DEBUG1, MODULE_FRAMEWORK, ErrMsg("pdbId %hhu has already opened local aio.", m_pdbId));
        return;
    }
    /* in logically, this vfs must be loaded and mounted and then open aio */
    StorageReleasePanic(m_storeType == StorageType::INVALID_TYPE, MODULE_FRAMEWORK, ErrMsg("Store Type is invalid."));
    StorageReleasePanic(m_staticVfs == nullptr, MODULE_FRAMEWORK, ErrMsg("VFS is nullptr."));
    if (m_storeType == StorageType::LOCAL || m_storeType == StorageType::TENANT_ISOLATION) {
        ::ErrorCode vfsRet = ::EnableAIO(m_staticVfs, AIO_MAX_REQNUM, AIO_CALLBACK_RECV_THREADNUM, nullptr,
                                         ThreadContext::DstoreThreadTryDestory);
        StorageReleasePanic((vfsRet != 0), MODULE_FRAMEWORK,
                            ErrMsg("pdbId %hhu open local aio failed,"
                                   "vfsRet(%lld).",
                                   m_pdbId, vfsRet));
        m_hasOpenLocalAio = true;
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("pdbId %hhu VFS succeed to open local aio.", m_pdbId));
    }
}
} /* namespace DSTORE */
