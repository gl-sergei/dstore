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
 * dstore_wal_file_manager.h
 *
 * Description:
 * Wal private header file, mainly focus on wal file management, and not supposed exposed to caller.
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_WAL_FILE_MANAGER_H
#define DSTORE_WAL_FILE_MANAGER_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include "port/posix_rwlock.h"
#include "dstore_wal_struct.h"
#include "control/dstore_control_file.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_vfs_adapter.h"
#include "dstore_wal_file_reader.h"

namespace DSTORE {
#ifdef UT
constexpr int DSTORE_WAL_FILE_OPEN_FLAG = FILE_READ_AND_WRITE_FLAG;
#else
constexpr int DSTORE_WAL_FILE_OPEN_FLAG = FILE_READ_AND_WRITE_FLAG | FILE_DIRECT_IO_FLAG | FILE_SYNC_FLAG;
#endif

struct WalFileInitPara {
    VFSAdapter *vfs;
    char *path;
    FileControlInfo flushCallBackInfo;
    FileControlInfo zeroCopyInfo;
    uint64 startPlsn;
    bool onlyForRead;
    bool diorw;
};

class WalFile : public BaseObject {
public:
    explicit WalFile(WalFileInitPara para);
    virtual ~WalFile();
    DISALLOW_COPY_AND_MOVE(WalFile)

    static uint64 GetFirstGroupStartPlsn(const WalFileHeaderData &walFileHeaderData);

    RetStatus CreateFile(const FileParameter &filePara);

    RetStatus PwriteAsync(const void *buf, uint64 count, int64 offset, const AsyncIoContext *aioContext);

    RetStatus PwriteSync(const void *buf, uint64 count, int64 offset);

    virtual RetStatus PreadAsync(void *buf, uint64_t count, int64_t offset, const AsyncIoContext *aioContext);

    RetStatus Read(void *buf, uint64 count, int64 offset, int64 *readSize);

    RetStatus Fsync();

    RetStatus Truncate(int64 length);

    RetStatus RenameFile(const char *destPathName);

    RetStatus RemoveFile();

    void Close() noexcept;

    void ResetState(uint64 startPlsn);

    /*
     * Get the startPlsn of this file.
     */
    uint64 GetStartPlsn();

    /*
    * Get the flushPlsn of this file.
    */
    uint64 GetFlushedPlsn();

    /*
     * Set the flushPlsn of this file.
     *
     * @param: flushPlsn: target flush plsn.
     */
    bool SetFlushedPlsn(uint64 flushPlsn);

    /*
     * Getter for m_maxValidDataOffset
     */
    uint64 GetMaxValidDataOffset();

    /*
     * Setter for m_maxValidDataOffset
     */
    void SetMaxValidDataOffset(uint64 validDataOffset, bool reset = false);

    /*
     * Get the fileDescriptor of  file.
     */
    inline ::FileDescriptor *GetFileDescriptor()
    {
        return m_fd;
    }

    char *GetFileName()
    {
        return m_path;
    }

    uint64 GetFirstWalGroupPlsn(DstoreMemoryContext memoryContext);

    WalFile *GetNext()
    {
        return m_next.load(std::memory_order_acquire);
    }

    void SetNext(WalFile *walFile)
    {
        m_next.store(walFile, std::memory_order_release);
    }

    void OpenFileIfNot();

    bool IsDioRw();
private:
    RetStatus ReadFileHeaderInfo(WalFileHeaderData *header, uint16 headerLen, DstoreMemoryContext memoryContext);

    char m_path[MAXPGPATH];
    VFSAdapter *m_vfs;
    uint64 m_startPlsn;
    uint64 m_flushedPlsn;
    uint64 m_maxValidDataOffset;
    FileControlInfo m_flushCallBackInfo;
    FileControlInfo m_zeroCopyInfo;
    std::atomic<WalFile *> m_next;
    ::FileDescriptor *m_fd;
    bool m_onlyForRead;
    bool m_diorw;
};

struct InitWalFilesPara {
    char dir[MAXPGPATH];          /* dir is root path for all wal files */
    VFSAdapter *vfs;
    PdbId pdbId;
    WalId walId;
    uint16 initWalFileCount;
    uint16 maxWalFileCount;
    uint16 maxFreeWalFileCount;
    uint64 walFileSize;
    uint64 startPlsn;
    bool onlyForRead;
    bool isForStandBy;
    FileParameter filePara;
    FileControlInfo flushCallBackInfo;
    FileControlInfo zeroCopyInfo;
};

struct GetWalFilesNamePara {
    PdbId pdbId;
    WalId walId;
    uint64 walFileSize;
    uint64 diskRecoveryPlsn;
    bool onlyAfter;
};

/*
 * Manager for all WalFiles, implement file create, delete, recycle, archive and so on.
 */
class WalFileManager : public BaseObject {
public:
    explicit WalFileManager(DstoreMemoryContext memoryContext);
    ~WalFileManager();
    DISALLOW_COPY_AND_MOVE(WalFileManager)

    void Destroy();

    RetStatus InitWalFiles(const InitWalFilesPara &para, bool dioRw);

    void CloseAllWalFiles();

    WalFile *GetWalFileByPlsn(uint64 plsn);

    bool CreateWalStreamFileObj(uint64 plsn, uint64 walFileSize, PdbId pdbId, WalId walId);

    WalFile *GetHeadWalFile();

    WalFile *GetTailWalFile();

    WalFile *GetNextWalFile(WalFile *walFile, bool needWait);

    bool WalFileWithPlsnRecyclable(uint64 plsn);

    void SetWritingFile(WalFile *walFile);

    void GetFileSharedLock();

    void GetFileUniqueLock();

    void ReleaseSharedFileLock();

    void ReleaseUniqueFileLock();

    void RecycleWalFileWorkerMain(bool isDropping = false);

    RetStatus RecycleWalFileForDropping();

    void StopRecycleWalFileWorker();

    void StartupRecycleWalFileWorker(bool isDropping = false);

    static char **GetWalFilesName(GetWalFilesNamePara para, uint32 &cnt, uint64 &firstStartPlsn);

    WalFile *GetWalFileByFileDescriptor(FileDescriptor *fd);

    /*
     * Pause backgroud archive thread for snapshot backup.
     */
    void PauseRecycle();

    /*
     * After snapshot backup finished, rerun backgroud archive thread.
     */
    void RerunRecycle();

    /* pause and resume wal file manager thread, resume indicates the end of a pause request */
    void RequestPauseWalFileManager();
    void RequestResumeWalFileManager();

    /* delete all wal files */
    RetStatus RemoveAllFiles();

    bool IsRecoveryRecycleFinish(bool *isStopRecycle = nullptr);
    bool GetRecoveryRecycleFinish() const
    {
        return m_recoveryRecycleFinish;
    }
    uint16 GetAllWalFileCount();

    inline void SetPauseWalFileRecycleFlag(bool flag)
    {
        m_pauseWalFileRecycle.store(flag, std::memory_order_release);
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Set pauseWalFileRecycleFlag to %s, pdbId: %u, walId: %lu.",
            flag ? "true" : "false", m_initWalFilesPara.pdbId, m_initWalFilesPara.walId));
    }

    inline bool GetPauseWalFileRecycleFlag()
    {
        return m_pauseWalFileRecycle.load(std::memory_order_relaxed);
    }
    inline bool GetWalFileIsDroppingFlag()
    {
        return m_walFileIsDropping.load(std::memory_order_relaxed);
    }

#ifdef UT
    bool GetPauseRecycleStart() const
    {
        return m_pauseRecycleStart.load(std::memory_order_relaxed);
    }

    bool GetPauseRecycleFinish() const
    {
        return m_pauseRecycleFinish.load(std::memory_order_relaxed);
    }

    bool GetWorkerRunning() const
    {
        return m_workerRunning;
    }
    uint32 GetNumPauses() const
    {
        return m_numPauseRequests;
    }
#endif

#ifndef UT
private:
#endif
    void CreateWalFiles(uint64 startPlsn, uint16 fileCount);
    void CreateNextWalFile();
    WalFile *CreateOnePendingWalFile(uint64 startPlsn);

    WalFile *CreateWalFileObject(uint64 startPlsn, char *fileName);

    RetStatus RecycleHeadFile();

    void RemoveHeadFile();

    void AppendWalFile(WalFile *walFile);

    uint64 GetRecyclablePlsn() const;

    bool WalFileRecyclable(WalFile *walFile);

    void PauseRecycleIfNeed();

    uint16 GetFreeWalFileCount();

    void PrintWalFilesInfo(bool isCreate, char *fileName);

    WalFile *m_headFile;
    std::atomic<WalFile *> m_writingFile;
    WalFile *m_tailFile;

    RWLock m_fileLock;
    InitWalFilesPara m_initWalFilesPara;

    std::thread *m_recycleThread;
    std::atomic_bool m_stopBgRecycleThread;
    std::atomic_bool m_pauseRecycleStart;
    std::atomic_bool m_pauseRecycleFinish;
    std::atomic_bool m_walFileIsDropping;

    DstoreMemoryContext m_memoryContext;

    bool m_workerRunning;
    volatile int m_numPauseRequests;
    std::mutex m_workerMtx;
    std::condition_variable m_wakeWorkerCv;
    std::condition_variable m_workerSleepCv;
    bool m_recoveryRecycleFinish;

    bool m_dioRw;
    std::atomic_bool m_pauseWalFileRecycle;
    uint64 m_pauseWalFileRecycleCnt;
};

}
#endif // STORAGE_WAL_FILE_MANAGER_H
