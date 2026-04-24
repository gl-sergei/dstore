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
 * dstore_csn.h
 *
 *
 * IDENTIFICATION
 *        storage/include/transaction/dstore_csn.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_CSN_MGR_H
#define DSTORE_DSTORE_CSN_MGR_H

#include <atomic>
#include "common/dstore_datatype.h"
#include "transaction/dstore_transaction_types.h"
#include "control/dstore_control_file.h"
#include "framework/dstore_instance_interface.h"
#include "framework/dstore_instance.h"
#include "transaction/dstore_tac_snapshot.h"

namespace DSTORE {

#ifdef UT
#define protected public
#endif

class alignas(DSTORE_CACHELINE_SIZE) CsnMgr : public BaseObject {
public:
    CsnMgr(ThreadCoreMgr *threadCoreMgr, ControlFile *controlFile, CommitSeqNo csnAssignmentIncrement);
    virtual ~CsnMgr() = default;

    DISALLOW_COPY_AND_MOVE(CsnMgr);

    RetStatus Initialize();
    virtual void Destroy();
    RetStatus PromoteInitialize();
    virtual RetStatus GetNextCsn(CommitSeqNo& outNextCsn, bool advance);
    virtual void UpdateLocalCsnMin();
    virtual CommitSeqNo GetBarrierMinCsn(PdbId pdbId);
    virtual CommitSeqNo GetRecycleCsnMin(PdbId pdbId);

    void SetLocalCsnMin(CommitSeqNo csn)
    {
        m_localCsnMin.store(csn, std::memory_order_release);
    }

    inline CommitSeqNo GetMaxReservedCsn()
    {
        return GsAtomicReadU64(&m_maxReservedCsn);
    }

    virtual char *DumpInfo();
    virtual CsnMode GetCsnMode() const
    {
        return CsnMode::DEFAULT;
    }
    virtual void DeductWaitTimeCallbackStart() {}
    virtual void DeductWaitTimeCallbackEnd() {}
    virtual void WaitUpperBoundSatisfy(UNUSE_PARAM CommitSeqNo commitCsn) {}
    inline CommitSeqNo GetFlashbackCsnMin() const
    {
        return m_flashbackCsnMin;
    }

    inline void SetFlashbackCsnMin(CommitSeqNo csn)
    {
        m_flashbackCsnMin = csn;
    }
    inline CommitSeqNo GetBackupRestoreCsnMin() const
    {
        return m_backupRestoreCsnMin;
    }

    inline void SetBackupRestoreCsnMin(CommitSeqNo csn)
    {
        m_backupRestoreCsnMin = csn;
    }
    inline void AddCsnToTacTracker(CommitSeqNo csn)
    {
        if (m_tacOrphanTrxTracker != nullptr) {
            m_tacOrphanTrxTracker->AddTacOrphanTrx(csn);
        }
    }

    inline void RefreshTacOrphanTrxExpiryTime()
    {
        if (m_tacOrphanTrxTracker != nullptr) {
            m_tacOrphanTrxTracker->RefreshOrphanTrxExpiryTime();
        }
    }

    RetStatus UpdateMaxReservedCsnIfNecessary(CommitSeqNo curCsn);

    virtual void inline SleepIfNoPendingCommitTrx(UNUSE_PARAM TimestampTz &startTimestamp) {};
    virtual void WakeUpCsnBroadcast() {};
    std::atomic<uint64> m_pendingCommitTrxCnt{0};

    inline CommitSeqNo GetLocalCsnMin() const
    {
        return m_localCsnMin.load(std::memory_order_acquire);
    }

    void SetLocalBarrierCsnMin(CommitSeqNo csn)
    {
        m_localBarrierCsnMin.store(csn, std::memory_order_release);
    }

    inline CommitSeqNo GetLocalBarrierCsnMin() const
    {
        return m_localBarrierCsnMin.load(std::memory_order_acquire);
    }

    void SetSendBarrierCsn(CommitSeqNo csn, PdbId pdbId)
    {
        m_sendBarrierCsn[pdbId - FIRST_USER_PDB_ID].store(csn, std::memory_order_release);
    }

    inline CommitSeqNo GetSendBarrierCsn(PdbId pdbId) const
    {
        return m_sendBarrierCsn[pdbId - FIRST_USER_PDB_ID].load(std::memory_order_acquire);
    }

protected:
    RetStatus UpdateMaxReservedCsn(CommitSeqNo newMaxReservedCsn);
    alignas(DSTORE_CACHELINE_SIZE) std::atomic<CommitSeqNo> m_localCsnMin;
    alignas(DSTORE_CACHELINE_SIZE) std::atomic<CommitSeqNo> m_flashbackCsnMin;
    alignas(DSTORE_CACHELINE_SIZE) std::atomic<CommitSeqNo> m_backupRestoreCsnMin;
    alignas(DSTORE_CACHELINE_SIZE) std::atomic<CommitSeqNo> m_localBarrierCsnMin;
    ThreadCoreMgr *m_threadCoreMgr = nullptr;
    ControlFile *m_controlFile = nullptr;
    CommitSeqNo m_csnAssignmentIncrement;
    std::atomic<CommitSeqNo> *m_sendBarrierCsn;
    LWLock m_maxReservedCsnLock;
    gs_atomic_uint64 m_nextCsn;
    gs_atomic_uint64 m_maxReservedCsn;
    LWLock m_updateLocalCsnMinLock;
    TacOrphanTrxTracker *m_tacOrphanTrxTracker;

    static const uint32 waitCsnUpdateTimeUs = 10;
};

}  // namespace DSTORE

#endif  // DSTORE_STORAGE_CSN_MGR_H
