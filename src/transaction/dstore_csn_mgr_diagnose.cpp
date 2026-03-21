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
 * dstore_csn_mgr_diagnose.cpp
 *
 * IDENTIFICATION
 *        src/transaction/dstore_csn_mgr_diagnose.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "diagnose/dstore_csn_mgr_diagnose.h"
#include "framework/dstore_instance.h"
#include "transaction/dstore_csn_mgr.h"
using namespace DSTORE;

CsnInfo *CsnMgrDiagnose::GetCsnInfo(uint32_t &csnInfoCnt)
{
    if (unlikely(g_storageInstance->GetType() == StorageInstanceType::DISTRIBUTE_MEMORY)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("memory node does not have transaction."));
        return nullptr;
    }
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    if (unlikely(!csnMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("CsnMgr is null."));
        StorageAssert(false);
        return nullptr;
    }
    csnInfoCnt = 1;
    CsnInfo *csnInfo = (CsnInfo *)DstorePalloc0(sizeof(CsnInfo) * csnInfoCnt);
    if (unlikely(!csnInfo)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Failed to allocate memory for CsnInfo."));
        StorageAssert(false);
        return nullptr;
    }
    csnInfo[0].selfNodeId = g_storageInstance->GetGuc()->selfNodeId;
    if (STORAGE_FUNC_FAIL(csnMgr->GetNextCsn(csnInfo[0].nextCsn, false))) {
        ErrLog(DSTORE_WARNING, MODULE_TRANSACTION, ErrMsg("Get nextcsn failed."));
        StorageAssert(false);
    }
    csnInfo[0].localCsnMin = csnMgr->GetLocalCsnMin();
    if (g_storageInstance->GetType() == StorageInstanceType::SINGLE) {
        return csnInfo;
    }
    return csnInfo;
}

void CsnMgrDiagnose::ReleaseCsnInfo(CsnInfo *csnInfo, uint32_t csnInfoCnt)
{
    if (csnInfo == nullptr) {
        return;
    }
    for (uint32_t i = 0; i < csnInfoCnt; ++i) {
        if (csnInfo[i].nodeList) {
            DstorePfreeExt(csnInfo[i].nodeList);
        }
    }
    DstorePfree(csnInfo);
}