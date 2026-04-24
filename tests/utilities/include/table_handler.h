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

#ifndef TABLE_HANDLER_H
#define TABLE_HANDLER_H
#include <cstdint>
#include <atomic>
#include <map>
#include <string>
#include "securec.h"

#include "framework/dstore_instance_interface.h"
#include "tuple/dstore_memheap_tuple.h"
#include "table_operation_struct.h"
#include "heap/dstore_heap_interface.h"
#include "index/dstore_index_interface.h"
#include "systable/dstore_relation.h"

class CommonPersistentHandler;

static const int MAX_SIMULATED_TABLE_NUM = 256;
constexpr char *TABLE_METADATA_DIR = (char *)"metadata";

struct FakeRelationMetaData {
    uint32_t            colNum = 0;
    bool                isIndex = false;
    uint16_t            keyNum = 0;
    bool                isUnique = false;
    StorageRelationData relation;
    void Init(bool isIndex);
    void Init(uint32_t attrNum, const StorageRelation rel);
};
using DiskRelation = FakeRelationMetaData *;

class DstoreTableHandler {
public:
    DstoreTableHandler(StorageInstanceInterface *instance);
    virtual ~DstoreTableHandler();
    void Init(StorageRelation heapRel, StorageRelation indexRel);
    void Destory();
    DISALLOW_COPY_AND_MOVE(DstoreTableHandler);

    int RecoveryTable(const char* tableName);
    int CreateTable(const TableInfo& tableInfo);
    int CreateIndex(const TableInfo& indexTableInfo);

    StorageRelation GetTable(const char* tableName);
    StorageRelation GetIndex(const char* indexName);
    int CheckTable(const TableInfo& tableInfo);
    int CheckIndex(const TableInfo &indexTableInfo);
        
    int Insert(Datum *values, bool *isNulls, uint32_t *colIndex = nullptr);
    int Sum(uint32_t* colSeq, Datum* indexValues, uint32_t heapColSeq, double &sumNum, uint32_t indexColNum = -1);
    int Update(ItemPointerData *newCtid, uint32_t *colIndex = nullptr, Datum* values = nullptr, bool* isNulls = nullptr);
    int Scan(uint32_t *colSeq, Datum *values, HeapTuple **tuple, uint32_t indexColNum = -1);
    int LockTuple(uint32_t *colSeq, Datum *values, HeapTuple **tuple, uint32_t indexColNum = -1);
    int GetCount(uint32_t& cnt, uint32_t* colSeq, Datum* indexValues, uint32_t indexColNum = -1);

    int Delete(uint32_t* colSeq, Datum* indexValues, uint32_t indexColNum);
    int GetMin(uint32_t *colSeq, Datum *values, HeapTuple **tuple, uint32_t minColSeq, uint32_t indexColNum = -1);
    int GetMax(uint32_t *colSeq, Datum *indexValues, HeapTuple **tuple, uint32_t maxColSeq, uint32_t indexColNum);
    
    StorageRelation GetRelation() { return m_heapRel; }
    void DestroyRelation(StorageRelation relation);

public:
    char m_tableName[NAME_MAX_LEN];
    StorageInstanceInterface *m_instance;

    StorageRelation  m_heapRel;
    StorageRelation  m_indexRel;

    bool            m_isCreateByInit;
    bool m_isCreateBtreeByInit;
    SysClassTupDef *BuildSysRelationTuple(const TableInfo &tableInfo, Oid tablespaceOid, PageId segmentId);
    TupleDesc GenerateHeapTupDesc(Oid relOid, ColumnDesc colDef[], uint8_t colNum);
    TupleDesc GenerateIndexTupDesc(StorageRelation heapRel, Oid indexRelOid, int numKeyAttrs, const uint32_t *colIndex);
    TupleDesc GenerateTupDescTemplate(int natts);

    IndexBuildInfo CreateIndexMeta(const TableInfo &indexTableInfo);
    IndexBuildInfo ConstructBtreeBuilder(StorageRelation heapRel, const TableInfo &indexTableInfo);

private:
    void OpenDiskTableFile(CommonPersistentHandler *tablePersistentHander, const char *tableName, bool isIndex = false,
                           uint32_t colNum = 0);
    StorageRelation ReadDiskTable(CommonPersistentHandler *tablePersistentHander, const char *path);

    void AlignDiskTable(char *relPointer, DiskRelation diskRel, uint32_t colNum, bool isIndex);

    void FlushTable(const char *tableName, StorageRelation relation);
    int CheckTable(StorageRelation relation, const TableInfo &tableInfo);
    StorageRelation CopyRelation(StorageRelation rel);

    size_t GetPgClassRelSize() const;
    size_t GetTupleDescSize(uint32_t colNum) const;
    size_t GetAttrSize(uint32_t colNum) const;
    size_t GetIndexInfoSize(int attrNum) const;
    size_t GetPgIndexSize() const;
    size_t GetPersistenceTableSize(int colNum, bool isIndex) const;
    void InsertHeapTable(const char *tableName, StorageRelation relation);
    void InsertIndexTable(const char *tableName, StorageRelation relation);

    void GetScanKeyByTypeKeyCache(ScanKey scanKey, char *cachePointer);
    void GetScanFuncByType(ScanKey scanKey, Oid leftType, Oid rightType);
    static IndexInfo *CreateIndexInfo(int attrNum);
    static IndexInfo *BuildIndexInfo(StorageRelation idxRel, uint16_t keyNum, bool unique);
    ScanKey ConstructEqualScanKey(uint32_t indexColNum, Datum *indexValues);
    ScanKey ConstructNormalScanKey(uint32_t indexColNum);
    uint16_t GetIndexKeyNum() const;
};

struct StorageTableContext {
    StorageInstanceInterface *storageInstance;
    std::atomic<Oid> nextOid;
    std::map<std::string, StorageRelation> heapTable;
    std::map<std::string, StorageRelation> indexTable;
    
    StorageTableContext(StorageInstanceInterface *instance, Oid relStartOid)
        : storageInstance(instance), nextOid(relStartOid)
    {}

    void Destory() noexcept;

    StorageRelation GetHeapRelationEntry(const char *tableName)
    {
        if (heapTable.count(tableName) != 0) {
            return heapTable[tableName];
        }
        return nullptr;
    }

    StorageRelation GetIndexRelationEntry(const char *indexName)
    {
        if (indexTable.count(indexName) != 0) {
            return indexTable[indexName];
        }
        return nullptr;
    }

    void InsertHeapTable(const char *tableName, StorageRelation relation);
    void InsertIndexTable(const char *tableName, StorageRelation relation);
    DstoreTableHandler *GetTableHandler(const char *tableName, const char *indexName);
    inline Oid GetNextOid()
    {
        return nextOid.fetch_add(1);
    }

    inline Oid GetCurOid()
    {
        return nextOid.load();
    }
};

extern StorageInstanceInterface *g_instance;
extern StorageTableContext *simulator;
#endif