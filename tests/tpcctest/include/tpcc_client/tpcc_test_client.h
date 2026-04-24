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

#ifndef TEST_DSTORE_TPCC_H
#define TEST_DSTORE_TPCC_H

#include <cstdint>
#include <vector>
#include <array>
#include <unordered_set>
#include <mutex>
#include <map>
#include <string>
#include "tpcc_table.h"

class DstoreTableHandler;
using namespace DSTORE;

class CommonPersistentHandler;

namespace TPCC_BENCHMARK {

enum CommandType : uint8_t { BUILD_DB, RUN_BENCH_MARK, RUN_CONSISTENCY_CHECK, RUN_ALL };

struct TpccConfig {
    uint32_t itemNumPerWareHouse;
    uint32_t customerPerDistrict;
    uint32_t orderPerDistrict;    
    uint32_t workThreadNum;
    uint32_t durationTime;
    CommandType cmdType;
    uint32_t wareHouseNum;
    /* bind warehouses to threads with no overlap */
    bool fixed;

    bool IsNotModify(const TpccConfig &cfg) const
    {
        return cfg.itemNumPerWareHouse == itemNumPerWareHouse && cfg.customerPerDistrict == customerPerDistrict &&
               cfg.orderPerDistrict == orderPerDistrict && cfg.workThreadNum == workThreadNum &&
               cfg.wareHouseNum == wareHouseNum && cfg.fixed == fixed;
    }
    bool operator==(const TpccConfig &cfg) const
    {
        return cfg.itemNumPerWareHouse == itemNumPerWareHouse && cfg.customerPerDistrict == customerPerDistrict &&
               cfg.orderPerDistrict == orderPerDistrict && cfg.workThreadNum == workThreadNum &&
               cfg.durationTime == durationTime && cfg.cmdType == cmdType && cfg.wareHouseNum == wareHouseNum &&
               cfg.fixed == fixed;
    }
    bool operator!=(const TpccConfig &cfg) const
    {
        return !(*this == cfg);
    }
};

enum TpccRunState: int8
{
    TPCC_RUN_STATE_UNKNOW = -1,
    TPCC_RUN_STATE_INIT = 0,
    TPCC_RUN_STATE_INIT_FINISHED,
    TPCC_RUN_STATE_CREATE_TABLE,
    TPCC_RUN_STATE_LOAD_DATA,
    TPCC_RUN_STATE_CREATE_INDEX,
    TPCC_RUN_STATE_EXECUTE,
    TPCC_RUN_STATE_END
};

struct WarehouseInterval {
    uint32_t startWhId = 0;
    uint32_t endWhId = 0;
    bool     isFinished = false;
};

struct ThreadRange {
    uint32_t startThreadId = 0;
    uint32_t endThreadId = 0;
};
constexpr int       NODE_MAX_COUNT = 10;
constexpr int       LOADATA_MAX_COUNT = 1000;
struct TpccRunStateMetaData {
    TpccRunState        state = TPCC_RUN_STATE_UNKNOW;
    Oid                 allocedMaxRelOid = 1; /* The maximum allocated Relation Oid. */
    bool                itemRelIsLoadFinished = false;
    uint32_t            heapCount = 0;
    char                heapTables[TABLE_MAX_CNT][NAME_MAX_LEN] = {0};
    uint32_t            indexCount = 0;
    char                indexTables[TABLE_MAX_CNT][NAME_MAX_LEN] = {0};
    uint32_t            loadDataSize = 0;
    uint32_t            nodeNum = 1;
    bool                initState[NODE_MAX_COUNT] = {0};
    WarehouseInterval   loadDatas[LOADATA_MAX_COUNT] = {0};
    ThreadRange         threadRange[NODE_MAX_COUNT] = {0};
    bool                startState[NODE_MAX_COUNT] = {0};
    bool                recoveryState[NODE_MAX_COUNT] = {0};
    bool                recoveryIndexState[NODE_MAX_COUNT] = {0};
    bool                loadDatasState[NODE_MAX_COUNT] = {0};
    bool                executeState[NODE_MAX_COUNT] = {0};
    bool                checkState[NODE_MAX_COUNT] = {0};
    bool                stopState[NODE_MAX_COUNT] = {0};

    void Init(uint32_t threadNum, uint32_t nodeCnt)
    {
        state = TPCC_RUN_STATE_UNKNOW;
        allocedMaxRelOid = 1;
        itemRelIsLoadFinished = false;
        heapCount = 0;
        errno_t rc = memset_s(heapTables, TABLE_MAX_CNT * NAME_MAX_LEN, 0, TABLE_MAX_CNT * NAME_MAX_LEN);
        storage_securec_check(rc, "\0", "\0");
        indexCount = 0;
        rc = memset_s(indexTables, TABLE_MAX_CNT * NAME_MAX_LEN, 0, TABLE_MAX_CNT * NAME_MAX_LEN);
        storage_securec_check(rc, "\0", "\0");
        loadDataSize = threadNum;
        nodeNum = nodeCnt;
        rc = memset_s(loadDatas, LOADATA_MAX_COUNT * sizeof(WarehouseInterval), 0,
            LOADATA_MAX_COUNT * sizeof(WarehouseInterval));
        storage_securec_check(rc, "\0", "\0");
        for (int i = 0; i < NODE_MAX_COUNT; ++i) {
            initState[i] = false;
            startState[i] = false;
            recoveryState[i] = false;
            recoveryIndexState[i] = false;
            loadDatasState[i] = false;
            executeState[i] = false;
            checkState[i] = false;
            stopState[i] = false;
            threadRange[i].startThreadId = 0;
            threadRange[i].endThreadId = 0;
        }
    }
    void ResetState()
    {
        for (int i = 0; i < NODE_MAX_COUNT; ++i) {
            initState[i] = false;
            startState[i] = false;
            recoveryState[i] = false;
            recoveryIndexState[i] = false;
            loadDatasState[i] = false;
            executeState[i] = false;
            checkState[i] = false;
            stopState[i] = false;
        }
    }
};

class TpccStorage;

using GenDataIntoTableFunc = void(TpccStorage::*)(uint32_t);

struct TpccRunStat;

enum TpccTransactionType: uint8
{
    NewOrderType,
    PaymentType,
    OrderStatusType,
    DeliveryType,
    StockLevelType,
    InvalidTrxType
};

/*
 * TPC-C benchmark on storage engine
 * */
class TpccStorage {
public:
    TpccStorage() = default;
    ~TpccStorage() ;

    TpccStorage(const TpccStorage &) = delete;
    TpccStorage &operator=(const TpccStorage &) = delete;
    TpccStorage(const TpccStorage &&) = delete;

    void Init(NodeId nodeId);
    bool CheckTpccConfigIsModify();
    void BackTpccConfigFile();
    bool RecoveryTables(CommonPersistentHandler *persistentHandler);
    bool RecoveryIndexs(CommonPersistentHandler *persistentHandler);
    bool CreateTables(CommonPersistentHandler *persistentHandler);
    /* Generate original data and insert into db for tpcc */
    void LoadData(CommonPersistentHandler *persistentHandler);
    bool CreateIndexs(CommonPersistentHandler *persistentHandler);
    bool CheckTables(CommonPersistentHandler* persistentHandler);
    bool CheckIndexs(CommonPersistentHandler *persistentHandler);

    void Execute(CommonPersistentHandler *persistentHandler);
    void End(CommonPersistentHandler *persistentHandler);

    TpccConfig GetTpccConfig() const {
        return m_tpccConfig;
    }
private:
    void LoadTpccConfig(const std::string& configMsg, const char* fileName, TpccConfig& tpccCfg);
    DstoreTableHandler *GetTableHandler(TableDesc *tableDesc, bool haveIndex = true);
    void ClearLocalTableHandler();
    
    /*
     * The five transactions of TPC-C
     * */
    void RunTransactions();
    bool NewOrderTransaction();
    bool PaymentTransaction();
    bool OrderStatusTransaction();
    bool DeliveryTransaction();
    bool StockLevelTransaction();

    bool SelectOldestNewOrder(int w_id, int d_id, Datum* values, bool* isNulls);
    bool DeleteOldestNewOrder(int w_id, int d_id, int oldestNewOrderId);
    bool UpdateOrderTable(int w_id, int d_id, int oldestNewOrderId, int o_carrier_id);
    bool UpdateOrderlineTable(int w_id, int d_id, int o_id, int o_ol_cnt, Timestamp ol_delivery_d);
    bool SelectNewestOOrder(int w_id, int d_id, Datum* values, bool* isNulls);
    bool SelectNewestNewOrder(int w_id, int d_id, Datum* values, bool* isNulls);
    bool GetDistrictOOrderCount(int w_id, int d_id, uint32_t& cnt);
    bool GetDistrictNewOrderCount(int w_id, int d_id, uint32_t& cnt);
    bool GetDistrictOrderLineCount(int w_id, int d_id, uint32_t& cnt);
    bool GetOrderOrderLineCount(int w_id, int d_id, int o_id, uint32_t& cnt);
    bool UpdateCustomerTable(int c_w_id, int c_d_id, int c_id, double h_amount);
    bool UpdateCustomerTableForDelivery(int w_id, int d_id, int c_id, double amount);
    bool SelectStockForUpdate(int w_id, int i_id, Datum* values, bool* isNulls);
    bool UpdateStockTable(int w_id, int i_id, int remoteCnt, int s_quantity, int s_ytd);
    bool SelectDistrictForUpdate(int w_id, int d_id, Datum* values, bool* isNulls);
    bool SelectDistrict(int w_id, int d_id, Datum* values, bool* isNulls);
    bool UpdateDistrictTable(int w_id, int d_id);
    bool UpdateDistrictTableForAmount(int w_id, int d_id, double h_amount);
    bool SelectWarehouseTable(int w_id, Datum* values, bool* isNulls);
    bool UpdateWarehouseTableForAmount(int w_id, double h_amount);
    bool SelectCustomer(int w_id, int c_id, int d_id, Datum* values, bool* isNulls);
    bool SelectOrderTable(int w_id, int d_id, int o_id, Datum *values, bool *isNulls);
    bool SelectNewOrderTable(int w_id, int d_id, int o_id, Datum *values, bool *isNulls);
    bool GetDistrictOrderlineCount(int c_id, int w_id, int d_id, uint32_t& cnt);
    bool SelectOrderlineTable(int c_id, int w_id, int d_id, int ol_number, Datum *values, bool *isNulls);
    bool SelectOrderlineTableForSum(int w_id, int d_id, int o_id, double& amountTotal);
    bool SelectLastOrderTable(int w_id, int d_id, Datum* values, bool* isNulls);
    bool SelectItem(int i_id, Datum* values, bool* isNulls);
    bool SelectStockTable(int s_w_id, int s_i_id, int& s_quantity);
    bool SelectOrderlineTableInRange(int ol_w_id, int ol_d_id, int rangeStart, int rangeEnd, std::unordered_set<int>& olItemIdSet);
    TpccTransactionType GetTransactionType(double r);

    /* Print the performance statistic */
    void PrintPerfState();
    void CalRunStat();
    /*
     * Generate original data and insert into tables for tpcc
     * */
    void GenDataIntoTable(uint32_t threadId, CommonPersistentHandler* persistentHandler);
    void GenDataIntoWarehouse(uint32_t whId);
    void GenDataIntoDistrict(uint32_t whId);
    void GenDataIntoItem(uint32_t whId);
    void GenDataIntoCustomer(uint32_t whId);
    void GenDataIntoHistroy(uint32_t whid, uint32_t d_id, uint32_t c_id);
    void GenDataIntoStock(uint32_t whId);
    void GenDataIntoOrder(uint32_t whId);
    void GenDataIntoOrderLine(uint32_t w_id, uint32_t d_id, uint32_t o_id, uint32_t o_ol_cnt);
    void InsertTable(TableNameType tableNameType, Datum *values, bool *nulls);
    GenDataIntoTableFunc GetGenDataFunc(TableNameType tableNameType);

    void GetSplitRange(uint32_t size, uint32_t seq, uint32_t &start, uint32_t &end);
    char *MakeNumberString (int x, int y);
    int RandomNumber (int min, int max);
    int NURand(unsigned A, unsigned x, unsigned y);
    int GetOtherWarehouseId(int w_id);
    void GenerateLastname(int num, char *name);

    void CheckConsistency(CommonPersistentHandler *persistentHandler);
    void CheckConsistency(uint32_t condition, __attribute__((__unused__)) uint32_t threadNum,
                          __attribute__((__unused__)) ThreadRange threadRange);
    void CheckCondition1(int wh_start, int wh_end);
    void CheckCondition2(int wh_start, int wh_end);
    void CheckCondition3(int wh_start, int wh_end);
    void CheckCondition4(int wh_start, int wh_end);
    void CheckCondition5(int wh_start, int wh_end);
    void CheckCondition6(int wh_start, int wh_end);
    void CheckCondition7(int wh_start, int wh_end);

    std::array<TableNameType, 6> m_needGenDataTables = {TABLE_WAREHOUSE, TABLE_DISTRICT, TABLE_ITEM,
                                                        TABLE_CUSTOMER,  TABLE_STOCK,    TABLE_ORDER};

    TpccConfig m_tpccConfig;
    TpccRunStat *m_stats;
    std::mutex      m_runStateMutex;
    NodeId          m_selfNodeId = 0;
};

} /* The end of TPCC_BENCHMARK */
#endif
