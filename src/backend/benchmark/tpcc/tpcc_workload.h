//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/tpcc/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/benchmark_common.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/executor/abstract_executor.h"
#include "backend/storage/data_table.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/concurrency/transaction_scheduler.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace tpcc {

enum TxnType {
  TXN_TYPE_INVALID = 0,  // invalid plan node type
  TXN_TYPE_NEW_ORDER = 1,
  TXN_TYPE_PAYMENT = 2,
  TXN_TYPE_DELEVERY = 3,
  TXN_TYPE_STOCK = 4
};

//===========
// Column ids
//===========
// NEW_ORDER
#define COL_IDX_NO_O_ID 0
#define COL_IDX_NO_D_ID 1
#define COL_IDX_NO_W_ID 2
// ORDERS
#define COL_IDX_O_ID 0
#define COL_IDX_O_C_ID 1
#define COL_IDX_O_D_ID 2
#define COL_IDX_O_W_ID 3
#define COL_IDX_O_ENTRY_D 4
#define COL_IDX_O_CARRIER_ID 5
#define COL_IDX_O_OL_CNT 6
#define COL_IDX_O_ALL_LOCAL 7
// ORDER_LINE
#define COL_IDX_OL_O_ID 0
#define COL_IDX_OL_D_ID 1
#define COL_IDX_OL_W_ID 2
#define COL_IDX_OL_NUMBER 3
#define COL_IDX_OL_I_ID 4
#define COL_IDX_OL_SUPPLY_W_ID 5
#define COL_IDX_OL_DELIVERY_D 6
#define COL_IDX_OL_QUANTITY 7
#define COL_IDX_OL_AMOUNT 8
#define COL_IDX_OL_DIST_INFO 9
// Customer
#define COL_IDX_C_ID 0
#define COL_IDX_C_D_ID 1
#define COL_IDX_C_W_ID 2
#define COL_IDX_C_FIRST 3
#define COL_IDX_C_MIDDLE 4
#define COL_IDX_C_LAST 5
#define COL_IDX_C_STREET_1 6
#define COL_IDX_C_STREET_2 7
#define COL_IDX_C_CITY 8
#define COL_IDX_C_STATE 9
#define COL_IDX_C_ZIP 10
#define COL_IDX_C_PHONE 11
#define COL_IDX_C_SINCE 12
#define COL_IDX_C_CREDIT 13
#define COL_IDX_C_CREDIT_LIM 14
#define COL_IDX_C_DISCOUNT 15
#define COL_IDX_C_BALANCE 16
#define COL_IDX_C_YTD_PAYMENT 17
#define COL_IDX_C_PAYMENT_CNT 18
#define COL_IDX_C_DELIVERY_CNT 19
#define COL_IDX_C_DATA 20
// District
#define COL_IDX_D_ID 0
#define COL_IDX_D_W_ID 1
#define COL_IDX_D_NAME 2
#define COL_IDX_D_STREET_1 3
#define COL_IDX_D_STREET_2 4
#define COL_IDX_D_CITY 5
#define COL_IDX_D_STATE 6
#define COL_IDX_D_ZIP 7
#define COL_IDX_D_TAX 8
#define COL_IDX_D_YTD 9
#define COL_IDX_D_NEXT_O_ID 10
// Stock
#define COL_IDX_S_I_ID 0
#define COL_IDX_S_W_ID 1
#define COL_IDX_S_QUANTITY 2
#define COL_IDX_S_DIST_01 3
#define COL_IDX_S_DIST_02 4
#define COL_IDX_S_DIST_03 5
#define COL_IDX_S_DIST_04 6
#define COL_IDX_S_DIST_05 7
#define COL_IDX_S_DIST_06 8
#define COL_IDX_S_DIST_07 9
#define COL_IDX_S_DIST_08 10
#define COL_IDX_S_DIST_09 11
#define COL_IDX_S_DIST_10 12
#define COL_IDX_S_YTD 13
#define COL_IDX_S_ORDER_CNT 14
#define COL_IDX_S_REMOTE_CNT 15
#define COL_IDX_S_DATA 16

extern configuration state;

void RunWorkload();

/////////////////////////////////////////////////////////
// TRANSACTION TYPES
/////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////
struct NewOrderPlans {

  executor::IndexScanExecutor* item_index_scan_executor_;
  executor::IndexScanExecutor* warehouse_index_scan_executor_;
  executor::IndexScanExecutor* district_index_scan_executor_;
  executor::IndexScanExecutor* district_update_index_scan_executor_;
  executor::UpdateExecutor* district_update_executor_;
  executor::IndexScanExecutor* customer_index_scan_executor_;
  executor::IndexScanExecutor* stock_index_scan_executor_;
  executor::IndexScanExecutor* stock_update_index_scan_executor_;
  executor::UpdateExecutor* stock_update_executor_;

  void SetContext(executor::ExecutorContext* context) {
    item_index_scan_executor_->SetContext(context);

    warehouse_index_scan_executor_->SetContext(context);

    district_index_scan_executor_->SetContext(context);
    district_update_index_scan_executor_->SetContext(context);
    district_update_executor_->SetContext(context);

    customer_index_scan_executor_->SetContext(context);

    stock_index_scan_executor_->SetContext(context);
    stock_update_index_scan_executor_->SetContext(context);
    stock_update_executor_->SetContext(context);
  }

  void Cleanup() {
    delete item_index_scan_executor_;
    item_index_scan_executor_ = nullptr;

    delete warehouse_index_scan_executor_;
    warehouse_index_scan_executor_ = nullptr;

    delete district_index_scan_executor_;
    district_index_scan_executor_ = nullptr;

    delete district_update_index_scan_executor_;
    district_update_index_scan_executor_ = nullptr;

    delete district_update_executor_;
    district_update_executor_ = nullptr;

    delete customer_index_scan_executor_;
    customer_index_scan_executor_ = nullptr;

    delete stock_index_scan_executor_;
    stock_index_scan_executor_ = nullptr;

    delete stock_update_index_scan_executor_;
    stock_update_index_scan_executor_ = nullptr;

    delete stock_update_executor_;
    stock_update_executor_ = nullptr;
  }
};

class NewOrder : public concurrency::TransactionQuery {
 public:
  NewOrder()
      : item_index_scan_executor_(nullptr),
        warehouse_index_scan_executor_(nullptr),
        district_index_scan_executor_(nullptr),
        district_update_index_scan_executor_(nullptr),
        district_update_executor_(nullptr),
        customer_index_scan_executor_(nullptr),
        stock_index_scan_executor_(nullptr),
        stock_update_index_scan_executor_(nullptr),
        stock_update_executor_(nullptr),
        context_(nullptr),
        start_time_(std::chrono::system_clock::now()),
        first_pop_(true),
        warehouse_id_(0),
        district_id_(0),
        customer_id_(0),
        o_ol_cnt_(0),
        o_all_local_(true) {}

  ~NewOrder() {}

  void SetContext(executor::ExecutorContext* context) {
    item_index_scan_executor_->SetContext(context);

    warehouse_index_scan_executor_->SetContext(context);

    district_index_scan_executor_->SetContext(context);
    district_update_index_scan_executor_->SetContext(context);
    district_update_executor_->SetContext(context);

    customer_index_scan_executor_->SetContext(context);

    stock_index_scan_executor_->SetContext(context);
    stock_update_index_scan_executor_->SetContext(context);
    stock_update_executor_->SetContext(context);

    context_ = context;
  }

  void Cleanup() {

    // Note: context is set in RunNewOrder, and it is unique_prt
    // delete context_;
    // context_ = nullptr;

    delete item_index_scan_executor_;
    item_index_scan_executor_ = nullptr;

    delete warehouse_index_scan_executor_;
    warehouse_index_scan_executor_ = nullptr;

    delete district_index_scan_executor_;
    district_index_scan_executor_ = nullptr;

    delete district_update_index_scan_executor_;
    district_update_index_scan_executor_ = nullptr;

    delete district_update_executor_;
    district_update_executor_ = nullptr;

    delete customer_index_scan_executor_;
    customer_index_scan_executor_ = nullptr;

    delete stock_index_scan_executor_;
    stock_index_scan_executor_ = nullptr;

    delete stock_update_index_scan_executor_;
    stock_update_index_scan_executor_ = nullptr;

    delete stock_update_executor_;
    stock_update_executor_ = nullptr;
  }

  void ReSetStartTime() {
    if (first_pop_ == true) {
      start_time_ = std::chrono::system_clock::now();
      first_pop_ = false;
    }
  }
  std::chrono::system_clock::time_point& GetStartTime() {
    return start_time_;
  };

  virtual const std::vector<Value>& GetCompareKeys() const {
    return stock_update_index_scan_executor_->GetValues();
  }

  // Common method
  virtual TxnType GetTxnType() {
    return TXN_TYPE_NEW_ORDER;
  };
  virtual std::vector<uint64_t>& GetPrimaryKeysByint() { return primary_keys_; }
  // Common method
  virtual peloton::PlanNodeType GetPlanType() {
    return peloton::PLAN_NODE_TYPE_UPDATE;
  };

  // Make them public for convenience
 public:
  executor::IndexScanExecutor* item_index_scan_executor_;
  executor::IndexScanExecutor* warehouse_index_scan_executor_;
  executor::IndexScanExecutor* district_index_scan_executor_;
  executor::IndexScanExecutor* district_update_index_scan_executor_;
  executor::UpdateExecutor* district_update_executor_;
  executor::IndexScanExecutor* customer_index_scan_executor_;
  executor::IndexScanExecutor* stock_index_scan_executor_;
  executor::IndexScanExecutor* stock_update_index_scan_executor_;
  executor::UpdateExecutor* stock_update_executor_;
  executor::ExecutorContext* context_;

  std::chrono::system_clock::time_point start_time_;

  // Flag to compute the execution time
  bool first_pop_;

  // uint64_t primary_key_;
  std::vector<uint64_t> primary_keys_;

  // For execute
  int warehouse_id_;
  int district_id_;
  int customer_id_;
  int o_ol_cnt_;
  bool o_all_local_;
  std::vector<int> i_ids_, ol_w_ids_, ol_qtys_;
};

struct PaymentPlans {

  executor::IndexScanExecutor* customer_pindex_scan_executor_;
  executor::IndexScanExecutor* customer_index_scan_executor_;
  executor::IndexScanExecutor* customer_update_bc_index_scan_executor_;
  executor::UpdateExecutor* customer_update_bc_executor_;
  executor::IndexScanExecutor* customer_update_gc_index_scan_executor_;
  executor::UpdateExecutor* customer_update_gc_executor_;

  executor::IndexScanExecutor* warehouse_index_scan_executor_;
  executor::IndexScanExecutor* warehouse_update_index_scan_executor_;
  executor::UpdateExecutor* warehouse_update_executor_;

  executor::IndexScanExecutor* district_index_scan_executor_;
  executor::IndexScanExecutor* district_update_index_scan_executor_;
  executor::UpdateExecutor* district_update_executor_;

  void SetContext(executor::ExecutorContext* context) {
    customer_pindex_scan_executor_->SetContext(context);
    customer_index_scan_executor_->SetContext(context);
    customer_update_bc_index_scan_executor_->SetContext(context);
    customer_update_bc_executor_->SetContext(context);
    customer_update_gc_index_scan_executor_->SetContext(context);
    customer_update_gc_executor_->SetContext(context);

    warehouse_index_scan_executor_->SetContext(context);
    warehouse_update_index_scan_executor_->SetContext(context);
    warehouse_update_executor_->SetContext(context);

    district_index_scan_executor_->SetContext(context);
    district_update_index_scan_executor_->SetContext(context);
    district_update_executor_->SetContext(context);
  }

  void Cleanup() {
    delete customer_pindex_scan_executor_;
    customer_pindex_scan_executor_ = nullptr;
    delete customer_index_scan_executor_;
    customer_index_scan_executor_ = nullptr;
    delete customer_update_bc_index_scan_executor_;
    customer_update_bc_index_scan_executor_ = nullptr;
    delete customer_update_bc_executor_;
    customer_update_bc_executor_ = nullptr;
    delete customer_update_gc_index_scan_executor_;
    customer_update_gc_index_scan_executor_ = nullptr;
    delete customer_update_gc_executor_;
    customer_update_gc_executor_ = nullptr;

    delete warehouse_index_scan_executor_;
    warehouse_index_scan_executor_ = nullptr;
    delete warehouse_update_index_scan_executor_;
    warehouse_update_index_scan_executor_ = nullptr;
    delete warehouse_update_executor_;
    warehouse_update_executor_ = nullptr;

    delete district_index_scan_executor_;
    district_index_scan_executor_ = nullptr;
    delete district_update_index_scan_executor_;
    district_update_index_scan_executor_ = nullptr;
    delete district_update_executor_;
    district_update_executor_ = nullptr;
  }
};

struct DeliveryPlans {

  executor::IndexScanExecutor* new_order_index_scan_executor_;
  executor::IndexScanExecutor* new_order_delete_index_scan_executor_;
  executor::DeleteExecutor* new_order_delete_executor_;

  executor::IndexScanExecutor* orders_index_scan_executor_;
  executor::IndexScanExecutor* orders_update_index_scan_executor_;
  executor::UpdateExecutor* orders_update_executor_;

  executor::IndexScanExecutor* order_line_index_scan_executor_;
  executor::IndexScanExecutor* order_line_update_index_scan_executor_;
  executor::UpdateExecutor* order_line_update_executor_;

  executor::IndexScanExecutor* customer_index_scan_executor_;
  executor::UpdateExecutor* customer_update_executor_;

  void SetContext(executor::ExecutorContext* context) {
    new_order_index_scan_executor_->SetContext(context);
    new_order_delete_index_scan_executor_->SetContext(context);
    new_order_delete_executor_->SetContext(context);

    orders_index_scan_executor_->SetContext(context);
    orders_update_index_scan_executor_->SetContext(context);
    orders_update_executor_->SetContext(context);

    order_line_index_scan_executor_->SetContext(context);
    order_line_update_index_scan_executor_->SetContext(context);
    order_line_update_executor_->SetContext(context);

    customer_index_scan_executor_->SetContext(context);
    customer_update_executor_->SetContext(context);
  }

  void Cleanup() {
    delete new_order_index_scan_executor_;
    new_order_index_scan_executor_ = nullptr;
    delete new_order_delete_index_scan_executor_;
    new_order_delete_index_scan_executor_ = nullptr;
    delete new_order_delete_executor_;
    new_order_delete_executor_ = nullptr;

    delete orders_index_scan_executor_;
    orders_index_scan_executor_ = nullptr;
    delete orders_update_index_scan_executor_;
    orders_update_index_scan_executor_ = nullptr;
    delete orders_update_executor_;
    orders_update_executor_ = nullptr;

    delete order_line_index_scan_executor_;
    order_line_index_scan_executor_ = nullptr;
    delete order_line_update_index_scan_executor_;
    order_line_update_index_scan_executor_ = nullptr;
    delete order_line_update_executor_;
    order_line_update_executor_ = nullptr;

    delete customer_index_scan_executor_;
    customer_index_scan_executor_ = nullptr;
    delete customer_update_executor_;
    customer_update_executor_ = nullptr;
  }
};

NewOrderPlans PrepareNewOrderPlan();

PaymentPlans PreparePaymentPlan();

DeliveryPlans PrepareDeliveryPlan();

size_t GenerateWarehouseId(const size_t& thread_id);
size_t GenerateWarehouseId();

bool RunNewOrder(NewOrderPlans& new_order_plans, const size_t& thread_id);
bool RunNewOrder(NewOrder* new_order);
void SetNewOrder(NewOrder* new_order);

bool RunPayment(PaymentPlans& payment_plans, const size_t& thread_id);

bool RunDelivery(DeliveryPlans& delivery_plans, const size_t& thread_id);

bool RunOrderStatus(const size_t& thread_id);

bool RunStockLevel(const size_t& thread_id, const int& order_range);

/////////////////////////////////////////////////////////
void GenerateAndCacheQuery();
void EnqueueCachedUpdate();
NewOrder* GenerateNewOrder();

/////////////////////////////////////////////////////////

std::vector<std::vector<Value>> ExecuteReadTest(
    executor::AbstractExecutor* executor);

void ExecuteUpdateTest(executor::AbstractExecutor* executor);

void ExecuteDeleteTest(executor::AbstractExecutor* executor);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
