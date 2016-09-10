//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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
namespace benchmark {
namespace tpcc {

class Delivery : public concurrency::TransactionQuery {
 public:
  Delivery()
      : new_order_index_scan_executor_(nullptr),
        new_order_delete_index_scan_executor_(nullptr),
        new_order_delete_executor_(nullptr),
        orders_index_scan_executor_(nullptr),
        orders_update_index_scan_executor_(nullptr),
        orders_update_executor_(nullptr),
        order_line_index_scan_executor_(nullptr),
        order_line_update_index_scan_executor_(nullptr),
        order_line_update_executor_(nullptr),
        customer_index_scan_executor_(nullptr),
        customer_update_executor_(nullptr),
        context_(nullptr),
        start_time_(std::chrono::system_clock::now()),
        first_pop_(true),
        warehouse_id_(0),
        o_carrier_id_(0),
        district_id_(0),
        customer_id_(0),
        h_amount_(0),
        customer_warehouse_id_(0),
        customer_district_id_(0),
        queue_(-1) {}

  ~Delivery() {}

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

    context_ = context;
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

  // Run txn
  virtual bool Run();

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
    // Just make passing compile. Remove it later
    std::vector<Value>* tmp = new std::vector<Value>;
    Value a;
    tmp->push_back(a);
    return *tmp;
  }

  // Common method
  virtual TxnType GetTxnType() {
    return TXN_TYPE_PAYMENT;
  };

  virtual std::vector<uint64_t>& GetPrimaryKeysByint() { return primary_keys_; }
  virtual int GetPrimaryKey() { return warehouse_id_; }

  // Common method
  virtual peloton::PlanNodeType GetPlanType() {
    return peloton::PLAN_NODE_TYPE_UPDATE;
  };

  virtual SingleRegion* RegionTransform() { return new SingleRegion(); }

  // According predicate (WID AND IID), set the region cover(vector) for this
  // txn
  void SetRegionCover() {}

  virtual SingleRegion& GetRegion() { return region_; }

 public:
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
  executor::ExecutorContext* context_;

  std::chrono::system_clock::time_point start_time_;

  // Flag to compute the execution time
  bool first_pop_;

  // uint64_t primary_key_;
  std::vector<uint64_t> primary_keys_;

  // For execute
  int warehouse_id_;
  int o_carrier_id_;
  int district_id_;
  int customer_id_;

  double h_amount_;

  int customer_warehouse_id_;
  int customer_district_id_;
  std::string customer_lastname_;

  SingleRegion region_;

  // For queue No.
  int queue_;
};
}
}
}  // end namespace peloton
