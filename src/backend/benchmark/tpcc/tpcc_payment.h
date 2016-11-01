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

class Payment : public concurrency::TransactionQuery {
 public:
  Payment()
      : customer_pindex_scan_executor_(nullptr),
        customer_index_scan_executor_(nullptr),
        customer_update_bc_index_scan_executor_(nullptr),
        customer_update_bc_executor_(nullptr),
        customer_update_gc_index_scan_executor_(nullptr),
        customer_update_gc_executor_(nullptr),
        warehouse_index_scan_executor_(nullptr),
        warehouse_update_index_scan_executor_(nullptr),
        warehouse_update_executor_(nullptr),
        district_index_scan_executor_(nullptr),
        district_update_index_scan_executor_(nullptr),
        district_update_executor_(nullptr),
        context_(nullptr),
        start_time_(std::chrono::system_clock::now()),
        exe_start_time_(std::chrono::system_clock::now()),
        first_pop_(true),
        first_pop_exe_time_(true),
        warehouse_id_(0),
        district_id_(0),
        customer_id_(0),
        h_amount_(0),
        customer_warehouse_id_(0),
        customer_district_id_(0),
        queue_(-1) {}

  ~Payment() {}

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

    context_ = context;
  }

  void Cleanup() {

    // Note: context is set in RunNewOrder, and it is unique_prt
    // delete context_;
    // context_ = nullptr;

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

  // Run txn
  virtual bool Run();

  virtual void SetStartTime(
      std::chrono::system_clock::time_point& delay_start_time) {
    if (first_pop_ == true) {
      start_time_ = delay_start_time;
      first_pop_ = false;
    }
  }
  std::chrono::system_clock::time_point& GetStartTime() {
    return start_time_;
  };

  virtual void SetExeStartTime(
      std::chrono::system_clock::time_point& delay_start_time) {
    if (first_pop_exe_time_ == true) {
      exe_start_time_ = delay_start_time;
      first_pop_exe_time_ = false;
    }
  }

  std::chrono::system_clock::time_point& GetExeStartTime() {
    return exe_start_time_;
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
  void SetRegionCover() {
    region_.SetCover(state.warehouse_count, state.item_count);
    region_.SetCoverWithDefault(warehouse_id_, state.item_count);
  }

  virtual SingleRegion& GetRegion() { return region_; }

  /*
   *    UPDATE WAREHOUSE SET * WHERE W_ID = ?", # h_amount, w_id
   *    UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
   *    UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
   */

  virtual void UpdateLogTableSingleRef(bool canonical) {
    // canonical means transform all D_W_ID and C_W_ID to W_ID
    if (canonical) {
      // UPDATE WAREHOUSE SET * WHERE W_ID = ?", # h_amount, w_id
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
    }
    // No canonical, use original ID
    else {
      // UPDATE WAREHOUSE SET * WHERE W_ID = ?", # h_amount, w_id
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
    }
  }

  virtual void UpdateLogTable(bool single_ref, bool canonical) {
    if (single_ref) {
      UpdateLogTableSingleRef(canonical);
      return;
    }

    if (canonical) {
      // UPDATE WAREHOUSE SET * WHERE W_ID = ?", # h_amount, w_id
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      return;
    }
    // No canonical, use original ID
    else {
      // UPDATE WAREHOUSE SET * WHERE W_ID = ?", # h_amount, w_id
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
    }
  }

  // Find out the max conflict condition and return the thread executing this
  // condition. If there are multiple threads executing this condition, choose
  // the thread who has the most of this condition
  virtual int LookupRunTableMaxSingleRef(bool canonical) {
    int max_conflict = CONFLICT_THRESHHOLD;
    std::string max_conflict_key;
    std::map<std::string, int> key_counter;

    if (canonical) {
      // UPDATE WAREHOUSE SET * WHERE W_ID = ?", # h_amount, w_id
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      int conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // Other SELECTs for WID
      for (int i = 0; i < 4; i++) {
        key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

        key_counter[key] += conflict;
        if (key_counter[key] > max_conflict) {
          max_conflict = key_counter[key];
          max_conflict_key = key;
        }
      }
    }
    // Not canonical, use original ID
    else {
      // UPDATE WAREHOUSE SET * WHERE W_ID = ?", # h_amount, w_id
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      int conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // TODO: other SELECTs
    }

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      // std::cout << "Can't find conflict" << std::endl;
      return -1;
    }

    // Now we get the key with max conflict, such as S_W_ID
    // Then we should lookup Run Table to get the thread who has this key
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
            max_conflict_key);

    int max_reference = RUNNING_REF_THRESHOLD;
    int queue_no = -1;

    // select max reference
    if (queue_info != nullptr) {
      std::vector<int> queues;

      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > max_reference) {
          // Get the queue No.
          queue_no = queue.first;
          max_reference = queue.second;

          // Once find out new max, clear vector
          queues.clear();
        } else if (queue.second != 0 && queue.second == max_reference) {
          queues.push_back(queue.first);
        }
      }

      if (queues.size() > 0) {
        int random_variable = std::rand() % queues.size();
        queue_no = queues.at(random_variable);
      }
    }

    return queue_no;
  }

  virtual int LookupRunTableMax(bool single_ref, bool canonical) {
    if (single_ref) {
      return LookupRunTableMaxSingleRef(canonical);
    }

    int max_conflict = CONFLICT_THRESHHOLD;
    std::string max_conflict_key;
    std::map<std::string, int> key_counter;

    if (canonical) {
      // UPDATE WAERHOUSE
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      int conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE DISTRICT
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // Other SELECTs for WID
      for (int i = 0; i < 4; i++) {
        key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

        key_counter[key] += conflict;
        if (key_counter[key] > max_conflict) {
          max_conflict = key_counter[key];
          max_conflict_key = key;
        }
      }
    }
    // No canonical, use origincal ID
    else {
      // UPDATE WAREHOUSE
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      int conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE DISTRICT
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // TODO: other SELECTs
    }

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      // std::cout << "Can't find conflict" << std::endl;
      return -1;
    }

    // Now we get the key with max conflict, such as S_W_ID
    // Then we should lookup Run Table to get the thread who has this key
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
            max_conflict_key);

    int max_reference = RUNNING_REF_THRESHOLD;
    int queue_no = -1;

    // select max reference
    if (queue_info != nullptr) {
      std::vector<int> queues;

      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > max_reference) {
          // Get the queue No.
          queue_no = queue.first;
          max_reference = queue.second;

          // Once find out new max, clear vector
          queues.clear();
        } else if (queue.second != 0 && queue.second == max_reference) {
          queues.push_back(queue.first);
        }
      }

      if (queues.size() > 0) {
        std::srand(unsigned(std::time(0)));
        int random_variable = std::rand() % queues.size();
        queue_no = queues.at(random_variable);
      }
    }

    return queue_no;
  }

  // Return a queue to schedule
  virtual int LookupRunTableSingleRef(bool canonical) {
    int queue_count =
        concurrency::TransactionScheduler::GetInstance().GetQueueCount();

    std::vector<int> queue_map(queue_count, 0);
    int max_conflict = CONFLICT_THRESHHOLD;
    int return_queue = -1;
    std::string key;

    /////////////////UPDATE WAREHOUSE//////////
    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
    } else {
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
    }

    // Get conflict from Log Table for the given condition
    double conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is no txn executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    /////////////////UPDATE DISTRICT//////////
    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
    } else {
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
    }

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    key = std::string("D_ID") + "-" + std::to_string(district_id_);
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
    } else {
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
    }
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    if (canonical) {
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
    } else {
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
    }
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    key = std::string("C_ID") + "-" + std::to_string(customer_id_);
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
    } else {
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
    }
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    if (canonical) {
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
    } else {
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
    }
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    key = std::string("C_ID") + "-" + std::to_string(customer_id_);
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }
    return return_queue;
  }

  virtual int LookupRunTable(bool single_ref, bool canonical) {
    if (single_ref) {
      return LookupRunTableSingleRef(canonical);
    }

    //////////////
    int queue_count =
        concurrency::TransactionScheduler::GetInstance().GetQueueCount();

    std::vector<int> queue_map(queue_count, 0);
    int max_conflict = CONFLICT_THRESHHOLD;
    int return_queue = -1;
    std::string key;

    //////////////////////UPDATE WAREHOUSE////////////////
    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
    } else {
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
    }
    // Get conflict from Log Table for the given condition
    double conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    /////////////////////UPDATE DISTRICT/////////////////
    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
    } else {
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
    }
    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
    } else {
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
    }
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);

    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
    } else {
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
    }
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);

    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }
    return return_queue;
  }

  // Increase each condition with the queue/thread. When a txn completes, it
  // will decrease the reference
  virtual void UpdateRunTableSingleRef(int queue_no, bool canonical) {
    if (canonical) {
      // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
    }
    // No canonical, use original ID
    else {
      // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
    }
  }

  virtual void UpdateRunTable(int queue_no, bool single_ref, bool canonical) {
    if (single_ref) {
      UpdateRunTableSingleRef(queue_no, canonical);
      return;
    }

    if (canonical) {
      // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
    } else {
      // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
    }
  }

  // Increase each condition with the queue/thread. When a txn completes, it
  // will decrease the reference
  virtual void DecreaseRunTableSingleRef(bool canonical) {
    int queue_no = GetQueueNo();

    if (canonical) {
      // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
    } else {
      // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
    }
  }

  virtual void DecreaseRunTable(bool single_ref, bool canonical) {
    /*
     *    UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
     *    UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
     *    UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
     *    UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
     */
    if (single_ref) {
      DecreaseRunTableSingleRef(canonical);
      return;
    }

    int queue_no = GetQueueNo();

    if (canonical) {
      // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
    } else {
      // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
    }
  }

  virtual bool ExistInRunTable(int queue) {

    //////////////////////////////////////////////////////////////////////
    // D_W_ID
    //////////////////////////////////////////////////////////////////////
    std::string key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);

    return concurrency::TransactionScheduler::GetInstance().ExistInRunTable(
        key, queue);
  }

  // For queue No.
  virtual void SetQueueNo(int queue_no) { queue_ = queue_no; }
  virtual int GetQueueNo() { return queue_; }

  ////////////////////////////////////////////////////////////////////////////
  // obsolete? conflict + success
  ////////////////////////////////////////////////////////////////////////////

  // Return a queue to schedule
  virtual int LookupRunTableFull(bool sigle_ref __attribute__((__unused__)),
                                 bool canonical __attribute__((__unused__))) {
    int queue_count =
        concurrency::TransactionScheduler::GetInstance().GetQueueCount();

    std::vector<double> queue_map(queue_count, 0);
    double max_conflict = 0;
    int return_queue = -1;

    std::string key = std::string("D_W_ID") + "-" +
                      std::to_string(warehouse_id_) + "-" +
                      std::string("D_ID") + "-" + std::to_string(district_id_);
    // Get conflict from Log Table for the given condition
    double conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);

    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    key = std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_) +
          "-" + std::string("C_D_ID") + "-" +
          std::to_string(customer_district_id_);
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    key = std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_) +
          "-" + std::string("C_ID") + "-" + std::to_string(customer_id_);
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_) +
          "-" + std::string("C_ID") + "-" + std::to_string(customer_id_);
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    if (queue_info != nullptr) {
      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > 0) {
          // Get the queue No.
          int queue_no = queue.first;

          // accumulate the conflict for this queue
          queue_map[queue_no] += conflict;

          // Get the latest conflict
          int queue_conflict = queue_map[queue_no];

          // Compare with the max, if current queue has larger conflict
          if (queue_conflict > max_conflict) {
            return_queue = queue_no;
            max_conflict = queue_conflict;
          }
        }
      }
    }

    return return_queue;
  }

  // Find out the max conflict condition and return the thread executing this
  // condition. If there are multiple threads executing this condPaymentition,
  // choose
  // the thread who has the most of this condition
  virtual int LookupRunTableMaxFullSingleRef(bool canonical) {
    double max_conflict = CONFLICT_THRESHHOLD;
    std::string max_conflict_key;
    std::map<std::string, double> key_counter;

    if (canonical) {
      //
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      double conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // Other SELECTs for WID
      for (int i = 0; i < 4; i++) {
        key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableFullGet(
                key);

        key_counter[key] += conflict;
        if (key_counter[key] > max_conflict) {
          max_conflict = key_counter[key];
          max_conflict_key = key;
        }
      }
    }
    // Not canonical, use original ID
    else {
      //
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      double conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // TODO: other SELECTs
    }

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      // std::cout << "Can't find conflict" << std::endl;
      return -1;
    }

    // Now we get the key with max conflict, such as S_W_ID
    // Then we should lookup Run Table to get the thread who has this key
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
            max_conflict_key);

    int max_reference = RUNNING_REF_THRESHOLD;
    int queue_no = -1;

    // select max reference
    if (queue_info != nullptr) {
      std::vector<int> queues;

      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > max_reference) {
          // Get the queue No.
          queue_no = queue.first;
          max_reference = queue.second;

          // Once find out new max, clear vector
          queues.clear();
        } else if (queue.second != 0 && queue.second == max_reference) {
          queues.push_back(queue.first);
        }
      }

      if (queues.size() > 0) {
        std::srand(unsigned(std::time(0)));
        int random_variable = std::rand() % queues.size();
        queue_no = queues.at(random_variable);
      }
    }

    return queue_no;
  }

  virtual int LookupRunTableMaxFull(bool single_ref, bool canonical) {
    if (single_ref) {
      return LookupRunTableMaxFullSingleRef(canonical);
    }

    double max_conflict = CONFLICT_THRESHHOLD;
    std::string max_conflict_key;
    std::map<std::string, double> key_counter;

    if (canonical) {
      // UPDATE WAERHOUSE
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      double conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE DISTRICT
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // Other SELECTs for WID
      for (int i = 0; i < 4; i++) {
        key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableFullGet(
                key);

        key_counter[key] += conflict;
        if (key_counter[key] > max_conflict) {
          max_conflict = key_counter[key];
          max_conflict_key = key;
        }
      }
    }
    // No canonical, use origincal ID
    else {
      // UPDATE WAREHOUSE
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      double conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE DISTRICT
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      key_counter[key] += conflict;
      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }

      // TODO: other SELECTs
    }

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      // std::cout << "Can't find conflict" << std::endl;
      return -1;
    }

    // Now we get the key with max conflict, such as S_W_ID
    // Then we should lookup Run Table to get the thread who has this key
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
            max_conflict_key);

    int max_reference = RUNNING_REF_THRESHOLD;
    int queue_no = -1;

    // select max reference
    if (queue_info != nullptr) {
      std::vector<int> queues;

      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > max_reference) {
          // Get the queue No.
          queue_no = queue.first;
          max_reference = queue.second;

          // Once find out new max, clear vector
          queues.clear();
        } else if (queue.second != 0 && queue.second == max_reference) {
          queues.push_back(queue.first);
        }
      }

      if (queues.size() > 0) {
        std::srand(unsigned(std::time(0)));
        int random_variable = std::rand() % queues.size();
        queue_no = queues.at(random_variable);
      }
    }

    return queue_no;
  }

  virtual void UpdateLogTableFullConflictSingleRef(bool canonical) {
    // canonical means transform all D_W_ID and C_W_ID to W_ID
    if (canonical) {
      //
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
    }
    // No canonical, use original ID
    else {
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
    }
  }

  // Increase the counter when conflict
  virtual void UpdateLogTableFullConflict(bool single_ref, bool canonical) {
    if (single_ref) {
      return UpdateLogTableFullConflictSingleRef(canonical);
    }

    if (canonical) {
      //
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      return;
    }
    // No canonical, use original ID
    else {
      //
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
    }
  }

  virtual void UpdateLogTableFullSuccessSingleRef(bool canonical) {
    // canonical means transform all D_W_ID and C_W_ID to W_ID
    if (canonical) {
      //
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
    }
    // No canonical, use original ID
    else {
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
    }
  }
  // Increase the counter when conflict
  virtual void UpdateLogTableFullSuccess(bool single_ref, bool canonical) {
    if (single_ref) {
      return UpdateLogTableFullSuccessSingleRef(canonical);
    }

    if (canonical) {
      //
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      return;
    }
    // No canonical, use original ID
    else {
      //
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
    }
  }
  //////////////////////end //////////////////////////////
  // Make them public for convenience
 public:
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

  executor::ExecutorContext* context_;

  std::chrono::system_clock::time_point start_time_;
  std::chrono::system_clock::time_point exe_start_time_;

  // Flag to compute the execution time
  bool first_pop_;
  bool first_pop_exe_time_;

  // uint64_t primary_key_;
  std::vector<uint64_t> primary_keys_;

  // For execute
  int warehouse_id_;
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
