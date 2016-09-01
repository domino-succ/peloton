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

#define PRELOAD 300000  // 2000,000
#define CLUSTER_ANALYSIS_TXN 10000
#define CONFLICT_THRESHHOLD 0
#define LOGTABLE "logtable"

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
        o_all_local_(true),
        queue_(-1) {}

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

  virtual void Cleanup() {

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

  // Run txn
  virtual bool Run();

  virtual void ReSetStartTime() {
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
  virtual int GetPrimaryKey() { return warehouse_id_; }
  // Common method
  virtual peloton::PlanNodeType GetPlanType() {
    return peloton::PLAN_NODE_TYPE_UPDATE;
  };

  // According the New-Order predicate, transform them into a region
  // New-Order predicate have two UPDATE types. In this experiment
  // we only consider one UPDATE (STOCK table update). It contains
  // two columns W_ID and I_ID. W_ID's range is from [1, state.warehouse_count]
  // and I_ID's range is from [1, state.item_count].
  // Note: this is new a Region which is different from the GetRegion();
  virtual SingleRegion* RegionTransform() {
    // Generate region and return
    // std::shared_ptr<Region> region(new Region(cover));
    return new SingleRegion(state.warehouse_count, ol_w_ids_, state.item_count,
                            i_ids_, o_all_local_, warehouse_id_);
  }

  // According predicate (WID AND IID), set the region cover(vector) for this
  // txn
  void SetRegionCover() {
    // Set region
    region_.SetCover(state.warehouse_count, ol_w_ids_, state.item_count,
                     i_ids_);
  }

  virtual SingleRegion& GetRegion() { return region_; }

  // Increase the counter when conflict
  virtual void UpdateLogTableSingleRef(bool canonical) {
    // Extract txn conditions include 4 conditions:
    // S_I_ID  S_W_ID D_ID D_W_ID
    // the corresponding values can be found from:
    // warehouse_id_; district_id_; i_ids_
    // For simplicity, the column name is hard coding here

    if (canonical) {
      // Extract D_W_ID and update it in Log Table
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);

      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // Extract D_ID and update it in Log Table
      key = std::string("D_ID") + "-" + std::to_string(district_id_);

      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // Extract S_W_ID and update it in Log Table
      for (auto wid : ol_w_ids_) {
        key = std::string("W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      }

      // Extract S_I_ID and update it in Log Table
      for (auto id : i_ids_) {
        key = std::string("I_ID") + "-" + std::to_string(id);
        concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      }
    } else {
      // Extract D_W_ID and update it in Log Table
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);

      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // Extract D_ID and update it in Log Table
      key = std::string("D_ID") + "-" + std::to_string(district_id_);

      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      // Extract S_W_ID and update it in Log Table
      for (auto wid : ol_w_ids_) {
        key = std::string("S_W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      }

      // Extract S_I_ID and update it in Log Table
      for (auto id : i_ids_) {
        key = std::string("S_I_ID") + "-" + std::to_string(id);
        concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      }
    }
  }

  /*
    UPDATE DISTRICT SET ** WHERE D_ID=? AND D_W_ID=?",#d_next_o_id, d_id, w_id
    UPDATE STOCK SET ** WHERE S_I_ID =? AND S_W_ID =?",#ol_i_id,ol_supply_w_id"
  */

  virtual void UpdateLogTable(bool single_ref, bool canonical) {
    if (single_ref) {
      UpdateLogTableSingleRef(canonical);
      return;
    }

    // Extract txn conditions include 4 conditions:
    // S_I_ID  S_W_ID D_ID D_W_ID
    // the corresponding values can be found from:
    // warehouse_id_; district_id_; i_ids_
    // For simplicity, the column name is hard coding here
    if (canonical) {
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
          std::string("D_ID") + "-" + std::to_string(district_id_);

      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
      for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
        int wid = ol_w_ids_[i];
        int iid = i_ids_[i];
        key = std::string("W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("I_ID") + "-" + std::to_string(iid);
        concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      }
    }
    // Use origincal ID
    else {
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
          std::string("D_ID") + "-" + std::to_string(district_id_);

      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

      PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
      for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
        int wid = ol_w_ids_[i];
        int iid = i_ids_[i];
        key = std::string("S_W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("S_I_ID") + "-" + std::to_string(iid);
        concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
      }
    }
  }

  // Find out the max conflict condition and return the thread executing this
  // condition. If there are multiple threads executing this condition, choose
  // the thread who has the most of this condition
  virtual int LookupRunTableMaxSingleRef(bool canonical) {
    int max_conflict = CONFLICT_THRESHHOLD;
    std::string max_conflict_key;
    std::string key;
    std::map<std::string, int> key_counter;

    //////////////////////////////////////////////////////////////////////
    // D_W_ID
    //////////////////////////////////////////////////////////////////////
    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
    } else {
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
    }

    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    key_counter[key] += conflict;

    if (key_counter[key] > max_conflict) {
      max_conflict = key_counter[key];
      max_conflict_key = key;
    }

    //////////////////////////////////////////////////////////////////////
    // D_ID
    //////////////////////////////////////////////////////////////////////
    // Extract D_ID and update it in Log Table

    key = std::string("D_ID") + "-" + std::to_string(district_id_);
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    key_counter[key] += conflict;

    if (key_counter[key] > max_conflict) {
      max_conflict = key_counter[key];
      max_conflict_key = key;
    }

    //////////////////////////////////////////////////////////////////////
    // S_W_ID
    //////////////////////////////////////////////////////////////////////
    // Extract S_W_ID and update it in Log Table
    for (auto wid : ol_w_ids_) {

      if (canonical) {
        key = std::string("W_ID") + "-" + std::to_string(wid);
      } else {
        key = std::string("S_W_ID") + "-" + std::to_string(wid);
      }

      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }
    }

    //////////////////////////////////////////////////////////////////////
    // S_I_ID
    //////////////////////////////////////////////////////////////////////
    // Extract S_I_ID and update it in Log Table
    for (auto id : i_ids_) {
      if (canonical) {
        key = std::string("I_ID") + "-" + std::to_string(id);
      } else {
        key = std::string("S_I_ID") + "-" + std::to_string(id);
      }

      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }
    }

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      std::cout << "Not find any conflict in Log Table" << std::endl;
      return -1;
    }

    // Now we get the key with max conflict, such as S_W_ID
    // Then we should lookup Run Table to get the thread who has this key
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGet(
            max_conflict_key);

    int max_reference = 8;
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

    //    if (queue_no == -1) {
    //      std::cout << "Max_conflict: " << max_conflict
    //                << " max_conflict_key: " << max_conflict_key << std::endl;
    //    }

    return queue_no;
  }

  virtual int LookupRunTableMax(bool single_ref, bool canonical) {
    if (single_ref) {
      return LookupRunTableMaxSingleRef(canonical);
    }

    // Begin pair ref
    int max_conflict = CONFLICT_THRESHHOLD;
    std::string max_conflict_key;
    std::string key;

    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
    } else {
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
    }

    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    if (conflict > max_conflict) {
      max_conflict = conflict;
      max_conflict_key = key;
    }

    PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
    for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
      int wid = ol_w_ids_[i];
      int iid = i_ids_[i];

      if (canonical) {
        key = std::string("W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("I_ID") + "-" + std::to_string(iid);
      } else {
        key = std::string("S_W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("S_I_ID") + "-" + std::to_string(iid);
      }

      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }
    }
    ////////new end here

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      // std::cout << "Can't find conflict" << std::endl;
      return -1;
    }

    // Now we get the key with max conflict, such as S_W_ID
    // Then we should lookup Run Table to get the thread who has this key
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGet(
            max_conflict_key);

    int max_reference = 0;
    int queue_no = -1;

    // randomly select
    //    if (queue_info != nullptr) {
    //      std::vector<int> queues;
    //
    //      for (auto queue : (*queue_info)) {
    //
    //        // reference = 0 means there is txn (of this condition) executing
    //        if (queue.second > 0) {
    //          queues.push_back(queue.first);
    //        }
    //      }
    // if (queues.size() > 0) {
    //      std::srand(unsigned(std::time(0)));
    //      int random_variable = std::rand() % queues.size();
    //      queue_no = queues.at(random_variable);
    // }
    //    }

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
    // Extract txn conditions include 4 conditions:
    // S_I_ID  S_W_ID D_ID D_W_ID
    // the corresponding values can be found from:
    // warehouse_id_; district_id_; i_ids_
    // For simplicity, the column name is hard coding here

    // Create a prepare queue map. This can be used to store the queue/thread
    // counter
    int queue_count =
        concurrency::TransactionScheduler::GetInstance().GetQueueCount();

    std::vector<int> queue_map(queue_count, 0);
    int max_conflict = CONFLICT_THRESHHOLD;
    int return_queue = -1;
    std::string key;

    //////////////////////////////////////////////////////////////////////
    // D_W_ID
    //////////////////////////////////////////////////////////////////////
    // Extract D_W_ID and update it in Log Table
    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
    } else {
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
    }

    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    // wid-3-->(3,100)(5,99)
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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

    //////////////////////////////////////////////////////////////////////
    // D_ID
    //////////////////////////////////////////////////////////////////////
    // Extract D_ID and update it in Log Table

    key = std::string("D_ID") + "-" + std::to_string(district_id_);

    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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

    //////////////////////////////////////////////////////////////////////
    // S_W_ID
    //////////////////////////////////////////////////////////////////////
    // Extract S_W_ID and update it in Log Table
    for (auto wid : ol_w_ids_) {

      if (canonical) {
        key = std::string("W_ID") + "-" + std::to_string(wid);
      } else {
        key = std::string("S_W_ID") + "-" + std::to_string(wid);
      }

      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

      queue_info =
          concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

      if (queue_info != nullptr) {
        for (auto queue : (*queue_info)) {

          // reference = 0 means there is txn (of this condition)
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
    }

    //////////////////////////////////////////////////////////////////////
    // S_I_ID
    //////////////////////////////////////////////////////////////////////
    // Extract S_I_ID and update it in Log Table
    for (auto id : i_ids_) {
      if (canonical) {
        key = std::string("I_ID") + "-" + std::to_string(id);
      } else {
        key = std::string("S_I_ID") + "-" + std::to_string(id);
      }

      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

      queue_info =
          concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

      if (queue_info != nullptr) {
        for (auto queue : (*queue_info)) {

          // reference = 0 means there is no txn (of this condition)
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
    }
    return return_queue;
  }
  virtual int LookupRunTable(bool single_ref, bool canonical) {
    if (single_ref) {
      return LookupRunTableSingleRef(canonical);
    }

    // Extract txn conditions include 4 conditions:
    // S_I_ID  S_W_ID D_ID D_W_ID
    // the corresponding values can be found from:
    // warehouse_id_; district_id_; i_ids_
    // For simplicity, the column name is hard coding here

    // Create a prepare queue map. This can be used to store the queue/thread
    // counter
    int queue_count =
        concurrency::TransactionScheduler::GetInstance().GetQueueCount();

    std::vector<int> queue_map(queue_count, 0);
    int max_conflict = CONFLICT_THRESHHOLD;
    int return_queue = -1;
    std::string key;

    //////////////////////////////////////////////////////////////////////
    // D_W_ID
    //////////////////////////////////////////////////////////////////////
    // Extract D_W_ID and update it in Log Table
    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
    } else {
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
    }

    // Get conflict from Log Table for the given condition
    double conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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

    //////////////////////////////////////////////////////////////////////
    // S_W_ID
    //////////////////////////////////////////////////////////////////////
    PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
    for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {

      int wid = ol_w_ids_[i];
      int iid = i_ids_[i];

      if (canonical) {
        key = std::string("W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("I_ID") + "-" + std::to_string(iid);
      } else {
        key = std::string("S_W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("S_I_ID") + "-" + std::to_string(iid);
      }

      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

      queue_info =
          concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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
    }
    ///////////////new end here

    return return_queue;
  }

  // Increase each condition with the queue/thread. When a txn completes, it
  // will decrease the reference
  virtual void UpdateRunTableSingleRef(int queue_no, bool canonical) {
    if (canonical) {
      //////////////////////////////////////////////////////////////////////
      // D_W_ID
      //////////////////////////////////////////////////////////////////////
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);

      // update run table
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // D_ID
      //////////////////////////////////////////////////////////////////////
      key = std::string("D_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::to_string(district_id_);

      // update run table
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // S_W_ID
      //////////////////////////////////////////////////////////////////////
      for (auto wid : ol_w_ids_) {
        key = std::string("W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
            key, queue_no);

        //        // Test
        //        if (wid != warehouse_id_) {
        //          std::cout << "wid != warehouse_id_: " << wid << "--" <<
        // warehouse_id_
        //                    << ". Assinged queue: " << queue_no << std::endl;
        //        }
        //        // end
      }

      //////////////////////////////////////////////////////////////////////
      // S_I_ID
      //////////////////////////////////////////////////////////////////////
      for (auto id : i_ids_) {
        key = std::string("I_ID") + "-" + std::to_string(id);
        concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
            key, queue_no);
      }

    } else {
      //////////////////////////////////////////////////////////////////////
      // D_W_ID
      //////////////////////////////////////////////////////////////////////
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);

      // update run table
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // D_ID
      //////////////////////////////////////////////////////////////////////
      key = std::string("D_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::to_string(district_id_);

      // update run table
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // S_W_ID
      //////////////////////////////////////////////////////////////////////
      for (auto wid : ol_w_ids_) {
        key = std::string("S_W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
            key, queue_no);
      }

      //////////////////////////////////////////////////////////////////////
      // S_I_ID
      //////////////////////////////////////////////////////////////////////
      for (auto id : i_ids_) {
        key = std::string("S_I_ID") + "-" + std::to_string(id);
        concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
            key, queue_no);
      }
    }
  }

  virtual void UpdateRunTable(int queue_no, bool single_ref, bool canonical) {
    if (single_ref) {
      UpdateRunTableSingleRef(queue_no, canonical);
      return;
    }

    if (canonical) {
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
          std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
      for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
        int wid = ol_w_ids_[i];
        int iid = i_ids_[i];
        key = std::string("W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("I_ID") + "-" + std::to_string(iid);
        concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
            key, queue_no);
      }
    } else {
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
          std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
      for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
        int wid = ol_w_ids_[i];
        int iid = i_ids_[i];
        key = std::string("S_W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("S_I_ID") + "-" + std::to_string(iid);
        concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
            key, queue_no);
      }
    }
  }

  // Increase each condition with the queue/thread. When a txn completes, it
  // will decrease the reference
  virtual void DecreaseRunTableSingleRef(bool canonical) {
    int queue_no = GetQueueNo();

    if (canonical) {
      //////////////////////////////////////////////////////////////////////
      // D_W_ID
      //////////////////////////////////////////////////////////////////////
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);

      // update run table
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // D_ID
      //////////////////////////////////////////////////////////////////////
      key = std::string("D_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::to_string(district_id_);

      // update run table
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // S_W_ID
      //////////////////////////////////////////////////////////////////////
      for (auto wid : ol_w_ids_) {
        key = std::string("W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
            key, queue_no);
      }

      //////////////////////////////////////////////////////////////////////
      // S_I_ID
      //////////////////////////////////////////////////////////////////////
      for (auto id : i_ids_) {
        key = std::string("I_ID") + "-" + std::to_string(id);
        concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
            key, queue_no);
      }
    } else {
      //////////////////////////////////////////////////////////////////////
      // D_W_ID
      //////////////////////////////////////////////////////////////////////
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);

      // update run table
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // D_ID
      //////////////////////////////////////////////////////////////////////
      key = std::string("D_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::to_string(district_id_);

      // update run table
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // S_W_ID
      //////////////////////////////////////////////////////////////////////
      for (auto wid : ol_w_ids_) {
        key = std::string("S_W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
            key, queue_no);
      }

      //////////////////////////////////////////////////////////////////////
      // S_I_ID
      //////////////////////////////////////////////////////////////////////
      for (auto id : i_ids_) {
        key = std::string("S_I_ID") + "-" + std::to_string(id);
        concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
            key, queue_no);
      }
    }
  }

  virtual void DecreaseRunTable(bool single_ref, bool canonical) {
    if (single_ref) {
      DecreaseRunTableSingleRef(canonical);
      return;
    }

    int queue_no = GetQueueNo();

    if (canonical) {
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
          std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
      for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
        int wid = ol_w_ids_[i];
        int iid = i_ids_[i];
        key = std::string("W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("I_ID") + "-" + std::to_string(iid);
        concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
            key, queue_no);
      }
    } else {
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
          std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);

      PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
      for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
        int wid = ol_w_ids_[i];
        int iid = i_ids_[i];
        key = std::string("S_W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("S_I_ID") + "-" + std::to_string(iid);
        concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
            key, queue_no);
      }
    }
  }

  //  bool IsQueueEmptySingleRef(int queue, bool canonical) {
  //    if (canonical) {
  //      //////////////////////////////////////////////////////////////////////
  //      // D_W_ID
  //      //////////////////////////////////////////////////////////////////////
  //      std::string key =
  //          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
  //
  //      concurrency::TransactionScheduler::GetInstance().RunTableLookup()
  //
  //    } else {
  //      //////////////////////////////////////////////////////////////////////
  //      // D_W_ID
  //      //////////////////////////////////////////////////////////////////////
  //      std::string key =
  //          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
  //
  //    }
  //  }
  //
  //  virtual bool IsQueueEmpty(int queue, bool single_ref, bool canonical) {
  //    if (single_ref) {
  //      return IsQueueEmptySingleRef(queue, canonical);
  //    }
  //    return false;
  //  }
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

  ///////////////////////////////////////////////////////////////////
  // obsolete :::: Full means conflict + success
  ///////////////////////////////////////////////////////////////////
  void UpdateLogTableFullConflictSingleRef(bool canonical) {
    if (canonical) {
      // Extract D_W_ID and update it in Log Table
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);

      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // Extract D_ID and update it in Log Table
      key = std::string("D_ID") + "-" + std::to_string(district_id_);

      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // Extract S_W_ID and update it in Log Table
      for (auto wid : ol_w_ids_) {
        key = std::string("W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance()
            .LogTableFullConflictIncrease(key);
      }

      // Extract S_I_ID and update it in Log Table
      for (auto id : i_ids_) {
        key = std::string("I_ID") + "-" + std::to_string(id);
        concurrency::TransactionScheduler::GetInstance()
            .LogTableFullConflictIncrease(key);
      }
    } else {
      // Extract txn conditions include 4 conditions:
      // S_I_ID  S_W_ID D_ID D_W_ID
      // the corresponding values can be found from:
      // warehouse_id_; district_id_; i_ids_
      // For simplicity, the column name is hard coding here

      // Extract D_W_ID and update it in Log Table
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);

      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // Extract D_ID and update it in Log Table
      key = std::string("D_ID") + "-" + std::to_string(district_id_);

      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      // Extract S_W_ID and update it in Log Table
      for (auto wid : ol_w_ids_) {
        key = std::string("S_W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance()
            .LogTableFullConflictIncrease(key);
      }

      // Extract S_I_ID and update it in Log Table
      for (auto id : i_ids_) {
        key = std::string("S_I_ID") + "-" + std::to_string(id);
        concurrency::TransactionScheduler::GetInstance()
            .LogTableFullConflictIncrease(key);
      }
    }
  }

  // Full means conflict + success
  virtual void UpdateLogTableFullConflict(bool single_ref, bool canonical) {
    if (single_ref) {
      return UpdateLogTableFullConflictSingleRef(canonical);
    }

    std::string key = std::string("D_W_ID") + "-" +
                      std::to_string(warehouse_id_) + "-" +
                      std::string("D_ID") + "-" + std::to_string(district_id_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullConflictIncrease(key);

    PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
    for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
      int wid = ol_w_ids_[i];
      int iid = i_ids_[i];
      key = std::string("S_W_ID") + "-" + std::to_string(wid) + "-" +
            std::string("S_I_ID") + "-" + std::to_string(iid);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
    }
  }

  void UpdateLogTableFullSuccessSingleRef(bool canonical) {
    if (canonical) {

      // Extract D_W_ID and update it in Log Table
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);

      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // Extract D_ID and update it in Log Table
      key = std::string("D_ID") + "-" + std::to_string(district_id_);

      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // Extract S_W_ID and update it in Log Table
      for (auto wid : ol_w_ids_) {
        key = std::string("W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance()
            .LogTableFullSuccessIncrease(key);
      }

      // Extract S_I_ID and update it in Log Table
      for (auto id : i_ids_) {
        key = std::string("I_ID") + "-" + std::to_string(id);
        concurrency::TransactionScheduler::GetInstance()
            .LogTableFullSuccessIncrease(key);
      }
    } else {
      // Extract txn conditions include 4 conditions:
      // S_I_ID  S_W_ID D_ID D_W_ID
      // the corresponding values can be found from:
      // warehouse_id_; district_id_; i_ids_
      // For simplicity, the column name is hard coding here

      // Extract D_W_ID and update it in Log Table
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);

      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // Extract D_ID and update it in Log Table
      key = std::string("D_ID") + "-" + std::to_string(district_id_);

      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      // Extract S_W_ID and update it in Log Table
      for (auto wid : ol_w_ids_) {
        key = std::string("S_W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance()
            .LogTableFullSuccessIncrease(key);
      }

      // Extract S_I_ID and update it in Log Table
      for (auto id : i_ids_) {
        key = std::string("S_I_ID") + "-" + std::to_string(id);
        concurrency::TransactionScheduler::GetInstance()
            .LogTableFullSuccessIncrease(key);
      }
    }
  }
  // Increase the counter when conflict
  virtual void UpdateLogTableFullSuccess(bool single_ref, bool canonical) {
    if (single_ref) {
      return UpdateLogTableFullSuccessSingleRef(canonical);
    }
    // Extract txn conditions include 4 conditions:
    // S_I_ID  S_W_ID D_ID D_W_ID
    // the corresponding values can be found from:
    // warehouse_id_; district_id_; i_ids_
    // For simplicity, the column name is hard coding here

    std::string key = std::string("D_W_ID") + "-" +
                      std::to_string(warehouse_id_) + "-" +
                      std::string("D_ID") + "-" + std::to_string(district_id_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullSuccessIncrease(key);

    PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
    for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
      int wid = ol_w_ids_[i];
      int iid = i_ids_[i];
      key = std::string("S_W_ID") + "-" + std::to_string(wid) + "-" +
            std::string("S_I_ID") + "-" + std::to_string(iid);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
    }
  }

  int LookupRunTableFullSingleRef(bool canonical) {
    // Extract txn conditions include 4 conditions:
    // S_I_ID  S_W_ID D_ID D_W_ID
    // the corresponding values can be found from:
    // warehouse_id_; district_id_; i_ids_
    // For simplicity, the column name is hard coding here

    // Create a prepare queue map. This can be used to store the queue/thread
    // counter
    int queue_count =
        concurrency::TransactionScheduler::GetInstance().GetQueueCount();

    std::vector<double> queue_map(queue_count, 0);
    double max_conflict = 0;
    int return_queue = -1;

    if (canonical) {
      //////////////////////////////////////////////////////////////////////
      // D_W_ID
      //////////////////////////////////////////////////////////////////////
      // Extract D_W_ID and update it in Log Table
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);

      // Get conflict from Log Table for the given condition
      double conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      // Get the queues from Run Table for the given condition.
      // Each queue: <queueNo. reference>
      std::unordered_map<int, int>* queue_info =
          concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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

      //////////////////////////////////////////////////////////////////////
      // D_ID
      //////////////////////////////////////////////////////////////////////
      // Extract D_ID and update it in Log Table

      key = std::string("D_ID") + "-" + std::to_string(district_id_);

      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      queue_info =
          concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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

      //////////////////////////////////////////////////////////////////////
      // S_W_ID
      //////////////////////////////////////////////////////////////////////
      // Extract S_W_ID and update it in Log Table
      for (auto wid : ol_w_ids_) {

        key = std::string("W_ID") + "-" + std::to_string(wid);

        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableFullGet(
                key);

        queue_info =
            concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

        if (queue_info != nullptr) {
          for (auto queue : (*queue_info)) {

            // reference = 0 means there is txn (of this condition)
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
      }

      //////////////////////////////////////////////////////////////////////
      // S_I_ID
      //////////////////////////////////////////////////////////////////////
      // Extract S_I_ID and update it in Log Table
      for (auto id : i_ids_) {
        key = std::string("I_ID") + "-" + std::to_string(id);

        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableFullGet(
                key);

        queue_info =
            concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

        if (queue_info != nullptr) {
          for (auto queue : (*queue_info)) {

            // reference = 0 means there is txn (of this condition)
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
      }
    }
    // If not canonical
    else {
      //////////////////////////////////////////////////////////////////////
      // D_W_ID
      //////////////////////////////////////////////////////////////////////
      // Extract D_W_ID and update it in Log Table
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);

      // Get conflict from Log Table for the given condition
      double conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      // Get the queues from Run Table for the given condition.
      // Each queue: <queueNo. reference>
      std::unordered_map<int, int>* queue_info =
          concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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

      //////////////////////////////////////////////////////////////////////
      // D_ID
      //////////////////////////////////////////////////////////////////////
      // Extract D_ID and update it in Log Table

      key = std::string("D_ID") + "-" + std::to_string(district_id_);

      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      queue_info =
          concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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

      //////////////////////////////////////////////////////////////////////
      // S_W_ID
      //////////////////////////////////////////////////////////////////////
      // Extract S_W_ID and update it in Log Table
      for (auto wid : ol_w_ids_) {

        key = std::string("S_W_ID") + "-" + std::to_string(wid);

        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableFullGet(
                key);

        queue_info =
            concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

        if (queue_info != nullptr) {
          for (auto queue : (*queue_info)) {

            // reference = 0 means there is txn (of this condition)
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
      }

      //////////////////////////////////////////////////////////////////////
      // S_I_ID
      //////////////////////////////////////////////////////////////////////
      // Extract S_I_ID and update it in Log Table
      for (auto id : i_ids_) {
        key = std::string("S_I_ID") + "-" + std::to_string(id);

        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableFullGet(
                key);

        queue_info =
            concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

        if (queue_info != nullptr) {
          for (auto queue : (*queue_info)) {

            // reference = 0 means there is txn (of this condition)
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
      }
    }

    return return_queue;
  }
  // Return a queue to schedule
  virtual int LookupRunTableFull(bool single_ref, bool canonical) {
    if (single_ref) {
      return LookupRunTableFullSingleRef(canonical);
    }

    // Extract txn conditions include 4 conditions:
    // S_I_ID  S_W_ID D_ID D_W_ID
    // the corresponding values can be found from:
    // warehouse_id_; district_id_; i_ids_
    // For simplicity, the column name is hard coding here

    // Create a prepare queue map. This can be used to store the queue/thread
    // counter
    int queue_count =
        concurrency::TransactionScheduler::GetInstance().GetQueueCount();

    std::vector<double> queue_map(queue_count, 0);
    double max_conflict = 0;
    int return_queue = -1;

    //////////////////////////////////////////////////////////////////////
    // D_W_ID
    //////////////////////////////////////////////////////////////////////
    // Extract D_W_ID and update it in Log Table
    std::string key = std::string("D_W_ID") + "-" +
                      std::to_string(warehouse_id_) + "-" +
                      std::string("D_ID") + "-" + std::to_string(district_id_);

    // Get conflict from Log Table for the given condition
    double conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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

    //////////////////////////////////////////////////////////////////////
    // S_W_ID
    //////////////////////////////////////////////////////////////////////
    PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
    for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {

      int wid = ol_w_ids_[i];
      int iid = i_ids_[i];
      key = std::string("S_W_ID") + "-" + std::to_string(wid) + "-" +
            std::string("S_I_ID") + "-" + std::to_string(iid);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);
      queue_info =
          concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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
    }

    return return_queue;
  }

  int LookupRunTableMaxFullSingleRef(bool canonical) {
    double max_conflict = 0;
    std::string max_conflict_key;
    int max_reference = 0;
    int queue_no = -1;

    if (canonical) {
      //////////////////////////////////////////////////////////////////////
      // D_W_ID
      //////////////////////////////////////////////////////////////////////
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);

      // Get conflict from Log Table for the given condition
      double conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      //////////////////////////////////////////////////////////////////////
      // D_ID
      //////////////////////////////////////////////////////////////////////
      // Extract D_ID and update it in Log Table

      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      //////////////////////////////////////////////////////////////////////
      // S_W_ID
      //////////////////////////////////////////////////////////////////////
      // Extract S_W_ID and update it in Log Table
      for (auto wid : ol_w_ids_) {

        key = std::string("W_ID") + "-" + std::to_string(wid);

        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableFullGet(
                key);

        if (conflict > max_conflict) {
          max_conflict = conflict;
          max_conflict_key = key;
        }
      }

      //////////////////////////////////////////////////////////////////////
      // S_I_ID
      //////////////////////////////////////////////////////////////////////
      // Extract S_I_ID and update it in Log Table
      for (auto id : i_ids_) {
        key = std::string("I_ID") + "-" + std::to_string(id);

        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableFullGet(
                key);

        if (conflict > max_conflict) {
          max_conflict = conflict;
          max_conflict_key = key;
        }
      }

      // If there is no conflict, return -1;
      if (max_conflict == CONFLICT_THRESHHOLD) {
        return -1;
      }

      // Now we get the key with max conflict, such as S_W_ID
      // Then we should lookup Run Table to get the thread who has this key
      // Each queue: <queueNo. reference>
      std::unordered_map<int, int>* queue_info =
          concurrency::TransactionScheduler::GetInstance().RunTableGet(
              max_conflict_key);

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
    }
    // If not canonical
    else {
      //////////////////////////////////////////////////////////////////////
      // D_W_ID
      //////////////////////////////////////////////////////////////////////
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);

      // Get conflict from Log Table for the given condition
      double conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      //////////////////////////////////////////////////////////////////////
      // D_ID
      //////////////////////////////////////////////////////////////////////
      // Extract D_ID and update it in Log Table

      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      //////////////////////////////////////////////////////////////////////
      // S_W_ID
      //////////////////////////////////////////////////////////////////////
      // Extract S_W_ID and update it in Log Table
      for (auto wid : ol_w_ids_) {

        key = std::string("S_W_ID") + "-" + std::to_string(wid);

        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableFullGet(
                key);

        if (conflict > max_conflict) {
          max_conflict = conflict;
          max_conflict_key = key;
        }
      }

      //////////////////////////////////////////////////////////////////////
      // S_I_ID
      //////////////////////////////////////////////////////////////////////
      // Extract S_I_ID and update it in Log Table
      for (auto id : i_ids_) {
        key = std::string("S_I_ID") + "-" + std::to_string(id);

        conflict =
            concurrency::TransactionScheduler::GetInstance().LogTableFullGet(
                key);

        if (conflict > max_conflict) {
          max_conflict = conflict;
          max_conflict_key = key;
        }
      }

      // If there is no conflict, return -1;
      if (max_conflict == CONFLICT_THRESHHOLD) {
        return -1;
      }

      // Now we get the key with max conflict, such as S_W_ID
      // Then we should lookup Run Table to get the thread who has this key
      // Each queue: <queueNo. reference>
      std::unordered_map<int, int>* queue_info =
          concurrency::TransactionScheduler::GetInstance().RunTableGet(
              max_conflict_key);

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
    }

    return queue_no;
  }
  // Find out the max conflict condition and return the thread executing this
  // condition. If there are multiple threads executing this condition, choose
  // the thread who has the most of this condition
  virtual int LookupRunTableMaxFull(bool single_ref, bool canonical) {
    if (single_ref) {
      return LookupRunTableMaxFullSingleRef(canonical);
    }

    double max_conflict = 0;
    std::string max_conflict_key;

    std::string key = std::string("D_W_ID") + "-" +
                      std::to_string(warehouse_id_) + "-" +
                      std::string("D_ID") + "-" + std::to_string(district_id_);

    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    if (conflict > max_conflict) {
      max_conflict = conflict;
      max_conflict_key = key;
    }

    PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
    for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
      int wid = ol_w_ids_[i];
      int iid = i_ids_[i];
      key = std::string("S_W_ID") + "-" + std::to_string(wid) + "-" +
            std::string("S_I_ID") + "-" + std::to_string(iid);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }
    }
    ////////////////////////new end here

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      return -1;
    }

    // Now we get the key with max conflict, such as S_W_ID
    // Then we should lookup Run Table to get the thread who has this key
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGet(
            max_conflict_key);

    int max_reference = 0;
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

  ////////////////////////end /////////////////////////////////////////

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

  SingleRegion region_;

  // For queue No.
  int queue_;
};

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
        first_pop_(true),
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
  void SetRegionCover() {
    region_.SetCover(state.warehouse_count, state.item_count);
    region_.SetCoverWithDefault(warehouse_id_, state.item_count);
  }

  virtual SingleRegion& GetRegion() { return region_; }

  /*
   *    UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
   *    UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
   *    UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
   */

  virtual void UpdateLogTableSingleRef(bool canonical) {
    // canonical means transform all D_W_ID and C_W_ID to W_ID
    if (canonical) {
      //
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
      //
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
      //
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

    if (canonical) {
      //
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      int conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }
      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }
    }
    // Not canonical, use original ID
    else {
      //
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      int conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      // UPDATE DISTRICT SET ** WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(
            max_conflict_key);

    int max_reference = 0;
    int queue_no = -1;

    // randomly select
    //    if (queue_info != nullptr) {
    //      std::vector<int> queues;
    //
    //      for (auto queue : (*queue_info)) {
    //
    //        // reference = 0 means there is txn (of this condition) executing
    //        if (queue.second > 0) {
    //          queues.push_back(queue.first);
    //        }
    //      }
    // if (queues.size() > 0) {
    //      std::srand(unsigned(std::time(0)));
    //      int random_variable = std::rand() % queues.size();
    //      queue_no = queues.at(random_variable);
    // }
    //    }

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

  virtual int LookupRunTableMax(bool single_ref, bool canonical) {
    if (single_ref) {
      return LookupRunTableMaxSingleRef(canonical);
    }

    int max_conflict = CONFLICT_THRESHHOLD;
    std::string max_conflict_key;

    if (canonical) {
      // UPDATE WAERHOUSE
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      int conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      // UPDATE DISTRICT
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }
    }
    // No canonical, use origincal ID
    else {
      // UPDATE WAREHOUSE
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      int conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }

      // UPDATE DISTRICT
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      conflict =
          concurrency::TransactionScheduler::GetInstance().LogTableGet(key);
      if (conflict > max_conflict) {
        max_conflict = conflict;
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
      if (conflict > max_conflict) {
        max_conflict = conflict;
        max_conflict_key = key;
      }
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(
            max_conflict_key);

    int max_reference = 0;
    int queue_no = -1;

    // randomly select
    //    if (queue_info != nullptr) {
    //      std::vector<int> queues;
    //
    //      for (auto queue : (*queue_info)) {
    //
    //        // reference = 0 means there is txn (of this condition) executing
    //        if (queue.second > 0) {
    //          queues.push_back(queue.first);
    //        }
    //      }
    // if (queues.size() > 0) {
    //      std::srand(unsigned(std::time(0)));
    //      int random_variable = std::rand() % queues.size();
    //      queue_no = queues.at(random_variable);
    // }
    //    }

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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      key = std::string("D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);
    }
    // No canonical, use original ID
    else {
      // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      key = std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key =
          std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      key = std::string("C_ID") + "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
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
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("W_ID") + "-" + std::to_string(customer_warehouse_id_) +
            "-" + std::string("D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);
    } else {
      // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
          key, queue_no);

      // UPDATE2 CUSTOMER SET ** WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
      key = std::string("C_W_ID") + "-" +
            std::to_string(customer_warehouse_id_) + "-" +
            std::string("C_D_ID") + "-" +
            std::to_string(customer_district_id_) + "-" + std::string("C_ID") +
            "-" + std::to_string(customer_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncrease(
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);

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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);
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
        concurrency::TransactionScheduler::GetInstance().RunTableGet(key);
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
  virtual int LookupRunTableMaxFull(bool sigle_ref __attribute__((__unused__)),
                                    bool canonical
                                    __attribute__((__unused__))) {
    double max_conflict = 0;
    std::string max_conflict_key;

    std::string key = std::string("D_W_ID") + "-" +
                      std::to_string(warehouse_id_) + "-" +
                      std::string("D_ID") + "-" + std::to_string(district_id_);
    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    if (conflict > max_conflict) {
      max_conflict = conflict;
      max_conflict_key = key;
    }

    key = std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_) +
          "-" + std::string("C_D_ID") + "-" +
          std::to_string(customer_district_id_);
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    if (conflict > max_conflict) {
      max_conflict = conflict;
      max_conflict_key = key;
    }

    key = std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_) +
          "-" + std::string("C_ID") + "-" + std::to_string(customer_id_);
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    if (conflict > max_conflict) {
      max_conflict = conflict;
      max_conflict_key = key;
    }

    key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_) +
          "-" + std::string("C_ID") + "-" + std::to_string(customer_id_);
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    if (conflict > max_conflict) {
      max_conflict = conflict;
      max_conflict_key = key;
    }

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      return -1;
    }

    // Now we get the key with max conflict, such as S_W_ID
    // Then we should lookup Run Table to get the thread who has this key
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGet(
            max_conflict_key);

    int max_reference = 0;
    int queue_no = -1;

    // randomly select
    //    if (queue_info != nullptr) {
    //      std::vector<int> queues;
    //
    //      for (auto queue : (*queue_info)) {
    //
    //        // reference = 0 means there is txn (of this condition) executing
    //        if (queue.second > 0) {
    //          queues.push_back(queue.first);
    //        }
    //      }
    // if (queues.size() > 0) {
    //      std::srand(unsigned(std::time(0)));
    //      int random_variable = std::rand() % queues.size();
    //      queue_no = queues.at(random_variable);
    // }
    //    }

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
  // Increase the counter when conflict
  virtual void UpdateLogTableFullConflict(bool sigle_ref
                                          __attribute__((__unused__)),
                                          bool canonical
                                          __attribute__((__unused__))) {
    std::string key = std::string("D_W_ID") + "-" +
                      std::to_string(warehouse_id_) + "-" +
                      std::string("D_ID") + "-" + std::to_string(district_id_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullConflictIncrease(key);

    key = std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_) +
          "-" + std::string("C_D_ID") + "-" +
          std::to_string(customer_district_id_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullConflictIncrease(key);

    key = std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_) +
          "-" + std::string("C_ID") + "-" + std::to_string(customer_id_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullConflictIncrease(key);

    key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_) +
          "-" + std::string("C_ID") + "-" + std::to_string(customer_id_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullConflictIncrease(key);
  }

  // Increase the counter when conflict
  virtual void UpdateLogTableFullSuccess(bool sigle_ref
                                         __attribute__((__unused__)),
                                         bool canonical
                                         __attribute__((__unused__))) {

    std::string key = std::string("D_W_ID") + "-" +
                      std::to_string(warehouse_id_) + "-" +
                      std::string("D_ID") + "-" + std::to_string(district_id_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullSuccessIncrease(key);

    key = std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_) +
          "-" + std::string("C_D_ID") + "-" +
          std::to_string(customer_district_id_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullSuccessIncrease(key);

    key = std::string("C_W_ID") + "-" + std::to_string(customer_warehouse_id_) +
          "-" + std::string("C_ID") + "-" + std::to_string(customer_id_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullSuccessIncrease(key);

    key = std::string("C_D_ID") + "-" + std::to_string(customer_district_id_) +
          "-" + std::string("C_ID") + "-" + std::to_string(customer_id_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullSuccessIncrease(key);
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

  // Flag to compute the execution time
  bool first_pop_;

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

NewOrder* GenerateNewOrder();
bool RunNewOrder(NewOrderPlans& new_order_plans, const size_t& thread_id);
bool RunNewOrder(NewOrder* new_order);
void SetNewOrder(NewOrder* new_order);

Payment* GeneratePayment();
bool RunPayment(PaymentPlans& payment_plans, const size_t& thread_id);
bool RunPayment(Payment* new_order);
void SetPayment(Payment* new_order);

bool RunDelivery(DeliveryPlans& delivery_plans, const size_t& thread_id);

bool RunOrderStatus(const size_t& thread_id);

bool RunStockLevel(const size_t& thread_id, const int& order_range);

bool RunScanStock();

/////////////////////////////////////////////////////////
void GenerateAndCacheQuery();
void GenerateALLAndCache(bool new_order);
bool EnqueueCachedUpdate();
std::unordered_map<int, ClusterRegion> ClusterAnalysis();

/////////////////////////////////////////////////////////

std::vector<std::vector<Value>> ExecuteReadTest(
    executor::AbstractExecutor* executor);

void ExecuteUpdateTest(executor::AbstractExecutor* executor);

void ExecuteDeleteTest(executor::AbstractExecutor* executor);

//// For simplicity, we only consider New-Order txn. Some are hard coding
// class RegionTpcc : public Region {
// public:
//  RegionTpcc() : wid_bitset_(nullptr), iid_bitset_(nullptr) {}
//
//  // Give two bitsets, just copy them
//  RegionTpcc(Bitset& wid, Bitset& iid) : wid_bitset_(wid), iid_bitset_(iid) {}
//
//  RegionTpcc(int wid_scale, std::vector<int> wids, int iid_scale,
//             std::vector<int> iids) {
//    // Resize bitset
//    wid_bitset_.Resize(wid_scale);
//    iid_bitset_.Resize(iid_scale);
//
//    // Set bit
//    wid_bitset_.Set(wids);
//    iid_bitset_.Set(iids);
//  }
//
//  ~RegionTpcc() {}
//
//  virtual int OverlapValue(Region& rh_region) {
//    // convert rh_region to RegionTpcc. We should refactor this later
//    int wid_overlap = GetWid().AND(((RegionTpcc)rh_region).GetWid());
//    int iid_overlap = GetIid().AND(((RegionTpcc)rh_region).GetIid());
//
//    return wid_overlap * iid_overlap;
//  }
//
//  // Computer the overlay (OP operation) and return a new Region.
//  // Note: new Region is using new, so remember to delete it
//  RegionTpcc Overlay(Region& rh_region) {
//    Bitset wid_overlay = GetWid().OR(((RegionTpcc)rh_region).GetWid());
//    Bitset iid_overlay = GetIid().OR(((RegionTpcc)rh_region).GetIid());
//
//    return RegionTpcc(wid_overlay, iid_overlay);
//  }
//
//  Bitset& GetWid() { return wid_bitset_; }
//  Bitset& GetIid() { return iid_bitset_; }
//
// private:
//  // For simplicity, only consider two conditions: wid and iid
//  Bitset wid_bitset_;
//  Bitset iid_bitset_;
//};

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
