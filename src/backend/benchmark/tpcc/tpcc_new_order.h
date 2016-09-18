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

extern configuration state;
extern int RUNNING_REF_THRESHOLD;

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

    // Other Selects (4 selects)
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

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      std::cout << "Not find any conflict in Log Table" << std::endl;
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

  virtual int LookupRunTableMax(bool single_ref, bool canonical) {
    if (single_ref) {
      return LookupRunTableMaxSingleRef(canonical);
    }

    // Begin pair ref
    int max_conflict = CONFLICT_THRESHHOLD;
    std::string max_conflict_key;
    std::string key;
    std::map<std::string, int> key_counter;

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

    key_counter[key] += conflict;

    if (key_counter[key] > max_conflict) {
      max_conflict = key_counter[key];
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

      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
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
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
            max_conflict_key);

    int max_reference = RUNNING_REF_THRESHOLD;
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

    //////////////////////////////////////////////////////////////////////
    // D_ID
    //////////////////////////////////////////////////////////////////////
    // Extract D_ID and update it in Log Table

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
          concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
              key);

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
          concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
              key);

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
          concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
              key);

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
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // D_ID
      //////////////////////////////////////////////////////////////////////
      key = std::string("D_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::to_string(district_id_);

      // update run table
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // S_W_ID
      //////////////////////////////////////////////////////////////////////
      for (auto wid : ol_w_ids_) {
        key = std::string("W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
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
        concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
            key, queue_no);
      }

    } else {
      //////////////////////////////////////////////////////////////////////
      // D_W_ID
      //////////////////////////////////////////////////////////////////////
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_);

      // update run table
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // D_ID
      //////////////////////////////////////////////////////////////////////
      key = std::string("D_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::to_string(district_id_);

      // update run table
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      //////////////////////////////////////////////////////////////////////
      // S_W_ID
      //////////////////////////////////////////////////////////////////////
      for (auto wid : ol_w_ids_) {
        key = std::string("S_W_ID") + "-" + std::to_string(wid);
        concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
            key, queue_no);
      }

      //////////////////////////////////////////////////////////////////////
      // S_I_ID
      //////////////////////////////////////////////////////////////////////
      for (auto id : i_ids_) {
        key = std::string("S_I_ID") + "-" + std::to_string(id);
        concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
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
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
      for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
        int wid = ol_w_ids_[i];
        int iid = i_ids_[i];
        key = std::string("W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("I_ID") + "-" + std::to_string(iid);
        concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
            key, queue_no);
      }
    } else {
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
          std::string("D_ID") + "-" + std::to_string(district_id_);
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);

      PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
      for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
        int wid = ol_w_ids_[i];
        int iid = i_ids_[i];
        key = std::string("S_W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("S_I_ID") + "-" + std::to_string(iid);
        concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
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

    if (canonical) {
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
          std::string("D_ID") + "-" + std::to_string(district_id_);

      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);

      PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
      for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
        int wid = ol_w_ids_[i];
        int iid = i_ids_[i];
        key = std::string("W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("I_ID") + "-" + std::to_string(iid);
        concurrency::TransactionScheduler::GetInstance()
            .LogTableFullConflictIncrease(key);
      }
    }
    // Use origincal ID
    else {
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
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

    if (canonical) {
      std::string key =
          std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
          std::string("D_ID") + "-" + std::to_string(district_id_);

      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);

      PL_ASSERT(ol_w_ids_.size() == i_ids_.size());
      for (uint32_t i = 0; i < ol_w_ids_.size(); i++) {
        int wid = ol_w_ids_[i];
        int iid = i_ids_[i];
        key = std::string("W_ID") + "-" + std::to_string(wid) + "-" +
              std::string("I_ID") + "-" + std::to_string(iid);
        concurrency::TransactionScheduler::GetInstance()
            .LogTableFullSuccessIncrease(key);
      }
    }
    // Use origincal ID
    else {
      std::string key =
          std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
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
          concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
              key);

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
          concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
              key);

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
            concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
                key);

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
            concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
                key);

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
          concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
              key);

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
          concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
              key);

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
            concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
                key);

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
            concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
                key);

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
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      queue_info =
          concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
              key);

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
          concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
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
          concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
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

    // Begin pair ref
    double max_conflict = CONFLICT_THRESHHOLD;
    std::string max_conflict_key;
    std::string key;
    std::map<std::string, int> key_counter;

    if (canonical) {
      key = std::string("W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
    } else {
      key = std::string("D_W_ID") + "-" + std::to_string(warehouse_id_) + "-" +
            std::string("D_ID") + "-" + std::to_string(district_id_);
    }

    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    key_counter[key] += conflict;

    if (key_counter[key] > max_conflict) {
      max_conflict = key_counter[key];
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
          concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
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
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
            max_conflict_key);

    int max_reference = RUNNING_REF_THRESHOLD;
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
}
}
}  // end namespace peloton
