//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.h
//
// Identification: src/backend/benchmark/ycsb_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/benchmark_common.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/executor/abstract_executor.h"
#include "backend/storage/data_table.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/concurrency/transaction_scheduler.h"

#include <chrono>

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace ycsb {

extern configuration state;

extern storage::DataTable* user_table;

void RunWorkload();

/////////////////////////////////////////////////////////
// TRANSACTION TYPES
/////////////////////////////////////////////////////////


bool RunScan();

/////////////////////////////////////////////////////////

struct ReadPlans {

  executor::IndexScanExecutor* index_scan_executor_;

  void ResetState() { index_scan_executor_->ResetState(); }

  void Cleanup() {
    delete index_scan_executor_;
    index_scan_executor_ = nullptr;
  }
};

ReadPlans PrepareReadPlan();

bool RunRead(ReadPlans& read_plans, ZipfDistribution& zipf);

/////////////////////////////////////////////////////////

struct UpdatePlans {

  executor::IndexScanExecutor* index_scan_executor_;
  executor::UpdateExecutor* update_executor_;

  void SetContext(executor::ExecutorContext* context) {
    index_scan_executor_->SetContext(context);
    update_executor_->SetContext(context);
  }

  void ResetState() { index_scan_executor_->ResetState(); }

  void Cleanup() {
    delete index_scan_executor_;
    index_scan_executor_ = nullptr;

    delete update_executor_;
    update_executor_ = nullptr;
  }
};

class UpdateQuery : public concurrency::TransactionQuery {
 public:
  UpdateQuery(executor::IndexScanExecutor* index_scan_executor,
              planner::IndexScanPlan* index_scan_plan,
              executor::UpdateExecutor* update_executor,
              planner::UpdatePlan* update_plan,
              std::vector<uint64_t>& primary_keys)
      : index_scan_executor_(index_scan_executor),
        index_scan_plan_(index_scan_plan),
        update_executor_(update_executor),
        update_plan_(update_plan),
        context_(nullptr),
        start_time_(std::chrono::system_clock::now()),
        primary_keys_(std::move(primary_keys)),
        first_pop_(true) {}

  void SetContext(executor::ExecutorContext* context) {
    index_scan_executor_->SetContext(context);
    update_executor_->SetContext(context);
    context_ = context;
  }
  ~UpdateQuery() {}

  void ResetState() { index_scan_executor_->ResetState(); }

  void Cleanup() {
    delete context_;
    context_ = nullptr;

    delete index_scan_plan_;
    index_scan_plan_ = nullptr;

    delete index_scan_executor_;
    index_scan_executor_ = nullptr;

    delete update_executor_;
    update_executor_ = nullptr;

    delete update_plan_;
    update_plan_ = nullptr;
  }

  executor::IndexScanExecutor* GetIndexScanExecutor() {
    return index_scan_executor_;
  }
  executor::UpdateExecutor* GetUpdateExecutor() { return update_executor_; }
  void SetIndexScanExecutor(executor::IndexScanExecutor* index_scan_executor) {
    index_scan_executor_ = index_scan_executor;
  }
  void SetUpdateExecutor(executor::UpdateExecutor* update_executor) {
    update_executor_ = update_executor;
  }
  // void SetPrimaryKey(uint64_t key) { primary_key_ = key; }

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
    return index_scan_executor_->GetValues();
  }
  // virtual uint64_t GetPrimaryKeyByint() { return primary_key_; }

  virtual std::vector<uint64_t>& GetPrimaryKeysByint() { return primary_keys_; }
  // Common method
  virtual peloton::PlanNodeType GetPlanType() {
    return peloton::PLAN_NODE_TYPE_UPDATE;
  };

  virtual SingleRegion* RegionTransform() {
    // Compute how large of the whole space

    // Generate the space with a vector
    // std::vector<uint32> cover(0, 0);

    // Generate region and return
    // std::shared_ptr<Region> region(new Region(cover));
    std::vector<int> test;
    return new SingleRegion(0, test, 0, test, true, 0);
  }

  virtual SingleRegion& GetRegion() { return region_; }

  // For Log Table
  virtual void UpdateLogTable() {}
  virtual void UpdateLogTableFullConflict() {}
  virtual void UpdateLogTableFullSuccess() {}

  // For Run Table
  virtual int LookupRunTable() { return 0; }
  virtual int LookupRunTableMax() { return 0; }
  virtual void UpdateRunTable(int queue_no) { std::cout << queue_no; }
  virtual void DecreaseRunTable() {}

  virtual int LookupRunTableFull() { return 0; }
  virtual int LookupRunTableMaxFull() { return 0; }

  // For metadata
  virtual void SetQueueNo(int queue_no) { std::cout << queue_no; }
  virtual int GetQueueNo() { return 0; }

 private:
  executor::IndexScanExecutor* index_scan_executor_;
  planner::IndexScanPlan* index_scan_plan_;
  executor::UpdateExecutor* update_executor_;
  planner::UpdatePlan* update_plan_;
  executor::ExecutorContext* context_;
  std::chrono::system_clock::time_point start_time_;

  // uint64_t primary_key_;
  std::vector<uint64_t> primary_keys_;

  bool first_pop_;

  SingleRegion region_;
};

UpdatePlans PrepareUpdatePlan();

bool RunUpdate(UpdatePlans& update_plans, ZipfDistribution& zipf);
void GenerateAndQueueUpdate(ZipfDistribution& zipf);
void GenerateAndCacheUpdate(ZipfDistribution& zipf);
UpdateQuery* GenerateUpdate(ZipfDistribution& zipf);
void EnqueueCachedUpdate();
bool PopAndExecuteQuery();
bool PopAndExecuteUpdate(UpdateQuery*& ret_query);
bool ExecuteQuery(concurrency::TransactionQuery* query);
bool ExecuteUpdate(UpdateQuery* query);
void DestroyUpdateQuery(concurrency::TransactionQuery* query);

/////////////////////////////////////////////////////////

struct MixedPlans {

  executor::IndexScanExecutor* index_scan_executor_;

  executor::IndexScanExecutor* update_index_scan_executor_;
  executor::UpdateExecutor* update_executor_;

  void SetContext(executor::ExecutorContext* context) {
    index_scan_executor_->SetContext(context);

    update_index_scan_executor_->SetContext(context);
    update_executor_->SetContext(context);
  }

  // in a mixed transaction, an executor is reused for several times.
  // so we must reset the state every time we use it.

  void Cleanup() {
    delete index_scan_executor_;
    index_scan_executor_ = nullptr;

    delete update_index_scan_executor_;
    update_index_scan_executor_ = nullptr;

    delete update_executor_;
    update_executor_ = nullptr;
  }
};

MixedPlans PrepareMixedPlan();

//bool RunMixed(MixedPlans& mixed_plans, ZipfDistribution& zipf,
//              fast_random& rng);

bool RunMixed(MixedPlans &mixed_plans, ZipfDistribution &zipf, fast_random &rng, double update_ratio, int operation_count, bool is_read_only);
/////////////////////////////////////////////////////////

std::vector<std::vector<Value>> ExecuteReadTest(
    executor::AbstractExecutor* executor);

void ExecuteUpdateTest(executor::AbstractExecutor* executor);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
