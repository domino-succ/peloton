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
              executor::UpdateExecutor* update_executor,
              planner::UpdatePlan* update_plan)
      : index_scan_executor_(index_scan_executor),
        update_executor_(update_executor),
        update_plan_(update_plan),
        context_(nullptr) {}

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

  virtual peloton::PlanNodeType GetPlanType() {
    return peloton::PLAN_NODE_TYPE_UPDATE;
  };

 private:
  executor::IndexScanExecutor* index_scan_executor_;
  executor::UpdateExecutor* update_executor_;
  planner::UpdatePlan* update_plan_;
  executor::ExecutorContext* context_;
};

UpdatePlans PrepareUpdatePlan();
bool RunUpdate(UpdatePlans& update_plans, ZipfDistribution& zipf);
void GenerateAndQueueUpdate(ZipfDistribution& zipf);
UpdateQuery* GenerateUpdate(ZipfDistribution& zipf);
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

bool RunMixed(MixedPlans& mixed_plans, ZipfDistribution& zipf, int read_count,
              int write_count);

/////////////////////////////////////////////////////////

std::vector<std::vector<Value>> ExecuteReadTest(
    executor::AbstractExecutor* executor);

void ExecuteUpdateTest(executor::AbstractExecutor* executor);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
