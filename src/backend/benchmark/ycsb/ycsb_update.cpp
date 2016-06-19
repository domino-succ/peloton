//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/tpcc/workload.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"
#include "backend/benchmark/ycsb/ycsb_workload.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/common/generator.h"

#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/container_tuple.h"

#include "backend/index/index_factory.h"

#include "backend/logging/log_manager.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/index_scan_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

extern DistributionAnalysis analysis;

/////////////////////////////////////////////////////////////////////////////////////////
// Queue Based YCSB methods
/////////////////////////////////////////////////////////////////////////////////////////

void GenerateAndCacheUpdate(ZipfDistribution &zipf) {

  /////////////////////////////////////////////////////////
  // Generate INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////
  std::vector<oid_t> key_column_ids;
  key_column_ids.push_back(0);

  std::vector<ExpressionType> expr_types;
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  //  auto lookup_key = zipf.GetNextNumber();
  std::vector<Value> values;
  //  values.push_back(ValueFactory::GetBigIntValue(lookup_key));

  std::vector<expression::AbstractExpression *> runtime_keys;
  auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up index scan executor
  planner::IndexScanPlan *index_scan_node = new planner::IndexScanPlan(
      user_table, predicate, column_ids, index_scan_desc);

  // Should delete after executing the query
  executor::IndexScanExecutor *index_scan_executor =
      new executor::IndexScanExecutor(index_scan_node, nullptr);

  /////////////////////////////////////////////////////////
  // Generate UPDATE: plan , executor , query
  /////////////////////////////////////////////////////////

  TargetList target_list;
  // Value update_val = ValueFactory::GetIntegerValue(2);
  //  target_list.emplace_back(
  //      1, expression::ExpressionUtil::ConstantValueFactory(update_val));

  DirectMapList direct_map_list;
  // Update the second attribute
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    if (col_itr != 1) {
      direct_map_list.emplace_back(col_itr,
                                   std::pair<oid_t, oid_t>(0, col_itr));
    }
  }

  // Generate update plan
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan *update_node =
      new planner::UpdatePlan(user_table, std::move(project_info));

  // Generate update executor
  executor::UpdateExecutor *update_executor =
      new executor::UpdateExecutor(update_node, nullptr);
  update_executor->AddChild(index_scan_executor);
  update_executor->Init();

  // Generate update query
  std::vector<uint64_t> lookup_key_s;

  // Generate lookup_keys
  uint64_t key = zipf.GetNextNumber();
  for (int i = 0; i < state.operation_count; i++) {
    // uint64_t key = zipf.GetNextNumber();
    lookup_key_s.push_back(key);

    // TODO: replace this using more elegant way
    analysis.Insert(key);
  }

  // Generate query
  UpdateQuery *query =
      new UpdateQuery(index_scan_executor, index_scan_node, update_executor,
                      update_node, lookup_key_s);

  /////////////////////////////////////////////////////////
  // Call txn scheduler to queue this executor
  /////////////////////////////////////////////////////////
  concurrency::TransactionScheduler::GetInstance().CacheQuery(query);
}

/////////////////////////////////////////////////////////
// Call txn scheduler to queue this executor
/////////////////////////////////////////////////////////

void EnqueueCachedUpdate() {
  uint64_t size = concurrency::TransactionScheduler::GetInstance().CacheSize();
  concurrency::TransactionQuery *query = nullptr;

  for (uint64_t i = 0; i < size; i++) {

    bool ret =
        concurrency::TransactionScheduler::GetInstance().DequeueCache(query);

    if (ret == false) {
      LOG_INFO("Error when dequeue cache: is the cache empty??");
      continue;
    }

    // Push the query into the queue
    // Note: when poping the query and after executing it, the update_executor
    // and
    // index_executor should be deleted, then query itself should be deleted
    if (state.scheduler == SCHEDULER_TYPE_CONFLICT_DETECT) {
      concurrency::TransactionScheduler::GetInstance().Enqueue(query);
    } else if (state.scheduler == SCHEDULER_TYPE_CONFLICT_LEANING) {
      concurrency::TransactionScheduler::GetInstance().RouterRangeEnqueue(
          query);
    } else if (state.scheduler == SCHEDULER_TYPE_CONFLICT_RANGE) {
      concurrency::TransactionScheduler::GetInstance().RangeEnqueue(query);
    } else {
      concurrency::TransactionScheduler::GetInstance().SingleEnqueue(query);
    }
  }
}

void GenerateAndQueueUpdate(ZipfDistribution &zipf) {

  /////////////////////////////////////////////////////////
  // Generate INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////
  std::vector<oid_t> key_column_ids;
  key_column_ids.push_back(0);

  std::vector<ExpressionType> expr_types;
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  //  auto lookup_key = zipf.GetNextNumber();
  std::vector<Value> values;
  //  values.push_back(ValueFactory::GetBigIntValue(lookup_key));

  std::vector<expression::AbstractExpression *> runtime_keys;
  auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up index scan executor
  planner::IndexScanPlan *index_scan_node = new planner::IndexScanPlan(
      user_table, predicate, column_ids, index_scan_desc);

  // Should delete after executing the query
  executor::IndexScanExecutor *index_scan_executor =
      new executor::IndexScanExecutor(index_scan_node, nullptr);

  /////////////////////////////////////////////////////////
  // Generate UPDATE: plan , executor , query
  /////////////////////////////////////////////////////////

  TargetList target_list;
  // Value update_val = ValueFactory::GetIntegerValue(2);
  //  target_list.emplace_back(
  //      1, expression::ExpressionUtil::ConstantValueFactory(update_val));

  DirectMapList direct_map_list;
  // Update the second attribute
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    if (col_itr != 1) {
      direct_map_list.emplace_back(col_itr,
                                   std::pair<oid_t, oid_t>(0, col_itr));
    }
  }

  // Generate update plan
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan *update_node =
      new planner::UpdatePlan(user_table, std::move(project_info));

  // Generate update executor
  executor::UpdateExecutor *update_executor =
      new executor::UpdateExecutor(update_node, nullptr);
  update_executor->AddChild(index_scan_executor);
  update_executor->Init();

  // Generate update query
  std::vector<uint64_t> lookup_key_s;

  // Generate lookup_keys
  for (int i = 0; i < state.operation_count; i++) {
    uint64_t key = zipf.GetNextNumber();
    lookup_key_s.push_back(key);

    // TODO: replace this using more elegant way
    analysis.Insert(key);
  }

  // Generate query
  UpdateQuery *query =
      new UpdateQuery(index_scan_executor, index_scan_node, update_executor,
                      update_node, lookup_key_s);

  /////////////////////////////////////////////////////////
  // Call txn scheduler to queue this executor
  /////////////////////////////////////////////////////////

  // Push the query into the queue
  // Note: when poping the query and after executing it, the update_executor and
  // index_executor should be deleted, then query itself should be deleted
  if (state.scheduler == SCHEDULER_TYPE_CONFLICT_DETECT) {
    concurrency::TransactionScheduler::GetInstance().Enqueue(query);
  } else if (state.scheduler == SCHEDULER_TYPE_CONFLICT_LEANING) {
    concurrency::TransactionScheduler::GetInstance().ModRangeEnqueue(query);
  } else if (state.scheduler == SCHEDULER_TYPE_CONFLICT_RANGE) {
    concurrency::TransactionScheduler::GetInstance().RangeEnqueue(query);
  } else {
    concurrency::TransactionScheduler::GetInstance().SingleEnqueue(query);
  }
}

UpdateQuery *GenerateUpdate(ZipfDistribution &zipf) {

  /////////////////////////////////////////////////////////
  // Generate INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////
  std::vector<oid_t> key_column_ids;
  key_column_ids.push_back(0);

  std::vector<ExpressionType> expr_types;
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  // auto lookup_key = zipf.GetNextNumber();
  std::vector<Value> values;
  // values.push_back(ValueFactory::GetIntegerValue(lookup_key));

  std::vector<expression::AbstractExpression *> runtime_keys;
  auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create Index Scan Plan. New a plan and remember to delete it!
  planner::IndexScanPlan *index_scan_node = new planner::IndexScanPlan(
      user_table, predicate, column_ids, index_scan_desc);

  // TODO: Should delete after executing the query
  executor::IndexScanExecutor *index_scan_executor =
      new executor::IndexScanExecutor(index_scan_node, nullptr);

  /////////////////////////////////////////////////////////
  // Generate UPDATE: plan , executor , query
  /////////////////////////////////////////////////////////

  TargetList target_list;
  // Value update_val = ValueFactory::GetIntegerValue(2);
  // target_list.emplace_back(
  //    1, expression::ExpressionUtil::ConstantValueFactory(update_val));

  DirectMapList direct_map_list;
  // Update the second attribute
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    if (col_itr != 1) {
      direct_map_list.emplace_back(col_itr,
                                   std::pair<oid_t, oid_t>(0, col_itr));
    }
  }

  // Generate update plan
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan *update_node =
      new planner::UpdatePlan(user_table, std::move(project_info));

  // Generate update executor
  executor::UpdateExecutor *update_executor =
      new executor::UpdateExecutor(update_node, nullptr);
  update_executor->AddChild(index_scan_executor);
  update_executor->Init();

  // Generate lookup_keys
  std::vector<uint64_t> lookup_key_s;
  for (int i = 0; i < state.operation_count; i++) {
    uint64_t key = zipf.GetNextNumber();
    lookup_key_s.push_back(key);
  }

  // Generate query
  UpdateQuery *query =
      new UpdateQuery(index_scan_executor, index_scan_node, update_executor,
                      update_node, lookup_key_s);

  return query;
}

bool ExecuteUpdate(UpdateQuery *query) {
  // Start a txn to execute the query
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // Set context
  /////////////////////////////////////////////////////////
  executor::ExecutorContext *context = new executor::ExecutorContext(nullptr);
  query->SetContext(context);

  /////////////////////////////////////////////////////////
  // INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  for (int idx = 0; idx < state.operation_count; idx++) {

    // Must reset before execute a query
    query->ResetState();

    // Set up parameter values
    std::vector<Value> values;

    // Get the lookup key
    auto lookup_key = query->GetPrimaryKeysByint().at(idx);
    values.push_back(ValueFactory::GetBigIntValue(lookup_key));

    // Set the lookup key for index scan executor
    query->GetIndexScanExecutor()->SetValues(values);

    // Create target list
    TargetList target_list;
    // std::string update_raw_value(ycsb_field_length - 1, 'u');
    int update_raw_value = 2;

    Value update_val = ValueFactory::GetIntegerValue(update_raw_value);

    target_list.emplace_back(
        1, expression::ExpressionUtil::ConstantValueFactory(update_val));

    // Set the target list for update executor
    query->GetUpdateExecutor()->SetTargetList(target_list);

    /////////////////////////////////////////////////////////
    // EXECUTE
    /////////////////////////////////////////////////////////
    ExecuteUpdateTest(query->GetUpdateExecutor());

    /////////////////////////////////////////////////////////
    // Transaction fail
    /////////////////////////////////////////////////////////
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }
  }

  /////////////////////////////////////////////////////////
  // Transaction success
  /////////////////////////////////////////////////////////
  assert(txn->GetResult() == Result::RESULT_SUCCESS);
  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    return true;
  } else {
    // transaction failed commitment.
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    return false;
  }
}

// If return true and query is null, that means the queue is empty
// If return true and query is not null, that means execute successfully
// If return false, that means execute fail
bool PopAndExecuteUpdate(UpdateQuery *&ret_query) {
  // Get a query from a queue
  concurrency::TransactionQuery *query = nullptr;
  concurrency::TransactionScheduler::GetInstance().SingleDequeue(query);

  // Queue is empty, set return query with null and return true
  if (query == nullptr) {
    ret_query = nullptr;
    return true;
  }

  // If queue is not empty, execute the query
  ret_query = reinterpret_cast<UpdateQuery *>(query);
  return ExecuteUpdate(ret_query);
}

UpdatePlans PrepareUpdatePlan() {

  /////////////////////////////////////////////////////////
  // INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> values;

  std::vector<expression::AbstractExpression *> runtime_keys;

  auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up index scan executor
  planner::IndexScanPlan index_scan_node(user_table, predicate, column_ids,
                                         index_scan_desc);

  executor::IndexScanExecutor *index_scan_executor =
      new executor::IndexScanExecutor(&index_scan_node, nullptr);

  /////////////////////////////////////////////////////////
  // UPDATE
  /////////////////////////////////////////////////////////

  TargetList target_list;
  DirectMapList direct_map_list;

  // Update the second attribute
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    if (col_itr != 1) {
      direct_map_list.emplace_back(col_itr,
                                   std::pair<oid_t, oid_t>(0, col_itr));
    }
  }

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(user_table, std::move(project_info));

  executor::UpdateExecutor *update_executor =
      new executor::UpdateExecutor(&update_node, nullptr);

  update_executor->AddChild(index_scan_executor);

  update_executor->Init();

  UpdatePlans update_plans;

  update_plans.index_scan_executor_ = index_scan_executor;

  update_plans.update_executor_ = update_executor;

  return update_plans;
}

bool RunUpdate(UpdatePlans &update_plans, ZipfDistribution &zipf) {

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  update_plans.SetContext(context.get());
  update_plans.ResetState();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  std::vector<Value> values;

  auto lookup_key = zipf.GetNextNumber();

  values.push_back(ValueFactory::GetIntegerValue(lookup_key));

  update_plans.index_scan_executor_->SetValues(values);

  TargetList target_list;
  // std::string update_raw_value(ycsb_field_length - 1, 'u');
  int update_raw_value = 2;

  Value update_val = ValueFactory::GetIntegerValue(update_raw_value);

  target_list.emplace_back(
      1, expression::ExpressionUtil::ConstantValueFactory(update_val));

  update_plans.update_executor_->SetTargetList(target_list);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  ExecuteUpdateTest(update_plans.update_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  // transaction passed execution.
  assert(txn->GetResult() == Result::RESULT_SUCCESS);

  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    return true;

  } else {
    // transaction failed commitment.
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    return false;
  }
}
}
}
}
