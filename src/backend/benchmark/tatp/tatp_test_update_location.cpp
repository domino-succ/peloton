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

#include "backend/benchmark/tatp/tatp_workload.h"
#include "backend/benchmark/tatp/tatp_configuration.h"
#include "backend/benchmark/tatp/tatp_loader.h"

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
namespace tatp {

/*
 * This function new a Amalgamate, so remember to delete it
 */
TestUpdateLocation *GenerateTestUpdateLocation(ZipfDistribution &zipf) {

  /*
  "UpdateLocation": {
  "SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr =
  ?"

  "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET vlr_location = ? WHERE
  s_id = ?"
        }


  */
  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // PLAN For Access
  /////////////////////////////////////////////////////////
  std::vector<oid_t> access_key_column_ids = {0, 1};  // pkey: sid, ai_type
  std::vector<ExpressionType> access_expr_types;
  access_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  access_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> access_key_values;

  auto access_pkey_index =
      access_info_table->GetIndexWithOid(access_info_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc access_index_scan_desc(
      access_pkey_index, access_key_column_ids, access_expr_types,
      access_key_values, runtime_keys);

  std::vector<oid_t> access_column_ids = {2, 3, 4, 5};  // select data1,2,3,4

  planner::IndexScanPlan access_index_scan_node(
      access_info_table, nullptr, access_column_ids, access_index_scan_desc);

  executor::IndexScanExecutor *access_index_scan_executor =
      new executor::IndexScanExecutor(&access_index_scan_node, nullptr);

  access_index_scan_executor->Init();

  /////////////////////////////////////////////////////////
  // PLAN For Sub
  /////////////////////////////////////////////////////////

  std::vector<oid_t> test_sub_key_column_ids = {0};  // pk: sid
  std::vector<ExpressionType> test_sub_expr_types;
  test_sub_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> test_sub_key_values;

  auto test_sub_pkey_index =
      test_sub_table->GetIndexWithOid(test_sub_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc test_sub_index_scan_desc(
      test_sub_pkey_index, test_sub_key_column_ids, test_sub_expr_types,
      test_sub_key_values, runtime_keys);

  std::vector<oid_t> test_sub_column_ids = {1};  // select sub from

  // Select
  planner::IndexScanPlan test_sub_index_scan_node(
      test_sub_table, nullptr, test_sub_column_ids, test_sub_index_scan_desc);

  executor::IndexScanExecutor *test_sub_index_scan_executor =
      new executor::IndexScanExecutor(&test_sub_index_scan_node, nullptr);

  test_sub_index_scan_executor->Init();

  // UPDATE

  // vr location
  std::vector<oid_t> test_sub_update_column_ids = {6};

  planner::IndexScanPlan test_sub_update_index_scan_node(
      test_sub_table, nullptr, test_sub_update_column_ids,
      test_sub_index_scan_desc);

  executor::IndexScanExecutor *test_sub_update_index_scan_executor =
      new executor::IndexScanExecutor(&test_sub_update_index_scan_node,
                                      nullptr);

  TargetList test_sub_target_list;
  DirectMapList test_sub_direct_map_list;

  // Keep the 4 columns unchanged
  for (oid_t col_itr = 0; col_itr < 6; ++col_itr) {
    test_sub_direct_map_list.emplace_back(col_itr,
                                          std::pair<oid_t, oid_t>(0, col_itr));
  }

  std::unique_ptr<const planner::ProjectInfo> test_sub_project_info(
      new planner::ProjectInfo(std::move(test_sub_target_list),
                               std::move(test_sub_direct_map_list)));
  planner::UpdatePlan test_sub_update_node(test_sub_table,
                                           std::move(test_sub_project_info));

  executor::UpdateExecutor *test_sub_update_executor =
      new executor::UpdateExecutor(&test_sub_update_node, nullptr);

  test_sub_update_executor->AddChild(test_sub_update_index_scan_executor);

  test_sub_update_executor->Init();

  /////////////////////////////////////////////////////////

  TestUpdateLocation *us = new TestUpdateLocation();

  us->access_index_scan_executor_ = access_index_scan_executor;
  us->sub_index_scan_executor_ = test_sub_index_scan_executor;
  us->sub_update_index_scan_executor_ = test_sub_update_index_scan_executor;
  us->sub_update_executor_ = test_sub_update_executor;

  // Set values
  us->SetValue(zipf);

  // Set txn's region cover
  us->SetRegionCover();

  return us;
}

/*
 * Set the parameters needed by execution. Set the W_ID, D_ID, C_ID, I_ID.
 * So when a txn has all of the parameters when enqueue
 */
void TestUpdateLocation::SetValue(ZipfDistribution &zipf) {
  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  if (state.zipf_theta > 0) {
    sid_ = zipf.GetNextNumber();
  } else {
    sid_ = GenerateSubscriberId();
  }

  // Take warehouse_id_ as the primary key
  primary_keys_.assign(1, sid_);
}

bool TestUpdateLocation::Run() {
  /*
  "UpdateLocation": {
  "SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr =
  ?"

  "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET vlr_location = ? WHERE
  s_id = ?"
        }
  */

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  int sid = sid_;

  /////////////////////////////////////////////////////////
  // BEGIN TRANSACTION
  /////////////////////////////////////////////////////////
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  SetContext(context.get());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // ACCOUNTS SELECTION
  /////////////////////////////////////////////////////////

  // "SELECT1 * FROM " + TABLENAME_ACCOUNTS + " WHERE custid = ?"
  LOG_TRACE("SELECT * FROM ACCOUNTS WHERE sid = %d", sid);

  access_index_scan_executor_->ResetState();

  std::vector<Value> access_key_values;
  access_key_values.push_back(ValueFactory::GetIntegerValue(sid));
  access_key_values.push_back(ValueFactory::GetIntegerValue(1));

  access_index_scan_executor_->SetValues(access_key_values);

  auto ga1_lists_values = ExecuteReadTest(access_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  /////////////////////////////////////////////////////////
  // SUBSCRIBER SELECTION
  /////////////////////////////////////////////////////////

  std::vector<Value> sub_key_values;

  sub_key_values.push_back(ValueFactory::GetIntegerValue(sid));

  // Select
  LOG_TRACE("SELECT bal FROM checking WHERE custid = %d", sid);

  sub_index_scan_executor_->ResetState();

  sub_index_scan_executor_->SetValues(sub_key_values);

  auto gc_lists_values = ExecuteReadTest(sub_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  // Update
  sub_update_index_scan_executor_->ResetState();

  sub_update_index_scan_executor_->SetValues(sub_key_values);

  TargetList sub_target_list;

  int location = GetRandomInteger(MIN_INT, MAX_INT);

  Value sub_update_val = ValueFactory::GetIntegerValue(location);

  // var location's column is 6
  sub_target_list.emplace_back(
      6, expression::ExpressionUtil::ConstantValueFactory(sub_update_val));

  sub_update_executor_->SetTargetList(sub_target_list);

  ExecuteUpdateTest(sub_update_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  // transaction passed execution.
  assert(txn->GetResult() == Result::RESULT_SUCCESS);

  /////////////////////////////////////////////////////////
  // TRANSACTION COMMIT
  /////////////////////////////////////////////////////////
  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    // transaction passed commitment.
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
