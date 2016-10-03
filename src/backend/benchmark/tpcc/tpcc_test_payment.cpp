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

#include "backend/benchmark/tpcc/tpcc_workload.h"
#include "backend/benchmark/tpcc/tpcc_test_payment.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"

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
namespace tpcc {

/*
 * This function new a TestPayment, so remember to delete it
 */
TestPayment *GenerateTestPayment() {

  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // PLAN FOR ITEM
  /////////////////////////////////////////////////////////
  /*
    std::vector<oid_t> item_key_column_ids;
    std::vector<ExpressionType> item_expr_types;
    item_key_column_ids.push_back(0);  // I_ID
    item_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

    std::vector<Value> item_key_values;

    auto item_pkey_index =
    item_table->GetIndexWithOid(item_table_pkey_index_oid);

    planner::IndexScanPlan::IndexScanDesc item_index_scan_desc(
        item_pkey_index, item_key_column_ids, item_expr_types, item_key_values,
        runtime_keys);

    std::vector<oid_t> item_column_ids = {2, 3, 4};  // I_NAME, I_PRICE, I_DATA

    planner::IndexScanPlan item_index_scan_node(
        item_table, nullptr, item_column_ids, item_index_scan_desc);

    executor::IndexScanExecutor *item_index_scan_executor =
        new executor::IndexScanExecutor(&item_index_scan_node, nullptr);

    item_index_scan_executor->Init();
  */

  /////////////////////////////////////////////////////////
  // PLAN FOR WAREHOUSE
  /////////////////////////////////////////////////////////

  std::vector<oid_t> warehouse_key_column_ids;
  std::vector<ExpressionType> warehouse_expr_types;
  warehouse_key_column_ids.push_back(0);  // W_ID
  warehouse_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> warehouse_key_values;

  auto warehouse_pkey_index =
      warehouse_table->GetIndexWithOid(warehouse_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc warehouse_index_scan_desc(
      warehouse_pkey_index, warehouse_key_column_ids, warehouse_expr_types,
      warehouse_key_values, runtime_keys);

  std::vector<oid_t> warehouse_column_ids = {7};  // W_TAX

  planner::IndexScanPlan warehouse_index_scan_node(warehouse_table, nullptr,
                                                   warehouse_column_ids,
                                                   warehouse_index_scan_desc);

  executor::IndexScanExecutor *warehouse_index_scan_executor =
      new executor::IndexScanExecutor(&warehouse_index_scan_node, nullptr);

  warehouse_index_scan_executor->Init();

  // update plan
  std::vector<oid_t> warehouse_update_column_ids = {8};  // D_NEXT_O_ID

  // Create plan node.
  planner::IndexScanPlan warehouse_update_index_scan_node(
      warehouse_table, nullptr, warehouse_update_column_ids,
      warehouse_index_scan_desc);

  executor::IndexScanExecutor *warehouse_update_index_scan_executor =
      new executor::IndexScanExecutor(&warehouse_update_index_scan_node,
                                      nullptr);

  TargetList warehouse_target_list;
  DirectMapList warehouse_direct_map_list;

  // Update the last attribute
  for (oid_t col_itr = 0; col_itr < 8; col_itr++) {
    warehouse_direct_map_list.emplace_back(col_itr,
                                           std::pair<oid_t, oid_t>(0, col_itr));
  }

  std::unique_ptr<const planner::ProjectInfo> warehouse_project_info(
      new planner::ProjectInfo(std::move(warehouse_target_list),
                               std::move(warehouse_direct_map_list)));
  planner::UpdatePlan warehouse_update_node(warehouse_table,
                                            std::move(warehouse_project_info));

  executor::UpdateExecutor *warehouse_update_executor =
      new executor::UpdateExecutor(&warehouse_update_node, nullptr);

  warehouse_update_executor->AddChild(warehouse_update_index_scan_executor);

  warehouse_update_executor->Init();

  /////////////////////////////////////////////////////////
  // PLAN FOR DISTRICT
  /////////////////////////////////////////////////////////

  std::vector<oid_t> district_key_column_ids;
  std::vector<ExpressionType> district_expr_types;

  district_key_column_ids.push_back(0);  // D_ID
  district_key_column_ids.push_back(1);  // D_W_ID
  district_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  district_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> district_key_values;

  auto district_pkey_index =
      district_table->GetIndexWithOid(district_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc district_index_scan_desc(
      district_pkey_index, district_key_column_ids, district_expr_types,
      district_key_values, runtime_keys);

  std::vector<oid_t> district_column_ids = {8, 10};  // D_TAX, D_NEXT_O_ID

  // Create plan node.
  planner::IndexScanPlan district_index_scan_node(
      district_table, nullptr, district_column_ids, district_index_scan_desc);

  executor::IndexScanExecutor *district_index_scan_executor =
      new executor::IndexScanExecutor(&district_index_scan_node, nullptr);

  district_index_scan_executor->Init();

  std::vector<oid_t> district_update_column_ids = {10};  // D_NEXT_O_ID

  // Create plan node.
  planner::IndexScanPlan district_update_index_scan_node(
      district_table, nullptr, district_update_column_ids,
      district_index_scan_desc);

  executor::IndexScanExecutor *district_update_index_scan_executor =
      new executor::IndexScanExecutor(&district_update_index_scan_node,
                                      nullptr);

  TargetList district_target_list;
  DirectMapList district_direct_map_list;

  // Update the last attribute
  for (oid_t col_itr = 0; col_itr < 10; col_itr++) {
    district_direct_map_list.emplace_back(col_itr,
                                          std::pair<oid_t, oid_t>(0, col_itr));
  }

  std::unique_ptr<const planner::ProjectInfo> district_project_info(
      new planner::ProjectInfo(std::move(district_target_list),
                               std::move(district_direct_map_list)));
  planner::UpdatePlan district_update_node(district_table,
                                           std::move(district_project_info));

  executor::UpdateExecutor *district_update_executor =
      new executor::UpdateExecutor(&district_update_node, nullptr);

  district_update_executor->AddChild(district_update_index_scan_executor);

  district_update_executor->Init();

  /////////////////////////////////////////////////////////
  // PLAN FOR CUSTOMER
  /////////////////////////////////////////////////////////

  /*


  std::vector<oid_t> customer_key_column_ids;
  std::vector<ExpressionType> customer_expr_types;

  customer_key_column_ids.push_back(0);  // C_ID
  customer_key_column_ids.push_back(1);  // C_D_ID
  customer_key_column_ids.push_back(2);  // C_W_ID
  customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> customer_key_values;

  auto customer_pkey_index =
      customer_table->GetIndexWithOid(customer_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(
      customer_pkey_index, customer_key_column_ids, customer_expr_types,
      customer_key_values, runtime_keys);

  std::vector<oid_t> customer_column_ids = {
      5, 13, 15};  // C_LAST, C_CREDIT, C_DISCOUNT

  // Create plan node.
  planner::IndexScanPlan customer_index_scan_node(
      customer_table, nullptr, customer_column_ids, customer_index_scan_desc);

  executor::IndexScanExecutor *customer_index_scan_executor =
      new executor::IndexScanExecutor(&customer_index_scan_node, nullptr);

  customer_index_scan_executor->Init();

*/

  /////////////////////////////////////////////////////////

  TestPayment *test = new TestPayment();

  // test->item_index_scan_executor_ = item_index_scan_executor;
  // test->customer_index_scan_executor_ = customer_index_scan_executor;

  test->warehouse_index_scan_executor_ = warehouse_index_scan_executor;
  test->warehouse_update_index_scan_executor_ =
      warehouse_update_index_scan_executor;
  test->warehouse_update_executor_ = warehouse_update_executor;

  test->district_index_scan_executor_ = district_index_scan_executor;
  test->district_update_index_scan_executor_ =
      district_update_index_scan_executor;
  test->district_update_executor_ = district_update_executor;

  // Set values
  SetTestPayment(test);

  // Set txn's region cover
  test->SetRegionCover();

  return test;
}

/*
 * Set the parameters needed by execution. Set the W_ID, D_ID, C_ID, I_ID.
 * So when a txn has all of the parameters when enqueue
 */
void SetTestPayment(TestPayment *test) {
  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  test->warehouse_id_ = GenerateWarehouseId();
  test->district_id_ = GetRandomInteger(0, state.districts_per_warehouse - 1);
  test->customer_id_ = GetRandomInteger(0, state.customers_per_district - 1);
  test->o_ol_cnt_ = GetRandomInteger(orders_min_ol_cnt, orders_max_ol_cnt);

  std::vector<int> i_ids, ol_w_ids, ol_qtys;
  bool o_all_local = true;

  for (auto ol_itr = 0; ol_itr < test->o_ol_cnt_; ol_itr++) {
    i_ids.push_back(GetRandomInteger(0, state.item_count - 1));
    ol_w_ids.push_back(test->warehouse_id_);

    bool remote = GetRandomBoolean(new_order_remote_txns);
    if (remote == true) {
      ol_w_ids[ol_itr] = GetRandomIntegerExcluding(0, state.warehouse_count - 1,
                                                   test->warehouse_id_);
      o_all_local = false;
    }

    ol_qtys.push_back(GetRandomInteger(0, order_line_max_ol_quantity));
  }

  test->i_ids_ = i_ids;
  test->ol_w_ids_ = ol_w_ids;
  test->ol_qtys_ = ol_qtys;
  test->o_all_local_ = o_all_local;

  // Take warehouse_id_ as the primary key
  test->primary_keys_.assign(1, test->warehouse_id_);
}

/*
 * As a method fo class TestPayment
 *
 * Before running, a New Order already has all of the parameters
 * (W_ID,D_ID,C_ID,I_ID).
 */
bool TestPayment::Run() {

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  int warehouse_id = warehouse_id_;
  int district_id = district_id_;
  // int customer_id = customer_id_;
  // int o_ol_cnt = o_ol_cnt_;
  // auto o_entry_ts = GetTimeStamp();

  // std::vector<int> &i_ids = i_ids_;
  // std::vector<int> &ol_w_ids = ol_w_ids_;
  // std::vector<int> &ol_qtys = ol_qtys_;

  // bool o_all_local = o_all_local_;

  /////////////////////////////////////////////////////////
  // BEGIN TRANSACTION
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  SetContext(context.get());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /*

  // std::vector<float> i_prices;
  for (auto item_id : i_ids) {

    LOG_TRACE(
        "getItemInfo: SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = %d",
        item_id);

    item_index_scan_executor_->ResetState();

    std::vector<Value> item_key_values;

    item_key_values.push_back(ValueFactory::GetIntegerValue(item_id));

    item_index_scan_executor_->SetValues(item_key_values);

    auto gii_lists_values = ExecuteReadTest(item_index_scan_executor_);

    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      LOG_TRACE("abort transaction");
      txn_manager.AbortTransaction();
      return false;
    }

    if (gii_lists_values.size() != 1) {
      LOG_ERROR("getItemInfo return size incorrect : %lu",
                gii_lists_values.size());
      assert(false);
    }
  }

  LOG_TRACE("getWarehouseTaxRate: SELECT W_TAX FROM WAREHOUSE WHERE W_ID = %d",
            warehouse_id);
*/

  /////////////////////////////////////////////////////////
  // WAREHOUSE SELECTION
  /////////////////////////////////////////////////////////
  warehouse_index_scan_executor_->ResetState();

  std::vector<Value> warehouse_key_values;

  warehouse_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

  warehouse_index_scan_executor_->SetValues(warehouse_key_values);

  auto gwtr_lists_values = ExecuteReadTest(warehouse_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  if (gwtr_lists_values.size() != 1) {
    LOG_ERROR("getWarehouseTaxRate return size incorrect : %lu",
              gwtr_lists_values.size());
    assert(false);
  }

  auto w_tax = gwtr_lists_values[0][0];

  LOG_TRACE("w_tax: %s", w_tax.GetInfo().c_str());

  LOG_TRACE(
      "getDistrict: SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = %d "
      "AND D_W_ID = %d",
      district_id, warehouse_id);

  // Update Warehouse
  double warehouse_new_balance = 30.3;

  warehouse_update_index_scan_executor_->ResetState();

  warehouse_update_index_scan_executor_->SetValues(warehouse_key_values);

  TargetList warehouse_target_list;

  // Update the 9th column
  Value warehouse_new_balance_value =
      ValueFactory::GetDoubleValue(warehouse_new_balance);

  warehouse_target_list.emplace_back(
      8, expression::ExpressionUtil::ConstantValueFactory(
             warehouse_new_balance_value));

  warehouse_update_executor_->SetTargetList(warehouse_target_list);

  // Execute the query
  ExecuteUpdateTest(warehouse_update_executor_);

  // Check if aborted
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  /////////////////////////////////////////////////////////
  // DISTRICT SELECTION
  /////////////////////////////////////////////////////////
  district_index_scan_executor_->ResetState();

  std::vector<Value> district_key_values;

  district_key_values.push_back(ValueFactory::GetIntegerValue(district_id));
  district_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

  district_index_scan_executor_->SetValues(district_key_values);

  auto gd_lists_values = ExecuteReadTest(district_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  if (gd_lists_values.size() != 1) {
    LOG_ERROR("getDistrict return size incorrect : %lu",
              gd_lists_values.size());
    assert(false);
  }

  auto d_tax = gd_lists_values[0][0];
  auto d_next_o_id = gd_lists_values[0][1];

  LOG_TRACE("d_tax: %s, d_next_o_id: %s", d_tax.GetInfo().c_str(),
            d_next_o_id.GetInfo().c_str());

  LOG_TRACE(
      "getCustomer: SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE "
      "C_W_ID = %d AND C_D_ID = %d AND C_ID = %d",
      warehouse_id, district_id, customer_id);

  /////////////////////////////////////////////////////////
  // CUSTOMER SELECTION
  /////////////////////////////////////////////////////////

  /*

  customer_index_scan_executor_->ResetState();

  std::vector<Value> customer_key_values;

  customer_key_values.push_back(ValueFactory::GetIntegerValue(customer_id));
  customer_key_values.push_back(ValueFactory::GetIntegerValue(district_id));
  customer_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

  customer_index_scan_executor_->SetValues(customer_key_values);

  auto gc_lists_values = ExecuteReadTest(customer_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  if (gc_lists_values.size() != 1) {
    LOG_ERROR("getCustomer return size incorrect : %lu",
              gc_lists_values.size());
    assert(false);
  }

  auto c_last = gc_lists_values[0][0];
  auto c_credit = gc_lists_values[0][1];
  auto c_discount = gc_lists_values[0][2];

  LOG_TRACE("c_last: %s, c_credit: %s, c_discount: %s",
            c_last.GetInfo().c_str(), c_credit.GetInfo().c_str(),
            c_discount.GetInfo().c_str());
  */

  /////////////////////////////////////////////////////////
  // DISTRICT UPDATE
  /////////////////////////////////////////////////////////

  int district_update_value = ValuePeeker::PeekAsInteger(d_next_o_id) + 1;
  LOG_TRACE("district update value = %d", district_update_value);

  LOG_TRACE(
      "incrementNextOrderId: UPDATE DISTRICT SET D_NEXT_O_ID = %d WHERE D_ID = "
      "%d AND D_W_ID = %d",
      district_update_value, district_id, warehouse_id);

  district_update_index_scan_executor_->ResetState();

  district_update_index_scan_executor_->SetValues(district_key_values);

  TargetList district_target_list;

  Value district_update_val =
      ValueFactory::GetIntegerValue(district_update_value);

  district_target_list.emplace_back(
      10,
      expression::ExpressionUtil::ConstantValueFactory(district_update_val));

  district_update_executor_->SetTargetList(district_target_list);

  ExecuteUpdateTest(district_update_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  LOG_TRACE(
      "createOrder: INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, "
      "O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)");

  // transaction passed execution.
  assert(txn->GetResult() == Result::RESULT_SUCCESS);

  /////////////////////////////////////////////////////////
  // TRANSACTION COMMIT
  /////////////////////////////////////////////////////////
  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    // transaction passed commitment.
    LOG_TRACE("commit txn, thread_id = %d, d_id = %d, next_o_id = %d",
              (int)thread_id, (int)district_id,
              (int)ValuePeeker::PeekAsInteger(d_next_o_id));
    return true;

  } else {
    // transaction failed commitment.
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    LOG_TRACE("abort txn, thread_id = %d, d_id = %d, next_o_id = %d",
              (int)thread_id, (int)district_id,
              (int)ValuePeeker::PeekAsInteger(d_next_o_id));
    return false;
  }
}
}
}
}