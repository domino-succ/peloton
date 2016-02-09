//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_set_op_test.cpp
//
// Identification: tests/executor/hash_set_op_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "backend/planner/set_op_plan.h"

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/hash_set_op_executor.h"
#include "backend/storage/data_table.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

namespace {

void RunTest(executor::HashSetOpExecutor &executor,
             size_t expected_num_tuples) {
  EXPECT_TRUE(executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
  while (executor.Execute()) {
    result_tiles.emplace_back(executor.GetOutput());
  }

  size_t actual_num_tuples_returned = 0;
  for (auto &tile : result_tiles) {
    actual_num_tuples_returned += tile->GetTupleCount();
  }

  EXPECT_EQ(expected_num_tuples, actual_num_tuples_returned);

  // In case you want to see it by yourself ...
  ExecutorTestsUtil::PrintTileVector(result_tiles);
}

TEST(HashSetOptTests, ExceptTest) {
  // Create the plan node
  planner::SetOpPlan node(SETOP_TYPE_EXCEPT);

  // Create and set up executor
  executor::HashSetOpExecutor executor(&node, nullptr);

  MockExecutor child_executor1;
  MockExecutor child_executor2;

  executor.AddChild(&child_executor1);
  executor.AddChild(&child_executor2);

  EXPECT_CALL(child_executor1, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor2, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor1, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  EXPECT_CALL(child_executor2, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create two tables and wrap them in logical tiles.
  // The tables should be populated with the same data.
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  size_t tile_size = 10;

  auto txn1 = txn_manager.BeginTransaction();
  auto txn_id1 = txn1->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table1(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn1, data_table1.get(),
                                   tile_size * 5, false,
                                   false, false);

  auto txn2 = txn_manager.BeginTransaction();
  auto txn_id2 = txn2->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table2(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn2, data_table2.get(),
                                   tile_size * 5, false,
                                   false, false);

  // Create two mock tiles. They wrap two physical tiles that should contain the
  // same data.
  // But we invalidate the 2/5 tuples of the first mock tile
  // and the last 2/5 tuples of the second tile.
  // This setting allows us to test all possible set-op's.
  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table1->GetTileGroup(0), txn_id1));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table2->GetTileGroup(0), txn_id2));

  for (oid_t id = 0; id < tile_size * 2 / 5; id++) {
    source_logical_tile1->RemoveVisibility(id);
    source_logical_tile2->RemoveVisibility(tile_size - 1 - id);
  }

  EXPECT_CALL(child_executor1, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()));

  EXPECT_CALL(child_executor2, GetOutput())
      .WillOnce(Return(source_logical_tile2.release()));

  RunTest(executor, tile_size * 2 / 5);
}

TEST(HashSetOptTests, ExceptAllTest) {
  // Create the plan node
  planner::SetOpPlan node(SETOP_TYPE_EXCEPT_ALL);

  // Create and set up executor
  executor::HashSetOpExecutor executor(&node, nullptr);

  MockExecutor child_executor1;
  MockExecutor child_executor2;

  executor.AddChild(&child_executor1);
  executor.AddChild(&child_executor2);

  EXPECT_CALL(child_executor1, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor2, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor1, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  EXPECT_CALL(child_executor2, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create two tables and wrap them in logical tiles.
  // The tables should be populated with the same data.
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  size_t tile_size = 10;

  auto txn1 = txn_manager.BeginTransaction();
  auto txn_id1 = txn1->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table1(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn1,
                                   data_table1.get(), tile_size * 5, false,
                                   false, false);

  auto txn2 = txn_manager.BeginTransaction();
  auto txn_id2 = txn2->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table2(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn2,
                                   data_table2.get(), tile_size * 5, false,
                                   false, false);

  auto txn3 = txn_manager.BeginTransaction();
  auto txn_id3 = txn3->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table3(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn3,
                                   data_table3.get(), tile_size * 5, false,
                                   false, false);

  auto txn4 = txn_manager.BeginTransaction();
  auto txn_id4 = txn4->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table4(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn4,
                                   data_table4.get(), tile_size * 5, false,
                                   false, false);

  // Create four mock tiles.

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table1->GetTileGroup(0), txn_id1));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table2->GetTileGroup(0), txn_id2));

  std::unique_ptr<executor::LogicalTile> source_logical_tile3(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table3->GetTileGroup(0), txn_id3));

  std::unique_ptr<executor::LogicalTile> source_logical_tile4(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table4->GetTileGroup(0), txn_id4));

  for (oid_t id = 0; id < tile_size * 2 / 5; id++) {
    source_logical_tile1->RemoveVisibility(id);
    source_logical_tile2->RemoveVisibility(tile_size - 1 - id);
    source_logical_tile3->RemoveVisibility(id);
    source_logical_tile4->RemoveVisibility(tile_size - 1 - id);
  }

  EXPECT_CALL(child_executor1, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile3.release()));

  EXPECT_CALL(child_executor2, GetOutput())
      .WillOnce(Return(source_logical_tile2.release()))
      .WillOnce(Return(source_logical_tile4.release()));

  RunTest(executor, 2 * (tile_size * 2 / 5));
}

TEST(HashSetOptTests, IntersectTest) {
  // Create the plan node
  planner::SetOpPlan node(SETOP_TYPE_INTERSECT);

  // Create and set up executor
  executor::HashSetOpExecutor executor(&node, nullptr);

  MockExecutor child_executor1;
  MockExecutor child_executor2;

  executor.AddChild(&child_executor1);
  executor.AddChild(&child_executor2);

  EXPECT_CALL(child_executor1, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor2, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor1, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  EXPECT_CALL(child_executor2, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create two tables and wrap them in logical tiles.
  // The tables should be populated with the same data.
  size_t tile_size = 10;
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn1 = txn_manager.BeginTransaction();
  auto txn_id1 = txn1->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table1(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn1, data_table1.get(),
                                   tile_size * 5, false,
                                   false, false);

  auto txn2 = txn_manager.BeginTransaction();
  auto txn_id2 = txn2->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table2(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn2, data_table2.get(),
                                   tile_size * 5, false,
                                   false, false);

  // Create two mock tiles. They wrap two physical tiles that should contain the
  // same data.
  // But we invalidate the 2/5 tuples of the first mock tile
  // and the last 2/5 tuples of the second tile.
  // This setting allows us to test all possible set-op's.
  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table1->GetTileGroup(0), txn_id1));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table2->GetTileGroup(0), txn_id2));

  for (oid_t id = 0; id < tile_size * 2 / 5; id++) {
    source_logical_tile1->RemoveVisibility(id);
    source_logical_tile2->RemoveVisibility(tile_size - 1 - id);
  }

  EXPECT_CALL(child_executor1, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()));

  EXPECT_CALL(child_executor2, GetOutput())
      .WillOnce(Return(source_logical_tile2.release()));

  RunTest(executor, tile_size - 2 * (tile_size * 2 / 5));
}

TEST(HashSetOptTests, IntersectAllTest) {
  // Create the plan node
  planner::SetOpPlan node(SETOP_TYPE_INTERSECT_ALL);

  // Create and set up executor
  executor::HashSetOpExecutor executor(&node, nullptr);

  MockExecutor child_executor1;
  MockExecutor child_executor2;

  executor.AddChild(&child_executor1);
  executor.AddChild(&child_executor2);

  EXPECT_CALL(child_executor1, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor2, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor1, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  EXPECT_CALL(child_executor2, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  // Create two tables and wrap them in logical tiles.
  // The tables should be populated with the same data.
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  size_t tile_size = 10;

  auto txn1 = txn_manager.BeginTransaction();
  auto txn_id1 = txn1->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table1(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn1, data_table1.get(),
                                   tile_size * 5, false,
                                   false, false);

  auto txn2 = txn_manager.BeginTransaction();
  auto txn_id2 = txn2->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table2(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn2, data_table2.get(),
                                   tile_size * 5, false,
                                   false, false);

  auto txn3 = txn_manager.BeginTransaction();
  auto txn_id3 = txn3->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table3(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn3, data_table3.get(),
                                   tile_size * 5, false,
                                   false, false);

  auto txn4 = txn_manager.BeginTransaction();
  auto txn_id4 = txn4->GetTransactionId();
  std::unique_ptr<storage::DataTable> data_table4(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(txn4, data_table4.get(),
                                   tile_size * 5, false,
                                   false, false);

  // Create four mock tiles.

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table1->GetTileGroup(0), txn_id1));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table2->GetTileGroup(0), txn_id2));

  std::unique_ptr<executor::LogicalTile> source_logical_tile3(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table3->GetTileGroup(0), txn_id3));

  std::unique_ptr<executor::LogicalTile> source_logical_tile4(
      executor::LogicalTileFactory::WrapTileGroup(
          data_table4->GetTileGroup(0), txn_id4));

  for (oid_t id = 0; id < tile_size * 2 / 5; id++) {
    source_logical_tile1->RemoveVisibility(id);
    source_logical_tile2->RemoveVisibility(tile_size - 1 - id);
    source_logical_tile3->RemoveVisibility(id);
    source_logical_tile4->RemoveVisibility(tile_size - 1 - id);
  }

  EXPECT_CALL(child_executor1, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile3.release()));

  EXPECT_CALL(child_executor2, GetOutput())
      .WillOnce(Return(source_logical_tile2.release()))
      .WillOnce(Return(source_logical_tile4.release()));

  RunTest(executor, 2 * (tile_size - 2 * (tile_size * 2 / 5)));
}
}

}  // namespace test
}  // namespace peloton
