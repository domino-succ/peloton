//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logging_test.cpp
//
// Identification: tests/planner/checkpoint_test.cpp
//
// Copyright (c) 2016, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"


#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "backend/logging/loggers/wal_frontend_logger.h"

#include "executor/mock_executor.h"
#include "executor/executor_tests_util.h"

#define DEFAULT_RECOVERY_CID 15

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Checkpoint Tests
//===--------------------------------------------------------------------===//

class LoggingTests : public PelotonTest {};

std::vector<storage::Tuple *> BuildLoggingTuples(storage::DataTable *table,
                                          int num_rows, bool mutate,
                                          bool random) {
  std::vector<storage::Tuple *> tuples;
  LOG_INFO("build a vector of %d tuples", num_rows);

  // Random values
  std::srand(std::time(nullptr));
  const catalog::Schema *schema = table->GetSchema();
  // Ensure that the tile group is as expected.
  assert(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  const bool allocate = true;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  for (int rowid = 0; rowid < num_rows; rowid++) {
    int populate_value = rowid;
    if (mutate) populate_value *= 3;

    storage::Tuple *tuple = new storage::Tuple(schema, allocate);

    // First column is unique in this case
    tuple->SetValue(0,
                    ValueFactory::GetIntegerValue(
                        ExecutorTestsUtil::PopulatedValue(populate_value, 0)),
                    testing_pool);

    // In case of random, make sure this column has duplicated values
    tuple->SetValue(
        1, ValueFactory::GetIntegerValue(ExecutorTestsUtil::PopulatedValue(
               random ? std::rand() % (num_rows / 3) : populate_value, 1)),
        testing_pool);

    tuple->SetValue(
        2, ValueFactory::GetDoubleValue(ExecutorTestsUtil::PopulatedValue(
               random ? std::rand() : populate_value, 2)),
        testing_pool);

    // In case of random, make sure this column has duplicated values
    Value string_value = ValueFactory::GetStringValue(
        std::to_string(ExecutorTestsUtil::PopulatedValue(
            random ? std::rand() % (num_rows / 3) : populate_value, 3)));
    tuple->SetValue(3, string_value, testing_pool);
    tuples.push_back(tuple);
  }
  return tuples;
}

TEST(LoggingTests, BasicInsertTest) {
  auto recovery_table = ExecutorTestsUtil::CreateTable(1024);
  auto &manager =  catalog::Manager::GetInstance();
  storage::Database db(DEFAULT_DB_ID);
  manager.AddDatabase(&db);
  db.AddTable(recovery_table);

  auto tuples = BuildLoggingTuples(recovery_table, 1, false, false);
  printf("%lu\n", recovery_table->GetDatabaseOid());
  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 0);
  EXPECT_EQ(recovery_table->GetTileGroupCount(), 1);
  EXPECT_EQ(tuples.size(),1 );
  logging::WriteAheadFrontendLogger fel(true);
//  auto bel = logging::WriteAheadBackendLogger::GetInstance();
  cid_t test_commit_id = 10;

  Value val0 = tuples[0]->GetValue(0);
  Value val1 = tuples[0]->GetValue(1);
  Value val2 = tuples[0]->GetValue(2);
  Value val3 = tuples[0]->GetValue(3);
  for(auto tuple : tuples){
      logging::TupleRecord curr_rec(LOGRECORD_TYPE_TUPLE_INSERT, test_commit_id,
		              recovery_table->GetOid(), ItemPointer(10,5),
			      INVALID_ITEMPOINTER, tuple, DEFAULT_DB_ID);
      curr_rec.SetTuple(tuple);
      fel.InsertTuple(&curr_rec);
      curr_rec.SetTuple(nullptr);
  }

  EXPECT_TRUE(val0.Compare(recovery_table->GetTileGroupById(10)->GetValue(5, 0)) == 0);
  EXPECT_TRUE(val1.Compare(recovery_table->GetTileGroupById(10)->GetValue(5, 1)) == 0);
  EXPECT_TRUE(val2.Compare(recovery_table->GetTileGroupById(10)->GetValue(5, 2)) == 0);
  EXPECT_TRUE(val3.Compare(recovery_table->GetTileGroupById(10)->GetValue(5, 3)) == 0);

  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 1);
  EXPECT_EQ(recovery_table->GetTileGroupCount(), 2);
//  delete recovery_table;

}

TEST(LoggingTests, BasicUpdateTest) {

}

TEST(LoggingTests, BasicDeleteTest) {

}

TEST(LoggingTests, OutOfOrderCommitTest){

}

}  // End test namespace
}  // End peloton namespace
