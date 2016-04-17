//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logging_test.cpp
//
// Identification: tests/logging/logging_test.cpp
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
#include "logging/logging_tests_util.h"

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Tests
//===--------------------------------------------------------------------===//

class LoggingTests : public PelotonTest {};

TEST_F(LoggingTests, BasicLoggingTest) {
  std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable(1));

  auto &log_manager = logging::LogManager::GetInstance();

  unsigned int num_backend_logger = 1;
  LoggingScheduler scheduler(num_backend_logger, &log_manager, table.get());

  scheduler.Init();
  // Logger 0 is always the front end logger
  // The first txn to commit starts with cid 2
  scheduler.Logger(1).Prepare();
  scheduler.Logger(1).Begin(2);
  scheduler.Logger(1).Insert(2);
  scheduler.Logger(1).Commit(2);
  scheduler.Logger(0).Collect();
  scheduler.Logger(0).Flush();
  scheduler.Logger(1).Done(1);
  scheduler.Run();

  auto results = scheduler.log_threads[0].results;
  EXPECT_EQ(2, results[0]);
}

}  // End test namespace
}  // End peloton namespace
