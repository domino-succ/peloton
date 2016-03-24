/*-------------------------------------------------------------------------
 *
 * simple_checkpoint.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/checkpoint/simple_checkpoint.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/logging/checkpoint/simple_checkpoint.h"
#include "backend/logging/log_manager.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/transaction.h"
#include "backend/executor/executors.h"
#include "backend/executor/executor_context.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/catalog/manager.h"
#include "backend/storage/tile.h"
#include "backend/storage/database.h"

#include "backend/common/logger.h"
#include "backend/common/types.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Simple Checkpoint
//===--------------------------------------------------------------------===//

SimpleCheckpoint &SimpleCheckpoint::GetInstance() {
  static SimpleCheckpoint simple_checkpoint;
  return simple_checkpoint;
}

void SimpleCheckpoint::Init() {
  // TODO check configuration
  if (true) {
    std::thread checkpoint_thread(&SimpleCheckpoint::DoCheckpoint, this);
    checkpoint_thread.detach();
  }
}

void SimpleCheckpoint::DoCheckpoint() {
  sleep(30);

  // while (true) {
  //======= construct  scan =============================================

  // build executor context
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // TODO should not use begin txn. Not ended?
  auto txn = txn_manager.BeginTransaction();
  assert(txn);

  LOG_TRACE("Txn ID = %lu ", txn->GetTransactionId());
  LOG_TRACE("Building the executor tree");
  auto executor_context = new executor::ExecutorContext(
      txn, bridge::PlanTransformer::BuildParams(nullptr));

  auto &log_manager = LogManager::GetInstance();

  auto checkpoint_pool = executor_context->GetExecutorContextPool();
  std::vector<LogRecord *> records;

  auto &catalog_manager = catalog::Manager::GetInstance();
  auto database_count = catalog_manager.GetDatabaseCount();

  for (oid_t database_idx = 0; database_idx < database_count; database_idx++) {
    auto database = catalog_manager.GetDatabase(database_idx);
    auto table_count = database->GetTableCount();
    auto database_oid = database->GetOid();
    for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
      /* Grab the target table */
      storage::DataTable *target_table = database->GetTable(table_idx);

      assert(target_table);
      LOG_INFO("SeqScan: database oid %lu table oid %lu: %s", database_idx,
               table_idx, target_table->GetName().c_str());

      auto schema = target_table->GetSchema();
      assert(schema);
      expression::AbstractExpression *predicate = nullptr;
      std::vector<oid_t> column_ids;
      column_ids.resize(target_table->GetSchema()->GetColumnCount());
      std::iota(column_ids.begin(), column_ids.end(), 0);

      /* Construct the Peloton plan node */
      auto scan_plan_node =
          new planner::SeqScanPlan(target_table, predicate, column_ids);

      executor::AbstractExecutor *scan_executor = nullptr;
      scan_executor =
          new executor::SeqScanExecutor(scan_plan_node, executor_context);

      LOG_TRACE("Initializing the executor tree");

      // Initialize the seq scan executor
      // bool init_failure = false;
      auto status = scan_executor->Init();

      // Abort and cleanup
      if (status == false) {
        // init_failure = true;
        txn->SetResult(Result::RESULT_FAILURE);
        //    goto cleanup;
      }
      LOG_TRACE("Running the seq scan executor");
      auto logger = log_manager.GetBackendLogger();

      //======= execute  scan =============================================
      // Execute seq scan until we get result tiles
      for (;;) {
        status = scan_executor->Execute();

        // Stop
        if (status == false) {
          break;
        }

        std::unique_ptr<executor::LogicalTile> logical_tile(
            scan_executor->GetOutput());
        auto tile_group_id = logical_tile->GetColumnInfo(0)
                                 .base_tile->GetTileGroup()
                                 ->GetTileGroupId();

        // Go over the logical tile
        for (oid_t tuple_id : *logical_tile) {
          expression::ContainerTuple<executor::LogicalTile> cur_tuple(
              logical_tile.get(), tuple_id);
          // Logging
          {
            // construct a physical tuple from the logical tuple
            storage::Tuple *tuple(new storage::Tuple(schema, true));
            for (auto column_id : column_ids) {
              tuple->SetValue(column_id, cur_tuple.GetValue(column_id),
                              checkpoint_pool);
            }
            ItemPointer location(tile_group_id, tuple_id);
            // TODO record->message is NULL
            auto record = logger->GetTupleRecord(
                LOGRECORD_TYPE_TUPLE_INSERT, txn->GetTransactionId(),
                target_table->GetOid(), location, INVALID_ITEMPOINTER, tuple,
                database_oid);
            assert(record);
            records.push_back(record);
          }
        }
      }
    }
  }
  //======= create file ==============================================

  // open log file and file descriptor
  // we open it in append + binary mode
  std::string file_name = "checkpoint.log";
  auto checkpoint_file = fopen(file_name.c_str(), "ab+");
  if (checkpoint_file == NULL) {
    LOG_ERROR("Checkpoint File is NULL");
  }

  // also, get the descriptor
  auto checkpoint_file_fd = fileno(checkpoint_file);
  if (checkpoint_file_fd == -1) {
    LOG_ERROR("log_file_fd is -1");
  }

  //===========flush ====================================================

  // First, write all the record in the queue
  for (auto record : records) {
    CopySerializeOutput output_buffer;
    record->Serialize(output_buffer);
    fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(),
           checkpoint_file);
  }

  // Then, flush
  int ret = fflush(checkpoint_file);
  if (ret != 0) {
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  // Finally, sync
  ret = fsync(checkpoint_file_fd);
  // fsync_count++;
  if (ret != 0) {
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }

  // Clean up the frontend logger's queue
  for (auto record : records) {
    delete record;
  }
  records.clear();

  //}
  // cleanup:
  // Do cleanup
  ;
}

}  // namespace logging
}  // namespace peloton
