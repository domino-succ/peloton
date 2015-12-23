/*-------------------------------------------------------------------------
 *
 * pelotonfrontendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/pelotonfrontendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <sys/mman.h>

#include "backend/common/exception.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/logging/loggers/peloton_frontend_logger.h"
#include "backend/logging/loggers/peloton_backend_logger.h"

namespace peloton {
namespace logging {

size_t GetLogFileSize(int log_file_fd);

LogRecordType GetNextLogRecordType(FILE *log_file, size_t log_file_size);

bool ReadTransactionRecordHeader(TransactionRecord &txn_record,
                                 FILE *log_file,
                                 size_t log_file_size);

bool ReadTupleRecordHeader(TupleRecord& tuple_record,
                           FILE *log_file,
                           size_t log_file_size);

/**
 * @brief create NVM backed log pool
 */
PelotonFrontendLogger::PelotonFrontendLogger() {

  logging_type = LOGGING_TYPE_PELOTON;

  // open log file and file descriptor
  // we open it in append + binary mode
  log_file = fopen(GetLogFileName().c_str(),"ab+");
  if(log_file == NULL) {
    LOG_ERROR("LogFile is NULL");
  }

  // also, get the descriptor
  log_file_fd = fileno(log_file);
  if( log_file_fd == -1) {
    LOG_ERROR("log_file_fd is -1");
  }
}

/**
 * @brief clean NVM space
 */
PelotonFrontendLogger::~PelotonFrontendLogger() {
  // Clean up the log records in global queue
  for (auto log_record : global_queue) {
    delete log_record;
  }

  // Clean up the global record pool
  global_peloton_log_record_pool.Clear();
}

//===--------------------------------------------------------------------===//
// Active Processing
//===--------------------------------------------------------------------===//

/**
 * @brief flush all log records to the file
 */
void PelotonFrontendLogger::FlushLogRecords(void) {

  std::vector<txn_id_t> committed_txn_list;
  std::vector<txn_id_t> not_committed_txn_list;

  //===--------------------------------------------------------------------===//
  // Collect the log records
  //===--------------------------------------------------------------------===//

  for (auto record : global_queue) {

    switch (record->GetType()) {
      case LOGRECORD_TYPE_TRANSACTION_BEGIN:
        global_peloton_log_record_pool.CreateTransactionLogList(record->GetTransactionId());
        break;

      case LOGRECORD_TYPE_TRANSACTION_COMMIT:
        committed_txn_list.push_back(record->GetTransactionId());
        break;

      case LOGRECORD_TYPE_TRANSACTION_ABORT:
        // Nothing to be done for abort
        break;

      case LOGRECORD_TYPE_TRANSACTION_END:
      case LOGRECORD_TYPE_TRANSACTION_DONE:
        // if a txn is not committed (aborted or active), log records will be removed here
        // Note that list is not be removed immediately, it is removed only after flush and commit.
        not_committed_txn_list.push_back(record->GetTransactionId());
        break;

      case LOGRECORD_TYPE_PELOTON_TUPLE_INSERT:
      case LOGRECORD_TYPE_PELOTON_TUPLE_DELETE:
      case LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE: {
        // Check the commit information,
        auto collected_tuple_record = CollectTupleRecord(reinterpret_cast<TupleRecord*>(record));

        // Don't delete record if CollectTupleRecord returned true
        if(collected_tuple_record == false)
          delete record;
      }
      break;

      case LOGRECORD_TYPE_INVALID:
      default:
        throw Exception("Invalid or unrecogized log record found");
        break;
    }

  }

  // Clear the global queue
  global_queue.clear();

  //===--------------------------------------------------------------------===//
  // Write out the log records
  //===--------------------------------------------------------------------===//

  // If committed txn list is not empty
  if (committed_txn_list.empty() == false) {

    // First, write out all the committed log records
    size_t written_log_record_count = WriteLogRecords(committed_txn_list);

    // Now, write a committing log entry to file
    // piggyback the number of written log records as a "txn_id" in this log
    WriteTransactionLogRecord(TransactionRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT, written_log_record_count));

    //===--------------------------------------------------------------------===//
    // Toggle the commit marks
    //===--------------------------------------------------------------------===//

    // Toggle the commit marks
    ToggleCommitMarks(committed_txn_list);

    // Write out a transaction done log record to file
    WriteTransactionLogRecord(TransactionRecord(LOGRECORD_TYPE_TRANSACTION_DONE));

  }

  //===--------------------------------------------------------------------===//
  // Clean up finished transaction log lists
  //===--------------------------------------------------------------------===//

  // remove any finished txn logs
  for (txn_id_t txn_id : not_committed_txn_list) {
    global_peloton_log_record_pool.RemoveTransactionLogList(txn_id);
  }

  // Notify the backend loggers
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    for (auto backend_logger : backend_loggers) {
      backend_logger->Commit();
    }
  }

}

size_t PelotonFrontendLogger::WriteLogRecords(std::vector<txn_id_t> committed_txn_list) {

  size_t written_log_record_count = 0;

  // Write out the log records of all the committed transactions to log file
  for (txn_id_t txn_id : committed_txn_list) {

    // Locate the transaction log list for this txn id
    auto txn_log_list = global_peloton_log_record_pool.SearchLogRecordList(txn_id);
    if (txn_log_list == nullptr) {
      continue;
    }

    written_log_record_count += txn_log_list->size();

    // Write out all the records in the list
    for (size_t txn_log_list_itr = 0;
        txn_log_list_itr < txn_log_list->size();
        txn_log_list_itr++) {

      // Write out the log record
      TupleRecord *record = txn_log_list->at(txn_log_list_itr);
      fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(), log_file);

    }
  }

  // No need to flush now, will flush later in WriteTxnLog
  return written_log_record_count;
}

void PelotonFrontendLogger::WriteTransactionLogRecord(TransactionRecord txn_log_record) {

  txn_log_record.Serialize(output_buffer);
  fwrite(txn_log_record.GetMessage(), sizeof(char), txn_log_record.GetMessageLength(),
         log_file);

  // Then, flush
  int ret = fflush(log_file);
  if (ret != 0) {
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  // Finally, sync
  ret = fsync(log_file_fd);
  if (ret != 0) {
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }
}


void PelotonFrontendLogger::ToggleCommitMarks(std::vector<txn_id_t> committed_txn_list) {

  // flip commit marks
  for (txn_id_t txn_id : committed_txn_list) {

    auto txn_log_list = global_peloton_log_record_pool.SearchLogRecordList(txn_id);
    if (txn_log_list == nullptr) {
      continue;
    }

    for (size_t txn_log_list_itr = 0;
        txn_log_list_itr < txn_log_list->size();
        txn_log_list_itr++) {
      // Get the log record
      TupleRecord *record = txn_log_list->at(txn_log_list_itr);
      cid_t current_commit_id = INVALID_CID;

      auto record_type = record->GetType();
      switch (record_type) {
        case LOGRECORD_TYPE_PELOTON_TUPLE_INSERT:
          current_commit_id = SetInsertCommitMark(record->GetInsertLocation());
          break;

        case LOGRECORD_TYPE_PELOTON_TUPLE_DELETE:
          current_commit_id = SetDeleteCommitMark(record->GetDeleteLocation());
          break;

        case LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE:
          SetDeleteCommitMark(record->GetDeleteLocation());
          current_commit_id = SetInsertCommitMark(record->GetInsertLocation());
          break;

        default:
          break;
      }

      // Update latest commit id
      if (latest_commit_id < current_commit_id) {
        latest_commit_id = current_commit_id;
      }
    }

    // TODO: All records are committed, its safe to remove them now
    global_peloton_log_record_pool.RemoveTransactionLogList(txn_id);
  }

}

bool PelotonFrontendLogger::CollectTupleRecord(TupleRecord* record) {

  if (record == nullptr) {
    return false;
  }

  auto record_type = record->GetType();
  if (record_type == LOGRECORD_TYPE_PELOTON_TUPLE_INSERT
      || record_type == LOGRECORD_TYPE_PELOTON_TUPLE_DELETE
      || record_type == LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE) {

    // Collect this log record
    auto status = global_peloton_log_record_pool.AddLogRecord(record);
    return (status == 0);
  }
  else {
    return false;
  }
}

cid_t PelotonFrontendLogger::SetInsertCommitMark(ItemPointer location) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  auto tile_group_header = tile_group->GetHeader();

  // Set the commit mark
  if (tile_group_header->GetInsertCommit(location.offset) == false) {
    tile_group_header->SetInsertCommit(location.offset, true);
  }

  LOG_TRACE("<%p, %lu> : slot is insert committed", tile_group.get(), location.offset);

  // Update max oid
  if( max_oid < location.block ){
    max_oid = location.block;
  }

  // TODO: sync changes
  auto begin_commit_id = tile_group_header->GetBeginCommitId(location.offset);
  return begin_commit_id;
}

cid_t PelotonFrontendLogger::SetDeleteCommitMark(ItemPointer location) {
  // Commit Insert Mark
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  auto tile_group_header = tile_group->GetHeader();

  if (tile_group_header->GetDeleteCommit(location.offset) == false) {
    tile_group_header->SetDeleteCommit(location.offset, true);
  }

  LOG_TRACE("<%p, %lu> : slot is delete committed", tile_group.get(), location.offset);

  // Update max oid
  if( max_oid < location.block ){
    max_oid = location.block;
  }

  // TODO: sync changes
  auto end_commit_id = tile_group_header->GetEndCommitId(location.offset);
  return end_commit_id;
}

//===--------------------------------------------------------------------===//
// Recovery 
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void PelotonFrontendLogger::DoRecovery() {

  // Set log file size
  log_file_size = GetLogFileSize(log_file_fd);

  // Go over the log size if needed
  if (log_file_size > 0) {
    bool reached_end_of_file = false;

    // check whether first item is LOGRECORD_TYPE_TRANSACTION_COMMIT
    // if not, no need to do recovery.
    // if yes, need to replay all log records before we hit LOGRECORD_TYPE_TRANSACTION_DONE
    bool need_recovery = NeedRecovery();
    if (need_recovery == true) {

      TransactionRecord dummy_transaction_record(LOGRECORD_TYPE_INVALID);
      cid_t current_commit_id = INVALID_CID;

      // Go over each log record in the log file
      while (reached_end_of_file == false) {

        // Read the first byte to identify log record type
        // If that is not possible, then wrap up recovery
        LogRecordType log_type = GetNextLogRecordType(log_file, log_file_size);

        switch (log_type) {

          case LOGRECORD_TYPE_TRANSACTION_DONE:
          case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
            // read but do nothing
            ReadTransactionRecordHeader(dummy_transaction_record, log_file, log_file_size);
          }
          break;

          case LOGRECORD_TYPE_PELOTON_TUPLE_INSERT: {
            TupleRecord insert_record(LOGRECORD_TYPE_PELOTON_TUPLE_INSERT);
            ReadTupleRecordHeader(insert_record, log_file, log_file_size);
            current_commit_id = SetInsertCommitMark(insert_record.GetInsertLocation());
          }
          break;

          case LOGRECORD_TYPE_PELOTON_TUPLE_DELETE: {
            TupleRecord delete_record(LOGRECORD_TYPE_PELOTON_TUPLE_DELETE);
            ReadTupleRecordHeader(delete_record, log_file, log_file_size);
            current_commit_id = SetDeleteCommitMark(delete_record.GetDeleteLocation());
          }
          break;

          case LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE: {
            TupleRecord update_record(LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE);
            ReadTupleRecordHeader(update_record, log_file, log_file_size);
            SetDeleteCommitMark(update_record.GetDeleteLocation());
            current_commit_id = SetInsertCommitMark(update_record.GetInsertLocation());
          }
          break;

          default:
            reached_end_of_file = true;
            break;
        }
      }

      // Update latest commit id
      if (latest_commit_id < current_commit_id) {
        latest_commit_id = current_commit_id;
      }

      // write out a trasaction done log record to file
      // to avoid redo next time during recovery
      WriteTransactionLogRecord(TransactionRecord(LOGRECORD_TYPE_TRANSACTION_DONE));
    }

    // After finishing recovery, set the next oid with maximum oid
    // observed during the recovery
    auto &manager = catalog::Manager::GetInstance();
    manager.SetNextOid(max_oid);

  }

  // TODO: How to reset transaction manager with latest cid, if no item is recovered
}

// Check whether need to recovery, if yes, reset fseek to the right place.
bool PelotonFrontendLogger::NeedRecovery(void) {

  // Otherwise, read the last transaction record
  fseek(log_file, -TransactionRecord::GetTransactionRecordSize(), SEEK_END);

  // Get its type
  auto log_record_type = GetNextLogRecordType(log_file, log_file_size);

  // Check if the previous transaction run is broken
  if( log_record_type == LOGRECORD_TYPE_TRANSACTION_COMMIT){
    TransactionRecord txn_record(LOGRECORD_TYPE_TRANSACTION_COMMIT);

    // read the last written out transaction log record
    if( ReadTransactionRecordHeader(txn_record, log_file, log_file_size) == false ) {
      return false;
    }

    // Peloton log records items have fixed size.
    // Compute log offset based on txn_id
    size_t tuple_log_record_count = txn_record.GetTransactionId();

    size_t rollback_offset = tuple_log_record_count * TupleRecord::GetTupleRecordSize()
    + TransactionRecord::GetTransactionRecordSize();

    // Rollback to the computed offset
    fseek(log_file, -rollback_offset, SEEK_END);
    return true;
  }
  else {
    return false;
  }

}

std::string PelotonFrontendLogger::GetLogFileName(void) {
  auto& log_manager = logging::LogManager::GetInstance();
  return log_manager.GetLogFileName();
}


}  // namespace logging
}  // namespace peloton
