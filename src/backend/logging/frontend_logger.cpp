//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// frontend_logger.cpp
//
// Identification: src/backend/logging/frontend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>

#include "backend/common/logger.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/checkpoint_manager.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/checkpoint.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/logging/loggers/wbl_frontend_logger.h"

// configuration for testing
extern int64_t peloton_wait_timeout;

namespace peloton {
namespace logging {

FrontendLogger::FrontendLogger(){
  logger_type = LOGGER_TYPE_FRONTEND;

  // Set wait timeout
  wait_timeout = peloton_wait_timeout;
}

FrontendLogger::~FrontendLogger() {
  for (auto backend_logger : backend_loggers) {
    delete backend_logger;
  }
}

/** * @brief Return the frontend logger based on logging type
 * @param logging type can be write ahead logging or write behind logging
 */
FrontendLogger *FrontendLogger::GetFrontendLogger(LoggingType logging_type) {
  FrontendLogger *frontend_logger = nullptr;

  if (IsBasedOnWriteAheadLogging(logging_type) == true) {
    frontend_logger = new WriteAheadFrontendLogger();
  } else if (IsBasedOnWriteBehindLogging(logging_type) == true) {
    frontend_logger = new WriteBehindFrontendLogger();
  } else {
    LOG_ERROR("Unsupported logging type");
  }

  return frontend_logger;
}

/**
 * @brief MainLoop
 */
void FrontendLogger::MainLoop(void) {
  auto &log_manager = LogManager::GetInstance();

  /////////////////////////////////////////////////////////////////////
  // STANDBY MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("FrontendLogger Standby Mode");

  // Standby before we need to do RECOVERY
  log_manager.WaitForModeTransition(LOGGING_STATUS_TYPE_STANDBY, false);

  // Do recovery if we can, otherwise terminate
  switch (log_manager.GetLoggingStatus()) {
    case LOGGING_STATUS_TYPE_RECOVERY: {
      LOG_TRACE("Frontendlogger] Recovery Mode");

      /////////////////////////////////////////////////////////////////////
      // RECOVERY MODE
      /////////////////////////////////////////////////////////////////////

      // First, do recovery if needed
      LOG_INFO("Log manager: Invoking DoRecovery");
      DoRecovery();
      LOG_INFO("Log manager: DoRecovery done");

      // Now, enter LOGGING mode
      log_manager.SetLoggingStatus(LOGGING_STATUS_TYPE_LOGGING);

      break;
    }

    case LOGGING_STATUS_TYPE_LOGGING: {
      LOG_TRACE("Frontendlogger] Logging Mode");
    } break;

    default:
      break;
  }

  /////////////////////////////////////////////////////////////////////
  // LOGGING MODE
  /////////////////////////////////////////////////////////////////////

  // Periodically, wake up and do logging
  while (log_manager.GetLoggingStatus() == LOGGING_STATUS_TYPE_LOGGING) {
    // Collect LogRecords from all backend loggers
    // LOG_INFO("Log manager: Invoking CollectLogRecordsFromBackendLoggers");
    CollectLogRecordsFromBackendLoggers();

    // Flush the data to the file
    // LOG_INFO("Log manager: Invoking FlushLogRecords");
    FlushLogRecords();
  }

  /////////////////////////////////////////////////////////////////////
  // TERMINATE MODE
  /////////////////////////////////////////////////////////////////////

  // flush any remaining log records
  CollectLogRecordsFromBackendLoggers();
  FlushLogRecords();

  /////////////////////////////////////////////////////////////////////
  // SLEEP MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger Sleep Mode");

  // Setting frontend logger status to sleep
  log_manager.SetLoggingStatus(LOGGING_STATUS_TYPE_SLEEP);
}

/**
 * @brief Collect the log records from BackendLoggers
 */
void FrontendLogger::CollectLogRecordsFromBackendLoggers() {
  auto sleep_period = std::chrono::microseconds(wait_timeout);
  std::this_thread::sleep_for(sleep_period);

  {
    cid_t max_committed_cid = 0;
    cid_t lower_bound = MAX_CID;

    // TODO: handle edge cases here (backend logger has not yet sent a log
    // message)

    // Look at the local queues of the backend loggers
    backend_loggers_lock.Lock();
    LOG_TRACE("Collect log buffers from %lu backend loggers",
              backend_loggers.size());
    for (auto backend_logger : backend_loggers) {
      {
        backend_logger->PrepareLogBuffers();
        auto &log_buffers = backend_logger->GetLogBuffers();
        auto log_buffer_size = log_buffers.size();

        // update max_possible_commit_id with the latest buffer

        // Skip current backend_logger, nothing to do
        if (log_buffer_size == 0) continue;

        // Move the log record from backend_logger to here
        for (oid_t log_record_itr = 0; log_record_itr < log_buffer_size;
             log_record_itr++) {
          // copy to front end logger
          cid_t buffer_lower_bound =
              log_buffers[log_record_itr]->GetLoggingCidLowerBound();
          cid_t buffer_committed =
              log_buffers[log_record_itr]->GetHighestCommittedTransaction();
          if (buffer_lower_bound != INVALID_CID) {
            lower_bound = std::min(lower_bound, buffer_lower_bound);
          }
          if (buffer_committed != INVALID_CID) {
            max_committed_cid = std::max(max_committed_cid, buffer_committed);
          }

          global_queue.push_back(std::move(log_buffers[log_record_itr]));
        }
        // cleanup the local queue
        log_buffers.clear();
      }
    }
    cid_t max_possible_commit_id;
    if (max_committed_cid == 0 && lower_bound == MAX_CID) {
      // nothing collected
      max_possible_commit_id = max_collected_commit_id;
    } else if (max_committed_cid == 0) {
      max_possible_commit_id = lower_bound;
    } else if (lower_bound == MAX_CID) {
      max_possible_commit_id = max_committed_cid;
    } else {
      max_possible_commit_id = lower_bound;
    }
    // max_collected_commit_id should never decrease
    assert(max_possible_commit_id >= max_collected_commit_id);
    max_collected_commit_id = max_possible_commit_id;

    backend_loggers_lock.Unlock();
  }
}

cid_t FrontendLogger::GetMaxFlushedCommitId() { return max_flushed_commit_id; }

void FrontendLogger::SetBackendLoggerLoggedCid(BackendLogger &bel) {
  backend_loggers_lock.Lock();
  bel.SetLoggingCidLowerBound(max_collected_commit_id);
  backend_loggers_lock.Unlock();
}

/**
 * @brief Add backend logger to the list of backend loggers
 * @param backend logger
 */
void FrontendLogger::AddBackendLogger(BackendLogger *backend_logger) {
  // Grant empty buffers
  for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
    std::unique_ptr<LogBuffer> buffer(new LogBuffer(backend_logger));
    backend_logger->GrantEmptyBuffer(std::move(buffer));
  }
  // Add backend logger to the list of backend loggers
  backend_loggers_lock.Lock();
  backend_logger->SetLoggingCidLowerBound(max_collected_commit_id);
  backend_loggers.push_back(backend_logger);
  backend_loggers_lock.Unlock();
}

}  // namespace logging
}
