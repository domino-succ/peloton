/*-------------------------------------------------------------------------
 *
 * pelotonfrontendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/pelotonfrontendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/frontend_logger.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/records/log_record_pool.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Peloton Frontend Logger 
//===--------------------------------------------------------------------===//

class PelotonFrontendLogger : public FrontendLogger {

  public:

    PelotonFrontendLogger(void);

   ~PelotonFrontendLogger(void);

    void FlushLogRecords(void);

    // Used by flush to update the commit mark
    bool CollectTupleRecord(TupleRecord* record);

    //===--------------------------------------------------------------------===//
    // Recovery 
    //===--------------------------------------------------------------------===//

    void DoRecovery(void);

    cid_t SetInsertCommitMark(ItemPointer location);

    cid_t SetDeleteCommitMark(ItemPointer location);

    //===--------------------------------------------------------------------===//
    // Utility functions
    //===--------------------------------------------------------------------===//

    size_t FlushRecords(std::vector<txn_id_t> committing_list);

    void CommitRecords(std::vector<txn_id_t> committing_list);

  private:
    std::string GetLogFileName(void);

    bool DoNeedRecovery(void);

    void WriteTxnLog(TransactionRecord txnLog);

    //===--------------------------------------------------------------------===//
    // Member Variables
    //===--------------------------------------------------------------------===//

    CopySerializeOutput output_buffer;

    // File pointer and descriptor
    FILE* log_file;
    int log_file_fd;

    // Size of the log file
    size_t log_file_size;

    // Global pool
    LogRecordPool global_peloton_log_record_pool;

    // Keep tracking max oid for setting next_oid in manager
    // For active processing after recovery
    oid_t max_oid = INVALID_OID;
    // Keep tracking latest cid for setting next commit in txn manager
    cid_t latest_cid = INVALID_CID;
};

}  // namespace logging
}  // namespace peloton
