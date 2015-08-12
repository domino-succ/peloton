/*-------------------------------------------------------------------------
 *
 * ariesfrontendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/ariesfrontendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/frontendlogger.h"
#include "backend/concurrency/transaction.h"

#include <fcntl.h>

namespace peloton {
namespace logging {

static std::vector<LogRecord> aries_global_queue;

//===--------------------------------------------------------------------===//
// Aries Frontend Logger 
//===--------------------------------------------------------------------===//

class AriesFrontendLogger : public FrontendLogger{

  public:

    AriesFrontendLogger(void);

   ~AriesFrontendLogger(void);

    void MainLoop(void);

    void CollectLogRecord(void);

    void Flush(void);

    void Recovery(void);

  private:

    // TODO :: Reorganizing

    bool ReadLogRecordHeader(LogRecordHeader& log_record_header);

    void ReadLogRecordBody(const LogRecordHeader log_record_header,
                           concurrency::Transaction* txn);

    storage::Tuple* ReadTuple(catalog::Schema* schema);

    storage::DataTable* GetTable(LogRecordHeader log_record_header);

    void InsertTuple(LogRecordHeader log_record_header, concurrency::Transaction* txn);

    void DeleteTuple(LogRecordHeader log_record_header, concurrency::Transaction* txn);
    
    void UpdateTuple(LogRecordHeader log_record_header, concurrency::Transaction* txn);

    size_t BodySizeCheck();

    size_t GetLogRecordCount(void) const;

    size_t LogFileSize(void);

    // FIXME :: Hard coded file name
    std::string filename = "/home/parallels/git/peloton/build/aries_log.txt";

    // FIXME :: Hard coded global_queue size
    oid_t aries_global_queue_size = 1;

    // File pointer and descriptor
    FILE* logFile;

    int logFileFd;

    // permit reading and writing by the owner, and to permit reading
    // only by group members and others.
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

};

}  // namespace logging
}  // namespace peloton
