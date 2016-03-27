#pragma once

#include "backend/logging/log_record.h"
#include "backend/common/serializer.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// TransactionRecord
//===--------------------------------------------------------------------===//

class TransactionRecord : public LogRecord {
 public:
  TransactionRecord(LogRecordType log_record_type,
                    const cid_t cid = INVALID_CID)
      : LogRecord(log_record_type, cid) {}

  ~TransactionRecord() {
    // Clean up the message
    delete[] message;
  }

  //===--------------------------------------------------------------------===//
  // Serial/Deserialization
  //===--------------------------------------------------------------------===//

  bool Serialize(CopySerializeOutput &output);

  void Deserialize(CopySerializeInputBE &input);

  static size_t GetTransactionRecordSize(void);

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  void Print(void);
};

}  // namespace logging
}  // namespace peloton
