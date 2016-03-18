//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction.h
//
// Identification: src/backend/concurrency/transaction.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <vector>
#include <map>
#include <unordered_map>

#include "backend/common/printable.h"
#include "backend/common/types.h"
#include "backend/common/exception.h"
#include "backend/concurrency/transaction_manager.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//

class Transaction : public Printable {
  friend class TransactionManager;

  Transaction(Transaction const &) = delete;

 public:
  Transaction()
      : txn_id(INVALID_TXN_ID),
        start_cid(INVALID_CID),
        end_cid(INVALID_CID),
        ref_count(BASE_REF_COUNT),
        waiting_to_commit(false),
        next(nullptr) {}

  Transaction(txn_id_t txn_id, cid_t start_cid)
      : txn_id(txn_id),
        start_cid(start_cid),
        end_cid(INVALID_CID),
        ref_count(BASE_REF_COUNT),
        waiting_to_commit(false),
        next(nullptr) {}

  ~Transaction() {
    if (next != nullptr) {
      next->DecrementRefCount();
    }
  }

  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  inline txn_id_t GetTransactionId() const { return txn_id; }

  inline cid_t GetStartCommitId() const { return start_cid; }

  inline cid_t GetEndCommitId() const { return end_cid; }

  // record read set
  void RecordRead(const ItemPointer &location);

  // record write set
  void RecordWrite(const ItemPointer &location);

  // record inserted tuple
  //void RecordInsert(ItemPointer location);

  // record deleted tuple
  //void RecordDelete(ItemPointer location);

  const std::map<oid_t, std::vector<oid_t>> &GetReadTuples();
  //const std::map<oid_t, std::vector<oid_t>> &GetInsertedTuples();

  const std::map<oid_t, std::vector<oid_t>> &GetWriteTuples();
  //const std::map<oid_t, std::vector<oid_t>> &GetDeletedTuples();

  // reset inserted tuples and deleted tuples
  // used by recovery (logging)
  void ResetState(void);

  // maintain reference counts for transactions
  inline void IncrementRefCount();

  inline void DecrementRefCount();

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Set result and status
  inline void SetResult(Result result);

  // Get result and status
  inline Result GetResult() const;

 protected:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // transaction id
  txn_id_t txn_id;

  // start commit id
  cid_t start_cid;

  // end commit id
  cid_t end_cid;

  // references
  std::atomic<size_t> ref_count;

  // waiting for commit ?
  std::atomic<bool> waiting_to_commit;

  // cid context
  Transaction *next __attribute__((aligned(16)));

  // inserted tuples
  std::map<oid_t, std::vector<oid_t>> read_tuples;

  // deleted tuples
  std::map<oid_t, std::vector<oid_t>> write_tuples;

  // synch helpers
  std::mutex txn_mutex;

  // result of the transaction
  Result result_ = peloton::RESULT_SUCCESS;
};

inline void Transaction::IncrementRefCount() { ++ref_count; }

inline void Transaction::DecrementRefCount() {
  // DROP transaction when ref count reaches 0
  // this returns the value immediately preceding the assignment
  if (ref_count.fetch_sub(1) == 1) {
    delete this;
  }
}

inline void Transaction::SetResult(Result result) { result_ = result; }

inline Result Transaction::GetResult() const { return result_; }

}  // End concurrency namespace
}  // End peloton namespace
