//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.cpp
//
// Identification: src/backend/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "transaction_manager.h"

namespace peloton {
namespace concurrency {

// Current transaction for the backend thread
thread_local Transaction *current_txn;
thread_local cid_t latest_read_ts = INVALID_CID;

bool TransactionManager::IsOccupied(const void *position_ptr) {
  ItemPointer &position = *((ItemPointer*)position_ptr);
  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(position.block)->GetHeader();
  auto tuple_id = position.offset;

  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    return false;
  }
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);

  // there are exactly two versions that can be owned by a transaction.
  // unless it is an insertion.
  if (own == true) {
    if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
      assert(tuple_end_cid == MAX_CID);
      // the only version that is visible is the newly inserted one.
      LOG_TRACE("the version is newly inserted and is owned by myself");
      return true;
    } else {
      // the older version is not visible.
      return false;
    }
  } else {
    bool activated = (current_txn->GetBeginCommitId() >= tuple_begin_cid);
    bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // uncommitted version.
        if (tuple_end_cid == INVALID_CID) {
          // dirty delete is invisible
          return false;
        } else {
          // dirty update or insert is visible
          LOG_TRACE("the version is dirty update or insert and is visible");
          return true;
        }
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          LOG_TRACE("the older version is visible");
          return true;
        } else {
          return false;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      if (activated && !invalidated) {
        LOG_TRACE("the tuple is not owned by any transaction and is visible: begin_cid = %lu, end_cid = %lu", tuple_begin_cid, tuple_end_cid);
        return true;
      } else {
        return false;
      }
    }
  }
}

}  // End storage namespace
}  // End peloton namespace
