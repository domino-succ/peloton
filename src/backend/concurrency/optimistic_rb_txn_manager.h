//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// optimistic_rb_txn_manager.h
//
// Identification: src/backend/concurrency/optimistic_rb_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/rollback_segment.h"
#include "backend/storage/data_table.h"
#include "backend/index/rb_btree_index.h"

namespace peloton {

namespace concurrency {

// Each transaction has a RollbackSegmentPool
extern thread_local storage::RollbackSegmentPool *current_segment_pool;
extern thread_local cid_t latest_read_timestamp;
extern thread_local std::unordered_map<ItemPointer, index::RBItemPointer *> updated_index_entries;
//===--------------------------------------------------------------------===//
// optimistic concurrency control with rollback segment
//===--------------------------------------------------------------------===//

class OptimisticRbTxnManager : public TransactionManager {
public:
  typedef char* RBSegType;

  OptimisticRbTxnManager() {}

  virtual ~OptimisticRbTxnManager() {}

  static OptimisticRbTxnManager &GetInstance();

  virtual VisibilityType IsVisible(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  virtual bool IsOwner(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id);

  virtual bool IsOwnable(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  inline bool IsInserted(
      const storage::TileGroupHeader *const tile_grou_header,
      const oid_t &tuple_id) {
      PL_ASSERT(IsOwner(tile_grou_header, tuple_id));
      return tile_grou_header->GetBeginCommitId(tuple_id) == MAX_CID;
  }

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id, const bool is_blind_write = false);

  virtual void YieldOwnership(const oid_t &tile_group_id,
    const oid_t &tuple_id);

  bool ValidateRead( 
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id,
    const cid_t &end_cid);


  virtual bool PerformInsert(const ItemPointer &location);

  bool PerformInsert(const ItemPointer &location, index::RBItemPointer *rb_item_ptr);

  // Get the read timestamp of the latest transaction on this thread, it is 
  // either the begin commit time of current transaction of the just committed
  // transaction.
  cid_t GetLatestReadTimestamp() {
    return latest_read_timestamp;
  }

  /**
   * Deprecated interfaces
   */
  virtual bool PerformRead(const ItemPointer &location);

  virtual void PerformUpdate(const ItemPointer &old_location __attribute__((unused)),
                             const ItemPointer &new_location __attribute__((unused)), UNUSED_ATTRIBUTE const bool is_blind_write = false) { PL_ASSERT(false); }

  virtual void PerformDelete(const ItemPointer &old_location  __attribute__((unused)),
                             const ItemPointer &new_location __attribute__((unused))) { PL_ASSERT(false); }

  virtual void PerformUpdate(const ItemPointer &location  __attribute__((unused))) { PL_ASSERT(false); }

  /**
   * Interfaces for rollback segment
   */

  // Add a new rollback segment to the tuple
  void PerformUpdateWithRb(const ItemPointer &location, char *new_rb_seg);

  // Insert a version, basically maintain secondary index
  bool RBInsertVersion(storage::DataTable *target_table,
    const ItemPointer &location, const storage::Tuple *tuple);

  // Rollback the master copy of a tuple to the status at the begin of the 
  // current transaction
  void RollbackTuple(std::shared_ptr<storage::TileGroup> tile_group,
                            const oid_t tuple_id);

  // Whe a txn commits, it needs to set an end timestamp to all RBSeg it has
  // created in order to make them invisible to future transactions
  void InstallRollbackSegments(storage::TileGroupHeader *tile_group_header,
                                const oid_t tuple_id, const cid_t end_cid);

  /**
   * @brief Test if a reader with read timestamp @read_ts should follow on the
   * rb chain started from rb_seg
   */
  inline bool IsRBVisible(char *rb_seg, cid_t read_ts) {
    if (rb_seg == nullptr) {
      return false;
    }

    cid_t rb_ts = storage::RollbackSegmentPool::GetTimeStamp(rb_seg);

    return read_ts < rb_ts;
  }

  // Return nullptr if the tuple is not activated to current txn.
  // Otherwise return the version that current tuple is activated
  inline char* GetActivatedRB(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_slot_id) {
    cid_t txn_begin_cid = current_txn->GetBeginCommitId();
    cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_slot_id);

    PL_ASSERT(tuple_begin_cid != MAX_CID);
    // Owner can not call this function
    PL_ASSERT(IsOwner(tile_group_header, tuple_slot_id) == false);

    RBSegType rb_seg = GetRbSeg(tile_group_header, tuple_slot_id);
    char *prev_visible;
    bool master_activated = (txn_begin_cid >= tuple_begin_cid);

    if (master_activated)
      prev_visible = tile_group_header->GetReservedFieldRef(tuple_slot_id);
    else
      prev_visible = nullptr;

    while (IsRBVisible(rb_seg, txn_begin_cid)) {
      prev_visible = rb_seg;
      rb_seg = storage::RollbackSegmentPool::GetNextPtr(rb_seg);
    }

    return prev_visible;
  }

  virtual void PerformDelete(const ItemPointer &location);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  virtual Transaction *BeginTransaction() {
    // Set current transaction
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();

    LOG_TRACE("Beginning transaction %lu", txn_id);


    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    auto eid = EpochManagerFactory::GetInstance().EnterEpoch(begin_cid);
    txn->SetEpochId(eid);

    latest_read_timestamp = begin_cid;
    // Create current transaction poll
    current_segment_pool = new storage::RollbackSegmentPool(BACKEND_TYPE_MM);

    return txn;
  }

  virtual void EndTransaction() {
    auto result = current_txn->GetResult();
    auto end_cid = current_txn->GetEndCommitId();

    if (result == RESULT_SUCCESS) {
      // Committed
      if (current_txn->IsReadOnly()) {
        // read only txn, just delete the segment pool because it's empty
        delete current_segment_pool;
      } else {
        // It's not read only txn
        current_segment_pool->SetPoolTimestamp(end_cid);
        living_pools_[end_cid] = std::shared_ptr<peloton::storage::RollbackSegmentPool>(current_segment_pool);
      }
    } else {
      // Aborted
      // TODO: Add coperative GC
      current_segment_pool->MarkedAsGarbage();
      garbage_pools_[current_txn->GetBeginCommitId()] = std::shared_ptr<peloton::storage::RollbackSegmentPool>(current_segment_pool);
    }

    EpochManagerFactory::GetInstance().ExitEpoch(current_txn->GetEpochId());

    updated_index_entries.clear();
    delete current_txn;
    current_txn = nullptr;
    current_segment_pool = nullptr;
  }

  // Init reserved area of a tuple
  // delete_flag is used to mark that the transaction that owns the tuple
  // has deleted the tuple
  // next_seg_pointer (8 bytes) | sindex entry ptr |
  // | delete_flag (1 bytes)
  void InitTupleReserved(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);
    SetRbSeg(tile_group_header, tuple_id, nullptr);
    SetSIndexPtr(tile_group_header, tuple_id, nullptr);
    ClearDeleteFlag(tile_group_header, tuple_id);
    *(reinterpret_cast<bool*>(reserved_area + delete_flag_offset)) = false;
    new (reserved_area + spinlock_offset) Spinlock();
  }

  // Get current segment pool of the transaction manager
  inline storage::RollbackSegmentPool *GetSegmentPool() {return current_segment_pool;}

  inline RBSegType GetRbSeg(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    LockTuple(tile_group_header, tuple_id);
    char **rb_seg_ptr = (char **)(tile_group_header->GetReservedFieldRef(tuple_id) + rb_seg_offset);
    if (*rb_seg_ptr != nullptr) {
      if (*(int *)(*rb_seg_ptr+16) == 0) {
        LOG_INFO("Reading an empty rollback segment");
      }
    }
    RBSegType result = *rb_seg_ptr;
    UnlockTuple(tile_group_header, tuple_id);
    return result;
  }

 private:
  static const size_t rb_seg_offset = 0;
  static const size_t sindex_ptr = rb_seg_offset + sizeof(char *);
  static const size_t delete_flag_offset = sindex_ptr + sizeof(char *);
  static const size_t spinlock_offset = delete_flag_offset + sizeof(char *);
  // TODO: add cooperative GC
  // The RB segment pool that is actively being used
  cuckoohash_map<cid_t, std::shared_ptr<storage::RollbackSegmentPool>> living_pools_;
  // The RB segment pool that has been marked as garbage
  cuckoohash_map<cid_t, std::shared_ptr<storage::RollbackSegmentPool>> garbage_pools_;

  inline void LockTuple(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                             spinlock_offset);
    lock->Lock();
  }

  inline void UnlockTuple(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                             spinlock_offset);
    lock->Unlock();
  }

  inline void SetRbSeg(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id, const RBSegType seg_ptr) {
    // Sanity check
    LockTuple(tile_group_header, tuple_id);
    if (seg_ptr != nullptr)
      PL_ASSERT(*(int *)(seg_ptr + 16) != 0);
    const char **rb_seg_ptr = (const char **)(tile_group_header->GetReservedFieldRef(tuple_id) + rb_seg_offset);
    *rb_seg_ptr = seg_ptr;
    UnlockTuple(tile_group_header, tuple_id);
  }

  inline void SetSIndexPtr(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id,
                          index::RBItemPointer *ptr) {
    index::RBItemPointer **index_ptr = (index::RBItemPointer **)(tile_group_header->GetReservedFieldRef(tuple_id) + sindex_ptr);
    *index_ptr = ptr;
  }

  inline index::RBItemPointer *GetSIndexPtr(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    index::RBItemPointer **index_ptr = (index::RBItemPointer **)(tile_group_header->GetReservedFieldRef(tuple_id) + sindex_ptr);
    return *index_ptr;
  }

  inline bool GetDeleteFlag(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    return *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + delete_flag_offset));
  }

  inline void SetDeleteFlag(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + delete_flag_offset)) = true;
  }

  inline void ClearDeleteFlag(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + delete_flag_offset)) = false;
  }
};
}
}