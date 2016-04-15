//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager.h
//
// Identification: src/backend/gc/gc_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <unordered_map>

#include "backend/common/types.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

#define MAX_TUPLES_PER_GC 1000
#define MAX_FREE_LIST_LENGTH 1000

class GCManager {
 public:
  GCManager(const GCManager &) = delete;
  GCManager &operator=(const GCManager &) = delete;
  GCManager(GCManager &&) = delete;
  GCManager &operator=(GCManager &&) = delete;

  // Get status of whether GC thread is running or not
  bool GetStatus() { return this->is_running_; }

  void StartGC();

  void StopGC();

  void AddPossiblyFreeTuple(const oid_t &database_id, const TupleMetadata &);

  ItemPointer ReturnFreeSlot(const oid_t &database_id, const oid_t &table_id);

 private:
  GCManager(const GC_TYPE type) : is_running_(true), gc_type_(type), possibly_free_list_(MAX_FREE_LIST_LENGTH) {}

  ~GCManager() {}
  
  void Poll();
  void DeleteTupleFromIndexes(const TupleMetadata &);

  
 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
  volatile bool is_running_;
  GCType gc_type_;
  LockfreeQueue<TupleMetadata> possibly_free_list_;
  cuckoohash_map<oid_t, LockfreeQueue<TupleMetadata>*> free_map_;
  std::unique_ptr<std::thread> gc_thread_;

};

}  // namespace gc
}  // namespace peloton
