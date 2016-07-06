//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/lockfree_queue.h"
#include "backend/common/platform.h"
#include "backend/common/generator.h"
#include "backend/planner/abstract_plan.h"
#include "backend/executor/abstract_executor.h"

namespace peloton {
namespace concurrency {

////////////////////////////////
// Transaction Scheduler is responsible for
// 1) queuing query plan. txn-schd takes plan-tree/plan-node as a parameter.
//      Besides, param_list and tuple_desc is also needed as parameters.
//      1 - Txn-schd puts these three parameters into a queue or several queues.
//      2 - Txn-schd compare the recived plan-tree with the current plans in the
// queue
//          (If the plan-tree is a update, the comparision is reseanable)
//          If the updated data by the received plan has overlap with some other
// txns,
//          the received plan should be put into that queue.
// 2) execute query plan. pick up a plan-tree from a queue and execute it.
//      1 - pick up a plan from a give queue
//      2 - pick up a plan (round robin)
///////////////////////////////

class TransactionQuery {
 public:
  //  TransactionQuery(executor::AbstractExecutor* executor) {
  //    executor_tree_ = executor;
  //    b_executor_ = true;
  //    type_ = PlanNodeType::PLAN_NODE_TYPE_INVALID;
  //  }
  //
  //  TransactionQuery(executor::AbstractExecutor* executor, PlanNodeType type)
  // {
  //    executor_tree_ = executor;
  //    b_executor_ = true;
  //    type_ = type;
  //  }
  inline TransactionQuery() {}
  virtual inline ~TransactionQuery() {}

  virtual PlanNodeType GetPlanType() = 0;

  virtual const std::vector<Value>& GetCompareKeys() const = 0;

  virtual std::vector<uint64_t>& GetPrimaryKeysByint() = 0;

  // private:
  // Plan_tree is a pointer shared with the passing by object
  // std::shared_ptr<planner::AbstractPlan> plan_tree_;

  // param_list is a pointer shared with the passing by object
  // std::shared_ptr<std::vector<peloton::Value>> param_list_;

  // Tuple_desc is a pointer shared with the passing by object
  // std::shared_ptr<TupleDesc> tuple_desc_;

  // Executor tree. A query can be a executor tree. Then it doest need other
  // params to execute
  // std::shared_ptr<const peloton::executor::AbstractExecutor> executor_tree_;
  // executor::AbstractExecutor* executor_tree_;

  // A flag to indicates this query is a plan or executor
  // bool b_executor_;

  // Node type
  // PlanNodeType type_;
};

class TransactionScheduler {
 public:
  // Singleton
  static TransactionScheduler& GetInstance();

  TransactionScheduler() : queue_counts_(1), partition_counts_(1), range_(0) {}

  // factor is the total size of the of rows in YCSB
  void ResizeWithRange(int queue_counts, int factor) {
    queues_.resize(queue_counts);
    queue_counts_ = queue_counts;

    // Range_ can't exceed UINT64_MAX
    range_ = (factor / queue_counts) + 1;
  }

  // In TPCC partition_counts is the # of warehouses
  void Resize(int queue_counts, int partition_counts) {
    queues_.resize(queue_counts);
    counter_.resize(queue_counts);
    random_generator_.Init(0, queue_counts);

    queue_counts_ = queue_counts;
    partition_counts_ = partition_counts;
  }

  int QueueCount() {
    PL_ASSERT(queue_counts_ == queues_.size());
    return queue_counts_;
  }

  // Enqueue the query into the query_cache_queue_
  void CacheQuery(TransactionQuery* query) {
    query_cache_queue_.Enqueue(query);
  }
  uint64_t CacheSize() { return query_cache_queue_.Size(); }
  bool DequeueCache(TransactionQuery*& query) {
    return query_cache_queue_.Dequeue(query);
  }

  // Enqueue the query into the concurrency_queue
  void SingleEnqueue(TransactionQuery* query) {
    concurrency_queue_.Enqueue(query);
  }

  void SinglePrepare(TransactionQuery* query) {
    SingleDetect(query);
    SingleEnqueue(query);
  }

  // Pop element from the concurrency_queue. Note: query should be the reference
  // type
  bool SingleDequeue(TransactionQuery*& query) {
    return concurrency_queue_.Dequeue(query);
  }

  /*
   * Hash function: Mod%
   */
  void Enqueue(TransactionQuery* query) {
    std::vector<uint64_t> key = query->GetPrimaryKeysByint();
    uint64_t queue_number = Hash(key.front());

    queues_[queue_number].Enqueue(query);
    // queues_.at(queue_number).Enqueue(query);
  }

  /*
   * Hash function: Range Hash
   */
  void RangeEnqueue(TransactionQuery* query) {

    std::vector<uint64_t> keys = query->GetPrimaryKeysByint();
    std::map<uint64_t, int> idx_count;
    std::pair<int, uint64_t> count_idx;

    // For each key
    for (std::vector<uint64_t>::iterator iter = keys.begin();
         iter != keys.end(); iter++) {

      // Get the associated queue index
      uint64_t idx = RangeHash(*iter);

      // Increment the index count
      int count = ++idx_count[idx];

      // If the current count is the biggest, update it
      if (count > count_idx.first) {
        count_idx.first = count;
        count_idx.second = idx;
      }
    }

    // We got which queue is the most updates, so push the query
    queues_[count_idx.second].Enqueue(query);
  }

  // Based on the router's range. The ranges in router are continuous for
  // example: [0,999) [1000, 3999)[4000, 4999)[5000, 8000)
  void RouterRangeEnqueue(TransactionQuery* query) {
    std::vector<uint64_t> keys = query->GetPrimaryKeysByint();
    std::map<uint64_t, int> idx_count;
    std::pair<int, uint64_t> count_idx;

    // For each key
    for (std::vector<uint64_t>::iterator iter = keys.begin();
         iter != keys.end(); iter++) {

      // Get the associated queue index
      uint64_t idx = RouterHash(*iter);

      // Increment the index count
      int count = ++idx_count[idx];

      // If the current count is the biggest, update it
      if (count > count_idx.first) {
        count_idx.first = count;
        count_idx.second = idx;
      }
    }

    // We got which queue is the most updates, so push the query
    queues_[count_idx.second].Enqueue(query);
  }

  void ModRangeEnqueue(TransactionQuery* query) {

    std::vector<uint64_t> keys = query->GetPrimaryKeysByint();
    std::map<uint64_t, int> idx_count;
    std::pair<int, uint64_t> count_idx;

    // For each key
    for (std::vector<uint64_t>::iterator iter = keys.begin();
         iter != keys.end(); iter++) {

      // Get the associated queue index
      uint64_t idx = Hash(*iter);

      // Increment the index count
      int count = ++idx_count[idx];

      // If the current count is the biggest, update it
      if (count > count_idx.first) {
        count_idx.first = count;
        count_idx.second = idx;
      }
    }

    // We got which queue is the most updates, so push the query
    queues_[count_idx.second].Enqueue(query);
  }

  /*
   * For a new coming query, it first checks each queue's counter, whether the
   * counter has its update key (w_id). Select the largest one as the target
   * queue. If every queue has the same number, randomly select a queue. After
   * enqueue, decrement the counter by one
   *
   * Note: when dequeue, the corresponding counter should be decrement by one
   */
  void CounterEnqueue(TransactionQuery* query) {
    // The w_id for a TPCC txn
    std::vector<uint64_t> keys = query->GetPrimaryKeysByint();

    // W_ID
    uint64_t key = keys.front();

    int queue_index = 0;
    int queue_number = 0;
    int max_counter = 0;
    std::map<int, int>::iterator counter_it;

    counter_lock_.Lock();

    // For each key
    for (std::vector<std::map<int, int>>::iterator iter = counter_.begin();
         iter != counter_.end(); iter++) {

      queue_index++;
      std::map<int, int>::iterator w_id_entry = (*iter).find(key);

      // Find a entry
      if (w_id_entry == (*iter).end()) {
        continue;
      }

      // If the counter is larger than the largest
      if (w_id_entry->second > max_counter) {
        max_counter = w_id_entry->second;
        queue_number = queue_index - 1;  // index begins from 0
        counter_it =
            w_id_entry;  // record the largest counter in order to increase it
      }
    }

    // After the checking, we already know which queue has the largest one (with
    // the same key). But if all the queues are equal, just randomly select a
    // queue. Otherwise, put the txn into the largest one.
    if (queue_number > 0) {
      queues_[queue_number].Enqueue(query);
      counter_it->second++;
    } else {
      int idx = random_generator_.GetSample();
      queues_[idx].Enqueue(query);
      counter_[idx][key]++;
    }

    counter_lock_.Unlock();
  }

  bool CounterDequeue(TransactionQuery*& query, uint64_t thread_id) {
    // Each thread is associated to a queue. But if the associated queue is
    // empty, the thread can pop elements from other queues. If every queue is
    // empty, return false
    for (uint64_t loop = 0; loop < queue_counts_; loop++) {
      uint64_t idx = (thread_id + loop) % queue_counts_;
      bool ret = queues_[idx].Dequeue(query);

      // When dequeue, decrease the counter by one
      if (ret == true) {
        // The w_id for a TPCC txn
        std::vector<uint64_t> keys = query->GetPrimaryKeysByint();

        // W_ID
        uint64_t key = keys.front();

        counter_lock_.Lock();
        counter_[idx][key]--;
        counter_lock_.Unlock();

        return true;
      }
    }
    // LOG_INFO("Queue is empty: %ld", thread_id);

    // Return false if every queue is empty
    return false;
  }

  bool Dequeue(TransactionQuery*& query);

  /*
   * Dequeue a txn according some static information. In TPCC, it is based on
   * the number of warehouses. For example, the # of warehouses is 5, and 5 is
   * the static information. The idea is not waste the thread to loop the queue.
   * When the # of warehouses is less than the # of threads (queues) some queues
   * are empty, so a thread should associated to a non-empty queue.
   *
   * So in TPCC a thread is associated to a partition (warehouse). If the # of
   * warehouse is less than the # of queues some queues are empty
   *
   * Note: if the # of warehouse is larger than the # of queues, idx will exceed
   */
  bool PartitionDequeue(TransactionQuery*& query, uint64_t thread_id) {

    // If the # of warehouse is larger than the # of queues, call Dequeue,
    // otherwise idx will exceed memory bound
    if (partition_counts_ > queue_counts_) {
      return Dequeue(query, thread_id);
    }

    // Each thread is associated to a partition (warehouse).
    for (uint64_t loop = 0; loop < partition_counts_; loop++) {
      uint64_t idx = (thread_id + loop) % partition_counts_;
      bool ret = queues_[idx].Dequeue(query);

      if (ret == true) return true;
    }
    // LOG_INFO("Queue is empty: %ld", thread_id);

    // Return false if every queue is empty
    return false;
  }

  bool Dequeue(TransactionQuery*& query, uint64_t thread_id) {
    // Each thread should associate to a queue. But if the associated queue is
    // empty, the thread can pop elements from other queues. If every queue is
    // empty, return false
    for (uint64_t loop = 0; loop < queue_counts_; loop++) {
      uint64_t idx = (thread_id + loop) % queue_counts_;
      bool ret = queues_[idx].Dequeue(query);

      if (ret == true) return true;
    }
    // LOG_INFO("Queue is empty: %ld", thread_id);

    // Return false if every queue is empty
    return false;
  }

  int SingleDetect(TransactionQuery* query) {
    TransactionQuery* queued_query = nullptr;
    int contention = 0;
    int count = concurrency_queue_.Size();

    for (int it = 0; it < count; it++) {
      // Pop a query
      bool ret = SingleDequeue(queued_query);

      // If queue is empty, we do need to wait
      if (ret == false) {
        return contention;
      }

      // Compare the value
      std::vector<Value> queue_keys = queued_query->GetCompareKeys();
      std::vector<Value> current_keys = query->GetCompareKeys();

      for (std::vector<Value>::iterator iter = current_keys.begin();
           iter != current_keys.end(); iter++) {
        for (std::vector<Value>::iterator iter_queue = queue_keys.begin();
             iter_queue != queue_keys.end(); iter_queue++) {
          if (*iter == *iter_queue) {
            contention++;
          }  // end if
        }    // end for
      }      // end for

      // After compare, enqueue the compare query
      if (queued_query != nullptr) {
        concurrency_queue_.Enqueue(queued_query);
      }
    }  // end for

    // return the contention factor
    return contention;
  }

  // The passing vector should be sorted
  void InitRouter(std::vector<uint64_t>& dist) {
    PL_ASSERT(queue_counts_ == queues_.size());

    uint64_t queue_size = dist.size() / queue_counts_ + 1;

    LOG_INFO("queue_size: %ld", queue_size);

    for (uint64_t q = 0; q < (queue_counts_ - 1); q++) {
      uint64_t upper_bound_idx = (q + 1) * queue_size;
      uint64_t upper_bound_number = dist[upper_bound_idx];

      router_[upper_bound_number] = q;
    }

    uint64_t upper_bound_number = dist.back();
    router_[upper_bound_number] = queue_counts_ - 1;
  }

  void DebugPrint() {
    for (std::vector<LockfreeQueue<TransactionQuery*>>::iterator iter =
             queues_.begin();
         iter != queues_.end(); iter++) {
      LOG_INFO("Queue's size: %d", (*iter).Size());
    }

    //    for (auto item : router_) {
    //      LOG_INFO("Queue's upper bound: %ld-->%d", item.first, item.second);
    //    }
  }

 private:
  uint64_t Hash(uint64_t key) { return key % queue_counts_; }

  // range_ is how many items (tuples) should be stored in a queue
  uint64_t RangeHash(uint64_t key) { return key / range_; }

  // RouterHash is based on the router. Router stores several ranges. Given a
  // key, routerhash returns which range the key belongs to
  int RouterHash(uint64_t key) {
    std::map<uint64_t, int>::iterator it = router_.lower_bound(key);
    PL_ASSERT(it != router_.end());

    return it->second;
  }

 private:
  uint64_t queue_counts_;

  // In TPCC, it is the number of warehouses. It is initiated using
  // state.warehouses
  uint64_t partition_counts_;

  // FIXME: I'd like to use unique_ptr, but not sure LockFreeQueue supports
  // smart pointer
  // The number of queues should be equal to the number of cores
  std::vector<LockfreeQueue<TransactionQuery*>> queues_;

  // For simple implementation
  LockfreeQueue<TransactionQuery*> concurrency_queue_;

  // For simple implementation
  LockfreeQueue<TransactionQuery*> query_cache_queue_;

  // Each queue has same range. range_ = UINT64_MAX/queue_counts_ + 1
  // For example, 68 / 7 = 9, so the range is 10: 0~9,10~19....
  // For a given number, idx = num/range_ is the queue idx (since idx is start
  // from 0)
  uint64_t range_;

  std::map<uint64_t, int> router_;

  // Each queue has a map <w_id : count>
  std::vector<std::map<int, int>> counter_;

  // To synch the counter
  Spinlock counter_lock_;

  // Random Generator
  UniformIntGenerator random_generator_;
};

// UINT64_MAX;

}  // end namespace concurrency
}  // end namespace peloton
