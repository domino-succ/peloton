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
#include "backend/common/dbscan.h"
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

  // Transaction run itself
  virtual bool Run() = 0;

  virtual void Cleanup() = 0;

  virtual PlanNodeType GetPlanType() = 0;
  virtual TxnType GetTxnType() = 0;

  virtual const std::vector<Value>& GetCompareKeys() const = 0;

  virtual std::vector<uint64_t>& GetPrimaryKeysByint() = 0;
  virtual int GetPrimaryKey() = 0;

  // For clustering
  virtual SingleRegion* RegionTransform() = 0;
  virtual SingleRegion& GetRegion() = 0;

  // For Log Table
  virtual void UpdateLogTable(bool single_ref, bool canonical) = 0;
  virtual void UpdateLogTableFullConflict(bool single_ref, bool canonical) = 0;
  virtual void UpdateLogTableFullSuccess(bool single_ref, bool canonical) = 0;

  // For Run Table
  virtual int LookupRunTable(bool single_ref, bool canonical) = 0;
  virtual int LookupRunTableMax(bool single_ref, bool canonical) = 0;

  virtual int LookupRunTableFull(bool single_ref, bool canonical) = 0;
  virtual int LookupRunTableMaxFull(bool single_ref, bool canonical) = 0;

  virtual void UpdateRunTable(int queue_no, bool single_ref,
                              bool canonical) = 0;
  virtual void DecreaseRunTable(bool single_ref, bool canonical) = 0;

  // For metadata
  virtual void SetQueueNo(int queue_no) = 0;
  virtual int GetQueueNo() = 0;

  // For experiment
  virtual void ReSetStartTime() = 0;
  virtual std::chrono::system_clock::time_point& GetStartTime() = 0;
  void RecordDelay(uint64_t& delay_total_ref, uint64_t& delay_max_ref,
                   uint64_t& delay_min_ref) {
    std::chrono::system_clock::time_point end_time =
        std::chrono::system_clock::now();

    uint64_t delay = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - GetStartTime()).count();

    delay_total_ref = delay_total_ref + delay;

    if (delay > delay_max_ref) {
      delay_max_ref = delay;
    }
    if (delay < delay_min_ref) {
      delay_min_ref = delay;
    }
  }

  // virtual std::shared_ptr<Region> RegionTransform() = 0;

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

  // In TPCC partition_counts is the # of warehouses or the # of clusters
  void Resize(int queue_counts, int partition_counts) {
    queues_.resize(queue_counts);
    counter_.resize(queue_counts);
    random_generator_.Init(0, queue_counts);
    random_cluster_.Init(1, partition_counts);

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
   * Randomly enqueue
   */

  void RandomEnqueue(TransactionQuery* query,
                     bool single_ref __attribute__((unused))) {
    // Get a random number from 0 to queue_counts
    int queue = random_generator_.GetSample();

    // Update Run Table with the queue. That is to increasing the queue
    // reference in Run Table
    // query->UpdateRunTable(queue, single_ref);

    // Set queue No. then when clean run table queue No. will be used
    query->SetQueueNo(queue);

    // Finally, enqueue this query
    queues_[queue].Enqueue(query);
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

  // Based on the clustering result. Since the number of queues is
  // equal to the number of cluster, and the cluster result is passed
  // to the scheduler. When dequeue, we use the method of ParitionDeuque
  void ClusterEnqueue(TransactionQuery* query) {
    // Get the region for current query
    Region& current = query->GetRegion();

    int max = 0;
    int cluster_idx = 0;

    // Compare the overlap query's region with each cluster's region
    for (auto& cluster : clusters_) {
      int overlap = current.OverlapValue(cluster);

      // If the current overlap is larger than the max, keep it
      if (overlap > max) {
        max = overlap;
        cluster_idx = cluster.GetClusterNo();
      }
    }

    // If there is no overlap with any cluster, randomly pickup a cluster
    if (cluster_idx == 0) {
      cluster_idx = random_cluster_.GetSample();
    }

    // Finally, we get the final cluster. Here use % since # of clusters might
    // be larger than # of queues. Note: cluster_idx should be from 0
    queues_[(cluster_idx - 1) % queue_counts_].Enqueue(query);
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

  /*
   * 1.For a new coming query, it first lookups the LogTable and selects the
   * conditions matches. For simplicity, we only consider New-Order, so the
   * conditions are hard coding: S_I_ID  S_W_ID D_ID D_W_ID, and get the number
   *   S_I_ID-190 : 4444
   *   S_W_ID-2   : 2234
   *   D_ID-3     : 123
   *   D_W_ID-2   : 452
   *
   * 2.Then lookup these four conditions in RunTable to see they belong to which
   * queue and accumulate the count for the corresponding
   *quTransactionSchedulereue.
   *   S_I_ID-190-->(3,100)(5,99)
   *   S_W_ID-2  -->(3,200)
   *   D_ID-3    -->(5,99)
   *   D_W_ID-2  -->(1,1000)
   *
   *   queue3 : 4444 + 2234
   *   queue5 : 4444+ 123
   *   queue1 : 452
   *
   * 3.Then pick up the queue with the largest score.
   *
   * 4.Increase the number for each conditions
   */

  std::atomic<int> g_queue_no;

  void OOHashEnqueue(TransactionQuery* query,
                     bool offline __attribute__((unused)), bool online,
                     bool single_ref, bool canonical) {
    int queue = -1;

    // Find out the corresponding queue
    if (online) {
      queue = query->LookupRunTableMax(single_ref, canonical);
    }
    // SUM
    else {
      queue = query->LookupRunTable(single_ref, canonical);
    }

    // These is no queue matched. Randomly select a queue
    if (queue == -1) {
      // queue = random_generator_.GetSample();

      queue = g_queue_no.fetch_add(1) % queue_counts_;

      // If this queue has txn executing, should not use this queue
      while (queues_[queue].Size() > 0) {
        queue = g_queue_no.fetch_add(1) % queue_counts_;
      }

      // Test
      std::cout << "Can't find a queue, so assign queue: " << queue
                << ". Queue size is: " << queues_[queue].Size()
                << ". Key: " << query->GetPrimaryKey() << std::endl;
      DumpRunTable(queue);
    }

    // Update Run Table with the queue. That is to increasing the queue
    // reference in Run Table
    query->UpdateRunTable(queue, single_ref, canonical);

    // Set queue No. then when clean run table queue No. will be used
    query->SetQueueNo(queue);

    // Finally, enqueue this query
    queues_[queue].Enqueue(query);
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

  // The different between this and Dequeue is SimpleDequeue do not steal txns
  // from other threads when its thread is empty
  bool SimpleDequeue(TransactionQuery*& query, uint64_t thread_id) {
    // Return false if every queue is empty
    return queues_[thread_id % queue_counts_].Dequeue(query);
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

  int GetCacheSize() { return query_cache_queue_.Size(); }

  void SetClusters(std::unordered_map<int, ClusterRegion>& clusters) {
    for (auto& entry : clusters) {
      clusters_.push_back(entry.second);
    }
  }

  int GetQueueCount() { return queue_counts_; }
  //////////////////////////////////////////////////////////////////////
  // OO-Hash
  //////////////////////////////////////////////////////////////////////

  // support multi-thread
  void LogTableIncrease(std::string& key) {
    counter_lock_.Lock();

    auto entry = log_table_.find(key);

    if (entry != log_table_.end()) {
      ++log_table_[key];
    }
    // Create
    else {
      log_table_.emplace(key, 1);
    }

    counter_lock_.Unlock();
  }

  // Return the condition conflict number
  int LogTableGet(std::string& key) {

    auto entry = log_table_.find(key);
    if (entry != log_table_.end()) {
      return entry->second;
    }

    return 0;
  }

  // support multi-thread
  void LogTableFullConflictIncrease(std::string& key) {
    counter_lock_.Lock();

    auto entry = log_table_full_.find(key);

    if (entry != log_table_full_.end()) {
      entry->second.first++;
    }
    // Create
    else {
      log_table_full_.emplace(key, std::make_pair(1, 0));
    }

    counter_lock_.Unlock();
  }

  void LogTableFullSuccessIncrease(std::string& key) {
    counter_lock_.Lock();

    auto entry = log_table_full_.find(key);

    if (entry != log_table_full_.end()) {
      entry->second.second++;
    }
    // Create
    else {
      log_table_full_.emplace(key, std::make_pair(0, 1));
    }

    counter_lock_.Unlock();
  }

  // Return the condition conflict rate
  double LogTableFullGet(std::string& key) {

    auto entry = log_table_full_.find(key);
    if (entry != log_table_full_.end()) {
      return ((entry->second.first * 1.0) /
              (entry->second.first + entry->second.second));
    }

    return 0;
  }

  // Return the condition thread/queue pointer. Since we might return null, so
  // returning reference wouldn't work here
  std::unordered_map<int, int>* RunTableGet(std::string& key) {

    std::unordered_map<int, int>* ret = nullptr;
    auto entry = run_table_.find(key);
    if (entry != run_table_.end()) {
      ret = &(entry->second);
    }

    return ret;
  }

  // support multi-thread
  void RunTableIncrease(std::string& key, int queue_no) {
    // counter_lock_.Lock();

    // Get the reference of the corresponding queue
    std::unordered_map<int, int>* queue_info = RunTableGet(key);

    if (queue_info != nullptr) {
      // Increase the reference for this queue
      auto entry = queue_info->find(queue_no);

      if (entry != queue_info->end()) {
        ++(*queue_info)[queue_no];
      }
      // If there is no such entry, create it
      else {
        queue_info->insert(std::make_pair(queue_no, 1));
      }
    }
    // If there is no such entry, create it
    else {
      std::unordered_map<int, int> queue_map = {{queue_no, 1}};
      run_table_.insert(std::make_pair(key, queue_map));
    }

    // counter_lock_.Unlock();
  }

  // support multi-thread
  void RunTableDecrease(std::string& key, int queue_no) {
    counter_lock_.Lock();

    // Get the reference of the corresponding queue
    std::unordered_map<int, int>* queue_info = RunTableGet(key);

    // Decrease the reference for this queue
    if (queue_info != nullptr) {
      // Find out the entry with queue NO.
      auto queue = queue_info->find(queue_no);

      // If it exists, decrease the reference
      if (queue != queue_info->end()) {

        // If the reference is larger than 1, decrease
        if (queue_info->at(queue_no) >= 1) {
          --(*queue_info)[queue_no];
        }
      }
    }

    counter_lock_.Unlock();
  }

  // Clear all queues using pop.
  // TODO: Is there any better way to clear the queues?
  void ClearQueue(TransactionQuery* query) {

    // Pop concurrency_queue_
    while (concurrency_queue_.Dequeue(query) == false) {
      if (concurrency_queue_.IsEmpty()) {
        break;
      }
    }

    // Pop query_cache_queue_
    while (query_cache_queue_.Dequeue(query) == false) {
      if (query_cache_queue_.IsEmpty()) {
        break;
      }
    }

    // Pop queues_
    for (auto& queue : queues_) {
      while (queue.Dequeue(query) == false) {
        if (queue.IsEmpty()) {
          break;
        }
      }
    }
  }

  // Iterate all queues and see the size, select the smallest one
  // If there are several queues with the same smallest size,
  // Randomly select one
  int GetMinQueue() {
    assert(queues_.size() == queue_counts_);

    // how many txns the queue has
    int min_size = queues_.front().Size();
    int queue = 0;

    std::vector<int> min_queues(1, 0);

    for (uint64_t queue_no = 1; queue_no < queue_counts_; queue_no++) {
      if (queues_[queue_no].Size() <= min_size) {
        min_size = queues_[queue_no].Size();
        min_queues.push_back(queue_no);
      }
    }

    int size = min_queues.size();

    if (size == 1) {
      queue = min_queues[0];
    }

    // should not reach here
    if (size == 0) {
      queue = -1;
    }

    if (size > 1) {
      srand(time(NULL));
      int idx = rand() % size;
      queue = min_queues[idx];
    }

    return queue;
  }

  // Write LogTable into a file
  void OutputLogTable(std::string filename) {
    // Create file
    std::stringstream oss;
    oss << filename;
    std::ofstream out(oss.str(), std::ofstream::out);

    // Iterate Log Table (map)
    for (auto& entry : log_table_) {
      out << entry.first << " ";
      out << entry.second << "\n";
    }
    //    for (auto& entry : log_table_full_) {
    //      out << entry.first << " ";
    //      out << entry.second.first << " ";
    //      out << entry.second.second << "\n";
    //    }

    out.flush();
    out.close();
  }

  // Load data into log table
  void LoadLog(std::string condition, int conflict) {
    log_table_.insert(std::make_pair(condition, conflict));
  }

  void LoadLogFull(std::string condition, int conflict, int success) {
    log_table_full_.emplace(condition, std::make_pair(conflict, success));
  }

  ///////////////////////////
  // For debug
  //////////////////////////
  void DumpLogTable() {
    for (auto& entry : log_table_) {
      std::cout << entry.first << " " << entry.second << std::endl;
    }
  }

  void DumpRunTable() {
    for (auto& entry : run_table_) {
      for (auto& queue : entry.second) {
        std::cout << "Key: " << entry.first << ". QueueNo: " << queue.first
                  << ". Txns: " << queue.second << std::endl;
      }
    }
  }

  void DumpRunTable(int queue_no) {
    for (auto& entry : run_table_) {
      for (auto& queue : entry.second) {
        if (queue.first == queue_no && entry.first.substr(0, 4) == "W_ID") {
          std::cout << "Key: " << entry.first << ". QueueNo: " << queue.first
                    << ". Txns: " << queue.second << std::endl;
        }
      }
    }
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
  UniformIntGenerator random_cluster_;

  // Only used when use clustering
  std::vector<ClusterRegion> clusters_;

  //////////////////////////////////////////////
  // The condition format (string): 'columnname' + '-' + 'value', such as wid-4
  //////////////////////////////////////////////

  /*
   * The process for online and offline hash.
   *
   * 1-When a txn fails, record its conditions in Log Table (just increase the
   *count)
   * 2-Run all txns, record all failed txns
   * 3-Rerun benchmark, when a txn execute, check Run Table
   */

  // Run Table: a Hash table that records condition-->thread-No. For example,
  // txnA has three conditions: wid=3, iid=100, did=10, and txnA is executed by
  // thread-5, then in Run-Table, txnA creates three entries:
  // |----------------
  // | wid=3   --> 3
  // | iid=100 --> 100
  // | did=10  --> 10
  // |----------------
  // If wid=3 --> 3 already exists, do nothing.
  // Note: wid=3 might be executed by several threads at the time, so it is
  // vector for threads in the table. wid-3-->(3,100)(5,99)
  std::unordered_map<std::string, std::unordered_map<int, int>> run_table_;

  // Log Table: a Hash table that records condition-->count for conflict txns.
  // For example, txnA conflict with another txnB. TxnA has three conditions:
  // wid=3, iid=100, did=10. It increase these three conditions in LogTable:
  // condition conflict success
  // |----------------
  // | wid=3   --> 3001 1234
  // | iid=100 --> 101  5678
  // | did=10  --> 11   6789
  // |----------------
  std::unordered_map<std::string, int> log_table_;

  // log_table_full_ records not only conflict but also success
  std::unordered_map<std::string, std::pair<int, int>> log_table_full_;
};

// UINT64_MAX;

}  // end namespace concurrency
}  // end namespace peloton
