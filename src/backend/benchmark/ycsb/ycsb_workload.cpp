//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ycsb_workload.cpp
//
// Identification: benchmark/ycsb/ycsb_workload.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>

#include "backend/benchmark/ycsb/ycsb_workload.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/common/generator.h"

#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/index_scan_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/expression_util.h"

#include "backend/index/index_factory.h"

#include "backend/logging/log_manager.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/index_scan_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;
oid_t *total_counts;
oid_t *generate_counts;
uint64_t *delay_totals;
uint64_t *delay_maxs;
uint64_t *delay_mins;

// If return true and query is null, that means the queue is empty
// If return true and query is not null, that means execute successfully
// If return false, that means execute fail
bool PopAndExecuteQuery(concurrency::TransactionQuery *&ret_query) {
  /////////////////////////////////////////////////////////
  // EXECUTE : Get a query from the queue and execute it
  /////////////////////////////////////////////////////////

  // Start a txn to execute the query
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Get a query from a queue
  concurrency::TransactionQuery *query = nullptr;
  concurrency::TransactionScheduler::GetInstance().SingleDequeue(query);

  // Return true
  if (query == nullptr) {
    ret_query = nullptr;
    return true;
  }

  peloton::PlanNodeType plan_type = query->GetPlanType();

  // Execute the query
  switch (plan_type) {

    case PLAN_NODE_TYPE_UPDATE: {
      ExecuteUpdateTest(
          reinterpret_cast<UpdateQuery *>(query)->GetUpdateExecutor());
      break;
    }

    default: {
      LOG_INFO("plan_type :: Unsupported Plan Tag: %u ", plan_type);
      elog(INFO, "Query: ");
      break;
    }
  }

  /////////////////////////////////////////////////////////
  // Transaction fail
  /////////////////////////////////////////////////////////
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    ret_query = query;
    return false;
  }

  /////////////////////////////////////////////////////////
  // Transaction Commit
  /////////////////////////////////////////////////////////
  assert(txn->GetResult() == Result::RESULT_SUCCESS);
  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    ret_query = query;
    return true;

  } else {
    // transaction failed commitment.
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    ret_query = query;
    return false;
  }
}

bool ExecuteQuery(concurrency::TransactionQuery *query) {
  // Start a txn to execute the query
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Execute the query
  assert(query != nullptr);
  peloton::PlanNodeType plan_type = query->GetPlanType();
  switch (plan_type) {
    case PLAN_NODE_TYPE_UPDATE: {
      ExecuteUpdateTest(
          reinterpret_cast<UpdateQuery *>(query)->GetUpdateExecutor());
      break;
    }
    default: {
      LOG_INFO("plan_type :: Unsupported Plan Tag: %u ", plan_type);
      elog(INFO, "Query: ");
      break;
    }
  }

  /////////////////////////////////////////////////////////
  // Transaction fail
  /////////////////////////////////////////////////////////
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  /////////////////////////////////////////////////////////
  // Transaction success
  /////////////////////////////////////////////////////////
  assert(txn->GetResult() == Result::RESULT_SUCCESS);
  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    return true;
  } else {
    // transaction failed commitment.
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    return false;
  }
}
void RecordDelay(UpdateQuery *query, uint64_t &delay_total_ref,
                 uint64_t &delay_max_ref, uint64_t &delay_min_ref) {
  std::chrono::system_clock::time_point end_time =
      std::chrono::system_clock::now();
  uint64_t delay = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - query->GetStartTime()).count();
  delay_total_ref = delay_total_ref + delay;

  if (delay > delay_max_ref) {
    delay_max_ref = delay;
  }
  if (delay < delay_min_ref) {
    delay_min_ref = delay;
  }
}

void RunBackend(oid_t thread_id) {
  PinToCore(thread_id);

  // auto update_ratio = state.update_ratio;
  // auto operation_count = state.operation_count;

  // auto update_ratio = state.update_ratio;
  oid_t &execution_count_ref = abort_counts[thread_id];
  oid_t &transaction_count_ref = commit_counts[thread_id];
  oid_t &total_count_ref = total_counts[thread_id];
  uint64_t &delay_total_ref = delay_totals[thread_id];
  uint64_t &delay_max_ref = delay_maxs[thread_id];
  uint64_t &delay_min_ref = delay_mins[thread_id];

  while (true) {
    if (is_running == false) {
      break;
    }
    // Pop a query from a queue and execute
    concurrency::TransactionQuery *ret_query = nullptr;
    bool ret_pop = false;

    //////////////////////////////////////////
    // Pop a query
    //////////////////////////////////////////
    switch (state.scheduler) {
      case SCHEDULER_TYPE_NONE:
      case SCHEDULER_TYPE_CONTROL:
      case SCHEDULER_TYPE_ABORT_QUEUE: {
        ret_pop =
            concurrency::TransactionScheduler::GetInstance().SingleDequeue(
                ret_query);
        break;
      }
      case SCHEDULER_TYPE_CONFLICT_DETECT:
      case SCHEDULER_TYPE_CONFLICT_LEANING:
      case SCHEDULER_TYPE_CONFLICT_RANGE: {
        ret_pop = concurrency::TransactionScheduler::GetInstance().Dequeue(
            ret_query, thread_id);
        break;
      }

      default: {
        LOG_ERROR("plan_type :: Unsupported scheduler: %u ", state.scheduler);
        break;
      }
    }  // end switch

    // process the pop result. If queue is empty, continue loop
    if (ret_pop == false) {
      LOG_INFO("Queue is empty");
      continue;
    }

    PL_ASSERT(ret_query != nullptr);
    total_count_ref++;

    // Before execute query, we should set the start time
    (reinterpret_cast<UpdateQuery *>(ret_query))->ReSetStartTime();
    //////////////////////////////////////////
    // Execute query
    //////////////////////////////////////////
    if (ExecuteUpdate(reinterpret_cast<UpdateQuery *>(ret_query)) == false) {

      // When fail, some conflict occurs. Increase the fail counter
      execution_count_ref++;

      // LOG_INFO("Execution fail:%d---thread:%d", execution_count_ref,
      // thread_id);

      switch (state.scheduler) {

        case SCHEDULER_TYPE_NONE: {
          // We do nothing in this case.Just delete the query
          // Since we discard the txn, donot record the throughput and delay
          reinterpret_cast<UpdateQuery *>(ret_query)->Cleanup();
          delete ret_query;
          break;
        }
        case SCHEDULER_TYPE_CONTROL: {
          // Control: The txn re-executed immediately
          while (ExecuteUpdate(reinterpret_cast<UpdateQuery *>(ret_query)) ==
                 false) {
            // If still fail, the counter increase, then enter loop again
            execution_count_ref++;
            if (is_running == false) {
              break;
            }
          }

          // If execute successfully, we should clean up the query
          // First compute the delay
          RecordDelay(reinterpret_cast<UpdateQuery *>(ret_query),
                      delay_total_ref, delay_max_ref, delay_min_ref);

          // Second, clean up
          reinterpret_cast<UpdateQuery *>(ret_query)->Cleanup();
          delete ret_query;

          // Increase the counter
          transaction_count_ref++;
          LOG_INFO("In Control Fail,transaction_count_ref:%d",
                   transaction_count_ref);
          break;
        }
        case SCHEDULER_TYPE_ABORT_QUEUE: {
          // Queue: put the txn at the end of the queue
          concurrency::TransactionScheduler::GetInstance().SingleEnqueue(
              ret_query);
          break;
        }

        case SCHEDULER_TYPE_CONFLICT_DETECT: {
          concurrency::TransactionScheduler::GetInstance().Enqueue(ret_query);
          break;
        }

        case SCHEDULER_TYPE_CONFLICT_LEANING: {
          concurrency::TransactionScheduler::GetInstance().RouterRangeEnqueue(
              ret_query);
          break;
        }

        case SCHEDULER_TYPE_CONFLICT_RANGE: {
          concurrency::TransactionScheduler::GetInstance().RangeEnqueue(
              ret_query);
          break;
        }

        default: {
          LOG_INFO("plan_type :: Unsupported scheduler: %u ", state.scheduler);
          break;
        }
      }  // end switch
    }    // end execute fail

    /////////////////////////////////////////////////
    // Execute success: the memory should be deleted
    /////////////////////////////////////////////////
    else {
      // First compute the delay
      RecordDelay(reinterpret_cast<UpdateQuery *>(ret_query), delay_total_ref,
                  delay_max_ref, delay_min_ref);

      // Second, clean up
      reinterpret_cast<UpdateQuery *>(ret_query)->Cleanup();
      delete ret_query;

      // Increase the counter
      transaction_count_ref++;
      // LOG_INFO("Success:%d, fail:%d---%d", transaction_count_ref,
      //         execution_count_ref, thread_id);
    }  // end else execute == true
  }    // end while do update or read
}

void QueryBackend(oid_t thread_id) {
  PinToCore(thread_id);

  auto update_ratio = state.update_ratio;
  oid_t &generate_count_ref = generate_counts[thread_id - state.backend_count];
  ZipfDistribution zipf(state.scale_factor * FACTOR - 1, state.zipf_theta);
  fast_random rng(rand());

  while (true) {
    if (is_running == false) {
      break;
    }
    auto rng_val = rng.next_uniform();
    // Generate Update into queue
    if (rng_val < update_ratio) {
      GenerateAndQueueUpdate(zipf);
    } else {
      GenerateAndQueueUpdate(zipf);
    }
    generate_count_ref++;
  }
}

void RunWorkload() {
  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  oid_t num_generate = state.generate_count;
  abort_counts = new oid_t[num_threads];
  memset(abort_counts, 0, sizeof(oid_t) * num_threads);
  commit_counts = new oid_t[num_threads];
  memset(commit_counts, 0, sizeof(oid_t) * num_threads);
  total_counts = new oid_t[num_threads];
  memset(total_counts, 0, sizeof(oid_t) * num_threads);
  generate_counts = new oid_t[num_generate];
  memset(generate_counts, 0, sizeof(oid_t) * num_generate);

  // Initiate Delay
  delay_totals = new uint64_t[num_threads];
  memset(delay_totals, 0, sizeof(uint64_t) * num_threads);
  delay_maxs = new uint64_t[num_threads];
  memset(delay_maxs, 0, sizeof(uint64_t) * num_threads);
  delay_mins = new uint64_t[num_threads];
  std::fill_n(delay_mins, num_threads, 1000000);

  size_t snapshot_round = (size_t)(state.duration / state.snapshot_duration);

  oid_t **abort_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    abort_counts_snapshots[round_id] = new oid_t[num_threads];
  }

  oid_t **commit_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    commit_counts_snapshots[round_id] = new oid_t[num_threads];
  }

  // Launch a group of threads
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
  }

  // Launch a bunch of threads to queue the query
  for (oid_t thread_itr = num_threads; thread_itr < num_threads + num_generate;
       ++thread_itr) {
    thread_group.push_back(std::move(std::thread(QueryBackend, thread_itr)));
  }

  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(int(state.snapshot_duration * 1000)));
    memcpy(abort_counts_snapshots[round_id], abort_counts,
           sizeof(oid_t) * num_threads);
    memcpy(commit_counts_snapshots[round_id], commit_counts,
           sizeof(oid_t) * num_threads);
  }

  is_running = false;

  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads + num_generate;
       ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // calculate the generate rate
  oid_t total_generate_count = 0;
  for (size_t i = 0; i < num_generate; ++i) {
    total_generate_count += generate_counts[i];
  }
  state.generate_rate = total_generate_count * 1.0 / state.duration;

  // calculate the generate rate
  oid_t total_exe_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_exe_count += total_counts[i];
  }

  // calculate the throughput and abort rate for the first round.
  oid_t total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[0][i];
  }

  oid_t total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[0][i];
  }

  state.snapshot_throughput.push_back(total_commit_count * 1.0 /
                                      state.snapshot_duration);
  state.snapshot_abort_rate.push_back(total_abort_count * 1.0 /
                                      total_commit_count);

  // calculate the throughput and abort rate for the remaining rounds.
  for (size_t round_id = 0; round_id < snapshot_round - 1; ++round_id) {
    total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_commit_count += commit_counts_snapshots[round_id + 1][i] -
                            commit_counts_snapshots[round_id][i];
    }

    total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_abort_count += abort_counts_snapshots[round_id + 1][i] -
                           abort_counts_snapshots[round_id][i];
    }

    state.snapshot_throughput.push_back(total_commit_count * 1.0 /
                                        state.snapshot_duration);
    state.snapshot_abort_rate.push_back(total_abort_count * 1.0 /
                                        total_commit_count);
  }

  // calculate the aggregated throughput and abort rate.
  total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[snapshot_round - 1][i];
  }

  total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[snapshot_round - 1][i];
  }

  state.throughput = total_commit_count * 1.0 / state.duration;
  state.abort_rate = total_abort_count * 1.0 / total_commit_count;

  LOG_INFO("Total_cout:%d, commit:%d, abort:%d, c+a:%d", total_exe_count,
           total_commit_count, total_abort_count,
           total_commit_count + total_abort_count);

  // calculate the average delay
  uint64_t total_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_delay += delay_totals[i];
  }
  state.delay_ave = (total_delay * 1.0) / (total_commit_count);

  // calculate the max delay
  uint64_t max_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    if (max_delay < delay_maxs[i]) {
      max_delay = delay_maxs[i];
    }
  }
  state.delay_max = max_delay;

  // calculate the min delay
  uint64_t min_delay = delay_mins[0];
  for (size_t i = 1; i < num_threads; ++i) {
    if (min_delay > delay_mins[i]) {
      min_delay = delay_mins[i];
    }
  }
  state.delay_min = min_delay;

  // cleanup everything.
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] abort_counts_snapshots[round_id];
    abort_counts_snapshots[round_id] = nullptr;
  }

  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] commit_counts_snapshots[round_id];
    commit_counts_snapshots[round_id] = nullptr;
  }
  delete[] abort_counts_snapshots;
  abort_counts_snapshots = nullptr;
  delete[] commit_counts_snapshots;
  commit_counts_snapshots = nullptr;

  delete[] abort_counts;
  abort_counts = nullptr;
  delete[] commit_counts;
  commit_counts = nullptr;

  delete[] generate_counts;
  generate_counts = nullptr;

  delete[] delay_totals;
  delay_totals = nullptr;
  delete[] delay_maxs;
  delay_maxs = nullptr;
  delete[] delay_mins;
  delay_mins = nullptr;
}

/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////

std::vector<std::vector<Value>> ExecuteReadTest(
    executor::AbstractExecutor *executor) {

  std::vector<std::vector<Value>> logical_tile_values;

  // Execute stuff
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());

    // is this possible?
    if (result_tile == nullptr) break;

    auto column_count = result_tile->GetColumnCount();
    LOG_TRACE("result column count = %d\n", (int)column_count);

    for (oid_t tuple_id : *result_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(
          result_tile.get(), tuple_id);
      std::vector<Value> tuple_values;
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
        auto value = cur_tuple.GetValue(column_itr);
        tuple_values.push_back(value);
      }

      // Move the tuple list
      logical_tile_values.push_back(std::move(tuple_values));
    }
  }

  return std::move(logical_tile_values);
}

void ExecuteUpdateTest(executor::AbstractExecutor *executor) {

  // Execute stuff
  while (executor->Execute() == true)
    ;
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
