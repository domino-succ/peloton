//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/tpcc/workload.cpp
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


#include "backend/benchmark/tpcc/tpcc_workload.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"

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
#include "backend/executor/insert_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/container_tuple.h"

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
namespace tpcc {

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;


size_t GenerateWarehouseId(const size_t &thread_id) {
  if (state.run_affinity) {
    if (state.warehouse_count <= state.backend_count) {
      return thread_id % state.warehouse_count;
    } else {
      int warehouse_per_partition = state.warehouse_count / state.backend_count;
      int start_warehouse = warehouse_per_partition * thread_id;
      int end_warehouse = ((int)thread_id == state.warehouse_count - 1) ? 
        (state.warehouse_count - 1) : warehouse_per_partition * (thread_id + 1);
      return GetRandomInteger(start_warehouse, end_warehouse);
    }
  } else {
    return GetRandomInteger(0, state.warehouse_count - 1);
  }
}


void RunBackend(oid_t thread_id) {
  PinToCore(thread_id);

  //UniformGenerator generator;

  oid_t &execution_count_ref = abort_counts[thread_id];
  oid_t &transaction_count_ref = commit_counts[thread_id];


  NewOrderPlans new_order_plans = PrepareNewOrderPlan();
  //PaymentPlans payment_plans = PreparePaymentPlan();
  
  // backoff
  uint32_t backoff_shifts = 0;
  while (true) {

    if (is_running == false) {
      break;
    }

    while (RunNewOrder(new_order_plans, thread_id) == false) {
      execution_count_ref++;
      // backoff
      if (state.run_backoff) {
        if (backoff_shifts < 63) {
          ++backoff_shifts;
        }
        uint64_t spins = 1UL << backoff_shifts;
        spins *= 100;
        while (spins) {
          _mm_pause();
          --spins;
        }
      }
    }

    // while (RunPayment(payment_plans) == false) {
    //   execution_count_ref++;
    //   // backoff
    //   if (state.run_backoff) {
    //     if (backoff_shifts < 63) {
    //       ++backoff_shifts;
    //     }
    //     uint64_t spins = 1UL << backoff_shifts;
    //     spins *= 100;
    //     while (spins) {
    //       _mm_pause();
    //       --spins;
    //     }
    //   }
    // }

    // while (RunStockLevel(thread_id) == false) {
    //   execution_count_ref++;
    //   // backoff
    //   if (state.run_backoff) {
    //     if (backoff_shifts < 63) {
    //       ++backoff_shifts;
    //     }
    //     uint64_t spins = 1UL << backoff_shifts;
    //     spins *= 100;
    //     while (spins) {
    //       _mm_pause();
    //       --spins;
    //     }
    //   }
    // }

    // while (RunDelivery(thread_id) == false) {
    //   execution_count_ref++;
    //   // backoff
    //   if (state.run_backoff) {
    //     if (backoff_shifts < 63) {
    //       ++backoff_shifts;
    //     }
    //     uint64_t spins = 1UL << backoff_shifts;
    //     spins *= 100;
    //     while (spins) {
    //       _mm_pause();
    //       --spins;
    //     }
    //   }
    // }
    
    // auto rng_val = generator.GetSample();
    
    // if (rng_val <= 0.04) {
    //   while (RunStockLevel() == false) {
    //     execution_count_ref++;
    //   }
    // } else if (rng_val <= 0.08) {
    //   while (RunDelivery() == false) {
    //     execution_count_ref++;
    //   }
    // } else if (rng_val <= 0.12) {
    //   while (RunOrderStatus() == false) {
    //     execution_count_ref++;
    //   }
    // } else if (rng_val <= 0.55) {
    //   while (RunPayment() == false) {
    //     execution_count_ref++;
    //   }
    // } else {
    //   while (RunNewOrder() == false) {
    //     execution_count_ref++;
    //   }
    // }

    backoff_shifts >>= 1;
    transaction_count_ref++;

  }
}

void RunWorkload() {

  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  
  abort_counts = new oid_t[num_threads];
  memset(abort_counts, 0, sizeof(oid_t) * num_threads);

  commit_counts = new oid_t[num_threads];
  memset(commit_counts, 0, sizeof(oid_t) * num_threads);

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
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
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

  state.snapshot_throughput
      .push_back(total_commit_count * 1.0 / state.snapshot_duration);
  state.snapshot_abort_rate
      .push_back(total_abort_count * 1.0 / total_commit_count);

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

    state.snapshot_throughput
        .push_back(total_commit_count * 1.0 / state.snapshot_duration);
    state.snapshot_abort_rate
        .push_back(total_abort_count * 1.0 / total_commit_count);
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


  LOG_INFO("============TABLE SIZES==========");
  LOG_INFO("warehouse count = %u", warehouse_table->GetAllCurrentTupleCount());
  LOG_INFO("district count  = %u", district_table->GetAllCurrentTupleCount());
  LOG_INFO("item count = %u", item_table->GetAllCurrentTupleCount());
  LOG_INFO("customer count = %u", customer_table->GetAllCurrentTupleCount());
  LOG_INFO("history count = %u", history_table->GetAllCurrentTupleCount());
  LOG_INFO("stock count = %u", stock_table->GetAllCurrentTupleCount());
  LOG_INFO("orders count = %u", orders_table->GetAllCurrentTupleCount());
  LOG_INFO("new order count = %u", new_order_table->GetAllCurrentTupleCount());
  LOG_INFO("order line count = %u", order_line_table->GetAllCurrentTupleCount());
}


/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////


std::vector<std::vector<Value>>
ExecuteReadTest(executor::AbstractExecutor* executor) {

  std::vector<std::vector<Value>> logical_tile_values;

  // Execute stuff
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());

    // is this possible?
    if(result_tile == nullptr)
      break;

    auto column_count = result_tile->GetColumnCount();

    for (oid_t tuple_id : *result_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
                                                                  tuple_id);
      std::vector<Value> tuple_values;
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
        auto value = cur_tuple.GetValue(column_itr);
        tuple_values.push_back(value);
      }

      // Move the tuple list
      logical_tile_values.push_back(std::move(tuple_values));
    }
  }

  return std::move(logical_tile_values);
}


void ExecuteUpdateTest(executor::AbstractExecutor* executor) {
  
  // Execute stuff
  while (executor->Execute() == true);
}


void ExecuteDeleteTest(executor::AbstractExecutor* executor) {
  
  // Execute stuff
  while (executor->Execute() == true);
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton