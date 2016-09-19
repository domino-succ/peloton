//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tpcc.cpp
//
// Identification: benchmark/tpcc/tpcc.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>
#include <iomanip>
#include <sys/stat.h>

#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"
#include "backend/benchmark/tpcc/tpcc_workload.h"

#include "backend/gc/gc_manager_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "backend/common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

configuration state;
int RUNNING_REF_THRESHOLD = state.running_ref;

// std::ofstream out("outputfile.summary");

static void WriteOutput() {
  // Create output directory
  struct stat st;
  if (stat("./tpcc-output", &st) == -1) {
    mkdir("./tpcc-output", 0700);
  }

  oid_t total_snapshot_memory = 0;
  for (auto &entry : state.snapshot_memory) {
    total_snapshot_memory += entry;
  }

  // Create file under output directory
  time_t tt;
  time(&tt);
  struct tm *p;
  p = localtime(&tt);
  std::stringstream oss;
  oss << "./tpcc-output/"
      << "output" << p->tm_year + 1900 << p->tm_mon + 1 << p->tm_mday
      << p->tm_hour << p->tm_min << p->tm_sec << ".summary";
  std::ofstream out(oss.str(), std::ofstream::out);

  LOG_INFO("----------------------------------------------------------");
  LOG_INFO(
      "scheduler:%d---%lf :: %lf tps, %lf abort, %lf tps_new, %lf abort_new, "
      "%lf tps_pay, %lf abort_pay, %lf delay, %lf generate, %d",
      state.scheduler, state.scale_factor, state.throughput, state.abort_rate,
      state.new_order_throughput, state.new_order_abort_rate,
      state.payment_throughput, state.payment_abort_rate, state.delay_ave,
      state.generate_rate,
      state.snapshot_memory[state.snapshot_throughput.size() - 1]);

  // out << state.scale_factor << "\n";

  for (size_t round_id = 0; round_id < state.snapshot_throughput.size();
       ++round_id) {
    out << "[" << std::setw(3) << std::left
        << state.snapshot_duration * round_id << " - " << std::setw(3)
        << std::left << state.snapshot_duration * (round_id + 1)
        << " s]: " << state.snapshot_throughput[round_id] << " "
        << state.snapshot_abort_rate[round_id] << " "
        << state.snapshot_memory[round_id] << "\n";
  }

  out << state.throughput << " ";
  out << state.abort_rate << " ";
  out << state.delay_ave << " ";
  out << state.delay_max << " ";
  out << state.delay_min << " ";
  out << state.snapshot_memory[state.snapshot_throughput.size() - 1] << " ";
  out << state.backend_count << " ";
  out << state.warehouse_count << " ";
  out << state.generate_rate << " ";
  out << state.canonical << " ";
  out << state.online << " ";
  out << state.single_ref << " ";
  out << state.lock_free << "\n";

  //  out << state.payment_throughput << " ";
  //  out << state.payment_abort_rate << " ";
  //
  //  out << state.new_order_throughput << " ";
  //  out << state.new_order_abort_rate << " ";
  //
  //  out << state.stock_level_latency << " ";
  //  out << state.order_status_latency << " ";
  //  out << state.scan_stock_latency << " ";

  // out << total_snapshot_memory << "\n";

  out.flush();
  out.close();
}

void LoadQuery(uint64_t count) {
  // The number of queues is equal to the threads (backend_count)
  if (state.scheduler != SCHEDULER_TYPE_CLUSTER) {
    concurrency::TransactionScheduler::GetInstance().Resize(
        state.backend_count, state.warehouse_count);
  }

  // If use CLUSTER method, we should analyze the query/txns when load query.
  if (state.scheduler == SCHEDULER_TYPE_CLUSTER) {

    // These queries are for clustering
    for (int i = 0; i < state.analysis_txns; i++) {
      GenerateAndCacheQuery();
    }

    std::cout << "Enter cluster analysis" << std::endl;

    // Get clustering result. Each cluster is a big region (vector) including
    // cluster NO.
    std::unordered_map<int, ClusterRegion> clusters = ClusterAnalysis();

    std::cout << "===========Print debug info =================" << std::endl;
    for (auto &cluster : clusters) {

      std::cout << "Cluster: " << cluster.first
                << ". Its members are: " << cluster.second.GetMemberCount();
      std::cout << std::endl;
    }
    // end test

    // Resize the number of queues according the number of clusters
    concurrency::TransactionScheduler::GetInstance().Resize(state.backend_count,
                                                            clusters.size());

    // Set the cluster result to scheduler. When enqueue, the new coming txn
    // compares the big region with each cluster
    concurrency::TransactionScheduler::GetInstance().SetClusters(clusters);
  }
  /////////////////////////end clustering//////////////////////////

  // These new queries are for TPCC executions
  bool new_order = true;
  for (uint64_t i = 0; i < count; i++) {
    GenerateALLAndCache(new_order);
    // GenerateALLAndCache(false);

    // change generating
    if (new_order) {
      new_order = false;
    } else {
      new_order = true;
    }
  }

  if (state.generate_count == 0) {
    LOG_INFO("No enqueue thread");
    for (uint64_t i = 0; i < count; i++) {
      EnqueueCachedUpdate();
    }
  }

  std::cout << "LOAD QUERY Count: " << count << std::endl;

  concurrency::TransactionScheduler::GetInstance().DebugPrint();
}

void LoadLogTable() {
  if (state.scheduler == SCHEDULER_TYPE_HASH) {
    // load file
    if (!state.log_table) {
      std::ifstream infile(LOGTABLE);
      std::string condition;
      int conflict;
      int success;

      // Put condition and conflict into log_table
      if (state.fraction) {
        while (infile >> condition >> conflict >> success) {
          concurrency::TransactionScheduler::GetInstance().LoadLogFull(
              condition, conflict, success);
        }
      } else {
        while (infile >> condition >> conflict) {
          concurrency::TransactionScheduler::GetInstance().LoadLog(condition,
                                                                   conflict);
        }
      }

      // Close file
      infile.close();

      // Debug
      // concurrency::TransactionScheduler::GetInstance().DumpLogTable();
    }
  }
}

// Main Entry Point
void RunBenchmark() {
  gc::GCManagerFactory::Configure(state.gc_protocol, state.gc_thread_count);
  concurrency::TransactionManagerFactory::Configure(state.protocol);
  LOG_INFO("Before configure index");
  index::IndexFactory::Configure(state.sindex);

  // Create the database
  CreateTPCCDatabase();

  // Load the database
  LoadTPCCDatabase();

  // If OOHASH, load Log Table File
  LoadLogTable();

  // Load queries/txns
  LoadQuery(PRELOAD);

  // Run the workload
  RunWorkload();

  // For OOHASH
  if (!state.log_table) {
    WriteOutput();
  }
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::tpcc::ParseArguments(argc, argv,
                                           peloton::benchmark::tpcc::state);

  peloton::benchmark::tpcc::RunBenchmark();

  return 0;
}
