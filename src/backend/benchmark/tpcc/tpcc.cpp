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

// std::ofstream out("outputfile.summary");

static void WriteOutput() {
  // Create output directory
  struct stat st;
  if (stat("./tpcc-output", &st) == -1) {
    mkdir("./tpcc-output", 0700);
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
  LOG_INFO("%lf :: %lf tps, %lf, %d", state.scale_factor, state.throughput,
           state.abort_rate,
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
  out << state.snapshot_memory[state.snapshot_throughput.size() - 1] << " ";
  out << state.backend_count << " ";
  out << state.warehouse_count << "\n";
  out.flush();
  out.close();
}
void LoadQuery(uint64_t count) {
  // The number of queues is equal to the threads (backend_count)
  concurrency::TransactionScheduler::GetInstance().Resize(state.backend_count);

  for (uint64_t i = 0; i < count; i++) {
    GenerateAndCacheQuery();
  }

  EnqueueCachedUpdate();

  std::cout << "LOAD QUERY Count: " << count << std::endl;

  concurrency::TransactionScheduler::GetInstance().DebugPrint();
}

#define PRELOAD 50000  // 2000,000

// Main Entry Point
void RunBenchmark() {
  gc::GCManagerFactory::Configure(state.gc_protocol, state.gc_thread_count);
  concurrency::TransactionManagerFactory::Configure(state.protocol);

  // Create the database
  CreateTPCCDatabase();

  // Load the database
  LoadTPCCDatabase();

  LoadQuery(PRELOAD);

  // Run the workload
  RunWorkload();

  WriteOutput();
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
