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

std::ofstream out("outputfile.summary");

static void WriteOutput() {
  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%lf tps, %lf, %d", 
    state.throughput, state.abort_rate, 
    state.snapshot_memory[state.snapshot_throughput.size() - 1]);
  LOG_INFO("payment: %lf tps, %lf", state.payment_throughput, state.payment_abort_rate);
  LOG_INFO("new_order: %lf tps, %lf", state.new_order_throughput, state.new_order_abort_rate);
  LOG_INFO("stock_level latency: %lf us", state.stock_level_latency);
  LOG_INFO("order_status latency: %lf us", state.order_status_latency);
  LOG_INFO("scan_stock latency: %lf us", state.scan_stock_latency);

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
  // out << "payment: ";
  // out << state.payment_throughput << " ";
  // out << state.payment_abort_rate << " ";
  // out << "new_order: ";
  // out << state.new_order_throughput << " ";
  // out << state.new_order_abort_rate << " ";

  oid_t total_snapshot_memory = 0;
  for (auto &entry : state.snapshot_memory) {
    total_snapshot_memory += entry;
  }

  out << total_snapshot_memory <<"\n";
  out.flush();
  out.close();
}

// Main Entry Point
void RunBenchmark() {
  gc::GCManagerFactory::Configure(state.gc_protocol, state.gc_thread_count);
  concurrency::TransactionManagerFactory::Configure(state.protocol);
  index::IndexFactory::Configure(state.sindex);

  // Create the database
  CreateTPCCDatabase();

  // Load the database
  LoadTPCCDatabase();

  // Run the workload
  RunWorkload();

  WriteOutput();
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::tpcc::ParseArguments(
      argc, argv, peloton::benchmark::tpcc::state);

  peloton::benchmark::tpcc::RunBenchmark();

  return 0;
}