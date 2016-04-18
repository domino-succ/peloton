//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb.cpp
//
// Identification: src/backend/benchmark/ycsb/ycsb.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>
#include <thread>

#include "backend/common/logger.h"
#include "backend/common/types.h"

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"
#include "backend/benchmark/ycsb/ycsb_workload.h"

#include "backend/logging/log_manager.h"

extern LoggingType peloton_logging_mode;

namespace peloton {
namespace benchmark {
namespace ycsb {

configuration state;

std::ofstream out("outputfile.summary");

static void WriteOutput(double stat) {
  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%lf %d %d :: %lf tps", state.update_ratio, state.scale_factor,
           state.column_count, stat);

  out << state.update_ratio << " ";
  out << state.scale_factor << " ";
  out << state.column_count << " ";
  out << stat << "\n";
  out.flush();
}

inline void YCSBBootstrapLogger() {
  if (!state.logging_enabled) return;
  peloton_logging_mode = LOGGING_TYPE_DRAM_NVM;
  auto& log_manager = peloton::logging::LogManager::GetInstance();
  if (peloton_logging_mode != LOGGING_TYPE_INVALID) {
    // Launching a thread for logging
    if (!log_manager.IsInLoggingMode()) {
      // Set default logging mode
      log_manager.SetSyncCommit(true);
      //		  elog(DEBUG2, "Wait for standby mode");

      // Wait for standby mode
      std::thread(&peloton::logging::LogManager::StartStandbyMode, &log_manager)
          .detach();
      log_manager.WaitForModeTransition(peloton::LOGGING_STATUS_TYPE_STANDBY,
                                        true);
      //		  elog(DEBUG2, "Standby mode");

      // Clean up database tile state before recovery from checkpoint
      log_manager.PrepareRecovery();

      // Do any recovery
      log_manager.StartRecoveryMode();
      //		  elog(DEBUG2, "Wait for logging mode");

      // Wait for logging mode
      log_manager.WaitForModeTransition(peloton::LOGGING_STATUS_TYPE_LOGGING,
                                        true);
      //		  elog(DEBUG2, "Logging mode");

      // Done recovery
      log_manager.DoneRecovery();
    }
  }
}

// Main Entry Point
void RunBenchmark() {
  YCSBBootstrapLogger();
  // Create and load the user table
  CreateYCSBDatabase();

  LoadYCSBDatabase();

  // Run the workload
  auto stat = RunWorkload();

  WriteOutput(stat);
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char** argv) {
  peloton::benchmark::ycsb::ParseArguments(argc, argv,
                                           peloton::benchmark::ycsb::state);

  peloton::benchmark::ycsb::RunBenchmark();

  return 0;
}
