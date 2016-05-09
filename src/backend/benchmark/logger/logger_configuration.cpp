//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/logger/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iomanip>
#include <algorithm>
#include <sys/stat.h>

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/benchmark/logger/logger_configuration.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"

namespace peloton {
namespace benchmark {
namespace logger {

void Usage(FILE* out) {
  fprintf(out,
          "Command line options :  logger <options> \n"
          "   -h --help              :  Print help message \n"
          "   -a --asynchronous-mode :  Asynchronous mode \n"
          "   -b --backend-count     :  Backend count \n"
          "   -c --column-count      :  # of columns \n"
          "   -d --flush-mode        :  Flush mode \n"
          "   -e --experiment-type   :  Experiment Type \n"
          "   -f --data-file-size    :  Data file size (MB) \n"
          "   -h --help              :  Print help message \n"
          "   -k --scale-factor      :  # of tuples \n"
          "   -l --logging-type      :  Logging type \n"
          "   -n --nvm-latency       :  NVM latency \n"
          "   -p --pcommit-latency   :  pcommit latency \n"
          "   -s --skew              :  Skew \n"
          "   -t --transactions      :  # of transactions \n"
          "   -u --write-ratio       :  Fraction of updates \n"
          "   -w --wait-timeout      :  Wait timeout (us) \n"
          "   -y --benchmark_type    :  Benchmark type \n"
  );
}

static struct option opts[] = {
    {"asynchronous_mode", optional_argument, NULL, 'a'},
    {"backend_count", optional_argument, NULL, 'b'},
    {"column_count", optional_argument, NULL, 'c'},
    {"flush_mode", optional_argument, NULL, 'd'},
    {"experiment-type", optional_argument, NULL, 'e'},
    {"data-file-size", optional_argument, NULL, 'f'},
    {"scale-factor", optional_argument, NULL, 'k'},
    {"logging-type", optional_argument, NULL, 'l'},
    {"nvm-latency", optional_argument, NULL, 'n'},
    {"pcommit-latency", optional_argument, NULL, 'p'},
    {"skew", optional_argument, NULL, 's'},
    {"transactions", optional_argument, NULL, 't'},
    {"update_ratio", optional_argument, NULL, 'u'},
    {"wait-timeout", optional_argument, NULL, 'w'},
    {"benchmark_type", optional_argument, NULL, 'y'},
    {NULL, 0, NULL, 0}};

static void ValidateLoggingType(
    const configuration& state) {
  std::cout << std::setw(20) << std::left << "logging_type "
      << " : ";

  std::cout << LoggingTypeToString(state.logging_type) << std::endl;
}

std::string BenchmarkTypeToString(BenchmarkType type) {
  switch (type) {
    case BENCHMARK_TYPE_INVALID:
      return "INVALID";

    case BENCHMARK_TYPE_YCSB:
      return "YCSB";
    case BENCHMARK_TYPE_TPCC:
      return "TPCC";

    default:
      LOG_ERROR("Invalid benchmark_type :: %d", type);
      exit(EXIT_FAILURE);
  }
  return "INVALID";
}

std::string ExperimentTypeToString(ExperimentType type) {
  switch (type) {
    case EXPERIMENT_TYPE_INVALID:
      return "INVALID";

    case EXPERIMENT_TYPE_THROUGHPUT:
      return "THROUGHPUT";
    case EXPERIMENT_TYPE_RECOVERY:
      return "RECOVERY";
    case EXPERIMENT_TYPE_STORAGE:
      return "STORAGE";
    case EXPERIMENT_TYPE_LATENCY:
      return "LATENCY";

    default:
      LOG_ERROR("Invalid experiment_type :: %d", type);
      exit(EXIT_FAILURE);
  }

  return "INVALID";
}

static void ValidateBenchmarkType(
    const configuration& state) {
  if (state.benchmark_type <= 0 || state.benchmark_type >= 3) {
    std::cout << "Invalid benchmark_type :: " << state.benchmark_type
        << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "benchmark_type "
      << " : " << BenchmarkTypeToString(state.benchmark_type) << std::endl;

}

static void ValidateDataFileSize(
    const configuration& state) {
  if (state.data_file_size <= 0) {
    std::cout << "Invalid pmem_file_size :: " << state.data_file_size
        << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "data_file_size "
      << " : " << state.data_file_size << std::endl;
}

static void ValidateExperimentType(
    const configuration& state) {
  if (state.experiment_type < 0 || state.experiment_type > 4) {
    std::cout << "Invalid experiment_type :: " << state.experiment_type
        << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "experiment_type "
      << " : " << ExperimentTypeToString(state.experiment_type) << std::endl;
}

static void ValidateWaitTimeout(
    const configuration& state) {
  if (state.wait_timeout < 0) {
    std::cout << "Invalid wait_timeout :: " << state.wait_timeout << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "wait_timeout "
      << " : " << state.wait_timeout << std::endl;
}

static void ValidateFlushMode(
    const configuration& state) {
  if (state.flush_mode <= 0 || state.flush_mode >= 3) {
    std::cout << "Invalid flush_mode :: " << state.flush_mode << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "flush_mode "
      << " : " << state.flush_mode << std::endl;
}

static void ValidateNVMLatency(
    const configuration& state) {
  if (state.nvm_latency < 0) {
    std::cout << "Invalid nvm_latency :: " << state.nvm_latency << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "nvm_latency "
      << " : " << state.nvm_latency << std::endl;
}

static void ValidatePCOMMITLatency(
    const configuration& state) {
  if (state.pcommit_latency < 0) {
    std::cout << "Invalid pcommit_latency :: " << state.pcommit_latency << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "pcommit_latency "
      << " : " << state.pcommit_latency << std::endl;
}

static void ValidateTransactionCount(const configuration &state) {
  if (state.transaction_count <= 0) {
    std::cout << "Invalid transaction_count :: " << state.transaction_count
        << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "transaction_count "
      << " : " << state.transaction_count << std::endl;
}

static void ValidateLogFileDir(
    configuration& state) {
  struct stat data_stat;

  // Assign log file dir based on logging type
  switch (state.logging_type) {
    // Log file on NVM
    case LOGGING_TYPE_NVM_WAL:
    case LOGGING_TYPE_NVM_WBL: {
      int status = stat(NVM_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        state.log_file_dir = NVM_DIR;
      }
    } break;

    // Log file on HDD
    case LOGGING_TYPE_HDD_WAL:
    case LOGGING_TYPE_HDD_WBL: {
      int status = stat(HDD_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        state.log_file_dir = HDD_DIR;
      }
    } break;

    case LOGGING_TYPE_INVALID:
    default: {
      int status = stat(TMP_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        state.log_file_dir = TMP_DIR;
      }
      else {
        throw Exception("Could not find temp directory : " + std::string(TMP_DIR));
      }
    } break;
  }

  std::cout << std::setw(20) << std::left << "log_file_dir "
      << " : " << state.log_file_dir << std::endl;
}

void ParseArguments(int argc, char* argv[], configuration &state) {
  // Default Values
  state.logging_type = LOGGING_TYPE_NVM_WAL;

  state.log_file_dir = TMP_DIR;
  state.data_file_size = 512;

  state.experiment_type = EXPERIMENT_TYPE_INVALID;
  state.wait_timeout = 200;
  state.benchmark_type = BENCHMARK_TYPE_YCSB;
  state.transaction_count = 100;
  state.flush_mode = 2;
  state.nvm_latency = 0;
  state.pcommit_latency = 0;

  // Default Values
  ycsb::state.scale_factor = 1;
  ycsb::state.column_count = 10;
  ycsb::state.update_ratio = 0.5;
  ycsb::state.backend_count = 2;
  ycsb::state.skew_factor = 1;

  ycsb::state.transaction_count = state.transaction_count;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ab:c:e:d:f:hk:l:n:p:s:t:u:w:y:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'l':
        state.logging_type = (LoggingType)atoi(optarg);
        break;
      case 'f':
        state.data_file_size = atoi(optarg);
        break;
      case 'e':
        state.experiment_type = (ExperimentType)atoi(optarg);
        break;
      case 'w':
        state.wait_timeout = atoi(optarg);
        break;
      case 'y':
        state.benchmark_type = (BenchmarkType)atoi(optarg);
        break;
      case 'd':
        state.flush_mode = atoi(optarg);
        break;
      case 'n':
        state.nvm_latency = atoi(optarg);
        break;
      case 'p':
        state.pcommit_latency = atoi(optarg);
        break;

        // YCSB
      case 'k':
        ycsb::state.scale_factor = atoi(optarg);
        break;
      case 't':
        state.transaction_count = atoi(optarg);
        ycsb::state.transaction_count = atoi(optarg);
        break;
      case 'c':
        ycsb::state.column_count = atoi(optarg);
        break;
      case 'u':
        ycsb::state.update_ratio = atof(optarg);
        break;
      case 'b':
        ycsb::state.backend_count = atoi(optarg);
        break;
      case 's':
        ycsb::state.skew_factor = atoi(optarg);
        break;

      case 'h':
        Usage(stderr);
        ycsb::Usage(stderr);
        exit(EXIT_FAILURE);
        break;

      default:
        exit(EXIT_FAILURE);
        break;
    }
  }

  // Print configuration
  ValidateLoggingType(state);
  ValidateDataFileSize(state);
  ValidateLogFileDir(state);
  ValidateWaitTimeout(state);
  ValidateExperimentType(state);
  ValidateBenchmarkType(state);
  ValidateTransactionCount(state);
  ValidateFlushMode(state);
  ValidateNVMLatency(state);
  ValidatePCOMMITLatency(state);

  // Print configuration
  ycsb::ValidateScaleFactor(ycsb::state);
  ycsb::ValidateColumnCount(ycsb::state);
  ycsb::ValidateUpdateRatio(ycsb::state);
  ycsb::ValidateBackendCount(ycsb::state);
  ycsb::ValidateSkewFactor(ycsb::state);

}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
