//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.h
//
// Identification: benchmark/ycsb/configuration.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "backend/common/types.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

static const oid_t ycsb_database_oid = 100;

static const oid_t user_table_oid = 1001;

static const oid_t user_table_pkey_index_oid = 2001;

static const oid_t ycsb_field_length = 100;

class configuration {
 public:
  // size of the table
  int scale_factor;

  // column count
  int column_count;

  // update column count
  int update_column_count;

  // read column count
  int read_column_count;

  // operation count
  int operation_count;

  // update ratio
  double update_ratio;

  // execution duration
  double duration;

  // number of threads used in GC,
  // Only available when gc type is n2o
  int gc_thread_count;

  // snapshot duration
  double snapshot_duration;

  unsigned long transaction_count;

  // number of backends
  int backend_count;

  std::vector<double> snapshot_throughput;

  std::vector<double> snapshot_abort_rate;

  std::vector<int> snapshot_memory;

  double throughput;

  double abort_rate;

  // Theta in zipf distribution to control skewness
  double zipf_theta;

  // Run mix workload or not
  bool run_mix;

  // enable exponential backoff
  bool run_backoff;

  // enable blind write
  bool blind_write;

  // protocol type
  ConcurrencyType protocol;

  // gc protocol type
  GCType gc_protocol;

  // index type
  IndexType index;
  
};

extern configuration state;

void Usage(FILE *out);

void ValidateScaleFactor(const configuration &state);

void ValidateColumnCount(const configuration &state);

void ValidateUpdateColumnCount(const configuration &state);

void ValidateReadColumnCount(const configuration &state);

void ValidateOperationCount(const configuration &state);

void ValidateUpdateRatio(const configuration &state);

void ValidateBackendCount(const configuration &state);

void ValidateDuration(const configuration &state);

void ValidateSnapshotDuration(const configuration &state);

void ValidateProtocol(const configuration &state);

void ValidateIndex(const configuration &state);

void ParseArguments(int argc, char *argv[], configuration &state);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
