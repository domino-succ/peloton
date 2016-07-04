//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.h
//
// Identification: src/backend/benchmark/ycsb_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/benchmark/benchmark_common.h"
#include <iostream>

namespace peloton {
namespace benchmark {
// Helper function to pin current thread to a specific core
void PinToCore(size_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}
void SleepMilliseconds(int n) {
  struct timespec ts_sleep = {n / 1000, (n % 1000) * 1000000L};
  nanosleep(&ts_sleep, NULL);
}
}
}
