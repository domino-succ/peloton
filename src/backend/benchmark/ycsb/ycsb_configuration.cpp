//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/ycsb/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/benchmark/ycsb/ycsb_configuration.h"

#include <iomanip>
#include <algorithm>


namespace peloton {
namespace benchmark {
namespace ycsb {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : ycsb <options> \n"
          "   -h --help              :  Print help message \n"
          "   -k --scale-factor      :  # of tuples \n"
          "   -t --transactions      :  # of transactions \n"
          "   -c --column_count      :  # of columns \n"
          "   -u --write_ratio       :  Fraction of updates \n");
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"scale-factor", optional_argument, NULL, 'k'},
    {"transactions", optional_argument, NULL, 't'},
    {"column_count", optional_argument, NULL, 'c'},
    {"update_ratio", optional_argument, NULL, 'u'},
    {NULL, 0, NULL, 0}};

static void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    std::cout << "Invalid scalefactor :: " << state.scale_factor << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "scale_factor "
      << " : " << state.scale_factor << std::endl;
}

static void ValidateColumnCount(const configuration &state) {
  if (state.column_count <= 0) {
    std::cout << "Invalid attribute_count :: " << state.column_count
        << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "attribute_count "
      << " : " << state.column_count << std::endl;
}

static void ValidateUpdateRatio(const configuration &state) {
  if (state.update_ratio < 0 || state.update_ratio > 1) {
    std::cout << "Invalid update_ratio :: " << state.update_ratio << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "update_ratio "
      << " : " << state.update_ratio << std::endl;
}

int orig_scale_factor;

void ParseArguments(int argc, char *argv[], configuration &state) {

  // Default Values
  state.scale_factor = 10.0;
  state.transactions = 1;
  state.column_count = 10;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ahk:t:c:u:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 't':
        state.transactions = atoi(optarg);
        break;
      case 'c':
        state.column_count = atoi(optarg);
        break;
      case 'w':
        state.update_ratio = atof(optarg);
        break;
      case 'h':
        Usage(stderr);
        break;

      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        Usage(stderr);
    }
  }

  // Print configuration
  ValidateScaleFactor(state);
  ValidateColumnCount(state);
  ValidateUpdateRatio(state);

  std::cout << std::setw(20) << std::left << "transactions "
      << " : " << state.transactions << std::endl;

}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
