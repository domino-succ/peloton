//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_array_test.cpp
//
// Identification: tests/common/dbscan_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <vector>

#include "harness.h"

#include "backend/common/dbscan.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// DBScanTest
//===--------------------------------------------------------------------===//

class DBScanTest : public PelotonTest {};

TEST_F(DBScanTest, BasicTest) {
  std::vector<uint32_t> region1(16, 0);
  std::vector<uint32_t> region2(16, 0);
  std::vector<uint32_t> region3(16, 0);
  std::vector<uint32_t> region4(16, 0);
  std::vector<uint32_t> region5(16, 0);
  std::vector<uint32_t> region6(16, 0);
  std::vector<uint32_t> region7(16, 0);
  std::vector<uint32_t> region8(16, 0);
  std::vector<uint32_t> region9(16, 0);

  region1[0] = 1;
  region1[1] = 1;
  region1[4] = 1;
  region1[5] = 1;

  region2[4] = 1;
  region2[5] = 1;
  region2[8] = 1;
  region2[9] = 1;

  region3[8] = 1;
  region3[9] = 1;
  region3[12] = 1;
  region3[13] = 1;

  std::vector<Region> data;
  data.push_back(region1);
  data.push_back(region2);
  data.push_back(region3);
  data.push_back(region4);
  data.push_back(region5);
  data.push_back(region6);
  data.push_back(region7);
  data.push_back(region8);
  data.push_back(region9);

  DBScan dbs(data, 1);

  EXPECT_EQ(2, dbs.Clustering());
}

}  // End test namespace
}  // End peloton namespace
