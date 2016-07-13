
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree.cpp
//
// Identification: src/index/bwtree.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "bwtree.h"

#ifdef BWTREE_PELOTON
namespace peloton {
namespace index { 
#endif

bool print_flag = false;
NodeID INVALID_NODE_ID = 0;

#ifdef BWTREE_PELOTON
}  // End index namespace
}  // End peloton namespace
#endif
