//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.cpp
//
// Identification: src/backend/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "transaction_scheduler.h"

namespace peloton {
namespace concurrency {

TransactionScheduler& TransactionScheduler::GetInstance() {
  static TransactionScheduler scheduler;
  return scheduler;
}
}
}
