//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_executor.h
//
// Identification: src/backend/executor/insert_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/executor/abstract_executor.h"
#include "backend/planner/insert_plan.h"

#include <vector>

namespace peloton {
namespace executor {

class InsertExecutor : public AbstractExecutor {
 public:
  InsertExecutor(const InsertExecutor &) = delete;
  InsertExecutor &operator=(const InsertExecutor &) = delete;
  InsertExecutor(InsertExecutor &&) = delete;
  InsertExecutor &operator=(InsertExecutor &&) = delete;

  explicit InsertExecutor(const planner::AbstractPlan *node,
                          ExecutorContext *executor_context);

  // for plan/executor caching.
  // for OLTP queries, most of the member variables in plan/executor can be
  // reused.
  // we only need to reset executor context as well as the parameter values.
  void SetContext(ExecutorContext *executor_context) {
    executor_context_ = executor_context;
  }

  //  void SetTuple(std::unique_ptr<storage::Tuple> tuple) {
  //    const planner::InsertPlan &node = GetPlanNode<planner::InsertPlan>();
  //    node.SetTuple(std::move(tuple));
  //  }

 protected:
  bool DInit();

  bool DExecute();

 private:
  bool done_ = false;
};

}  // namespace executor
}  // namespace peloton
