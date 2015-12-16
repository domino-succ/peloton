//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_join_executor.h
//
// Identification: src/backend/executor/abstract_join_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/schema.h"
#include "backend/executor/abstract_executor.h"
#include "backend/planner/project_info.h"

#include <vector>

namespace peloton {
namespace executor {

class AbstractJoinExecutor : public AbstractExecutor {
  AbstractJoinExecutor(const AbstractJoinExecutor &) = delete;
  AbstractJoinExecutor &operator=(const AbstractJoinExecutor &) = delete;

 public:
  explicit AbstractJoinExecutor(const planner::AbstractPlan *node,
                                ExecutorContext *executor_context);

  virtual ~AbstractJoinExecutor() {
  }

  const char *GetJoinTypeString() const {
    switch (join_type_) {
      case JOIN_TYPE_LEFT:
        return "JOIN_TYPE_LEFT";
      case JOIN_TYPE_RIGHT:
        return "JOIN_TYPE_RIGHT";
      case JOIN_TYPE_INNER:
        return "JOIN_TYPE_INNER";
      case JOIN_TYPE_OUTER:
        return "JOIN_TYPE_OUTER";
      case JOIN_TYPE_INVALID:
      default:
        return "JOIN_TYPE_INVALID";
    }
  }

 protected:
  bool DInit();

  bool DExecute() = 0;

  //===--------------------------------------------------------------------===//
  // Helper functions
  //===--------------------------------------------------------------------===//

  // Build a join output logical tile
  std::unique_ptr<LogicalTile> BuildOutputLogicalTile(
      LogicalTile *left_tile,
      LogicalTile *right_tile);

  // Build the schema of the joined tile based on the projection info
  std::vector<LogicalTile::ColumnInfo> BuildSchema(
      std::vector<LogicalTile::ColumnInfo> &left,
      std::vector<LogicalTile::ColumnInfo> &right);

  // Build position lists
  std::vector<std::vector<oid_t> > BuildPostitionLists(
      LogicalTile *left_tile,
      LogicalTile *right_tile);

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Result of  join. */
  std::vector<LogicalTile *> result;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Join predicate. */
  const expression::AbstractExpression *predicate_ = nullptr;

  /** @brief Projection info */
  const planner::ProjectInfo *proj_info_ = nullptr;

  /* @brief Join Type */
  PelotonJoinType join_type_;
};

}  // namespace executor
}  // namespace peloton
