//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// index_scan_executor.cpp
//
// Identification: src/backend/executor/index_scan_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/index_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"
#include "backend/index/index.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/common/logger.h"
#include "backend/catalog/manager.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for indexscan executor.
 * @param node Indexscan node corresponding to this executor.
 */
IndexScanExecutor::IndexScanExecutor(const planner::AbstractPlan *node,
                                     ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

IndexScanExecutor::~IndexScanExecutor() {
  // Nothing to do here
}

/**
 * @brief Let base class Dinit() first, then do my job.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  assert(children_.size() == 0);

  // Grab info from plan node and check it
  const planner::IndexScanPlan &node = GetPlanNode<planner::IndexScanPlan>();

  index_ = node.GetIndex();
  assert(index_ != nullptr);

  result_itr = START_OID;
  done_ = false;

  column_ids_ = node.GetColumnIds();
  key_column_ids_ = node.GetKeyColumnIds();
  expr_types_ = node.GetExprTypes();
  values_ = node.GetValues();
  runtime_keys_ = node.GetRunTimeKeys();
  predicate_ = node.GetPredicate();

  if (runtime_keys_.size() != 0) {
    assert(runtime_keys_.size() == values_.size());

    if (!key_ready) {
      values_.clear();

      for (auto expr : runtime_keys_) {
        auto value = expr->Evaluate(nullptr, nullptr, executor_context_);
        LOG_INFO("Evaluated runtime scan key: %s", value.GetInfo().c_str());
        values_.push_back(value);
      }

      key_ready = true;
    }
  }

  table_ = node.GetTable();

  if (table_ != nullptr) {
    full_column_ids_.resize(table_->GetSchema()->GetColumnCount());
    std::iota(full_column_ids_.begin(), full_column_ids_.end(), 0);
  }

  return true;
}

/**
 * @brief Creates logical tile(s) after scanning index.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DExecute() {
  LOG_INFO("Index Scan executor :: 0 child");

  if (!done_) {
    auto status = ExecIndexLookup();
    if (status == false) return false;
    ExecPredication();
    ExecProjection();
  }

  // Already performed the index lookup
  assert(done_);

  while (result_itr < result.size()) {  // Avoid returning empty tiles
    if (result[result_itr]->GetTupleCount() == 0) {
      result_itr++;
      continue;
    } else {
      SetOutput(result[result_itr]);
      result_itr++;
      return true;
    }

  }  // end while

  return false;
}

void IndexScanExecutor::ExecPredication() {
  if (nullptr == predicate_) return;
  unsigned int removed_count = 0;
  for (auto tile : result) {
    for (auto tuple_id : *tile) {
      expression::ContainerTuple<LogicalTile> tuple(tile, tuple_id);
      if (predicate_->Evaluate(&tuple, nullptr, executor_context_).IsFalse()) {
        removed_count++;
        tile->RemoveVisibility(tuple_id);
      }
    }
  }
  LOG_INFO("predicate removed %d row", removed_count);
}

void IndexScanExecutor::ExecProjection() {
  if (column_ids_.size() == 0) return;

  for (auto tile : result) {
    tile->ProjectColumns(full_column_ids_, column_ids_);
  }
}

bool IndexScanExecutor::ExecIndexLookup() {
  assert(!done_);

  /*
   * If query is IN+Subquery, the values should be set using context params
   * for now, the flag params_exec_ is set to 1 only in nestloop join
   * in this case, the params is the results of the outer plan
   * We can add more cases in future
   */
  //TODO: we probably need to add more for other cases in future
  if (executor_context_->GetParamsExec() == 1) {
	  values_.clear();
	  std::vector<Value> vecValue = executor_context_->GetParams();

	  for (auto val : vecValue) {
	        values_.push_back(val);
	  }
  }

  std::vector<ItemPointer> tuple_locations;

  if (0 == key_column_ids_.size()) {
    tuple_locations = index_->ScanAllKeys();
  } else {
    tuple_locations = index_->Scan(values_, key_column_ids_, expr_types_,
                                   SCAN_DIRECTION_TYPE_FORWARD);
  }

  LOG_INFO("Tuple_locations.size(): %lu", tuple_locations.size());

  if (tuple_locations.size() == 0) return false;
  auto transaction = executor_context_->GetTransaction();
  //txn_id_t txn_id = transaction_->GetTransactionId();
  //cid_t commit_id = transaction_->GetStartCommitId();

  auto &transaction_manager = concurrency::TransactionManager::GetInstance();
  std::vector<ItemPointer> visible_items;

  for (auto tuple_location : tuple_locations) {
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tuple_location.block);
    auto tile_group_header = tile_group.get()->GetHeader();
    auto tile_group_id = tuple_location.block;
    auto tuple_id = tuple_location.offset;

    while (true) {
      txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
      cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
      cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
      if (transaction_manager.IsVisible(tuple_txn_id, tuple_begin_cid, tuple_end_cid)) {
        ItemPointer visible_item(tile_group_id, tuple_id);
        visible_items.push_back(visible_item);
        transaction->RecordRead(visible_item);
        break;
      } else {
        ItemPointer next_item = tile_group_header->GetNextItemPointer(tuple_id);
        if (next_item.block == INVALID_OID && next_item.offset == INVALID_OID) {
          break;
        }
        tile_group_id = next_item.block;
        tuple_id = next_item.offset;
        tile_group = manager.GetTileGroup(tile_group_id);
        tile_group_header = tile_group.get()->GetHeader();
      }
    }
  }

  // Get the list of blocks
  std::map<oid_t, std::vector<oid_t>> blocks;
  for (auto visible_item : visible_items) {
    blocks[visible_item.block].push_back(visible_item.offset);
  }
  // Construct a logical tile for each block
  for (auto block : blocks) {
    LogicalTile *logical_tile = LogicalTileFactory::GetTile();

    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(block.first);

    // Add relevant columns to logical tile
    logical_tile->AddColumns(tile_group, full_column_ids_);

    // Print tile group visibility
    // tile_group_header->PrintVisibility(txn_id, commit_id);

    logical_tile->AddPositionList(std::move(block.second));

    result.push_back(logical_tile);
  }

  done_ = true;

  LOG_TRACE("Result tiles : %lu", result.size());

  return true;
}

}  // namespace executor
}  // namespace peloton
