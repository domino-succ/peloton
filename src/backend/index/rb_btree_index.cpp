//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rb_btree_index.cpp
//
// Identification: src/backend/index/rb_btree_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/rb_btree_index.h"
#include "backend/index/index_key.h"
#include "backend/common/logger.h"
#include "backend/storage/tuple.h"
#include "backend/concurrency/transaction.h"

namespace peloton {

namespace concurrency {
  extern thread_local Transaction *current_txn;
}

namespace index {

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::RBBTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      container(KeyComparator(metadata)),
      equals(metadata),
      comparator(metadata) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
RBBTreeIndex<KeyType, ValueType, KeyComparator,
           KeyEqualityChecker>::~RBBTreeIndex() {
  // we should not rely on shared_ptr to reclaim memory.
  // this is because the underlying index can split or merge leaf nodes,
  // which invokes data data copy and deletes.
  // as the underlying index is unaware of shared_ptr,
  // memory allocated should be managed carefully by programmers.
  for (auto entry = container.begin(); entry != container.end(); ++entry) {
    delete entry->second;
    entry->second = nullptr;
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool RBBTreeIndex<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                                 const ItemPointer &location,
                                                 RBItemPointer **result) {
  KeyType index_key;

  index_key.SetFromKey(key);
  *result = new RBItemPointer(location, MAX_CID);
  std::pair<KeyType, ValueType> entry(index_key, *result);
  {
    index_lock.Lock();

    // Insert the key, val pair
    container.insert(entry);

    index_lock.Unlock();
  }

  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool RBBTreeIndex<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                                 const RBItemPointer &rb_location) {
  KeyType index_key;
  index_key.SetFromKey(key);

  {
    index_lock.Lock();

    // Delete the < key, location > pair
    bool try_again = true;
    while (try_again == true) {
      // Unset try again
      try_again = false;

      // Lookup matching entries
      auto entries = container.equal_range(index_key);
      for (auto iterator = entries.first; iterator != entries.second;
           iterator++) {
        RBItemPointer value = *(iterator->second);

        if (value == rb_location) {
          delete iterator->second;
          iterator->second = nullptr;
          container.erase(iterator);
          // Set try again
          try_again = true;
          break;
        }
      }
    }

    index_lock.Unlock();
  }

  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool RBBTreeIndex<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::CondInsertEntry(
    const storage::Tuple *key, const ItemPointer &location,
    std::function<bool(const void *)> predicate UNUSED_ATTRIBUTE,
    RBItemPointer **rb_itempointer_ptr) {

  KeyType index_key;
  index_key.SetFromKey(key);

  *rb_itempointer_ptr = nullptr;

  {
    index_lock.Lock();

    // find the <key, location> pair
    auto entries = container.equal_range(index_key);
    for (auto entry = entries.first; entry != entries.second; ++entry) {

      RBItemPointer rb_item_pointer = *(entry->second);

      if (concurrency::current_txn->GetBeginCommitId() < rb_item_pointer.timestamp) {
        // this key is already visible or dirty in the index
        index_lock.Unlock();
        return false;
      }
    }

    // Insert the key, val pair
    *rb_itempointer_ptr = new RBItemPointer(location, MAX_CID);
    container.insert(std::pair<KeyType, ValueType>(
        index_key, *rb_itempointer_ptr));

    index_lock.Unlock();
  }
  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool RBBTreeIndex<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                                 const ItemPointer &location) {
  KeyType index_key;
  index_key.SetFromKey(key);

  {
    index_lock.Lock();

    // Delete the < key, location > pair
    bool try_again = true;
    while (try_again == true) {
      // Unset try again
      try_again = false;

      // Lookup matching entries
      auto entries = container.equal_range(index_key);
      for (auto iterator = entries.first; iterator != entries.second;
           iterator++) {
        RBItemPointer value = *(iterator->second);

        if ((value.location.block == location.block) &&
            (value.location.offset == location.offset)) {
          delete iterator->second;
          iterator->second = nullptr;
          container.erase(iterator);
          // Set try again
          try_again = true;
          break;
        }
      }
    }

    index_lock.Unlock();
  }

  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const std::vector<Value> &values, const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types,
    const ScanDirectionType &scan_direction, std::vector<RBItemPointer> &result) {

  std::unordered_map<ItemPointer, RBItemPointer *> result_map;

  KeyType index_key;

  // Checkif we have leading (leftmost) column equality
  // refer : http://www.postgresql.org/docs/8.2/static/indexes-multicolumn.html
  oid_t leading_column_id = 0;
  auto key_column_ids_itr = std::find(key_column_ids.begin(),
                                      key_column_ids.end(), leading_column_id);

  // SPECIAL CASE : leading column id is one of the key column ids
  // and is involved in a equality constraint
  bool special_case = false;
  if (key_column_ids_itr != key_column_ids.end()) {
    auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
    if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQUAL) {
      special_case = true;
    }
  }

  LOG_TRACE("Special case : %d ", special_case);

  {
    index_lock.Lock();

    auto scan_begin_itr = container.begin();
    std::unique_ptr<storage::Tuple> start_key;
    bool all_constraints_are_equal = false;

    // If it is a special case, we can figure out the range to scan in the index
    if (special_case == true) {
      start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

      // Construct the lower bound key tuple
      all_constraints_are_equal = ConstructLowerBoundTuple(
          start_key.get(), values, key_column_ids, expr_types);
      LOG_TRACE("All constraints are equal : %d ", all_constraints_are_equal);
      index_key.SetFromKey(start_key.get());

      // Set scan begin iterator
      scan_begin_itr = container.equal_range(index_key).first;
    }

    switch (scan_direction) {
      case SCAN_DIRECTION_TYPE_FORWARD:
      case SCAN_DIRECTION_TYPE_BACKWARD: {
        // Scan the index entries in forward direction
        for (auto scan_itr = scan_begin_itr; scan_itr != container.end();
             scan_itr++) {
          auto scan_current_key = scan_itr->first;
          auto tuple =
              scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

          // Compare the current key in the scan with "values" based on
          // "expression types"
          // For instance, "5" EXPR_GREATER_THAN "2" is true
          if (Compare(tuple, key_column_ids, expr_types, values) == true) {

            RBItemPointer *item_pointer = scan_itr->second;

            if (item_pointer->timestamp > concurrency::current_txn->GetBeginCommitId()) {
              auto itr = result_map.find(item_pointer->location);
              if (itr == result_map.end() || itr->second->timestamp < item_pointer->timestamp) {
                result_map.emplace(item_pointer->location, item_pointer);
              }
            }
          } else {
            // We can stop scanning if we know that all constraints are equal
            if (all_constraints_are_equal == true) {
              break;
            }
          }
        }

      } break;

      case SCAN_DIRECTION_TYPE_INVALID:
      default:
        throw Exception("Invalid scan direction \n");
        break;
    }

    index_lock.Unlock();
  }

  for (auto itr = result_map.begin(); itr != result_map.end(); itr++) {
    result.push_back(*itr->second);
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    std::vector<RBItemPointer> &result) {

  std::unordered_map<ItemPointer, RBItemPointer *> result_map;

  {
    index_lock.Lock();

    auto itr = container.begin();

    // scan all entries
    while (itr != container.end()) {
      RBItemPointer *item_pointer = itr->second;
      if (item_pointer->timestamp > concurrency::current_txn->GetBeginCommitId()) {
        auto res = result_map.find(item_pointer->location);
        if (res == result_map.end() || res->second->timestamp < item_pointer->timestamp) {
          result_map.emplace(item_pointer->location, item_pointer);
        }
      }
      itr++;
    }

    index_lock.Unlock();
  }

  for (auto itr = result_map.begin(); itr != result_map.end(); itr++) {
    result.push_back(*itr->second);
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key, std::vector<RBItemPointer> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  std::unordered_map<ItemPointer, RBItemPointer *> result_map;

  {
    index_lock.Lock();

    // find the <key, location> pair
    auto entries = container.equal_range(index_key);
    for (auto entry = entries.first; entry != entries.second; ++entry) {

      RBItemPointer *item_pointer = entry->second;

      if (item_pointer->timestamp > concurrency::current_txn->GetBeginCommitId()) {
        auto itr = result_map.find(item_pointer->location);
        if (itr == result_map.end() || itr->second->timestamp < item_pointer->timestamp) {
          result_map.emplace(item_pointer->location, item_pointer);
        }
      }
    }

    index_lock.Unlock();
  }

  for (auto itr = result_map.begin(); itr != result_map.end(); itr++) {
    result.push_back(*itr->second);
  }
}

///////////////////////////////////////////////////////////////////////

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const std::vector<Value> &values, const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types,
    const ScanDirectionType &scan_direction,
    std::vector<RBItemPointer *> &result) {

  std::unordered_map<ItemPointer, RBItemPointer *> result_map;

  KeyType index_key;
  // Check if we have leading (leftmost) column equality
  // refer : http://www.postgresql.org/docs/8.2/static/indexes-multicolumn.html
  oid_t leading_column_id = 0;
  auto key_column_ids_itr = std::find(key_column_ids.begin(),
                                      key_column_ids.end(), leading_column_id);

  // SPECIAL CASE : leading column id is one of the key column ids
  // and is involved in a equality constraint
  bool special_case = false;
  if (key_column_ids_itr != key_column_ids.end()) {
    auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
    if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQUAL) {
      special_case = true;
    }
  }

  LOG_TRACE("Special case : %d ", special_case);

  {
    index_lock.Lock();

    auto scan_begin_itr = container.begin();
    std::unique_ptr<storage::Tuple> start_key;
    bool all_constraints_are_equal = false;

    // If it is a special case, we can figure out the range to scan in the index
    if (special_case == true) {
      start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

      // Construct the lower bound key tuple
      all_constraints_are_equal = ConstructLowerBoundTuple(
          start_key.get(), values, key_column_ids, expr_types);
      LOG_TRACE("All constraints are equal : %d ", all_constraints_are_equal);
      index_key.SetFromKey(start_key.get());

      // Set scan begin iterator
      scan_begin_itr = container.equal_range(index_key).first;
    }

    switch (scan_direction) {
      case SCAN_DIRECTION_TYPE_FORWARD:
      case SCAN_DIRECTION_TYPE_BACKWARD: {
        // Scan the index entries in forward direction
        for (auto scan_itr = scan_begin_itr; scan_itr != container.end();
             scan_itr++) {
          auto scan_current_key = scan_itr->first;
          auto tuple =
              scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

          // Compare the current key in the scan with "values" based on
          // "expression types"
          // For instance, "5" EXPR_GREATER_THAN "2" is true
          if (Compare(tuple, key_column_ids, expr_types, values) == true) {
            RBItemPointer *location_header = scan_itr->second;

            if (location_header->timestamp > concurrency::current_txn->GetBeginCommitId()) {
              auto itr = result_map.find(location_header->location);
              if (itr == result_map.end() || itr->second->timestamp < location_header->timestamp) {
                result_map.emplace(location_header->location, location_header);
              }
            }
          } else {
            // We can stop scanning if we know that all constraints are equal
            if (all_constraints_are_equal == true) {
              break;
            }
          }
        }

      } break;

      case SCAN_DIRECTION_TYPE_INVALID:
      default:
        throw Exception("Invalid scan direction \n");
        break;
    }

    index_lock.Unlock();
  }

  for (auto itr = result_map.begin(); itr != result_map.end(); itr++) {
    result.push_back(itr->second);
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    std::vector<RBItemPointer *> &result) {

  std::unordered_map<ItemPointer, RBItemPointer *> result_map;

  {
    index_lock.Lock();

    auto itr = container.begin();

    // scan all entries
    while (itr != container.end()) {
      RBItemPointer *location = itr->second;

      if (location->timestamp > concurrency::current_txn->GetBeginCommitId()) {
        auto itr = result_map.find(location->location);
        if (itr == result_map.end() || itr->second->timestamp < location->timestamp) {
          result_map.emplace(location->location, location);
        }
      }

      itr++;
    }

    index_lock.Unlock();
  }

  for (auto itr = result_map.begin(); itr != result_map.end(); itr++) {
    result.push_back(itr->second);
  }
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key, std::vector<RBItemPointer *> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  std::unordered_map<ItemPointer, RBItemPointer *> result_map;

  {
    index_lock.Lock();

    // find the <key, location> pair
    auto entries = container.equal_range(index_key);
    for (auto entry = entries.first; entry != entries.second; ++entry) {
      RBItemPointer *location = entry->second;

      if (location->timestamp > concurrency::current_txn->GetBeginCommitId()) {
        auto itr = result_map.find(location->location);
        if (itr == result_map.end() || itr->second->timestamp < location->timestamp) {
          result_map.emplace(location->location, location);
        }
      }
    }

    index_lock.Unlock();
  }

  for (auto itr = result_map.begin(); itr != result_map.end(); itr++) {
    result.push_back(itr->second);
  }
}

//////////////////////////////////////////////////////////////////////////////
// Original method
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool RBBTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::InsertEntry(UNUSED_ATTRIBUTE const
                                                  storage::Tuple *key,
                                                  UNUSED_ATTRIBUTE const
                                                  ItemPointer &location) {
  // Add your implementation here
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool RBBTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::CondInsertEntry(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE const ItemPointer &location,
    UNUSED_ATTRIBUTE std::function<bool(const void *)> predicate,
    UNUSED_ATTRIBUTE ItemPointer **itemptr_ptr ) {
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    UNUSED_ATTRIBUTE const std::vector<Value> &values,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
    UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    UNUSED_ATTRIBUTE const std::vector<Value> &values,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
    UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
RBBTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

///////////////////////////////////////////////////////////////////////////////////////////

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::string RBBTreeIndex<KeyType, ValueType, KeyComparator,
                       KeyEqualityChecker>::GetTypeName() const {
  return "RBBtree";
}

// Explicit template instantiation
template class RBBTreeIndex<IntsKey<1>, RBItemPointer *, IntsComparator<1>,
                          IntsEqualityChecker<1>>;
template class RBBTreeIndex<IntsKey<2>, RBItemPointer *, IntsComparator<2>,
                          IntsEqualityChecker<2>>;
template class RBBTreeIndex<IntsKey<3>, RBItemPointer *, IntsComparator<3>,
                          IntsEqualityChecker<3>>;
template class RBBTreeIndex<IntsKey<4>, RBItemPointer *, IntsComparator<4>,
                          IntsEqualityChecker<4>>;

template class RBBTreeIndex<GenericKey<4>, RBItemPointer *,
                          GenericComparator<4>, GenericEqualityChecker<4>>;
template class RBBTreeIndex<GenericKey<8>, RBItemPointer *,
                          GenericComparator<8>, GenericEqualityChecker<8>>;
template class RBBTreeIndex<GenericKey<12>, RBItemPointer *,
                          GenericComparator<12>, GenericEqualityChecker<12>>;
template class RBBTreeIndex<GenericKey<16>, RBItemPointer *,
                          GenericComparator<16>, GenericEqualityChecker<16>>;
template class RBBTreeIndex<GenericKey<24>, RBItemPointer *,
                          GenericComparator<24>, GenericEqualityChecker<24>>;
template class RBBTreeIndex<GenericKey<32>, RBItemPointer *,
                          GenericComparator<32>, GenericEqualityChecker<32>>;
template class RBBTreeIndex<GenericKey<48>, RBItemPointer *,
                          GenericComparator<48>, GenericEqualityChecker<48>>;
template class RBBTreeIndex<GenericKey<64>, RBItemPointer *,
                          GenericComparator<64>, GenericEqualityChecker<64>>;
template class RBBTreeIndex<GenericKey<96>, RBItemPointer *,
                          GenericComparator<96>, GenericEqualityChecker<96>>;
template class RBBTreeIndex<GenericKey<128>, RBItemPointer *,
                          GenericComparator<128>, GenericEqualityChecker<128>>;
template class RBBTreeIndex<GenericKey<256>, RBItemPointer *,
                          GenericComparator<256>, GenericEqualityChecker<256>>;
template class RBBTreeIndex<GenericKey<512>, RBItemPointer *,
                          GenericComparator<512>, GenericEqualityChecker<512>>;

template class RBBTreeIndex<TupleKey, RBItemPointer *, TupleKeyComparator,
                          TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
