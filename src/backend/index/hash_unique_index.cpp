//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_unique_index.cpp
//
// Identification: src/backend/index/hash_unique_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/hash_unique_index.h"
#include "backend/index/index_key.h"
#include "backend/common/logger.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator,
          KeyEqualityChecker>::HashUniqueIndex(IndexMetadata *metadata)
    : Index(metadata),
      container(KeyHasher(metadata), KeyEqualityChecker(metadata)),
      hasher(metadata),
      equals(metadata),
      comparator(metadata) { }


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator,
          KeyEqualityChecker>::~HashUniqueIndex() {
  // we should not rely on shared_ptr to reclaim memory.
  // this is because the underlying index can split or merge leaf nodes,
  // which invokes data data copy and deletes.
  // as the underlying index is unaware of shared_ptr,
  // memory allocated should be managed carefully by programmers.
  auto lt = container.lock_table();
  for (const auto &entry : lt) {
    if (metadata->GetIndexType() == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
      delete entry;
      entry = nullptr;
    }
  }
}

// void insert_helper(std::vector<ItemPointer*> &existing_vector, void *new_location) {
//   existing_vector.push_back((ItemPointer*)new_location);
// }

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
bool HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator,
               KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                                const ItemPointer &location,
                                                ItemPointer **itempointer_ptr) {
  KeyType index_key;

  index_key.SetFromKey(key);

  ItemPointer *new_location = new ItemPointer(location);
  if (itempointer_ptr != nullptr) {
    *itempointer_ptr = new_location;
  }
  // std::vector<ValueType> val;
  // val.push_back(new_location);
  // if there's no key in the hash map, then insert a vector containing location.
  // otherwise, directly insert location into the vector that already exists in the hash map.
  
  bool ret = container.insert(index_key, new_location);

  if (ret == true) {
    return true;
  } else {
    delete new_location;
    new_location = nullptr;
    return false;
  }
}

template <typename KeyType, typename ValueType, class KeyHasher,
  class KeyComparator, class KeyEqualityChecker>
bool HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator,
  KeyEqualityChecker>::InsertEntryInTupleIndex(const storage::Tuple *key, ItemPointer *location) {
  KeyType index_key;

  index_key.SetFromKey(key);

  ItemPointer *new_location = location;
  // std::vector<ValueType> val;
  // val.push_back(new_location);
  // if there's no key in the hash map, then insert a vector containing location.
  // otherwise, directly insert location into the vector that already exists in the hash map.

  bool ret = container.insert(index_key, new_location);

  if (ret == true) {
    return true;
  } else {
    delete new_location;
    new_location = nullptr;
    return false;
  }
}

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
bool HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator,
               KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                                const ItemPointer &location) {
  KeyType index_key;

  index_key.SetFromKey(key);

  return container.erase(index_key);

  // TODO: add retry logic
  container.update_fn(index_key, 
    [](std::vector<ItemPointer*> &existing_vector, void *old_location) {
      existing_vector.erase(std::remove_if(existing_vector.begin(), existing_vector.end(),
                               ItemPointerEqualityChecker(*((ItemPointer*)old_location))),
      existing_vector.end());
    },
    (void *)&location);

  return true;
}

template <typename KeyType, typename ValueType, class KeyHasher,
  class KeyComparator, class KeyEqualityChecker>
bool HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator,
  KeyEqualityChecker>::DeleteEntryInTupleIndex(const storage::Tuple *key, UNUSED_ATTRIBUTE ItemPointer *location) {
  KeyType index_key;

  index_key.SetFromKey(key);

  return container.erase(index_key);
  // TODO: add retry logic
}

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
bool HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator,
                KeyEqualityChecker>::CondInsertEntry(
    UNUSED_ATTRIBUTE const storage::Tuple *key, UNUSED_ATTRIBUTE const ItemPointer &location,
    UNUSED_ATTRIBUTE std::function<bool(const void *)> predicate,
    UNUSED_ATTRIBUTE ItemPointer **itempointer_ptr) {

  KeyType index_key;
  
  index_key.SetFromKey(key);

  ItemPointer *new_location = new ItemPointer(location);
  std::vector<ValueType> val;
  val.push_back(new_location);

  *itempointer_ptr = new_location;

  container.upsert(index_key, 
    [](std::vector<ItemPointer*> &existing_vector, void *new_location, std::function<bool(const void *)> predicate, void **arg_ptr) {
      for (auto entry : existing_vector) {
        if (predicate((void*)entry)) {
          *arg_ptr = nullptr;
          return;
        }
      }
      existing_vector.push_back((ItemPointer*)new_location);
      return;
    }, 
    (void*)new_location, predicate, (void**)itempointer_ptr, val);

  if (*itempointer_ptr == new_location) {
    
    return true;

  } else {
    LOG_TRACE("predicate fails, abort transaction");
    
    delete new_location;
    new_location = nullptr;
    
    return false;
  }
}

template <typename KeyType, typename ValueType, class KeyHasher,
  class KeyComparator, class KeyEqualityChecker>
bool HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator,
  KeyEqualityChecker>::CondInsertEntryInTupleIndex(
  UNUSED_ATTRIBUTE const storage::Tuple *key, UNUSED_ATTRIBUTE ItemPointer *location,
  UNUSED_ATTRIBUTE std::function<bool(const void *)> predicate) {

  KeyType index_key;

  index_key.SetFromKey(key);

  ItemPointer *new_location = location;
  std::vector<ValueType> val;
  val.push_back(new_location);

  container.upsert(index_key,
                   [](std::vector<ItemPointer*> &existing_vector, void *new_location, std::function<bool(const void *)> predicate, void **arg_ptr) {
                     for (auto entry : existing_vector) {
                       if (predicate((void*)entry)) {
                         *arg_ptr = nullptr;
                         return;
                       }
                     }
                     existing_vector.push_back((ItemPointer*)new_location);
                     return;
                   },
                   (void*)new_location, predicate, (void**)(&new_location), val);

  if (new_location != nullptr) {

    return true;

  } else {
    LOG_TRACE("predicate fails, abort transaction");

    delete new_location;
    new_location = nullptr;

    return false;
  }
}

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator, 
             KeyEqualityChecker>::Scan(UNUSED_ATTRIBUTE const std::vector<Value> &values,
                                       UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
                                       UNUSED_ATTRIBUTE const std::vector<ExpressionType> &exprs,
                                       UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
                                       UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {
  LOG_ERROR("hash index does not support scan!");
  assert(false);
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void
HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    std::vector<ItemPointer> &result) {
  {
    auto lt = container.lock_table();
    for (const auto &itr : lt) {
      for (const auto entry : itr.second) {
        result.push_back(ItemPointer(*entry));
      }
    }
  }
  LOG_INFO("scane key called, result size = %lu", result.size());
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key, std::vector<ItemPointer> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  std::vector<ItemPointer*> values;

  bool ret = container.find(index_key, values);

  if (ret == true) {
    for (auto entry : values) {
      result.push_back(*entry);
    }
  } else {
    assert(result.size() == 0);
  }
  LOG_INFO("scane key called, result size = %lu", result.size());
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator, 
             KeyEqualityChecker>::Scan(UNUSED_ATTRIBUTE const std::vector<Value> &values,
                                       UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
                                       UNUSED_ATTRIBUTE const std::vector<ExpressionType> &exprs,
                                       UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
                                       UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {
  LOG_ERROR("hash index does not support scan!");
  assert(false);
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator, 
             KeyEqualityChecker>::ScanAllKeys(std::vector<ItemPointer *> &result) {
  {
    auto lt = container.lock_table();
    for (const auto &itr : lt) {
      result.insert(result.end(), itr.second.begin(), itr.second.end());
    }
  }
  LOG_INFO("scane key called, result size = %lu", result.size());
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key, std::vector<ItemPointer *> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  container.find(index_key, result);
  LOG_INFO("scane key called, result size = %lu", result.size());
}




template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
std::string HashUniqueIndex<KeyType, ValueType, KeyHasher, KeyComparator,
                      KeyEqualityChecker>::GetTypeName() const {
  return "Hash";
}

// Explicit template instantiation
template class HashUniqueIndex<IntsKey<1>, ItemPointer *, IntsHasher<1>,
                         IntsComparator<1>, IntsEqualityChecker<1>>;
template class HashUniqueIndex<IntsKey<2>, ItemPointer *, IntsHasher<2>,
                         IntsComparator<2>, IntsEqualityChecker<2>>;
template class HashUniqueIndex<IntsKey<3>, ItemPointer *, IntsHasher<3>,
                         IntsComparator<3>, IntsEqualityChecker<3>>;
template class HashUniqueIndex<IntsKey<4>, ItemPointer *, IntsHasher<4>,
                         IntsComparator<4>, IntsEqualityChecker<4>>;

template class HashUniqueIndex<GenericKey<4>, ItemPointer *, GenericHasher<4>,
                         GenericComparator<4>, GenericEqualityChecker<4>>;
template class HashUniqueIndex<GenericKey<8>, ItemPointer *, GenericHasher<8>,
                         GenericComparator<8>, GenericEqualityChecker<8>>;
template class HashUniqueIndex<GenericKey<12>, ItemPointer *, GenericHasher<12>,
                         GenericComparator<12>, GenericEqualityChecker<12>>;
template class HashUniqueIndex<GenericKey<16>, ItemPointer *, GenericHasher<16>,
                         GenericComparator<16>, GenericEqualityChecker<16>>;
template class HashUniqueIndex<GenericKey<24>, ItemPointer *, GenericHasher<24>,
                         GenericComparator<24>, GenericEqualityChecker<24>>;
template class HashUniqueIndex<GenericKey<32>, ItemPointer *, GenericHasher<32>,
                         GenericComparator<32>, GenericEqualityChecker<32>>;
template class HashUniqueIndex<GenericKey<48>, ItemPointer *, GenericHasher<48>,
                         GenericComparator<48>, GenericEqualityChecker<48>>;
template class HashUniqueIndex<GenericKey<64>, ItemPointer *, GenericHasher<64>,
                         GenericComparator<64>, GenericEqualityChecker<64>>;
template class HashUniqueIndex<GenericKey<96>, ItemPointer *, GenericHasher<96>,
                         GenericComparator<96>, GenericEqualityChecker<96>>;
template class HashUniqueIndex<GenericKey<128>, ItemPointer *, GenericHasher<128>,
                         GenericComparator<128>, GenericEqualityChecker<128>>;
template class HashUniqueIndex<GenericKey<256>, ItemPointer *, GenericHasher<256>,
                         GenericComparator<256>, GenericEqualityChecker<256>>;
template class HashUniqueIndex<GenericKey<512>, ItemPointer *, GenericHasher<512>,
                         GenericComparator<512>, GenericEqualityChecker<512>>;

template class HashUniqueIndex<TupleKey, ItemPointer *, TupleKeyHasher,
                         TupleKeyComparator, TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace