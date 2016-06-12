//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_index.cpp
//
// Identification: src/backend/index/hash_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/hash_index.h"
#include "backend/index/index_key.h"
#include "backend/common/logger.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
          KeyEqualityChecker>::HashIndex(IndexMetadata *metadata, UNUSED_ATTRIBUTE const size_t &preallocate_size)
    : Index(metadata),
      container(KeyHasher(metadata), KeyEqualityChecker(metadata), preallocate_size),
      hasher(metadata),
      equals(metadata),
      comparator(metadata) { }

// struct ItemPointerEqualityChecker {
//   ItemPointer arg_;
//   ItemPointerEqualityChecker(ItemPointer arg) : arg_(arg) {}
//   bool operator()(ItemPointer *x) {
//     return x->block == arg_.block && x->offset == arg_.offset;
//   }
// };

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
          KeyEqualityChecker>::~HashIndex() {
  // we should not rely on shared_ptr to reclaim memory.
  // this is because the underlying index can split or merge leaf nodes,
  // which invokes data data copy and deletes.
  // as the underlying index is unaware of shared_ptr,
  // memory allocated should be managed carefully by programmers.
  auto lt = container.lock_table();
  for (const auto &entry_vector : lt) {
    for (auto entry : entry_vector.second) {
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
bool HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
               KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                                const ItemPointer &location) {
  KeyType index_key;

  index_key.SetFromKey(key);

  ItemPointer *new_location = new ItemPointer(location);
  std::vector<ValueType> val;
  val.push_back(new_location);
  // if there's no key in the hash map, then insert a vector containing location.
  // otherwise, directly insert location into the vector that already exists in the hash map.
  container.upsert(index_key, 
    [](std::vector<ItemPointer*> &existing_vector, void *new_location){
        existing_vector.push_back((ItemPointer*)new_location);
    }, 
    (void *)new_location, val);

  return true;
}

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
bool HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
               KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                                const ItemPointer &location) {
  KeyType index_key;

  index_key.SetFromKey(key);

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
bool HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
                KeyEqualityChecker>::CondInsertEntry(
    const storage::Tuple *key, const ItemPointer &location,
    std::function<bool(const void *)> predicate,
    ItemPointer **itempointer_ptr) {

  KeyType index_key;
  
  index_key.SetFromKey(key);

  ItemPointer *new_location = new ItemPointer(location);
  std::vector<ValueType> val;
  val.push_back(new_location);

  *itempointer_ptr = new_location;

  container.upsert(index_key, 
    [](std::vector<ItemPointer*> &existing_vector, void *location, std::function<bool(const void *)> predicate, void **arg_ptr) {
      for (auto entry : existing_vector) {
        if (predicate((void*)entry)) {
          *arg_ptr = nullptr;
          return;
        }
      }
      existing_vector.push_back((ItemPointer*)location);
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
void HashIndex<KeyType, ValueType, KeyHasher, KeyComparator, 
             KeyEqualityChecker>::Scan(const std::vector<Value> &values,
                                       const std::vector<oid_t> &key_column_ids,
                                       const std::vector<ExpressionType> &expr_types,
                                       UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
                                       std::vector<ItemPointer> &result) {
  KeyType index_key;
  std::unique_ptr<storage::Tuple> start_key;
  start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

  bool all_constraints_are_equal = ConstructLowerBoundTuple(
          start_key.get(), values, key_column_ids, expr_types);
  if (all_constraints_are_equal == false) {
    LOG_ERROR("not all constraints are equal!");
    assert(false);
  }

  index_key.SetFromKey(start_key.get());
  
  std::vector<ItemPointer*> tmp_result;
  container.find(index_key, tmp_result);
  for (auto entry : tmp_result) {
    result.push_back(ItemPointer(*entry));
  }
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void HashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    std::vector<ItemPointer> &result) {
  {
    auto lt = container.lock_table();
    for (const auto &itr : lt) {
      for (const auto entry : itr.second) {
        result.push_back(ItemPointer(*entry));
      }
    }
  }
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void HashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanKey(
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
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void HashIndex<KeyType, ValueType, KeyHasher, KeyComparator, 
             KeyEqualityChecker>::Scan(const std::vector<Value> &values,
                                       const std::vector<oid_t> &key_column_ids,
                                       const std::vector<ExpressionType> &expr_types,
                                       UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
                                       std::vector<ItemPointer *> &result) {

  KeyType index_key;
  std::unique_ptr<storage::Tuple> start_key;
  start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

  bool all_constraints_are_equal = ConstructLowerBoundTuple(
          start_key.get(), values, key_column_ids, expr_types);
  if (all_constraints_are_equal == false) {
    LOG_ERROR("not all constraints are equal!");
    assert(false);
  }

  index_key.SetFromKey(start_key.get());
  
  container.find(index_key, result);
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void HashIndex<KeyType, ValueType, KeyHasher, KeyComparator, 
             KeyEqualityChecker>::ScanAllKeys(std::vector<ItemPointer *> &result) {
  {
    auto lt = container.lock_table();
    for (const auto &itr : lt) {
      result.insert(result.end(), itr.second.begin(), itr.second.end());
    }
  }
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void HashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key, std::vector<ItemPointer *> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  container.find(index_key, result);
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
std::string HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
                      KeyEqualityChecker>::GetTypeName() const {
  return "Hash";
}

// Explicit template instantiation
template class HashIndex<IntsKey<1>, ItemPointer *, IntsHasher<1>,
                         IntsComparator<1>, IntsEqualityChecker<1>>;
template class HashIndex<IntsKey<2>, ItemPointer *, IntsHasher<2>,
                         IntsComparator<2>, IntsEqualityChecker<2>>;
template class HashIndex<IntsKey<3>, ItemPointer *, IntsHasher<3>,
                         IntsComparator<3>, IntsEqualityChecker<3>>;
template class HashIndex<IntsKey<4>, ItemPointer *, IntsHasher<4>,
                         IntsComparator<4>, IntsEqualityChecker<4>>;

template class HashIndex<GenericKey<4>, ItemPointer *, GenericHasher<4>,
                         GenericComparator<4>, GenericEqualityChecker<4>>;
template class HashIndex<GenericKey<8>, ItemPointer *, GenericHasher<8>,
                         GenericComparator<8>, GenericEqualityChecker<8>>;
template class HashIndex<GenericKey<12>, ItemPointer *, GenericHasher<12>,
                         GenericComparator<12>, GenericEqualityChecker<12>>;
template class HashIndex<GenericKey<16>, ItemPointer *, GenericHasher<16>,
                         GenericComparator<16>, GenericEqualityChecker<16>>;
template class HashIndex<GenericKey<24>, ItemPointer *, GenericHasher<24>,
                         GenericComparator<24>, GenericEqualityChecker<24>>;
template class HashIndex<GenericKey<32>, ItemPointer *, GenericHasher<32>,
                         GenericComparator<32>, GenericEqualityChecker<32>>;
template class HashIndex<GenericKey<48>, ItemPointer *, GenericHasher<48>,
                         GenericComparator<48>, GenericEqualityChecker<48>>;
template class HashIndex<GenericKey<64>, ItemPointer *, GenericHasher<64>,
                         GenericComparator<64>, GenericEqualityChecker<64>>;
template class HashIndex<GenericKey<96>, ItemPointer *, GenericHasher<96>,
                         GenericComparator<96>, GenericEqualityChecker<96>>;
template class HashIndex<GenericKey<128>, ItemPointer *, GenericHasher<128>,
                         GenericComparator<128>, GenericEqualityChecker<128>>;
template class HashIndex<GenericKey<256>, ItemPointer *, GenericHasher<256>,
                         GenericComparator<256>, GenericEqualityChecker<256>>;
template class HashIndex<GenericKey<512>, ItemPointer *, GenericHasher<512>,
                         GenericComparator<512>, GenericEqualityChecker<512>>;

template class HashIndex<TupleKey, ItemPointer *, TupleKeyHasher,
                         TupleKeyComparator, TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace