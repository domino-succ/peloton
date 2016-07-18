//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_unique_index.h
//
// Identification: src/backend/index/hash_unique_index.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>

#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/index/index.h"

#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace index {

/**
 * Using libcuckoo for hash index
 *
 * @see Index
 */
template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
class HashUniqueIndex : public Index {
  friend class IndexFactory;

  // Define the container type
  typedef cuckoohash_map<KeyType, ValueType, KeyHasher,
                         KeyEqualityChecker> MapType;

 public:
  HashUniqueIndex(IndexMetadata *metadata);

  ~HashUniqueIndex();

  bool InsertEntry(const storage::Tuple *key, const ItemPointer &location, ItemPointer **itempointer_ptr);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer &location);

  bool CondInsertEntry(const storage::Tuple *key, const ItemPointer &location,
                       std::function<bool(const void *)> predicate,
                       ItemPointer **itempointer_ptr);

  void Scan(const std::vector<Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            const ScanDirectionType &scan_direction,
            std::vector<ItemPointer> &);

  void ScanAllKeys(std::vector<ItemPointer> &);

  void ScanKey(const storage::Tuple *key, std::vector<ItemPointer> &);

  void Scan(const std::vector<Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &exprs,
            const ScanDirectionType &scan_direction,
            std::vector<ItemPointer *> &result);

  void ScanAllKeys(std::vector<ItemPointer *> &result);

  void ScanKey(const storage::Tuple *key,
               std::vector<ItemPointer *> &result);

  std::string GetTypeName() const;

  virtual size_t GetIndexSize() const {
    LOG_INFO("hash index size = %lu", container.size());
    return container.size();
  }

  bool Cleanup() { return true; }

  size_t GetMemoryFootprint() {
    // TODO: implement it
    return 0;
  }

 protected:
  MapType container;

  // equality checker and comparator
  KeyHasher hasher;
  KeyEqualityChecker equals;
  KeyComparator comparator;
};

}  // End index namespace
}  // End peloton namespace

