//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_factory.cpp
//
// Identification: src/backend/index/index_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/common/macros.h"
#include "backend/index/index_factory.h"
#include "backend/index/index_key.h"
#include "backend/index/bwtree_index.h"
#include "backend/index/btree_index.h"
#include "backend/index/hash_index.h"
#include "backend/index/rb_btree_index.h"
#include "backend/index/rb_hash_index.h"

namespace peloton {
namespace index {

SecondaryIndexType IndexFactory::secondary_index_type_ = SECONDARY_INDEX_TYPE_VERSION;

Index *IndexFactory::GetInstance(IndexMetadata *metadata, const size_t &preallocate_size) {
  bool ints_only = false;

  LOG_TRACE("Creating index %s", metadata->GetName().c_str());
  const auto key_size = metadata->key_schema->GetLength();

  auto index_type = metadata->GetIndexMethodType();
  LOG_TRACE("Index type : %d", index_type);

  // no int specialization beyond this point
  if (key_size > sizeof(int64_t) * 4) {
    ints_only = false;
  }

  if (ints_only && (index_type == INDEX_TYPE_BTREE)) {
    if (key_size <= sizeof(uint64_t)) {
      return new BTreeIndex<IntsKey<1>, ItemPointer *,
                            IntsComparator<1>, IntsEqualityChecker<1>>(
          metadata);
    } else if (key_size <= sizeof(int64_t) * 2) {
      return new BTreeIndex<IntsKey<2>, ItemPointer *,
                            IntsComparator<2>, IntsEqualityChecker<2>>(
          metadata);
    } else if (key_size <= sizeof(int64_t) * 3) {
      return new BTreeIndex<IntsKey<3>, ItemPointer *,
                            IntsComparator<3>, IntsEqualityChecker<3>>(
          metadata);
    } else if (key_size <= sizeof(int64_t) * 4) {
      return new BTreeIndex<IntsKey<4>, ItemPointer *,
                            IntsComparator<4>, IntsEqualityChecker<4>>(
          metadata);
    } else {
      throw IndexException("We currently only support tree index on non-unique "
                           "integer keys of size 32 bytes or smaller...");
    }
  }

  if (index_type == INDEX_TYPE_BTREE) {
    if (key_size <= 4) {
      return new BTreeIndex<GenericKey<4>,
                            ItemPointer *,
                            GenericComparator<4>,
                            GenericEqualityChecker<4>>(
          metadata);
    } else if (key_size <= 8) {
      return new BTreeIndex<GenericKey<8>, ItemPointer *,
                            GenericComparator<8>, GenericEqualityChecker<8>>(
          metadata);
    } else if (key_size <= 12) {
      return new BTreeIndex<GenericKey<12>, ItemPointer *,
                            GenericComparator<12>, GenericEqualityChecker<12>>(
          metadata);
    } else if (key_size <= 16) {
      return new BTreeIndex<GenericKey<16>, ItemPointer *,
                            GenericComparator<16>, GenericEqualityChecker<16>>(
          metadata);
    } else if (key_size <= 24) {
      return new BTreeIndex<GenericKey<24>, ItemPointer *,
                            GenericComparator<24>, GenericEqualityChecker<24>>(
          metadata);
    } else if (key_size <= 32) {
      return new BTreeIndex<GenericKey<32>, ItemPointer *,
                            GenericComparator<32>, GenericEqualityChecker<32>>(
          metadata);
    } else if (key_size <= 48) {
      return new BTreeIndex<GenericKey<48>, ItemPointer *,
                            GenericComparator<48>, GenericEqualityChecker<48>>(
          metadata);
    } else if (key_size <= 64) {
      return new BTreeIndex<GenericKey<64>, ItemPointer *,
                            GenericComparator<64>, GenericEqualityChecker<64>>(
          metadata);
    } else if (key_size <= 96) {
      return new BTreeIndex<GenericKey<96>, ItemPointer *,
                            GenericComparator<96>, GenericEqualityChecker<96>>(
          metadata);
    } else if (key_size <= 128) {
      return new BTreeIndex<GenericKey<128>, ItemPointer *,
                            GenericComparator<128>,
                            GenericEqualityChecker<128>>(metadata);
    } else if (key_size <= 256) {
      return new BTreeIndex<GenericKey<256>, ItemPointer *,
                            GenericComparator<256>,
                            GenericEqualityChecker<256>>(metadata);
    } else if (key_size <= 512) {
      return new BTreeIndex<GenericKey<512>, ItemPointer *,
                            GenericComparator<512>,
                            GenericEqualityChecker<512>>(metadata);
    } else {
      return new BTreeIndex<TupleKey, ItemPointer *,
                            TupleKeyComparator, TupleKeyEqualityChecker>(
          metadata);
    }
  }


  if (ints_only && (index_type == INDEX_TYPE_HASH)) {
    if (key_size <= sizeof(uint64_t)) {
      return new HashIndex<IntsKey<1>, ItemPointer *, IntsHasher<1>,
                            IntsComparator<1>, IntsEqualityChecker<1>>(
          metadata, preallocate_size);
    } else if (key_size <= sizeof(int64_t) * 2) {
      return new HashIndex<IntsKey<2>, ItemPointer *, IntsHasher<2>,
                            IntsComparator<2>, IntsEqualityChecker<2>>(
          metadata, preallocate_size);
    } else if (key_size <= sizeof(int64_t) * 3) {
      return new HashIndex<IntsKey<3>, ItemPointer *, IntsHasher<3>,
                            IntsComparator<3>, IntsEqualityChecker<3>>(
          metadata, preallocate_size);
    } else if (key_size <= sizeof(int64_t) * 4) {
      return new HashIndex<IntsKey<4>, ItemPointer *, IntsHasher<4>,
                            IntsComparator<4>, IntsEqualityChecker<4>>(
          metadata, preallocate_size);
    } else {
      throw IndexException("We currently only support tree index on non-unique "
                           "integer keys of size 32 bytes or smaller...");
    }
  }

  if (index_type == INDEX_TYPE_HASH) {
    if (key_size <= 4) {
      return new HashIndex<GenericKey<4>,
                            ItemPointer *,
                            GenericHasher<4>,
                            GenericComparator<4>,
                            GenericEqualityChecker<4>>(
          metadata, preallocate_size);
    } else if (key_size <= 8) {
      return new HashIndex<GenericKey<8>, ItemPointer *, GenericHasher<8>,
                            GenericComparator<8>, GenericEqualityChecker<8>>(
          metadata, preallocate_size);
    } else if (key_size <= 12) {
      return new HashIndex<GenericKey<12>, ItemPointer *, GenericHasher<12>,
                            GenericComparator<12>, GenericEqualityChecker<12>>(
          metadata, preallocate_size);
    } else if (key_size <= 16) {
      return new HashIndex<GenericKey<16>, ItemPointer *, GenericHasher<16>,
                            GenericComparator<16>, GenericEqualityChecker<16>>(
          metadata, preallocate_size);
    } else if (key_size <= 24) {
      return new HashIndex<GenericKey<24>, ItemPointer *, GenericHasher<24>,
                            GenericComparator<24>, GenericEqualityChecker<24>>(
          metadata, preallocate_size);
    } else if (key_size <= 32) {
      return new HashIndex<GenericKey<32>, ItemPointer *, GenericHasher<32>,
                            GenericComparator<32>, GenericEqualityChecker<32>>(
          metadata, preallocate_size);
    } else if (key_size <= 48) {
      return new HashIndex<GenericKey<48>, ItemPointer *, GenericHasher<48>,
                            GenericComparator<48>, GenericEqualityChecker<48>>(
          metadata, preallocate_size);
    } else if (key_size <= 64) {
      return new HashIndex<GenericKey<64>, ItemPointer *, GenericHasher<64>,
                            GenericComparator<64>, GenericEqualityChecker<64>>(
          metadata, preallocate_size);
    } else if (key_size <= 96) {
      return new HashIndex<GenericKey<96>, ItemPointer *, GenericHasher<96>,
                            GenericComparator<96>, GenericEqualityChecker<96>>(
          metadata, preallocate_size);
    } else if (key_size <= 128) {
      return new HashIndex<GenericKey<128>, ItemPointer *, GenericHasher<128>,
                            GenericComparator<128>,
                            GenericEqualityChecker<128>>(metadata, preallocate_size);
    } else if (key_size <= 256) {
      return new HashIndex<GenericKey<256>, ItemPointer *, GenericHasher<256>,
                            GenericComparator<256>,
                            GenericEqualityChecker<256>>(metadata, preallocate_size);
    } else if (key_size <= 512) {
      return new HashIndex<GenericKey<512>, ItemPointer *, GenericHasher<512>,
                            GenericComparator<512>,
                            GenericEqualityChecker<512>>(metadata, preallocate_size);
    } else {
      return new HashIndex<TupleKey, ItemPointer *, TupleKeyHasher,
                            TupleKeyComparator, TupleKeyEqualityChecker>(
          metadata, preallocate_size);
    }
  }

  if (ints_only && (index_type == INDEX_TYPE_BWTREE)) {
    if (key_size <= sizeof(uint64_t)) {
      return new BWTreeIndex<IntsKey<1>, ItemPointer *,
                             IntsComparator<1>,
                             IntsEqualityChecker<1>,
                             IntsHasher<1>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= sizeof(int64_t) * 2) {
      return new BWTreeIndex<IntsKey<2>, ItemPointer *,
                             IntsComparator<2>,
                             IntsEqualityChecker<2>,
                             IntsHasher<2>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= sizeof(int64_t) * 3) {
      return new BWTreeIndex<IntsKey<3>, ItemPointer *,
                             IntsComparator<3>,
                             IntsEqualityChecker<3>,
                             IntsHasher<3>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= sizeof(int64_t) * 4) {
      return new BWTreeIndex<IntsKey<4>, ItemPointer *,
                             IntsComparator<4>,
                             IntsEqualityChecker<4>,
                             IntsHasher<4>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else {
      throw IndexException("We currently only support tree index on non-unique "
                           "integer keys of size 32 bytes or smaller...");
    }
  }

  if (index_type == INDEX_TYPE_BWTREE) {
    if (key_size <= 4) {
      return new BWTreeIndex<GenericKey<4>, ItemPointer *,
                             GenericComparator<4>,
                             GenericEqualityChecker<4>,
                             GenericHasher<4>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= 8) {
      return new BWTreeIndex<GenericKey<8>, ItemPointer *,
                             GenericComparator<8>,
                             GenericEqualityChecker<8>,
                             GenericHasher<8>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= 12) {
      return new BWTreeIndex<GenericKey<12>, ItemPointer *,
                             GenericComparator<12>,
                             GenericEqualityChecker<12>,
                             GenericHasher<12>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= 16) {
      return new BWTreeIndex<GenericKey<16>, ItemPointer *,
                             GenericComparator<16>,
                             GenericEqualityChecker<16>,
                             GenericHasher<16>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= 24) {
      return new BWTreeIndex<GenericKey<24>, ItemPointer *,
                             GenericComparator<24>,
                             GenericEqualityChecker<24>,
                             GenericHasher<24>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= 32) {
      return new BWTreeIndex<GenericKey<32>, ItemPointer *,
                             GenericComparator<32>,
                             GenericEqualityChecker<32>,
                             GenericHasher<32>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= 48) {
      return new BWTreeIndex<GenericKey<48>, ItemPointer *,
                             GenericComparator<48>,
                             GenericEqualityChecker<48>,
                             GenericHasher<48>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= 64) {
      return new BWTreeIndex<GenericKey<64>, ItemPointer *,
                             GenericComparator<64>,
                             GenericEqualityChecker<64>,
                             GenericHasher<64>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= 96) {
      return new BWTreeIndex<GenericKey<96>, ItemPointer *,
                             GenericComparator<96>,
                             GenericEqualityChecker<96>,
                             GenericHasher<96>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    } else if (key_size <= 128) {
      return new BWTreeIndex<GenericKey<128>, ItemPointer *,
                             GenericComparator<128>,
                             GenericEqualityChecker<128>,
                             GenericHasher<128>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(metadata);
    } else if (key_size <= 256) {
      return new BWTreeIndex<GenericKey<256>, ItemPointer *,
                             GenericComparator<256>,
                             GenericEqualityChecker<256>,
                             GenericHasher<256>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(metadata);
    } else if (key_size <= 512) {
      return new BWTreeIndex<GenericKey<512>, ItemPointer *,
                             GenericComparator<512>,
                             GenericEqualityChecker<512>,
                             GenericHasher<512>,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(metadata);
    } else {
      return new BWTreeIndex<TupleKey, ItemPointer *,
                             TupleKeyComparator,
                             TupleKeyEqualityChecker,
                             TupleKeyHasher,
                             ItemPointerComparator,
                             ItemPointerHashFunc>(
          metadata);
    }
  }

  if (ints_only && (index_type == INDEX_TYPE_RBBTREE)) {
    if (key_size <= sizeof(uint64_t)) {
      return new RBBTreeIndex<IntsKey<1>, RBItemPointer *,
                             IntsComparator<1>, IntsEqualityChecker<1>>(
          metadata);
    } else if (key_size <= sizeof(int64_t) * 2) {
      return new RBBTreeIndex<IntsKey<2>, RBItemPointer *,
                             IntsComparator<2>, IntsEqualityChecker<2>>(
          metadata);
    } else if (key_size <= sizeof(int64_t) * 3) {
      return new RBBTreeIndex<IntsKey<3>, RBItemPointer *,
                             IntsComparator<3>, IntsEqualityChecker<3>>(
          metadata);
    } else if (key_size <= sizeof(int64_t) * 4) {
      return new RBBTreeIndex<IntsKey<4>, RBItemPointer *,
                             IntsComparator<4>, IntsEqualityChecker<4>>(
          metadata);
    } else {
      throw IndexException("We currently only support tree index on non-unique "
                           "integer keys of size 32 bytes or smaller...");
    }
  }

  if (index_type == INDEX_TYPE_RBBTREE) {
    if (key_size <= 4) {
      return new RBBTreeIndex<GenericKey<4>, RBItemPointer *,
                             GenericComparator<4>, GenericEqualityChecker<4>>(
          metadata);
    } else if (key_size <= 8) {
      return new RBBTreeIndex<GenericKey<8>, RBItemPointer *,
                             GenericComparator<8>, GenericEqualityChecker<8>>(
          metadata);
    } else if (key_size <= 12) {
      return new RBBTreeIndex<GenericKey<12>, RBItemPointer *,
                             GenericComparator<12>, GenericEqualityChecker<12>>(
          metadata);
    } else if (key_size <= 16) {
      return new RBBTreeIndex<GenericKey<16>, RBItemPointer *,
                             GenericComparator<16>, GenericEqualityChecker<16>>(
          metadata);
    } else if (key_size <= 24) {
      return new RBBTreeIndex<GenericKey<24>, RBItemPointer *,
                             GenericComparator<24>, GenericEqualityChecker<24>>(
          metadata);
    } else if (key_size <= 32) {
      return new RBBTreeIndex<GenericKey<32>, RBItemPointer *,
                             GenericComparator<32>, GenericEqualityChecker<32>>(
          metadata);
    } else if (key_size <= 48) {
      return new RBBTreeIndex<GenericKey<48>, RBItemPointer *,
                             GenericComparator<48>, GenericEqualityChecker<48>>(
          metadata);
    } else if (key_size <= 64) {
      return new RBBTreeIndex<GenericKey<64>, RBItemPointer *,
                             GenericComparator<64>, GenericEqualityChecker<64>>(
          metadata);
    } else if (key_size <= 96) {
      return new RBBTreeIndex<GenericKey<96>, RBItemPointer *,
                             GenericComparator<96>, GenericEqualityChecker<96>>(
          metadata);
    } else if (key_size <= 128) {
      return new RBBTreeIndex<GenericKey<128>, RBItemPointer *,
                             GenericComparator<128>,
                             GenericEqualityChecker<128>>(metadata);
    } else if (key_size <= 256) {
      return new RBBTreeIndex<GenericKey<256>, RBItemPointer *,
                             GenericComparator<256>,
                             GenericEqualityChecker<256>>(metadata);
    } else if (key_size <= 512) {
      return new RBBTreeIndex<GenericKey<512>, RBItemPointer *,
                             GenericComparator<512>,
                             GenericEqualityChecker<512>>(metadata);
    } else {
      return new RBBTreeIndex<TupleKey, RBItemPointer *,
                             TupleKeyComparator, TupleKeyEqualityChecker>(
          metadata);
    }
  }

  if (ints_only && (index_type == INDEX_TYPE_RBHASH)) {
    if (key_size <= sizeof(uint64_t)) {
      return new RBHashIndex<IntsKey<1>, RBItemPointer *, IntsHasher<1>,
                            IntsComparator<1>, IntsEqualityChecker<1>>(
          metadata, preallocate_size);
    } else if (key_size <= sizeof(int64_t) * 2) {
      return new RBHashIndex<IntsKey<2>, RBItemPointer *, IntsHasher<2>,
                            IntsComparator<2>, IntsEqualityChecker<2>>(
          metadata, preallocate_size);
    } else if (key_size <= sizeof(int64_t) * 3) {
      return new RBHashIndex<IntsKey<3>, RBItemPointer *, IntsHasher<3>,
                            IntsComparator<3>, IntsEqualityChecker<3>>(
          metadata, preallocate_size);
    } else if (key_size <= sizeof(int64_t) * 4) {
      return new RBHashIndex<IntsKey<4>, RBItemPointer *, IntsHasher<4>,
                            IntsComparator<4>, IntsEqualityChecker<4>>(
          metadata, preallocate_size);
    } else {
      throw IndexException("We currently only support tree index on non-unique "
                           "integer keys of size 32 bytes or smaller...");
    }
  }

  if (index_type == INDEX_TYPE_RBHASH) {
    if (key_size <= 4) {
      return new RBHashIndex<GenericKey<4>,
                            RBItemPointer *,
                            GenericHasher<4>,
                            GenericComparator<4>,
                            GenericEqualityChecker<4>>(
          metadata, preallocate_size);
    } else if (key_size <= 8) {
      return new RBHashIndex<GenericKey<8>, RBItemPointer *, GenericHasher<8>,
                            GenericComparator<8>, GenericEqualityChecker<8>>(
          metadata, preallocate_size);
    } else if (key_size <= 12) {
      return new RBHashIndex<GenericKey<12>, RBItemPointer *, GenericHasher<12>,
                            GenericComparator<12>, GenericEqualityChecker<12>>(
          metadata, preallocate_size);
    } else if (key_size <= 16) {
      return new RBHashIndex<GenericKey<16>, RBItemPointer *, GenericHasher<16>,
                            GenericComparator<16>, GenericEqualityChecker<16>>(
          metadata, preallocate_size);
    } else if (key_size <= 24) {
      return new RBHashIndex<GenericKey<24>, RBItemPointer *, GenericHasher<24>,
                            GenericComparator<24>, GenericEqualityChecker<24>>(
          metadata, preallocate_size);
    } else if (key_size <= 32) {
      return new RBHashIndex<GenericKey<32>, RBItemPointer *, GenericHasher<32>,
                            GenericComparator<32>, GenericEqualityChecker<32>>(
          metadata, preallocate_size);
    } else if (key_size <= 48) {
      return new RBHashIndex<GenericKey<48>, RBItemPointer *, GenericHasher<48>,
                            GenericComparator<48>, GenericEqualityChecker<48>>(
          metadata, preallocate_size);
    } else if (key_size <= 64) {
      return new RBHashIndex<GenericKey<64>, RBItemPointer *, GenericHasher<64>,
                            GenericComparator<64>, GenericEqualityChecker<64>>(
          metadata, preallocate_size);
    } else if (key_size <= 96) {
      return new RBHashIndex<GenericKey<96>, RBItemPointer *, GenericHasher<96>,
                            GenericComparator<96>, GenericEqualityChecker<96>>(
          metadata, preallocate_size);
    } else if (key_size <= 128) {
      return new RBHashIndex<GenericKey<128>, RBItemPointer *, GenericHasher<128>,
                            GenericComparator<128>,
                            GenericEqualityChecker<128>>(metadata, preallocate_size);
    } else if (key_size <= 256) {
      return new RBHashIndex<GenericKey<256>, RBItemPointer *, GenericHasher<256>,
                            GenericComparator<256>,
                            GenericEqualityChecker<256>>(metadata, preallocate_size);
    } else if (key_size <= 512) {
      return new RBHashIndex<GenericKey<512>, RBItemPointer *, GenericHasher<512>,
                            GenericComparator<512>,
                            GenericEqualityChecker<512>>(metadata, preallocate_size);
    } else {
      return new RBHashIndex<TupleKey, RBItemPointer *, TupleKeyHasher,
                            TupleKeyComparator, TupleKeyEqualityChecker>(
          metadata, preallocate_size);
    }
  }

  throw IndexException("Unsupported index scheme.");
  return NULL;
}

}  // End index namespace
}  // End peloton namespace
