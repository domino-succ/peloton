//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.h
//
// Identification: src/backend/benchmark/ycsb_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/benchmark/benchmark_common.h"
#include <iostream>

namespace peloton {
namespace benchmark {

const bool own_schema = true;
const bool adapt_table = false;
const bool is_inlined = true;
const bool unique_index = false;
const bool allocate = true;

const size_t name_length = 32;

// constraints checking system: CCS
storage::DataTable *ccs_table;

static const oid_t ccs_table_oid = 9004;
static const oid_t ccs_table_pkey_index_oid =
    90040;  // TXN_ID, TABLE_NAME, COL_NAME, VALUE
static const oid_t ccs_table_skey_index_oid =
    90041;  // TABLE_NAME, COL_NAME, TYPE, VALUE

// Helper function to pin current thread to a specific core
void PinToCore(size_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

void SleepMilliseconds(int n) {
  struct timespec ts_sleep = {n / 1000, (n % 1000) * 1000000L};
  nanosleep(&ts_sleep, NULL);
}

void CreateCcsTable(storage::Database *database, oid_t database_oid) {
  /*
   CREATE TABLE CSS (
   TXN_ID INTEGER DEFAULT '0' NOT NULL,
   TABLE_NAME INTEGER DEFAULT NULL,
   COLUMN_NAME INTEGER DEFAULT NULL,
   EXPRESSION_TYPE SMALLINT DEFAULT '0' NOT NULL,
   EXPRESSION_VALUE INTEGER DEFAULT '0' NOT NULL,
   THREAD INTEGER DEFAULT '0' NOT NULL,
   FACTOR INTEGER DEFAULT '0' NOT NULL,
   PRIMARY KEY (TXN_ID,TABLE_NAME,COLUMN_NAME,EXPRESSION_VALUE)
   SECONDARY KEY (TABLE_NAME,COLUMN_NAME,EXPRESSION_TYPE,EXPRESSION_VALUE)
   );
   */

  // Create schema first
  std::vector<catalog::Column> ccs_columns;

  // TXN_ID
  auto txn_id_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "TXN_ID", is_inlined);
  ccs_columns.push_back(txn_id_column);

  // TABLE NAME
  auto table_name_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "TABLE_NAME", is_inlined);
  ccs_columns.push_back(table_name_column);

  // COL NAME
  auto column_name_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "COL_NAME", is_inlined);
  ccs_columns.push_back(column_name_column);

  // TYPE
  auto type_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "TYPE", is_inlined);
  ccs_columns.push_back(type_column);

  // VALUE
  auto value_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "VALUE", is_inlined);
  ccs_columns.push_back(value_column);

  // THREAD NO.
  auto thread_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "THREAD", is_inlined);
  ccs_columns.push_back(thread_column);

  // FACTOR
  auto factor_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "FACTOR", is_inlined);
  ccs_columns.push_back(factor_column);

  catalog::Schema *table_schema = new catalog::Schema(ccs_columns);
  std::string table_name("CCS");

  ccs_table = storage::TableFactory::GetDataTable(
      database_oid, ccs_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  database->AddTable(ccs_table);

  // Primary index on TXN_ID TABLE_NAME, COL_NAME, VALUE
  std::vector<oid_t> key_attrs = {0, 1, 2, 4};

  auto tuple_schema = ccs_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "ccs_pkey", ccs_table_pkey_index_oid, INDEX_TYPE_HASH,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = nullptr;
  pkey_index = index::IndexFactory::GetInstance(index_metadata);

  ccs_table->AddIndex(pkey_index);

  // Secondary index on TABLE_NAME, COL_NAME, TYPE, VALUE
  key_attrs = {1, 2, 3, 4};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  index_metadata = new index::IndexMetadata(
      "ccs_skey", ccs_table_skey_index_oid, INDEX_TYPE_HASH,
      INDEX_CONSTRAINT_TYPE_INVALID, tuple_schema, key_schema, false);

  index::Index *skey_index = nullptr;

  skey_index = index::IndexFactory::GetInstance(index_metadata);

  ccs_table->AddIndex(skey_index);
}
}
}
