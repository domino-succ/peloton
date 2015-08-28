//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.cpp
//
// Identification: src/backend/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>
#include <thread>
#include <iomanip>
#include <mutex>

#include "backend/concurrency/transaction_manager.h"

#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/common/exception.h"
#include "backend/common/synch.h"
#include "backend/common/logger.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace concurrency {

TransactionManager::TransactionManager() {
  next_txn_id = ATOMIC_VAR_INIT(START_TXN_ID);
  next_cid = ATOMIC_VAR_INIT(START_CID);

  // BASE transaction
  // All transactions are based on this transaction
  last_txn = new Transaction(START_TXN_ID, START_CID);
  last_txn->cid = START_CID;
  last_cid = START_CID;
}

TransactionManager::~TransactionManager() {
  // delete BASE txn
  delete last_txn;
}

// Get entry in table
Transaction *TransactionManager::GetTransaction(txn_id_t txn_id) {
  {
    std::lock_guard<std::mutex> lock(txn_table_mutex);
    if (txn_table.count(txn_id) != 0) {
      return txn_table.at(txn_id);
    }
  }

  return nullptr;
}

txn_id_t TransactionManager::GetNextTransactionId() {
  if (next_txn_id == MAX_TXN_ID) {
    throw TransactionException("Txn id equals MAX_TXN_ID");
  }

  return next_txn_id++;
}

// Begin a new transaction
Transaction *TransactionManager::BeginTransaction() {
  Transaction *next_txn =
      new Transaction(GetNextTransactionId(), GetLastCommitId());

  {
    std::lock_guard<std::mutex> lock(txn_table_mutex);
    auto txn_id = next_txn->txn_id;

    // erase entry in transaction table
    if (txn_table.count(txn_id) != 0) {
      txn_table.insert(std::make_pair(txn_id, next_txn));
    }
  }

 // Log the BEGIN TXN record
 {
    auto& log_manager = logging::LogManager::GetInstance();
    if(log_manager.IsInLoggingMode()){
      auto logger = log_manager.GetBackendLogger();
      auto record = new logging::TransactionRecord(LOGRECORD_TYPE_TRANSACTION_BEGIN,
                                                   next_txn->txn_id);
      logger->Log(record);
    }
 }

  return next_txn;
}

std::vector<Transaction *> TransactionManager::GetCurrentTransactions() {
  std::vector<Transaction *> txns;

  {
    std::lock_guard<std::mutex> lock(txn_table_mutex);
    for (auto entry : txn_table) txns.push_back(entry.second);
  }

  return txns;
}

bool TransactionManager::IsValid(txn_id_t txn_id) {
  return (txn_id < next_txn_id);
}

void TransactionManager::ResetStates(void){
  next_txn_id = ATOMIC_VAR_INIT(START_TXN_ID);
  next_cid = ATOMIC_VAR_INIT(START_CID);

  // BASE transaction
  // All transactions are based on this transaction
  delete last_txn;
  last_txn = new Transaction(START_TXN_ID, START_CID);
  last_txn->cid = START_CID;
  last_cid = START_CID;

  for(auto txn : txn_table){
    auto curr_txn = txn.second;
    delete curr_txn;
  }
  txn_table.clear();
  for(auto txn : pg_txn_table){
    auto curr_txn = txn.second;
    delete curr_txn;
  }
  pg_txn_table.clear();
}

void TransactionManager::EndTransaction(Transaction *txn,
                                        bool sync __attribute__((unused))) {

  // Log the END TXN record
  {
    auto& log_manager = logging::LogManager::GetInstance();
    if(log_manager.IsInLoggingMode()){
      auto logger = log_manager.GetBackendLogger();
      auto record = new logging::TransactionRecord(LOGRECORD_TYPE_TRANSACTION_END,
                                                   txn->txn_id);
      logger->Log(record);

      // Check for sync commit
      // If true, wait for the fronted logger to flush the data
      if( log_manager.GetSyncCommit())  {
        while(logger->IsWaitingForFlushing()){
          sleep(1);
        }
      }

    }
  }


  {
    std::lock_guard<std::mutex> lock(txn_table_mutex);
    // erase entry in transaction table
    txn_table.erase(txn->txn_id);
  }
}

// Store an entry in PG Transaction table
Transaction *TransactionManager::StartPGTransaction(TransactionId txn_id) {
  {
    std::lock_guard<std::mutex> lock(pg_txn_table_mutex);

    // If entry already exists
    if (pg_txn_table.count(txn_id) != 0) return pg_txn_table.at(txn_id);

    // Else, create one for this new pg txn id
    auto txn = BeginTransaction();
    auto status = pg_txn_table.insert(std::make_pair(txn_id, txn));
    if (status.second == true) {
      return txn;
    }
  }

  return nullptr;
}

// Get entry in PG Transaction table
Transaction *TransactionManager::GetPGTransaction(TransactionId txn_id) {
  {
    std::lock_guard<std::mutex> lock(pg_txn_table_mutex);

    // If entry already exists
    if (pg_txn_table.count(txn_id) != 0) {
      auto txn = pg_txn_table.at(txn_id);
      return txn;
    }
  }

  return nullptr;
}

//===--------------------------------------------------------------------===//
// Commit Processing
//===--------------------------------------------------------------------===//

TransactionManager &TransactionManager::GetInstance() {
  static TransactionManager txn_manager;
  return txn_manager;
}

void TransactionManager::BeginCommitPhase(Transaction *txn) {
  // successor in the transaction list will point to us
  txn->IncrementRefCount();

  {
    std::lock_guard<std::mutex> lock(txn_table_mutex);

    // append to the pending transaction list
    last_txn->next = txn;

    // the last transaction pointer also points to us
    txn->IncrementRefCount();

    // assign cid to the txn
    txn->cid = last_txn->cid + 1;

    auto tmp = last_txn;
    last_txn = txn;

    // drop a reference to previous last transaction pointer
    tmp->DecrementRefCount();
  }
}

void TransactionManager::CommitModifications(Transaction *txn, bool sync
                                             __attribute__((unused))) {

  // (A) commit inserts
  auto inserted_tuples = txn->GetInsertedTuples();
  for (auto entry : inserted_tuples) {
    storage::TileGroup *tile_group = entry.first;

    for (auto tuple_slot : entry.second)
      tile_group->CommitInsertedTuple(tuple_slot, txn->cid);
  }

  // (B) commit deletes
  auto deleted_tuples = txn->GetDeletedTuples();
  for (auto entry : deleted_tuples) {
    storage::TileGroup *tile_group = entry.first;
    for (auto tuple_slot : entry.second)
      tile_group->CommitDeletedTuple(tuple_slot, txn->txn_id, txn->cid);
  }

  // Log the COMMIT TXN record
  {
    auto& log_manager = logging::LogManager::GetInstance();
    if(log_manager.IsInLoggingMode()){
      auto logger = log_manager.GetBackendLogger();
      auto record = new logging::TransactionRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT,
                                                   txn->txn_id);
      logger->Log(record);
    }
  }
}

void TransactionManager::CommitPendingTransactions(
    std::vector<Transaction *> &pending_txns, Transaction *txn) {
  // add ourself to the list
  pending_txns.push_back(txn);

  // commit all pending transactions
  auto current_txn = txn->next;

  while (current_txn != nullptr && current_txn->waiting_to_commit == true) {
    // try to increment last finished cid
    if (atomic_cas(&last_cid, current_txn->cid - 1, current_txn->cid)) {
      // if that worked, add transaction to list
      pending_txns.push_back(current_txn);
      LOG_TRACE("Pending Txn  : %lu \n", current_txn->txn_id);

      current_txn = current_txn->next;
      continue;
    }
    // it did not work, so some other txn must have squeezed in
    // so, stop processing commit dependencies
    else {
      break;
    }
  }
}

std::vector<Transaction *> TransactionManager::EndCommitPhase(Transaction *txn,
                                                              bool sync) {
  std::vector<Transaction *> txn_list;

  // try to increment last commit id
  if (atomic_cas(&last_cid, txn->cid - 1, txn->cid)) {
    LOG_TRACE("update lcid worked : %lu \n", txn->txn_id);

    // everything went fine and the txn was committed
    // if that worked, commit all pending transactions
    CommitPendingTransactions(txn_list, txn);

  }
  // it did not work, so add to waiting list
  // some other transaction with lower commit id will commit us later
  else {
    LOG_TRACE("add to wait list : %lu \n", txn->txn_id);

    txn->waiting_to_commit = true;

    // make sure that the transaction we are waiting for has not finished
    // before we could add ourselves to the list of pending transactions
    // we try incrementing the last finished cid again
    if (atomic_cas(&last_cid, txn->cid - 1, txn->cid)) {
      // it worked on the second try
      txn->waiting_to_commit = false;

      CommitPendingTransactions(txn_list, txn);
    }
  }

  // clear txn entry in txn table
  EndTransaction(txn, sync);

  return std::move(txn_list);
}

void TransactionManager::CommitTransaction(Transaction *txn, bool sync) {
  assert(txn != nullptr);

  // validate txn id
  if (!IsValid(txn->txn_id)) {
    throw TransactionException("Transaction not found in transaction table : " +
                               std::to_string(txn->txn_id));
  }

  // begin commit phase : get cid and add to transaction list
  BeginCommitPhase(txn);

  // commit all modifications
  CommitModifications(txn, sync);

  // end commit phase : increment last_cid and process pending txns if needed
  std::vector<Transaction *> committed_txns = EndCommitPhase(txn, sync);

  // process all committed txns
  for (auto committed_txn : committed_txns) committed_txn->DecrementRefCount();

  // XXX LOG : group commit entry
  // we already record commit entry in CommitModifications, isn't it?

}

//===--------------------------------------------------------------------===//
// Abort Processing
//===--------------------------------------------------------------------===//

void TransactionManager::AbortTransaction(Transaction *txn) {

  // Log the ABORT TXN record
  {
    auto& log_manager = logging::LogManager::GetInstance();
    if(log_manager.IsInLoggingMode()){
      auto logger = log_manager.GetBackendLogger();
      auto record = new logging::TransactionRecord(LOGRECORD_TYPE_TRANSACTION_ABORT,
                                                   txn->txn_id);
      logger->Log(record);

    }
  }

  // (A) rollback inserts
  auto inserted_tuples = txn->GetInsertedTuples();
  for (auto entry : inserted_tuples) {
    storage::TileGroup *tile_group = entry.first;

    for (auto tuple_slot : entry.second)
      tile_group->AbortInsertedTuple(tuple_slot);
  }

  // (B) rollback deletes
  auto deleted_tuples = txn->GetDeletedTuples();
  for (auto entry : txn->GetDeletedTuples()) {
    storage::TileGroup *tile_group = entry.first;

    for (auto tuple_slot : entry.second)
      tile_group->AbortDeletedTuple(tuple_slot);
  }

  EndTransaction(txn, false);

  // drop a reference
  txn->DecrementRefCount();

}

}  // End concurrency namespace
}  // End peloton namespace
