//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>  //NOLINT
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "type/limits.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // LOG_DEBUG("LockTable : txn_id : %d, table_id : %d", txn->GetTransactionId(), oid);
  // 第一步,检查txn的状态
  TransactionState state = txn->GetState();
  IsolationLevel level = txn->GetIsolationLevel();
  if (state == TransactionState::ABORTED || state == TransactionState::COMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw("mother fucker");
    return false;
  }
  if (state == TransactionState::SHRINKING) {
    if (level == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
      return false;
    }
    if (level == IsolationLevel::READ_COMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
          lock_mode == LockMode::INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
      }
    } else if (level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED};
    }
  } else if (state == TransactionState::GROWING && level == IsolationLevel::READ_UNCOMMITTED &&
             (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
              lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED};
  }
  // 第二步,获取lock_request_queue
  table_lock_map_latch_.lock();
  // std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;

  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  // 第三步,检查此锁请求是否为一次锁升级
  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid && request->rid_ == RID()) {
      // 1.判断是否可以升级
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException{txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT};
      }
      if (lock_mode == request->lock_mode_) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      if (!IsUpgradeLegal(request->lock_mode_, lock_mode)) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException{txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE};
      }

      // 2.释放当前已经持有的锁,并在queue中标记正在尝试升级
      lock_request_queue->latch_.unlock();
      UnlockTable(txn, oid, true);
      lock_request_queue->latch_.lock();
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      // 找到了相同的txn就break掉
      break;
    }
  }
  // 第四步,将锁请求加入请求队列
  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.emplace_back(new_request);

  // 第五步,尝试获取锁
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request_queue, new_request, txn)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->latch_.unlock();
      lock_request_queue->request_queue_.remove(new_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }

  // 进行Transaction集合的维护
  switch (new_request->lock_mode_) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->emplace(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->emplace(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->emplace(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->emplace(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->emplace(oid);
  }
  lock_request_queue->latch_.unlock();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid, bool is_upgrade) -> bool {
  // LOG_DEBUG("UnLockTable : txn_id : %d, table_id : %d", txn->GetTransactionId(), oid);
  TransactionState state = txn->GetState();
  IsolationLevel level = txn->GetIsolationLevel();
  LockMode cur_lock_mode;
  table_lock_map_latch_.lock();
  assert(table_lock_map_.find(oid) != table_lock_map_.end());
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  // 第一步 首先判断txn在oid上是不是有锁
  bool is_have = false;
  std::shared_ptr<LockRequest> iter;

  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid && request->rid_ == RID()) {
      if (request->granted_) {
        iter = request;
        is_have = true;
      }
      break;
    }
  }
  if (!is_have) {
    lock_request_queue->latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD};
  }

  // 第二步,当要解锁表上的锁时,这个txn不能持有表里面row的锁
  // ?仔细一想,好像锁升级的时候也不能持有row上的锁
  if ((txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end() &&
       !txn->GetSharedRowLockSet()->at(oid).empty()) ||
      (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end() &&
       !txn->GetExclusiveRowLockSet()->at(oid).empty())) {
    lock_request_queue->latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS};
  }

  cur_lock_mode = iter->lock_mode_;

  // 变化txn状态
  // ?出现了bug,在事务的commit和abort阶段应该会集中调用unlock,但是这时候就不能将状态改成shrinking了
  if (!is_upgrade && state == TransactionState::GROWING) {
    if ((level == IsolationLevel::REPEATABLE_READ &&
         (cur_lock_mode == LockMode::SHARED || cur_lock_mode == LockMode::EXCLUSIVE)) ||
        (level == IsolationLevel::READ_COMMITTED && cur_lock_mode == LockMode::EXCLUSIVE)) {
      txn->SetState(TransactionState::SHRINKING);
    } else if (level == IsolationLevel::READ_UNCOMMITTED) {
      assert(cur_lock_mode != LockMode::SHARED);
      if (cur_lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
  }

  // 进行事务锁集合的维护
  txn->GetExclusiveRowLockSet()->erase(oid);
  txn->GetSharedRowLockSet()->erase(oid);
  switch (cur_lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
  }
  // 核心步骤,释放锁
  lock_request_queue->request_queue_.remove(iter);
  lock_request_queue->latch_.unlock();
  lock_request_queue->cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // LOG_DEBUG("LockRow :table_oid_t: %d, txn_id_t:%d", oid, txn->GetTransactionId());
  // LOG_DEBUG("LockRow : txn_id : %d, table_id : %d, rid_page_id: %d, rid_slot_num : %u", txn->GetTransactionId(), oid,
  //           rid.GetPageId(), rid.GetSlotNum());
  // row中新增的第零步,不应该加意向锁
  // 且当在row上加锁时,表上应该加上适当的锁
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT};
    }
    // 操,大意了,没有闪,被讲义误导了,在row上请求shared锁,表上但凡有个锁都行?
  } else if (lock_mode == LockMode::SHARED) {
    if (!txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionSharedLocked(oid) &&
        !txn->IsTableExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
        !txn->IsTableIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT};
    }
  } else {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW};
  }

  // 第一步,检查txn的状态
  TransactionState state = txn->GetState();
  IsolationLevel level = txn->GetIsolationLevel();
  if (state == TransactionState::ABORTED || state == TransactionState::COMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw("mother fucker");
    return false;
  }
  if (state == TransactionState::SHRINKING) {
    if (level == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
      return false;
    }
    if (level == IsolationLevel::READ_COMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
      }
    } else if (level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED};
    }
  } else if (state == TransactionState::GROWING && level == IsolationLevel::READ_UNCOMMITTED &&
             (lock_mode == LockMode::SHARED)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED};
  }

  // 第二步,获取lock_request_queue
  table_lock_map_latch_.lock();
  // std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  // 第三步,检查此锁请求是否为一次锁升级
  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid && request->rid_ == rid) {
      // 1.判断是否可以升级
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException{txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT};
      }
      if (lock_mode == request->lock_mode_) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      if (!IsUpgradeLegal(request->lock_mode_, lock_mode)) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException{txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE};
      }

      // 2.释放当前已经持有的锁,并在queue中标记正在尝试升级
      lock_request_queue->latch_.unlock();
      UnlockRow(txn, oid, rid, true);
      lock_request_queue->latch_.lock();
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      // 找到了相同的txn就break掉
      break;
    }
  }
  // 第四步,将锁请求加入请求队列
  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.emplace_back(new_request);

  // 第五步,尝试获取锁
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantRowLock(lock_request_queue, new_request, txn)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(new_request);
      lock_request_queue->latch_.unlock();
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  // 进行Transaction集合的维护
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedLockSet()->emplace(rid);
    if (txn->GetSharedRowLockSet()->find(oid) == txn->GetSharedRowLockSet()->end()) {
      txn->GetSharedRowLockSet()->emplace(std::make_pair(oid, std::unordered_set<RID>()));
    }
    txn->GetSharedRowLockSet()->at(oid).emplace(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveLockSet()->emplace(rid);
    if (txn->GetExclusiveRowLockSet()->find(oid) == txn->GetExclusiveRowLockSet()->end()) {
      txn->GetExclusiveRowLockSet()->emplace(std::make_pair(oid, std::unordered_set<RID>()));
    }
    txn->GetExclusiveRowLockSet()->at(oid).emplace(rid);
  }
  lock_request_queue->latch_.unlock();
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool is_upgrade) -> bool {
  // LOG_DEBUG("UnLockRow : txn_id : %d, table_id : %d, rid_page_id: %d, rid_slot_num : %u", txn->GetTransactionId(),
  // oid,
  //           rid.GetPageId(), rid.GetSlotNum());
  TransactionState state = txn->GetState();
  IsolationLevel level = txn->GetIsolationLevel();
  LockMode cur_lock_mode;
  table_lock_map_latch_.lock();
  assert(table_lock_map_.find(oid) != table_lock_map_.end());
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  // 第一步 首先判断txn在oid上是不是有锁
  bool is_have = false;
  std::shared_ptr<LockRequest> iter;
  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid && request->rid_ == rid) {
      if (request->granted_) {
        iter = request;
        is_have = true;
      }
      break;
    }
  }
  if (!is_have) {
    lock_request_queue->latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD};
  }

  // // 第二步,当要解锁表上的锁时,这个txn不能持有表里面row的锁
  // // ?仔细一想,好像锁升级的时候也不能持有row上的锁
  // if ((txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end() &&
  //      !txn->GetSharedRowLockSet()->at(oid).empty()) ||
  //     (txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end() &&
  //      !txn->GetExclusiveRowLockSet()->at(oid).empty())) {
  //   lock_request_queue->latch_.unlock();
  //   txn->SetState(TransactionState::ABORTED);
  //   throw TransactionAbortException{txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS};
  // }

  cur_lock_mode = (iter)->lock_mode_;

  // 变化txn状态
  // ?出现了bug,在事务的commit和abort阶段应该会集中调用unlock,但是这时候就不能将状态改成shrinking了
  if (!is_upgrade && state == TransactionState::GROWING) {
    if ((level == IsolationLevel::REPEATABLE_READ &&
         (cur_lock_mode == LockMode::SHARED || cur_lock_mode == LockMode::EXCLUSIVE)) ||
        (level == IsolationLevel::READ_COMMITTED && cur_lock_mode == LockMode::EXCLUSIVE)) {
      txn->SetState(TransactionState::SHRINKING);
    } else if (level == IsolationLevel::READ_UNCOMMITTED) {
      assert(cur_lock_mode != LockMode::SHARED);
      if (cur_lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
  }

  // 进行事务锁集合的维护
  if (cur_lock_mode == LockMode::SHARED) {
    txn->GetSharedLockSet()->erase(rid);
    txn->GetSharedRowLockSet()->at(oid).erase(rid);
  } else if (cur_lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveLockSet()->erase(rid);
    txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
  }
  // 核心步骤,释放锁

  lock_request_queue->request_queue_.remove(iter);
  lock_request_queue->latch_.unlock();
  lock_request_queue->cv_.notify_all();

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_.emplace(t1, std::vector<txn_id_t>());
  }

  // 如果这条边已经存在,直接返回
  if (std::find(waits_for_.at(t1).begin(), waits_for_.at(t1).end(), t2) != waits_for_.at(t1).end()) {
    waits_for_latch_.unlock();
    return;
  }
  waits_for_.at(t1).emplace_back(t2);
  waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_latch_.lock();
    return;
  }
  std::vector<txn_id_t> v = waits_for_.at(t1);
  auto iter = std::find(v.begin(), v.end(), t2);
  if (iter == v.end()) {
    waits_for_latch_.lock();
    return;
  }
  v.erase(iter);
  if (v.empty()) {
    waits_for_.erase(t1);
  }
  waits_for_latch_.lock();
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  waits_for_latch_.lock();
  // 首先排序,从最小的txn_id开始找
  std::vector<txn_id_t> sort_v{};
  for (const auto &wait : waits_for_) {
    sort_v.emplace_back(wait.first);
  }
  // LOG_DEBUG("number of txns: %ld", sort_v.size());
  std::sort(sort_v.begin(), sort_v.end());
  for (auto i : sort_v) {
    auto status = DFS(i);
    if (status) {
      // 找最年轻的txn的id返回
      txn_id_t max_txn_id = -1;
      for (size_t j = index_; j < path_.size(); ++j) {
        if (path_[j] > max_txn_id) {
          max_txn_id = path_[j];
        }
      }
      assert(max_txn_id != -1);
      *txn_id = max_txn_id;
      path_.clear();
      index_ = 0;
      waits_for_latch_.unlock();
      return true;
    }
  }
  path_.clear();
  index_ = 0;
  waits_for_latch_.unlock();
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  waits_for_latch_.lock();
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &i : waits_for_) {
    for (auto j : i.second) {
      edges.emplace_back(i.first, j);
    }
  }
  waits_for_latch_.unlock();
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      std::vector<txn_id_t> grant_txn_id{};
      // 第一步 建图
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      for (const auto &table_lock : table_lock_map_) {
        // 将所有已经授予资源的txn放在一起
        table_lock.second->latch_.lock();
        for (const auto &request : (table_lock.second->request_queue_)) {
          if (request->granted_) {
            grant_txn_id.emplace_back(request->txn_id_);
          }
        }
        // 再遍历一次,对于每个缺资源的和有资源的都要建立一条边
        for (const auto &request : table_lock.second->request_queue_) {
          if (!request->granted_) {
            for (auto txn_id : grant_txn_id) {
              map_txn_oid_.emplace(request->txn_id_, request->oid_);
              AddEdge(request->txn_id_, txn_id);
            }
          }
        }
        table_lock.second->latch_.unlock();
        grant_txn_id.clear();
      }
      table_lock_map_latch_.unlock();

      // 继续建图
      for (const auto &row_lock : row_lock_map_) {
        row_lock.second->latch_.lock();
        for (auto &request : row_lock.second->request_queue_) {
          if (request->granted_) {
            grant_txn_id.emplace_back(request->txn_id_);
          }
        }
        for (auto &request : row_lock.second->request_queue_) {
          if (!request->granted_) {
            for (auto txn_id : grant_txn_id) {
              map_txn_rid_.emplace(request->txn_id_, request->rid_);
              AddEdge(request->txn_id_, txn_id);
            }
          }
        }
        row_lock.second->latch_.unlock();
        grant_txn_id.clear();
      }
      row_lock_map_latch_.unlock();
      // 用while,因为可能不止一个死循环
      txn_id_t cycle_txn;
      while (HasCycle(&cycle_txn)) {
        TransactionManager::GetTransaction(cycle_txn)->SetState(TransactionState::ABORTED);
        // 开删
        // 先删这个事务指向的
        waits_for_.erase(cycle_txn);
        // 再删被指向的
        for (auto &wait : waits_for_) {
          assert(wait.first != cycle_txn);
          RemoveEdge(wait.first, cycle_txn);
        }
        if (map_txn_oid_.count(cycle_txn) > 0) {
          table_lock_map_[map_txn_oid_[cycle_txn]]->latch_.lock();
          // // ?request忘记删了
          // for (auto request : table_lock_map_[map_txn_oid_[cycle_txn]]->request_queue_) {
          //   if (request->txn_id_ == cycle_txn) {
          //     // list中删除应该没事
          //     table_lock_map_[map_txn_oid_[cycle_txn]]->request_queue_.remove(request);
          //     delete request;
          //   }
          // }
          table_lock_map_[map_txn_oid_[cycle_txn]]->cv_.notify_all();
          table_lock_map_[map_txn_oid_[cycle_txn]]->latch_.unlock();
        }

        if (map_txn_rid_.count(cycle_txn) > 0) {
          row_lock_map_[map_txn_rid_[cycle_txn]]->latch_.lock();
          // for (auto request : row_lock_map_[map_txn_rid_[cycle_txn]]->request_queue_) {
          //   if (request->txn_id_ == cycle_txn) {
          //     // list中删除应该没事
          //     row_lock_map_[map_txn_rid_[cycle_txn]]->request_queue_.remove(request);
          //     delete request;
          //   }
          // }
          row_lock_map_[map_txn_rid_[cycle_txn]]->cv_.notify_all();
          row_lock_map_[map_txn_rid_[cycle_txn]]->latch_.unlock();
        }
      }
      waits_for_.clear();
      map_txn_oid_.clear();
      map_txn_rid_.clear();
    }
  }
}

auto LockManager::DFS(txn_id_t cur) -> bool {
  // ?我是个傻逼
  if (waits_for_.find(cur) == waits_for_.end()) {
    return false;
  }
  std::vector<txn_id_t> tmp(waits_for_.at(cur));
  std::sort(tmp.begin(), tmp.end());
  for (auto txn_id : tmp) {
    for (size_t i = 0; i < path_.size(); ++i) {
      if (path_[i] == cur) {
        index_ = i;
        return true;
      }
    }
    path_.emplace_back(txn_id);
    auto status = DFS(txn_id);
    if (status) {
      return true;
    }
    path_.pop_back();
  }

  return false;
}

auto LockManager::IsUpgradeLegal(LockMode cur_mode, LockMode up_mode) -> bool {
  switch (cur_mode) {
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (up_mode == LockMode::EXCLUSIVE) {
        return true;
      }
      return false;
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED:
      if (up_mode == LockMode::EXCLUSIVE || up_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      return false;
    case LockMode::INTENTION_SHARED:
      return true;
  }
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequestQueue> &lock_request_queue,
                            const std::shared_ptr<LockRequest> &cur_request, Transaction *txn) -> bool {
  // 1.看跟已授予lock的类型是不是相容,注意要看所有的,不能只看单个
  // ?是不是要加request->rid_ == RID()
  for (auto &request : lock_request_queue->request_queue_) {
    if (request->granted_ && request->rid_ == RID() && !IsCompatible(cur_request->lock_mode_, request->lock_mode_)) {
      return false;
    }
  }
  // 2.看是否当前请求是升级锁的请求
  // ?是不是当有升级要求然后自己不是就直接G
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    if (lock_request_queue->upgrading_ != cur_request->txn_id_) {
      return false;
    }
    // 授予锁
    cur_request->granted_ = true;
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    return true;
  }
  // 3.再次遍历,前面的waiting 的request必须与现在这个相兼容
  bool is_grant = false;
  for (auto &request : lock_request_queue->request_queue_) {
    if (!request->granted_ && request->txn_id_ == cur_request->txn_id_) {
      is_grant = true;
      break;
    }
    if (!request->granted_ && request->rid_ == RID() && !IsCompatible(request->lock_mode_, cur_request->lock_mode_)) {
      break;
    }
  }
  if (!is_grant) {
    return false;
  }
  cur_request->granted_ = true;
  return true;
}
// 是不是可以这样理解,在考虑是不是赋予row锁时,不需要考虑其他事务对于这个表的影响,因为我一定要先获得表锁,就相当于我有赋予某个锁的能力
// 只需看是否在这个row上是否与其他事务(有可能是同一个事务)产生冲突
auto LockManager::GrantRowLock(const std::shared_ptr<LockRequestQueue> &lock_request_queue,
                               const std::shared_ptr<LockRequest> &cur_request, Transaction *txn) -> bool {
  // 1.看跟已授予lock的类型是不是相容,注意要看所有的,不能只看单个
  for (auto &request : lock_request_queue->request_queue_) {
    if (request->granted_ && request->rid_ == cur_request->rid_ &&
        !IsCompatible(cur_request->lock_mode_, request->lock_mode_)) {
      return false;
    }
  }
  // 2.看是否当前请求是升级锁的请求
  // ?是不是当有升级要求然后自己不是就直接G
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    if (lock_request_queue->upgrading_ != cur_request->txn_id_) {
      return false;
    }
    // 授予锁
    cur_request->granted_ = true;
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    return true;
  }
  // 3.再次遍历,前面的waiting 的request必须与现在这个相兼容
  bool is_grant = false;
  for (auto &request : lock_request_queue->request_queue_) {
    if (!request->granted_ && request->txn_id_ == cur_request->txn_id_) {
      is_grant = true;
      break;
    }
    if (!request->granted_ && request->rid_ == cur_request->rid_ &&
        !IsCompatible(request->lock_mode_, cur_request->lock_mode_)) {
      break;
    }
  }
  if (!is_grant) {
    return false;
  }
  cur_request->granted_ = true;
  return true;
}

auto LockManager::IsCompatible(LockMode cur_mode, LockMode mode) -> bool {
  switch (cur_mode) {
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (mode == LockMode::INTENTION_SHARED) {
        return true;
      }
      return false;
    case LockMode::SHARED:
      if (mode == LockMode::INTENTION_SHARED || mode == LockMode::SHARED) {
        return true;
      }
      return false;
    case LockMode::INTENTION_EXCLUSIVE:
      if (mode == LockMode::INTENTION_SHARED || mode == LockMode::INTENTION_EXCLUSIVE) {
        return true;
      }
      return false;
    case LockMode::INTENTION_SHARED:
      if (mode == LockMode::EXCLUSIVE) {
        return false;
      }
      return true;
  }
}

}  // namespace bustub
