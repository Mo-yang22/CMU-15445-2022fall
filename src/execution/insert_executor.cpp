//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  is_end_ = false;
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  try {
    auto status = lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
    if (!status) {
      throw ExecutionException{"Insert Executor Get Table Lock Failed"};
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException{"Insert Executor Get Table Lock Failed" + e.GetInfo()};
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int32_t insert_count = 0;
  const TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo *> indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {
    table_info->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());
    // insert应该是要插入后再在行上加锁
    try {
      auto status = lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->TableOid(), *(rid));
      if (!status) {
        throw ExecutionException{"Insert Executor Get Row Lock Failed"};
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException{"Insert Executor Get Row Lock Failed" + e.GetInfo()};
    }
    for (auto index : indexs) {
      index->index_->InsertEntry(
          child_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()), *rid,
          exec_ctx_->GetTransaction());
      const Tuple tmp = child_tuple;
      txn->GetIndexWriteSet()->push_back(
          IndexWriteRecord(*rid, plan_->TableOid(), WType::INSERT, tmp, index->index_oid_, exec_ctx_->GetCatalog()));
    }
    insert_count++;
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, insert_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
