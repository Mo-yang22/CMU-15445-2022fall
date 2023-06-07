//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/executors/delete_executor.h"
#include "type/type.h"
#include "type/type_id.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  is_end_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  const TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo *> indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  Tuple child_tuple{};
  int32_t delete_count{0};
  while (child_executor_->Next(&child_tuple, rid)) {
    RID child_rid = child_tuple.GetRid();
    table_info->table_->MarkDelete(child_rid, exec_ctx_->GetTransaction());
    for (auto index : indexs) {
      // 一定要注意是怎么删除的
      index->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()), child_rid,
          exec_ctx_->GetTransaction());
    }
    delete_count++;
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, delete_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
