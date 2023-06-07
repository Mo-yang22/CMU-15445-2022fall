//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "storage/index/b_plus_tree_index.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"
namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())),
      table_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    Value value = plan_->KeyPredicate()->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
    Tuple key{{value}, index_info_->index_->GetKeySchema()};
    std::vector<RID> res{};
    tree_->ScanKey(key, &res, exec_ctx_->GetTransaction());
    for (auto rid : res) {
      Tuple right_tuple{};
      auto status = table_info_->table_->GetTuple(rid, &right_tuple, exec_ctx_->GetTransaction());
      assert(status);
      std::vector<Value> values{};
      for (uint32_t idx = 0; idx < child_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        values.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < table_info_->schema_.GetColumnCount(); idx++) {
        values.push_back(right_tuple.GetValue(&table_info_->schema_, idx));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    if (res.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values{};
      for (uint32_t idx = 0; idx < child_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        values.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < table_info_->schema_.GetColumnCount(); idx++) {
        values.push_back(ValueFactory::GetNullValueByType(table_info_->schema_.GetColumn(idx).GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
  }
  return false;
}

}  // namespace bustub
