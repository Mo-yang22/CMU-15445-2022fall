//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/logger.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (left_executor_->Next(&tuple, &rid)) {
    left_table_.emplace_back(tuple);
  }
  while (right_executor_->Next(&tuple, &rid)) {
    right_table_.emplace_back(tuple);
  }
  left_index_ = 0;
  right_index_ = 0;
  is_none_ = true;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto expr = &plan_->Predicate();

  while (true) {
    if (left_index_ == left_table_.size()) {
      return false;
    }
    if (!right_table_.empty()) {
      auto value = expr->EvaluateJoin(&left_table_[left_index_], left_executor_->GetOutputSchema(),
                                      &right_table_[right_index_], right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        is_none_ = false;
        std::vector<Value> values{};
        for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          values.push_back(left_table_[left_index_].GetValue(&left_executor_->GetOutputSchema(), idx));
        }
        for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          values.push_back(right_table_[right_index_].GetValue(&right_executor_->GetOutputSchema(), idx));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        if (right_index_ < right_table_.size() - 1) {
          right_index_++;
          assert(right_index_ < right_table_.size());
        } else {
          is_none_ = true;
          right_index_ = 0;
          left_index_++;
        }
        return true;
      }
    }
    if (!right_table_.empty() && right_index_ < right_table_.size() - 1) {
      right_index_++;
    } else {
      if (is_none_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values{};
        for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          values.push_back(left_table_[left_index_].GetValue(&left_executor_->GetOutputSchema(), idx));
        }
        for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          values.push_back(
              ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(idx).GetType()));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        right_index_ = 0;
        left_index_++;
        is_none_ = true;
        return true;
      }
      right_index_ = 0;
      left_index_++;
      is_none_ = true;
    }
  }
}

}  // namespace bustub
