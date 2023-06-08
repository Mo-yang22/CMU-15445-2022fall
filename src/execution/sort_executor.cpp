#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <vector>
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      order_by_(plan->GetOrderBy()) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  tuples_.clear();
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples_.emplace_back(child_tuple);
  }
  // 将数据全部排序
  std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &a, const Tuple &b) -> bool {
    for (const auto &c : order_by_) {
      Value a_v = c.second->Evaluate(&a, child_executor_->GetOutputSchema());
      Value b_v = c.second->Evaluate(&b, child_executor_->GetOutputSchema());
      OrderByType order_by_type = c.first;
      if (a_v.CompareEquals(b_v) == CmpBool::CmpFalse) {
        assert(order_by_type != OrderByType::INVALID);
        if (order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC) {
          return a_v.CompareLessThan(b_v) == CmpBool::CmpTrue;
        }
        return a_v.CompareGreaterThan(b_v) == CmpBool::CmpTrue;
      }
    }
    return false;
  });
  index_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ >= tuples_.size()) {
    return false;
  }
  *tuple = tuples_[index_++];
  return true;
}

}  // namespace bustub
