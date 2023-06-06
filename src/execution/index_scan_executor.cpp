//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      index_info_{exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())},
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())},
      iter_{tree_->GetBeginIterator()} {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tree_->GetEndIterator()) {
    tuple = nullptr;
    rid = nullptr;
    return false;
  }
  RID cur_rid = (*iter_).second;
  ++iter_;
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  auto status = table_info->table_->GetTuple(cur_rid, tuple, exec_ctx_->GetTransaction());
  if (!status) {
    LOG_DEBUG("error");
  }
  *rid = cur_rid;
  return true;
}

}  // namespace bustub
