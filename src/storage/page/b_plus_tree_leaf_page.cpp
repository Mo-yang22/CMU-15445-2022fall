//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  return array_[index].second;
}

// 正常情况是要返回false的，返回true代表这个叶子节点中有这个key，而要求的是不重复
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &key,ValueType *value, KeyComparator comp) ->bool{
  int l = 0;
  int r = GetSize();
  while(l < r){
    int mid = l + (r - l) / 2;
    if(comp(array_[mid].first,key) < 0){
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  // return !static_cast<bool>(l == GetSize() || comp(array_[l].first,key) > 0);
  if(l == GetSize() || comp(array_[l].first,key) >0){
    return false;
  }
  *value = array_[l].second;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertInLeaf(const KeyType &key, ValueType &value, KeyComparator comp)->bool{
  // 之前就已经满了,直接返回false
  if (GetSize() == GetMaxSize() - 1){
    return false;
  }
  // 二分查找到要插入的index,为 l
  int l = 0;
  int r = GetSize();
  while (l < r) {
    int mid = l + (r - l) / 2;
    if (comp(array_[mid].first,key) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  // 从后往前将l后面的元素都往后移动一个
  for (int i = GetSize(); i > l; i--){
    array_[i] = array_[i-1];
  }
  // 在l处插入k/v
  array_[l] = {key,value};
  IncreaseSize(1);
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
