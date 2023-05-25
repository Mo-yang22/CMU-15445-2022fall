//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <sstream>
#include <utility>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  // 将初始化的size设置为0
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comp) const -> int {
  int l = 1;
  int r = GetSize();
  while (l < r) {
    int mid = l + (r - l) / 2;
    if (comp(array_[mid].first, key) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return l;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Find(const KeyType &key, KeyComparator &comp) -> ValueType {
  for (int i = 1; i < GetSize(); i++) {
    KeyType cur_key = array_[i].first;
    if (comp(key, cur_key) < 0) {
      return array_[i - 1].second;
    }
  }
  return array_[GetSize() - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &left_value, const KeyType &key,
                                                     const ValueType &value) {
  int index = ValueIndex(left_value);
  // LOG_DEBUG("index: %d",index);
  assert(index != -1);
  // for (int i = index + 1; i < GetSize() - 1; i++) {
  //   array_[i + 1] = array_[i];
  // }
  std::move_backward(array_ + index + 1, array_ + GetSize(), array_ + GetSize() + 1);
  // assert(i >= 0);
  array_[index + 1] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &left_value, const KeyType &key,
                                                     const ValueType &value) {
  SetSize(2);
  SetKeyAt(1, key);
  SetValueAt(0, left_value);
  SetValueAt(1, value);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  std::copy(items, items + size, array_ + GetSize());
  // 将这些复制过来的指针指向的节点的父节点改成现在的这个
  for (int i = GetSize(); i < GetSize() + size; ++i) {
    auto child_page = buffer_pool_manager->FetchPage(ValueAt(i));
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page);
    // LOG_DEBUG("PageId() : %d",GetPageId());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(ValueAt(i), true);
  }
  IncreaseSize(size);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // 注意非叶子节点的最小size是GetMinSize() + 1
  int start_index = GetMinSize();
  int move_num = GetSize() - start_index;

  recipient->CopyNFrom(array_ + start_index, move_num, buffer_pool_manager);
  IncreaseSize(-1 * move_num);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, KeyComparator &comp) {
  int l = 1;
  int r = GetSize();
  while (l < r) {
    int mid = l + (r - l) / 2;
    if (comp(array_[mid].first, key) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  if (l == GetSize() || comp(array_[l].first, key) > 0) {
    return;
  }
  for (int i = l; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager,
                                               const KeyType &mid_key) {
  SetKeyAt(0, mid_key);
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  SetSize(0);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToLast(BPlusTreeInternalPage *recipient,
                                                     BufferPoolManager *buffer_pool_manager, const KeyType &mid_key) {
  SetKeyAt(0, mid_key);
  recipient->CopyNFrom(array_, 1, buffer_pool_manager);
  // 整体左移
  for (int i = 0; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFirst(BPlusTreeInternalPage *recipient,
                                                     BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(array_[GetSize() - 1], buffer_pool_manager);
  IncreaseSize(-1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &item, BufferPoolManager *buffer_pool_manager) {
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  array_[0] = item;

  auto child_page = buffer_pool_manager->FetchPage(ValueAt(0));
  auto child_node = reinterpret_cast<BPlusTreePage *>(child_page);
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(ValueAt(0), true);

  IncreaseSize(1);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
