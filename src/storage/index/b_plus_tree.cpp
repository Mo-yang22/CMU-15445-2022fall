#include <any>
#include <cstring>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 对root_page_id上锁
  root_latch_.RLock();
  auto page = FindLeaf(key, Operation::SEARCH, transaction);
  auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  if (leaf_node->LookUp(key, &value, comparator_)) {
    result->emplace_back(value);
    page->RUnlatch();
    // ?
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return true;
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);
  if (IsEmpty()) {
    // create an empty leaf node L, which is also the root, then insert
    StartNewTree(key, value);
    ReleaseLatchFromQueue(Operation::INSERT, transaction);
    return true;
  }
  // 不管怎么样,一定能找到叶子节点
  // Find the leaf node L that should contain key value K
  auto page = FindLeaf(key, Operation::INSERT, transaction);
  auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());

  // 先看是不是重复的key
  ValueType tmp;
  if (leaf_node->LookUp(key, &tmp, comparator_)) {
    ReleaseLatchFromQueue(Operation::INSERT, transaction);
    return false;
  }
  // L has less than n-1 key values
  if (leaf_node->GetSize() < leaf_max_size_ - 1) {
    leaf_node->Insert(key, value, comparator_);
    ReleaseLatchFromQueue(Operation::INSERT, transaction);
    return true;
  }
  // 需要分裂
  // leaf_node本身就有一个位置没有用,刚好用作插入
  leaf_node->Insert(key, value, comparator_);
  // ?忘记插入父母了
  auto right_node = Split(leaf_node, transaction);
  auto right_leaf_node = reinterpret_cast<LeafPage *>(right_node);
  KeyType up_key = right_leaf_node->KeyAt(0);
  InsertInParent(leaf_node, right_leaf_node, up_key, transaction);

  ReleaseLatchFromQueue(Operation::INSERT, transaction);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left_node, BPlusTreePage *right_node, const KeyType &key,
                                    Transaction *transaction) {
  // left_node is the root of the tree
  if (left_node->IsRootPage()) {
    // create a new node

    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    UpdateRootPageId();
    page->WLatch();
    transaction->AddIntoPageSet(page);
    assert(left_node->GetPageId() != root_page_id_);
    auto inner_node = reinterpret_cast<InternalPage *>(page->GetData());
    inner_node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    inner_node->PopulateNewRoot(left_node->GetPageId(), key, right_node->GetPageId());

    left_node->SetParentPageId(root_page_id_);
    right_node->SetParentPageId(root_page_id_);
    return;
  }

  // Let P = parent(N)
  page_id_t p_page_id = left_node->GetParentPageId();

  auto p_page = buffer_pool_manager_->FetchPage(p_page_id);

  auto p_node = reinterpret_cast<InternalPage *>(p_page->GetData());
  // P has less than n points
  if (p_node->GetSize() < internal_max_size_) {
    p_node->InsertNodeAfter(left_node->GetPageId(), key, right_node->GetPageId());
    // ?
    buffer_pool_manager_->UnpinPage(p_page_id, true);
    return;
  }

  // Copy P to a block of memory T that can hold P and (K',N')
  assert(internal_max_size_ == p_node->GetSize());
  auto mem = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (p_node->GetSize() + 1)];
  auto copy_node = reinterpret_cast<InternalPage *>(mem);
  std::memcpy(mem, p_page->GetData(), INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (p_node->GetSize()));
  // 假设copy_node的size超过了max_size,会存在问题吗
  // Insert (K',N') into T just  after N
  copy_node->InsertNodeAfter(left_node->GetPageId(), key, right_node->GetPageId());
  // 将copy_node分裂
  auto new_node = Split(copy_node, transaction);
  auto new_sibling_node = reinterpret_cast<InternalPage *>(new_node);
  KeyType new_key = new_sibling_node->KeyAt(0);
  assert(copy_node->GetSize() == copy_node->GetMinSize());
  std::memcpy(p_page->GetData(), mem, INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (copy_node->GetMinSize()));
  InsertInParent(p_node, new_sibling_node, new_key, transaction);
  // ?
  buffer_pool_manager_->UnpinPage(p_page_id, true);
  delete[] mem;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) -> N * {
  // 分裂的策略是左边取min_size,右边取剩余的
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  new_page->WLatch();
  transaction->AddIntoPageSet(new_page);
  auto new_node = reinterpret_cast<N *>(new_page->GetData());
  if (node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    auto new_leaf_node = reinterpret_cast<LeafPage *>(new_node);
    new_leaf_node->Init(new_page_id, leaf_node->GetParentPageId(), leaf_max_size_);
    leaf_node->MoveHalfTo(new_leaf_node);
    // ?忘记写兄弟节点的变化了
    new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(new_node->GetPageId());
    return new_node;
  }
  auto inner_node = reinterpret_cast<InternalPage *>(node);
  auto new_inner_node = reinterpret_cast<InternalPage *>(new_node);
  new_inner_node->Init(new_page_id, inner_node->GetParentPageId(), internal_max_size_);
  inner_node->MoveHalfTo(new_inner_node, buffer_pool_manager_);
  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto page = buffer_pool_manager_->NewPage(&root_page_id_);
  UpdateRootPageId(1);
  auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  leaf_node->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);
  if (IsEmpty()) {
    ReleaseLatchFromQueue(Operation::DELETE, transaction);
    return;
  }
  auto page = FindLeaf(key, Operation::DELETE, transaction);
  auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  RemoveEntry(leaf_node, key, transaction);
  ReleaseLatchFromQueue(Operation::DELETE, transaction);
  DeleteAllPage(transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(BPlusTreePage *node, const KeyType &key, Transaction *transaction) {
  if (node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    leaf_node->Delete(key, comparator_);
    // 当叶子节点同时也是根节点,并且没有元素了
    if (node->IsRootPage() && node->GetSize() == 0) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId();
      return;
    }
    // 当为根节点或者元组数量大于半满状态
    if (node->IsRootPage()) {
      return;
    }
    if (leaf_node->GetSize() >= leaf_node->GetMinSize()) {
      return;
    }
    // is_right表示是不是sibling_node在右边
    KeyType mid_key;
    bool is_right;
    auto sibling_node = reinterpret_cast<BPlusTreePage *>(FindSibling(leaf_node, &mid_key, &is_right, transaction));

    // entries in N and N' can fit in a single node
    // 优先合并
    if (sibling_node->GetSize() + leaf_node->GetSize() <= leaf_node->GetMaxSize() - 1) {
      Coalesce(&node, &sibling_node, is_right, mid_key, transaction);
      return;
    }
    Redistribute(node, sibling_node, is_right, mid_key, transaction);
    return;
  }
  auto inner_node = reinterpret_cast<InternalPage *>(node);
  inner_node->Delete(key, comparator_);
  if (node->IsRootPage() && node->GetSize() == 1) {
    page_id_t new_root_page_id = inner_node->ValueAt(0);
    // buffer_pool_manager_->DeletePage(node->GetPageId());
    transaction->AddIntoDeletedPageSet(node->GetPageId());
    root_page_id_ = new_root_page_id;
    // ?还要将新的根节点的父节点设置为无效
    auto root_node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    root_node->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }

  if (node->IsRootPage()) {
    return;
  }
  if (inner_node->GetSize() >= inner_node->GetMinSize()) {
    return;
  }
  // N has too few k/v
  KeyType mid_key;
  bool is_right;
  auto sibling_node = reinterpret_cast<BPlusTreePage *>(FindSibling(inner_node, &mid_key, &is_right, transaction));
  // entries in N and N' can fit in a single node
  // Coalesce nodes
  if (sibling_node->GetSize() + inner_node->GetSize() <= inner_node->GetMaxSize()) {
    // 将sibling_node合并到node上,假如sibling_node在左边,则交换
    Coalesce(&node, &sibling_node, is_right, mid_key, transaction);
    return;
  }
  // Redistribution: borrow an entry from N'
  Redistribute(node, sibling_node, is_right, mid_key, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::FindSibling(N *node, KeyType *key, bool *is_right, Transaction *transaction) -> N * {
  auto parent_node =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  int index = parent_node->ValueIndex(node->GetPageId());
  assert(index != -1);
  // 优先取右方的兄弟
  int sibling_index;
  KeyType mid_key;
  if (index == parent_node->GetSize() - 1) {
    sibling_index = index - 1;
    mid_key = parent_node->KeyAt(index);
    *is_right = false;
  } else {
    sibling_index = index + 1;
    mid_key = parent_node->KeyAt(sibling_index);
    *is_right = true;
  }
  *key = mid_key;
  auto sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(sibling_index));
  auto sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
  sibling_page->WLatch();
  transaction->AddIntoPageSet(sibling_page);
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  return sibling_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(BPlusTreePage **node, BPlusTreePage **sibling_node, bool is_right, const KeyType &mid_key,
                              Transaction *transaction) {
  if (!is_right) {
    // sibling_node需要在node的右边
    std::swap(node, sibling_node);
  }

  if ((*node)->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(*node);
    auto leaf_sibling_node = reinterpret_cast<LeafPage *>(*sibling_node);
    leaf_sibling_node->MoveAllTo(leaf_node);
    leaf_node->SetNextPageId(leaf_sibling_node->GetNextPageId());
  } else {
    auto inner_node = reinterpret_cast<InternalPage *>(*node);
    auto inner_sibling_node = reinterpret_cast<InternalPage *>(*sibling_node);
    inner_sibling_node->MoveAllTo(inner_node, buffer_pool_manager_, mid_key);
  }

  auto parent_node =
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage((*node)->GetParentPageId())->GetData());
  transaction->AddIntoDeletedPageSet((*sibling_node)->GetPageId());
  RemoveEntry(parent_node, mid_key, transaction);
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(BPlusTreePage *node, BPlusTreePage *sibling_node, bool is_right,
                                  const KeyType &mid_key, Transaction *transaction) {
  KeyType up_key;
  // sibling_node 在右边的情况,因为左右的操作很不一致,所以不交换然后做一样的操作了
  if (is_right) {
    if (!node->IsLeafPage()) {
      auto inner_node = reinterpret_cast<InternalPage *>(node);
      auto inner_sibling_node = reinterpret_cast<InternalPage *>(sibling_node);
      inner_sibling_node->MoveFirstToLast(inner_node, buffer_pool_manager_, mid_key);
      up_key = inner_sibling_node->KeyAt(0);
    } else {
      auto leaf_node = reinterpret_cast<LeafPage *>(node);
      auto leaf_sibling_node = reinterpret_cast<LeafPage *>(sibling_node);
      leaf_sibling_node->MoveFirstToLast(leaf_node);
      up_key = leaf_sibling_node->KeyAt(0);
    }
    auto parent_node =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
    int index = parent_node->KeyIndex(mid_key, comparator_);
    parent_node->SetKeyAt(index, up_key);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    return;
  }
  // sibling_node在左边的情况
  if (!node->IsLeafPage()) {
    auto inner_node = reinterpret_cast<InternalPage *>(node);
    auto inner_sibling_node = reinterpret_cast<InternalPage *>(sibling_node);
    inner_sibling_node->MoveLastToFirst(inner_node, buffer_pool_manager_);
    // 原本的第一个key就是无效的,现在整体往右移动了一个,肯定要把这个key 重新赋值
    inner_node->SetKeyAt(1, mid_key);
    // 妙
    up_key = inner_node->KeyAt(0);
  } else {
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    auto leaf_sibling_node = reinterpret_cast<LeafPage *>(sibling_node);
    leaf_sibling_node->MoveLastToFirst(leaf_node);
    up_key = leaf_node->KeyAt(0);
  }
  auto parent_node =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  int index = parent_node->KeyIndex(mid_key, comparator_);
  parent_node->SetKeyAt(index, up_key);
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  KeyType key;
  root_latch_.RLock();
  auto page = FindLeaf(key, Operation::SEARCH, nullptr, true, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_latch_.RLock();
  auto page = FindLeaf(key, Operation::SEARCH, nullptr);
  auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  assert(root_page_id_ != INVALID_PAGE_ID);
  root_latch_.RLock();
  KeyType key;
  auto page = FindLeaf(key, Operation::SEARCH, nullptr, false, true);
  auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, leaf_node->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
// ?这里竟然忘记改了
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Operation operation, Transaction *transaction, bool leftMost,
                              bool rightMost) -> Page * {
  assert(operation == Operation::SEARCH ? !(leftMost && rightMost) : transaction != nullptr);
  assert(root_page_id_ != INVALID_PAGE_ID);
  auto cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

  // 对当前节点加锁
  if (operation == Operation::SEARCH) {
    root_latch_.RUnlock();
    cur_page->RLatch();
  } else {
    cur_page->WLatch();
    if (IsPageSafe(cur_node, operation)) {
      ReleaseLatchFromQueue(operation, transaction);
    }
    transaction->AddIntoPageSet(cur_page);
  }
  page_id_t next_page_id;
  Page *next_page;
  BPlusTreePage *next_node;
  while (!cur_node->IsLeafPage()) {
    auto *cur_inner_node = reinterpret_cast<InternalPage *>(cur_node);
    if (leftMost) {
      next_page_id = cur_inner_node->ValueAt(0);
    } else if (rightMost) {
      next_page_id = cur_inner_node->ValueAt(cur_inner_node->GetSize() - 1);
    } else {
      next_page_id = cur_inner_node->Find(key, comparator_);
    }
    next_page = buffer_pool_manager_->FetchPage(next_page_id);
    next_node = reinterpret_cast<BPlusTreePage *>(next_page->GetData());

    if (operation == Operation::SEARCH) {
      next_page->RLatch();
      cur_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
    } else {
      next_page->WLatch();
      if (IsPageSafe(next_node, operation)) {
        ReleaseLatchFromQueue(operation, transaction);
      }
      transaction->AddIntoPageSet(next_page);
    }
    cur_page = next_page;
    cur_node = next_node;
  }
  return cur_page;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
// 仅仅删除和插入需要判断是不是安全的节点
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsPageSafe(BPlusTreePage *node, Operation operation) -> bool {
  if (operation == Operation::SEARCH) {
    return true;
  }
  if (node->IsLeafPage()) {
    if (operation == Operation::DELETE) {
      if (node->IsRootPage()) {
        return node->GetSize() > 1;
      }
      return node->GetSize() > node->GetMinSize();
    }
    return node->GetSize() < node->GetMaxSize() - 1;
  }
  if (operation == Operation::DELETE) {
    if (node->IsRootPage()) {
      return node->GetSize() > 1;
    }
    return node->GetSize() > node->GetMinSize();
  }
  return node->GetSize() < node->GetMaxSize();
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchFromQueue(Operation operation, Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  while (!transaction->GetPageSet()->empty()) {
    Page *page = transaction->GetPageSet()->back();
    transaction->GetPageSet()->pop_back();
    if (page == nullptr) {
      if (operation == Operation::SEARCH) {
        root_latch_.RUnlock();
      } else {
        root_latch_.WUnlock();
      }
    } else {
      if (operation == Operation::SEARCH) {
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      } else {
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteAllPage(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  auto set = transaction->GetDeletedPageSet();
  for (auto page_id : *set) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  set->clear();
}
/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
