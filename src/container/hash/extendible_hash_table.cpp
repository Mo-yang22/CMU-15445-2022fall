//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back((NewBucket(0)));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::NewBucket(int local_depth) -> std::shared_ptr<Bucket> {
  return std::make_shared<Bucket>(bucket_size_, local_depth);
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  // 第一步,分裂,并把dir中的对应关系处理好
  std::shared_ptr<Bucket> new_bucket = NewBucket(bucket->GetDepth());
  bool first = false;
  int mask = (1 << bucket->GetDepth()) - 1;
  // 将第一个碰到的index设为第一组,其他的为第二组,划分组的依据是低LocalDepth位相同
  size_t first_index;
  for (size_t i = 0; i < dir_.size(); ++i) {
    if (!first && dir_[i] == bucket) {
      first = true;
      first_index = i;
    } else if (first && dir_[i] == bucket) {
      if ((first_index & mask) != (i & mask)) {
        dir_[i] = new_bucket;
      }
    }
  }
  // 第二步,将bucket的内容重新分配
  std::list<std::pair<K, V>> list = bucket->GetItems();
  for (const auto &i : list) {
    if (dir_[IndexOf(i.first)] == bucket) {
      continue;
    }
    std::pair<K, V> tmp = i;
    bucket->Remove(i.first);
    InsertHelper(tmp.first, tmp.second);
  }
  num_buckets_++;
}
template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertHelper(const K &key, const V &value) {
  size_t index = IndexOf(key);
  std::shared_ptr<Bucket> bucket_ptr = dir_[index];
  // 插入失败就有后面发生的故事
  if (!bucket_ptr->Insert(key, value)) {
    size_t tmp_size = dir_.size();
    // Global Depth等于Local Depth
    if (GetLocalDepthInternal(index) == GetGlobalDepthInternal()) {
      // Global加一
      global_depth_++;
      // dir_扩张
      dir_.resize(tmp_size * 2);
      // 分配好新增的目录项的内容
      for (size_t t = tmp_size; t < tmp_size * 2; ++t) {
        dir_[t] = dir_[t - tmp_size];
      }
    }
    // Local Depth加一
    bucket_ptr->IncrementDepth();

    RedistributeBucket(bucket_ptr);
    // 重新插入
    InsertHelper(key, value);
  }
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  InsertHelper(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto i = list_.begin(); i != list_.end(); ++i) {
    if ((*i).first == key) {
      value = (*i).second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto i = list_.begin(); i != list_.end(); ++i) {
    if ((*i).first == key) {
      list_.erase(i);
      return true;
    }
  }
  return false;
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // 首先寻找是不是存在key
  for (auto i = list_.begin(); i != list_.end(); i++) {
    if ((*i).first == key) {
      (*i).second = value;
      return true;
    }
  }
  // 是不是满了
  if (IsFull()) {
    return false;
  }
  // 正常插入
  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
