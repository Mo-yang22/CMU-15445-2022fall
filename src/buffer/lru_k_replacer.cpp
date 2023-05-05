//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cmath>
#include <exception>
#include "common/config.h"
#include "type/limits.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), is_evictable_(num_frames, false) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto i = history_.rbegin(); i != history_.rend(); ++i) {
    if (!is_evictable_[(*i)]) {
      continue;
    }
    *frame_id = (*i);
    curr_size_--;
    // 将i指向的元素删了,i就不能用了呀,先保存下来
    frame_id_t tmp = (*i);
    history_.erase(history_map_[*i].second);
    history_map_.erase(tmp);
    // 初始化为false,防止在Remove时错误
    is_evictable_[tmp] = false;
    return true;
  }
  for (auto i = cache_.rbegin(); i != cache_.rend(); ++i) {
    if (!is_evictable_[(*i)]) {
      continue;
    }
    frame_id_t tmp = (*i);
    *frame_id = tmp;
    curr_size_--;
    history_map_.erase(tmp);
    cache_.erase(cache_map_[tmp]);
    cache_map_.erase(tmp);
    // 初始化为false
    is_evictable_[tmp] = false;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    std::throw_with_nested("invalid frame_id");
    BUSTUB_ASSERT("cuo", "wu");
  }
  // 不存在
  if (history_map_.find(frame_id) == history_map_.end()) {
    history_.push_front(frame_id);
    history_map_.insert({frame_id, {1, history_.begin()}});
    // curr_size_++;
    // is_evictable_[frame_id] = true;
  } else if (history_map_[frame_id].first < k_ - 1) {
    history_map_[frame_id].first++;
  } else if (history_map_[frame_id].first == k_ - 1) {
    history_map_[frame_id].first++;
    auto i = history_map_[frame_id].second;
    history_.erase(i);
    cache_.push_front(frame_id);
    cache_map_.insert({frame_id, cache_.begin()});
  } else if (history_map_[frame_id].first == k_) {
    auto pos = cache_map_[frame_id];
    cache_.erase(pos);
    cache_.push_front(frame_id);
    cache_map_[frame_id] = cache_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    std::throw_with_nested("invalid frame_id");
    BUSTUB_ASSERT("cuo", "wu");
  }
  // ? 要首先判断是不是已经被驱逐了
  if (history_map_.find(frame_id) == history_map_.end()) {
    return;
  }
  if (is_evictable_[frame_id] && !set_evictable) {
    is_evictable_[frame_id] = false;
    curr_size_--;
  }
  if (!is_evictable_[frame_id] && set_evictable) {
    is_evictable_[frame_id] = true;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_) ||
      (history_map_.find(frame_id) != history_map_.end() && !is_evictable_[frame_id])) {
    std::throw_with_nested("invalid frame_id");
    BUSTUB_ASSERT("cuo", "wu");
  }
  if (history_map_.find(frame_id) != history_map_.end() && history_map_[frame_id].first < k_) {
    curr_size_--;
    history_.erase(history_map_[frame_id].second);
    history_map_.erase((frame_id));
    is_evictable_[frame_id] = false;
  } else if (history_map_.find(frame_id) != history_map_.end() && history_map_[frame_id].first == k_) {
    curr_size_--;
    history_map_.erase(frame_id);
    cache_.erase(cache_map_[frame_id]);
    cache_map_.erase(frame_id);
    is_evictable_[frame_id] = false;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
