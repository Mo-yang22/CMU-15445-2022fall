//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!NewFrame(&frame_id)) {
    page_id = nullptr;
    return nullptr;
  }
  // 为新页重置内存和元数据
  page_id_t new_page_id = AllocatePage();

  ResetPage(frame_id, new_page_id);

  page_table_->Insert(new_page_id, frame_id);

  replacer_->SetEvictable(frame_id, false);
  // 应该要在SetEvictable函数后把pin_count加1
  pages_[frame_id].pin_count_++;
  replacer_->RecordAccess(frame_id);
  *page_id = new_page_id;
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // 假如成功找到,frame_id会返回正确的值
  bool is_find = page_table_->Find(page_id, frame_id);
  // 当需要从磁盘中提取page,但是此时又没有可以被驱逐的frame,就g了
  if (!is_find) {
    if (!NewFrame(&frame_id)) {
      return nullptr;
    }
    ResetPage(frame_id, page_id);
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

    page_table_->Insert(page_id, frame_id);
  }

  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;
  replacer_->RecordAccess(frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // 如果page_id不再缓存池中,或者这个page的pin_count_已经小于等于0了
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  // 减少frame的pin数
  pages_[frame_id].pin_count_--;
  // 如果减完为0的话,将这个frame设置为可以Evict
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  // 设置这个frame中含有的page的dirty位
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  // for (size_t i = 0; i <pool_size_; ++i) {
  //   std::cout<<pages_[i].pin_count_<<' ';
  // }
  // std::cout<<std::endl;
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPageId() == INVALID_PAGE_ID) {
      continue;
    }
    FlushPgImp(pages_[i].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // 在页表中找不到,直接返回true
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  // page的pin_count_不为0,无法删除,返回false
  if (pages_[frame_id].GetPinCount() != 0) {
    return false;
  }
  // 在页表中删除
  page_table_->Remove(page_id);

  // 在replace_移除对frame_id的跟踪
  replacer_->Remove(frame_id);
  // 在free_list_中插入
  free_list_.emplace_back(frame_id);
  // 重置page,并将page的page_id设置为INVALD
  ResetPage(frame_id, INVALID_PAGE_ID);
  // 调用
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

void BufferPoolManagerInstance::ResetPage(frame_id_t frame_id, page_id_t page_id) {
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
}
auto BufferPoolManagerInstance::NewFrame(frame_id_t *frame_id_ptr) -> bool {
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.erase(free_list_.begin());
  } else {
    //
    if (!replacer_->Evict(&frame_id)) {
      frame_id_ptr = nullptr;
      return false;
    }
    // ?卧槽,在驱逐页的时候忘记在页表中删除了
    // 无敌,就是这个错误
    page_table_->Remove(pages_[frame_id].GetPageId());
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
  }
  *frame_id_ptr = frame_id;
  return true;
}
}  // namespace bustub
