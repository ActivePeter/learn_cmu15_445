//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

frame_id_t BufferPoolManager::_get_frame() {
  if (free_list_.size() > 0) {
    auto ret = free_list_.front();
    free_list_.pop_front();
    return ret;
  }
  auto fid = -1;
  if (replacer_->Victim(&fid)) {
    //获取到了过期page，若page为脏，需要
    // 2.     If R is dirty, write it back to the disk.
    // 3.     Delete R from the page table
    auto &p = pages_[fid];
    if (p.is_dirty_) {
      disk_manager_->WritePage(p.page_id_, p.data_);
      p.is_dirty_=false;
    }
    page_table_.erase(p.page_id_);
    return fid;
  }
  return -1;
}

void BufferPoolManager::_disk_load_page_data_2_frame(
  page_id_t pid,frame_id_t fid
){
  //数据读入
  disk_manager_->ReadPage(pid,pages_[fid].data_);
  //pageid
  pages_[fid].page_id_=pid;
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  //pagetable更新
  page_table_[pid]=fid;
  //引用计数
  pages_[fid].pin_count_=1;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  //获取指定pageid 的page
  std::lock_guard<std::mutex> _g(latch_);

  // 1.     Search the page table for the requested page (P).
  auto f = page_table_.find(page_id);
  // 1.1    If P exists, pin it and return it immediately.
  if (f != page_table_.end()) {
    pages_[f->second].pin_count_++;
    replacer_->Pin(f->second);
    return &pages_[f->second];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table
  auto fid = _get_frame();
  if (fid < 0) return nullptr;

  _disk_load_page_data_2_frame(page_id,fid);
  // // and insert P.
  // page_table_[page_id] = fid;
  // // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  // disk_manager_->ReadPage(page_id, pages_[fid].data_);
  // pages_[fid].pin_count_ = 1;
  
  //由于是从freelist或者replacer取出的，所以不用pin replacer
  return &pages_[fid];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> _g(latch_);
  // pin计数为0时，放入replacer
  auto fid = page_table_[page_id];
  if (pages_[fid].pin_count_ == 0) {
    return false;
  }
  pages_[fid].is_dirty_ =pages_[fid].is_dirty_|| is_dirty;
  pages_[fid].pin_count_--;
  if (pages_[fid].pin_count_ == 0) {
    replacer_->Unpin(fid);
  }
  // printf("unpin page %d, left refcnt %d\n",
  //   page_id,pages_[fid].pin_count_);
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> _g(latch_);

  auto f = page_table_.find(page_id);
  if (f == page_table_.end()) {
    return false;
  }
  // Make sure you call DiskManager::WritePage!
  if (pages_[f->second].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[f->second].data_);
    pages_[f->second].is_dirty_=false;
  }
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  std::lock_guard<std::mutex> _g(latch_);
  //磁盘上创建新的page

  auto fid = _get_frame();
  if (fid < 0) {
    // 1.   If all the pages in the buffer pool are pinned, return nullptr.?
    //当前内存中的page全都被持有，所以没法给新的page绑定，所以返回null
    printf("NewPageImpl no mem page\n");
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. 
  //      Always pick from the free list first.
  // 0.   Make sure you call DiskManager::AllocatePage!
  auto pid=disk_manager_->AllocatePage();
  // _disk_load_page_data_2_frame(pid,fid);
  {
    //pageid
    pages_[fid].page_id_=pid;
    // 3.   Update P's metadata, zero out memory and add P to the page table.
    //pagetable更新
    page_table_[pid]=fid;
    //引用计数
    pages_[fid].pin_count_=1;
  }
  *page_id=pid;
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return &pages_[fid];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> _g(latch_);

  // 1.   Search the page table for the requested page (P).
  auto f=page_table_.find(page_id);
  // 1.   If P does not exist, return true.
  if(f==page_table_.end()){
    return true;
  }
  auto &page=pages_[f->second];

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if(page.pin_count_>0){
    printf("delete failed, page pin cnt %d\n",page.pin_count_);
    return false;
  }
  
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  free_list_.push_back(f->second);
  // 0.   Make sure you call DiskManager::DeallocatePage!
  disk_manager_->DeallocatePage(page_id);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::lock_guard<std::mutex> _g(latch_);
  for(auto &p:page_table_){
    if(pages_[p.second].is_dirty_){
      pages_[p.second].is_dirty_=false;
      auto &page=pages_[p.second];
      disk_manager_->WritePage(page.page_id_,page.data_);
    }
  }
  // You can do it!
}

}  // namespace bustub
