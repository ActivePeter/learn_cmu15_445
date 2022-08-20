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

#include "common/macros.h"

namespace bustub {
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}
auto BufferPoolManagerInstance::GetPageByPageId(page_id_t pageid) 
    -> GetPageByPageIdRet{
  auto find = page_table_.find(pageid);
  if (find != page_table_.end()) {
    return {find->second,&pages_[find->second]};
  }
  return {-1,nullptr};
}
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  auto res = GetPageByPageId(page_id);
  auto p=res.page;
  if (p) {
    disk_manager_->WritePage(page_id, p->GetData());
  } else {
    // printf("page not found!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
    // exit(0);
    return false;
  }
  // Make sure you call DiskManager::WritePage!
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (auto pair : page_table_) {
    if (pages_[pair.second].IsDirty()) {
      FlushPgImp(pair.first);
    }
  }
}

auto BufferPoolManagerInstance::PickPageFromFreeListOrReplacer() -> frame_id_t {
  frame_id_t fid = -1;
  if (free_list_.size() != 0) {
    fid = free_list_.front();
    free_list_.pop_front();
    //获取page后需要pin这个page
    pages_[fid].PinCountUp();
  } else if (replacer_->Victim(&fid)) {
    
    pages_[fid].PinCountUp();
  }

  return fid;
}
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  auto page_id_ = AllocatePage();
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool allpin = true;
  for (size_t i = 0; i < pool_size_; ++i) {
    if(pages_[i].GetPinCount()==0){
      allpin=false;
    }
  }
  if (allpin) {
    // printf("page all pin\n");
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer.
  //        Always pick from the free list first.
  frame_id_t fid = PickPageFromFreeListOrReplacer();
  if (fid < 0) {
    // printf("no page left in freelist or replacer\n");
    return nullptr;
  }
  Page *p=&pages_[fid];
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  p->page_id_ = page_id_;
  page_table_[page_id_] = fid;
  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = page_id_;

  return p;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  auto find = page_table_.find(page_id);
  // 1.1    If P exists, pin it and return it immediately.
  if (find != page_table_.end()) {
    auto p = &pages_[find->second];
    p->PinCountUp();
    replacer_->Pin(find->second);
    return p;
  } else {
    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    //        Note that pages are always found from the free list first.
    auto fid = PickPageFromFreeListOrReplacer();
    if (fid < 0) {
      // printf("should have a useable page");
      return nullptr;
    }
    // 2.     If R is dirty, write it back to the disk.
    auto p = &pages_[fid];
    if (p->IsDirty()) {
      disk_manager_->WritePage(p->GetPageId(), p->GetData());
    }
    // 3.     Delete R from the page table and insert P.
    page_table_.erase(p->GetPageId());
    page_table_[page_id]=fid;
    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    auto addr=p->NewDiskPageBind(page_id);
    disk_manager_->ReadPage(page_id,addr);

    return p;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  auto res=GetPageByPageId(page_id);
  auto find=res.page;
  auto fid=res.fid;
  // 1.   If P does not exist, return true.
  if(find==nullptr){
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  else if(find->GetPinCount()>0){
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  else{
    page_table_.erase(page_id);
    free_list_.emplace_back(fid);
    return true;
  }
}

auto BufferPoolManagerInstance::UnpinPgImp(
  page_id_t page_id, bool is_dirty) -> bool {
    auto res=GetPageByPageId(page_id);
    if(res.page){
      res.page->PinCountDown();
      if(is_dirty){
        FlushPage(page_id);
      }
      if(res.page->GetPinCount()==0){

        replacer_->Unpin(page_table_[page_id]);
        page_table_.erase(page_id);//
        return true;
      }
    }
   return false; }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
