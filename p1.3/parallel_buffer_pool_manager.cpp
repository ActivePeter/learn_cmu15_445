//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,

                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  bufferpool_mans.reserve(num_instances);
  for(size_t i=0;i<num_instances;i++){
    bufferpool_mans.emplace_back(new BufferPoolManagerInstance(pool_size,disk_manager,log_manager));
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for(auto v:bufferpool_mans){
    delete v;
  }
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return bufferpool_mans[0]->GetPoolSize();
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  
  return bufferpool_mans[pageid_2_bufferpoolman[page_id]];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  
  return GetBufferPoolManager(page_id)->FetchPage(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->UnpinPage(page_id,is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  for(size_t i=0;i<bufferpool_mans.size();i++){
    auto index=(newpgimp_scanbegin+i)%bufferpool_mans.size();
    auto &bpmi=bufferpool_mans[index];
    auto newpg=bpmi->NewPage(page_id);
    if(newpg){
      newpgimp_scanbegin++;newpgimp_scanbegin%=bufferpool_mans.size();
      return newpg;
    }
  }
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  newpgimp_scanbegin++;newpgimp_scanbegin%=bufferpool_mans.size();
  return nullptr;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  if(GetBufferPoolManager(page_id)->DeletePage(page_id)){
    pageid_2_bufferpoolman.erase(page_id);
    return true;
  }
  return false;
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for(auto pair :pageid_2_bufferpoolman){
    bufferpool_mans[pair.second]->FlushAllPages();
  }
}

}  // namespace bustub
