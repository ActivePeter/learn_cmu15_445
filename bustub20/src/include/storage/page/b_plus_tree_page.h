//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

#define PAGE_REF_INTERNEL(_mem_page_) (InternalPage*)_mem_page_->GetData()
#define PAGE_REF_LEAF(_mem_page_) (LeafPage*)_mem_page_->GetData()

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */
class BPlusTreePage {
 public:
  // BPlusTreePage(){}
  // virtual ~BPlusTreePage(){}
  bool IsLeafPage() const;
  bool IsRootPage() const;
  void SetPageType(IndexPageType page_type);
  IndexPageType GetPageType();

  int GetSize() const;
  void SetSize(int size);
  void IncreaseSize(int amount);

  int GetMaxSize() const;
  void SetMaxSize(int max_size);
  int GetMinSize() const;

  page_id_t GetParentPageId() const;
  void SetParentPageId(page_id_t parent_page_id);

  page_id_t GetPageId() const;
  void SetPageId(page_id_t page_id);

  void SetLSN(lsn_t lsn = INVALID_LSN);

  void RLock(){
    latch_.lock();
    // latch_.RLock();
  }
  void RUnlock(){
    latch_.unlock();
    // latch_.RUnlock();
  }
  void WLock(){
    printf("lock %d \n",page_id_);
    std::mutex test;
    test.lock();
    printf("locked1\n");
    // latch_.WLock();
    latch_.lock();

    printf("locked2\n");
  }
  void WUnlock(){
    // latch_.WUnlock();
    latch_.unlock();
  }
  //到达需要分裂的size，中间节点和子节点不一样
  // virtual bool ReachSplitSize()=0;
  // virtual int SplitSize()=0;
 private:
  // member variable, attributes that both internal and leaf page share
  IndexPageType page_type_ __attribute__((__unused__));
  lsn_t lsn_ __attribute__((__unused__));
  int size_ __attribute__((__unused__));
  int max_size_ __attribute__((__unused__));
  page_id_t parent_page_id_ __attribute__((__unused__));
  page_id_t page_id_ __attribute__((__unused__));
  // ReaderWriterLatch latch_;
  std::mutex latch_;
};
struct PaUtil{
  void binary_search(){

  }
};

}  // namespace bustub
