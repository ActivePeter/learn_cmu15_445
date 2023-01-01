//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

struct FindSibResult{
  bool sib_on_right;
  int sib_index;
};

enum class BPlusTreeConcurrentControlMode {
  Insert,
  Delete,
  Lookup
};

class BPlusTreeConcurrentControl{
public:
  void lock_one(BPlusTreePage*);
  // void wlock_one(BPlusTreePage*);
  void if_safe_then_free_pre();

  BPlusTreeConcurrentControl(BPlusTreeConcurrentControl const&) = delete;
  BPlusTreeConcurrentControl& operator=(BPlusTreeConcurrentControl const&) = delete;
  BPlusTreeConcurrentControl(BPlusTreeConcurrentControl&&) = default;
  BPlusTreeConcurrentControl(
    BPlusTreeConcurrentControlMode mode,
    BufferPoolManager*bpman_ref){
    mode_=mode;
    bpman_ref_=bpman_ref;
  }
  BPlusTreeConcurrentControl()=default;
  ~BPlusTreeConcurrentControl(){
    for(auto &v:locked_pages_){
      unlock_and_unpin_page(v);
    }
  }
  void init(BPlusTreeConcurrentControlMode mode,
    BufferPoolManager*bpman_ref){
    mode_=mode;
    bpman_ref_=bpman_ref;
  }
private:
  void free_pre();
  void unlock_and_unpin_page(BPlusTreePage*page){
    if(read_mode()){
      page->RUnlock();
    }else{
      page->WUnlock();
    }
  }
  bool read_mode(){
    if(mode_==BPlusTreeConcurrentControlMode::Lookup){
      return true;
    }
    return false;
  }
  BPlusTreeConcurrentControlMode mode_;
   BufferPoolManager*bpman_ref_;
  std::list<BPlusTreePage*> locked_pages_;//is read, page
};

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
 public:
  using ParentPage = BPlusTreePage;
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);
  INDEXITERATOR_TYPE end();

  void Print(BufferPoolManager *bpm) {
    ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
  }

  void Draw(BufferPoolManager *bpm, const std::string &outf) {
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
    out << "}" << std::endl;
    out.close();
  }

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);
  // expose for test purpose
  Page *FindLeafPage(const KeyType &key,BPlusTreeConcurrentControl*conccur ,bool leftMost = false);
  FindSibResult FindSibInInternel(int this_index,InternalPage* ip);
 
 private:
  bool StartNewTree(const KeyType &key, const ValueType &value);
  
  
  Page* _NewInternalPage(page_id_t parent_id);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value, BPlusTreeConcurrentControl&conccur,Transaction *transaction = nullptr);

  void InsertIntoParent(
    BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
    BPlusTreeConcurrentControl& concurr,Transaction *transaction = nullptr);

  template <typename N>
  N *Split(N *node);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  bool CoalesceEdgeHandle(InternalPage** parent,
    KeyType old_parent_first_key,
    Transaction *transaction);
  bool CoalesceCheckParentRootExpire(
    BPlusTreePage* one_childpage,
    InternalPage* parent);
  // template <typename N>
  bool Coalesce(LeafPage **neighbor_node, LeafPage **node,
     BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                int index,bool sib_on_right,
                 Transaction *transaction = nullptr
                );
  bool Coalesce(InternalPage **neighbor_node, InternalPage **node,
     BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                int index,bool sib_on_right,
                 Transaction *transaction = nullptr
                );

  // template <typename N>
  void Redistribute(LeafPage *neighbor_node, LeafPage *node, int index);
  void Redistribute(InternalPage *neighbor_node, InternalPage *node, int index,KeyType middle_key);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // void InitPageFromType(Page*page,IndexPageType type);

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  std::mutex big_mu_;
  // IndexPageType root_page_type;
};

}  // namespace bustub
