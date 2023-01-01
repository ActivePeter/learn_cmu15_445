//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {


class BPlusTreeConcurrentControl;

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>
//用于遍历b+树
// 注：会持有page，销毁时应该unpin
INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager* bpman);
  // IndexIterator();
  ~IndexIterator();
  
  IndexIterator(IndexIterator &&) = default;
  // IndexIterator& operator=(IndexIterator &&) = default;

  IndexIterator(IndexIterator const&) = delete;
  IndexIterator& operator=(IndexIterator const&) = delete;

  void init(int pos,Page* curpage);
  bool isEnd() const;

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    //  printf("index ==\n");
     if(isEnd()&&itr.isEnd()){
      // printf("index == 1\n");
      return true;
     } 
    if((isEnd()&&!itr.isEnd())||
      (!isEnd()&&itr.isEnd())){
        return false;
      }
      if(!curpage_){
        throw Exception(ExceptionType::INVALID,"curpage not exist");
      }
    //  printf("iter == %d %d\n");
    std::cout<<(curpage_==itr.curpage_&&pos_==itr.pos_)<<std::endl;
     return curpage_==itr.curpage_&&pos_==itr.pos_;
    //  throw std::runtime_error("unimplemented"); 
     }

  bool operator!=(const IndexIterator &itr) const { 
    // printf("index !=\n");
    return !(*this==itr);
    // throw std::runtime_error("unimplemented"); 
    }
  BPlusTreeConcurrentControl& concurr(){
    return *concurr_.get();
  }
 private:
  // add your own private member variables here
  Page* curpage_;
  int pos_;
  BufferPoolManager *bpman_;
  std::unique_ptr<BPlusTreeConcurrentControl> concurr_;
};

}  // namespace bustub
