//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/logger.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  next_page_id_=INVALID_PAGE_ID;
  // seq.reserve(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { 
  return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_=next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
  const KeyType &key, const KeyComparator &comparator) const {
  for(int i=0;i<GetSize();i++){
    if(comparator(array[i].first,key)>=0){
      return i;
    }
  }
  //not found
  return -1; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  if(index>=GetSize()){
    throw Exception(ExceptionType::OUT_OF_RANGE,"KeyAt");
  }
  
  // KeyType key{};
  return array[index].first;
  // return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  if(index>=GetSize()){
    throw Exception(ExceptionType::OUT_OF_RANGE,"KeyAt");
  }
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(
  const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  auto size_=GetSize();
  auto max_size_=GetMaxSize();

  if(size_==max_size_+1){
    throw Exception(ExceptionType::INVALID,"should split when max size");
    return size_;
  }
  for(int i=0;i<size_;i++){
    //找到key<某一个元素的位置，插入
    if(comparator(key,array[i].first)<0){
      //从末尾开始往前移动
      for(int j=size_;j>i;j--){
        array[j]=array[j-1];
      }
      array[i]={key,value};
      size_++;
      SetSize(size_);
      printf("leaf final insert v:%ld\n",value.Get());
      // if(size_==max_size_){
      //   //do split
      // }
      return size_;
    }
  }
  //应该insert to back
  array[size_]={key,value};
  IncreaseSize(1);
  //这里page size 指kv对的数量
  return size_+1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  if(GetSize()!=GetMaxSize()+1){
    throw Exception(ExceptionType::INVALID,"move half when not full");
  }
  auto leftsz=GetSize()-GetSize()/2;
  recipient->CopyNFrom(array+leftsz,GetSize()/2);
  SetSize( leftsz);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  //剩余空间
  if(GetMaxSize()-GetSize()<size){
    throw Exception(ExceptionType::OUT_OF_RANGE,"CopyNFrom not enough space in page");
  }
  memcpy((void*)(array+GetSize()),(void*)items,size*sizeof(MappingType));
  SetSize(GetSize()+size);
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::SplitSize(){
  return GetMaxSize()+1;
}

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::ReachSplitSize(){
  if(this->GetSize()==SplitSize()){
    return true;
  }else if(this->GetSize()>SplitSize()){
    printf("should split bf\n");
    LOG_ERROR("should split bf");
    abort();
  }
  return false;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  //todo binary search
  for(int i=0;i<GetSize();i++){
    auto comp=comparator(key,array[i].first);
    if(comp==0){
      *value=array[i].second;
      return true;
    }else if(comp==-1){
      //key 比当前遍历节点小，后续的更大，所以不可能了
      return false;
    }
  }
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::LookupIndex(const KeyType &key, const KeyComparator &comparator) const {
  //todo binary search
  for(int i=0;i<GetSize();i++){
    auto comp=comparator(key,array[i].first);
    if(comp==0){
      // *value=array[i].second;
      return i;
    }else if(comp==-1){
      //key 比当前遍历节点小，后续的更大，所以不可能了
      return -1;
    }
  }
  return -1;
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
  const KeyType &key, const KeyComparator &comparator) {
  auto i=LookupIndex(key,comparator);
  if(i>-1){
    printf("delete %ld\n",pa::array_move_forward(array,i,GetSize()-i-1)
      .second.Get());
    
    IncreaseSize(-1);
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array,GetSize());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  if(GetSize()==0){
    throw Exception(ExceptionType::INVALID,"MoveFirstToEndOf");
  }
  MappingType take= pa::array_move_forward(array,0,GetSize()-1);
  IncreaseSize(-1);
  recipient->CopyLastFrom(take);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array[GetSize()]=item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyFirstFrom(array[GetSize()-1]) ;
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  pa::array_move_back(array,0,GetSize());
  array[0]=item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
