//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/logger.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(
  page_id_t page_id, page_id_t parent_id, int max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
    
    // seq.reserve(max_size);
  }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BeginWithTwoNode(
  page_id_t a, const KeyType &key,page_id_t b) {
    SetSize(2);
    array[0].second=a;
    array[1]={key,b};
    printf("internel begin with two node %d %d\n",a,b);
    for (int i = 0; i < this->GetSize(); i++) {
      std::cout << this->KeyAt(i) << ": " << this->ValueAt(i) << ",";
    }
    std::cout<<std::endl;
    // seq.reserve(max_size);
  }
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  // assert(index!=0);//0为无效index
  // assert(index<(int)seq.size());//超出范围无效
  auto size=GetSize();
    if(index>=size){
      throw Exception(ExceptionType::OUT_OF_RANGE,"internel KeyAt");
    }

  
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(
  int index, const KeyType &key) {
    auto size=GetSize();
    if(index>=size){
      throw Exception(ExceptionType::OUT_OF_RANGE,"SetKeyAt");
    }
    array[index].first=key;
  }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(
  const ValueType &value) const { 
    //not found
    return -1; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(
  int index) const {
    if(index>GetSize()){
      throw Exception(ExceptionType::OUT_OF_RANGE,
      "internel ValueAt out of range");
    }
    return array[index].second;
     }



/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(
  const KeyType &key, const KeyComparator &comparator) const {
  auto sz=GetSize();
  for(int i=sz-1;i>=1;i--){
    //第一个>=key的元素，
    if(comparator(array[i].first,key)<=0){
      return array[i].second;
    }
  }
  return array[0].second;
  // throw Exception(ExceptionType::INVALID,"a child page of internel must be found,unless it is not inited");
  // return INVALID_PAGE_ID;
}

//return the last entry key <=target key
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookupKeyIndex(
  const KeyType &key, const KeyComparator &comparator) const {
  auto sz=GetSize();
  for(int i=sz-1;i>=1;i--){
    //第一个>=key的元素，
    if(comparator(array[i].first,key)<=0){
      return i;
    }
  }
  //0 指向第一个entry，是dummynode
  return 0;
  // throw Exception(ExceptionType::INVALID,"a child page of internel must be found,unless it is not inited");
  // return INVALID_PAGE_ID;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * 
 * NOTE!!!: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(//populate 填充
  const ValueType &old_value, const KeyType &new_key,
  const ValueType &new_value) {
    //写到了BeginWithTwoNode里
  }
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  for(int i=0;i<GetSize();i++){
    if(array[i].second==old_value){
      pa::array_insert(array,GetSize(),i+1,{new_key,new_value});
      IncreaseSize(1);
      return GetSize();
    }
  }
  throw Exception(ExceptionType::INVALID,"internel insert fail");
  // return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
  BPlusTreeInternalPage *recipient,
  BufferPoolManager *buffer_pool_manager) {
  if(GetSize()!=GetMaxSize()+2){//第一个node不算数，dummy node
    throw Exception(ExceptionType::INVALID,"internel move half when not full");
  }
  auto leftsz=GetSize()-GetSize()/2;
  recipient->CopyNFrom(array+leftsz,GetSize()/2,buffer_pool_manager);
  SetSize( leftsz);
}
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitSize(){
  return GetMaxSize()+2;
}
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_INTERNAL_PAGE_TYPE::ReachSplitSize(){
  if(this->GetSize()==SplitSize()){
    return true;
  }else if(this->GetSize()>SplitSize()){
    printf("should split bf\n");
    LOG_ERROR("should split bf");
    abort();
  }
  return false;
}
/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(
  MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    //剩余空间
  if(GetMaxSize()-GetSize()<size){
    printf("max size:%d,size:%d,need cpyin:%d",GetMaxSize(),GetSize(),size);
    throw Exception(ExceptionType::OUT_OF_RANGE,"CopyNFrom not enough space in page");
  }
  memcpy((void*)(array+GetSize()),(void*)items,size*sizeof(MappingType));
  SetSize(GetSize()+size);
  //此处要更新子page的parent指针
  for(int i=0;i<size;i++){
    Page* p=buffer_pool_manager->FetchPage( items[i].second);
    if(!p){
      throw Exception(ExceptionType::INVALID,"internel copy n from fetch page failed");
    }
    BPlusTreePage* bppage=(BPlusTreePage*)p->GetData();
    bppage->SetParentPageId(GetPageId());
    // printf("change page%d parent from %d to %d",);

    buffer_pool_manager->UnpinPage(items[i].second,true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  pa::array_move_forward(array,index,GetSize()-index-1);
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() { return INVALID_PAGE_ID; }
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  array[0].first=middle_key;
  recipient->CopyNFrom(array,GetSize(),buffer_pool_manager);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
  BPlusTreeInternalPage *recipient, const KeyType &middle_key,
  BufferPoolManager *buffer_pool_manager) {
    auto take=pa::array_move_forward(array,0,GetSize()-1);
    take.first=middle_key;
    recipient->CopyLastFrom(take,buffer_pool_manager);
    IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // std::cout<<"copy last "<<pair.first<<","<<pair.second<<std::endl;
  array[GetSize()]=pair;
  IncreaseSize(1);
  std::cout<<std::endl;
  Page* page_mem=buffer_pool_manager->FetchPage(pair.second);
  BPlusTreePage* page=(BPlusTreePage*) page_mem->GetData();
  page->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(pair.second,true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
  BPlusTreeInternalPage *recipient, const KeyType &middle_key,
  BufferPoolManager *buffer_pool_manager) {
    recipient->CopyFirstFrom(array[GetSize()-1],buffer_pool_manager);
    IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  pa::array_move_back(array,0,GetSize());
  array[0]=pair;
  IncreaseSize(1);

  Page* page_mem=buffer_pool_manager->FetchPage(pair.second);
  BPlusTreePage* page=(BPlusTreePage*) page_mem->GetData();
  page->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(pair.second,true);
  // page->
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
