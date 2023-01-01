/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager* bpman){
    // curpage_=curpage;
    // pos_=pos;
    bpman_=bpman;
    this->concurr_=std::unique_ptr<BPlusTreeConcurrentControl>(
        new BPlusTreeConcurrentControl(BPlusTreeConcurrentControlMode::Lookup,bpman_));
}
INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::init(int pos,Page* curpage){
    pos_=pos;
    this->curpage_=curpage;
}
// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    if(curpage_){
        //结束对page的持有，应该unpin
        bpman_->UnpinPage(curpage_->GetPageId(),false);
    }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() const{ 
    if(!curpage_){
        return true;
    }
    LeafPage* lp=PAGE_REF_LEAF(curpage_);
    return pos_==lp->GetMaxSize();
    
    }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { 
    if(isEnd()){
        throw Exception(ExceptionType::OUT_OF_RANGE,"IndexIterator is at end, cant deref") ;
    }
    LeafPage*lp=PAGE_REF_LEAF(curpage_);
    return lp->GetItem(pos_);
    // throw std::runtime_error("unimplemented"); 
    }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() { 
    printf("plus\n");
    if(isEnd()){
        throw Exception(ExceptionType::OUT_OF_RANGE,"IndexIterator is at end, cant plus");
    }
    pos_++;
     LeafPage*lp=PAGE_REF_LEAF(curpage_);
    if(pos_==lp->GetSize()){
        page_id_t nextpid= lp->GetNextPageId();
        curpage_= bpman_->FetchPage(nextpid);
        pos_=0;
    }
printf("plus\n");
    return *this;
    // throw std::runtime_error("unimplemented");
     }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
