//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

void BPlusTreeConcurrentControl::lock_one(BPlusTreePage* page){
  printf("%ld page lock\n",(uint64_t)page);
  if(this->read_mode()){
    page->RLock();
  }else{
    page->WLock();
  }
  
  this->locked_pages_.push_back(page);
}

void BPlusTreeConcurrentControl::free_pre(){
  while(this->locked_pages_.size()>1){
    auto pre=locked_pages_.front();
    locked_pages_.pop_front();
    this->unlock_and_unpin_page(pre);
  }
}
void BPlusTreeConcurrentControl::if_safe_then_free_pre(){
  
  //如果前面的可以被释放就释放
  switch (mode_)
  {
  case BPlusTreeConcurrentControlMode::Insert:
    {
      //insert 模式，确保子节点不会split，那么就可以释放父节点
      //leaf和internel page split的size阈值不同
      // 小于阈值-1.那么+1后还没到阈值，不会触发split
      if(this->locked_pages_.back()->GetSize()
        <this->locked_pages_.back()->SplitSize()-1){
          free_pre();
      }
    }
    break;
  case BPlusTreeConcurrentControlMode::Delete:
    {
      //删除时
      // 子节点为叶子节点，删除一个值后，数量变少，可能触发与边上的合并与重分配，此时父节点不应该被释放
      // 子节点为中间节点，这个子节点后面的节点若触发了合并，那么这个节点的数量也会变化，
      //    若触发了调整，那么这个字节点不会变化
      // 


    }
    break;
  
  default:
    // LOG_ERROR("other concurr not impled");
    // abort();
    break;
  }
}
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return true; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(
  const KeyType &key, std::vector<ValueType> *result,
   Transaction *transaction) {
    if(result->size()!=0){
      throw Exception(ExceptionType::OUT_OF_RANGE,"GetValue return array size != 0");
    }
    if(this->root_page_id_==INVALID_PAGE_ID){
      throw Exception(ExceptionType::INVALID,"no root page");
    }
    auto curpageid=root_page_id_;
    while(1){
      // printf("GetValue loop");
      auto page_=buffer_pool_manager_->FetchPage(curpageid);
      auto page=(ParentPage*)(LeafPage*)page_->GetData();
      if(page->IsLeafPage()){
        LeafPage* lfp=(LeafPage*)page;
        ValueType ret;
        if(lfp->Lookup(key,&ret,comparator_)){
          result->push_back(ret);
          //bf ret
          buffer_pool_manager_->UnpinPage(curpageid,true);
          return true;
        }
        buffer_pool_manager_->UnpinPage(curpageid,true);
        return false;
      }else{
        InternalPage* ip=(InternalPage*)page;
        page_id_t v=ip->Lookup(key,comparator_);
        buffer_pool_manager_->UnpinPage(curpageid,false);
        if(v<0){
          //没找到
          return false;
        }
        curpageid=v;
      }
    }
    
    throw Exception(ExceptionType::INVALID,"cant get here");
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(
  const KeyType &key, const ValueType &value, Transaction *transaction) { 
    BPlusTreeConcurrentControl concurr(
      BPlusTreeConcurrentControlMode::Insert,this->buffer_pool_manager_);
      // std::lock_guard<std::mutex> hold(big_mu_);
      // big_mu_.lock();
    if(root_page_id_==INVALID_PAGE_ID){
      // printf("insert start new tree\n");
      //没有rootpage
      if(!StartNewTree(key,value)){
        //竞争 start 失败
        goto insert_into_leaf;
      }
      return true;
    }else{
insert_into_leaf:
      // printf("insert into leaf\n");
      return InsertIntoLeaf(key,value,concurr,transaction);
    }
    // big_mu_.unlock();
    return false; 
  }


// NewInternelPage
// remember to unpin after using
// @return new page
INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::_NewInternalPage(page_id_t parent_id){
  page_id_t pid;
  auto page=buffer_pool_manager_->NewPage(&pid);
  if(!page) throw Exception(
    ExceptionType::OUT_OF_MEMORY,"_NewInternalPage");
  
  InternalPage* p=PAGE_REF_INTERNEL(page);
  new (p) InternalPage();
  p->Init(pid,parent_id,internal_max_size_);
  
  return page;
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t pid;
  auto page=buffer_pool_manager_->NewPage(&pid);
  if(page){
    
    this->big_mu_.lock();
    //此处root page id的更新一定得是原子的
    //所以start失败就得执行另一种情况
    if(root_page_id_!=INVALID_PAGE_ID){
      big_mu_.unlock();
      return false;
    }
    root_page_id_=pid;//获取到pageid

    UpdateRootPageId(root_page_id_);
    new (page->GetData()) LeafPage();
    LeafPage* lfpagecast=(LeafPage*)page->GetData();
    lfpagecast->Init(pid,INVALID_PAGE_ID,leaf_max_size_);
    lfpagecast->WLock();
    lfpagecast->Insert(key,value,comparator_);
    lfpagecast->WUnlock();
    buffer_pool_manager_->UnpinPage(pid,true);

    big_mu_.unlock();
    return true;
    // root_page_type=IndexPageType::LEAF_PAGE;//初始为leafpage类型
    // InitPageFromType(page,root_page_type);
  }
  throw Exception(ExceptionType::OUT_OF_MEMORY,"StartNewTree");
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
   BPlusTreeConcurrentControl&conccur, Transaction *transaction) {
  //有rootpage,找插入位置，
  if(root_page_id_==INVALID_PAGE_ID){
    throw Exception(ExceptionType::INVALID,"InsertIntoLeaf");
  }
  Page* curpage=FindLeafPage(key,&conccur,false);
  LeafPage* lf=(LeafPage*)curpage->GetData();
  // auto newsize=
  lf->Insert(key,value,comparator_);
  // printf("%d\n",lf->GetSize());
  // printf("%d\n",lf->GetMaxSize()+2);
  // printf("%d\n",lf->SplitSize());
  // printf("after insert sz %d, max sz %d\n",newsize,lf->GetMaxSize());
  printf("lf sz %d %d\n",lf->GetSize(),lf->SplitSize());
  if(lf->GetSize()==lf->SplitSize()){
    //到达了maxsize，需要将leaf split
    LeafPage* newp=Split(lf);
    // conccur.lock_one(newp);
    //1.更新父节点
    {
      //这里的key为newp中最小值
      InsertIntoParent(lf,newp->GetItem(0).first,newp,conccur);
    }
    //2.新page，也需要unpin一下
    // buffer_pool_manager_->UnpinPage(newp->GetPageId(),true);
  }
  //路径上被搜索到的都被concurr锁了,concurr解锁时自动unpin
  // buffer_pool_manager_->UnpinPage(lf->GetPageId(),true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  BPlusTreePage*n=node;
  if(n->IsLeafPage()){
    if(n->GetSize()!=n->GetMaxSize()+1){
      throw Exception(ExceptionType::INVALID,"leaf split when not full");
    }
  }else{
    if(n->GetSize()!=n->GetMaxSize()+2){
      throw Exception(ExceptionType::INVALID,"internel split when not full");
    }
  }
  
  page_id_t pid;
  auto newp_=buffer_pool_manager_->NewPage(&pid);
  if(newp_){
    auto newp=(ParentPage*)(LeafPage*)(newp_->GetData());
    if(n->IsLeafPage()){
      // printf("split leaf\n");
      //leafpage copy
      LeafPage*old=(LeafPage*)(n);
      LeafPage* lfp=(LeafPage*)(newp);
      new ((char*)lfp) LeafPage();
      lfp->Init(pid,INVALID_PAGE_ID,old->GetMaxSize());
      //后半部分，复制到新page
      old->MoveHalfTo(lfp);
      auto oldnext=old->GetNextPageId();
      old->SetNextPageId(lfp->GetPageId());
      lfp->SetNextPageId(oldnext);
      printf("split sz %d\n",lfp->SplitSize());
      return reinterpret_cast<N*>(lfp);
    }else{
      // printf("split internel\n");
      InternalPage*old=reinterpret_cast<InternalPage*>(n);
      InternalPage*newp=PAGE_REF_INTERNEL(newp_);
      newp->Init(newp_->GetPageId(),n->GetParentPageId(),internal_max_size_);
      old->MoveHalfTo(newp,buffer_pool_manager_);
      
      return reinterpret_cast<N*>(newp);
      //internal node unhandled
      // throw Exception(ExceptionType::INVALID,"unhandled internal page spilit");
    }
  }else{
    throw Exception(ExceptionType::OUT_OF_MEMORY,"Split no mem");
  }
  return nullptr;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 * 
 * unpin the loaded parent when return
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(
  BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
  BPlusTreeConcurrentControl& concurr,Transaction *transaction) {
    InternalPage* ip;
    //1.1没有父节点，需要创建父节点
    if(old_node->GetParentPageId()==INVALID_PAGE_ID){
      // printf("new parent for 2 page\n");
      auto newip_mem=_NewInternalPage(INVALID_PAGE_ID);
      InternalPage* ip=PAGE_REF_INTERNEL(newip_mem);
      ip->BeginWithTwoNode(old_node->GetPageId(),key,new_node->GetPageId());
      // concurr.lock_one(ip);
      // buffer_pool_manager_->UnpinPage(ip->GetPageId(),true);
      if(old_node->GetPageId()==root_page_id_){
        root_page_id_=newip_mem->GetPageId();
        UpdateRootPageId(root_page_id_);
      }
      old_node->SetParentPageId(newip_mem->GetPageId());
      new_node->SetParentPageId(newip_mem->GetPageId());
      // newip->PopulateNewRoot();
      return;
    }
    // printf("exist parent for 2 page %d\n",old_node->GetParentPageId());
    //1.2将newnode加入到oldnode的父节点中
    auto ipmem=buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    ip=PAGE_REF_INTERNEL(ipmem);
    // concurr.lock_one(ip);
    // printf("- InsertIntoParent 1\n");
    //加入到的位置并不总是在最后，可能分裂前的节点在父节点中的区间在中间，
    //  那么分裂后，新的key应当插入在之前节点的key之后
    new_node->SetParentPageId(ip->GetPageId());
    // printf("- InsertIntoParent 2\n");
    // int size=
    ip->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
    
    // printf("- InsertIntoParent 3\n");
    if(ip->ReachSplitSize()){
      //达到最大，需要分裂
      InternalPage*newip= Split(ip);
      // concurr.lock_one(newip);
      InsertIntoParent(ip,newip->KeyAt(0),newip,concurr);
      // buffer_pool_manager_->UnpinPage(newip->GetPageId(),true);
    }
    buffer_pool_manager_->UnpinPage(ip->GetPageId(),true);
    // printf("InsertIntoParent done\n");
}

INDEX_TEMPLATE_ARGUMENTS
FindSibResult BPLUSTREE_TYPE::FindSibInInternel(
  int this_index,InternalPage* ip){
    
    FindSibResult res{true,0};
    
    if(this_index==ip->GetSize()-1){
      //右边找不到
      // printf("get sib on left\n");
      // sibling_pid=parent_p->ValueAt(i-1);
      res.sib_on_right=false;
      res.sib_index=this_index-1;
    }else{
      // printf("get sib on right\n");
      // sibling_pid=parent_p->ValueAt(i+1);
      res.sib_on_right=true;
      res.sib_index=this_index+1;
    }
    return res;
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(
  const KeyType &key, Transaction *transaction) {
    //export this private
    auto bpman=buffer_pool_manager_;
    BPlusTreeConcurrentControl concurr(
      BPlusTreeConcurrentControlMode::Delete,buffer_pool_manager_
    );
    Page* p=FindLeafPage(key,&concurr);
    LeafPage*lp= PAGE_REF_LEAF(p);

    // page 的 unpin 直接交给并发管理器
    //
    // pa::Unique<Page*> holdpage(p,[&](Page*& v){
    //   // printf("pa unique unpin page remove p %d %d\n",
    //   //   v->GetPageId(),
    //   //   v->GetPinCount());
    //   // bpman->UnpinPage(v->GetPageId(),true);
    // });

    auto oldsz=lp->GetSize();
    if(oldsz==0){
      return;
    }
    auto firstkey=lp->KeyAt(0);
    auto sz=lp->RemoveAndDeleteRecord(key,comparator_);
    //1.del succ
    if(sz!=oldsz){
      // printf("del succ\n");
      //1.>=half sz
      if(lp->GetSize()>=lp->GetMaxSize()/2){
        //根据pdf，半满情况下
        // 即使删除的是父节点区间边界值，也不用做父节点的更新。
      }
      //2.<half sz
      else{
        //2.0
        if(root_page_id_==lp->GetPageId()){
          //2.0.1 more then one key
          if(lp->GetSize()>0){
            // lp->RemoveAndDeleteRecord(key,comparator_);
          }else{
            //will unpin page when out of range;
            // {
            //   auto consumehold=std::move(holdpage);
            // }
            printf("pa unique unpined page remove p %d\n",root_page_id_);
            if(!buffer_pool_manager_->DeletePage(root_page_id_)){
              throw Exception(ExceptionType::INVALID,"lf page del failed");
            }
            root_page_id_=INVALID_PAGE_ID;
          }
          // throw Exception(ExceptionType::NOT_IMPLEMENTED,"remove root is not impled");
        }else{
          //2.1
          Page* parent_p_mem=buffer_pool_manager_->FetchPage(lp->GetParentPageId());
          InternalPage* parent_p=PAGE_REF_INTERNEL(parent_p_mem);
          pa::Unique<Page*> holdpage(parent_p_mem,[&](Page*& v){
            // printf("pa unique unpin page remove parent\n");
            bpman->UnpinPage(v->GetPageId(),true);
          });
          //2.1有共同父节点的兄弟节点
          if(parent_p->GetSize()>1){
            //先找到兄弟节点,
            //这里记录下当前节点的位置
            int i=parent_p->LookupKeyIndex(firstkey,comparator_);
            auto res=FindSibInInternel(i,parent_p);
            page_id_t sibling_pid=parent_p->ValueAt(res.sib_index);
            auto sib_on_left=!res.sib_on_right;

            // printf("get sib succ\n");
            Page* sib_lp_mem=buffer_pool_manager_->FetchPage(sibling_pid);
            if(!sib_lp_mem){
              throw Exception(ExceptionType::INVALID,"fetch sibling page failed");
            }
            auto bpman=buffer_pool_manager_;
            pa::Unique<Page*> holdpage(sib_lp_mem,[&](Page*& v){
              // printf("pa unique unpin page remove sib\n");
              bpman->UnpinPage(v->GetPageId(),true);
            });
            LeafPage* sib_lp=PAGE_REF_LEAF(sib_lp_mem);
            //  2.1.1 兄弟节点>半满
            //  与兄弟节点 redistribute(兄弟节点拿出前面的给当前的)
            if(sib_lp->GetSize()>sib_lp->GetMaxSize()/2){
              
              Redistribute(sib_lp,lp,sib_on_left?sib_lp->GetSize()-1:0);
              // printf("Redistribute %s\n",sib_on_left?"left":"right");
              if(sib_on_left){
                //划分为当前lp第一个（移入的，划分下标为i
                parent_p->SetKeyAt(i,lp->GetItem(0).first);
              }else{
                //划分sib第一个，划分下标为i+1
                // printf("change parent key")
                parent_p->SetKeyAt(i+1,sib_lp->GetItem(0).first);
              }
            }
            //  2.1.2 兄弟节点=半满
            //  与兄弟节点 coalesce(合并)
            else if(sib_lp->GetSize()==sib_lp->GetMaxSize()/2){
              //coalese, 与其他节点合并，会导致从父节点中移除某个数据
              printf("coalesce\n");
              Coalesce(&sib_lp,&lp,&parent_p,i,!sib_on_left,transaction);
              
              //throw Exception(ExceptionType::NOT_IMPLEMENTED,"colease is not handled");
              // Coalesce(&sib_lp,&lp,&parent_p);
            }else{
              throw Exception(ExceptionType::INVALID,"sibling cant be < M/2");
            }
          }
          //2.2
          else{
            throw Exception(ExceptionType::INVALID,"no sibling node");
          }
        }
      }
    }
    //2.del fail
    else{
      // printf("del fail\n");
    }
  }

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * 
 * index: node 对应在父节点中的划分
 * 
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::CoalesceEdgeHandle(InternalPage** parent,
  KeyType old_parent_first_key,
  Transaction *transaction){
    auto bpman=buffer_pool_manager_;    
    if((*parent)->GetSize()-1>=(*parent)->GetMaxSize()/2){
    
    if(root_page_id_==(*parent)->GetPageId()){
      
    }else{//父节点没有达到半满，需要补充
      //load parent
      // 加载父节点的父节点，用于获取父节点的兄弟节点。
      Page* parent_parent=buffer_pool_manager_->FetchPage((*parent)->GetParentPageId());
      if(!parent_parent){
        throw Exception(ExceptionType::INVALID,"parent's parent not found");
      }
      pa::Unique<Page*> holdpage_parent(parent_parent,[&](Page*& v){
        // printf("pa unique unpin page coal\n");
        bpman->UnpinPage(v->GetPageId(),true);
      });
      InternalPage* parent_parent_ip=PAGE_REF_INTERNEL(parent_parent);
      auto parent_i=parent_parent_ip->LookupKeyIndex(
        old_parent_first_key,comparator_);
      // std::cout<<"look for key "<<old_parent_first_key<<std::endl;
      //load sib page
      auto sib_pos=FindSibInInternel(parent_i,parent_parent_ip);
      auto sib_pid=parent_parent_ip->ValueAt(sib_pos.sib_index);
      Page* sib_page_mem=buffer_pool_manager_->FetchPage(sib_pid);
      if(!sib_page_mem){
        throw Exception(ExceptionType::INVALID,"fetch page failed coalesce");
      }
      pa::Unique<Page*> holdpage_sib(sib_page_mem,[&](Page*& v){
        // printf("pa unique unpin page coal\n");
        bpman->UnpinPage(v->GetPageId(),true);
      });
      InternalPage* sib_page=PAGE_REF_INTERNEL(sib_page_mem);
      
      //大于半满 redis
      // 因为第一个节点是dummy node，所以size-1
      if(sib_page->GetSize()-1>sib_page->GetMaxSize()/2){
        // printf("redis in coal\n");
        // std::cout<<"  middle key "<<parent_parent_ip->KeyAt(parent_i)<<std::endl;
        Redistribute(sib_page,*parent,sib_pos.sib_on_right?0:1,
          parent_parent_ip->KeyAt(sib_pos.sib_on_right?parent_i+1:parent_i));
        
        if(sib_pos.sib_on_right){
          parent_parent_ip->SetKeyAt(parent_i+1,sib_page->KeyAt(0));
        }else{
          parent_parent_ip->SetKeyAt(parent_i,(*parent)->KeyAt(0));
        }
      }
      //等于半满，coal
      else if(sib_page->GetSize()-1==sib_page->GetMaxSize()/2){
        // printf("coal in coal\n");
        Coalesce(&sib_page,parent,&parent_parent_ip,parent_i,
          sib_pos.sib_on_right,transaction);
      }else{
        throw Exception(ExceptionType::INVALID,"sib page cant < M/2");
      }
    }
  }
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
bool BPLUSTREE_TYPE::CoalesceCheckParentRootExpire(
  BPlusTreePage* one_childpage,
  InternalPage*parent
) {
  if((parent)->GetPageId()==root_page_id_&&(parent)->GetSize()==1){
      
      // printf(" parent root expire\n");
      buffer_pool_manager_->UnpinPage(root_page_id_,false);
      buffer_pool_manager_->DeletePage(root_page_id_);
      root_page_id_=one_childpage->GetPageId();
      UpdateRootPageId(root_page_id_);
      one_childpage->SetParentPageId(INVALID_PAGE_ID);
      return true;
    }
    return false;
}
INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
bool BPLUSTREE_TYPE::Coalesce(InternalPage **neighbor_node, InternalPage **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                              int index,
                              bool sib_on_right, Transaction *transaction) {

 //used for search position in parent page                 
  auto old_parent_first_key=(*parent)->KeyAt(1);
  if(sib_on_right){
    //neighbor on right
    // neighbor move to node
    (*neighbor_node)->MoveAllTo(*node,(*parent)->KeyAt(index+1),buffer_pool_manager_);
    (*parent)->Remove(index+1);
    buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(),false);
    if(!buffer_pool_manager_->DeletePage((*neighbor_node)->GetPageId())){
      throw Exception(ExceptionType::INVALID,"coalesce delete page failed");
    }

    
    if(CoalesceCheckParentRootExpire(*node,*parent)){
      return true;
    }
  }else{
    //neighbor on left
    // node move to neighbor 
    (*node)->MoveAllTo(*neighbor_node,(*parent)->KeyAt(index),buffer_pool_manager_);
    (*parent)->Remove(index);
     
    
    buffer_pool_manager_->UnpinPage((*node)->GetPageId(),false);
    if(!buffer_pool_manager_->DeletePage((*node)->GetPageId())){
      throw Exception(ExceptionType::INVALID,"coalesce delete page failed");
    }

    if(CoalesceCheckParentRootExpire(*neighbor_node,*parent)){
      return true;
    }
  }
  return CoalesceEdgeHandle(parent,old_parent_first_key,transaction);
  // throw Exception(ExceptionType::NOT_IMPLEMENTED,"Coalesce failed");
  // return false;
}

INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
bool BPLUSTREE_TYPE::Coalesce(LeafPage **neighbor_node, LeafPage **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                              int index,
                              bool sib_on_right, Transaction *transaction) { 
  //used for search position in parent page                 
  auto old_parent_first_key=(*parent)->KeyAt(1);
  if(sib_on_right){
    //neighbor on right
    // neighbor move to node
    (*neighbor_node)->MoveAllTo(*node);
    (*parent)->Remove(index+1);
    (*node)->SetNextPageId((*neighbor_node)->GetNextPageId());
    buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(),false);
    if(!buffer_pool_manager_->DeletePage((*neighbor_node)->GetPageId())){
      throw Exception(ExceptionType::INVALID,"coalesce delete page failed");
    }
    
    if(CoalesceCheckParentRootExpire(*node,*parent)){
      return true;
    }
  }else{
    //neighbor on left
    // node move to neighbor 
    (*node)->MoveAllTo(*neighbor_node);
    (*parent)->Remove(index);
     
    (*neighbor_node)->SetNextPageId((*node)->GetNextPageId());
    
    buffer_pool_manager_->UnpinPage((*node)->GetPageId(),false);
    if(!buffer_pool_manager_->DeletePage((*node)->GetPageId())){
      throw Exception(ExceptionType::INVALID,"coalesce delete page failed");
    }

    if(CoalesceCheckParentRootExpire(*neighbor_node,*parent)){
      return true;
    }
  }
  return CoalesceEdgeHandle(parent,old_parent_first_key,transaction);
  // throw Exception(ExceptionType::NOT_IMPLEMENTED,"Coalesce failed");
  // return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
//   if(neighbor_node->IsLeafPage()){
//     // BPlusTreePage* p=(BPlusTreePage*)neighbor_node;
//     if(index==0){
//       ((LeafPage*)neighbor_node)->MoveFirstToEndOf((LeafPage*)node);
//     }else{
//       ((LeafPage*)neighbor_node)->MoveLastToFrontOf((LeafPage*)node);
//     }
//   }else{

//   }
// }
INDEX_TEMPLATE_ARGUMENTS
// template <>
void BPLUSTREE_TYPE::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  if(index==0){
    (neighbor_node)->MoveFirstToEndOf(node);
  }else{
    (neighbor_node)->MoveLastToFrontOf(node);
  }
}
INDEX_TEMPLATE_ARGUMENTS
// template <>
void BPLUSTREE_TYPE::Redistribute(
  InternalPage *neighbor_node,
  InternalPage *node,
  int index,KeyType middle_key) {
  // KeyType key{};
  if(index==0){
    //sib on right
    // std::cout<<"movefirst"<<middle_key<<std::endl;
    neighbor_node->MoveFirstToEndOf(node,middle_key,buffer_pool_manager_);
  }else{
    //sib on left
    // std::cout<<"movelast"<<neighbor_node->KeyAt(neighbor_node->GetSize()-1)<<std::endl;
    neighbor_node->MoveLastToFrontOf(node,middle_key,buffer_pool_manager_);
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { 
  KeyType k{};
  auto ret=INDEXITERATOR_TYPE(buffer_pool_manager_);
  Page* lp_page= FindLeafPage(k,&ret.concurr(),true);
  ret.init(0,lp_page);
  return ret;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
  auto ret=INDEXITERATOR_TYPE(buffer_pool_manager_);
  Page* lp_page= FindLeafPage(key,&ret.concurr(),false);
  LeafPage* lp= PAGE_REF_LEAF(lp_page);
  int first_big=lp->KeyIndex(key,comparator_);
  if(first_big==-1){
    // printf("Begin return1\n");
    return end();
  }
  ret.init(first_big,lp_page);
  // printf("Begin return2\n");
  return  ret;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { 
  // printf("end return\n");
  auto ret=INDEXITERATOR_TYPE(buffer_pool_manager_);
  ret.init(0,nullptr);
  return ret; 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * 
 * remember to unpin
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, 
  BPlusTreeConcurrentControl*conccur,bool leftMost) {
  auto curpageid=root_page_id_;
  Page* curpage=nullptr;
  //找到叶节点
  
  while(1){
    curpage=buffer_pool_manager_->FetchPage(curpageid);
    ParentPage* page=(ParentPage*)(LeafPage*)curpage->GetData();
    // ParentPage* page2=reinterpret_cast<ParentPage*>(curpage->GetData());
    if(conccur){
      conccur->lock_one(page);
      //第一层没有pre
      if(curpageid!=root_page_id_){
        conccur->if_safe_then_free_pre();
      }
    }
    // auto lfp=(LeafPage*)page;
    // auto lfp_=(LeafPage*)page2;
    // auto lfp2=static_cast<LeafPage*>(page);
    // auto lfp3=reinterpret_cast<LeafPage*>(page);
    // printf("%ld",(uint64_t)lfp+(uint64_t)lfp2+(uint64_t)lfp3
    //   +(uint64_t)page2+(uint64_t)lfp_);

    //看子节点会不会
    if(page->IsLeafPage()){
      break;
    }else{
      //internel page 找区间
      InternalPage* ip=(InternalPage*)page;
      if(leftMost){
        curpageid=ip->ValueAt(0);
        if(!conccur){//没加锁就先unpin了
          buffer_pool_manager_->UnpinPage(ip->GetPageId(),false);  
        }
        
      }else{
        page_id_t v=ip->Lookup(key,comparator_);
        assert(v!=-1);
        if(!conccur)
        {//没加锁就先unpin了，
          //unpin 父page
          buffer_pool_manager_->UnpinPage(curpageid,false);
        }
        curpageid=v;
      }
    }
  }

  if(!curpage){
    throw Exception(ExceptionType::INVALID,"FindLeafPage not found");
  }
  return curpage;
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_
    ->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
