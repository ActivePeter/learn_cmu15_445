//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {
    // void NodeListQueue::reserve(int cnt)
    // {
    //     this->pool
    // }

    // void NodeListQueue::insert_free()
    // {

    // }

    // Node* NodeListQueue::use_free()
    // {

    // }

    // NodeListQueue::~NodeListQueue()
    // {

    // }



    LRUReplacer::LRUReplacer(size_t num_pages) {
        //用链表来表示队列
        //集中管理链表节点的alloc
        //
    }

    LRUReplacer::~LRUReplacer(){

    }

    auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
        //移出最旧的
        if(list.size()>0){
        mu.lock();
            auto back=list.back();
            list.pop_back();
            map.erase(back);
        mu.unlock();
        
            *frame_id=back;
        return true;
        }
        return false; }

    void LRUReplacer::Pin(frame_id_t frame_id) {
        //线程使用，从队列移出
        mu.lock();
        auto find=map.find(frame_id);
        if(find!=map.end()){
            list.erase(find->second);
            map.erase(frame_id);
        }
        // list.erase
        mu.unlock();
    }

    void LRUReplacer::Unpin(frame_id_t frame_id) {
        //线程不使用,这里看做一般的插入操作
        mu.lock();
        if(map.find(frame_id)==map.end()){
            //没有。就加入
            list.push_front(frame_id);
            map[frame_id]=list.begin();
        }
        mu.unlock();
    }

    auto LRUReplacer::Size() -> size_t {
        size_t s;
        // mu.lock();
        s=list.size();
        // mu.unlock();/
        return s;
         }

}  // namespace bustub
