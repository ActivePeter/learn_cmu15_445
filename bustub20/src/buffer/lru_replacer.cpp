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

LRUReplacer::LRUReplacer(size_t num_pages) { num_pages_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> _g(latch);
  if (queue.size() == 0) return false;

  *frame_id = queue.front();
  searchnode.erase(queue.front());
  queue.pop_front();

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> _g(latch);
  //线程使用，从队列中剔除
  auto f = searchnode.find(frame_id);
  if (f == searchnode.end()) {
    // printf(
    //     "Pin"
    //     "exist");
        return;
  }

  //从队列移出并去除hash索引
  queue.erase(f->second);
  searchnode.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> _g(latch);
  //线程不用，放入回收队列
  if (searchnode.find(frame_id) != searchnode.end()) {
    //已有节点，出错
    // printf(
    //     "UnPin"
    //     "exist");
    return;
  }

  //加入队列，并且添加hash索引
  queue.push_back(frame_id);
  auto iter = queue.end();
  iter--;
  searchnode[frame_id] = iter;
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> _g(latch);
  return queue.size();
}

}  // namespace bustub
