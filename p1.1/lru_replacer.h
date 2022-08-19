//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include <unordered_map>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

// struct Node{
//   frame_id_t v;
//   Node* pre;
//   Node* next;
// };
// struct NodeListQueue{
//   public:
//   void reserve(int cnt);
//   private:
//   void insert_free();
//   Node* use_free();
  
//   Node**freelist;
//   Node* pool;
//   ~NodeListQueue();
// };

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  size_t max_cnt;
 std::mutex mu;
  std::list<frame_id_t> list;
  std::unordered_map<frame_id_t,
    std::list<frame_id_t>::iterator> map;
  // TODO(student): implement me!
};

}  // namespace bustub
