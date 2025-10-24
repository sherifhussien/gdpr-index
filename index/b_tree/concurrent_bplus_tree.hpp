#pragma once

#include <atomic>
#include <vector>
#include <unordered_set>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <map>


template<typename K = std::string, typename V = std::string, size_t ORDER = 64>
class BPlusTree : public IIndex<K, V> {
private:
  // property: leaf nodes contain between ⌈m/2⌉ and m keys
  static constexpr size_t MIN_KEYS_LEAF = (ORDER + 1) / 2; // ceil(ORDER/2)
  static constexpr size_t MAX_KEYS_LEAF = ORDER;

  // property: internal nodes except the root contain between ⌈m/2⌉ and m children
  static constexpr size_t MIN_CHILDREN_INTERNAL = (ORDER + 1) / 2; // ceil(ORDER/2)
  static constexpr size_t MAX_CHILDREN_INTERNAL = ORDER;
  
  // property: each internal node with k children contains k-1 keys
  static constexpr size_t MIN_KEYS_INTERNAL = MIN_CHILDREN_INTERNAL - 1;
  static constexpr size_t MAX_KEYS_INTERNAL = MAX_CHILDREN_INTERNAL - 1;

  /**
   *  [ko, k1, k2, ..., km-1]
   *  [c0, c1, c2, ..., cm-1, cm]
   *
   *  ci -> [ki-1, ki)
   */
  struct Node {
    std::vector<K> keys;
    std::vector<std::unordered_set<V>> values;    // leaf
    std::vector<std::shared_ptr<Node>> children;  // internal
    std::shared_ptr<Node> next;
    bool is_leaf;

    mutable std::shared_mutex node_mutex;

    Node(bool leaf = false): is_leaf(leaf), next(nullptr) {
      if(leaf) {
        keys.reserve(MAX_KEYS_LEAF+1); // +1 to handle temporary overflow
        values.reserve(MAX_KEYS_LEAF+1);
      } else {
        keys.reserve(MAX_KEYS_INTERNAL+1);
        children.reserve(MAX_KEYS_INTERNAL+1);
      }
    }

    auto is_safe_for_insert() const -> bool {
      return is_leaf ? keys.size() < MAX_KEYS_LEAF : keys.size() < MAX_KEYS_INTERNAL;
    }

    auto is_safe_for_delete() const -> bool {
      return is_leaf ? keys.size() > MIN_KEYS_LEAF : keys.size() > MIN_KEYS_INTERNAL;
    }

  };

  std::atomic<std::shared_ptr<Node>> root;
  mutable std::shared_mutex tree_mutex;

public:
  BPlusTree(): root(std::make_shared<Node>(true)) {}

  auto insert(const K& key, const V& value) -> bool {
    if (optimistic_insert(key, value)) return true;
    pessimistic_insert(key, value);
    return true;
  }

  auto search(const K& key) const -> std::unordered_set<V> {
    while(true) { // retry loop
      // capture initial root for validation
      auto initial_root = root.load(std::memory_order_acquire);

      auto current = initial_root;
      std::shared_lock<std::shared_mutex> current_lock(current->node_mutex);

      // EARLY VALIDATION
      if (root.load(std::memory_order_acquire) != initial_root) {
        current_lock.unlock();
        continue;  // root changed
      }
  
      while (!current->is_leaf) {
        // key < *it
        auto it = std::upper_bound(current->keys.begin(), current->keys.end(), key);
        size_t idx = it - current->keys.begin();
        auto child = current->children[idx];
        
        // lock coupling
        std::shared_lock<std::shared_mutex> child_lock(child->node_mutex);
        current_lock.unlock();
        
        current = child;
        current_lock = std::move(child_lock);
      }
  
      // key <= *it
      auto it = std::lower_bound(current->keys.begin(), current->keys.end(), key);
      if (it == current->keys.end() || *it != key) {
        return {};
      }
      
      size_t idx = it - current->keys.begin();
      return std::unordered_set<V>(current->values[idx].begin(), current->values[idx].end());
    }
  }

  auto range_search(const K& start_key, const K& end_key) const -> std::map<K, std::unordered_set<V>> {
    std::map<K, std::unordered_set<V>> result;
    if (start_key >= end_key) {
      return result;
    }

    while(true) {
      // Capture initial root for validation
      auto initial_root = root.load(std::memory_order_acquire);
      
      auto current = initial_root;
      std::shared_lock<std::shared_mutex> current_lock(current->node_mutex);

      // EARLY VALIDATION
      if (root.load(std::memory_order_acquire) != initial_root) {
        current_lock.unlock();
        continue;  // root changed
      }

      while(!current->is_leaf) {
        // key < *it
        auto it = std::upper_bound(current->keys.begin(), current->keys.end(), start_key);
        size_t idx = it - current->keys.begin();
        auto child = current->children[idx];

        // lock coupling
        std::shared_lock<std::shared_mutex> child_lock(child->node_mutex);
        current_lock.unlock();

        current = child;
        current_lock = std::move(child_lock);
      }

      bool done = false;
      while(!done) {
        // find the first key >= start_key in the current leaf
        auto it = std::lower_bound(current->keys.begin(), current->keys.end(), start_key);
        size_t idx = std::distance(current->keys.begin(), it);

        // scan through keys in the current leaf
        for (; idx < current->keys.size(); ++idx) {
          if (current->keys[idx] >= end_key) {
            done = true;
            break;
          }
          result.emplace(current->keys[idx], current->values[idx]);
        }

        if (done) {
          break;
        }

        auto next_node = current->next;
        if (next_node) {
          std::shared_lock<std::shared_mutex> next_lock(next_node->node_mutex);
          current_lock.unlock();
          current = next_node;
          current_lock = std::move(next_lock);
        } else {
          // No more leaves in the chain
          done = true;
        }
      }
      return result;
    }
  }

private:
  auto optimistic_insert(const K& key, const V& val) -> bool {
    while (true) {  // retry loop
      // capture root for validation
      auto initial_root = root.load(std::memory_order_acquire);
      auto current = initial_root;

      std::unique_lock<std::shared_mutex> exclusive_lock;
      if (current->is_leaf) {
        exclusive_lock = std::unique_lock<std::shared_mutex>(current->node_mutex);
        
        // EARLY VALIDATION
        if (root.load(std::memory_order_acquire) != initial_root) {
          exclusive_lock.unlock();
          continue;  // root changed
        }
      } else {
        std::shared_lock<std::shared_mutex> shared_lock(current->node_mutex);
        
        // EARLY VALIDATION
        if (root.load(std::memory_order_acquire) != initial_root) {
          shared_lock.unlock();
          continue;  // root changed
        }
        
        while (!current->is_leaf) {
          auto it = std::upper_bound(current->keys.begin(), current->keys.end(), key);
          size_t idx = it - current->keys.begin();
          auto child = current->children[idx];

          if (child->is_leaf) {
            exclusive_lock = std::unique_lock<std::shared_mutex>(child->node_mutex);
            shared_lock.unlock();
            current = child;
          } else {
            std::shared_lock<std::shared_mutex> child_lock(child->node_mutex);
            shared_lock.unlock();
            current = child;
            shared_lock = std::move(child_lock);
          }
        }
      }

      // leaf if exclusively locked
      
      if (!current->is_safe_for_insert()) {
        return false;
      }

      auto it = std::lower_bound(current->keys.begin(), current->keys.end(), key);
      size_t pos = it - current->keys.begin();
      
      if (it != current->keys.end() && *it == key) {
        current->values[pos].insert(val);
      } else {
        current->keys.insert(it, key);
        current->values.insert(current->values.begin() + pos, std::unordered_set<V>{val});
      }

      return true;
    }
  }

  auto pessimistic_insert(const K& key, const V& value) -> void {
    // not released until the root is safe from overflow
    std::unique_lock<std::shared_mutex> guard(tree_mutex);
    std::vector<std::shared_ptr<Node>> held;

    auto current = root.load(std::memory_order_acquire);
    current->node_mutex.lock();
    held.push_back(current);

    std::shared_ptr<Node> head = current->is_safe_for_insert() ? current : nullptr;
    
    // release tree latch, head is safe
    if (head) guard.unlock();

    while (!current->is_leaf) {
      // key < *it
      auto it = std::upper_bound(current->keys.begin(), current->keys.end(), key);
      size_t idx = it - current->keys.begin();
      auto child = current->children[idx];
      
      // lock child
      child->node_mutex.lock();
      
      // release all held ancestors if child is safe
      if (child->is_safe_for_insert()) {
        for (auto& node : held) {
          node->node_mutex.unlock();
        }
        held.clear();
        head = child;
        
        // Release tree latch if we still hold it
        if (guard.owns_lock()) guard.unlock();
      }
      
      held.push_back(child);
      current = child;
    }

    // Insert starting from safe node
    auto start_node = head ? head : root.load(std::memory_order_acquire);
    auto [sibling, promoted_key] = insert_recursive(start_node, key, value);
    
    // Handle split
    if (sibling) {
      auto current_root = root.load(std::memory_order_acquire);
      if (start_node == current_root) {
        // Root split
        auto new_root = std::make_shared<Node>(false);
        new_root->keys.push_back(promoted_key);
        // property: the root has at least two children if it is not a leaf node
        new_root->children.push_back(current_root);
        new_root->children.push_back(sibling);
        root.store(new_root, std::memory_order_release);
      } else {
        auto it = std::upper_bound(start_node->keys.begin(), start_node->keys.end(), key);
        size_t pos = it - start_node->keys.begin();
        start_node->keys.insert(start_node->keys.begin() + pos, promoted_key);
        start_node->children.insert(start_node->children.begin() + pos + 1, sibling);
      }
    }
    
    // release all locks
    for (auto& node : held) {
      node->node_mutex.unlock();
    }
  }

  // return {sibling, promoted_key} on split
  auto insert_recursive(std::shared_ptr<Node> node, const K& key, const V& value) -> std::pair<std::shared_ptr<Node>, K> {
    if (node->is_leaf) {
      return insert_into_leaf(node, key, value);
    } else {
      return insert_into_internal(node, key, value);
    }
  }

  auto insert_into_leaf(std::shared_ptr<Node> leaf, const K& key, const V& value) -> std::pair<std::shared_ptr<Node>, K> {
    // key <= *it
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    size_t pos = it - leaf->keys.begin();
    
    if (it != leaf->keys.end() && *it == key) {
      leaf->values[pos].insert(value);
    } else {
      leaf->keys.insert(it, key);
      leaf->values.insert(leaf->values.begin() + pos, std::unordered_set<V>{value});
    }
    
    // split if overflow
    if (leaf->keys.size() > MAX_KEYS_LEAF) {
      return split_leaf(leaf);
    }
    
    return {nullptr, K{}};
  }

  auto insert_into_internal(std::shared_ptr<Node> internal, const K& key, const V& value) -> std::pair<std::shared_ptr<Node>, K> {
    // key < *it
    auto it = std::upper_bound(internal->keys.begin(), internal->keys.end(), key);
    size_t pos = it - internal->keys.begin();
    
    auto [child_sibling, promoted_key] = insert_recursive(internal->children[pos], key, value);
    if (child_sibling) {
      // Insert promoted key and sibling
      internal->keys.insert(internal->keys.begin() + pos, promoted_key);
      internal->children.insert(internal->children.begin() + pos + 1, child_sibling);
    }
    
    // split if overflow
    if (internal->keys.size() > MAX_KEYS_INTERNAL) {
      return split_internal(internal);
    }

    return {nullptr, K{}};
  }

  auto split_leaf(std::shared_ptr<Node> leaf) -> std::pair<std::shared_ptr<Node>, K> {
    size_t total = leaf->keys.size();
    size_t mid = total / 2; // right‐biased
    
    auto sibling = std::make_shared<Node>(true);
    
    // Move second half to sibling
    // [mid..end]
    sibling->keys.assign(
        std::make_move_iterator(leaf->keys.begin() + mid),
        std::make_move_iterator(leaf->keys.end())
    );
    sibling->values.assign(
        std::make_move_iterator(leaf->values.begin() + mid),
        std::make_move_iterator(leaf->values.end())
    );
    
    // Fix leaf chain
    sibling->next = leaf->next;
    leaf->next = sibling;
    
    // Resize original leaf
    leaf->keys.resize(mid);
    leaf->values.resize(mid);

    return {sibling, sibling->keys.front()};
  }

  auto split_internal(std::shared_ptr<Node> internal) -> std::pair<std::shared_ptr<Node>, K> {
    size_t total = internal->keys.size();
    size_t mid = total / 2;
    
    K promoted_key = internal->keys[mid];
    auto sibling = std::make_shared<Node>(false);
    
    // Move second half to sibling (excluding promoted key)
    // [mid+1..end]
    sibling->keys.assign(
        std::make_move_iterator(internal->keys.begin() + mid + 1),
        std::make_move_iterator(internal->keys.end())
    );
    sibling->children.assign(
        std::make_move_iterator(internal->children.begin() + mid + 1),
        std::make_move_iterator(internal->children.end())
    );
    
    // Resize original internal
    internal->keys.resize(mid);
    internal->children.resize(mid + 1);
    
    return {sibling, promoted_key};
  }

/*

  auto optimisticRemove(const K& key) -> bool {
    Node* n;
    {
      std::lock_guard<std::mutex> guard(tree_latch);
      n = root_.get();
      // case: single‐leaf root
      if (n->is_leaf) n->lock.lock();
      else n->lock.lock_shared();
    }

    while (!n->is_leaf) {
      // key < *it
      auto it = std::upper_bound(n->keys.begin(), n->keys.end(), key);
      size_t idx = it - n->keys.begin();
      Node* c = n->children[idx].get();

      // lock child
      if (c->is_leaf) c->lock.lock();
      else c->lock.lock_shared();

      // release parent
      n->lock.unlock_shared();
      n = c;
    }

    // leaf
    bool safe = n->is_safe_for_delete();
    if (safe) {
      // key <= *it
      auto it = std::lower_bound(n->keys.begin(), n->keys.end(), key);
      if (it != n->keys.end() && *it == key) {
        size_t idx = it - n->keys.begin();
        n->keys.erase(it);
        n->values.erase(n->values.begin() + idx);
      }
    }

    n->lock.unlock();
    return safe;
  }

  auto pessimisticRemove(const K& key) -> void {
    // won't be released until the root is safe from collapse
    std::unique_lock<std::mutex> guard(tree_latch);
    
    std::vector<Node*> held;
    
    Node* n = root_.get();
    n->lock.lock();
    held.push_back(n);
    
    Node* head = root_->children.size() > 2 ? root_.get() : nullptr;
    Node* parent_of_head = nullptr;
    size_t index_in_parent = 0;
    
    // release tree latch, head is safe
    if (head) guard.unlock();

    while (!n->is_leaf) {
      // key < *it
      auto it = std::upper_bound(n->keys.begin(), n->keys.end(), key);
      size_t idx = it - n->keys.begin();
      Node* c = n->children[idx].get();

      // lock child
      c->lock.lock();

      // release all held ancestors if safe
      if (c->is_safe_for_delete()) {
        // guard.unlock(); // release tree latch
        for (Node* x : held) {
          x->lock.unlock();
        }
        held.clear();
        head = c; // reset head to current child
        parent_of_head = n;
        index_in_parent = idx;
      }
      held.push_back(c);
      n = c;
    }

    // release tree latch, head is safe
    if (head && guard.owns_lock()) guard.unlock();

    // nullptr head implies that root_ is not safe
    remove_rec(parent_of_head, head ? head : root_.get(), index_in_parent, key, held);

    // collapse root if it lost all keys and has one child
    if (!root_->is_leaf && root_->keys.empty() && root_->children.size() == 1) {
      root_ = std::move(root_->children.front());
    }

    // unlock all held locks
    for (Node* x : held) {
      x->lock.unlock();  // TODO: what if node was deleted in a merge? or collapsed?
    }
  }

  // returns true if parent must rebalance child at idx
  auto remove_rec(Node* parent, Node* node, size_t idx_in_parent, const K& key, std::vector<Node*>& held) -> bool {
    if (node->is_leaf) {
      return remove_from_leaf(parent, node, idx_in_parent, key, held);
    } else {
      return remove_from_internal(parent, node, idx_in_parent, key, held);
    }
  }

  // ========== LEAF REMOVAL ==========

  // returns true if parent must rebalance child at idx
  auto remove_from_leaf(Node* parent, Node* leaf, size_t pos, const K& key, std::vector<Node*>& held) -> bool {
    // key <= *it
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    if (it == leaf->keys.end() || *it != key) return false;

    size_t idx = it - leaf->keys.begin();
    leaf->keys.erase(it);
    leaf->values.erase(leaf->values.begin() + idx);

    // underflow
    if (parent && leaf->keys.size() < MIN_KEYS_LEAF) {
      handle_leaf_underflow(parent, pos, held);
      return true; // parent may need rebalancing (after merge)
    }
    return false;  // no parent‐rebalance needed
  }

  // ========== INTERNAL NODE REMOVAL ==========

  // returns true if parent must rebalance child at idx 
  auto remove_from_internal(Node* parent, Node* inode, size_t pos, const K& key, std::vector<Node*>& held) -> bool {
    // key < *it
    auto it = std::upper_bound(inode->keys.begin(), inode->keys.end(), key);
    size_t ci = it - inode->keys.begin();

    if (!remove_rec(inode, inode->children[ci].get(), ci, key, held)) {
      return false;
    }
    // after child deletion+possible merge, this node may underflow
    if (parent && inode->keys.size() < MIN_KEYS_INTERNAL) {
      handle_internal_underflow(parent, pos, held);
      return true; // parent may need rebalancing (after merge)
    }
    return false;
  }

  auto remove_from_held(std::vector<Node*>& held, Node* node_to_remove) -> void {
    auto it = std::find(held.begin(), held.end(), node_to_remove);
    if (it != held.end()) {
      held.erase(it);
    }
  }

  // ========== LEAF UNDERFLOW HANDLING ==========

  auto handle_leaf_underflow(Node* parent, size_t idx, std::vector<Node*>& held) -> void {
    // Try borrow or merge in this order
    if (borrow_leaf_from_left(parent, idx)) return;
    if (borrow_leaf_from_right(parent, idx)) return;
    if (idx > 0)
      merge_leaf_with_left(parent, idx, held);
    else if (idx + 1 < parent->children.size())
      merge_leaf_with_right(parent, idx);
  }

  auto borrow_leaf_from_left(Node* p, size_t i) -> bool {
    if (i == 0) return false;

    auto &L = p->children[i-1], &C = p->children[i];
    if (L->keys.size() <= MIN_KEYS_LEAF) return false;

    C->keys.insert(C->keys.begin(), L->keys.back());
    C->values.insert(
        C->values.begin(),
        std::move(L->values.back())
    );
    L->keys.pop_back(); 
    L->values.pop_back();
    // update parent separator
    p->keys[i-1] = C->keys.front();
    return true;
  }

  auto borrow_leaf_from_right(Node* p, size_t i) -> bool {
    if (i+1 >= p->children.size()) return false;

    auto &C = p->children[i], &R = p->children[i+1];
    if (R->keys.size() <= MIN_KEYS_LEAF) return false;

    C->keys.push_back(R->keys.front());
    C->values.push_back(std::move(R->values.front()));
    R->keys.erase(R->keys.begin());
    R->values.erase(R->values.begin());
    // Update parent separator
    p->keys[i] = R->keys.front();
    return true;
  }

  auto merge_leaf_with_left(Node* p, size_t i, std::vector<Node*>& held) -> void {
    auto &L = p->children[i-1], &C = p->children[i];

    // Append C into L
    L->keys.insert(
        L->keys.end(),
        std::make_move_iterator(C->keys.begin()),
        std::make_move_iterator(C->keys.end())
    );
    L->values.insert(
        L->values.end(),
        std::make_move_iterator(C->values.begin()),
        std::make_move_iterator(C->values.end())
    );
    L->next = C->next;

    // Unlock and remove the node being merged from held vector before deletion
    Node* node_to_delete = C.get();
    node_to_delete->lock.unlock();
    remove_from_held(held, node_to_delete);

    // Remove C
    p->keys.erase(p->keys.begin() + (i-1));
    p->children.erase(p->children.begin() + i);
  }

  auto merge_leaf_with_right(Node* p, size_t i) -> void {
    auto &C = p->children[i], &R = p->children[i+1];
    // Append R into C
    C->keys.insert(
        C->keys.end(),
        std::make_move_iterator(R->keys.begin()),
        std::make_move_iterator(R->keys.end())
    );
    C->values.insert(
        C->values.end(),
        std::make_move_iterator(R->values.begin()),
        std::make_move_iterator(R->values.end())
    );
    C->next = R->next;
    // Remove R
    p->keys.erase(p->keys.begin() + i);
    p->children.erase(p->children.begin() + (i+1));
  }

  // ========== INTERNAL NODE UNDERFLOW HANDLING ==========

  auto handle_internal_underflow(Node* parent, size_t idx, std::vector<Node*>& held) -> void {
    if (borrow_internal_from_left(parent, idx))  return;
    if (borrow_internal_from_right(parent, idx)) return;
    if (idx > 0) 
      merge_internal_with_left(parent, idx, held);
    else if (idx + 1 < parent->children.size())
      merge_internal_with_right(parent, idx);
  }

  auto borrow_internal_from_left(Node* p, size_t i) -> bool {
    if (i == 0) return false;

    auto &L = p->children[i-1], &C = p->children[i];
    if (L->keys.size() <= MIN_KEYS_INTERNAL) return false;
    
    // pull separator down, move L’s last child up
    C->keys.insert(C->keys.begin(), p->keys[i-1]);
    p->keys[i-1] = L->keys.back();
    L->keys.pop_back();
    C->children.insert(
        C->children.begin(),
        std::move(L->children.back())
    );
    L->children.pop_back();
    return true;
  }

  auto borrow_internal_from_right(Node* p, size_t i) -> bool {
    if (i+1 >= p->children.size()) return false;
    
    auto &C = p->children[i], &R = p->children[i+1];
    if (R->keys.size() <= MIN_KEYS_INTERNAL) return false;
    
    // pull separator down, move R’s first child
    C->keys.push_back(p->keys[i]);
    p->keys[i] = R->keys.front();
    R->keys.erase(R->keys.begin());
    C->children.push_back(std::move(R->children.front()));
    R->children.erase(R->children.begin());
    return true;
  }

  auto merge_internal_with_left(Node* p, size_t i, std::vector<Node*>& held) -> void {
    auto &L = p->children[i-1], &C = p->children[i];
    // pull separator down
    L->keys.push_back(p->keys[i-1]);
    L->keys.insert(
        L->keys.end(),
        std::make_move_iterator(C->keys.begin()),
        std::make_move_iterator(C->keys.end())
    );
    L->children.insert(
        L->children.end(),
        std::make_move_iterator(C->children.begin()),
        std::make_move_iterator(C->children.end())
    );

    // Unlock and remove the node being merged from held vector before deletion
    Node* node_to_delete = C.get();
    node_to_delete->lock.unlock();
    remove_from_held(held, node_to_delete);

    // Remove C
    p->keys.erase(p->keys.begin() + (i-1));
    p->children.erase(p->children.begin() + i);
  }

  auto merge_internal_with_right(Node* p, size_t i) -> void {
    auto &C = p->children[i], &R = p->children[i+1];
    // pull separator down
    C->keys.push_back(p->keys[i]);
    C->keys.insert(
        C->keys.end(),
        std::make_move_iterator(R->keys.begin()),
        std::make_move_iterator(R->keys.end())
    );
    C->children.insert(
        C->children.end(),
        std::make_move_iterator(R->children.begin()),
        std::make_move_iterator(R->children.end())
    );
    p->keys.erase(p->keys.begin() + i);
    p->children.erase(p->children.begin() + (i+1));
  }

*/

};
