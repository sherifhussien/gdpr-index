#pragma once

#include <atomic>
#include <functional>
#include <iostream>
#include <random>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_set>

constexpr int MAX_LEVEL = 16;

// ----- Pointer Marking Utilities -----
template<typename T>
bool is_marked(T* ptr) {
  return (reinterpret_cast<uintptr_t>(ptr) & 1) != 0;
}

template<typename T>
auto get_marked_ref(T* ptr) -> T* {
  return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(ptr) | 1);
}

template<typename T>
auto get_unmarked_ref(T* ptr) -> T* {
  return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(ptr) & ~1);
}

// ----- Node -----
template<
  typename K = std::string,
  typename V = std::string
>
struct Node {
  K key;
  std::unordered_set<V> values;
  mutable std::shared_mutex value_mutex;
  int level;
  std::atomic<Node<K, V>*> next[1];
};

// ----- Skip List -----
template<
  typename K = std::string,
  typename V = std::string
>
class LockFreeSkipList: public IIndex<K, V> {
private:
  Node<K, V>* head;
  Node<K, V>* tail;

  auto create_node(const K& key, int level) -> Node<K, V>* {
    void* mem = new char[sizeof(Node<K, V>) + (level - 1) * sizeof(std::atomic<Node<K, V>*>)];
    Node<K, V>* node = new (mem) Node<K, V>{key, {}, {}, level};
    for (int i = 0; i < level; ++i) {
      node->next[i].store(nullptr);
    }
    return node;
  }

  auto create_node(const K& key, const V& value, int level) -> Node<K, V>* {
    void* mem = new char[sizeof(Node<K, V>) + (level - 1) * sizeof(std::atomic<Node<K, V>*>)];
    Node<K, V>* node = new (mem) Node<K, V>{key, {value}, {}, level};
    for (int i = 0; i < level; ++i) {
      node->next[i].store(nullptr);
    }
    return node;
  }

  auto random_level() const -> int {
    static thread_local std::mt19937 rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    std::uniform_int_distribution<int> dist(0, 1);
    int level = 1;
    while (dist(rng) && level < MAX_LEVEL) {
      level++;
    }
    return level;
  }

  auto find(const K& key, Node<K, V>** preds, Node<K, V>** succs) const -> bool {
    while (true) {
      Node<K, V>* pred = head;
      bool restart_traversal = false;
      for (int level = MAX_LEVEL - 1; level >= 0; --level) {
        Node<K, V>* curr = get_unmarked_ref(pred->next[level].load());
        while (true) {
          Node<K, V>* succ = (curr == tail) ? tail : curr->next[level].load();
          
          while (is_marked(succ)) {
            Node<K, V>* unmarked_succ = get_unmarked_ref(succ);
            if (!pred->next[level].compare_exchange_weak(curr, unmarked_succ)) {
              restart_traversal = true;
              break;
            }
            curr = get_unmarked_ref(pred->next[level].load());
            succ = (curr == tail) ? tail : curr->next[level].load();
          }
          
          if (restart_traversal) break;
          if (curr != tail && curr->key < key) {
            pred = curr;
            curr = get_unmarked_ref(succ);
          } else {
            break;
          }
        }
        if (restart_traversal) break;
        if (preds) preds[level] = pred;
        if (succs) succs[level] = curr;
      }
      if (restart_traversal) {
        continue;
      }
      return (succs[0] != tail && succs[0]->key == key);
    }
  }

public:
  LockFreeSkipList() {
    head = create_node({}, MAX_LEVEL);
    tail = create_node({}, MAX_LEVEL);
    for (int i = 0; i < MAX_LEVEL; ++i) {
      head->next[i].store(tail);
    }
  }

  ~LockFreeSkipList() {
    Node<K, V>* curr = get_unmarked_ref(head->next[0].load());
    while (curr != tail) {
      Node<K, V>* next = get_unmarked_ref(curr->next[0].load());
      curr->~Node();
      delete[] reinterpret_cast<char*>(curr);
      curr = next;
    }
    head->~Node();
    delete[] reinterpret_cast<char*>(head);
    tail->~Node();
    delete[] reinterpret_cast<char*>(tail);
  }

  auto search(const K& key) const -> std::unordered_set<V> {
    Node<K, V>* succs[MAX_LEVEL];
    if (find(key, nullptr, succs)) {
      Node<K, V>* node = succs[0];
      if (!is_marked(node->next[0].load())) {
        std::shared_lock lock(node->value_mutex);
        return node->values;
      }
    }
    return {};
  }

  auto insert(const K& key, const V& value) -> bool {
    Node<K, V>* preds[MAX_LEVEL];
    Node<K, V>* succs[MAX_LEVEL];
    while (true) {
      if (find(key, preds, succs)) {
        Node<K, V>* node = succs[0];
        if (is_marked(node->next[0].load())) {
          continue;
        }
        std::unique_lock lock(node->value_mutex);
        return node->values.insert(value).second;
      } else {
        int new_level = random_level();
        Node<K, V>* new_node = create_node(key, value, new_level);
        new_node->next[0].store(succs[0]);
        if (!preds[0]->next[0].compare_exchange_weak(succs[0], new_node)) {
          new_node->~Node();
          delete[] reinterpret_cast<char*>(new_node);
          continue;
        }
        for (int level = 1; level < new_level; ++level) {
          new_node->next[level].store(succs[level]);
          if (!preds[level]->next[level].compare_exchange_weak(succs[level], new_node)) {
            break;
          }
        }
        return true;
      }
    }
  }

  /**
   * Logically deletes the node with the given key.
   */
  auto remove(const K& key) -> bool {
    Node<K, V>* succs[MAX_LEVEL];
    if (!find(key, nullptr, succs)) {
      return false;
    }
    Node<K, V>* node_to_delete = succs[0];
    for (int level = node_to_delete->level - 1; level >= 0; --level) {
      Node<K, V>* succ = node_to_delete->next[level].load();
      while (!is_marked(succ)) {
        // atomically updates succ if CAS failed
        if (node_to_delete->next[level].compare_exchange_weak(succ, get_marked_ref(succ))) {
          break;
        }
      }
    }
    return true;
  }

  // ----- For Debugging -----
  auto display() const -> void {
    std::cout << "\n--- Skip List Structure ---\n";
    for (int level = MAX_LEVEL - 1; level >= 0; --level) {
      std::cout << "Level " << std::setw(2) << level << ": H -> ";

      Node<K, V>* curr = get_unmarked_ref(head->next[level].load());
      while (curr != tail) {
        std::cout << curr->key;

        // indicate if the node is marked for deletion
        if (is_marked(curr->next[0].load())) {
          std::cout << "(m)";
        }
        std::cout << " -> ";
        
        curr = get_unmarked_ref(curr->next[level].load());
      }
      std::cout << "T" << std::endl;
    }
    std::cout << "-------------------------\n" << std::endl;
  }
};