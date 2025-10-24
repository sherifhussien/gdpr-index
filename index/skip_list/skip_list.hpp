#pragma once

#include <algorithm>
#include <random>
#include <iostream>
#include <functional>
#include <memory>
#include <set>
#include <vector>

template<typename K, typename V>
class SkipListNode {
public:
    using NodePtr = std::shared_ptr<SkipListNode<K, V>>;
    using ValueList = std::vector<V>;

protected:
    K key;
    ValueList values;
    std::vector<NodePtr> forward;
    int level;

public:
    SkipListNode(const K& k, int lvl) : key(k), level(lvl), forward(lvl + 1, nullptr) {}
    virtual ~SkipListNode() = default;

    const K& get_key() const { return key; }
    
    // ==== Value management ====
    auto add_value(const V& value) -> void {
        // *it >= value
        auto it = std::lower_bound(values.begin(), values.end(), value);
        values.insert(it, value);
    }
    
    auto remove_value(const V& value) -> bool {
        // *it >= value
        auto it = std::lower_bound(values.begin(), values.end(), value);
        if (it != values.end() && *it == value) {
            values.erase(it);
            return true;
        }
        return false;
    }

    auto get_values() const -> const ValueList& { return values; }
    auto has_values() const -> bool { return !values.empty(); }
    auto get_value_count() const -> size_t { return values.size(); }

    // ==== Forward pointer management ====
    auto get_forward(int level) const -> NodePtr {
        return (level <= this->level) ? forward[level] : nullptr;
    }
    
    auto set_forward(int level, NodePtr node) -> void{
        if (level <= this->level) {
            forward[level] = node;
        }
    }
    
    int get_level() const { return level; }
};

template<typename K, typename V>
class SkipList {
public:
    using NodeType = SkipListNode<K, V>;
    using NodePtr = std::shared_ptr<NodeType>;

protected:
    static constexpr int MAX_LEVEL = 16;
    static constexpr double P = 0.5;
    
    NodePtr header; // spans all levels
    int current_level; // current highest level
    std::mt19937 rng;
    std::uniform_real_distribution<double> dist;
    
    virtual auto random_level() -> int {
        int level = 0;
        while (level < MAX_LEVEL && dist(rng) < P) {
            level++;
        }
        return level;
    }
    
    virtual auto createNode(const K& key, int level) -> NodePtr {
        return std::make_shared<NodeType>(key, level);
    }
    
    auto find_predecessors(const K& key) const -> std::vector<NodePtr> {
        std::vector<NodePtr> update(MAX_LEVEL + 1);
        NodePtr current = header;
        
        for (int i = current_level; i >= 0; i--) {
            while (current->get_forward(i) && 
                   (current->get_forward(i)->get_key() < key)) {
                current = current->get_forward(i);
            }
            update[i] = current;
        }
        return update;
    }

    auto find_node(const K& key) const -> NodePtr {
        NodePtr current = header;
        for (int i = current_level; i >= 0; i--) {
            while (current->get_forward(i) && 
                    (current->get_forward(i)->get_key() < key)) {
                current = current->get_forward(i);
            }
        }
        
        current = current->get_forward(0);
        return (current && (current->get_key() == key)) 
                ? current : nullptr;
    }

public:
    SkipList() : current_level(0), rng(0), dist(0.0, 1.0) { // rng(std::random_device{}())
        header = createNode(K{}, MAX_LEVEL);
    }
    
    virtual ~SkipList() = default;

    // ==========  Insert Operation ==========

    virtual auto insert(const K& key, const V& value) -> bool {
        auto update = find_predecessors(key);
        NodePtr current = update[0]->get_forward(0);
        
        if (current && (current->get_key() == key)) {
            current->add_value(value);
            return true;
        }
        
        int newLevel = random_level();
        if (newLevel > current_level) {
            for (int i = current_level + 1; i <= newLevel; i++) {
                update[i] = header;
            }
            current_level = newLevel;
        }
        
        NodePtr newNode = createNode(key, newLevel);
        newNode->add_value(value);
        
        // Update forward pointers
        for (int i = 0; i <= newLevel; i++) {
            newNode->set_forward(i, update[i]->get_forward(i));
            update[i]->set_forward(i, newNode);
        }
        
        return true;
    }
    
    // ==========  Search Operation ==========
    
    virtual auto search(const K& key) const -> std::vector<V> {
        NodePtr current = find_node(key);

        return current ? current->get_values() : std::vector<V>{};
    }
    
    // ==========  Remove Operation ==========
    
    // For thesis (performance)
    virtual auto remove(const K& key) -> bool {
        auto update = find_predecessors(key);
        NodePtr current = update[0]->get_forward(0);
        
        if (!current || (current->get_key() != key)) {
            return false;
        }
        
        for (int i = 0; i <= current_level; i++) {
            if (update[i]->get_forward(i) == current) {
                update[i]->set_forward(i, current->get_forward(i));
            }
        }
        
        while (current_level > 0 && !header->get_forward(current_level)) {
            current_level--;
        }
        
        return true;
    }
    
    virtual auto remove_value(const K& key, const V& value) -> bool {
        NodePtr current = find_node(key);
        if (!current) {
            return false;
        }

        bool removed = current->remove_value(value);

        if (!current->has_values()) {
            remove(key);
        }
        
        return removed;
    }
    
    // ========== Utility methods ==========
    
    virtual bool contains(const K& key) const {
        return find_node(key) != nullptr;
    }
    
    virtual auto size() const -> size_t {
        size_t count = 0;
        NodePtr current = header->get_forward(0);
        while (current) {
            count++;
            current = current->get_forward(0);
        }
        return count;
    }
    
    virtual auto empty() const -> bool {
        return header->get_forward(0) == nullptr;
    }
    
    virtual auto display() const -> void {
        for (int i = current_level; i >= 0; i--) {
            std::cout << "Level " << i << ": ";
            NodePtr current = header->get_forward(i);
            while (current) {
                std::cout << current->get_key() << "(" << current->get_value_count() << ") ";
                current = current->get_forward(i);
            }
            std::cout << std::endl;
        }
    }
};