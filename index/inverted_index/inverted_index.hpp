#pragma once

#include <unordered_set>
#include <unordered_map>
#include <string>
#include <shared_mutex>
#include <mutex>
#include <memory>
#include <vector>
#include <array>

#include "../iindex.hpp"

template<
  typename K = std::string,
  typename V = std::string,
  size_t NUM_SHARDS = 256
>
class InvertedIndex: public IIndex<K, V>  {
private:
  
  struct Bucket {
    std::unordered_set<V> values;
    mutable std::shared_mutex mutex;
  };

  // reduce lock contention by sharding
  struct Shard {
    std::unordered_map<K, std::shared_ptr<Bucket>> index; // thread unsafe w/o locking
    mutable std::shared_mutex mutex;

    Shard() {
      // reduces rehashes
      index.reserve(10000000 / NUM_SHARDS);
    }
  };

  std::array<Shard, NUM_SHARDS> shards;

  auto getShard(const K& key) -> Shard& {
    return shards[std::hash<K>{}(key) % NUM_SHARDS];
  }

  auto getShard(const K& key) const -> const Shard& {
    return shards[std::hash<K>{}(key) % NUM_SHARDS];
  }

  auto getOrCreateBucket(const K& key) -> std::shared_ptr<Bucket> {
    auto& shard = getShard(key);

    // optimistic
    {
      std::shared_lock<std::shared_mutex> shard_lock(shard.mutex);
      auto it = shard.index.find(key);
      if (it != shard.index.end()) {
        return it->second;
      }
    }

    std::unique_lock<std::shared_mutex> shard_lock(shard.mutex);
    // double check
    auto it = shard.index.find(key);
    if (it != shard.index.end()) {
      return it->second;
    }

    auto bucket = std::make_shared<Bucket>();
    auto [inserted_it, success] = shard.index.try_emplace(key, bucket);
    return inserted_it->second;
  }

public:

  auto insert(const K& key, const V& value) -> bool {
    auto bucket = getOrCreateBucket(key);

    std::unique_lock<std::shared_mutex> bucket_lock(bucket->mutex);
    auto [it, inserted] = bucket->values.insert(value);

    return inserted;
  }

  auto search(const K& key) const -> std::unordered_set<V> {
    const auto& shard = getShard(key);

    std::shared_lock<std::shared_mutex> shard_lock(shard.mutex);

    auto it = shard.index.find(key);
    if (it == shard.index.end()) {
      return {}; 
    }

    auto bucket = it->second;
    std::shared_lock<std::shared_mutex> bucket_lock(bucket->mutex);

    return bucket->values;
  }

  auto remove(const K& key) -> bool {
    auto& shard = getShard(key);
    
    std::unique_lock<std::shared_mutex> shard_lock(shard.mutex);

    auto it = shard.index.find(key);
    if (it == shard.index.end()) {
      return false;
    }

    shard.index.erase(it);
    return true;
  }

  auto remove(const K& key, const V& value) -> bool {
    auto& shard = getShard(key);

    std::unique_lock<std::shared_mutex> shard_lock(shard.mutex);
    
    auto it = shard.index.find(key);
    if (it == shard.index.end()) return false;
    
    auto bucket = it->second;
    std::unique_lock<std::shared_mutex> bucket_lock(bucket->mutex);

    bool removed = bucket->values.erase(value) > 0;

    if (removed && bucket->values.empty()) {
      bucket_lock.unlock();
      shard.index.erase(it);
    }
    return removed;
  }
};
