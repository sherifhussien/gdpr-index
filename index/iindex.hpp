#pragma once

#include <unordered_set>

template<
    typename K = std::string,
    typename V = std::string
>
class IIndex {
public:
  virtual auto insert(const K& key, const V& value) -> bool = 0;
  virtual auto search(const K& key) const -> std::unordered_set<V> = 0;
  
  virtual ~IIndex() = default;
};