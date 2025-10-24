#include <barrier>
#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <thread>
#include <vector>

#include "iindex.hpp"
#include "skip_list/lock_free_skip_list.hpp"
#include "inverted_index/inverted_index.hpp"
#include "b_tree/concurrent_bplus_tree.hpp"


enum class IndexType {
  SKIP_LIST,
  INVERTED_INDEX,
  BPLUS_TREE,
};

enum class OpType {
  GET,
  PUT,
  SCAN,
};

struct Operation {
  OpType type;
  std::string key;
  std::string value;
  int range;
};

class SizeParser {
public:
  static size_t parseSize(const std::string& sizeStr) {
    if (sizeStr.empty()) {
      throw std::invalid_argument("Empty size string");
    }

    // Convert to lowercase for case-insensitive comparison
    std::string str = sizeStr;
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);

    // Find where the number ends and unit begins
    size_t numEnd = 0;
    while (numEnd < str.length() && (std::isdigit(str[numEnd]) || str[numEnd] == '.')) {
      numEnd++;
    }

    if (numEnd == 0) {
      throw std::invalid_argument("No number found in size string");
    }

    // Parse the number part
    double value = std::stod(str.substr(0, numEnd));
    if (value < 0) {
      throw std::invalid_argument("Size cannot be negative");
    }

    // Parse the unit part
    std::string unit = str.substr(numEnd);
    
    // Remove any whitespace
    unit.erase(std::remove_if(unit.begin(), unit.end(), ::isspace), unit.end());

    size_t bytes = 0;
    if (unit.empty() || unit == "b" || unit == "bytes") {
      bytes = static_cast<size_t>(value);
    } else if (unit == "kb" || unit == "k") {
      bytes = static_cast<size_t>(value * 1024);
    } else if (unit == "mb" || unit == "m") {
      bytes = static_cast<size_t>(value * 1024 * 1024);
    } else {
      throw std::invalid_argument("Unknown unit: " + unit + ". Supported units: B, KB, MB");
    }

    return bytes;
  }
};

class IndexParser {
public:
  static auto parseDataStructure(const std::string& dsStr) -> IndexType {
    std::string str = dsStr;
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    
    if (str == "skip-list") {
      return IndexType::SKIP_LIST;
    } else if (str == "inverted-index") {
      return IndexType::INVERTED_INDEX;
    } else if (str == "bplus-tree") {
      return IndexType::BPLUS_TREE;
    } else {
      throw std::invalid_argument("Unknown data structure: " + dsStr + ". Supported: LockFreeSkipList, InvertedIndex, BPlusTree");
    }
  }
};

class Parser {
private:
  static auto generateRandomString(const std::string& prefix, size_t length) -> std::string {
    // static std::random_device rd;
    thread_local std::mt19937 rng(1);

    const std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<> dis(0, chars.size() - 1);

    std::string result = prefix;
    result.reserve(length);

    for (size_t i = prefix.length(); i < length; ++i) {
      result += chars[dis(rng)];
    }
    
    return result;
  }

  static auto generateString(const std::string& prefix, size_t length) -> std::string {
    std::string result = prefix;
    const char padding_char = '*';

    result.resize(length, padding_char);
    return result;
  }

public:
  static auto parseFile(const std::string& fileName, size_t keySize, size_t valueSize) -> std::vector<Operation> {
    std::vector<Operation> operations;
    std::ifstream file(fileName);
    std::string line;
    
    std::regex getPattern(R"delim(query\(GET\("([^"]+)"\)\))delim");
    std::regex putPattern(R"delim(query\(PUT\("([^"]+)","([^"]+)"\)\))delim");
    std::regex rangePattern(R"delim(query\(SCAN\("([^"]+)","([^"]+)"\)\))delim");

    std::string fixedValue(valueSize, '*');

    while(std::getline(file, line)) {
      std::smatch match;

      if(std::regex_match(line, match, putPattern)) {
        std::string randomKey = generateString(match[1], keySize);
        operations.push_back({OpType::PUT, randomKey, fixedValue});
      } else if(std::regex_match(line, match, getPattern)) {
        std::string randomKey = generateString(match[1], keySize);
        operations.push_back({OpType::GET, randomKey});
      } else if(std::regex_match(line, match, rangePattern)) {
        std::string randomKey = generateString(match[1], keySize);
        operations.push_back({OpType::SCAN, randomKey, {}, std::stoi(match[2])});
      }
    }
    return operations;
  }
};

auto workerThread(int threadId, int numThreads, 
                const std::vector<Operation>& operations, IIndex<>& index, 
                std::atomic<size_t>& totalOps, std::barrier<>& startBarrier, std::barrier<>& endBarrier,
                const std::vector<std::string>& sorted_keys,
                const std::unordered_map<std::string, size_t>& key_to_index_map
              ) -> void {
  
  // wait for all threads to be ready
  startBarrier.arrive_and_wait();

  for(size_t i = threadId; i < operations.size(); i += numThreads) {
    const Operation& op = operations[i];
    
    if(op.type == OpType::PUT) {
      index.insert(op.key, op.value);
    } else if(op.type == OpType::GET) {
      index.search(op.key);
    } 
    
    else if(op.type == OpType::SCAN) {
        if (auto* btree = dynamic_cast<BPlusTree<>*>(&index)) {
          auto it = key_to_index_map.find(op.key);
          if (it != key_to_index_map.end()) {
            size_t start_idx = it->second;
            size_t end_idx = start_idx + op.range;

            if (end_idx < sorted_keys.size()) {
              const std::string& end_key = sorted_keys[end_idx];
              btree->range_search(op.key, end_key);
            }
          }
       }
    }

    totalOps.fetch_add(1);
  }

  // signal completion
  endBarrier.arrive_and_wait();
}

class Runner {
public:
  std::unique_ptr<IIndex<>> index;
  std::vector<std::string> sorted_keys; // for SCAN operations
  std::unordered_map<std::string, size_t> key_to_index_map; // for SCAN operations

public:
  Runner(IndexType type) {
    switch (type) {
    case IndexType::SKIP_LIST:
      index = std::make_unique<LockFreeSkipList<>>();
      break;
    case IndexType::INVERTED_INDEX:
      index = std::make_unique<InvertedIndex<>>();
      break;
    case IndexType::BPLUS_TREE:
      index = std::make_unique<BPlusTree<>>();
      break;
    default:
      throw std::invalid_argument("Unsupported data structure type");
    }
  }

  auto loadPhase(const std::string& loadFile, size_t keySize, size_t valueSize) -> void {
    std::cout << "=== Load Phase ===" << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    auto operations = Parser::parseFile(loadFile, keySize, valueSize);
    std::cout << "Parsed " << operations.size() << " operations" << std::endl;

    std::vector<std::string> generated_keys;
    generated_keys.reserve(operations.size());
    for(const auto& op: operations) {
      if(op.type == OpType::PUT) {
        index->insert(op.key, op.value);
        
        // for SCAN
        generated_keys.push_back(op.key);
      }
    }

    // for SCAN
    this->sorted_keys = generated_keys;
    std::sort(this->sorted_keys.begin(), this->sorted_keys.end());

    this->key_to_index_map.reserve(this->sorted_keys.size());
    for (size_t i = 0; i < this->sorted_keys.size(); ++i) {
      this->key_to_index_map[this->sorted_keys[i]] = i;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Load phase completed in " << duration.count() << " ms" << std::endl;
    std::cout << "Sample key: " << operations[0].key << std::endl;
    std::cout << "Sample value: " << operations[0].value << std::endl;
    std::cout << std::endl;
  }

  auto runPhase(const std::string& runFile, int numThreads, size_t keySize, size_t valueSize) -> void {
    std::cout << "=== Run Phase (Threads: " << numThreads << ") ===" << std::endl;
    
    auto operations = Parser::parseFile(runFile, keySize, valueSize);
    std::cout << "Parsed " << operations.size() << " operations" << std::endl;

    std::atomic<size_t> totalOps(0);
    std::vector<std::thread> threads;

    // Create barriers for synchronization
    std::barrier startBarrier(numThreads + 1);
    std::barrier endBarrier(numThreads + 1);
    
    
    for(size_t i = 0;i < numThreads; i++) {
      threads.emplace_back(workerThread, i, numThreads, 
                        std::ref(operations), std::ref(*index), std::ref(totalOps),
                        std::ref(startBarrier), std::ref(endBarrier),
                        std::ref(this->sorted_keys),
                        std::ref(this->key_to_index_map)
                      );
    }
    
    // Signal all threads to start
    startBarrier.arrive_and_wait();
    
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "Start Run Phase!\n";
    auto start = std::chrono::high_resolution_clock::now();
    
    // Wait for all threads to complete
    endBarrier.arrive_and_wait();
    auto end = std::chrono::high_resolution_clock::now();

    for(auto& t: threads) {
      t.join();
    }
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    double seconds = duration.count() / 1000000.0;
    double throughput = totalOps.load() / seconds;

    std::cout << "Execution time: " << seconds << " seconds" << std::endl;
    std::cout << "Total operations: " << totalOps.load() << std::endl;
    std::cout << "Throughput: " << throughput << " ops/sec" << std::endl;
    std::cout << std::endl;
  }
};

auto main(int argc, char const *argv[]) -> int {
  if (argc != 7) {
    std::cerr << "Usage: " << argv[0] << " <load_file> <run_file> <data_structure> <num_threads> <key_size> <value_size>" << std::endl;
    std::cerr << "  data_structure: skip-list, inverted-index, bplus-tree" << std::endl;
    std::cerr << "  num_threads: 1, 4, 8, or 16" << std::endl;
    std::cerr << "  key_size: 64B, 256B" << std::endl;
    std::cerr << "  value_size: 64B, 256B, 1KB, 4KB" << std::endl;

    return 1;
  }

  std::string loadFile = argv[1];
  std::string runFile = argv[2];
  int numThreads = std::stoi(argv[4]);
  IndexType indexType = IndexParser::parseDataStructure(argv[3]);
  size_t keySize = SizeParser::parseSize(argv[5]);
  size_t valueSize = SizeParser::parseSize(argv[6]);

  if (numThreads != 1 && numThreads != 4 && numThreads != 8 && numThreads != 16) {
    std::cerr << "Error: num_threads must be 1, 4, 8, or 16" << std::endl;
    return 1;
  }

  Runner runner(indexType);

  runner.loadPhase(loadFile, keySize, valueSize);
  runner.runPhase(runFile, numThreads, keySize, valueSize);
  
  return 0;
}


/*
auto main(int argc, char const *argv[]) -> int {
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <load_file> <run_file> <data_structure>" << std::endl;
    std::cerr << "  data_structure: skip-list, inverted-index, bplus-tree" << std::endl;
    return 1;
  }

  std::string loadFile = argv[1];
  std::string runFile = argv[2];
  IndexType indexType = IndexParser::parseDataStructure(argv[3]);

  // Define all configurations to test
  std::string keySize = "64B";
  std::vector<int> threadCounts = {1, 4, 8, 16};
  std::vector<std::string> valueSizes = {"64B", "256B", "1KB", "4KB"};

  std::cout << "=== Evaluation - All Configurations ===" << std::endl;
  std::cout << std::endl;

  // Run all configurations
  for (int numThreads : threadCounts) {
    for (const std::string& valueSizeStr : valueSizes) {
      try {
        size_t keySizeBytes = SizeParser::parseSize(keySize);
        size_t valueSizeBytes = SizeParser::parseSize(valueSizeStr);

        std::cout << ">>> Configuration: " << numThreads << " threads, " 
                  << keySize << " key, " << valueSizeStr << " value <<<" << std::endl;

        Runner runner(indexType);

        runner.loadPhase(loadFile, keySizeBytes, valueSizeBytes);
        runner.runPhase(runFile, numThreads, keySizeBytes, valueSizeBytes);
        
        std::cout << "========================================" << std::endl;
        std::cout << std::endl;

      } catch (const std::exception& e) {
        std::cerr << "Error with threads=" << numThreads 
                  << ", valueSize=" << valueSizeStr << ": " << e.what() << std::endl;
      }
    }
  }
  
  return 0;
}
*/
