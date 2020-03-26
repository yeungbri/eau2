/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <map>
#include "serial.h"

/** Key of a key-value store which consists of a name and 
 * the home node that it exists on */
class Key {
 public:
  std::string name_;  // name to refer to key
  size_t home_;       // index of home node
  Key(std::string name, size_t home) : name_(name), home_(home) {}

  // TODO: explain what this is doing
  /** Comparator for using Key in a std::map */
  bool operator==(const Key& other) {
    return name_ == other.name_ && home_ == other.home_;
  }
};

/** Stores the binary representation of an object in the kv-store */
class Value {
 public:
  char* data_;    // serialized data
  size_t length_; // length of serialized data

  Value(char* data, size_t length) : length_(length) {
    data_ = new char[length];
    // Data is not guaranteed to be available i.e. across network
    memcpy(data_, data, length);
  }

  ~Value() {
    delete[] data_;
  }

  char* data() { return data_; }

  size_t length() { return length_; }
};

// TODO: explain what this is doing
/** Used for comparing keys in a std::map */
struct KeyCompare {
  bool operator()(const Key& lhs, const Key& rhs) const {
    return lhs.home_ < rhs.home_;
  }
};

/** Key Value Store - users can associate keys with values and
 * retrieve them */
class KVStore {
 public:
  std::map<Key, Value, KeyCompare> store_;

  KVStore() {}

  /** Retrieves the associated value given the key */
  Value get(Key& k) {
    auto search = store_.find(k);
    if (search != store_.end()) {
      return search->second;
    } else {
      std::cout << "Cannot get key " << k.name_ << "\n";
    }
  }

  /** Associates the given value with the given key */
  void put(Key& k, Value& v) { store_.insert_or_assign(k, v); }
};