/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once
#include "serial.h"
#include <map>

class Key
{
public:
  std::string name_; // name to refer to key
  size_t home_;      // index of home node
  Key(std::string name, size_t home) : name_(name), home_(home) { }

  bool operator==(const Key& other)
  {
    return name_ == other.name_ && home_ == other.home_;
  }
};

class Value {
public:
  char* data_;

  Value(char* data) : data_(data) { }

  char* data() {
    return data_;
  }
};

struct KeyCompare
{
   bool operator() (const Key& lhs, const Key& rhs) const
   {
       return lhs.home_ < rhs.home_;
   }
};


class KVStore {
public:
  std::map<Key, Value, KeyCompare> store_;

  KVStore() {

  }

  Value get(Key& k) {
    auto search = store_.find(k);
    if (search != store_.end()) {
      return search->second;
    } else {
      std::cout << "Cannot get key " << k.name_ << "\n";
    }
  }

  void put(Key& k, Value& v) {
    store_.insert_or_assign(k, v);
  }
};