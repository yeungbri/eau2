/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once
#include "dataframe.h"
#include "serial.h"
#include <map>

class Key {
public:
  std::string name_; // name to refer to key
  size_t home_;      // index of home node
  Key(std::string name, size_t home) : name_(name), home_(home) { }
};

class Value {
public:
  char* data_;
  size_t length_;

  Value(char* data, size_t length) : data_(data), length_(length) { }

  char* data() {
    return data_;
  }

  size_t length() {
    return length_;
  }
};

class KVStore {
public:
  std::map<Key, Value> store_;

  KVStore() {

  }

  DataFrame* get_dataframe(Key k) {
    Value v = get(k);
    Deserializer d(v.data(), v.length());
    DataFrame* res = new DataFrame(d);
    return res;
  }

  Value get(Key k) {
    auto search = store_.find(k);
    if (search != store_.end()) {
      return search->second;
    } else {
      std::cout << "Cannot get key " << k.name_ << "\n";
    }
  }

  void put(Key k, DataFrame* df) {
    Serializer ser;
    df->serialize(ser);
    put(k, Value(ser.data(), ser.length()));
  }

  void put(Key k, Value v) {
    store_.insert_or_assign(k, v);
  }

  Value waitAndGet(Key k) {

  }
};