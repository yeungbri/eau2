/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <map>

#include "../serial.h"

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

  void serialize(Serializer ser) {
    ser.write_string(name_);
    ser.write_size_t(home_);
  }

  static std::shared_ptr<Key> deserialize(Deserializer dser) {
    std::string name = dser.read_string();
    size_t home = dser.read_size_t();
    return std::make_shared<Key>(name, home);
  }
};

/** Stores the binary representation of an object in the kv-store */
class Value {
 public:
  char* data_;     // serialized data
  size_t length_;  // length of serialized data

  Value(char* data, size_t length) : length_(length) {
    data_ = new char[length];
    // Data is not guaranteed to be available i.e. across network
    memcpy(data_, data, length);
  }

  ~Value() { delete[] data_; }

  char* data() { return data_; }

  size_t length() { return length_; }

  void serialize(Serializer ser) {
    ser.write_size_t(length_);
    ser.write_chars(data_, length_);
  }

  static std::shared_ptr<Value> deserialize(Deserializer dser) {
    size_t len = dser.read_size_t();
    char* data = dser.read_chars(len);
    return std::make_shared<Value>(data, len);
  }
};