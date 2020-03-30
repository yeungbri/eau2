/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <map>
#include <memory>
#include "../serial.h"

/** 
 * Key of a key-value store which consists of a unique string name and
 * the home node that it exists on 
 * */
class Key
{
public:
  std::string name_; // name to refer to key
  size_t home_;      // index of home node
  Key(std::string name, size_t home) : name_(name), home_(home) {}
  ~Key() = default;

  /**
   * Serializes this key with its name first, and then its home node.
   */
  void serialize(Serializer &ser)
  {
    ser.write_string(name_);
    ser.write_size_t(home_);
  }

  /**
   * Deserializes and returns a key from a given deserializer.
   */
  static std::shared_ptr<Key> deserialize(Deserializer &dser)
  {
    std::string name = dser.read_string();
    size_t home = dser.read_size_t();
    return std::make_shared<Key>(name, home);
  }
};

/** 
 * Stores the binary representation of an object in the kv-store
 */
class Value
{
public:
  char *data_;    // serialized data
  size_t length_; // length of serialized data

  Value(char *data, size_t length) : length_(length)
  {
    data_ = new char[length];
    // Data is not guaranteed to be available i.e. across network
    memcpy(data_, data, length);
  }

  /**
   * TODO: Properly manage the memory stored by the data_ object
   */
  ~Value()
  {
    //delete[] data_;
  }

  /** Gets a pointer to the data stored. */
  char *data() { return data_; }

  /** Length of the data stored. */
  size_t length() { return length_; }

  /**
   * Serializes the data in this value with the size first, and then the data
   */
  void serialize(Serializer &ser)
  {
    ser.write_size_t(length_);
    ser.write_chars(data_, length_);
  }

  /**
   * Returns a pointer to the value deserialized.
   */
  static std::shared_ptr<Value> deserialize(Deserializer &dser)
  {
    size_t len = dser.read_size_t();
    char *data = dser.read_chars(len);
    return std::make_shared<Value>(data, len);
  }
};