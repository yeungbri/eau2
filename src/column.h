/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include "serial.h"
#include <iostream>
#include <vector>
#include "kvstore/kvstore.h"
#include "chunk.h"
#include <cmath>

class IntColumn;
class BoolColumn;
class DoubleColumn;
class StringColumn;

const size_t MAX_CHUNK_SIZE = 2;

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. */
class Column {
public:
  std::vector<Key> keys_;
  size_t sz_;

  Column() {
    sz_ = 0;
  }

  virtual ~Column() = default;

  /** Returns the number of elements in the column. */
  virtual size_t size() {
    return sz_;
  }

  /** Type converters: Return same column under its actual type, or
   *  nullptr if of the wrong type.  */
  virtual std::shared_ptr<IntColumn> as_int() { return nullptr; }
  virtual std::shared_ptr<BoolColumn> as_bool() { return nullptr; }
  virtual std::shared_ptr<DoubleColumn> as_double() { return nullptr; }
  virtual std::shared_ptr<StringColumn> as_string() { return nullptr; }

  /** Type appropriate push_back methods. Calling the wrong method is
   * undefined behavior. **/
  virtual void push_back(int val, std::shared_ptr<KVStore> store) {};
  virtual void push_back(bool val, std::shared_ptr<KVStore> store) {};
  virtual void push_back(double val, std::shared_ptr<KVStore> store) {};
  virtual void push_back(std::string val, std::shared_ptr<KVStore> store) {};

  /** Return the type of this column as a char: 'S', 'B', 'I' and 'D'.*/
  virtual char get_type() = 0;

  virtual void serialize(Serializer& ser) { }

  virtual std::string gen_name_()
  {
    const int len = 100;
    std::string ret = "";
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        ret += alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return ret;
  }
};

/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 */
class BoolColumn : public Column {
public:
  std::vector<bool> cached_chunk_;

public:
  BoolColumn() = default;

  BoolColumn(std::vector<Key> keys, std::vector<bool> cache) {
    keys_ = keys;
    cached_chunk_ = cache;
  }

  virtual ~BoolColumn() { keys_.clear(); }

  /**
   * Given absolute idx value, return the value in cache if it exists. Else,
   * query the KVStore for the correct chunk, and retrieve the value there.
   */
  bool get(size_t idx, std::shared_ptr<KVStore> store) {
    assert(idx < sz_);
    size_t chunk_idx = idx / MAX_CHUNK_SIZE;
    size_t element_idx = idx % MAX_CHUNK_SIZE;
    if (chunk_idx == keys_.size())
    {
      std::cout << "chunk idx is: " << chunk_idx << ", keys size is" << keys_.size() << std::endl;
      std::cout << "helloooo" << std::endl;
      return cached_chunk_.at(element_idx);
    } else
    {
      std::cout << "in else case!" << std::endl;
      Value v = store->get(keys_.at(chunk_idx));
      std::cout << "got value" << std::endl;
      Deserializer dser(v.data(), v.length());
      std::cout << "deserialized" << std::endl;
      auto bcc = BoolColumnChunk::deserialize(dser);
      std::cout << "got the chunk" << std::endl;
      return bcc->get(element_idx);
    }
  }

  std::shared_ptr<BoolColumn> as_bool() { return std::shared_ptr<BoolColumn>(this); }

  virtual char get_type() { return 'B'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(bool b, std::shared_ptr<KVStore> store) {
    std::cout << "about to push back: " << b << std::endl;
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      BoolColumnChunk chunk(cached_chunk_);
      Serializer ser;
      chunk.serialize(ser);
      std::string keyName = gen_name_();
      size_t node = (sz_ / MAX_CHUNK_SIZE) % store->num_nodes();
      auto k = std::make_shared<Key>(keyName, node);
      auto v = std::make_shared<Value>(ser.data(), ser.length());
      store->put(*k, *v);
      keys_.push_back(*k);
      std::cout << "serialized!" << std::endl;
      cached_chunk_.clear();
    } else
    {
      cached_chunk_.push_back(b);
    }
    sz_++;
  }
  
  void serialize(Serializer &ser) {
    ser.write_size_t(keys_.size());
    for (size_t i=0; i<keys_.size(); i++) {
      keys_.at(i).serialize(ser);
    }
    ser.write_bool_vector(cached_chunk_);
  }

  static std::shared_ptr<BoolColumn> deserialize(Deserializer &dser) {
    size_t num_chunks = dser.read_size_t();
    std::vector<Key> arr;
    for (size_t i=0; i<num_chunks; i++) {
      arr.push_back(*Key::deserialize(dser));
    }
    std::vector<bool> cache = dser.read_bool_vector();
    return std::make_shared<BoolColumn>(arr, cache);
  }
};

/*************************************************************************
 * IntColumn::
 * Holds int values.
 */
class IntColumn : public Column {
public:
  std::vector<int> cached_chunk_;

public:
  IntColumn() = default;

  IntColumn(std::vector<Key> keys, std::vector<int> cache) {
    keys_ = keys;
    cached_chunk_ = cache;
  }

  virtual ~IntColumn() { keys_.clear(); }

  /**
   * Given absolute idx value, return the value in cache if it exists. Else,
   * query the KVStore for the correct chunk, and retrieve the value there.
   */
  int get(size_t idx, std::shared_ptr<KVStore> store) {
    assert(idx < sz_);
    size_t chunk_idx = idx / MAX_CHUNK_SIZE;
    size_t element_idx = idx % MAX_CHUNK_SIZE;
    if (chunk_idx == keys_.size())
    {
      return cached_chunk_.at(element_idx);
    } else
    {
      Value v = store->get(keys_.at(chunk_idx));
      Deserializer dser(v.data(), v.length());
      auto chunk = IntColumnChunk::deserialize(dser);
      return chunk->get(element_idx);
    }
  }

  std::shared_ptr<IntColumn> as_int() { return std::shared_ptr<IntColumn>(this); }

  virtual char get_type() { return 'I'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(int i, std::shared_ptr<KVStore> store) {
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      IntColumnChunk chunk(cached_chunk_);
      Serializer ser;
      chunk.serialize(ser);
      std::string keyName = gen_name_();
      size_t node = (sz_ / MAX_CHUNK_SIZE) % store->num_nodes();
      auto k = std::make_shared<Key>(keyName, node);
      auto v = std::make_shared<Value>(ser.data(), ser.length());
      store->put(*k, *v); // TODO: may need to allocate k and v on heap
      cached_chunk_.clear();
    }
    cached_chunk_.push_back(i);
    sz_++;
  }
  
  void serialize(Serializer &ser) {
    ser.write_size_t(keys_.size());
    for (size_t i=0; i<keys_.size(); i++) {
      keys_.at(i).serialize(ser);
    }
    ser.write_int_vector(cached_chunk_);
  }

  static std::shared_ptr<IntColumn> deserialize(Deserializer &dser) {
    size_t num_chunks = dser.read_size_t();
    std::vector<Key> arr;
    for (size_t i=0; i<num_chunks; i++) {
      arr.push_back(*Key::deserialize(dser));
    }
    std::vector<int> cache = dser.read_int_vector();
    return std::make_shared<IntColumn>(arr, cache);
  }
};

/*************************************************************************
 * DoubleColumn::
 * Holds double values.
 */
class DoubleColumn : public Column {
public:
  std::vector<double> cached_chunk_;

public:
  DoubleColumn() = default;

  DoubleColumn(std::vector<Key> keys, std::vector<double> cache) {
    keys_ = keys;
    cached_chunk_ = cache;
  }

  virtual ~DoubleColumn() { keys_.clear(); }

  /**
   * Given absolute idx value, return the value in cache if it exists. Else,
   * query the KVStore for the correct chunk, and retrieve the value there.
   */
  double get(size_t idx, std::shared_ptr<KVStore> store) {
    assert(idx < sz_);
    size_t chunk_idx = idx / MAX_CHUNK_SIZE;
    size_t element_idx = idx % MAX_CHUNK_SIZE;
    if (chunk_idx == keys_.size())
    {
      return cached_chunk_.at(element_idx);
    } else
    {
      Value v = store->get(keys_.at(chunk_idx));
      Deserializer dser(v.data(), v.length());
      auto chunk = DoubleColumnChunk::deserialize(dser);
      return chunk->get(element_idx);
    }
  }

  std::shared_ptr<DoubleColumn> as_double() { return std::shared_ptr<DoubleColumn>(this); }

  virtual char get_type() { return 'D'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(double d, std::shared_ptr<KVStore> store) {
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      DoubleColumnChunk chunk(cached_chunk_);
      Serializer ser;
      chunk.serialize(ser);
      std::string keyName = gen_name_();
      size_t node = (sz_ / MAX_CHUNK_SIZE) % store->num_nodes();
      auto k = std::make_shared<Key>(keyName, node);
      auto v = std::make_shared<Value>(ser.data(), ser.length());
      store->put(*k, *v);
      cached_chunk_.clear();
    }
    cached_chunk_.push_back(d);
    sz_++;
  }
  
  void serialize(Serializer &ser) {
    ser.write_size_t(keys_.size());
    for (size_t i=0; i<keys_.size(); i++) {
      keys_.at(i).serialize(ser);
    }
    ser.write_double_vector(cached_chunk_);
  }

  static std::shared_ptr<DoubleColumn> deserialize(Deserializer &dser) {
    size_t num_chunks = dser.read_size_t();
    std::vector<Key> arr;
    for (size_t i=0; i<num_chunks; i++) {
      arr.push_back(*Key::deserialize(dser));
    }
    std::vector<double> cache = dser.read_double_vector();
    return std::make_shared<DoubleColumn>(arr, cache);
  }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
class StringColumn : public Column {
public:
  std::vector<std::string> cached_chunk_;

public:
  StringColumn() = default;

  StringColumn(std::vector<Key> keys, std::vector<std::string> cache) {
    keys_ = keys;
    cached_chunk_ = cache;
  }

  virtual ~StringColumn() { keys_.clear(); }

  /**
   * Given absolute idx value, return the value in cache if it exists. Else,
   * query the KVStore for the correct chunk, and retrieve the value there.
   */
  std::string get(size_t idx, std::shared_ptr<KVStore> store) {
    assert(idx < sz_);
    size_t chunk_idx = idx / MAX_CHUNK_SIZE;
    size_t element_idx = idx % MAX_CHUNK_SIZE;
    if (chunk_idx == keys_.size())
    {
      return cached_chunk_.at(element_idx);
    } else
    {
      Value v = store->get(keys_.at(chunk_idx));
      Deserializer dser(v.data(), v.length());
      auto chunk = StringColumnChunk::deserialize(dser);
      return chunk->get(element_idx);
    }
  }

  std::shared_ptr<StringColumn> as_string() { return std::shared_ptr<StringColumn>(this); }

  virtual char get_type() { return 'S'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(std::string s, std::shared_ptr<KVStore> store) {
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      StringColumnChunk chunk(cached_chunk_);
      Serializer ser;
      chunk.serialize(ser);
      std::string keyName = gen_name_();
      size_t node = (sz_ / MAX_CHUNK_SIZE) % store->num_nodes();
      auto k = std::make_shared<Key>(keyName, node);
      auto v = std::make_shared<Value>(ser.data(), ser.length());
      store->put(*k, *v); // TODO: may need to allocate k and v on heap
      cached_chunk_.clear();
    }
    cached_chunk_.push_back(s);
    sz_++;
  }
  
  void serialize(Serializer &ser) {
    ser.write_size_t(keys_.size());
    for (size_t i=0; i<keys_.size(); i++) {
      keys_.at(i).serialize(ser);
    }
    ser.write_string_vector(cached_chunk_);
  }

  static std::shared_ptr<StringColumn> deserialize(Deserializer &dser) {
    size_t num_chunks = dser.read_size_t();
    std::vector<Key> arr;
    for (size_t i=0; i<num_chunks; i++) {
      arr.push_back(*Key::deserialize(dser));
    }
    std::vector<std::string> cache = dser.read_string_vector();
    return std::make_shared<StringColumn>(arr, cache);
  }
};
