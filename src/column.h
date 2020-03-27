/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include "serial.h"
#include <iostream>
#include <vector>
#include "kvstore.h"
#include "chunk.h"
#include <cmath>

class IntColumn;
class BoolColumn;
class DoubleColumn;
class StringColumn;

const size_t MAX_CHUNK_SIZE = 1000;

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
  virtual IntColumn *as_int() { return nullptr; }
  virtual BoolColumn *as_bool() { return nullptr; }
  virtual DoubleColumn *as_double() { return nullptr; }
  virtual StringColumn *as_string() { return nullptr; }

  /** Type appropriate push_back methods. Calling the wrong method is
   * undefined behavior. **/
  virtual void push_back(int val) {};
  virtual void push_back(bool val) {};
  virtual void push_back(double val) {};
  virtual void push_back(std::string val) {};

  virtual void push_back_n(std::vector<int> vals) {};
  virtual void push_back_n(std::vector<bool> vals) {};
  virtual void push_back_n(std::vector<double> vals) {};
  virtual void push_back_n(std::vector<std::string> vals) {};

  /** Return the type of this column as a char: 'S', 'B', 'I' and 'D'.*/
  virtual char get_type() = 0;

  virtual void serialize(Serializer& ser) { }
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
  bool get(size_t idx, KVStore* store) {
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
      BoolColumnChunk* bcc = BoolColumnChunk::deserialize(dser);
      return bcc->get(element_idx);
    }
  }

  BoolColumn *as_bool() { return this; }

  virtual char get_type() { return 'B'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(bool b, KVStore* store) {
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      BoolColumnChunk chunk(cached_chunk_);
      Serializer ser;
      chunk.serialize(ser);
      std::string keyName = ""; // TODO: create unique key name
      size_t node = (sz_ / MAX_CHUNK_SIZE) % store->num_nodes();
      Key k(keyName, node);
      Value v(ser.data(), ser.length());
      store->put(k, v); // TODO: may need to allocate k and v on heap
      cached_chunk_.clear();
    }
    cached_chunk_.push_back(b);
    sz_++;
  }
  
  void serialize(Serializer &ser) {
    ser.write_size_t(keys_.size());
    for (size_t i=0; i<keys_.size(); i++) {
      keys_.at(i).serialize(ser);
    }
    ser.write_bool_vector(cached_chunk_);
  }

  static BoolColumn* deserialize(Deserializer &dser) {
    size_t num_chunks = dser.read_size_t();
    std::vector<Key> arr;
    for (size_t i=0; i<num_chunks; i++) {
      arr.push_back(*Key::deserialize(dser));
    }
    std::vector<bool> cache = dser.read_bool_vector();
    return new BoolColumn(arr, cache);
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
  int get(size_t idx, KVStore* store) {
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
      IntColumnChunk* chunk = IntColumnChunk::deserialize(dser);
      return chunk->get(element_idx);
    }
  }

  IntColumn *as_int() { return this; }

  virtual char get_type() { return 'I'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(int i, KVStore* store) {
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      IntColumnChunk chunk(cached_chunk_);
      Serializer ser;
      chunk.serialize(ser);
      std::string keyName = ""; // TODO: create unique key name
      size_t node = (sz_ / MAX_CHUNK_SIZE) % store->num_nodes();
      Key k(keyName, node);
      Value v(ser.data(), ser.length());
      store->put(k, v); // TODO: may need to allocate k and v on heap
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

  static IntColumn* deserialize(Deserializer &dser) {
    size_t num_chunks = dser.read_size_t();
    std::vector<Key> arr;
    for (size_t i=0; i<num_chunks; i++) {
      arr.push_back(*Key::deserialize(dser));
    }
    std::vector<int> cache = dser.read_int_vector();
    return new IntColumn(arr, cache);
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
  double get(size_t idx, KVStore* store) {
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
      DoubleColumnChunk* chunk = DoubleColumnChunk::deserialize(dser);
      return chunk->get(element_idx);
    }
  }

  DoubleColumn *as_double() { return this; }

  virtual char get_type() { return 'D'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(double d, KVStore* store) {
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      DoubleColumnChunk chunk(cached_chunk_);
      Serializer ser;
      chunk.serialize(ser);
      std::string keyName = ""; // TODO: create unique key name
      size_t node = (sz_ / MAX_CHUNK_SIZE) % store->num_nodes();
      Key k(keyName, node);
      Value v(ser.data(), ser.length());
      store->put(k, v); // TODO: may need to allocate k and v on heap
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

  static DoubleColumn* deserialize(Deserializer &dser) {
    size_t num_chunks = dser.read_size_t();
    std::vector<Key> arr;
    for (size_t i=0; i<num_chunks; i++) {
      arr.push_back(*Key::deserialize(dser));
    }
    std::vector<double> cache = dser.read_double_vector();
    return new DoubleColumn(arr, cache);
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
  std::string get(size_t idx, KVStore* store) {
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
      StringColumnChunk* chunk = StringColumnChunk::deserialize(dser);
      return chunk->get(element_idx);
    }
  }

  StringColumn *as_string() { return this; }

  virtual char get_type() { return 'S'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(std::string s, KVStore* store) {
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      StringColumnChunk chunk(cached_chunk_);
      Serializer ser;
      chunk.serialize(ser);
      std::string keyName = ""; // TODO: create unique key name
      size_t node = (sz_ / MAX_CHUNK_SIZE) % store->num_nodes();
      Key k(keyName, node);
      Value v(ser.data(), ser.length());
      store->put(k, v); // TODO: may need to allocate k and v on heap
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

  static StringColumn* deserialize(Deserializer &dser) {
    size_t num_chunks = dser.read_size_t();
    std::vector<Key> arr;
    for (size_t i=0; i<num_chunks; i++) {
      arr.push_back(*Key::deserialize(dser));
    }
    std::vector<std::string> cache = dser.read_string_vector();
    return new StringColumn(arr, cache);
  }
};
