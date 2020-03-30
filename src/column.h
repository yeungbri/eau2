/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <iostream>
#include <vector>
#include <cassert>
#include <cmath>
#include "kvstore/kvstore.h"
#include "chunk.h"
#include "serial.h"

class IntColumn;
class BoolColumn;
class DoubleColumn;
class StringColumn;

const size_t MAX_CHUNK_SIZE = 3;

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. */
class Column
{
public:
  std::vector<Key> keys_; // Keys to the values that contain this column's chunks
  size_t sz_;             // number of elements in this column

  Column() { sz_ = 0; }

  virtual ~Column() = default;

  /** Returns the number of elements in the column. */
  virtual size_t size() { return sz_; }

  /** Type converters: Return same column under its actual type, or
   *  nullptr if of the wrong type.  */
  virtual IntColumn *as_int() { return nullptr; }
  virtual BoolColumn *as_bool() { return nullptr; }
  virtual DoubleColumn *as_double() { return nullptr; }
  virtual StringColumn *as_string() { return nullptr; }

  /** Type appropriate push_back methods. Calling the wrong method is
   * undefined behavior. **/
  virtual void push_back(int val, std::shared_ptr<KVStore> store){};
  virtual void push_back(bool val, std::shared_ptr<KVStore> store){};
  virtual void push_back(double val, std::shared_ptr<KVStore> store){};
  virtual void push_back(std::string val, std::shared_ptr<KVStore> store){};

  /** Return the type of this column as a char: 'S', 'B', 'I' and 'D'.*/
  virtual char get_type() = 0;

  virtual void serialize(Serializer &ser) {}

  // referenced from https://stackoverflow.com/a/440240 on 29MAR20, 2:10pm
  /** Generates a random (unique) name for a key */
  virtual std::string gen_name_()
  {
    const int len = 100;
    std::string ret = "";
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i)
    {
      ret += alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return ret;
  }

  /** Stores the given chunk in the store by serializing it. */
  virtual void store_chunk(ColumnChunk &chunk, std::shared_ptr<KVStore> store)
  {
    Serializer ser;
    chunk.serialize(ser);
    std::string keyName = gen_name_();
    size_t node = (sz_ / MAX_CHUNK_SIZE) % store->num_nodes();
    auto k = std::make_shared<Key>(keyName, node);
    auto v = std::make_shared<Value>(ser.data(), ser.length());
    store->put(*k, *v);
    keys_.push_back(*k);
  }

  /**
   * Serializes this column's keys and size. Subclasses are responsible for 
   * serializing their caches, because each column subclass has caches of
   * different types.
   */
  virtual void serialize_help(Serializer &ser)
  {
    ser.write_size_t(keys_.size());
    for (auto key : keys_)
    {
      key.serialize(ser);
    }
  }

  /**
   * Deserializes this column's keys and size. Subclasses are responsible for
   * deserializing their caches because they are of different types.
   */
  static std::vector<Key> deserialize_help(Deserializer &dser)
  {
    size_t num_chunks = dser.read_size_t();
    std::vector<Key> arr;
    for (size_t i = 0; i < num_chunks; i++)
    {
      arr.push_back(*Key::deserialize(dser));
    }
    return arr;
  }
};

/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 */
class BoolColumn : public Column
{
public:
  std::vector<bool> cached_chunk_;

public:
  BoolColumn() = default;

  BoolColumn(std::vector<Key> keys, std::vector<bool> cache)
  {
    keys_ = keys;
    cached_chunk_ = cache;
    sz_ = keys.size() * MAX_CHUNK_SIZE + cache.size();
  }

  virtual ~BoolColumn() { keys_.clear(); }

  /**
   * Given absolute idx value, return the value in cache if it exists. Else,
   * query the KVStore for the correct chunk, and retrieve the value there.
   */
  bool get(size_t idx, std::shared_ptr<KVStore> store)
  {
    assert(idx < sz_);
    size_t chunk_idx = idx / MAX_CHUNK_SIZE;
    size_t element_idx = idx % MAX_CHUNK_SIZE;
    if (chunk_idx == keys_.size())
    {
      return cached_chunk_.at(element_idx);
    }
    else
    {
      Value v = store->get(keys_.at(chunk_idx));
      Deserializer dser(v.data(), v.length());
      auto bcc = BoolColumnChunk::deserialize(dser);
      auto ret = bcc->get(element_idx);
      return ret;
    }
  }

  BoolColumn *as_bool() { return this; }

  virtual char get_type() { return 'B'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(bool b, std::shared_ptr<KVStore> store)
  {
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      BoolColumnChunk chunk(cached_chunk_);
      store_chunk(chunk, store);
      cached_chunk_.clear();
    }
    cached_chunk_.push_back(b);
    sz_++;
  }

  void serialize(Serializer &ser)
  {
    serialize_help(ser);
    ser.write_bool_vector(cached_chunk_);
  }

  static std::shared_ptr<BoolColumn> deserialize(Deserializer &dser)
  {
    auto arr = Column::deserialize_help(dser);
    std::vector<bool> cache = dser.read_bool_vector();
    return std::make_shared<BoolColumn>(arr, cache);
  }
};

/*************************************************************************
 * IntColumn::
 * Holds int values.
 */
class IntColumn : public Column
{
public:
  std::vector<int> cached_chunk_;

public:
  IntColumn() = default;

  IntColumn(std::vector<Key> keys, std::vector<int> cache)
  {
    keys_ = keys;
    cached_chunk_ = cache;
    sz_ = keys.size() * MAX_CHUNK_SIZE + cache.size();
  }

  virtual ~IntColumn() { keys_.clear(); }

  /**
   * Given absolute idx value, return the value in cache if it exists. Else,
   * query the KVStore for the correct chunk, and retrieve the value there.
   */
  int get(size_t idx, std::shared_ptr<KVStore> store)
  {
    assert(idx < sz_);
    size_t chunk_idx = idx / MAX_CHUNK_SIZE;
    size_t element_idx = idx % MAX_CHUNK_SIZE;
    if (chunk_idx == keys_.size())
    {
      return cached_chunk_.at(element_idx);
    }
    else
    {
      Value v = store->get(keys_.at(chunk_idx));
      Deserializer dser(v.data(), v.length());
      auto chunk = IntColumnChunk::deserialize(dser);
      return chunk->get(element_idx);
    }
  }

  IntColumn *as_int() { return this; }

  virtual char get_type() { return 'I'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(int i, std::shared_ptr<KVStore> store)
  {
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      IntColumnChunk chunk(cached_chunk_);
      store_chunk(chunk, store);
      cached_chunk_.clear();
    }
    cached_chunk_.push_back(i);
    sz_++;
  }

  void serialize(Serializer &ser)
  {
    serialize_help(ser);
    ser.write_int_vector(cached_chunk_);
  }

  static std::shared_ptr<IntColumn> deserialize(Deserializer &dser)
  {
    auto arr = Column::deserialize_help(dser);
    std::vector<int> cache = dser.read_int_vector();
    return std::make_shared<IntColumn>(arr, cache);
  }
};

/*************************************************************************
 * DoubleColumn::
 * Holds double values.
 */
class DoubleColumn : public Column
{
public:
  std::vector<double> cached_chunk_;

public:
  DoubleColumn() = default;

  DoubleColumn(std::vector<Key> keys, std::vector<double> cache)
  {
    keys_ = keys;
    cached_chunk_ = cache;
    sz_ = keys.size() * MAX_CHUNK_SIZE + cache.size();
  }

  virtual ~DoubleColumn() { keys_.clear(); }

  /**
   * Given absolute idx value, return the value in cache if it exists. Else,
   * query the KVStore for the correct chunk, and retrieve the value there.
   */
  double get(size_t idx, std::shared_ptr<KVStore> store)
  {
    assert(idx < sz_);
    size_t chunk_idx = idx / MAX_CHUNK_SIZE;
    size_t element_idx = idx % MAX_CHUNK_SIZE;
    if (chunk_idx == keys_.size())
    {
      return cached_chunk_.at(element_idx);
    }
    else
    {
      Value v = store->get(keys_.at(chunk_idx));
      Deserializer dser(v.data(), v.length());
      auto chunk = DoubleColumnChunk::deserialize(dser);
      return chunk->get(element_idx);
    }
  }

  DoubleColumn *as_double() { return this; }

  virtual char get_type() { return 'D'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(double d, std::shared_ptr<KVStore> store)
  {
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      DoubleColumnChunk chunk(cached_chunk_);
      store_chunk(chunk, store);
      cached_chunk_.clear();
    }
    cached_chunk_.push_back(d);
    sz_++;
  }

  void serialize(Serializer &ser)
  {
    serialize_help(ser);
    ser.write_double_vector(cached_chunk_);
  }

  static std::shared_ptr<DoubleColumn> deserialize(Deserializer &dser)
  {
    auto arr = Column::deserialize_help(dser);
    std::vector<double> cache = dser.read_double_vector();
    return std::make_shared<DoubleColumn>(arr, cache);
  }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
class StringColumn : public Column
{
public:
  std::vector<std::string> cached_chunk_;

public:
  StringColumn() = default;

  StringColumn(std::vector<Key> keys, std::vector<std::string> cache)
  {
    keys_ = keys;
    cached_chunk_ = cache;
    sz_ = keys.size() * MAX_CHUNK_SIZE + cache.size();
  }

  virtual ~StringColumn() { keys_.clear(); }

  /**
   * Given absolute idx value, return the value in cache if it exists. Else,
   * query the KVStore for the correct chunk, and retrieve the value there.
   */
  std::string get(size_t idx, std::shared_ptr<KVStore> store)
  {
    assert(idx < sz_);
    size_t chunk_idx = idx / MAX_CHUNK_SIZE;
    size_t element_idx = idx % MAX_CHUNK_SIZE;
    if (chunk_idx == keys_.size())
    {
      return cached_chunk_.at(element_idx);
    }
    else
    {
      Value v = store->get(keys_.at(chunk_idx));
      Deserializer dser(v.data(), v.length());
      auto chunk = StringColumnChunk::deserialize(dser);
      return chunk->get(element_idx);
    }
  }

  StringColumn *as_string() { return this; }

  virtual char get_type() { return 'S'; }

  /**
   * Inserts element into cache. If cache is full, serialize it and store it in
   * the KV store, and empty the cache.
   */
  virtual void push_back(std::string s, std::shared_ptr<KVStore> store)
  {
    if (cached_chunk_.size() >= MAX_CHUNK_SIZE)
    {
      StringColumnChunk chunk(cached_chunk_);
      store_chunk(chunk, store);
      cached_chunk_.clear();
    }
    cached_chunk_.push_back(s);
    sz_++;
  }

  void serialize(Serializer &ser)
  {
    serialize_help(ser);
    ser.write_string_vector(cached_chunk_);
  }

  static std::shared_ptr<StringColumn> deserialize(Deserializer &dser)
  {
    auto arr = Column::deserialize_help(dser);
    std::vector<std::string> cache = dser.read_string_vector();
    return std::make_shared<StringColumn>(arr, cache);
  }
};
