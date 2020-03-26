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
  KVStore *store_;
  std::vector<Key> chunks_;
  size_t sz;

  Column(KVStore *store) : store_(store) {
    sz = 0;
  }

  virtual ~Column() = default;

  /** Returns the number of elements in the column. */
  virtual size_t size() {
    return sz;
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
  BoolColumn() = default;

  BoolColumn(KVStore *store, std::vector<bool> bools) : Column(store) {
    size_t chunk_count = std::round( (double) bools.size()/ MAX_CHUNK_SIZE);
    size_t next_node = 0;
    for (size_t i=0; i<chunk_count; i++) {
      BoolColumnChunk chunk;
      for (size_t j=0; j<MAX_CHUNK_SIZE; j++) {
        chunk.push_back(bools.at(i * MAX_CHUNK_SIZE + j));
      }

      std::string key_name = ""; //TODO: name key
      Key* k = new Key(key_name, next_node);
      Serializer ser;
      chunk.serialize(ser);
      Value* v = new Value(ser.data(), ser.length());
      store->put(*k, *v);
      next_node = next_node + 1 % store_->num_nodes();
    }
  }

  virtual ~BoolColumn() { chunks_.clear(); }

  BoolColumnChunk* get_chunk(size_t chunk_idx) {
    Value v = store_->get(chunks_.at(chunk_idx));
    Deserializer dser(v.data(), v.length());
    return BoolColumnChunk::deserialize(dser); 
  }

  bool get(size_t idx) {
    assert(idx < sz);
    size_t chunk_idx = idx / MAX_CHUNK_SIZE;
    BoolColumnChunk* bcc = get_chunk(chunk_idx);
    return bcc->get(idx - chunk_idx * MAX_CHUNK_SIZE);
  }

  BoolColumn *as_bool() { return this; }

  virtual char get_type() { return 'B'; }
  
  virtual void push_back(bool b) {
    if (sz % MAX_CHUNK_SIZE == 0) {
      // make a new chunk since last is full
      std::vector<bool> v {b};
      BoolColumnChunk(v);
    } else {
      // place bool into last chunk
      BoolColumnChunk* bcc = get_chunk(sz / MAX_CHUNK_SIZE);
      bcc->push_back(b);
    }
  }
  
  void serialize(Serializer &ser) {
    ser.write_size_t(chunks_.size());
    for (size_t i=0; i<chunks_.size(); i++) {
      chunks_.at(i).serialize(ser);
    }
  }

  // TODO: Pass in the store to this method to initalize new boolcol
  static BoolColumn* deserialize(Deserializer &dser) {
    size_t num_chunks = dser.read_size_t();
    std::vector<Key> arr;
    for (size_t i=0; i<num_chunks; i++) {
      arr.push_back(Key::deserialize(dser));
    }
    return new BoolColumn(arr);
  }
};

/*************************************************************************
 * IntColumn::
 * Holds int values.
 */
class IntColumn : public Column {
 public:
  IntColumn() = default;

  IntColumn(std::vector<int> ints) {
    for (int i : ints) {
      chunks_.push_back(i);
    }
  }

  virtual ~IntColumn() { chunks_.clear(); }

  int get(size_t idx) { return chunks_.at(idx); }

  IntColumn *as_int() { return this; }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, int val) { chunks_[idx] = val; }

  size_t size() { return chunks_.size(); }

  virtual char get_type() { return 'I'; }

  virtual void push_back(int i) { chunks_.push_back(i); }

  void serialize(Serializer &ser) {
    ser.write_int_vector(chunks_);
  }

  static IntColumn* deserialize(Deserializer &dser) {
    std::vector<int> arr = dser.read_int_vector();
    return new IntColumn(arr);
  }

 public:
  std::vector<int> chunks_;
};

/*************************************************************************
 * DoubleColumn::
 * Holds double values.
 */
class DoubleColumn : public Column {
 public:
  DoubleColumn() = default;

  DoubleColumn(std::vector<double> doubles) {
    for (double f : doubles) {
      chunks_.push_back(f);
    }
  }

  virtual ~DoubleColumn() { chunks_.clear(); }

  double get(size_t idx) 
  { 
    return chunks_.at(idx); 
  }

  DoubleColumn *as_double() { return this; }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, double val) { chunks_[idx] = val; }

  size_t size() { return chunks_.size(); }

  virtual char get_type() { return 'D'; }

  virtual void push_back(double f) { chunks_.push_back(f); }

  void serialize(Serializer &ser) {
    ser.write_double_vector(chunks_);
  }

  static DoubleColumn* deserialize(Deserializer &dser) {
    std::vector<double> arr = dser.read_double_vector();
    return new DoubleColumn(arr);
  }

 public:
  std::vector<double> chunks_;
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
class StringColumn : public Column {
 public:
  StringColumn() = default;

  StringColumn(std::vector<std::string> strings) {
    for (std::string s : strings) {
      chunks_.push_back(s);
    }
  }

  virtual ~StringColumn() { chunks_.clear(); }

  std::string get(size_t idx) { return chunks_.at(idx); }

  StringColumn *as_string() { return this; }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, std::string val) { chunks_[idx] = val; }

  size_t size() { return chunks_.size(); }

  char get_type() { return 'S'; }

  virtual void push_back(std::string s) { chunks_.push_back(s); }
  
  void serialize(Serializer &ser) {
    ser.write_string_vector(chunks_);
  }

  static StringColumn* deserialize(Deserializer &dser) {
    std::vector<std::string> arr = dser.read_string_vector();
    return new StringColumn(arr);
  }

 public:
  std::vector<std::string> chunks_;
};
