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
  std::vector<Key> keys_;
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
  BoolColumn() = default;

  BoolColumn(KVStore *store, std::vector<bool> bools) : Column(store) {
    push_back_n(bools);
  }

  virtual ~BoolColumn() { keys_.clear(); }

  BoolColumnChunk* get_chunk(size_t chunk_idx) {
    Value v = store_->get(keys_.at(chunk_idx));
    Deserializer dser(v.data(), v.length());
    return BoolColumnChunk::deserialize(dser); 
  }

  bool get(size_t idx) {
    assert(idx < size());
    size_t chunk_idx = idx / MAX_CHUNK_SIZE;
    BoolColumnChunk* bcc = get_chunk(chunk_idx);
    return bcc->get(idx - chunk_idx * MAX_CHUNK_SIZE);
  }

  BoolColumn *as_bool() { return this; }

  virtual char get_type() { return 'B'; }
  
  /**
   * Inserts variable number of elements into this column. Preferable to use
   * this over push_back if vals.size() > 1, to reduce the amount we need to
   * repeatedly serialize/deserialize.
   */
  virtual void push_back_n(std::vector<bool> vals)
  {
    size_t next_node = 0;   // The node to store next chunk (determined round-robin style)
    size_t chunks_used = 0; // How many chunks we've used so far
    bool done = false;      // Becomes true once all vals have been added

    while (!done) {
      // If a partially-filled chunk exists, use it. Else, create a new chunk.
      bool reusable = size() % MAX_CHUNK_SIZE == 0;
      BoolColumnChunk chunk = reusable ? *get_chunk(size() / MAX_CHUNK_SIZE) : BoolColumnChunk();
      size_t chunk_size = chunk.size();

      Key* k;
      // Reuse the old key if we are reusing a chunk
      if (reusable)
      {
        k = &keys_.at(size() / MAX_CHUNK_SIZE);
      // Create a unique key with the name and the node this chunk will live on
      } else {      
        std::string key_name = ""; // TODO: name key
        k = new Key(key_name, next_node);
        keys_.push_back(*k);
        // Advance to the next node, round-robin style
        next_node = next_node + 1 % store_->num_nodes();
      }

      // Fill the chunk up to the MAX_CHUNK_SIZE minus what's already in the chunk
      for (size_t j=0; j<MAX_CHUNK_SIZE - chunk_size; j++) {
        // If we've added all the vals... mark done, and exit this loop
        if (chunks_used * MAX_CHUNK_SIZE + j >= vals.size())
        {
          done = true;
          break;
        }
        chunk.push_back(vals.at(chunks_used * MAX_CHUNK_SIZE + j));
        sz++;
      }

      // Serialize this chunk and store it as a Value, and put pair in KV store
      Serializer ser;
      chunk.serialize(ser);
      Value* v = new Value(ser.data(), ser.length());
      store_->put(*k, *v);
    }
  }

  /**
   * Inserts this element into the column by first retrieving a partially-
   * filled chunk from the KVStore (if it exists) or creating a new chunk
   * if it does not. Must serialize the chunk before adding it back.
   */
  virtual void push_back(bool b) {
    size_t chunk_idx = size() / MAX_CHUNK_SIZE;
    BoolColumnChunk chunk = size() % MAX_CHUNK_SIZE == 0 ? BoolColumnChunk() : *get_chunk(chunk_idx);
    chunk.push_back(b);
    Serializer ser;
    chunk.serialize(ser);
    Key k = keys_.at(chunk_idx);
    Value* v = new Value(ser.data(), ser.length());
    store_->put(k, *v);
    sz++;
  }
  
  void serialize(Serializer &ser) {
    ser.write_size_t(keys_.size());
    for (size_t i=0; i<keys_.size(); i++) {
      keys_.at(i).serialize(ser);
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
      keys_.push_back(i);
    }
  }

  virtual ~IntColumn() { keys_.clear(); }

  int get(size_t idx) { return keys_.at(idx); }

  IntColumn *as_int() { return this; }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, int val) { keys_[idx] = val; }

  size_t size() { return keys_.size(); }

  virtual char get_type() { return 'I'; }

  virtual void push_back(int i) { keys_.push_back(i); }

  void serialize(Serializer &ser) {
    ser.write_int_vector(keys_);
  }

  static IntColumn* deserialize(Deserializer &dser) {
    std::vector<int> arr = dser.read_int_vector();
    return new IntColumn(arr);
  }

 public:
  std::vector<int> keys_;
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
      keys_.push_back(f);
    }
  }

  virtual ~DoubleColumn() { keys_.clear(); }

  double get(size_t idx) 
  { 
    return keys_.at(idx); 
  }

  DoubleColumn *as_double() { return this; }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, double val) { keys_[idx] = val; }

  size_t size() { return keys_.size(); }

  virtual char get_type() { return 'D'; }

  virtual void push_back(double f) { keys_.push_back(f); }

  void serialize(Serializer &ser) {
    ser.write_double_vector(keys_);
  }

  static DoubleColumn* deserialize(Deserializer &dser) {
    std::vector<double> arr = dser.read_double_vector();
    return new DoubleColumn(arr);
  }

 public:
  std::vector<double> keys_;
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
      keys_.push_back(s);
    }
  }

  virtual ~StringColumn() { keys_.clear(); }

  std::string get(size_t idx) { return keys_.at(idx); }

  StringColumn *as_string() { return this; }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, std::string val) { keys_[idx] = val; }

  size_t size() { return keys_.size(); }

  char get_type() { return 'S'; }

  virtual void push_back(std::string s) { keys_.push_back(s); }
  
  void serialize(Serializer &ser) {
    ser.write_string_vector(keys_);
  }

  static StringColumn* deserialize(Deserializer &dser) {
    std::vector<std::string> arr = dser.read_string_vector();
    return new StringColumn(arr);
  }

 public:
  std::vector<std::string> keys_;
};
