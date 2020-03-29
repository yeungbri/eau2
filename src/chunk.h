/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <iostream>
#include <vector>

#include "serial.h"

class IntColumnChunk;
class BoolColumnChunk;
class DoubleColumnChunk;
class StringColumnChunk;

/**************************************************************************
 * ColumnChunk ::
 * Represents a chunk of a column */
class ColumnChunk
{
public:
  ColumnChunk() = default;

  virtual ~ColumnChunk() = default;

  /** Type converters: Return same column under its actual type, or
   *  nullptr if of the wrong type.  */
  virtual std::shared_ptr<IntColumnChunk> as_int() { return nullptr; }
  virtual std::shared_ptr<BoolColumnChunk> as_bool() { return nullptr; }
  virtual std::shared_ptr<DoubleColumnChunk> as_double() { return nullptr; }
  virtual std::shared_ptr<StringColumnChunk> as_string() { return nullptr; }

  /** Type appropriate push_back methods. Calling the wrong method is
   * undefined behavior. **/
  virtual void push_back(int val){};
  virtual void push_back(bool val){};
  virtual void push_back(double val){};
  virtual void push_back(std::string val){};

  /** Returns the number of elements in the column. */
  virtual size_t size() = 0;

  virtual void serialize(Serializer &ser) {}
};

class IntColumnChunk : public ColumnChunk
{
public:
  std::vector<int> vals_;

  IntColumnChunk() {}

  IntColumnChunk(std::vector<int> vals) : vals_(vals) {}

  ~IntColumnChunk() { vals_.clear(); }

  std::shared_ptr<IntColumnChunk> as_int() { return std::shared_ptr<IntColumnChunk>(this); }

  int get(size_t idx) { return vals_.at(idx); }

  void push_back(int val) { vals_.push_back(val); }

  size_t size() { return vals_.size(); }

  void serialize(Serializer &ser) { ser.write_int_vector(vals_); }

  static std::shared_ptr<IntColumnChunk> deserialize(Deserializer &dser)
  {
    std::vector<int> arr = dser.read_int_vector();
    return std::make_shared<IntColumnChunk>(arr);
  }
};

class BoolColumnChunk : public ColumnChunk
{
public:
  std::vector<bool> vals_;

  BoolColumnChunk() = default;

  BoolColumnChunk(std::vector<bool> vals) : vals_(vals) {}

  ~BoolColumnChunk() { vals_.clear(); }

  std::shared_ptr<BoolColumnChunk> as_bool() { return std::shared_ptr<BoolColumnChunk>(this); }

  bool get(size_t idx) { return vals_.at(idx); }

  void push_back(bool val) { vals_.push_back(val); }

  size_t size() { return vals_.size(); }

  void serialize(Serializer &ser) { ser.write_bool_vector(vals_); }

  static std::shared_ptr<BoolColumnChunk> deserialize(Deserializer &dser)
  {
    std::vector<bool> arr = dser.read_bool_vector();
    return std::make_shared<BoolColumnChunk>(arr);
  }
};

class DoubleColumnChunk : public ColumnChunk
{
public:
  std::vector<double> vals_;

  DoubleColumnChunk() {}

  DoubleColumnChunk(std::vector<double> vals) : vals_(vals) {}

  ~DoubleColumnChunk() { vals_.clear(); }

  std::shared_ptr<DoubleColumnChunk> as_double() { return std::shared_ptr<DoubleColumnChunk>(this); }

  double get(size_t idx) { return vals_.at(idx); }

  void push_back(double val) { vals_.push_back(val); }

  size_t size() { return vals_.size(); }

  void serialize(Serializer &ser) { ser.write_double_vector(vals_); }

  static std::shared_ptr<DoubleColumnChunk> deserialize(Deserializer &dser)
  {
    std::vector<double> arr = dser.read_double_vector();
    return std::make_shared<DoubleColumnChunk>(arr);
  }
};

class StringColumnChunk : public ColumnChunk
{
public:
  std::vector<std::string> vals_;

  StringColumnChunk() {}

  StringColumnChunk(std::vector<std::string> vals) : vals_(vals) {}

  ~StringColumnChunk() { vals_.clear(); }

  std::shared_ptr<StringColumnChunk> as_string() { return std::shared_ptr<StringColumnChunk>(this); }

  std::string get(size_t idx) { return vals_.at(idx); }

  void push_back(std::string val) { vals_.push_back(val); }

  size_t size() { return vals_.size(); }

  void serialize(Serializer &ser) { ser.write_string_vector(vals_); }

  static std::shared_ptr<StringColumnChunk> deserialize(Deserializer &dser)
  {
    std::vector<std::string> arr = dser.read_string_vector();
    return std::make_shared<StringColumnChunk>(arr);
  }
};