/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include "serial.h"
#include <iostream>
#include <vector>
#include "serial.h"

class IntColumn;
class BoolColumn;
class FloatColumn;
class StringColumn;

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. */
class Column {
 public:
  virtual ~Column() = default;

  /** Type converters: Return same column under its actual type, or
   *  nullptr if of the wrong type.  */
  virtual IntColumn *as_int() { return nullptr; }
  virtual BoolColumn *as_bool() { return nullptr; }
  virtual FloatColumn *as_float() { return nullptr; }
  virtual StringColumn *as_string() { return nullptr; }

  /** Type appropriate push_back methods. Calling the wrong method is
   * undefined behavior. **/
  virtual void push_back(int val) {};
  virtual void push_back(bool val) {};
  virtual void push_back(float val) {};
  virtual void push_back(std::string val) {};

  /** Returns the number of elements in the column. */
  virtual size_t size() = 0;

  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  virtual char get_type() = 0;

  virtual void serialize(Serializer& ser) { }

  virtual Column deserialize(Deserializer& dser) { }
};

/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 */
class BoolColumn : public Column {
 public:
  BoolColumn() = default;

  BoolColumn(std::vector<bool> bools) {
    for (bool b : bools) {
      _arr.push_back(b);
    }
  }

  virtual ~BoolColumn() { _arr.clear(); }

  bool get(size_t idx) { return _arr.at(idx); }

  BoolColumn *as_bool() { return this; }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, bool val) { _arr[idx] = val; }

  virtual size_t size() { return _arr.size(); }

  virtual char get_type() { return 'B'; }
  
  virtual void push_back(bool b) { _arr.push_back(b); }

 public:
  std::vector<bool> _arr;
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
      _arr.push_back(i);
    }
  }

  virtual ~IntColumn() { _arr.clear(); }

  int get(size_t idx) { return _arr.at(idx); }

  IntColumn *as_int() { return this; }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, int val) { _arr[idx] = val; }

  size_t size() { return _arr.size(); }

  virtual char get_type() { return 'I'; }

  virtual void push_back(int i) { _arr.push_back(i); }

 public:
  std::vector<int> _arr;
};

/*************************************************************************
 * FloatColumn::
 * Holds float values.
 */
class FloatColumn : public Column {
 public:
  FloatColumn() = default;

  FloatColumn(std::vector<float> floats) {
    for (float f : floats) {
      _arr.push_back(f);
    }
  }

  virtual ~FloatColumn() { _arr.clear(); }

  float get(size_t idx) { return _arr.at(idx); }

  FloatColumn *as_float() { return this; }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, float val) { _arr[idx] = val; }

  size_t size() { return _arr.size(); }

  virtual char get_type() { return 'F'; }

  virtual void push_back(float f) { _arr.push_back(f); }

  void serialize(Serializer &ser) {
    ser.write_float_vector(_arr);
  }

  static FloatColumn* deserialize(Deserializer &dser) {
    std::vector<float> arr = dser.read_float_vector();
    return new FloatColumn(arr);
  }

 public:
  std::vector<float> _arr;
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
      _arr.push_back(s);
    }
  }

  virtual ~StringColumn() { _arr.clear(); }

  std::string get(size_t idx) { return _arr.at(idx); }

  StringColumn *as_string() { return this; }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, std::string val) { _arr[idx] = val; }

  size_t size() { return _arr.size(); }

  char get_type() { return 'S'; }

  virtual void push_back(std::string s) { _arr.push_back(s); }

 public:
  std::vector<std::string> _arr;
};
