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
class DoubleColumn;
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
  virtual DoubleColumn *as_double() { return nullptr; }
  virtual StringColumn *as_string() { return nullptr; }

  /** Type appropriate push_back methods. Calling the wrong method is
   * undefined behavior. **/
  virtual void push_back(int val) {};
  virtual void push_back(bool val) {};
  virtual void push_back(double val) {};
  virtual void push_back(std::string val) {};

  /** Returns the number of elements in the column. */
  virtual size_t size() = 0;

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
  
  void serialize(Serializer &ser) {
    ser.write_bool_vector(_arr);
  }

  static BoolColumn* deserialize(Deserializer &dser) {
    std::vector<bool> arr = dser.read_bool_vector();
    return new BoolColumn(arr);
  }

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

  void serialize(Serializer &ser) {
    ser.write_int_vector(_arr);
  }

  static IntColumn* deserialize(Deserializer &dser) {
    std::vector<int> arr = dser.read_int_vector();
    return new IntColumn(arr);
  }

 public:
  std::vector<int> _arr;
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
      _arr.push_back(f);
    }
  }

  virtual ~DoubleColumn() { _arr.clear(); }

  double get(size_t idx) { return _arr.at(idx); }

  DoubleColumn *as_double() { return this; }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, double val) { _arr[idx] = val; }

  size_t size() { return _arr.size(); }

  virtual char get_type() { return 'D'; }

  virtual void push_back(double f) { _arr.push_back(f); }

  void serialize(Serializer &ser) {
    ser.write_double_vector(_arr);
  }

  static DoubleColumn* deserialize(Deserializer &dser) {
    std::vector<double> arr = dser.read_double_vector();
    return new DoubleColumn(arr);
  }

 public:
  std::vector<double> _arr;
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
  
  void serialize(Serializer &ser) {
    ser.write_string_vector(_arr);
  }

  static StringColumn* deserialize(Deserializer &dser) {
    std::vector<std::string> arr = dser.read_string_vector();
    return new StringColumn(arr);
  }

 public:
  std::vector<std::string> _arr;
};
