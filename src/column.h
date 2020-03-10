/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 * CS4500 Assignment 4
 */

//lang::CwC

#pragma once
#include <cstdarg>
#include "array.h"
#include <iostream>

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
class Column : public Object
{
public:
  virtual ~Column() = default;

  /** Type converters: Return same column under its actual type, or
   *  nullptr if of the wrong type.  */
  virtual IntColumn *as_int() {
    return nullptr;
  }
  virtual BoolColumn *as_bool() {
    return nullptr;
  }
  virtual FloatColumn *as_float() {
    return nullptr;
  }
  virtual StringColumn *as_string() {
    return nullptr;
  }

  /** Type appropriate push_back methods. Calling the wrong method is
    * undefined behavior. **/
  virtual void push_back(int val) {}
  virtual void push_back(bool val) {}
  virtual void push_back(float val) {}
  virtual void push_back(String *val) {}

  /** Returns the number of elements in the column. */
  virtual size_t size() = 0;

  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  virtual char get_type() = 0;
};

/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 */
class BoolColumn : public Column
{
public:
  BoolColumn() : BoolColumn(size_t(10))
  {
  }

  BoolColumn(size_t size)
  {
    _arr = new BoolArray(size);
  }

  BoolColumn(int n, ...) : BoolColumn(size_t(n))
  {
    va_list bools;
    va_start(bools, n);
    for (int i = 0; i < n; i++)
    {
      _arr->push_back(bool(va_arg(bools, int)));
    }
    va_end(bools);
  }

  virtual ~BoolColumn()
  {
    delete _arr;
  }

  bool get(size_t idx)
  {
    return _arr->get(idx);
  }

  BoolColumn *as_bool()
  {
    return this;
  }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, bool val)
  {
    _arr->set(idx, val);
  }

  virtual size_t size()
  {
    return _arr->length();
  }

  virtual char get_type() {
    return 'B';
  }

public:
  BoolArray *_arr;
};

/*************************************************************************
 * IntColumn::
 * Holds int values.
 */
class IntColumn : public Column
{
public:
  IntColumn() : IntColumn(size_t(10))
  {
  }

  IntColumn(size_t size)
  {
    _arr = new IntArray(size);
  }

  IntColumn(int n, ...) : IntColumn(size_t(n))
  {
    va_list ints;
    va_start(ints, n);
    for (int i = 0; i < n; i++)
    {
      _arr->push_back(va_arg(ints, int));
    }
    va_end(ints);
  }

  virtual ~IntColumn()
  {
    delete _arr;
  }

  int get(size_t idx)
  {
    return _arr->get(idx);
  }

  IntColumn *as_int()
  {
    return this;
  }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, int val)
  {
    _arr->set(idx, val);
  }

  size_t size()
  {
    return _arr->length();
  }

  virtual char get_type() {
    return 'I';
  }

public:
  IntArray *_arr;
};

/*************************************************************************
 * FloatColumn::
 * Holds float values.
 */
class FloatColumn : public Column
{
public:
  FloatColumn() : FloatColumn(size_t(10))
  {
  }

  FloatColumn(size_t size)
  {
    _arr = new FloatArray(size);
  }

  FloatColumn(int n, ...) : FloatColumn(size_t(n))
  {
    va_list floats;
    va_start(floats, n);
    for (int i = 0; i < n; i++)
    {
      double val = va_arg(floats, double);
      _arr->push_back((float) val);
    }
    va_end(floats);
  }

  virtual ~FloatColumn()
  {
    delete _arr;
  }

  float get(size_t idx)
  {
    return _arr->get(idx);
  }

  FloatColumn *as_float()
  {
    return this;
  }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, float val)
  {
    _arr->set(idx, val);
  }

  size_t size()
  {
    return _arr->length();
  }

  virtual char get_type() {
    return 'F';
  }

public:
  FloatArray *_arr;
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
class StringColumn : public Column
{
public:
  StringColumn() : StringColumn(size_t(10))
  {
  }

  StringColumn(size_t size)
  {
    _arr = new StringArray(size);
  }

  StringColumn(int n, ...) : StringColumn(size_t(n))
  {
    va_list strings;
    va_start(strings, n);
    for (int i = 0; i < n; i++)
    {
      _arr->push_back(va_arg(strings, String *));
    }
    va_end(strings);
  }

  virtual ~StringColumn()
  {
    delete _arr;
  }

  String *get(size_t idx)
  {
    return _arr->get(idx);
  }

  StringColumn *as_string()
  {
    return this;
  }

  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, String *val)
  {
    _arr->set(idx, val);
  }

  size_t size()
  {
    return _arr->length();
  }

  char get_type() {
    return 'S';
  }

public:
  StringArray *_arr;
};
