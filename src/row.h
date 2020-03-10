/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::CwC

#pragma once
#include "schema.h"
#include "fielder.h"
#include "string.h"

static size_t DEFAULT_ROW_SIZE = 10;

/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 */
class Row : public Object
{
public:
  Schema _schema; // External Schema
  size_t _length;
  size_t _capacity;
  size_t _idx; // Not our responsibility
  String *_name;

  // Attribution: https://stackoverflow.com/a/18577481/12602247 at 2/11 7:52PM
  struct Data
  {
    enum
    {
      is_int,
      is_float,
      is_bool,
      is_string
    } type;
    union {
      int ival;
      float fval;
      bool bval;
      String *sval;
    } val;
  };
  struct Data **_elements;

  /** Build a row following a schema. */
  Row(Schema &scm, String* name) : Row(scm)
  {
    _name = name;
  }

  Row(Schema &scm)
  {
    _schema = scm;
    _length = scm.width();
    _capacity = DEFAULT_ROW_SIZE;
    _elements = new struct Data *[DEFAULT_ROW_SIZE];
    for (size_t i = 0; i < _length; i++)
    {
      _elements[i] = new struct Data();
    }
  }

  Row(Row &row)
  {
    _schema = row._schema;
    _capacity = row._capacity;
    _length = row._length;
    _idx = row._idx;
    _elements = new struct Data *[_capacity];
    for (size_t i = 0; i < _length; ++i)
    {
      struct Data *element = new struct Data();
      element->type = row._elements[i]->type;
      element->val = row._elements[i]->val;
      _elements[i] = element;
    }
  }

  virtual ~Row()
  {
    delete[] _elements;
  }

  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
  void set(size_t col, int val)
  {
    if (_schema.col_type(col) == 'I')
    {
      if (col == _capacity)
      {
        _grow();
      }
      _elements[col]->type = Data::is_int;
      _elements[col]->val.ival = val;
    }
  }
  void set(size_t col, float val)
  {
    if (_schema.col_type(col) == 'F')
    {
      if (col == _length)
      {
        _grow();
      }
      _elements[col]->type = Data::is_float;
      _elements[col]->val.fval = val;
    }
  }
  void set(size_t col, bool val)
  {
    if (_schema.col_type(col) == 'B')
    {
      _elements[col]->type = Data::is_bool;
      _elements[col]->val.bval = val;
    }
  }

  /** Acquire ownership of the string. */
  void set(size_t col, String *val)
  {
    if (_schema.col_type(col) == 'S')
    {
      _elements[col]->type = Data::is_string;
      _elements[col]->val.sval = new String(*val);
    }
  }

  /** Set/get the index of this row (ie. its position in the dataframe. This is
   *  only used for informational purposes, unused otherwise */
  void set_idx(size_t idx)
  {
    _idx = idx;
  }

  size_t get_idx()
  {
    return _idx;
  }

  /** Getters: get the value at the given column. If the column is not
    * of the requested type, the result is undefined. */
  int get_int(size_t col)
  {
    return _elements[col]->val.ival;
  }

  bool get_bool(size_t col)
  {
    return _elements[col]->val.bval;
  }

  float get_float(size_t col)
  {
    return _elements[col]->val.fval;
  }

  String *get_string(size_t col)
  {
    return _elements[col]->val.sval;
  }

  /** Number of fields in the row. */
  size_t width()
  {
    size_t result = 0;
    for (size_t i = 0; i < _length; i++)
    {
      if (_elements[i] != nullptr)
      {
        result++;
      }
    }
    return result;
  }

  /** Type of the field at the given position. An idx >= width is  undefined. */
  char col_type(size_t idx)
  {
    if (idx < _length)
    {
      switch (_elements[idx]->type)
      {
      case Data::is_int:
        return 'I';
      case Data::is_float:
        return 'F';
      case Data::is_bool:
        return 'B';
      case Data::is_string:
        return 'S';
      default:
        return 'S';
      }
    }
    return 'S';
  }

  /** Given a Fielder, visit every field of this row. The first argument is
    * index of the row in the dataframe.
    * Calling this method before the row's fields have been set is undefined. */
  void visit(size_t idx, Fielder &f)
  {
    for (size_t i = 0; i < _length; i++)
    {
      f.start(i);
      switch (col_type(i))
      {
      case 'I':
        f.accept(_elements[i]->val.ival);
        break;
      case 'B':
        f.accept(_elements[i]->val.bval);
        break;
      case 'F':
        f.accept(_elements[i]->val.fval);
        break;
      case 'S':
        f.accept(_elements[i]->val.sval);
        break;
      }
      f.done();
    }
  }

  void _grow()
  {
    _capacity = _capacity * 2;
    Data **elementsCopy = new struct Data *[_capacity];
    for (size_t i = 0; i < _length; ++i)
    {
      elementsCopy[i] = _elements[i];
    }
    for (size_t i = _length; i < _capacity; ++i)
    {
      elementsCopy[i] = new struct Data();
    }
  }

  /**
   * Caller of this method is responsible for updating this schema
   * If _elements is not big enough, make it bigger
   * Add this item to _elements
   */
  void push_back(bool b)
  {
    if (_length == _capacity)
    {
      _grow();
    }
    _elements[_length]->val.bval = b;
    _length++;
  }

  void push_back(int i)
  {
    if (_length == _capacity)
    {
      _grow();
    }
    _elements[_length]->val.ival = i;
    _length++;
  }

  void push_back(float f)
  {
    if (_length == _capacity)
    {
      _grow();
    }
    _elements[_length]->val.fval = f;
    _length++;
  }

  void push_back(String *s)
  {
    if (_length == _capacity)
    {
      _grow();
    }
    _elements[_length]->val.sval = s;
    _length++;
  }
};
