/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once
#include "schema.h"
#include "fielder.h"
#include <string>
#include <vector>

static size_t DEFAULT_ROW_SIZE = 10;

/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 */
class Row
{
public:
  Schema _schema; // External Schema
  size_t _idx; // Not our responsibility
  std::string _name;

  // Attribution: https://stackoverflow.com/a/18577481/12602247 at 2/11 7:52PM
  struct Data
  {
    enum
    {
      is_int,
      is_float,
      is_bool,
      is_string,
      is_missing
    } type;
    union {
      int ival;
      float fval;
      bool bval;
      char* sval;
      char* mval;
    } val;
  };

  std::vector<struct Data *> _elements;

  /** Build a row following a schema. */
  Row(Schema &scm, std::string name) : Row(scm)
  {
    _name = name;
  }

  Row(Schema &scm)
  {
    _schema = scm;
    for (size_t i = 0; i < _schema.width(); i++)
    {
      struct Data* element = new struct Data();
      element->type = Data::is_missing;
      element->val.mval = new char[0];
      _elements.push_back(element);
    }
  }

  Row(Row &row)
  {
    _schema = row._schema;
    _idx = row._idx;
    for (size_t i = 0; i < _schema.width(); ++i)
    {
      struct Data *element = new struct Data();
      element->type = row._elements[i]->type;
      element->val = row._elements[i]->val;
      _elements.push_back(element);
    }
  }

  virtual ~Row()
  {
    for (auto field : _elements)
    {
      //delete field->val.sval;
      delete field;
    }
    _elements.clear();
  }

  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
  void set(size_t col, int val)
  {
    if (_schema.col_type(col) == 'I')
    {
      _elements[col]->type = Data::is_int;
      _elements[col]->val.ival = val;
    }
  }
  void set(size_t col, float val)
  {
    if (_schema.col_type(col) == 'F')
    {
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
  void set(size_t col, std::string val)
  {
    if (_schema.col_type(col) == 'S')
    {
      _elements[col]->type = Data::is_string;
      //delete _elements[col]->val.sval;
      _elements[col]->val.sval = strdup(val.c_str());
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

  std::string get_string(size_t col)
  {
    return _elements[col]->val.sval;
  }

  /** Number of fields in the row. */
  size_t width()
  {
    size_t result = 0;
    for (size_t i = 0; i < _elements.size(); i++)
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
    if (idx < _elements.size())
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
  void visit(Fielder &f)
  {
    for (size_t i = 0; i < _elements.size(); i++)
    {
      f.start(i);
      if (_elements[i]->type != Data::is_missing)
      {
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
          f.accept(std::string(_elements[i]->val.sval));
          break;
        }
      }
      f.done();
    }
  }

  // void set_missing(int idx)
  // {
  //   std::cout << _elements[idx]->type << std::endl;
  //   std::cout << Data::is_missing << std::endl;
  //   _elements[idx]->type = Data::is_missing;
  // }

  /**
   * Caller of this method is responsible for updating this schema
   * If _elements is not big enough, make it bigger
   * Add this item to _elements
   */
  void push_back(bool b)
  {
    struct Data *element = new struct Data();
    element->type = Data::is_bool;
    element->val.bval = b;
    _elements.push_back(element);
  }

  void push_back(int i)
  {
    struct Data *element = new struct Data();
    element->type = Data::is_int;
    element->val.ival = i;
    _elements.push_back(element);;
  }

  void push_back(float f)
  {
    struct Data *element = new struct Data();
    element->type = Data::is_float;
    element->val.fval = f;
    _elements.push_back(element);
  }

  void push_back(std::string s)
  {
    struct Data *element = new struct Data();
    element->type = Data::is_string;
    element->val.sval = strdup(s.c_str());
    _elements.push_back(element);;
  }
};
