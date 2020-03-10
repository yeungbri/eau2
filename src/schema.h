/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

#pragma once

#include <iostream>
#include "array.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object
{
public:
  /** Copying constructor */
  Schema(Schema &from)
  {
    if (this != &from)
    {
      StringArray *fromTypes = new StringArray(*from._types);
      StringArray *fromRows = new StringArray(*from._rows);
      StringArray *fromCols = new StringArray(*from._cols);
      this->_types = fromTypes;
      this->_rows = fromRows;
      this->_cols = fromCols;
    }
  }

  /** Create an empty schema **/
  Schema()
  {
    _types = new StringArray();
    _rows = new StringArray();
    _cols = new StringArray();
  }

  /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr argument is
    * undefined. **/
  Schema(const char *types) : Schema()
  {
    for (int i = 0; i < strlen(types); ++i)
    {
      if (types[i] == 'B' || types[i] == 'I' || types[i] == 'F' || types[i] == 'S')
      {
        char *type = new char[2];
        type[0] = types[i];
        type[1] = '\0';
        _types->push_back(new String(type));
        _cols->push_back(nullptr);
      }
    }
  }

  virtual ~Schema()
  {
    // Something is causing a double free in our tests
    // delete _types;
    // delete _rows;
    // delete _cols;
  }

  /** Add a column of the given type and name (can be nullptr), name
    * is external. Names are expectd to be unique, duplicates result
    * in undefined behavior. */
  void add_column(char typ, String *name)
  {
    char *type = new char[2];
    type[0] = typ;
    type[1] = '\0';
    _types->push_back(new String(type));
    _cols->push_back(name);
  }

  /** Add a row with a name (possibly nullptr), name is external.  Names are
   *  expectd to be unique, duplicates result in undefined behavior. */
  void add_row(String *name)
  {
    _rows->push_back(name);
  }

  /** Return name of row at idx; nullptr indicates no name. An idx >= width
    * is undefined. */
  String *row_name(size_t idx)
  {
    return _rows->get(idx);
  }

  /** Return name of column at idx; nullptr indicates no name given.
    *  An idx >= width is undefined.*/
  String *col_name(size_t idx)
  {
    return _cols->get(idx);
  }

  /** Return type of column at idx. An idx >= width is undefined. */
  char col_type(size_t idx)
  {
    String *str_type = _types->get(idx);
    return str_type->c_str()[0];
  }

  /** Given a column name return its index, or -1. */
  int col_idx(const char *name)
  {
    for (size_t i = 0; i < width(); ++i)
    {
      if (_cols->get(i) && strcmp(_cols->get(i)->c_str(), name) == 0)
      {
        return i;
      }
    }
    return -1;
  }

  /** Given a row name return its index, or -1. */
  int row_idx(const char *name)
  {
    for (size_t i = 0; i < length(); ++i)
    {
      if (strcmp(_rows->get(i)->c_str(), name) == 0)
      {
        return i;
      }
    }
    return -1;
  }

  /** The number of columns */
  size_t width()
  {
    return _types->length();
  }

  /** The number of rows */
  size_t length()
  {
    return _rows->length();
  }

public:
  StringArray *_types;
  StringArray *_rows;
  StringArray *_cols;
};