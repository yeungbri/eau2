/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once

#include <iostream>
#include <vector>
#include <string>

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema 
{
public:
  /** Copying constructor */
  Schema(Schema &from)
  {
    if (this != &from)
    {
      for (auto s : from._types)
      {
        _types.push_back(s);
      }
      for (auto s : from._rows)
      {
        _rows.push_back(s);
      }
      for (auto s : from._cols)
      {
        _cols.push_back(s);
      }
    }
  }

  /** Create an empty schema **/
  Schema() = default; 

  /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a empty string argument is
    * undefined. **/
  Schema(const char *types)
  {
    for (int i = 0; i < strlen(types); ++i)
    {
      if (types[i] == 'B' || types[i] == 'I' || types[i] == 'F' || types[i] == 'S')
      {
        add_column(types[i], "");
      }
    }
  }

  virtual ~Schema()
  {
    _types.clear();
    _rows.clear();
    _cols.clear();
  }

  /** Add a column of the given type and name (can be empty string), name
    * is external. Names are expectd to be unique, duplicates result
    * in undefined behavior. */
  void add_column(char typ, std::string name)
  {
    _types.push_back(std::string(1, typ));
    _cols.push_back(name);
  }

  /** Add a row with a name (possibly empty string), name is external.  Names are
   *  expectd to be unique, duplicates result in undefined behavior. */
  void add_row(std::string name)
  {
    _rows.push_back(name);
  }

  /** Return name of row at idx; empty string indicates no name. An idx >= width
    * is undefined. */
  std::string row_name(size_t idx)
  {
    return _rows.at(idx);
  }

  /** Return name of column at idx; empty string indicates no name given.
    *  An idx >= width is undefined.*/
  std::string col_name(size_t idx)
  {
    return _cols.at(idx);
  }

  /** Return type of column at idx. An idx >= width is undefined. */
  char col_type(size_t idx)
  {
    std::string str_type = _types.at(idx);
    return str_type.c_str()[0];
  }

  /** Given a column name return its index, or -1. */
  int col_idx(const char *name)
  {
    for (size_t i = 0; i < width(); ++i)
    {
      if (_cols.at(i) == name)
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
      if (_rows.at(i) == name)
      {
        return i;
      }
    }
    return -1;
  }

  /** The number of columns */
  size_t width()
  {
    return _types.size();
  }

  /** The number of rows */
  size_t length()
  {
    return _rows.size();
  }

public:
  std::vector<std::string> _types;
  std::vector<std::string> _rows;
  std::vector<std::string> _cols;
};