/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once

#include <iostream>
#include <string>
#include <vector>

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'D'.
 */
class Schema
{
public:
  std::vector<std::string> _types;
  size_t nrows_ = 0;

  /** Copying constructor */
  Schema(Schema &from)
  {
    if (this != &from)
    {
      for (auto s : from._types)
      {
        _types.push_back(s);
      }
    }
    nrows_ = from.nrows_;
  }

  /** Create an empty schema **/
  Schema() = default;

  /** Create a schema from a string of types. A string that contains
   * characters other than those identifying the four type results in
   * undefined behavior. The argument is external, a empty string argument is
   * undefined. **/
  Schema(const char *types)
  {
    for (size_t i = 0; i < strlen(types); ++i)
    {
      if (types[i] == 'B' || types[i] == 'I' || types[i] == 'D' ||
          types[i] == 'S')
      {
        add_column(types[i]);
      }
    }
  }

  Schema(std::vector<std::string> types, size_t nrows) : _types(types), nrows_(nrows) {}

  void serialize(Serializer &ser)
  {
    ser.write_string_vector(_types);
    ser.write_size_t(nrows_);
  }

  static std::shared_ptr<Schema> deserialize(Deserializer &dser)
  {
    std::vector<std::string> types = dser.read_string_vector();
    size_t nrows = dser.read_size_t();
    return std::make_shared<Schema>(types, nrows);
  }

  virtual ~Schema()
  {
    _types.clear();
  }

  /** Add a column of the given type and name (can be empty string), name
   * is external. Names are expectd to be unique, duplicates result
   * in undefined behavior. */
  void add_column(char typ)
  {
    _types.push_back(std::string(1, typ));
  }

  void add_row()
  {
    nrows_ += 1;
  }

  /** Return type of column at idx. An idx >= width is undefined. */
  char col_type(size_t idx)
  {
    std::string str_type = _types.at(idx);
    return str_type.c_str()[0];
  }

  /** The number of columns */
  size_t width() { return _types.size(); }

  /** The number of rows */
  size_t length() { return nrows_; }
};