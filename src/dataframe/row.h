/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <string>
#include <cstring>
#include <vector>
#include "schema.h"
#include "fielder.h"
#include "wrapper.h"

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
  Schema _schema;

  // Attribution: https://stackoverflow.com/a/18577481/12602247 at 2/11 7:52PM
  struct Data
  {
    enum
    {
      is_int,
      is_double,
      is_bool,
      is_string,
      is_missing
    } type;
    union {
      int ival;
      double fval;
      bool bval;
      char *sval;
      char *mval;
    } val;
  };

  std::vector<std::shared_ptr<Data>> _elements;

  /**
   * Constructs a row given a schema. All elements are missing at initialization.
   */
  Row(Schema &scm)
  {
    _schema = scm;
    for (size_t i = 0; i < _schema.width(); i++)
    {
      auto element = std::make_shared<Data>();
      element->type = Data::is_missing;
      element->val.mval = new char[0];
      _elements.push_back(element);
    }
  }

  /**
   * Row copy constructor. Fills in the values from the old row.
   */
  Row(Row &row)
  {
    _schema = row._schema;
    for (size_t i = 0; i < _schema.width(); ++i)
    {
      auto element = std::make_shared<Data>();
      element->type = row._elements[i]->type;
      element->val = row._elements[i]->val;
      _elements.push_back(element);
    }
  }

  virtual ~Row()
  {
    _elements.clear();
  }

  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
  void set(size_t col, Int val)
  {
    if (_schema.col_type(col) == 'I')
    {
      if (val.is_missing())
      {
        _elements[col]->type = Data::is_missing;
      } else
      {
        _elements[col]->type = Data::is_int;
        _elements[col]->val.ival = val.val();;
      }
    }
  }
  void set(size_t col, Double val)
  {
    if (_schema.col_type(col) == 'D')
    {
      if (val.is_missing())
      {
        _elements[col]->type = Data::is_missing;
      } else
      {
        _elements[col]->type = Data::is_double;
        _elements[col]->val.fval = val.val();
      }
    }
  }
  void set(size_t col, Bool val)
  {
    if (_schema.col_type(col) == 'B')
    {
      if (val.is_missing())
      {
        _elements[col]->type = Data::is_missing;
      } else
      {
        _elements[col]->type = Data::is_bool;
        _elements[col]->val.bval = val.val();
      }
    }
  }
  void set(size_t col, String val)
  {
    if (_schema.col_type(col) == 'S')
    {
      if (val.is_missing())
      {
        _elements[col]->type = Data::is_missing;
      } else
      {
        _elements[col]->type = Data::is_string;
        _elements[col]->val.sval = strdup(val.val().c_str());
      }
    }
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

  double get_double(size_t col)
  {
    return _elements[col]->val.fval;
  }

  std::string get_string(size_t col)
  {
    // there is a chance that sval is a nullptr, because it's a char* type
    auto val = _elements[col]->val.sval;
    if (val)
    {
      return val;
    } else
    {
      return "";
    }
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
      case Data::is_double:
        return 'D';
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
        case 'D':
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

  /**
   * Marks the element at the specified index as missing.
   * Adds gibberish values.
   */
  void set_missing(int idx)
  {
    _elements[idx]->type = Data::is_missing;
    _elements[idx]->val.bval = false;
    _elements[idx]->val.ival = 0;
    _elements[idx]->val.fval = 0;
    _elements[idx]->val.sval = nullptr;
  }

  /**
   * Returns true if the value at the given index is missing, false otherwise.
   */
  bool is_missing(int idx)
  {
    return _elements[idx]->type == Data::is_missing;
  }
};
