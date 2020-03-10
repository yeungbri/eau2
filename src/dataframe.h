/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 * CS4500 Assignment 4
 */

//lang::CwC

#pragma once

#include <thread>
#include "object.h"
#include "string.h"
#include "array.h"
#include "helper.h"
#include "schema.h"
#include "column.h"
#include "rower.h"
#include "fielder.h"
#include "row.h"
#include "thread.h"

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame : public Object
{
public:
  Schema &_schema;
  ColumnArray *_columns;
  RowArray *_rows;
  static const int THREAD_COUNT = 4;

  /** Create a data frame with the same columns as the given df but with no rows or rownames */
  DataFrame(DataFrame &df) : _schema(df.get_schema())
  {
    Schema* schema = new Schema(df.get_schema());
    int schema_rows = schema->length();
    for (size_t i=0; i<schema_rows; i++) {
      schema->_rows->remove(i);
    }
    _schema = *schema;
    _columns = new ColumnArray(*df._columns);
    _rows = new RowArray(0);
  }

  /** Create a data frame from a schema and columns. All columns are created
    * empty. */
  DataFrame(Schema &schema) : _schema(schema)
  {
    _columns = new ColumnArray(_schema.width());
    for (size_t i = 0; i < _schema.width(); ++i)
    {
      Column *col;
      switch (schema.col_type(i))
      {
      case 'B':
        col = new BoolColumn();
        break;
      case 'I':
        col = new IntColumn();
        break;
      case 'F':
        col = new FloatColumn();
        break;
      case 'S':
        col = new StringColumn();
        break;
      default:
        throw std::runtime_error("bad column type!");
      }
      _columns->push_back(col);
    }
    _rows = new RowArray(_schema.length());
    for (size_t i = 0; i < _schema.length(); ++i)
    {
      Row *row = new Row(_schema, nullptr);
      _rows->push_back(row);
    }
  }

  virtual ~DataFrame()
  {
    delete _columns;
    delete _rows;
  }

  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema &get_schema()
  {
    return _schema;
  }

  /** Adds a column this dataframe, updates the schema, the new column
    * is external, and appears as the last column of the dataframe, the
    * name is optional and external. A nullptr colum is undefined. */
  void add_column(Column *col, String *name)
  {
    if (col)
    {
      _schema.add_column(col->get_type(), name);
      _columns->push_back(col);
      for (size_t i = 0; i < nrows(); ++i)
      {
        switch (_schema.col_type(i))
        {
        case 'B':
          _rows->get(i)->push_back(static_cast<BoolColumn *>(col)->get(i));
          break;
        case 'I':
          _rows->get(i)->push_back(static_cast<IntColumn *>(col)->get(i));
          break;
        case 'F':
          _rows->get(i)->push_back(static_cast<FloatColumn *>(col)->get(i));
          break;
        case 'S':
          _rows->get(i)->push_back(static_cast<StringColumn *>(col)->get(i));
          break;
        }
      }
    }
  }

  /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
  int get_int(size_t col, size_t row)
  {
    auto tempRow = _rows->get(row);
    auto tempCol = tempRow->get_int(col);
    return _rows->get(row)->get_int(col);
  }

  bool get_bool(size_t col, size_t row)
  {
    return _rows->get(row)->get_bool(col);
  }

  float get_float(size_t col, size_t row)
  {
    return _rows->get(row)->get_float(col);
  }

  String *get_string(size_t col, size_t row)
  {
    return _rows->get(row)->get_string(col);
  }

  /** Return the offset of the given column name or -1 if no such col. */
  int get_col(String &col)
  {
    return _schema.col_idx(col.c_str());
  }

  /** Return the offset of the given row name or -1 if no such row. */
  int get_row(String &row)
  {
    return _schema.row_idx(row.c_str());
  }

  /** Set the value at the given column and row to the given value.
    * If the column is not  of the right type or the indices are out of
    * bound, the result is undefined. */
  void set(size_t col, size_t row, int val)
  {
    static_cast<IntColumn *>(_columns->get(col))->set(row, val);
    _rows->get(row)->set(col, val);
  }

  void set(size_t col, size_t row, bool val)
  {
    static_cast<BoolColumn *>(_columns->get(col))->set(row, val);
    _rows->get(row)->set(col, val);
  }

  void set(size_t col, size_t row, float val)
  {
    static_cast<FloatColumn *>(_columns->get(col))->set(row, val);
    _rows->get(row)->set(col, val);
  }

  void set(size_t col, size_t row, String *val)
  {
    static_cast<StringColumn *>(_columns->get(col))->set(row, val);
    _rows->get(row)->set(col, val);
  }

  /** Set the fields of the given row object with values from the columns at
    * the given offset.  If the row is not form the same schema as the
    * dataframe, results are undefined.
    */
  void fill_row(size_t idx, Row &row)
  {
    Row *source = _rows->get(idx);
    for (size_t i = 0; i < source->width(); ++i)
    {
      switch (_schema.col_type(i))
      {
      case 'B':
        source->set(i, row.get_bool(i));
        break;
      case 'I':
        source->set(i, row.get_int(i));
        break;
      case 'F':
        source->set(i, row.get_float(i));
        break;
      case 'S':
        source->set(i, row.get_string(i));
        break;
      }
    }
  }

  /** Add a row at the end of this dataframe. The row is expected to have
   *  the right schema and be filled with values, otherwise undedined.  */
  void add_row(Row &row)
  {
    Row *newRow = new Row(row);
    _rows->push_back(newRow);
    _schema.add_row(row._name);
    for (size_t i = 0; i < ncols(); ++i)
    {
      switch (row.col_type(i))
      {
      case 'B':
        _columns->get(i)->push_back(row.get_bool(i));
        break;
      case 'I':
        _columns->get(i)->push_back(row.get_int(i));
        break;
      case 'F':
        _columns->get(i)->push_back(row.get_float(i));
        break;
      case 'S':
        _columns->get(i)->push_back(row.get_string(i));
        break;
      }
    }
  }

  /** The number of rows in the dataframe. */
  size_t nrows()
  {
    return _rows->length();
  }

  /** The number of columns in the dataframe.*/
  size_t ncols()
  {
    return _columns->length();
  }

    /**
   * Method that pmap threads execute.
   */
  void pmap_help(Rower &r, size_t start)
  {
    for (size_t i = start; i < nrows(); i += THREAD_COUNT)
    {
      r.accept(*_rows->get(i));
    }
  }

  /** This method clones the Rower and executes the map in parallel. Join is
  * used at the end to merge the results. */
  // void pmap(Rower &r)
  // {
  //   Rower** rowers = new Rower*[THREAD_COUNT];
  //   for (int i = 0; i < THREAD_COUNT; ++i)
  //   {
  //     rowers[i] = dynamic_cast<Rower*>(r.clone());
  //   }
  //   std::thread threads[THREAD_COUNT];
  //   for (int i = 0; i < THREAD_COUNT; ++i)
  //   {
  //     size_t nrows = this->nrows();
  //     threads[i] = RowerThread()
  //   }
  //   for (int i = 0; i < THREAD_COUNT; ++i)
  //   {
  //     threads[i].join();
  //   std::cout << "hi\n";
  //   }
  //   for (int i = 0; i < THREAD_COUNT; ++i)
  //   {
  //     r.join_delete(rowers[i]);
  //   }
  // }

  void pmap(Rower &r)
  {
    Rower** rowers = new Rower*[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; ++i)
    {
      rowers[i] = static_cast<Rower*>(r.clone());
    }
    std::thread threads[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; ++i)
    {
      size_t nrows = this->nrows();
      threads[i] = std::thread(&DataFrame::pmap_help, this, std::ref(*rowers[i]), i);
    }
    for (int i = 0; i < THREAD_COUNT; ++i)
    {
      threads[i].join();
    }
    for (int i = 0; i < THREAD_COUNT; ++i)
    {
      r.join_delete(rowers[i]);
    }
  }

  /** Visit rows in order */
  void map(Rower &r)
  {
    for (size_t i = 0; i < _schema.length(); ++i)
    {
      r.accept(*(_rows->get(i)));
    }
  }

  /** Create a new dataframe, constructed from rows for which the given Rower
    * returned true from its accept method. */
  DataFrame *filter(Rower &r)
  {
    DataFrame *df = new DataFrame(*this);
    for (size_t i = 0; i < nrows(); i++)
    {
      Row &currRow = *(_rows->get(i));
      if (r.accept(currRow))
      {
        df->add_row(currRow);
      }
    }
    return df;
  }

  /** Print the dataframe in SoR format to standard output. */
  void print()
  {
    Rower *rower = new PrintRower();

    for (size_t i = 0; i < _rows->length(); ++i)
    {
      Row *row = _rows->get(i);
      rower->accept(*row);
    }
  }
};