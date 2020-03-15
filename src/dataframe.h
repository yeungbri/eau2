/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once

#include <thread>
#include "helper.h"
#include "schema.h"
#include "column.h"
#include "rower.h"
#include "fielder.h"
#include "row.h"

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame
{
public:
  Schema &_schema;
  std::vector<Column*> _columns;
  std::vector<Row*> _rows;
  static const int THREAD_COUNT = 4;

  /** Create a data frame with the same columns as the given df but with no rows or rownames */
  DataFrame(DataFrame &df) : _schema(df.get_schema())
  {
    _schema._rows.clear();
  }

  /** Create a data frame from a schema and columns. All columns are created
    * empty. */
  DataFrame(Schema &schema) : _schema(schema)
  {
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
      _columns.push_back(col);
    }
    for (size_t i = 0; i < _schema.length(); ++i)
    {
      Row *row = new Row(_schema, "");
      _rows.push_back(row);
    }
  }

  virtual ~DataFrame() {
    // for (auto row : _rows)
    // {
    //   delete row;
    // }
    // for (auto col : _columns)
    // {
    //   delete col;
    // }
    _columns.clear();
    _rows.clear();
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
  void add_column(Column *col, std::string name)
  {
    if (col)
    {
      _schema.add_column(col->get_type(), name);
      _columns.push_back(col);
      for (size_t i = 0; i < nrows(); ++i)
      {
        switch (_schema.col_type(i))
        {
        case 'B':
          _rows.at(i)->push_back(static_cast<BoolColumn *>(col)->get(i));
          break;
        case 'I':
          _rows.at(i)->push_back(static_cast<IntColumn *>(col)->get(i));
          break;
        case 'F':
          _rows.at(i)->push_back(static_cast<FloatColumn *>(col)->get(i));
          break;
        case 'S':
          _rows.at(i)->push_back(static_cast<StringColumn *>(col)->get(i));
          break;
        }
      }
    }
  }

  /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
  int get_int(size_t col, size_t row)
  {
    return _rows.at(row)->get_int(col);
  }

  bool get_bool(size_t col, size_t row)
  {
    return _rows.at(row)->get_bool(col);
  }

  float get_float(size_t col, size_t row)
  {
    return _rows.at(row)->get_float(col);
  }

  std::string get_string(size_t col, size_t row)
  {
    return _rows.at(row)->get_string(col);
  }

  /** Return the offset of the given column name or -1 if no such col. */
  int get_col(std::string &col)
  {
    return _schema.col_idx(col.c_str());
  }

  /** Return the offset of the given row name or -1 if no such row. */
  int get_row(std::string &row)
  {
    return _schema.row_idx(row.c_str());
  }

  /** Set the value at the given column and row to the given value.
    * If the column is not  of the right type or the indices are out of
    * bound, the result is undefined. */
  void set(size_t col, size_t row, int val)
  {
    static_cast<IntColumn *>(_columns.at(col))->set(row, val);
    _rows.at(row)->set(col, val);
  }

  void set(size_t col, size_t row, bool val)
  {
    static_cast<BoolColumn *>(_columns.at(col))->set(row, val);
    _rows.at(row)->set(col, val);
  }

  void set(size_t col, size_t row, float val)
  {
    static_cast<FloatColumn *>(_columns.at(col))->set(row, val);
    _rows.at(row)->set(col, val);
  }

  void set(size_t col, size_t row, std::string val)
  {
    static_cast<StringColumn *>(_columns.at(col))->set(row, val);
    _rows.at(row)->set(col, val);
  }

  /** Set the fields of the given row object with values from the columns at
    * the given offset.  If the row is not form the same schema as the
    * dataframe, results are undefined.
    */
  void fill_row(size_t idx, Row &row)
  {
    Row *source = _rows.at(idx);
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
    _rows.push_back(newRow);
    _schema.add_row(row._name);
    for (size_t i = 0; i < ncols(); ++i)
    {
      switch (row.col_type(i))
      {
      case 'B':
        _columns.at(i)->push_back(row.get_bool(i));
        break;
      case 'I':
        _columns.at(i)->push_back(row.get_int(i));
        break;
      case 'F':
        _columns.at(i)->push_back(row.get_float(i));
        break;
      case 'S':
        _columns.at(i)->push_back(row.get_string(i));
        break;
      }
    }
  }

  /** The number of rows in the dataframe. */
  size_t nrows()
  {
    return _rows.size();
  }

  /** The number of columns in the dataframe.*/
  size_t ncols()
  {
    return _columns.size();
  }

    /**
   * Method that pmap threads execute.
   */
  void pmap_help(Rower &r, size_t start)
  {
    for (size_t i = start; i < nrows(); i += THREAD_COUNT)
    {
      r.accept(*_rows.at(i));
    }
  }

  /** This method clones the Rower and executes the map in parallel. Join is
  * used at the end to merge the results. */
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
      r.accept(*(_rows.at(i)));
    }
  }

  /** Create a new dataframe, constructed from rows for which the given Rower
    * returned true from its accept method. */
  DataFrame *filter(Rower &r)
  {
    DataFrame *df = new DataFrame(*this);
    for (size_t i = 0; i < nrows(); i++)
    {
      Row &currRow = *(_rows.at(i));
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

    for (size_t i = 0; i < _rows.size(); ++i)
    {
      Row *row = _rows.at(i);
      rower->accept(*row);
    }
  }
};