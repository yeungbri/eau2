/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once

#include <thread>

#include "column.h"
#include "fielder.h"
#include "helper.h"
#include "row.h"
#include "rower.h"
#include "schema.h"
#include "kvstore.h"

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame {
 public:
  Schema &schema_;
  std::vector<Column *> cols_;
  static const int THREAD_COUNT = 4;

  /** Create a data frame with the same columns as the given df but with no rows
   * or rownames */
  DataFrame(DataFrame &df) : schema_(df.get_schema()) {
    //schema_._row_names.clear();
  }

  /** Create a data frame from a schema and columns. All columns are created
   * empty. */
  DataFrame(Schema &schema) : schema_(schema) {
    for (size_t i = 0; i < ncols(); ++i) {
      Column *col;
      switch (schema.col_type(i)) {
        case 'B':
          col = new BoolColumn();
          break;
        case 'I':
          col = new IntColumn();
          break;
        case 'D':
          col = new DoubleColumn();
          break;
        case 'S':
          col = new StringColumn();
          break;
        default:
          throw std::runtime_error("bad column type!");
      }
      cols_.push_back(col);
    }
  }

  virtual ~DataFrame() {
    // for (auto col : cols_)
    // {
    //   delete col;
    // }
    cols_.clear();
  }

  /** Returns the dataframe's schema. Modifying the schema after a dataframe
   * has been created is undefined. */
  Schema &get_schema() { return schema_; }

  /** Adds a column this dataframe, updates the schema, the new column
   * is external, and appears as the last column of the dataframe, the
   * name is optional and external. A nullptr colum is undefined. */
  void add_column(Column *col, std::string name) {
    if (col) {
      schema_.add_column(col->get_type(), name);
      cols_.push_back(col);
    }
  }

  /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
  int get_int(size_t col, size_t row) {
    return cols_.at(col)->as_int()->get(row);
  }

  bool get_bool(size_t col, size_t row) {
    return cols_.at(col)->as_bool()->get(row);
  }

  double get_double(size_t col, size_t row) {
    return cols_.at(col)->as_double()->get(row);
  }

  std::string get_string(size_t col, size_t row) {
    return cols_.at(col)->as_string()->get(row);
  }

  /** Return the offset of the given column name or -1 if no such col. */
  int get_col(std::string &col) { return schema_.col_idx(col.c_str()); }

  /** Return the offset of the given row name or -1 if no such row. */
  int get_row(std::string &row) { return schema_.row_idx(row.c_str()); }

  /** Set the value at the given column and row to the given value.
   * If the column is not  of the right type or the indices are out of
   * bound, the result is undefined. */
  void set(size_t col, size_t row, bool val) {
    cols_.at(col)->as_bool()->set(row, val);
  }

  void set(size_t col, size_t row, int val) {
    cols_.at(col)->as_int()->set(row, val);
  }

  void set(size_t col, size_t row, double val) {
    cols_.at(col)->as_double()->set(row, val);
  }

  void set(size_t col, size_t row, std::string val) {
    cols_.at(col)->as_string()->set(row, val);
  }

  /** Set the fields of the given row object with values from the columns at
   * the given offset.  If the row is not from the same schema as the
   * dataframe, results are undefined.
   */
  void fill_row(size_t idx, Row &row) {
    for (size_t i = 0; i < ncols(); i++) {
      Column *col = cols_.at(i);
      switch (col->get_type()) {
        case 'B':
          row.set(i, col->as_bool()->get(idx));
          break;
        case 'I':
          row.set(i, col->as_int()->get(idx));
          break;
        case 'D':
          row.set(i, col->as_double()->get(idx));
          break;
        case 'S':
          row.set(i, col->as_string()->get(idx));
          break;
      }
    }
  }

  /** Add a row at the end of this dataframe. The row is expected to have
   *  the right schema and be filled with values, otherwise undefined.  */
  void add_row(Row &row) {
    schema_.add_row(row._name);
    for (size_t i = 0; i < ncols(); ++i) {
      switch (row.col_type(i)) {
        case 'B':
          cols_.at(i)->push_back(row.get_bool(i));
          break;
        case 'I':
          cols_.at(i)->push_back(row.get_int(i));
          break;
        case 'D':
          cols_.at(i)->push_back(row.get_double(i));
          break;
        case 'S':
          cols_.at(i)->push_back(row.get_string(i));
          break;
      }
    }
  }

  /** The number of rows in the dataframe. */
  size_t nrows() { return schema_.length(); }

  /** The number of columns in the dataframe.*/
  size_t ncols() { return schema_.width(); }

  /** Visit rows in order */
  void map(Rower &r) {
    for (size_t i = 0; i < nrows(); ++i) {
      Row row(schema_);
      fill_row(i, row);
      r.accept(row);
    }
  }

  /** Print the dataframe in SoR format to standard output. */
  void print() {
    Rower *rower = new PrintRower();
    for (size_t i = 0; i < nrows(); ++i) {
      Row row(schema_);
      fill_row(i, row);
      rower->accept(row);
    }
    delete rower;
  }

  void serialize(Serializer& ser) {
    schema_.serialize(ser);
    for (size_t i=0; i<ncols(); i++) {
      cols_.at(i)->serialize(ser);
    }
  }

  static DataFrame* deserialize(Deserializer& dser) 
  {
    Schema* schema = Schema::deserialize(dser);

    std::vector<Column*> cols;
    for (size_t i=0; i<schema->width(); i++) {
      Column* c;
      switch (schema->col_type(i)) {
        case 'B':
          c = BoolColumn::deserialize(dser);
          break;
        case 'I':
          c = IntColumn::deserialize(dser);
          break;
        case 'D':
          c = DoubleColumn::deserialize(dser);
          break;
        case 'S':
          c = StringColumn::deserialize(dser);
          break;
        default:
          throw std::runtime_error("bad column type!");
      }
      cols.push_back(c);
    }

    DataFrame* df = new DataFrame(*schema);
    df->cols_ = cols;
    return df;
  }

  /** Returns a dataframe with sz values and puts it in the key value store
   *  under the key */
  static DataFrame *fromArray(Key *key, KVStore *store,
                              std::vector<double> vals) {
    Schema s;
    DataFrame *res = new DataFrame(s);
    DoubleColumn *dc = new DoubleColumn(vals);
    res->add_column(dc, "Double column");

    Serializer ser;
    res->serialize(ser);
    Value* value = new Value(ser.data());
    store->put(*key, *value);
    return res;
  }
};