/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once

#include <thread>
#include "column.h"
#include "fielder.h"
#include "row.h"
#include "rower.h"
#include "schema.h"
#include "../util/reader.h"
#include "../util/writer.h"

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
  Schema schema_;
  std::vector<std::shared_ptr<Column>> cols_;
  static const int THREAD_COUNT = 4;

  /** Create a data frame with the same columns as the given df but with no rows
   * or rownames */
  DataFrame(DataFrame &df) : schema_(df.get_schema()) {}

  /** Create a data frame from a schema and columns. All columns are created
   * empty. */
  DataFrame(Schema &schema) : schema_(schema)
  {
    for (size_t i = 0; i < schema.width(); ++i)
    {
      std::shared_ptr<Column> col;
      switch (schema.col_type(i))
      {
      case 'B':
        col = std::make_shared<BoolColumn>();
        break;
      case 'I':
        col = std::make_shared<IntColumn>();
        break;
      case 'D':
        col = std::make_shared<DoubleColumn>();
        break;
      case 'S':
        col = std::make_shared<StringColumn>();
        break;
      default:
        throw std::runtime_error("bad column type!");
      }
      cols_.push_back(col);
    }
  }

  virtual ~DataFrame()
  {
    cols_.clear();
  }

  /** Returns the dataframe's schema. Modifying the schema after a dataframe
   * has been created is undefined. */
  Schema &get_schema() { return schema_; }

  /** Adds a column this dataframe, updates the schema, the new column
   * is external, and appears as the last column of the dataframe. A
   * nullptr column is undefined. */
  void add_column(std::shared_ptr<Column> col)
  {
    if (col)
    {
      schema_.add_column(col->get_type());
      cols_.push_back(col);
    }
  }

  /** Add a row at the end of this dataframe. The row is expected to have
   *  the right schema and be filled with values, otherwise undefined.  */
  void add_row(Row &row, std::shared_ptr<KVStore> store)
  {
    schema_.add_row();
    for (size_t i = 0; i < ncols(); ++i)
    {
      switch (row.col_type(i))
      {
      case 'B':
        cols_.at(i)->push_back(row.get_bool(i), store);
        break;
      case 'I':
        cols_.at(i)->push_back(row.get_int(i), store);
        break;
      case 'D':
        cols_.at(i)->push_back(row.get_double(i), store);
        break;
      case 'S':
        cols_.at(i)->push_back(row.get_string(i), store);
        break;
      }
    }
  }

  /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
  int get_int(size_t col, size_t row, std::shared_ptr<KVStore> store)
  {
    return cols_.at(col)->as_int()->get(row, store);
  }

  bool get_bool(size_t col, size_t row, std::shared_ptr<KVStore> store)
  {
    return cols_.at(col)->as_bool()->get(row, store);
  }

  double get_double(size_t col, size_t row, std::shared_ptr<KVStore> store)
  {
    return cols_.at(col)->as_double()->get(row, store);
  }

  std::string get_string(size_t col, size_t row, std::shared_ptr<KVStore> store)
  {
    return cols_.at(col)->as_string()->get(row, store);
  }

  /** Set the fields of the given row object with values from the columns at
   * the given offset.  If the row is not from the same schema as the
   * dataframe, results are undefined.
   */
  void fill_row(size_t idx, Row &row, std::shared_ptr<KVStore> store)
  {
    for (size_t i = 0; i < ncols(); i++)
    {
      auto col = cols_.at(i);
      switch (col->get_type())
      {
      case 'B':
        row.set(i, col->as_bool()->get(idx, store));
        break;
      case 'I':
        row.set(i, col->as_int()->get(idx, store));
        break;
      case 'D':
        row.set(i, col->as_double()->get(idx, store));
        break;
      case 'S':
        row.set(i, col->as_string()->get(idx, store));
        break;
      }
    }
  }

  /** The number of rows in the dataframe. */
  size_t nrows() { return schema_.length(); }

  /** The number of columns in the dataframe.*/
  size_t ncols() { return schema_.width(); }

  /** Visit rows in order */
  void map(Rower &r, std::shared_ptr<KVStore> store) {
    for (size_t i = 0; i < nrows(); ++i) {
      Row row(schema_);
      fill_row(i, row, store);
      r.accept(row);
    }
  }

  void map(Reader& reader) {
    // TODO for m5
  }

  void local_map(Reader& r, std::shared_ptr<KVStore> store)
  {
    Row row(schema_);
    for (size_t i = 0; i < nrows(); ++i)
    {
      fill_row(i, row, store);
      r.visit(row);
    }
  }

  /** Print the dataframe in SoR format to standard output. */
  void print(std::shared_ptr<KVStore> store)
  {
    auto rower = std::make_shared<PrintRower>();
    for (size_t i = 0; i < nrows(); ++i)
    {
      Row row(schema_);
      fill_row(i, row, store);
      rower->accept(row);
    }
  }

  /**
   * Serializes this dataframe into a byte stream.
   */
  void serialize(Serializer &ser)
  {
    schema_.serialize(ser);
    for (size_t i = 0; i < ncols(); i++)
    {
      cols_.at(i)->serialize(ser);
    }
  }

  /**
   * Deserializes the dataframe given a byte stream, and returns a pointer
   * to a new dataframe object.
   */
  static std::shared_ptr<DataFrame> deserialize(Deserializer &dser)
  {
    auto schema = Schema::deserialize(dser);

    std::vector<std::shared_ptr<Column>> cols;
    for (size_t i = 0; i < schema->width(); i++)
    {
      std::shared_ptr<Column> c;
      switch (schema->col_type(i))
      {
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

    auto df = std::make_shared<DataFrame>(*schema);
    df->cols_ = cols;
    return df;
  }

  /** Returns a dataframe with sz values and puts it in the key value store
   *  under the key */
  static std::shared_ptr<DataFrame> fromArray(std::shared_ptr<Key> key, std::shared_ptr<KVStore> store, std::vector<double> vals)
  {
    Schema s;
    auto res = std::make_shared<DataFrame>(s);
    auto dc = std::make_shared<DoubleColumn>();
    for (double val : vals)
    {
      dc->push_back(val, store);
    }
    res->add_column(dc);

    Serializer ser;
    res->serialize(ser);
    auto value = std::make_shared<Value>(ser.data(), ser.length());
    store->put(*key, *value);
    return res;
  }

  /** Returns a dataframe with a single scalar double */
  static std::shared_ptr<DataFrame> fromScalar(std::shared_ptr<Key> key, std::shared_ptr<KVStore> store, double val)
  {
    Schema s;
    auto res = std::make_shared<DataFrame>(s);
    auto dc = std::make_shared<DoubleColumn>();
    dc->push_back(val, store);
    res->add_column(dc);

    Serializer ser;
    res->serialize(ser);
    auto value = std::make_shared<Value>(ser.data(), ser.length());
    store->put(*key, *value);
    return res;
  }

  /** Returns a dataframe with a single scalar int */
  static std::shared_ptr<DataFrame> fromScalarInt(std::shared_ptr<Key> key, std::shared_ptr<KVStore> store, int val)
  {
    Schema s;
    auto res = std::make_shared<DataFrame>(s);
    auto dc = std::make_shared<IntColumn>();
    dc->push_back(val, store);
    res->add_column(dc);

    Serializer ser;
    res->serialize(ser);
    auto value = std::make_shared<Value>(ser.data(), ser.length());
    store->put(*key, *value);
    return res;
  }

  static std::shared_ptr<DataFrame> fromVisitor(
    std::shared_ptr<Key> key, std::shared_ptr<KVStore> store, std::string col_types, Writer& count)
  {
    Schema s(col_types.c_str());
    auto res = std::make_shared<DataFrame>(s);
    while (!count.done())
    {
      auto row = std::make_shared<Row>(s);
      count.visit(*row);
      res->add_row(*row, store);
    }
    Serializer ser;
    res->serialize(ser);
    auto value = std::make_shared<Value>(ser.data(), ser.length());
    store->put(*key, *value);
    return res;
  }

  static std::shared_ptr<DataFrame> fromFile(std::string file, std::shared_ptr<Key> key, std::shared_ptr<KVStore> store)
  {
    return std::make_shared<DataFrame>();
  }
};