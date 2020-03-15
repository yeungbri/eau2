/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#include <gtest/gtest.h>
#include <iostream>
#include "../src/dataframe.h"
#include <string>
#include <vector>

#define ASSERT_EXIT_ZERO(a) ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*")

std::string s1 = "Hello";
std::string s2 = "Bye Bye";
std::string empty_string = "";
std::string col_name = "New Column";
std::string row_name = "New Row";
std::string s3 = "a";
std::string s4 = "1./0v^#$&%*";
std::string fcolName = "float";
std::string icolName = "int";
std::string bcolName = "bool";
std::string scolName = "String";
std::string apple = "apple";
std::string pear = "pear";
std::string orange = "orange";
Schema fibsfibs("FIBSFIBS");
Schema fibs("FIBS");

std::vector<Row*> generateTenRowsFIBSFIBS()
{
  std::vector<Row*> rows;
  for (size_t i = 0; i < 10; ++i)
  {
    std::string name = std::to_string(i);
    Row *row = new Row(fibsfibs, name);
    row->set(0, float(1.0));
    row->set(1, int(2));
    row->set(2, true);
    row->set(3, s1);
    row->set(4, float(69.0));
    row->set(5, int(420));
    row->set(6, false);
    row->set(7, s2);
    rows.push_back(row);
  }
  return rows;
}

std::vector<Row*> generateTenRowsFIBS()
{
  std::vector<Row*> rows;
  for (size_t i = 0; i < 10; ++i)
  {
    std::string name = std::to_string(i);
    Row *row = new Row(fibs, name);
    row->set(0, float(0.5 + i * 1.0));
    row->set(1, int(i * 2));
    row->set(2, i % 2 == 0);
    row->set(3, s1);
    rows.push_back(row);
  }
  return rows;
}

// Test Schema
TEST(a4, testSchema)
{
  Schema s("FBIS");
  EXPECT_EQ(s.width(), 4);
  EXPECT_EQ(s.length(), 0);

  s.add_column('S', col_name);
  s.add_column('F', col_name);
  EXPECT_EQ(s.col_name(1), "");
  EXPECT_EQ(s.col_name(2), "");
  EXPECT_EQ(s.col_name(3), "");
  EXPECT_EQ(s.col_name(4), col_name);

  EXPECT_EQ(s.col_type(0), 'F');
  EXPECT_EQ(s.col_type(1), 'B');
  EXPECT_EQ(s.col_type(2), 'I');
  EXPECT_EQ(s.col_type(3), 'S');
  EXPECT_EQ(s.col_type(4), 'S');

  EXPECT_EQ(s.col_idx(""), 0);
  // Return first col with that name in case of duplicates
  EXPECT_EQ(s.col_idx("New Column"), 4);

  s.add_row(row_name);
  EXPECT_EQ(s.row_name(0), row_name);
  EXPECT_EQ(s.row_idx("New Row"), 0);

  EXPECT_EQ(s.width(), 6);
  EXPECT_EQ(s.length(), 1);

  // Test copy constructor
  Schema s2(s);
  EXPECT_EQ(s2.width(), 6);
  EXPECT_EQ(s2.length(), 1);
  EXPECT_EQ(s2.col_name(1), "");
  EXPECT_EQ(s2.col_name(2), "");
  EXPECT_EQ(s2.col_name(3), "");
  EXPECT_EQ(s2.col_name(4), col_name);
  EXPECT_EQ(s2.col_type(0), 'F');
  EXPECT_EQ(s2.col_type(1), 'B');
  EXPECT_EQ(s2.col_type(2), 'I');
  EXPECT_EQ(s2.col_type(3), 'S');
  EXPECT_EQ(s2.col_idx(""), 0);
  EXPECT_EQ(s2.col_idx("New Column"), 4);
  EXPECT_EQ(s2.row_name(0), row_name);
  EXPECT_EQ(s2.row_idx("New Row"), 0);
}

// Test Column
TEST(a4, testColumn)
{
  std::vector<bool> bools = {0, 1, 0, 1};
  BoolColumn bc(bools);
  EXPECT_EQ(bc.get(0), 0);
  EXPECT_EQ(bc.get(1), 1);
  EXPECT_EQ(bc.get(2), 0);
  EXPECT_EQ(bc.get(3), 1);

  EXPECT_EQ(bc.as_bool(), &bc);
  EXPECT_EQ(bc.as_int(), nullptr);
  EXPECT_EQ(bc.as_float(), nullptr);
  EXPECT_EQ(bc.as_string(), nullptr);

  EXPECT_EQ(bc.size(), 4);
  EXPECT_EQ(bc.get_type(), 'B');

  bc.set(0, 1);
  EXPECT_EQ(bc.get(0), 1);
  bc.set(0, 0);
  EXPECT_EQ(bc.get(0), 0);

  std::vector<int> ints = {1, 2, 3, -4};
  IntColumn ic(ints);
  EXPECT_EQ(ic.get(0), 1);
  EXPECT_EQ(ic.get(1), 2);
  EXPECT_EQ(ic.get(2), 3);
  EXPECT_EQ(ic.get(3), -4);

  EXPECT_EQ(ic.as_bool(), nullptr);
  EXPECT_EQ(ic.as_int(), &ic);
  EXPECT_EQ(ic.as_float(), nullptr);
  EXPECT_EQ(ic.as_string(), nullptr);

  EXPECT_EQ(ic.size(), 4);
  EXPECT_EQ(ic.get_type(), 'I');

  ic.set(0, 12345);
  EXPECT_EQ(ic.get(0), 12345);
  ic.set(0, -35);
  EXPECT_EQ(ic.get(0), -35);

  std::vector<float> floats = {0.234, -0.678, 123.123, 67.0};
  FloatColumn fc(floats);
  ASSERT_FLOAT_EQ(fc.get(0), 0.234);
  ASSERT_FLOAT_EQ(fc.get(1), -0.678);
  ASSERT_FLOAT_EQ(fc.get(2), 123.123);
  ASSERT_FLOAT_EQ(fc.get(3), 67);

  EXPECT_EQ(fc.as_bool(), nullptr);
  EXPECT_EQ(fc.as_int(), nullptr);
  EXPECT_EQ(fc.as_float(), &fc);
  EXPECT_EQ(fc.as_string(), nullptr);

  EXPECT_EQ(fc.size(), 4);
  EXPECT_EQ(fc.get_type(), 'F');

  fc.set(0, 12345);
  ASSERT_FLOAT_EQ(fc.get(0), 12345);
  fc.set(0, -35);
  ASSERT_FLOAT_EQ(fc.get(0), -35);

  std::vector<std::string> strings = {s1.c_str(), s2.c_str(), s3.c_str(), s4.c_str()};
  StringColumn sc(strings);
  EXPECT_EQ(sc.get(0), s1);
  EXPECT_EQ(sc.get(1), s2);
  EXPECT_EQ(sc.get(2), s3);
  EXPECT_EQ(sc.get(3), s4);

  EXPECT_EQ(sc.as_bool(), nullptr);
  EXPECT_EQ(sc.as_int(), nullptr);
  EXPECT_EQ(sc.as_float(), nullptr);
  EXPECT_EQ(sc.as_string(), &sc);

  EXPECT_EQ(sc.size(), 4);
  EXPECT_EQ(sc.get_type(), 'S');

  sc.set(0, s2);
  EXPECT_EQ(sc.get(0), s2);
  sc.set(0, s3);
  EXPECT_EQ(sc.get(0), s3);
}

// Test Dataframe
TEST(a4, testGetSchema)
{
  Schema schema("FIBS");
  DataFrame df(schema);
  // EXPECT_EQ(df.get_schema()._types, schema._types);
  // EXPECT_EQ(df.ncols(), schema.width());
  // EXPECT_EQ(df.nrows(), schema.length());
  // Schema emptySchema;
  // DataFrame emptyDf(emptySchema);
  // EXPECT_EQ(emptyDf.get_schema()._types, emptySchema._types);
  // EXPECT_EQ(emptyDf.ncols(), emptySchema.width());
  // EXPECT_EQ(emptyDf.nrows(), emptySchema.length());
}

TEST(a4, testAddColumnRow)
{
  // test add columns, verify column names and size
  Schema schema("FIBS");
  Schema schemaCopy("FIBSFIBS");
  DataFrame df(schema);
  EXPECT_EQ(df.ncols(), 4);
  EXPECT_EQ(df.get_schema()._types, schema._types);
  FloatColumn fcol2;
  IntColumn icol2;
  BoolColumn bcol2;
  StringColumn scol2;
  df.add_column(&fcol2, fcolName);
  df.add_column(&icol2, icolName);
  df.add_column(&bcol2, bcolName);
  df.add_column(&scol2, scolName);
  EXPECT_EQ(df.ncols(), 8);
  EXPECT_EQ(df.get_col(fcolName), 4);
  EXPECT_EQ(df.get_col(icolName), 5);
  EXPECT_EQ(df.get_col(bcolName), 6);
  EXPECT_EQ(df.get_col(scolName), 7);

  for (size_t i = 0; i < schemaCopy.width(); ++i)
  {
    EXPECT_EQ(df.get_schema().col_type(i), schemaCopy.col_type(i));
  }

  // test add rows, verify number of rows, and row names
  std::vector<Row*> rows = generateTenRowsFIBSFIBS();
  for (int i = 0; i < 10; ++i)
  {
    Row *row = rows[i];
    df.add_row(*row);
  }

  EXPECT_EQ(df.nrows(), 10);
  for (int i = 0; i < 10; ++i)
  {
    std::string tmp = std::to_string(i);
    EXPECT_EQ(df.get_row(tmp), i);
  }
  for (auto row : rows)
  {
    delete row;
  }
}

TEST(a4, testGetSet)
{
  Schema schema;
  DataFrame df(schema);
  FloatColumn fcol;
  IntColumn icol;
  BoolColumn bcol;
  StringColumn scol;
  for (int i = 0; i < 15; ++i)
  {
    std::string str = std::to_string(i);
    scol.push_back(str);
  }
  df.add_column(&fcol, fcolName);
  df.add_column(&icol, icolName);
  df.add_column(&bcol, bcolName);
  df.add_column(&scol, scolName);
  std::vector<Row*> rows = generateTenRowsFIBS();
  for (int i = 0; i < 10; ++i)
  {
    Row *row = rows[i];
    df.add_row(*row);
  }
  EXPECT_EQ(df.nrows(), 10);
  EXPECT_EQ(df.ncols(), 4);
  EXPECT_FLOAT_EQ(df.get_float(0, 5), 5.5);
  EXPECT_FLOAT_EQ(df.get_float(0, 0), 0.5);
  EXPECT_EQ(df.get_int(1, 5), 10);
  EXPECT_EQ(df.get_int(1, 0), 0);
  EXPECT_EQ(df.get_bool(2, 5), false);
  EXPECT_EQ(df.get_bool(2, 0), true);
  EXPECT_EQ(df.get_string(3, 0) == "Hello", true);
  for (auto row : rows)
  {
    delete row;
  }
}

TEST(a4, testFillRow)
{
  Schema schema("SIS");
  DataFrame df(schema);
  Row r(schema);

  r.set(0, s1);
  r.set(1, 1);
  r.set(2, s2);
  df.add_row(r);

  EXPECT_EQ(df.nrows(), 1);
  EXPECT_EQ(df.get_string(0, 0), s1);
  EXPECT_EQ(df.get_int(1, 0), 1);
  EXPECT_EQ(df.get_string(2, 0), s2);

  Row r2(schema);
  r2.set(0, apple);
  r2.set(1, 2);
  r2.set(2, apple);
  df.fill_row(0, r2);

  EXPECT_EQ(df.nrows(), 1);
  EXPECT_EQ(df.get_string(0, 0), apple);
  EXPECT_EQ(df.get_int(1, 0), 2);
  EXPECT_EQ(df.get_string(2, 0), apple);
}

TEST(a4, testMap)
{
  Schema s("II");
  DataFrame df(s);
  CounterRower countRower;
  IntSumRower intRower;
  Row r(df.get_schema());
  for (int i = 0; i < 1000; i++)
  {
    r.set(0, i);
    r.set(1, i+1);
    df.add_row(r);
  }

  df.map(countRower);
  EXPECT_EQ(countRower._count, 2000);

  df.map(intRower);
  EXPECT_EQ(intRower._sum, 1000000);
}

TEST(a4, testFilter)
{
  StringSearchRower apple_sr("apple");
  Schema schema("SIS");
  DataFrame df(schema);
  Row r(schema);

  for (size_t i = 0; i < 10; i++)
  {
    if (i % 2 == 0)
    {
      r.set(0, apple);
    }
    else
    {
      r.set(0, pear);
    }
    r.set(1, 1);
    r.set(2, orange);
    df.add_row(r);
  }

  DataFrame *apple_df = df.filter(apple_sr);
  EXPECT_EQ(apple_df->nrows(), 5);
  EXPECT_EQ(apple_df->get_string(0, 0), apple);
  EXPECT_EQ(apple_df->get_string(0, 1), apple);
  EXPECT_EQ(apple_df->get_string(0, 2), apple);
  EXPECT_EQ(apple_df->get_string(0, 3), apple);
  EXPECT_EQ(apple_df->get_string(0, 4), apple);
  delete apple_df;
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}