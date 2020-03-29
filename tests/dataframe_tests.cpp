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
std::string s3 = "a";
std::string s4 = "1./0v^#$&%*";
std::string apple = "apple";
std::string pear = "pear";
std::string orange = "orange";
Schema fibsfibs("DIBSDIBS");
Schema fibs("DIBS");

std::vector<std::shared_ptr<Row>> generateTenRowsFIBSFIBS()
{
  std::vector<std::shared_ptr<Row>> rows;
  for (size_t i = 0; i < 10; ++i)
  {
    std::string name = std::to_string(i);
    auto row = std::make_shared<Row>(fibsfibs);
    row->set(0, double(1.0));
    row->set(1, int(2));
    row->set(2, true);
    row->set(3, s1);
    row->set(4, double(69.0));
    row->set(5, int(420));
    row->set(6, false);
    row->set(7, s2);
    rows.push_back(row);
  }
  return rows;
}

std::vector<std::shared_ptr<Row>> generateTenRowsFIBS()
{
  std::vector<std::shared_ptr<Row>> rows;
  for (size_t i = 0; i < 10; ++i)
  {
    std::string name = std::to_string(i);
    auto row = std::make_shared<Row>(fibs);
    row->set(0, double(0.5 + i * 1.0));
    row->set(1, int(i * 2));
    row->set(2, i % 2 == 0);
    row->set(3, s1);
    rows.push_back(row);
  }
  return rows;
}

// Test Schema
TEST(dataframe, testSchema)
{
  Schema s("DBIS");
  EXPECT_EQ(s.width(), 4);
  EXPECT_EQ(s.length(), 0);

  s.add_column('S');
  s.add_column('D');

  EXPECT_EQ(s.col_type(0), 'D');
  EXPECT_EQ(s.col_type(1), 'B');
  EXPECT_EQ(s.col_type(2), 'I');
  EXPECT_EQ(s.col_type(3), 'S');
  EXPECT_EQ(s.col_type(4), 'S');

  s.add_row();

  EXPECT_EQ(s.width(), 6);
  EXPECT_EQ(s.length(), 1);

  // Test copy constructor
  Schema s2(s);
  EXPECT_EQ(s2.width(), 6);
  EXPECT_EQ(s2.length(), 1);
  EXPECT_EQ(s2.col_type(0), 'D');
  EXPECT_EQ(s2.col_type(1), 'B');
  EXPECT_EQ(s2.col_type(2), 'I');
  EXPECT_EQ(s2.col_type(3), 'S');
}

// Test Column
TEST(dataframe, testColumn)
{
  std::vector<bool> bools = {0, 1, 0, 1};
  auto store = std::make_shared<KVStore>(0, nullptr);
  BoolColumn bc;

  for (bool b : bools)
  {
    bc.push_back(b, store);
  }
  EXPECT_EQ(bc.get(0, store), 0);
  // EXPECT_EQ(bc.get(1, store), 1);
  // EXPECT_EQ(bc.get(2, store), 0);
  // EXPECT_EQ(bc.get(3, store), 1);


  // EXPECT_EQ(bc.as_bool().get(), &bc);
  // EXPECT_EQ(bc.as_int(), nullptr);
  // EXPECT_EQ(bc.as_double(), nullptr);
  // EXPECT_EQ(bc.as_string(), nullptr);

  // EXPECT_EQ(bc.size(), 4);
  // EXPECT_EQ(bc.get_type(), 'B');

  // std::vector<int> ints = {1, 2, 3, -4};
  // IntColumn ic;
  // for (int i : ints)
  // {
  //   ic.push_back(i, store);
  // }
  // EXPECT_EQ(ic.get(0, store), 1);
  // EXPECT_EQ(ic.get(1, store), 2);
  // EXPECT_EQ(ic.get(2, store), 3);
  // EXPECT_EQ(ic.get(3, store), -4);

  // EXPECT_EQ(ic.as_bool(), nullptr);
  // EXPECT_EQ(ic.as_int().get(), &ic);
  // EXPECT_EQ(ic.as_double(), nullptr);
  // EXPECT_EQ(ic.as_string(), nullptr);

  // EXPECT_EQ(ic.size(), 4);
  // EXPECT_EQ(ic.get_type(), 'I');

  // std::vector<double> doubles = {0.234, -0.678, 123.123, 67.0};
  // DoubleColumn fc;
  // for (double d : doubles)
  // {
  //   fc.push_back(d, store);
  // }
  // ASSERT_FLOAT_EQ(fc.get(0, store), 0.234);
  // ASSERT_FLOAT_EQ(fc.get(1, store), -0.678);
  // ASSERT_FLOAT_EQ(fc.get(2, store), 123.123);
  // ASSERT_FLOAT_EQ(fc.get(3, store), 67);

  // EXPECT_EQ(fc.as_bool(), nullptr);
  // EXPECT_EQ(fc.as_int(), nullptr);
  // EXPECT_EQ(fc.as_double().get(), &fc);
  // EXPECT_EQ(fc.as_string(), nullptr);

  // EXPECT_EQ(fc.size(), 4);
  // EXPECT_EQ(fc.get_type(), 'D');

  // std::vector<std::string> strings = {s1.c_str(), s2.c_str(), s3.c_str(), s4.c_str()};
  // StringColumn sc;
  // for (std::string s : strings)
  // {
  //   sc.push_back(s, store);
  // }
  // EXPECT_EQ(sc.get(0, store), s1);
  // EXPECT_EQ(sc.get(1, store), s2);
  // EXPECT_EQ(sc.get(2, store), s3);
  // EXPECT_EQ(sc.get(3, store), s4);

  // EXPECT_EQ(sc.as_bool(), nullptr);
  // EXPECT_EQ(sc.as_int(), nullptr);
  // EXPECT_EQ(sc.as_double(), nullptr);
  // EXPECT_EQ(sc.as_string().get(), &sc);

  // EXPECT_EQ(sc.size(), 4);
  // EXPECT_EQ(sc.get_type(), 'S');
}

// Test Dataframe
TEST(dataframe, testGetSchema)
{
  Schema schema("DIBS");
  DataFrame df(schema);
  EXPECT_EQ(df.get_schema()._types, schema._types);
  EXPECT_EQ(df.ncols(), schema.width());
  EXPECT_EQ(df.nrows(), schema.length());
  Schema emptySchema;
  DataFrame emptyDf(emptySchema);
  EXPECT_EQ(emptyDf.get_schema()._types, emptySchema._types);
  EXPECT_EQ(emptyDf.ncols(), emptySchema.width());
  EXPECT_EQ(emptyDf.nrows(), emptySchema.length());
}

TEST(dataframe, testAddColumnRow)
{
  auto store = std::make_shared<KVStore>(0, nullptr);
  Schema schema("DIBS");
  Schema schemaCopy("DIBSDIBS");
  DataFrame df(schema);
  EXPECT_EQ(df.ncols(), 4);
  EXPECT_EQ(df.get_schema()._types, schema._types);
  std::shared_ptr<DoubleColumn> fcol2;
  std::shared_ptr<IntColumn> icol2;
  std::shared_ptr<BoolColumn> bcol2;
  std::shared_ptr<StringColumn> scol2;
  df.add_column(fcol2);
  df.add_column(icol2);
  df.add_column(bcol2);
  df.add_column(scol2);
  EXPECT_EQ(df.ncols(), 8);

  for (size_t i = 0; i < schemaCopy.width(); ++i)
  {
    EXPECT_EQ(df.get_schema().col_type(i), schemaCopy.col_type(i));
  }

  // test add rows, verify number of rows, and row names
  std::vector<std::shared_ptr<Row>> rows = generateTenRowsFIBSFIBS();
  for (int i = 0; i < 10; ++i)
  {
    std::shared_ptr<Row> row = rows[i];
    df.add_row(*row, store);
  }

  EXPECT_EQ(df.nrows(), 10);
}

TEST(dataframe, testGetSet)
{
  Schema schema;
  DataFrame df(schema);
  std::shared_ptr<DoubleColumn> fcol;
  std::shared_ptr<IntColumn> icol;
  std::shared_ptr<BoolColumn> bcol;
  std::shared_ptr<StringColumn> scol;
  auto store = std::make_shared<KVStore>(0, nullptr);
  df.add_column(fcol);
  df.add_column(icol);
  df.add_column(bcol);
  df.add_column(scol);
  std::vector<std::shared_ptr<Row>> rows = generateTenRowsFIBS();
  for (int i = 0; i < 10; ++i)
  {
    auto row = rows[i];
    df.add_row(*row, store);
  }
  EXPECT_EQ(df.nrows(), 10);
  EXPECT_EQ(df.ncols(), 4);
  EXPECT_FLOAT_EQ(df.get_double(0, 5, store), 5.5);
  EXPECT_FLOAT_EQ(df.get_double(0, 0, store), 0.5);
  EXPECT_EQ(df.get_int(1, 5, store), 10);
  EXPECT_EQ(df.get_int(1, 0, store), 0);
  EXPECT_EQ(df.get_bool(2, 5, store), false);
  EXPECT_EQ(df.get_bool(2, 0, store), true);
  EXPECT_EQ(df.get_string(3, 0, store), "Hello");
}

TEST(dataframe, testFillRow)
{
  Schema schema("SIS");
  DataFrame df(schema);
  Row r(schema);
  auto store = std::make_shared<KVStore>(0, nullptr);

  r.set(0, s1);
  r.set(1, 1);
  r.set(2, s2);
  df.add_row(r, store);

  EXPECT_EQ(df.nrows(), 1);
  EXPECT_EQ(df.get_string(0, 0, store), s1);
  EXPECT_EQ(df.get_int(1, 0, store), 1);
  EXPECT_EQ(df.get_string(2, 0, store), s2);

  Row r2(schema);
  r2.set(0, apple);
  r2.set(1, 2);
  r2.set(2, apple);
  df.add_row(r2, store);

  EXPECT_EQ(df.nrows(), 2);
  EXPECT_EQ(df.get_string(0, 1, store), apple);
  EXPECT_EQ(df.get_int(1, 1, store), 2);
  EXPECT_EQ(df.get_string(2, 1, store), apple);

  Row filledR1(schema);
  df.fill_row(0, filledR1, store);
  EXPECT_EQ(filledR1.get_string(0), s1);
  EXPECT_EQ(filledR1.get_int(1), 1);
  EXPECT_EQ(filledR1.get_string(2), s2);

  Row filledR2(schema);
  df.fill_row(1, filledR2, store);
  EXPECT_EQ(filledR2.get_string(0), apple);
  EXPECT_EQ(filledR2.get_int(1), 2);
  EXPECT_EQ(filledR2.get_string(2), apple);
}

/*
TEST(dataframe, testMap)
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
*/

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}