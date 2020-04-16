/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>
#include "../src/dataframe/dataframe.h"
#include "../src/dataframe/wrapper.h"

/**
 * Strings we will use in our tests.
 */
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

/**
 * Creates and returns a vector of 10 row pointers with the schema 'FIBSFIBS'
 */
std::vector<std::shared_ptr<Row>> generateTenRowsFIBSFIBS()
{
  std::vector<std::shared_ptr<Row>> rows;
  for (size_t i = 0; i < 10; ++i)
  {
    std::string name = std::to_string(i);
    auto row = std::make_shared<Row>(fibsfibs);
    row->set(0, Double(double(1.0)));
    row->set(1, Int(int(2)));
    row->set(2, Bool(true));
    row->set(3, String(s1));
    row->set(4, Double(double(69.0)));
    row->set(5, Int(int(420)));
    row->set(6, Bool(false));
    row->set(7, String(s2));
    rows.push_back(row);
  }
  return rows;
}

/**
 * Creates and returns a vector of 10 row pointers with the schema 'FIBS'
 */
std::vector<std::shared_ptr<Row>> generateTenRowsFIBS()
{
  std::vector<std::shared_ptr<Row>> rows;
  for (size_t i = 0; i < 10; ++i)
  {
    std::string name = std::to_string(i);
    auto row = std::make_shared<Row>(fibs);
    row->set(0, Double(double(0.5 + i * 1.0)));
    row->set(1, Int(int(i * 2)));
    row->set(2, Bool(i % 2 == 0));
    row->set(3, String(s1));
    rows.push_back(row);
  }
  return rows;
}

// Test Schema
TEST(dataframe, testSchema)
{
  // Create a schema, test its size and column-type-correctness.
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

  // Verify that adding a row correctly updates the schema size.
  s.add_row();

  EXPECT_EQ(s.width(), 6);
  EXPECT_EQ(s.length(), 1);

  // Test copy constructor, and verify all elements remain the same.
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
  // Create a column of bools
  std::vector<bool> bools = {0, 1, 0, 1};
  auto store = std::make_shared<KVStore>(0, nullptr, 1);
  BoolColumn bc;
  for (bool b : bools)
  {
    bc.push_back(b, store);
  }
  // Verify that each element was pushed back correctly. 
  EXPECT_EQ(bc.get(0, store), 0);
  EXPECT_EQ(bc.get(1, store), 1);
  EXPECT_EQ(bc.get(2, store), 0);
  EXPECT_EQ(bc.get(3, store), 1);

  EXPECT_EQ(bc.as_bool(), &bc);
  EXPECT_EQ(bc.as_int(), nullptr);
  EXPECT_EQ(bc.as_double(), nullptr);
  EXPECT_EQ(bc.as_string(), nullptr);
  // Verify the size and type.
  EXPECT_EQ(bc.size(), 4);
  EXPECT_EQ(bc.get_type(), 'B');

  // Create a column of ints
  std::vector<int> ints = {1, 2, 3, -4};
  IntColumn ic;
  for (int i : ints)
  {
    ic.push_back(i, store);
  }
  // Verify that each element was pushed back correctly. 
  EXPECT_EQ(ic.get(0, store), 1);
  EXPECT_EQ(ic.get(1, store), 2);
  EXPECT_EQ(ic.get(2, store), 3);
  EXPECT_EQ(ic.get(3, store), -4);

  EXPECT_EQ(ic.as_bool(), nullptr);
  EXPECT_EQ(ic.as_int(), &ic);
  EXPECT_EQ(ic.as_double(), nullptr);
  EXPECT_EQ(ic.as_string(), nullptr);

  EXPECT_EQ(ic.size(), 4);
  EXPECT_EQ(ic.get_type(), 'I');

  // Create a column of doubles
  std::vector<double> doubles = {0.234, -0.678, 123.123, 67.0};
  DoubleColumn fc;
  for (double d : doubles)
  {
    fc.push_back(d, store);
  }
  // Verify that each element was pushed back correctly. 
  ASSERT_FLOAT_EQ(fc.get(0, store), 0.234);
  ASSERT_FLOAT_EQ(fc.get(1, store), -0.678);
  ASSERT_FLOAT_EQ(fc.get(2, store), 123.123);
  ASSERT_FLOAT_EQ(fc.get(3, store), 67);

  // Verify size and type
  EXPECT_EQ(fc.as_bool(), nullptr);
  EXPECT_EQ(fc.as_int(), nullptr);
  EXPECT_EQ(fc.as_double(), &fc);
  EXPECT_EQ(fc.as_string(), nullptr);

  EXPECT_EQ(fc.size(), 4);
  EXPECT_EQ(fc.get_type(), 'D');

  // Create a column of strings
  std::vector<std::string> strings = {s1.c_str(), s2.c_str(), s3.c_str(), s4.c_str()};
  StringColumn sc;
  for (std::string s : strings)
  {
    sc.push_back(s, store);
  }
  // Verify that each element was pushed back correctly. 
  EXPECT_EQ(sc.get(0, store), s1);
  EXPECT_EQ(sc.get(1, store), s2);
  EXPECT_EQ(sc.get(2, store), s3);
  EXPECT_EQ(sc.get(3, store), s4);
  // Verify size and type.
  EXPECT_EQ(sc.as_bool(), nullptr);
  EXPECT_EQ(sc.as_int(), nullptr);
  EXPECT_EQ(sc.as_double(), nullptr);
  EXPECT_EQ(sc.as_string(), &sc);

  EXPECT_EQ(sc.size(), 4);
  EXPECT_EQ(sc.get_type(), 'S');
}

// Test Dataframe's constructor given a schema.
TEST(dataframe, testGetSchema)
{
  // Create a dataframe with a schema, and verify that all elements of the 
  // schema are present in the dataframe.
  Schema schema("DIBS");
  DataFrame df(schema);
  EXPECT_EQ(df.get_schema()._types, schema._types);
  EXPECT_EQ(df.ncols(), schema.width());
  EXPECT_EQ(df.nrows(), schema.length());
  // Create a dataframe with an empty schema, verify that the dataframe is empty
  Schema emptySchema;
  DataFrame emptyDf(emptySchema);
  EXPECT_EQ(emptyDf.get_schema()._types, emptySchema._types);
  EXPECT_EQ(emptyDf.ncols(), emptySchema.width());
  EXPECT_EQ(emptyDf.nrows(), emptySchema.length());
}

// Tests that adding columns and rows to dataframes works properly
TEST(dataframe, testAddColumnRow)
{
  // Create a Dataframe with 4 columns, and verify the count.
  auto store = std::make_shared<KVStore>(0, nullptr, 1);
  Schema schema("DIBS");
  Schema schemaCopy("DIBSDIBS");
  DataFrame df(schema);
  EXPECT_EQ(df.ncols(), 4);
  EXPECT_EQ(df.get_schema()._types, schema._types);
  // Add 4 more columns, and verify that the size is updated.
  auto fcol2 = std::make_shared<DoubleColumn>();
  auto icol2 = std::make_shared<IntColumn>();
  auto bcol2 = std::make_shared<BoolColumn>();
  auto scol2 = std::make_shared<StringColumn>();
  df.add_column(fcol2);
  df.add_column(icol2);
  df.add_column(bcol2);
  df.add_column(scol2);
  EXPECT_EQ(df.ncols(), 8);

  // Verify that the type of each column is correct.
  for (size_t i = 0; i < schemaCopy.width(); ++i)
  {
    EXPECT_EQ(df.get_schema().col_type(i), schemaCopy.col_type(i));
  }

  // test add rows, verify number of rows
  std::vector<std::shared_ptr<Row>> rows = generateTenRowsFIBSFIBS();
  for (int i = 0; i < 10; ++i)
  {
    std::shared_ptr<Row> row = rows[i];
    df.add_row(*row, store);
  }

  EXPECT_EQ(df.nrows(), 10);
}

// Tests that elements are correctly added and can be retrieved properly
// using the add_row method.
TEST(dataframe, testGetSet)
{
  // Create a dataframe with 4 empty columns
  Schema schema;
  DataFrame df(schema);
  auto fcol = std::make_shared<DoubleColumn>();
  auto icol = std::make_shared<IntColumn>();
  auto bcol = std::make_shared<BoolColumn>();
  auto scol = std::make_shared<StringColumn>();
  auto store = std::make_shared<KVStore>(0, nullptr, 1);
  df.add_column(fcol);
  df.add_column(icol);
  df.add_column(bcol);
  df.add_column(scol);
  // Add 10 rows of data to the dataframe
  std::vector<std::shared_ptr<Row>> rows = generateTenRowsFIBS();
  for (int i = 0; i < 10; ++i)
  {
    auto row = rows[i];
    df.add_row(*row, store);
  }
  // Verify that the size of the dataframe is updated
  EXPECT_EQ(df.nrows(), 10);
  EXPECT_EQ(df.ncols(), 4);
  // Verify that each element in the dataframe is correct
  for (int i = 0; i < 10; ++i)
  {
    EXPECT_FLOAT_EQ(df.get_double(0, i, store), double(0.5 + i * 1.0));
  }
  for (int i = 0; i < 10; ++i)
  {
    EXPECT_EQ(df.get_int(1, i, store), i * 2);
  }
  for (int i = 0; i < 10; ++i)
  {
    EXPECT_EQ(df.get_bool(2, i, store), i % 2 == 0);
  }
  for (int i = 0; i < 10; ++i)
  {
    EXPECT_EQ(df.get_string(3, i, store), "Hello");
  }
}

// Tests the dataframe's fill_row method
TEST(dataframe, testFillRow)
{
  // Creates a dataframe with 3 empty columns.
  Schema schema("SIS");
  DataFrame df(schema);
  Row r(schema);
  auto store = std::make_shared<KVStore>(0, nullptr, 1);

  // Adds a row to the dataframe.
  r.set(0, String(s1));
  r.set(1, Int(1));
  r.set(2, String(s2));
  df.add_row(r, store);

  // Verify that the row was added correctly.
  EXPECT_EQ(df.nrows(), 1);
  EXPECT_EQ(df.get_string(0, 0, store), s1);
  EXPECT_EQ(df.get_int(1, 0, store), 1);
  EXPECT_EQ(df.get_string(2, 0, store), s2);

  // Add another row to the dataframe.
  Row r2(schema);
  r2.set(0, String(apple));
  r2.set(1, Int(2));
  r2.set(2, String(apple));
  df.add_row(r2, store);

  // Verify the second row was added correctly as well.
  EXPECT_EQ(df.nrows(), 2);
  EXPECT_EQ(df.get_string(0, 1, store), apple);
  EXPECT_EQ(df.get_int(1, 1, store), 2);
  EXPECT_EQ(df.get_string(2, 1, store), apple);

  // Given an empty row, fill it with the data from the dataframe's first row
  // and verify that it is correct.
  Row filledR1(schema);
  df.fill_row(0, filledR1, store);
  EXPECT_EQ(filledR1.get_string(0), s1);
  EXPECT_EQ(filledR1.get_int(1), 1);
  EXPECT_EQ(filledR1.get_string(2), s2);

  // Do the same with the second row, verify that it is correct.
  Row filledR2(schema);
  df.fill_row(1, filledR2, store);
  EXPECT_EQ(filledR2.get_string(0), apple);
  EXPECT_EQ(filledR2.get_int(1), 2);
  EXPECT_EQ(filledR2.get_string(2), apple);
}

// Tests the dataframe's map functionality
TEST(dataframe, testMap)
{
  Schema s("II");
  auto store = std::make_shared<KVStore>(0, nullptr, 1);
  DataFrame df(s);
  CounterRower countRower;
  IntSumRower intRower;
  Row r(df.get_schema());
  for (int i = 0; i < 1000; i++)
  {
    r.set(0, Int(i));
    r.set(1, Int(i+1));
    df.add_row(r, store);
  }

  df.map(countRower, store);
  EXPECT_EQ(countRower._count, 2000);

  df.map(intRower, store);
  EXPECT_EQ(intRower._sum, 1000000);
}

// Runs all of the tests.
int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}