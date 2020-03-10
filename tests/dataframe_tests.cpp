/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#include <gtest/gtest.h>
#include "dataframe.h"
#include <iostream>

#define ASSERT_EXIT_ZERO(a) ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*")

Row **generateTenRowsFIBSFIBS()
{
  Row **rows = new Row *[10];
  Schema *schema = new Schema("FIBSFIBS");
  for (size_t i = 0; i < 10; ++i)
  {
    String *name = new String(std::to_string(i).c_str());
    Row *row = new Row(*schema, name);
    String *s1 = new String("Hello");
    String *s2 = new String("Bye Bye");
    row->set(0, float(1.0));
    row->set(1, int(2));
    row->set(2, true);
    row->set(3, s1);
    row->set(4, float(69.0));
    row->set(5, int(420));
    row->set(6, false);
    row->set(7, s2);
    rows[i] = row;
  }
  return rows;
}

Row **generateTenRowsFIBS()
{
  Row **rows = new Row *[10];
  Schema *schema = new Schema("FIBS");
  for (size_t i = 0; i < 10; ++i)
  {
    String *name = new String(std::to_string(i).c_str());
    Row *row = new Row(*schema, name);
    String *s1 = new String("Hello");
    row->set(0, float(0.5 + i * 1.0));
    row->set(1, int(i * 2));
    row->set(2, i % 2 == 0);
    row->set(3, s1);
    rows[i] = row;
  }
  return rows;
}

// Test Schema
TEST(a4, testSchema)
{
  Schema s("FBIS");
  EXPECT_EQ(s.width(), 4);
  EXPECT_EQ(s.length(), 0);

  String empty_str("");
  String col_name("New Column");
  s.add_column('S', &col_name);
  s.add_column('F', &col_name);
  EXPECT_EQ(s.col_name(1), nullptr);
  EXPECT_EQ(s.col_name(2), nullptr);
  EXPECT_EQ(s.col_name(3), nullptr);
  EXPECT_EQ(s.col_name(4), &col_name);

  EXPECT_EQ(s.col_type(0), 'F');
  EXPECT_EQ(s.col_type(1), 'B');
  EXPECT_EQ(s.col_type(2), 'I');
  EXPECT_EQ(s.col_type(3), 'S');
  EXPECT_EQ(s.col_type(4), 'S');

  EXPECT_EQ(s.col_idx(""), -1);
  // Return first col with that name in case of duplicates
  EXPECT_EQ(s.col_idx("New Column"), 4);

  String row_name("New Row");
  s.add_row(&row_name);
  EXPECT_EQ(s.row_name(0), &row_name);
  EXPECT_EQ(s.row_idx("New Row"), 0);

  EXPECT_EQ(s.width(), 6);
  EXPECT_EQ(s.length(), 1);

  // Test copy constructor
  Schema s2(s);
  EXPECT_EQ(s2.width(), 6);
  EXPECT_EQ(s2.length(), 1);
  EXPECT_EQ(s2.col_name(1), nullptr);
  EXPECT_EQ(s2.col_name(2), nullptr);
  EXPECT_EQ(s2.col_name(3), nullptr);
  EXPECT_EQ(s2.col_name(4), &col_name);
  EXPECT_EQ(s2.col_type(0), 'F');
  EXPECT_EQ(s2.col_type(1), 'B');
  EXPECT_EQ(s2.col_type(2), 'I');
  EXPECT_EQ(s2.col_type(3), 'S');
  EXPECT_EQ(s2.col_idx(""), -1);
  EXPECT_EQ(s2.col_idx("New Column"), 4);
  EXPECT_EQ(s2.row_name(0), &row_name);
  EXPECT_EQ(s2.row_idx("New Row"), 0);
}

// Test Column
TEST(a4, testColumn)
{
  BoolColumn bc(4, 0, 1, 0, 1);
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

  IntColumn ic(4, 1, 2, 3, -4);
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

  FloatColumn fc(4, 0.234, -0.678, 123.123, 67.0);
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

  String s1("");
  String s2("asdf");
  String s3("a");
  String s4("1./0v^#$&%*");

  StringColumn sc(4, &s1, &s2, &s3, &s4);
  EXPECT_EQ(sc.get(0), &s1);
  EXPECT_EQ(sc.get(1), &s2);
  EXPECT_EQ(sc.get(2), &s3);
  EXPECT_EQ(sc.get(3), &s4);

  EXPECT_EQ(sc.as_bool(), nullptr);
  EXPECT_EQ(sc.as_int(), nullptr);
  EXPECT_EQ(sc.as_float(), nullptr);
  EXPECT_EQ(sc.as_string(), &sc);

  EXPECT_EQ(sc.size(), 4);
  EXPECT_EQ(sc.get_type(), 'S');

  sc.set(0, &s2);
  EXPECT_EQ(sc.get(0), &s2);
  sc.set(0, &s3);
  EXPECT_EQ(sc.get(0), &s3);

  ;
}

// Test Dataframe
TEST(a4, testGetSchema)
{
  Schema schema("FIBS");
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

TEST(a4, testAddColumnRow)
{
  // test add columns, verify column names and size
  Schema schema("FIBS");
  Schema schemaCopy("FIBSFIBS");
  DataFrame df(schema);
  EXPECT_EQ(df.ncols(), 4);
  EXPECT_EQ(df.get_schema()._types, schema._types);
  String fcolName("float");
  String icolName("int");
  String bcolName("bool");
  String scolName("String");
  FloatColumn fcol2;
  IntColumn icol2;
  BoolColumn bcol2;
  StringColumn scol2;
  df.add_column(&fcol2, &fcolName);
  df.add_column(&icol2, &icolName);
  df.add_column(&bcol2, &bcolName);
  df.add_column(&scol2, &scolName);
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
  Row **rows = generateTenRowsFIBSFIBS();
  for (int i = 0; i < 10; ++i)
  {
    Row *row = rows[i];
    df.add_row(*row);
  }

  EXPECT_EQ(df.nrows(), 10);
  for (int i = 0; i < 10; ++i)
  {
    String c(std::to_string(i).c_str());
    EXPECT_EQ(df.get_row(c), i);
  }

  delete[] rows;
}

TEST(a4, testGetSet)
{
  Schema schema;
  DataFrame df(schema);
  FloatColumn fcol;
  String fcolName("float");
  IntColumn icol;
  String icolName("int");
  BoolColumn bcol;
  String bcolName("bool");
  StringColumn scol(15);
  for (int i = 0; i < 15; ++i)
  {
    String *str = new String(std::to_string(i).c_str());
    scol.set(i, str);
  }
  String scolName("String");
  df.add_column(&fcol, &fcolName);
  df.add_column(&icol, &icolName);
  df.add_column(&bcol, &bcolName);
  df.add_column(&scol, &scolName);
  Row **rows = generateTenRowsFIBS();
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
  EXPECT_EQ(strcmp(df.get_string(3, 0)->c_str(), "Hello"), 0);

  delete[] rows;
}

TEST(a4, testFillRow)
{
  Schema schema("SIS");
  DataFrame df(schema);
  Row r(schema);

  String s0("apple");
  String s1("pear");
  String s2("orange");

  r.set(0, &s1);
  r.set(1, 1);
  r.set(2, &s2);
  df.add_row(r);

  EXPECT_EQ(df.nrows(), 1);
  EXPECT_EQ(df.get_string(0, 0)->equals(&s1), true);
  EXPECT_EQ(df.get_int(1, 0), 1);
  EXPECT_EQ(df.get_string(2, 0)->equals(&s2), true);

  Row r2(schema);
  r2.set(0, &s0);
  r2.set(1, 2);
  r2.set(2, &s0);
  df.fill_row(0, r2);

  EXPECT_EQ(df.nrows(), 1);
  EXPECT_EQ(df.get_string(0, 0)->equals(&s0), true);
  EXPECT_EQ(df.get_int(1, 0), 2);
  EXPECT_EQ(df.get_string(2, 0)->equals(&s0), true);
}

TEST(a4, testMap)
{
  Schema s("II");
  DataFrame df(s);
  CounterRower countRower;
  IntSumRower intRower;
  Row r(df.get_schema());

  for(size_t i = 0; i <  1000000; i++) {
    r.set(0,(int)i);
    r.set(1,(int)i+1);
    df.add_row(r);
  }

  countRower._count = 0;
  df.map(countRower);
  EXPECT_EQ(countRower._count, 2000000);

  intRower._sum = 0;
  df.map(intRower);
  EXPECT_EQ(intRower._sum, 1000000000000);
}

TEST(a4, testFilter)
{
  StringSearchRower apple_sr("apple");
  Schema schema("SIS");
  DataFrame df(schema);
  Row r(schema);

  String s0("apple");
  String s1("pear");
  String s2("orange");
  String s3("grape");
  for(size_t i = 0; i < 10; i++) {
    if (i % 2 == 0) {
      r.set(0, &s0);
    } else {
      r.set(0, &s1);
    }
    r.set(1, 1);
    r.set(2, &s2);
    df.add_row(r);
  }

  DataFrame* apple_df = df.filter(apple_sr);
  EXPECT_EQ(apple_df->nrows(), 5);
  EXPECT_EQ(apple_df->get_string(0, 0)->equals(&s0), true);
  EXPECT_EQ(apple_df->get_string(0, 1)->equals(&s0), true);
  EXPECT_EQ(apple_df->get_string(0, 2)->equals(&s0), true);
  EXPECT_EQ(apple_df->get_string(0, 3)->equals(&s0), true);
  EXPECT_EQ(apple_df->get_string(0, 4)->equals(&s0), true);
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}