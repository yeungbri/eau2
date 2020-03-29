/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::CwC

#include "../src/column.h"
#include "../src/network/message.h"
#include "../src/schema.h"
#include "../src/serial.h"
#include "../src/dataframe.h"
#include <gtest/gtest.h>

#define ASSERT_EXIT_ZERO(a) \
  ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

void test_ackmsg() {
  Ack ackmsg(MsgKind::Ack, 1, 2, 0);
  Serializer ser;
  ackmsg.serialize(ser);
  Deserializer dser(ser.data(), ser.length());
  auto d_ackmsg = ackmsg.deserialize(dser);
  ASSERT_TRUE(ackmsg.kind_ == d_ackmsg->kind_);
  ASSERT_TRUE(ackmsg.sender_ == d_ackmsg->sender_);
  ASSERT_TRUE(ackmsg.target_ == d_ackmsg->target_);
  ASSERT_TRUE(ackmsg.id_ == d_ackmsg->id_);
  exit(0);
}

TEST(serial, test_ackmsg) { ASSERT_EXIT_ZERO(test_ackmsg) }

void test_string() {
  std::string s1 = "hello";
  std::string s2 = "goodbye";
  std::string s3 = "bye";
  Serializer ser;
  ser.write_string(s1);
  ser.write_string(s2);
  ser.write_string(s3);

  Deserializer dser(ser.data(), ser.length());
  std::string d1 = dser.read_string();
  std::string d2 = dser.read_string();
  std::string d3 = dser.read_string();
  ASSERT_TRUE(s1 == d1);
  ASSERT_TRUE(s2 == d2);
  ASSERT_TRUE(s3 == d3);
  exit(0);
}

TEST(serial, test_string) { ASSERT_EXIT_ZERO(test_string) }

void test_string_vector() {
  std::vector<std::string> vs = {"hello", "goodbye", "bye"};
  std::vector<std::string> vs2 = {"apple", "orange", "pear"};
  Serializer ser;
  ser.write_string_vector(vs);
  ser.write_string_vector(vs2);

  Deserializer dser(ser.data(), ser.length());
  std::vector<std::string> dvs = dser.read_string_vector();
  std::vector<std::string> dvs2 = dser.read_string_vector();

  ASSERT_TRUE(vs.size() == dvs.size());
  for (int i = 0; i < vs.size(); i++) {
    ASSERT_TRUE(vs.at(i) == dvs.at(i));
  }
  ASSERT_TRUE(vs2.size() == dvs2.size());
  for (int i = 0; i < vs.size(); i++) {
    ASSERT_TRUE(vs2.at(i) == dvs2.at(i));
  }
  exit(0);
}

TEST(serial, test_string_vector) { ASSERT_EXIT_ZERO(test_string_vector) }

void test_double() {
  double f1 = 0.123;
  double f2 = 8.123;
  double f3 = 0;

  Serializer ser;
  ser.write_double(f1);
  ser.write_double(f2);
  ser.write_double(f3);

  Deserializer dser(ser.data(), ser.length());
  double df1 = dser.read_double();
  double df2 = dser.read_double();
  double df3 = dser.read_double();

  ASSERT_TRUE(f1 = df1);

  exit(0);
}

TEST(serial, test_double) { ASSERT_EXIT_ZERO(test_double) }

void test_double_column() {
  auto store = std::make_shared<KVStore>(0, nullptr);
  std::vector<double> fv = {0.1, 0.123, 1.80};
  DoubleColumn fc;
  for (double d : fv)
  {
    fc.push_back(d, store);
  }

  Serializer ser;
  fc.serialize(ser);

  Deserializer dser(ser.data(), ser.length());
  auto fc2 = fc.deserialize(dser);

  for (int i = 0; i < fv.size(); i++) {
    ASSERT_TRUE(fc.get(i, store) == fc2->get(i, store));
  }
  exit(0);
}

TEST(serial, test_double_column) { ASSERT_EXIT_ZERO(test_double_column) }

void test_schema() {
  Schema s("DDD");
  Serializer ser;
  s.serialize(ser);

  Deserializer dser(ser.data(), ser.length());
  auto s2 = s.deserialize(dser);

  ASSERT_TRUE(s.width() == s2->width());
  for (int i=0; i<3; i++) {
    ASSERT_TRUE(s._types.at(i) == s2->_types.at(i));
    ASSERT_TRUE(s._types.at(i) == "D");
    ASSERT_TRUE(s2->_types.at(i) == "D");
  }
  exit(0);
}

TEST(serial, test_schema) { ASSERT_EXIT_ZERO(test_schema) }

void test_dataframe() {
  Schema s("D");
  auto store = std::make_shared<KVStore>(0, nullptr);

  std::vector<double> fv = {0.1, 0.123, 1.80};
  std::shared_ptr<DoubleColumn> fc = std::make_shared<DoubleColumn>();
  for (double d : fv)
  {
    fc->push_back(d, store);
  }
  std::vector<int> iv = {1, 2, 3};
  auto ic = std::make_shared<IntColumn>();
  for (int i : iv)
  {
    ic->push_back(i, store);
  }
  std::vector<bool> bv = {0, 1, 1};
  auto bc = std::make_shared<BoolColumn>();
  for (bool b : bv)
  {
    bc->push_back(b, store);
  }
  std::vector<std::string> sv = {"hello", "good", "bye"};
  std::shared_ptr<StringColumn> sc = std::make_shared<StringColumn>();
  for (std::string s : sv)
  {
    sc->push_back(s, store);
  }

  DataFrame df(s);
  df.add_column(fc);
  df.add_column(ic);
  df.add_column(bc);
  df.add_column(sc);
  
  Serializer ser;
  df.serialize(ser);

  Deserializer dser(ser.data(), ser.length());
  auto df2 = DataFrame::deserialize(dser);

  ASSERT_FLOAT_EQ(df2->get_double(1, 0, store), fv[0]);
  ASSERT_FLOAT_EQ(df2->get_double(1, 1, store), fv[1]);
  ASSERT_FLOAT_EQ(df2->get_double(1, 2, store), fv[2]);

  for (int i; i<iv.size(); i++) {
    ASSERT_EQ(df.get_int(2, i, store), iv[i]);
  }

  ASSERT_EQ(df.get_bool(3, 0, store), bv[0]);

  ASSERT_EQ(df.get_string(4, 0, store), sv[0]);
  
  exit(0);
}

TEST(serial, test_dataframe) { ASSERT_EXIT_ZERO(test_dataframe) }

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}