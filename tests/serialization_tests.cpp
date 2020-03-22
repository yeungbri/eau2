/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::CwC

#include "../src/column.h"
#include "../src/helper.h"
#include "../src/message.h"
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
  Deserializer dser(ser.data());
  Message *d_ackmsg = ackmsg.deserialize(dser);
  ASSERT_TRUE(ackmsg.kind_ == d_ackmsg->kind_);
  ASSERT_TRUE(ackmsg.sender_ == d_ackmsg->sender_);
  ASSERT_TRUE(ackmsg.target_ == d_ackmsg->target_);
  ASSERT_TRUE(ackmsg.id_ == d_ackmsg->id_);
  delete d_ackmsg;
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

  Deserializer dser(ser.data());
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

  Deserializer dser(ser.data());
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

void test_float() {
  float f1 = 0.123;
  float f2 = 8.123;
  float f3 = 0;

  Serializer ser;
  ser.write_float(f1);
  ser.write_float(f2);
  ser.write_float(f3);

  Deserializer dser(ser.data());
  float df1 = dser.read_float();
  float df2 = dser.read_float();
  float df3 = dser.read_float();

  ASSERT_TRUE(f1 = df1);

  exit(0);
}

TEST(serial, test_float) { ASSERT_EXIT_ZERO(test_float) }

void test_float_column() {
  std::vector<float> fv = {0.1, 0.123, 1.80};
  FloatColumn fc(fv);

  Serializer ser;
  fc.serialize(ser);

  Deserializer dser(ser.data());
  FloatColumn *fc2 = fc.deserialize(dser);

  for (int i = 0; i < fv.size(); i++) {
    ASSERT_TRUE(fc.get(i) == fc2->get(i));
  }

  delete fc2;
  exit(0);
}

TEST(serial, test_float_column) { ASSERT_EXIT_ZERO(test_float_column) }

void test_schema() {
  Schema s("FFF");
  Serializer ser;
  s.serialize(ser);

  Deserializer dser(ser.data());
  Schema* s2 = s.deserialize(dser);

  ASSERT_TRUE(s.width() == s2->width());
  for (int i=0; i<3; i++) {
    ASSERT_TRUE(s._types.at(i) == s2->_types.at(i));
    ASSERT_TRUE(s._types.at(i) == "F");
    ASSERT_TRUE(s2->_types.at(i) == "F");
  }
  exit(0);
}

TEST(serial, test_schema) { ASSERT_EXIT_ZERO(test_schema) }

void test_dataframe() {
  Schema s("F");

  std::vector<float> fv = {0.1, 0.123, 1.80};
  FloatColumn fc(fv);
  std::vector<int> iv = {1, 2, 3};
  IntColumn ic(iv);
  std::vector<bool> bv = {0, 1, 1};
  BoolColumn bc(bv);
  std::vector<std::string> sv = {"hello", "good", "bye"};
  StringColumn sc(sv);

  DataFrame df(s);
  df.add_column(&fc, "My float col");
  df.add_column(&ic, "int col");
  df.add_column(&bc, "bool col");
  df.add_column(&sc, "string col");
  
  Serializer ser;
  df.serialize(ser);

  Deserializer dser(ser.data());
  DataFrame* df2 = DataFrame::deserialize(dser);

  // std::cout << df.cols_[0]->as_float()->get(0) << "\n";
  std::cout << df2->ncols() << "\n";
  ASSERT_FLOAT_EQ(df2->get_float(1, 0), fv[0]);
  ASSERT_FLOAT_EQ(df2->get_float(1, 1), fv[1]);
  ASSERT_FLOAT_EQ(df2->get_float(1, 2), fv[2]);

  // ASSERT_EQ(df.get_int(2, 0), iv[0]);

  // ASSERT_EQ(df.get_bool(3, 0), bv[0])
  exit(0);
}

TEST(serial, test_dataframe) { ASSERT_EXIT_ZERO(test_dataframe) }

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}