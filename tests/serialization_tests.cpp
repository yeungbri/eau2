/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::CwC

#include "../src/message.h"
#include "../src/serial.h"
#include "../src/helper.h"
#include <gtest/gtest.h>

#define ASSERT_EXIT_ZERO(a) ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

void test1() {
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

TEST(W1, test1) {
  ASSERT_EXIT_ZERO(test1)
}

void test2() {
  std::string s1 = "hello";
  std::string s2 = "goodbye";
  std::string s3 = "bye";
  Serializer ser;
  ser.write_string(s1);
  ser.write_string(s2);
  ser.write_string(s3);
  std::cout << ser.length() << std::endl;

  Deserializer dser(ser.data());
  std::string d1 = dser.read_string();
  std::string d2 = dser.read_string();
  std::string d3 = dser.read_string();
  ASSERT_TRUE(s1 == d1);
  ASSERT_TRUE(s2 == d2);
  ASSERT_TRUE(s3 == d3);
  exit(0);
}

TEST(W1, test2) {
  ASSERT_EXIT_ZERO(test2)
}

void test3() {
  std::vector<std::string> vs = {"hello", "goodbye", "bye"};
  std::vector<std::string> vs2 = {"apple", "orange", "pear"};
  Serializer ser;
  ser.write_string_vector(vs);

  Deserializer dser(ser.data());
  std::vector<std::string> dvs = dser.read_string_vector();
  std::vector<std::string> dvs2 = dser.read_string_vector();

  ASSERT_TRUE(vs.size() == dvs.size());
  for (int i=0; i<vs.size(); i++) {
    std::cout << vs.at(i) << " " << dvs.at(i) << "\n";
    ASSERT_TRUE(vs.at(i) == dvs.at(i));
  }
  // ASSERT_TRUE(vs2.size() == dvs2.size());
  // for (int i=0; i<vs.size(); i++) {
  //   ASSERT_TRUE(vs2.at(i) == dvs2.at(i));
  // }
  exit(0);
}

TEST(W1, test3) {
  ASSERT_EXIT_ZERO(test3)
}

void test4() {
  exit(0);
}

TEST(W1, test4) { ASSERT_EXIT_ZERO(test4) }

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}