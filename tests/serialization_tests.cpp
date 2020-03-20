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
    Deserializer dser(ser.data(), ser.length());
    Message *d_ackmsg = ackmsg.deserialize(dser);
    ASSERT_TRUE(ackmsg.kind_ == d_ackmsg->kind_); 
    std::cout << (size_t) d_ackmsg->kind_ << "\n";
    std::cout << d_ackmsg->sender_ << "\n";
    std::cout << d_ackmsg->target_ << "\n";
    std::cout << d_ackmsg->id_ << "\n";
    // ASSERT_TRUE(ackmsg.sender_ == d_ackmsg->sender_); 
    // ASSERT_TRUE(ackmsg.target_ == d_ackmsg->target_); 
    ASSERT_TRUE(ackmsg.id_ == d_ackmsg->id_);
    delete d_ackmsg;
    exit(0);
}

TEST(W1, test1) {
  ASSERT_EXIT_ZERO(test1)
}

void test2() {
  exit(0);
}

TEST(W1, test2) {
  ASSERT_EXIT_ZERO(test2)
}

void test3() {
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