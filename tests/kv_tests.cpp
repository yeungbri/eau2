/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#include <gtest/gtest.h>
#include <cassert>
#include "../src/application.h"

#define ASSERT_EXIT_ZERO(a) \
  ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

/**
 * Test Application. One thread stores values from 0 - 9, the other retrieves
 * them and verifies that they are correct.
 */
class TestApp : public Application
{
public:
  size_t idx_;
  std::shared_ptr<NetworkIfc> net_;
  MessageCheckerThread checker_;

public:
  TestApp(size_t idx, std::shared_ptr<NetworkIfc> net)
      : Application(idx, net), checker_(idx, kv, net) {}

  virtual ~TestApp() = default;

  void producer()
  {
    for (int i = 0; i < 10; ++i)
    {
      std::string name = std::to_string(i);
      auto key = std::make_shared<Key>(name, 0);
      DataFrame::fromScalar(key, kv, i);
    }
    std::cout << "Producer finished adding everything!" << std::endl;
  }

  void consumer()
  {
    for (int i = 0; i < 10; ++i)
    {
      std::string name = std::to_string(i);
      auto key = std::make_shared<Key>(name, 0);
      Value val = kv->waitAndGet(*key);
      if (val.data() != nullptr)
      {
        Deserializer dserVerify(val.data(), val.length());
        auto result = DataFrame::deserialize(dserVerify);
        assert(int(result->get_double(0, 0, kv)) == i);
      } else
      {
        std::cout << "null test" << std::endl;
      }
    }
  }

  void run_() override
  {
    kv->register_node();
    checker_.start();
    switch (this_node())
    {
    case 0:
      producer();
      break;
    case 1:
      consumer();
      break;
    }
  }
};

/**
 * Runs the application on the specified thread.
 */
class TestThread : public Thread
{
public:
  TestApp d_;

  TestThread(int node, std::shared_ptr<NetworkPseudo> net) : d_(node, net){};

  ~TestThread() = default;

  void run() { d_.run_(); }
};

void testSimpleKV()
{
  auto net = std::make_shared<NetworkPseudo>(2);

  TestThread t1(0, net);
  t1.start();

  Thread::sleep(1000);
  std::cout << "Testing that Thread 0 stored all KV Pairs correctly!" << std::endl;
  for (int i = 0; i < 10; ++i)
  {
    Key k(std::to_string(i), 0);
    auto v = t1.d_.kv->get(k);
    Deserializer dser(v.data(), v.length());
    auto result = DataFrame::deserialize(dser);
    int actual = result->get_double(0, 0, t1.d_.kv);
    if (actual != i)
    {
      std::cout << "Actual: " << actual << ", Expected: " << i << std::endl;
    }
  }

  std::cout << "All KV Pairs stored successfully by Thread 0!" << std::endl;

  TestThread t2(1, net);
  t2.start();

  Thread::sleep(10000);
  t1.join();
  t2.join();
  exit(0);
}

TEST(simpleKV, testSimpleKV) { ASSERT_EXIT_ZERO(testSimpleKV) }

// Runs all of the tests.
int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}