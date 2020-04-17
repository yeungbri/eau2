/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// Lang::Cpp

#pragma once
#include "../src/application.h"

/**
 * A Demo application pulled from Milestone 3. Modified to use C++11 features
 * such as smart pointers. This application sums doubles from 1 to some n, and
 * verifies that the sum is stored and retrieved correctly across distributed
 * nodes. 
 */
class Demo : public Application
{
public:
  std::shared_ptr<Key> main = std::make_shared<Key>("main", 0);
  std::shared_ptr<Key> verify = std::make_shared<Key>("verif", 1);
  std::shared_ptr<Key> check = std::make_shared<Key>("ck", 0);
  MessageCheckerThread message_checker_;

  size_t DF_TEST_SIZE = 100 * 1000;   // The number of elements we are storing. 

  /**
   * Create an instance of the application given:
   * @param idx the node number
   * @param net a network interface to send and receive messages
   * @param num_nodes how many nodes are running, total (must be at least 1)
   */
  Demo(size_t idx, std::shared_ptr<NetworkIfc> net, size_t num_nodes)
      : Application(idx, net, num_nodes), message_checker_(idx, kv, net) {}

  /**
   * Kills the message checker thread by breaking its loop and joining.
   */
  virtual ~Demo()
  {
    message_checker_.terminate();
    message_checker_.join();
  }

  /**
   * Starts the instance of this application. Depending on what node (of 3 nodes)
   * this one is, it will either:
   * - Produce a dataframe with a bunch of numbers and sum them
   * - Count a dataframe of numbers
   * - Verify that the count and the original sum match
   */
  void run_() override
  {
    kv->register_node();
    message_checker_.start();
    switch (this_node())
    {
    case 0:
      producer();
      break;
    case 1:
      counter();
      break;
    case 2:
      summarizer();
    }
  }

  /**
   * Sums up a bunch of doubles, and stores the list of numbers in the KVStore.
   * It also stores the sum, to be double-checked against later.
   */
  void producer()
  {
    std::vector<double> vals;
    double sum = 0;
    for (size_t i = 0; i < DF_TEST_SIZE; ++i)
    {
      vals.push_back(i);
      sum += i;
    }
    DataFrame::fromArray(main, kv, vals);
    DataFrame::fromScalar(check, kv, sum);
  }

  /**
   * Retrieves the dataframe of numbers from the KVStore, and sums each number.
   * The elements on the KVStore are stored across all 3 nodes.
   */
  void counter()
  {
    Value val = kv->waitAndGet(*main);
    Deserializer dser(val.data(), val.length());
    auto df = DataFrame::deserialize(dser);
    size_t sum = 0;
    for (size_t i = 0; i < DF_TEST_SIZE; ++i)
    {
      sum += df->get_double(0, i, kv);
    }
    DataFrame::fromScalar(verify, kv, sum);
  }

  /**
   * Verifies that the counted sum and the original sum are the same.
   * If SUCCESS is printed, then this works properly.
   */
  void summarizer()
  {
    Value val = kv->waitAndGet(*verify);
    Deserializer dserVerify(val.data(), val.length());
    auto result = DataFrame::deserialize(dserVerify);

    val = kv->waitAndGet(*check);
    Deserializer dserCheck(val.data(), val.length());
    auto expected = DataFrame::deserialize(dserCheck);

    std::cout << (expected->get_double(0, 0, kv) == result->get_double(0, 0, kv) ? "SUCCESS" : "FAILURE") << "\n";
  }
};

/**
 * Runs the application on the specified thread.
 */
class DemoThread : public Thread
{
public:
  Demo d_;

  DemoThread(int node, std::shared_ptr<NetworkPseudo> net) : d_(node, net, 3){};

  ~DemoThread() = default;

  void run() { d_.run_(); }
};

/**
 * Runs the demo code from the M3 specs. This program spawns 3 threads, one for
 * each of the application's tasks, and tests the dataframe's ability to 
 * distribute the load across 3 separate "nodes".
 */
class Milestone3
{
public:
  static void run()
  {
    auto net = std::make_shared<NetworkPseudo>(3);

    DemoThread t1(0, net);
    DemoThread t2(1, net);
    DemoThread t3(2, net);

    t1.start();
    t2.start();
    t3.start();

    t1.join();
    t2.join();
    t3.join();
  }
};