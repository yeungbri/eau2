/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <cassert>
#include <string>
#include <vector>
#include "dataframe.h"

/**
 * A thread for this node that is responsible for monitoring the node's message
 * queue. If a message appears on this queue, this thread will query the KVStore
 * and return the appropriate response.
 */
class MessageCheckerThread : public Thread
{
public:
  size_t idx_;
  std::shared_ptr<KVStore> store_;
  std::shared_ptr<NetworkIfc> net_;
  MessageCheckerThread(size_t idx, std::shared_ptr<KVStore> store, std::shared_ptr<NetworkIfc> net)
      : idx_(idx), store_(store), net_(net) {}

  virtual ~MessageCheckerThread() = default;

  void handle_get(std::shared_ptr<Message> msg)
  {
    auto get_msg = std::dynamic_pointer_cast<Get>(msg);
    std::cout << "1" << std::endl;
    auto val = store_->get(get_msg->k_);
    std::cout << "2" << std::endl;
    auto reply_msg = std::make_shared<Reply>(
      MsgKind::Reply, idx_, get_msg->sender_, 0, val.data(), val.length());
    std::cout << "3" << std::endl;
    std::cout << idx_ << " sent a reply to " << get_msg->sender_ << "!!!" << std::endl;
    net_->send_msg(reply_msg);
  }

  void handle_reply(std::shared_ptr<Message> msg)
  {
    std::cout << "REPLY!!!!!!!!!!!!!" << std::endl;
  }

  virtual void run()
  {
    auto my_queue = std::dynamic_pointer_cast<NetworkPseudo>(net_)->msg_queues_.at(idx_);
    while (true)
    {
      if (idx_ == 0){
        std::cout << idx_ << "LOOPING!!" << std::endl;
      }
      // check if there are any new messages
      if (my_queue->size() > 0)
      {
        auto msg = my_queue->pop();
        switch (msg->kind_)
        {
        case MsgKind::Put:
          std::cout << idx_ << " received a PUT MESSAGE!!!!" << std::endl;
          store_->put(std::dynamic_pointer_cast<Put>(msg)->k_, std::dynamic_pointer_cast<Put>(msg)->v_);
          break;
        case MsgKind::Get:
          std::cout << idx_ << " received a GET MESSAGE!!!!" << std::endl;
          handle_get(msg);
          break;
        case MsgKind::Reply:
          std::cout << idx_ << " received a REPLY MESSAGE!!!!" << std::endl;
          handle_reply(msg);
          break;
        default:
          std::cout << "Unknown message type!" << std::endl;
        }
      }
      else
      {
        std::cout << "no messages for node " << idx_ << " yet..." << std::endl;
        Thread::sleep(500);
      }
    }
  }
};

/** An application runs on a node and owns one local kvstore */
class Application
{
public:
  size_t idx_;                 // index of node it is running on
  std::shared_ptr<KVStore> kv; // local kvstore

  /**
   * Creates an application with a KVStore, given its index (node #) and a
   * network interface to communicate with.
   */
  Application(size_t idx, std::shared_ptr<NetworkIfc> net) : idx_(idx)
  {
    kv = std::make_shared<KVStore>(idx, net);
  }

  ~Application() = default;

  /** Invoke to run the application. Application subclasses implement this. */
  virtual void run_() {}

  /** Returns this application's home node. */
  size_t this_node() { return idx_; }
};

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

  Demo(size_t idx, std::shared_ptr<NetworkIfc> net)
      : Application(idx, net), message_checker_(idx, kv, net) {}

  virtual ~Demo()
  {
    message_checker_.join();
  }

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

  void producer()
  {
    std::cout << this_node() << ": Producer about to start..." << std::endl;
    size_t SZ = 10;
    std::vector<double> vals;
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i)
    {
      vals.push_back(i);
      sum += i;
    }
    DataFrame::fromArray(main, kv, vals);
    DataFrame::fromScalar(check, kv, sum);
    std::cout << this_node() << ": Producer is done" << std::endl;
  }

  void counter()
  {
    std::cout << this_node() << ": Counter about to start..." << std::endl;
    Value val = kv->waitAndGet(*main);
    std::cout << this_node() << ": Counter got value from main key!" << std::endl;
    Deserializer dser(val.data(), val.length());
    auto df = DataFrame::deserialize(dser);
    size_t sum = 0;
    for (size_t i = 0; i < 100 * 1000; ++i)
      sum += df->get_double(0, i, kv);
    std::cout << this_node() << ": The sum is  " << sum << "\n";
    DataFrame::fromScalar(verify, kv, sum);
    std::cout << this_node() << ": Counter is done" << std::endl;
  }

  void summarizer()
  {
    std::cout << this_node() << ": Summarizer is about to start.." << std::endl;
    Value val = kv->waitAndGet(*verify);
    std::cout << this_node() << ": Summarizer got value from verify key!" << std::endl;
    Deserializer dserVerify(val.data(), val.length());
    auto result = DataFrame::deserialize(dserVerify);

    val = kv->waitAndGet(*check);
    Deserializer dserCheck(val.data(), val.length());
    auto expected = DataFrame::deserialize(dserCheck);

    std::cout << (expected->get_double(0, 0, kv) == result->get_double(0, 0, kv) ? "SUCCESS" : "FAILURE") << "\n";
  }
};

/** Trival application to test serialization on a single node */
// class Trivial : public Application {
//  public:
//   Trivial(size_t idx) : Application(idx) {}

//   /** Creates a dataframe from all the numbers from 1 - 999999 and
//    * stores it onto the local kvstore. Then retrieves the stored
//    * value and checks all values are there */
//   void run_() {
//     size_t SZ = 1000 * 1000;
//     std::vector<double> vals;
//     double sum = 0;
//     for (size_t i = 0; i < SZ; ++i) {
//       vals.push_back(i);
//       sum += i;
//     }
//     Key key("triv", 0);
//     DataFrame* df = DataFrame::fromArray(&key, &kv, vals);
//     assert(df->get_double(0, 1, &kv) == 1);

//     Value val = kv.get(key);
//     Deserializer dser(val.data(), val.length());
//     DataFrame* df2 = DataFrame::deserialize(dser);
//     for (size_t i = 0; i < SZ; ++i) {
//       assert(df2->get_double(0, i, &kv) == i);
//       sum -= df2->get_double(0, i, &kv);
//     }
//     assert(sum == 0);
//     std::cout << "SUCCESS" << std::endl;

//     delete df;
//     delete df2;
//   }
// };
