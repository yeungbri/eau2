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

class MessageCheckerThread : public Thread {
  size_t idx_;
  std::shared_ptr<KVStore> store_;
  std::shared_ptr<NetworkIfc> net_;
  MessageCheckerThread(size_t idx, std::shared_ptr<KVStore> store, std::shared_ptr<NetworkIfc> net)
      : idx_(idx), store_(store), net_(net) {}

  void run_() {
    // check if there are any new messages
    auto my_queue = std::dynamic_pointer_cast<NetworkPseudo>(net_)->msg_queues_.at(idx_);
    if (my_queue->size() > 0) {
      auto msg = my_queue->pop();
      switch(msg->kind_) {
        case MsgKind::Put:
          store_->put(std::dynamic_pointer_cast<Put>(msg)->k_, std::dynamic_pointer_cast<Put>(msg)->v_);
          break;
        case MsgKind::Get:
          std::cout << "GET!" << std::endl;
          break;
        default:
          std::cout << "Unknown message type!" << std::endl;
      }
    };
  }
};

/** An application runs on a node and owns one local kvstore */
class Application {
 public:
  size_t idx_;                 // index of node it is running on
  std::shared_ptr<KVStore> kv; // local kvstore

  Application(size_t idx, std::shared_ptr<NetworkIfc> net) : idx_(idx)
  {
    kv = std::make_shared<KVStore>(idx, net);
  }

  ~Application() = default;

  /** Invoke to run the application */
  virtual void run_() {}

  size_t this_node() { return idx_; }
};

class Demo : public Application {
 public:
  std::shared_ptr<Key> main = std::make_shared<Key>("main", 0);
  std::shared_ptr<Key> verify = std::make_shared<Key>("verif", 0);
  std::shared_ptr<Key> check = std::make_shared<Key>("ck", 0);

  Demo(size_t idx, std::shared_ptr<NetworkIfc> net) : Application(idx, net) {}

  void run_() override {
    kv->register_node();
    switch (this_node()) {
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

  void producer() {
    size_t SZ = 100 * 1000;
    std::vector<double> vals;
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) {
      vals.push_back(i);
      sum += i;
    }
    DataFrame::fromArray(main, kv, vals);
    DataFrame::fromScalar(check, kv, sum);
  }

  void counter() {
    Value val = kv->waitAndGet(*main);
    Deserializer dser(val.data(), val.length());
    auto df = DataFrame::deserialize(dser);
    size_t sum = 0;
    for (size_t i = 0; i < 100 * 1000; ++i) sum += df->get_double(0, i, kv);
    std::cout << "The sum is  " << sum << "\n";
    DataFrame::fromScalar(verify, kv, sum);
  }

  void summarizer() {
    Value val = kv->waitAndGet(*verify);
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
