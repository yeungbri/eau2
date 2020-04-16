/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// Lang::Cpp

#pragma once
#include "../src/application.h"

/** Trival application to test serialization on a single node */
class Trivial : public Application {
 public:
  Trivial(size_t idx, std::shared_ptr<NetworkIfc> net) : Application(idx, net, 1) {}

  /** Creates a dataframe from all the numbers from 1 - 999999 and
   * stores it onto the local kvstore. Then retrieves the stored
   * value and checks all values are there */
  void run_() {
    size_t SZ = 1000 * 1000;
    std::vector<double> vals;
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) {
      vals.push_back(i);
      sum += i;
    }
    auto key = std::make_shared<Key>("triv", 0);
    auto df = DataFrame::fromArray(key, kv, vals);
    assert(df->get_double(0, 1, kv) == 1);

    Value val = kv->get(*key);
    Deserializer dser(val.data(), val.length());
    auto df2 = DataFrame::deserialize(dser);
    for (size_t i = 0; i < SZ; ++i) {
      assert(df2->get_double(0, i, kv) == i);
      sum -= df2->get_double(0, i, kv);
    }
    assert(sum == 0);
    std::cout << "SUCCESS" << std::endl;
  }
};


/** 
 * Trivial example from Milestone 2 that demonstrates the dataframe's 
 * ability to read integrate with a KV store.
 */
class Milestone2
{
public:
  static void run()
  {
    auto net = std::make_shared<NetworkPseudo>(1);
    Trivial t(0, net);
    t.run_();
  }
};