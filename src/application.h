/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once
#include "dataframe.h"
#include "kvstore.h"
#include <string>
#include <vector>

class Application {
public:
  size_t idx_;
  KVStore kv = KVStore();

  Application(size_t idx) : idx_(idx) {
  }

  virtual void run_() { }

  size_t this_node() {
    return idx_;
  }
};

class Trivial : public Application {
public:
  Trivial(size_t idx) : Application(idx) { }

  void run_() {
    size_t SZ = 1000*1000;
    std::vector<float> vals;
    float sum = 0;
    for (size_t i = 0; i < SZ; ++i){
        vals.push_back(i);
        sum += i;
    }
    
    Key key("triv", 0);
    DataFrame* df = DataFrame::fromArray(&key, &kv, SZ, vals);
    assert(df->get_float(0, 1) == 1);
    
    DataFrame* df2 = kv.get(key);
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_float(0, i);
    assert(sum==0);
    
    delete df; delete df2;
  }
};

// class Demo : public Application {
// public:
//   Key main = Key("main", 0);
//   Key verify = Key("verif", 0);
//   Key check = Key("ck", 0);
 
//   Demo(size_t idx): Application(idx) { }

//   void run_() override {
//     switch(this_node()) {
//       case 0:   producer();     break;
//       case 1:   counter();      break;
//       case 2:   summarizer();
//     }
//   }

//   void producer() {
//     size_t SZ = 100*1000;
//     float* vals = new float[SZ];
//     float sum = 0;
//     for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
//     DataFrame::fromArray(&main, &kv, SZ, vals);
//     DataFrame::fromScalar(&check, &kv, sum);
//   }

//   void counter() {
//     DataFrame* v = kv.waitAndGet(main);
//     size_t sum = 0;
//     for (size_t i = 0; i < 100*1000; ++i) sum += v->get_float(0,i);
//     std::cout << "The sum is  " << sum << "\n";
//     DataFrame::fromScalar(&verify, &kv, sum);
//   }

//   void summarizer() {
//     DataFrame* result = kv.waitAndGet(verify);
//     DataFrame* expected = kv.waitAndGet(check);
//     std::cout << (expected->get_float(0,0)==result->get_float(0,0) ? "SUCCESS":"FAILURE") << "\n";
//   }
// };