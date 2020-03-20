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
        double* vals = new double[SZ];
        double sum = 0;
        for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
        Key key("triv", 0);
        DataFrame* df = DataFrame::fromArray(&key, &kv, SZ, vals);
        assert(df->get_double(0,1) == 1);
        DataFrame* df2 = kv.get(key);
        for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
        assert(sum==0);
        delete df; delete df2;
    }
};