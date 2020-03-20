/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once
#include "dataframe.h"
#include <map>

class Key {
public:
    std::string name_; // name to refer to key
    size_t home_;      // index of home node
    Key(std::string name, size_t home) : name_(name), home_(home) { }
};

class Value {
public:
    char* data_;
    Value(char* data) : data_(data) { }
};

class KVStore {
public:
    std::map<Key, Value> store_;

    KVStore() {

    }

    Value get(Key k) {
        auto search = store_.find(k);
        if (search != store_.end()) {
            return search->second;
        } else {
            std::cout << "Cannot get key " << k.name_ << "\n";
        }
    }

    void put(Key k, Value v) {
        store_.insert_or_assign(k, v);
    }

    Value wait_and_get(Key k) {

    }
};