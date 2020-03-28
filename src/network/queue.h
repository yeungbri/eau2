/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <vector>
#include <map>

#include "thread.h"
#include "message.h"

/**
 * FIFO queue of messages with atomic push and pop
 * author: vitekj@me.com
 */
class MessageQueue {
 public:
  std::vector<Message*> queue_;
  Lock lock_;

  MessageQueue() {}

  void push(Message* msg) {
    lock_.lock();
    queue_.push_back(msg);
    lock_.notify_all();
    lock_.unlock();
  }

  Message* pop() {
    lock_.lock();
    while (queue_.size() == 0) {
      lock_.wait();
    }
    Message* result = queue_.back();
    queue_.pop_back();
    lock_.unlock();
    return result;
  }
};

/**
 * Associates threads to node id's
 * author: vitekj@me.com
 */
class ThreadNodeMap {
 public:
    std::map<std::string, size_t> map;
    Lock lock_;

    /** Associates thread with node idx */
    void set_u(std::string k, size_t v) {
        lock_.lock();
        map.insert_or_assign(k, v);
        lock_.unlock();
    }

    /** Get the node idx associated with thread */
    size_t get(std::string k) {
        lock_.lock();
        size_t res;
        auto search = map.find(k);
        if (search != map.end()) {
            res = search->second;
        } else {
            std::cout << "Cannot get key " << k << "\n";
        }
        lock_.unlock();
        return res;
    }
};