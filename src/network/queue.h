/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>
#include <map>

#include "message.h"

/** A convenient lock and condition variable wrapper. */
class Lock {
 public:
  std::mutex mtx_;
  std::condition_variable_any cv_;

  /** Request ownership of this lock.
   *
   *  Note: This operation will block the current thread until the lock can
   *  be acquired.
   */
  void lock() { mtx_.lock(); }

  /** Release this lock (relinquish ownership). */
  void unlock() { mtx_.unlock(); }

  /** Sleep and wait for a notification on this lock.
   *
   *  Note: After waking up, the lock is owned by the current thread and
   *  needs released by an explicit invocation of unlock().
   */
  void wait() { cv_.wait(mtx_); }

  // Notify all threads waiting on this lock
  void notify_all() { cv_.notify_all(); }
};

/**
 * 
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
    Message* result = queue_.pop_back();
    lock_.unlock();
    return result;
  }
};

/**
 * Associates threads to node id's
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