/*
 * Code is referenced from CS4500 lecture, authored by Prof. Jan Vitek.
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
class MessageQueue
{
public:
  std::vector<std::shared_ptr<Message>> queue_;
  Lock lock_;

  MessageQueue() {}

  /**
   * Pushes message on to queue
   */
  void push(std::shared_ptr<Message> msg)
  {
    lock_.lock();
    queue_.push_back(msg);
    lock_.unlock();
    lock_.notify_all();
  }

  /**
   * Removes and returns the first message in the queue
   */
  std::shared_ptr<Message> pop()
  {
    lock_.lock();
    while (queue_.size() == 0)
    {
      lock_.wait();
    }
    auto result = queue_.back();
    queue_.pop_back();
    lock_.unlock();
    lock_.notify_all();
    return result;
  }

  /**
   * Returns size of queue
   */
  size_t size()
  {
    return queue_.size();
  }

  /**
   * Prints the contents of this queue
   */
  void print()
  {
    for (auto m : queue_)
    {
      m->print();
    }
  }
};

/**
 * Associates threads to node id's
 * author: vitekj@me.com
 */
class ThreadNodeMap
{
public:
  std::map<std::string, size_t> map;
  Lock lock_;

  /** Associates thread with node idx */
  void set_u(std::string k, size_t v)
  {
    lock_.lock();
    map.insert_or_assign(k, v);
    lock_.unlock();
  }

  /** Get the node idx associated with thread */
  size_t get(std::string k)
  {
    lock_.lock();
    auto search = map.find(k);
    if (search != map.end())
    {
      lock_.unlock();
      return search->second;
    }
    else
    {
      lock_.unlock();
      std::string errmsg = "Cannot get key: ";
      errmsg += k;
      throw std::runtime_error(errmsg);
    }
  }
};