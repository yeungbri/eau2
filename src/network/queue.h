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

  void push(std::shared_ptr<Message> msg)
  {
    lock_.lock();
    queue_.push_back(msg);
    if (msg->kind_ == MsgKind::Get)
    {
      std::cout << "GET message pushed, key name is: " << std::dynamic_pointer_cast<Get>(msg)->k_.name_ << std::endl;
    } else 
    {
      std::cout << "message from " << msg->sender_ << " to " << msg->target_ << " pushed onto queue!" << std::endl;
    }
    std::cout << "All GET MESSAGES: " << std::endl;
    for (auto m : queue_)
    {
      std::cout << std::dynamic_pointer_cast<Get>(m)->k_.name_ << std::endl;
    }
    std::cout << "Size of message queue is now: " << queue_.size() << std::endl;
    lock_.unlock();
    lock_.notify_all();
  }

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

  size_t size()
  {
    return queue_.size();
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