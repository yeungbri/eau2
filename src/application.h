/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <cassert>
#include <string>
#include <vector>
#include "dataframe/dataframe.h"

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

  void handle_put(std::shared_ptr<Message> msg)
  {
    // std::cout << idx_ << " received a PUT MESSAGE!!!!" << std::endl;
    auto put_msg = std::dynamic_pointer_cast<Put>(msg);
    // std::cout << "About to put " << put_msg->k_.name_ << " from " << put_msg->k_.home_ << " into node " << idx_ << std::endl;
    store_->put(put_msg->k_, put_msg->v_);
  }

  void handle_get(std::shared_ptr<Message> msg)
  {
    // std::cout << idx_ << " received a GET MESSAGE!!!!" << std::endl;
    auto get_msg = std::dynamic_pointer_cast<Get>(msg);
    Value val;
    try 
    {
      val = store_->get(get_msg->k_);
    } catch (std::runtime_error& ex)
    {
      val = Value(nullptr, 0);
    }
    // std::cout << "GET MESSAGE success! About to send reply!" << std::endl;
    auto reply_msg = std::make_shared<Reply>(
      MsgKind::Reply, idx_, get_msg->sender_, 0, val.data(), val.length());
    net_->send_msg(reply_msg);
  }

  void handle_reply(std::shared_ptr<Message> msg)
  {
    auto reply = std::dynamic_pointer_cast<Reply>(msg);
    store_->handle_reply(*reply);
  }

  virtual void run()
  {
    auto my_queue = std::dynamic_pointer_cast<NetworkPseudo>(net_)->msg_queues_.at(idx_);
    while (true)
    {
      // check if there are any new messages
      if (my_queue->size() > 0)
      {
        auto msg = my_queue->pop();
        switch (msg->kind_)
        {
        case MsgKind::Put:
          handle_put(msg);
          break;
        case MsgKind::Get:
          handle_get(msg);
          break;
        case MsgKind::Reply:
          // std::cout << idx_ << " received a REPLY MESSAGE!!!!" << std::endl;
          handle_reply(msg);
          break;
        default:
          std::cout << "Unknown message type!" << std::endl;
        }
      }
      else
      {
        Thread::sleep(1);
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
  Application(size_t idx, std::shared_ptr<NetworkIfc> net, size_t num_nodes) : idx_(idx)
  {
    kv = std::make_shared<KVStore>(idx, net, num_nodes);
  }

  ~Application() = default;

  /** Invoke to run the application. Application subclasses implement this. */
  virtual void run_() {}

  /** Returns this application's home node. */
  size_t this_node() { return idx_; }
};