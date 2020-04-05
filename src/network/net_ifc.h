/*
 * Code is referenced from CS4500 lecture, authored by Prof. Jan Vitek.
 */

// lang::Cpp

#pragma once
#include <vector>
#include "message.h"
#include "queue.h"
#include "thread.h"

/**
 * NetworkIfc: Abstract communication layer between eau2 nodes.
 * Contains two implementations: one for in-process communications
 * and another for real network connections.
 * 
 * author: vitekj@me.com
 */
class NetworkIfc
{
public:
  NetworkIfc() = default;
  virtual ~NetworkIfc() = default;

  /** Registers node with given index to the cluster */
  virtual void register_node(size_t idx) {}

  /** Return the index of the node */
  virtual size_t index() { return 0; }

  /** Sends a message, msg is consumed (deleted) */
  virtual void send_msg(std::shared_ptr<Message> msg) = 0;

  /** Waits for a message to arrive, message becomes owned */
  virtual std::shared_ptr<Message> recv_msg() = 0;
};

/**
 * Communications layer between nodes represented by threads
 * 
 * author: vitekj@me.com
 */
class NetworkPseudo : public NetworkIfc
{
public:
  ThreadNodeMap threads_;                                 // map thread ids to size_t
  std::vector<std::shared_ptr<MessageQueue>> msg_queues_; // array of message queues, 1 per thread

  NetworkPseudo(size_t num_nodes)
  {
    for (size_t i = 0; i < num_nodes; i++)
    {
      msg_queues_.push_back(std::make_shared<MessageQueue>());
    }
  };

  virtual ~NetworkPseudo() = default;

  void register_node(size_t idx)
  {
    std::string tid = Thread::thread_id();
    threads_.set_u(tid, idx);
  }

  void send_msg(std::shared_ptr<Message> msg)
  {
    msg_queues_.at(msg->target_)->push(msg);
  }

  std::shared_ptr<Message> recv_msg()
  {
    std::string tid = Thread::thread_id();
    size_t i = threads_.get(tid);
    return msg_queues_.at(i)->pop();
  }

  virtual void print()
  {
    int i = 0;
    for (auto queue : msg_queues_)
    {
      std::cout << "PRINTING MESSAGES FOR NODE " << i << std::endl;
      queue->print();
      ++i;
    }
  }
};