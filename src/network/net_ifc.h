/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <map>
#include <thread>
#include <cstdlib>
#include <mutex>
#include <condition_variable>
#include <sstream>

#include "message.h"

/**
 * NetworkIfc: Abstract communication layer between eau2 nodes.
 * Contains two implementations: one for in-process communications
 * and another for real network connections.
 */
class NetworkIfc {
 public:
  /** Registers node with given index to the cluster */
  virtual void register_node(size_t idx) {}

  /** Return the index of the node */
  virtual size_t index() {}

  /** Sends a message, msg is consumed (deleted) */
  virtual void send_msg(Message* msg) = 0;

  /** Waits for a message to arrive, message becomes owned */
  virtual Message* recv_msg() = 0;
};

/**
 * Communications layer between nodes represented by threads
 */
class NetworkPseudo : public NetworkIfc {
 public:
  std::map<std::string, size_t> threads_;  // map thread ids to size_t


  NetworkPsuedo() {}

  void register_node(size_t idx) {
    std::stringstream buf;
    buf << std::this_thread::get_id();
    std::string tid(buf.str());
    threads_.
  }

  void send_msg(Message* msg) {}

  Message* recv_msg() {

  }
};