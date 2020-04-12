/*
 * Code is referenced from CS4500 lecture, authored by Prof. Jan Vitek.
 */

// lang::Cpp

#pragma once
#include <arpa/inet.h>
#include <unistd.h>

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
class NetworkIfc {
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
 * Each node is identified by node idx and its socket address
 *
 * author: vitekj@me.com
 */
class NodeInfo {
 public:
  NodeInfo(size_t i, sockaddr_in a) : id(i), addr(a) {}

  size_t id;
  sockaddr_in addr;
};

/**
 * IP based network communications layer. Each node has an index between 0
 * and num_nodes - 1. The NodeInfo directory is ordered by node index. Each
 * node has a socket and an ip address
 *
 * author: vitekj@me.com
 */
class NetworkIP : public NetworkIfc {
 public:
  std::vector<NodeInfo*> nodes_;  // all nodes
  size_t this_node_;              // node index
  int sock_;                      // socket
  sockaddr_in ip_;                // ip address

  ~NetworkIP() { close(sock_); }

  /** Return this node's index */
  size_t index() { return this_node_; }

  void init_sock_(unsigned port) {
    assert((sock_ = socket(AF_INET, SOCK_STREAM, 0)) >= 0);
    int opt = 1;
    assert(setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == 0);
    ip_.sin_family = AF_INET;
    ip_.sin_addr.s_addr = INADDR_ANY;
    ip_.sin_port = htons(port);
    assert(bind(sock_, (sockaddr*) &ip_, sizeof(ip_)) >= 0);
    assert(listen(sock_, 100) >= 0); // 100 is connections queue size
  }

  /** Initialize node 0 */
  void server_init(size_t idx, size_t port) {
    this_node_ = idx;
    init_sock_(port);

    // register all the nodes and send out directory to all nodes

    // initialize node infos for each node
    for (size_t i = 0; i < arg.num_nodes; + i) {
      NodeInfo* n = new NodeInfo(0, ip_);
      nodes_.push_back(n);
    }
    // update node infos with info from register messages
    for (size_t i = 2; i < arg.num_nodes;
         ++i) {  // i = 2 since first client is on server node
      Register* msg = dynamic_cast<Register*>(recv_m());
      nodes_.at(msg->sender_)->id = msg->sender();
      nodes_.at(msg->sender_)->addr.sin_family = AF_INET;
      nodes_.at(msg->sender_)->addr.sin_addr = msg->client_.sin_addr;
      nodes_.at(msg->sender_)->addr.sin_port = htons(msg->port);
    }
    // keep track of which address is at which port
    std::vector<size_t> ports;
    std::vector<std::string> addrs;
    for (size_t i = 0; i < arg.num_nodes - 1; ++i) {
      ports.push_back(ntohs(nodes_.at(i + 1)->addr.sin_port));
      addrs.push_back(inet_ntoa(nodes_.at(i + 1)->addr.sin_addr));
    }
    // send directory to nodes
    Directory ipd(ports, addrs);
    for (size_t i = 1; i < arg.num_nodes; ++i) {
      ipd.target_ = i;
      send_m(&ipd);
    }
  }

  /** Initialize a client node */
  void client_init(size_t idx, size_t port, char* server_addr,
                   size_t server_port) {
    this_node_ = idx;
    init_sock_(port);
    nodes_.at(0)->id = 0;
    nodes_.at(0)->addr.sin_family = AF_INET;
    nodes_.at(0)->addr.sin_port = htons(server_port);
    if (inet_pton(AF_INET, server_addr, &nodes_.at(0)->addr.sin_addr) <= 0) {
      assert(false && "Invalid server IP address format");
    }

    // send register message
    Register msg(idx, port);
    send_m(&msg);

    // receive directory and update node info
    Directory* ipd = dynamic_cast<Directory*>(recv_m());
    std::vector<NodeInfo*> nodes(args.num_nodes);
    nodes.push_back(nodes_.at(0));
    for (size_t i = 1; i < ipd->clients; ++i) {
      // TODO: i or i + 1?
      nodes.at(i)->id = i;
      nodes.at(i)->addr.sin_family = AF_INET;
      nodes.at(i)->addr.sin_port = htons(ipd->ports_.at(i));
      if (inet_pton(AF_INET, server_addr, &nodes_.at(0)->addr.sin_addr) <= 0) {
        std::cout << "Invalid IP directory address for node " << i << std::endl;
      }
    }
    nodes_.clear();
    nodes_ = nodes;
    delete ipd;
  }

  /** Based on the message target, creates new connection to the appropraite 
   *  server and then serializes the message over the connection fd **/
  void send_m(Message* msg) {
    NodeInfo* tgt = nodes_.at(msg->target_);
    int conn = 
  }

  /** Listens on the server socket. When a message becomes availble, reads its
   * data dserialize it and return the object **/
  void recv_m() {}
};

/**
 * Communications layer between nodes represented by threads
 *
 * author: vitekj@me.com
 */
class NetworkPseudo : public NetworkIfc {
 public:
  ThreadNodeMap threads_;  // map thread ids to size_t
  std::vector<std::shared_ptr<MessageQueue>>
      msg_queues_;  // array of message queues, 1 per thread

  NetworkPseudo(size_t num_nodes) {
    for (size_t i = 0; i < num_nodes; i++) {
      msg_queues_.push_back(std::make_shared<MessageQueue>());
    }
  };

  virtual ~NetworkPseudo() = default;

  void register_node(size_t idx) {
    std::string tid = Thread::thread_id();
    threads_.set_u(tid, idx);
  }

  void send_msg(std::shared_ptr<Message> msg) {
    msg_queues_.at(msg->target_)->push(msg);
  }

  std::shared_ptr<Message> recv_msg() {
    std::string tid = Thread::thread_id();
    size_t i = threads_.get(tid);
    return msg_queues_.at(i)->pop();
  }

  virtual void print() {
    int i = 0;
    for (auto queue : msg_queues_) {
      std::cout << "PRINTING MESSAGES FOR NODE " << i << std::endl;
      queue->print();
      ++i;
    }
  }
};