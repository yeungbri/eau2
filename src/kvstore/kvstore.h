/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <map>
#include "../network/net_ifc.h"
#include "../util/serial.h"

/** 
 * Used for comparing keys in a std::map. Needed to maintain order and
 * for comparing and retrieving keys. Uses the name_ of the key because
 * the name_ is a unique field of each key.
 */
struct KeyCompare
{
  bool operator()(const Key &lhs, const Key &rhs) const
  {
    return lhs.name_ < rhs.name_;
  }
};

/** 
 * Key Value Store - users can associate keys with values and retrieve them.
 * 
 * The KVStore is responsible for querying other nodes for values if it does
 * not have the requested value, and it is responsible for putting pairs in 
 * other nodes if the value does not belong in this store.
 * 
 * The KVStore is also responsible for registering with the network upon
 * instantiation, so other nodes can query it.
 * 
 * This store utilizes a lock to make it thread-safe.
 */
class KVStore
{
public:
  std::map<Key, Value, KeyCompare> store_;
  size_t idx_;
  std::shared_ptr<NetworkIfc> net_;
  Lock lock_;
  size_t num_nodes_ = 1;
  std::vector<std::shared_ptr<Value>> replies_;
  size_t MAX_REPLY_SIZE = 1000;

  KVStore(size_t idx, std::shared_ptr<NetworkIfc> net, size_t num_nodes) : idx_(idx), net_(net), num_nodes_(num_nodes) {}
  ~KVStore() = default;

  size_t num_nodes() { return num_nodes_; }

  void set_num_nodes(size_t num_nodes) { num_nodes_ = num_nodes; }

  void handle_reply(Reply& reply)
  {
    lock_.lock();
    auto value = std::make_shared<Value>(reply.data_, reply.len_);
    replies_.push_back(value);
    lock_.unlock();
    lock_.notify_all();
  }

  std::shared_ptr<Value> wait_and_pop()
  {
    lock_.lock();
    while (replies_.size() == 0)
    {
      lock_.wait();
    }
    auto result = replies_.back();
    replies_.pop_back();
    lock_.unlock();
    lock_.notify_all();
    return result;
  }

  /** 
   * Retrieves the associated value given the key. If it does not exist, 
   * this will print an error and return a nullptr value. This get is
   * non-blocking and assumes that the given key exists on this node.
   */
  Value& get(Key k)
  {
    int tries = 0;
    lock_.lock();
    while (tries < 5)
    {
      auto search = store_.find(k);
      if (search != store_.end())
      {
        lock_.unlock();
        auto ret = search->second;
        return search->second;
      }
      Thread::sleep(1);
      tries += 1;
    }
    lock_.unlock();
    throw std::runtime_error("Cannot find key!");
  }

  Value& wait_and_get_help(Key& k)
  {
    // ask cluster
    while (true)
    {
      auto get_msg = std::make_shared<Get>(MsgKind::Get, idx_, k.home_, 0, k);
      net_->send_msg(get_msg);
      auto val = wait_and_pop();
      if (val->length() != 0)
      {
        return *val;
      }
      // std::cout << "Key " << k.name_ << " doesn't exist yet, trying again" << std::endl;
      Thread::sleep(1);
    }
  }

  /** 
   * This calls the non-blocking value if the key exists on this node. Otherwise,
   * it queries another node for the value, and blocks until it returns. 
   */
  Value& waitAndGet(Key k)
  {
    size_t target_idx = k.home_;
    if (target_idx == idx_)
    {
      return get(k);
    }
    else
    {
      return wait_and_get_help(k);
    }
  }

  /** 
   * Associates the given value with the given key. If the key exists, the key's
   * value is overridden with the new value. If it does not, a new pair is
   * created. If the key should exist on another node, this KVStore will let
   * the other node know. It does not wait for an acknowledgement, so it is
   * non-blocking.
   */
  void put(Key k, Value v)
  {
    size_t target_idx = k.home_;
    if (target_idx == idx_)
    {
      lock_.lock();
      store_.insert_or_assign(k, v);
      lock_.unlock();
    }
    else
    {
      // std::cout << "currently on node " << idx_ << " but message needs to go to node " << target_idx << std::endl;
      auto put_msg = std::make_shared<Put>(MsgKind::Put, idx_, target_idx, 0, k, v);
      net_->send_msg(put_msg);
    }
  }

  /** 
   * Registers node with cluster. Called after the constructor AFTER this 
   * KVStore has been split off in its own thread (so net_ has the correct
   * thread id)
   */
  void register_node()
  {
    net_->register_node(idx_);
  }
};