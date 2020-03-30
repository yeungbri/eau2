/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <map>
#include "../network/net_ifc.h"
#include "../serial.h"

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
  std::shared_ptr<NetworkIfc> net_;
  size_t idx_;
  Lock lock_;
  size_t num_nodes_ = 1;

  KVStore(size_t idx, std::shared_ptr<NetworkIfc> net) : idx_(idx), net_(net) {}

  size_t num_nodes() { return num_nodes_; }

  void set_num_nodes(size_t num_nodes) { num_nodes_ = num_nodes; }

  /** 
   * Retrieves the associated value given the key. If it does not exist, 
   * this will print an error and return a nullptr value. This get is
   * non-blocking and assumes that the given key exists on this node.
   */
  Value get(Key &k)
  {
    Value res(nullptr, 0);
    lock_.lock();
    auto search = store_.find(k);
    if (search != store_.end())
    {
      res = search->second;
    }
    else
    {
      std::cout << "Cannot get key " << k.name_ << "\n";
    }
    lock_.unlock();
    return res;
  }

  /** 
   * This calls the non-blocking value if the key exists on this node. Otherwise,
   * it queries another node for the value, and blocks until it returns. 
   */
  Value waitAndGet(Key &k)
  {
    size_t target_idx = k.home_;
    if (target_idx == idx_)
    {
      return get(k);
    }
    else
    {
      // ask cluster
      auto get_msg = std::make_shared<Get>(MsgKind::Get, idx_, target_idx, 0, k);
      net_->send_msg(get_msg);
      auto re_msg = std::dynamic_pointer_cast<Reply>(net_->recv_msg());
      Value res = Value(re_msg->data_, re_msg->len_);
      return res;
    }
  }

  /** 
   * Associates the given value with the given key. If the key exists, the key's
   * value is overridden with the new value. If it does not, a new pair is
   * created. If the key should exist on another node, this KVStore will let
   * the other node know. It does not wait for an acknowledgement, so it is
   * non-blocking.
   */
  void put(Key &k, Value &v)
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