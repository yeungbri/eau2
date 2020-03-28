/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <map>

#include "network/net_ifc.h"
#include "serial.h"

/** Key of a key-value store which consists of a name and
 * the home node that it exists on */
class Key {
 public:
  std::string name_;  // name to refer to key
  size_t home_;       // index of home node
  Key(std::string name, size_t home) : name_(name), home_(home) {}

  // TODO: explain what this is doing
  /** Comparator for using Key in a std::map */
  bool operator==(const Key& other) {
    return name_ == other.name_ && home_ == other.home_;
  }

  void serialize(Serializer ser) {
    ser.write_string(name_);
    ser.write_size_t(home_);
  }

  static Key* deserialize(Deserializer dser) {
    std::string name = dser.read_string();
    size_t home = dser.read_size_t();
    return new Key(name, home);
  }
};

/** Stores the binary representation of an object in the kv-store */
class Value {
 public:
  char* data_;     // serialized data
  size_t length_;  // length of serialized data

  Value(char* data, size_t length) : length_(length) {
    data_ = new char[length];
    // Data is not guaranteed to be available i.e. across network
    memcpy(data_, data, length);
  }

  ~Value() { delete[] data_; }

  char* data() { return data_; }

  size_t length() { return length_; }

  void serialize(Serializer ser) {
    ser.write_size_t(length_);
    ser.write_chars(data_, length_);
  }

  static Value* deserialize(Deserializer dser) {
    size_t len = dser.read_size_t();
    char* data = dser.read_chars(len);
    return new Value(data, len);
  }
};

// TODO: explain what this is doing
/** Used for comparing keys in a std::map */
struct KeyCompare {
  bool operator()(const Key& lhs, const Key& rhs) const {
    return lhs.home_ < rhs.home_;
  }
};

/** Key Value Store - users can associate keys with values and
 * retrieve them */
class KVStore {
 public:
  std::map<Key, Value, KeyCompare> store_;
  NetworkIfc* net_;
  size_t idx_;
  Lock lock_;

  KVStore() {}

  size_t num_nodes() { return 3; }

  /** Retrieves the associated value given the key */
  Value get(Key& k) {
    Value res("", 0);
    lock_.lock();
    auto search = store_.find(k);
    if (search != store_.end()) {
      res = search->second;
    } else {
      std::cout << "Cannot get key " << k.name_ << "\n";
    }
    lock_.unlock();
    return res;
  }

  /** Blocking get */
  Value waitAndGet(Key& k) {
    size_t target_idx = k.home_;
    if (target_idx == idx_) {
      return get(k);
    } else {
      // ask cluster
      Get* get_msg = new Get(MsgKind::Get, idx_, target_idx, 0, k);
      net_->send_msg(get_msg);
      Reply* re_msg = dynamic_cast<Reply*>(net_->recv_msg());
      Value res = Value(re_msg->data_, re_msg->len_);
      return res;
    }
  }

  /** Associates the given value with the given key */
  void put(Key& k, Value& v) {
    size_t target_idx = k.home_;
    if (target_idx == idx_) {
      lock_.lock();
      store_.insert_or_assign(k, v);
      lock_.unlock();
    } else {
      Put* put_msg = new Put(MsgKind::Put, idx_, target_idx, 0, k, v);
      net_->send_msg(put_msg);
    }
  }

  // TODO: move to constructor
  /** Registers node with cluster */
  void register_node(size_t idx) {
    idx_ = idx;
    net_->register_node(idx);
  }

  void set_net(NetworkIfc* net) {
    // net_ = dynamic_cast<NetworkPseudo*>(net);
    net_ = net;
  }
};