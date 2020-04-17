/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <netinet/in.h>

#include <iostream>
#include <string>
#include <vector>

#include "../kvstore/kv.h"

/** Types of messages */
enum class MsgKind
{
  Ack,
  Nack,
  Put,
  Reply,
  Get,
  WaitAndGet,
  Status,
  Kill,
  Register,
  Directory
};

/** Base class for network messages between nodes */
class Message
{
public:
  MsgKind kind_;  // the message kind
  size_t sender_; // the index of the sender node
  size_t target_; // the index of the receiver node
  size_t id_;     // an id t unique within the node

  Message(MsgKind kind, size_t sender, size_t target, size_t id)
  {
    kind_ = kind;
    sender_ = sender;
    target_ = target;
    id_ = id;
  }

  virtual ~Message() = default;

  Message(Deserializer &d)
  {
    kind_ = (MsgKind)d.read_size_t();
    sender_ = d.read_size_t();
    target_ = d.read_size_t();
    id_ = d.read_size_t();
  }

  /**
   * Serializes this message. Message subclasses will be responsible for
   * serializing their own unique fields.
   */
  virtual void serialize(Serializer &ser)
  {
    ser.write_size_t((size_t)kind_);
    ser.write_size_t(sender_);
    ser.write_size_t(target_);
    ser.write_size_t(id_);
  }

  virtual void print() = 0;

  /** Each message subclass is responsible for implementing its own dser. */
  static std::shared_ptr<Message> deserialize(Deserializer &d);
};

/**
 * Acknowledgement message for confirming a message has been received.
 */
class Ack : public Message
{
public:
  Ack(MsgKind kind, size_t sender, size_t target, size_t id)
      : Message(kind, sender, target, id){};

  Ack(Deserializer &d) : Message(d) {}

  virtual void print()
  {
    std::cout << "[ACK] from " << sender_ << " to " << target_ << std::endl;
  }
};

/**
 * Message for a KVStore to send to another node, requesting that the other
 * node puts this key-value pair in its KVStore.
 */
class Put : public Message
{
public:
  Key k_;
  Value v_;

  Put(MsgKind kind, size_t sender, size_t target, size_t id, Key &k, Value &v)
      : Message(kind, sender, target, id), k_(k), v_(v){};

  Put(Deserializer &d)
      : Message(d), k_(*Key::deserialize(d)), v_(*Value::deserialize(d)) {}

  void serialize(Serializer &ser)
  {
    Message::serialize(ser);
    k_.serialize(ser);
    v_.serialize(ser);
  }

  virtual void print()
  {
    std::cout << "[PUT] from " << sender_ << " to " << target_
              << ", key name: " << k_.name_ << std::endl;
  }
};

/**
 * A query message to another KVStore on another node to get a value from it.
 */
class Get : public Message
{
public:
  Key k_;

  Get(MsgKind kind, size_t sender, size_t target, size_t id, Key &k)
      : Message(kind, sender, target, id), k_(k){};

  Get(Deserializer &d) : Message(d), k_(*Key::deserialize(d)) {}

  void serialize(Serializer &ser)
  {
    Message::serialize(ser);
    k_.serialize(ser);
  }

  virtual void print()
  {
    std::cout << "[GET] from " << sender_ << " to " << target_
              << ", key name: " << k_.name_ << std::endl;
  }
};

/**
 * A response to a GET message, containing the data requested.
 */
class Reply : public Message
{
public:
  char *data_;
  size_t len_;

  Reply(MsgKind kind, size_t sender, size_t target, size_t id, char *data,
        size_t len)
      : Message(kind, sender, target, id), data_(data), len_(len){};

  Reply(Deserializer &d) : Message(d)
  {
    len_ = d.read_size_t();
    data_ = d.read_chars(len_);
  }

  void serialize(Serializer &ser)
  {
    Message::serialize(ser);
    ser.write_size_t(len_);
    ser.write_chars(data_, len_);
  }

  virtual void print()
  {
    std::cout << "[REPLY] from " << sender_ << " to " << target_ << std::endl;
  }
};

/**
 * Message for retrieving the cluster's status.
 * TODO: Not yet implemented.
 */
class Status : public Message
{
public:
  std::string msg_; // owned

  Status(MsgKind kind, size_t sender, size_t target, size_t id, std::string msg)
      : Message(kind, sender, target, id)
  {
    msg_ = msg;
  };

  Status(Deserializer &d) : Message(d) { msg_ = d.read_string(); }

  void serialize(Serializer &ser)
  {
    Message::serialize(ser);
    ser.write_string(msg_);
  }

  virtual void print() { std::cout << "[STATUS]" << std::endl; }
};

/**
 * Message for registering a node to a cluster
 */
class Register : public Message
{
public:
  sockaddr_in client_;
  size_t port_;

  Register(Deserializer &d) : Message(d)
  {
    client_ = d.read_sockaddr_in();
    port_ = d.read_size_t();
  }

  // Assumes node 0 is server
  Register(size_t sender, sockaddr_in client, size_t port)
      : Message(MsgKind::Register, sender, 0, 1), client_(client), port_(port) {}

  Register(MsgKind kind, size_t sender, size_t target, size_t id,
           sockaddr_in client, size_t port)
      : Message(kind, sender, target, id)
  {
    client_ = client;
    port_ = port;
  };

  void serialize(Serializer &ser)
  {
    Message::serialize(ser);
    ser.write_sockaddr_in(client_);
    ser.write_size_t(port_);
  }

  virtual void print() { std::cout << "[REGISTER]" << std::endl; }
};

/**
 * Message for geting a list of other nodes on the cluster
 * TODO: not yet implemented
 */
class Directory : public Message
{
public:
  // size_t client_;                       // I think this is for num of clients
  std::vector<size_t> ports_;          // owned
  std::vector<std::string> addresses_; // owned; strings owned

  Directory(Deserializer &d) : Message(d)
  {
    ports_ = d.read_size_t_vector();
    addresses_ = d.read_string_vector();
  }

  // Caller must modify target_ after creation
  Directory(size_t sender, std::vector<size_t> ports,
            std::vector<std::string> addrs)
      : Message(MsgKind::Directory, sender, 0, 1), ports_(ports),
        addresses_(addrs) {}

  Directory(MsgKind kind, size_t sender, size_t target, size_t id,
            // size_t client,
            std::vector<size_t> ports, std::vector<std::string> addresses)
      : Message(kind, sender, target, id)
  {
    // client_ = client;
    ports_ = ports;
    addresses_ = addresses;
  };

  void serialize(Serializer &ser)
  {
    Message::serialize(ser);
    ser.write_size_t_vector(ports_);
    ser.write_string_vector(addresses_);
  }

  size_t clients()
  {
    assert(ports_.size() == addresses_.size());
    return ports_.size();
  }

  virtual void print() { std::cout << "[DIRECTORY]" << std::endl; }
};

/**
 * Deserializes a message, depending on the type that's read from the
 * deserializer. Each class has a constructor that can deserialize itself.
 */
std::shared_ptr<Message> Message::deserialize(Deserializer &d)
{
  size_t msg_type = d.read_size_t();
  d.set_index(0);
  switch ((MsgKind)msg_type)
  {
  case MsgKind::Ack:
    return std::make_shared<Ack>(d);
  case MsgKind::Status:
    return std::make_shared<Status>(d);
  case MsgKind::Directory:
    return std::make_shared<Directory>(d);
  case MsgKind::Register:
    return std::make_shared<Register>(d);
  default:
    return nullptr;
  }
}