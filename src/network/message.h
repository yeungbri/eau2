/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <netinet/in.h>

#include <string>
#include <vector>

#include "../serial.h"

/** Types of messages */
enum class MsgKind {
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
class Message {
 public:
  MsgKind kind_;   // the message kind
  size_t sender_;  // the index of the sender node
  size_t target_;  // the index of the receiver node
  size_t id_;      // an id t unique within the node

  Message(MsgKind kind, size_t sender, size_t target, size_t id) {
    kind_ = kind;
    sender_ = sender;
    target_ = target;
    id_ = id;
  }

  Message(Deserializer& d) {
    kind_ = (MsgKind)d.read_size_t();
    sender_ = d.read_size_t();
    target_ = d.read_size_t();
    id_ = d.read_size_t();
  }

  virtual void serialize(Serializer& ser) {
    ser.write_size_t((size_t)kind_);
    ser.write_size_t(sender_);
    ser.write_size_t(target_);
    ser.write_size_t(id_);
  }

  static Message* deserialize(Deserializer& d);
};

/** Acknowledgement message for confirming a message
 * has been received. */
class Ack : public Message {
 public:
  Ack(MsgKind kind, size_t sender, size_t target, size_t id)
      : Message(kind, sender, target, id){};

  Ack(Deserializer& d) : Message(d) {}
};

class Put : public Message {
 public:
  Key& k_;
  Value& v_;

  Put(MsgKind kind, size_t sender, size_t target, size_t id, Key& k, Value& v)
      : Message(kind, sender, target, id), k_(k), v_(v){};

  Put(Deserializer& d)
      : Message(d), k_(*Key::deserialize(d)), v_(*Value::deserialize(d)) {}

  void serialize(Serializer& ser) {
    Message::serialize(ser);
    k_.serialize(ser);
    v_.serialize(ser);
  }
};

class Get : public Message {
 public:
  Key& k_;

  Get(MsgKind kind, size_t sender, size_t target, size_t id, Key& k)
      : Message(kind, sender, target, id), k_(k){};

  Get(Deserializer& d) : Message(d), k_(*Key::deserialize(d)) {}

  void serialize(Serializer& ser) {
    Message::serialize(ser);
    k_.serialize(ser);
  }
};

class Reply : public Message {
 public:
  char* data_;
  size_t len_;

  Reply(MsgKind kind, size_t sender, size_t target, size_t id, char* data, size_t len)
      : Message(kind, sender, target, id), data_(data), len_(len){};

  Reply(Deserializer& d) : Message(d) {
    len_ = d.read_size_t();
    data_ = d.read_chars(len_);
  }

  void serialize(Serializer& ser) {
    Message::serialize(ser);
    ser.write_size_t(len_);
    ser.write_chars(data_, len_);
  }
};

/** Message for retrieving the cluster's status */
class Status : public Message {
 public:
  std::string msg_;  // owned

  Status(MsgKind kind, size_t sender, size_t target, size_t id, std::string msg)
      : Message(kind, sender, target, id) {
    msg_ = msg;
  };

  Status(Deserializer& d) : Message(d) { msg_ = d.read_string(); }

  void serialize(Serializer& ser) {
    Message::serialize(ser);
    ser.write_string(msg_);
  }
};

/** Message for registering a node to a cluster */
class Register : public Message {
 public:
  sockaddr_in client_;
  size_t port_;

  Register(MsgKind kind, size_t sender, size_t target, size_t id,
           sockaddr_in client, size_t port)
      : Message(kind, sender, target, id) {
    client_ = client;
    port_ = port;
  };
};

/** Message for geting a list of other nodes on the cluster */
class Directory : public Message {
 public:
  size_t client_;
  size_t* ports_;                       // owned
  std::vector<std::string> addresses_;  // owned; strings owned

  Directory(MsgKind kind, size_t sender, size_t target, size_t id,
            size_t client, size_t* ports, std::vector<std::string> addresses)
      : Message(kind, sender, target, id) {
    client_ = client;
    ports_ = ports;
    addresses_ = addresses;
  };
};

Message* Message::deserialize(Deserializer& d) {
  size_t msg_type = d.read_size_t();
  d.set_index(0);
  switch ((MsgKind)msg_type) {
    case MsgKind::Ack:
      return new Ack(d);
    case MsgKind::Status:
      return new Status(d);
  }
}