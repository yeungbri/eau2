/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once
#include <netinet/in.h>
#include <vector>
#include <string>
#include "serial.h"

enum class MsgKind {
  Ack, Nack, Put,
  Reply, Get, WaitAndGet, Status,
  Kill, Register, Directory
};

class Message {
public:
  MsgKind kind_;  // the message kind
  size_t sender_; // the index of the sender node
  size_t target_; // the index of the receiver node
  size_t id_;     // an id t unique within the node

  Message(MsgKind kind, size_t sender, size_t target, size_t id) {
    kind_ = kind;
    sender_ = sender;
    target_ = target;
    id_ = id;
  }

  Message(Deserializer& d) {
    kind_ = (MsgKind) d.read_size_t();
    sender_ = d.read_size_t();
    target_ = d.read_size_t();
    id_ = d.read_size_t();
  }

  virtual void serialize(Serializer& ser) {
    ser.write_size_t((size_t) kind_);
    ser.write_size_t(sender_);
    ser.write_size_t(target_);
    ser.write_size_t(id_);
  }

  static Message* deserialize(Deserializer& d);
};

class Ack : public Message {
public:
  Ack(MsgKind kind, size_t sender, size_t target, size_t id) : 
    Message(kind, sender, target ,id) { };

  Ack(Deserializer& d) : Message(d) { }
};

class Status : public Message {
public:
  std::string msg_; // owned
  size_t msglen_;

  Status(MsgKind kind, size_t sender, size_t target, size_t id, std::string msg) : 
    Message(kind, sender, target ,id) {
    msg_ = msg;
  };

  Status(Deserializer& d) : Message(d) {
    msglen_ = d.read_size_t();
    msg_ = d.read_string(msglen_);
  }

  void serialize(Serializer& ser) {
    Message::serialize(ser);
    ser.write_size_t(msglen_);
    ser.write_string(msg_);
  }
};

class Register : public Message {
public:
  sockaddr_in client_;
  size_t port_;

  Register(MsgKind kind, size_t sender, size_t target, size_t id, sockaddr_in client, 
    size_t port) : Message(kind, sender, target ,id) {
    client_ = client;
    port_ = port;
  };
};

class Directory : public Message {
public:
  size_t client_;
  size_t * ports_;  // owned
  std::vector<std::string> addresses_;  // owned; strings owned

  Directory(MsgKind kind, size_t sender, size_t target, size_t id, size_t client, 
    size_t* ports, std::vector<std::string> addresses) : Message(kind, sender, target ,id) {
    client_ = client;
    ports_ = ports;
    addresses_ = addresses;
  };
};

Message* Message::deserialize(Deserializer& d) {
  switch((MsgKind) d.read_size_t()) {
    case MsgKind::Ack: return new Ack(d);
    case MsgKind::Status: return new Status(d);
  }
}