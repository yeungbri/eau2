/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::CwC

#pragma once
#include "string.h"
#include "object.h"
#include <netinet/in.h>
#include "serial.h"

class StringArray {
public:
   String* vals_;
   size_t len_;

   StringArray() { }

   StringArray(String* vals, size_t len) {
       vals_ = vals;
       len_ = len;
   }
};

class DoubleArray {
public:
    double* vals_;
    size_t len_;

    DoubleArray(double* vals, size_t len) {
        vals_ = vals;
        len_ = len;
    }
};

enum class MsgKind {
    Ack, Nack, Put,
    Reply, Get, WaitAndGet, Status,
    Kill, Register, Directory
};

class Message : public Object {
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
};

class Ack : public Message {
public:
    Ack(MsgKind kind, size_t sender, size_t target, size_t id) : Message(kind, sender, target ,id) { };
};

class Status : public Message {
public:
    String* msg_; // owned

    Status(MsgKind kind, size_t sender, size_t target, size_t id, String* msg) : Message(kind, sender, target ,id) {
        msg_ = msg;
    };
};

class Register : public Message {
public:
    sockaddr_in client_;
    size_t port_;

    Register(MsgKind kind, size_t sender, size_t target, size_t id, sockaddr_in client, size_t port) : Message(kind, sender, target ,id) {
        client_ = client;
        port_ = port;
    };
};

class Directory : public Message {
public:
    size_t client_;
    size_t * ports_;  // owned
    StringArray addresses_;  // owned; strings owned

    Directory(MsgKind kind, size_t sender, size_t target, size_t id, size_t client, size_t* ports, StringArray addresses) : Message(kind, sender, target ,id) {
        client_ = client;
        ports_ = ports;
        addresses_ = addresses;
    };
};