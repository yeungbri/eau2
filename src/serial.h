/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once
#include "helper.h"

class Serializer {
public:
  char* data_ = new char[1024];
  size_t length_ = 0;
  size_t capacity_ = 1024;

  // Double capacity if max capacity is reached
  void grow(size_t add_len) {
    if (length_ + add_len > capacity_) {
      capacity_ = 2 * capacity_;
      char *new_data = new char[capacity_];
      memcpy(new_data, data_, length_);
      delete[] data_;
      data_ = new_data;
    }
  }

  void write_size_t(size_t v) {
    memcpy(data_ + length_, &v, sizeof(size_t));
    length_ += sizeof(size_t);
    std::cout << length_ << "\n";
  }

  void write_chars(char* v, size_t len) {
    memcpy(data_ + length_, v, len);
    length_ += len;
  }

  void write_float(float v) {
    memcpy(data_ + sizeof(float), &v, sizeof(float));
    length_ += sizeof(float);
  }

  void write_string(std::string s) {
    write_size_t(s.length());
    write_chars(strdup(s.c_str()), s.size());
  }

  void write_string_vector(std::vector<std::string> v) {
    write_size_t(v.size());
    for (int i=0; i<v.size(); i++) {
      write_string(v.at(i));
    }
  }

  char* data() {
    return data_;
  }

  size_t length() {
    return length_;
  }
};

class Deserializer {
public:
  char* data_;
  size_t length_;

  Deserializer(char* data, size_t length) : data_(data), length_(length) {
    std::cout << sizeof(data_) << "\n";
  }

  size_t read_size_t() {
    size_t v;
    memcpy(&v, data_ + length_, sizeof(size_t));
    length_ += sizeof(size_t);
    return v;
  }

  char* read_chars(size_t len) {
    char* res = new char[len];
    memcpy(res, data_ + length_, len);
    length_ += len;
    return res;
  }

  float read_float() {
    float v;
    memcpy(&v, data_ + length_, sizeof(float));
    length_ += sizeof(float);
    return v;
  }

  std::string read_string() {
    size_t len = read_size_t();
    return std::string(read_chars(len)); // TODO: leak
  }

  std::vector<std::string> read_string_vector() {
    size_t vector_size = read_size_t();
    std::vector<std::string> res;
    for (int i=0; i<vector_size; i++) {
      res.push_back(read_string());
    }
    return res;
  }

  size_t length() {
    return length_;
  }
};