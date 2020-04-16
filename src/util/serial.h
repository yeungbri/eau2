/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <vector>
#include <string>
#include <cstring>
#include "../network/util/network.h"

/**
 * Serializer::
 *
 * A Serializer is used to serialize objects into binary form for 
 * writing to disk or sending over the network. The class provides
 * methods for serialzing primitives, which objects can use to 
 * implement serialize methods for themselves.
 */
class Serializer
{
public:
  char *data_ = new char[1024]; // stores binary representation of data
  size_t length_ = 0;           // length of binary representation of data
  size_t capacity_ = 1024;      // amount of bytes data can hold

  ~Serializer()
  {
    delete[] data_;
  }

  /** Doubles data capacity when it runs out of room */
  void grow(size_t add_len)
  {
    if (length_ + add_len > capacity_)
    {
      capacity_ = 2 * capacity_;
      char *new_data = new char[capacity_];
      memcpy(new_data, data_, length_);
      delete[] data_;
      data_ = new_data;
    }
  }

  /** Below are methods for serializing primitive data types */
  void write_size_t(size_t v)
  {
    grow(sizeof(size_t));
    memcpy(data_ + length_, &v, sizeof(size_t));
    length_ += sizeof(size_t);
  }

  void write_chars(char *v, size_t len)
  {
    grow(len);
    memcpy(data_ + length_, v, len);
    length_ += len;
  }

  void write_int(int v)
  {
    grow(sizeof(int));
    memcpy(data_ + length_, &v, sizeof(int));
    length_ += sizeof(int);
  }

  void write_bool(bool v)
  {
    grow(sizeof(bool));
    memcpy(data_ + length_, &v, sizeof(bool));
    length_ += sizeof(bool);
  }

  void write_double(double v)
  {
    grow(sizeof(double));
    memcpy(data_ + length_, &v, sizeof(double));
    length_ += sizeof(double);
  }

  void write_string(std::string s)
  {
    write_size_t(s.length());
    write_chars(strdup(s.c_str()), s.length());
  }

  void write_sockaddr_in(sockaddr_in si)
  {
    // write_size_t(si.sin_len);
    write_size_t(si.sin_family);
    write_size_t(si.sin_port);
    write_size_t(si.sin_addr.s_addr);
    // write_chars(8, si.sin_zero);
  }

  // Vectors of primitives
  void write_double_vector(std::vector<double> v)
  {
    write_size_t(v.size());
    for (size_t i = 0; i < v.size(); i++)
    {
      write_double(v.at(i));
    }
  }

  void write_int_vector(std::vector<int> v)
  {
    write_size_t(v.size());
    for (size_t i = 0; i < v.size(); i++)
    {
      write_int(v.at(i));
    }
  }

  void write_size_t_vector(std::vector<size_t> v)
  {
    write_size_t(v.size());
    for (size_t i = 0; i < v.size(); i++)
    {
      write_size_t(v.at(i));
    }
  }

  void write_bool_vector(std::vector<bool> v)
  {
    write_size_t(v.size());
    for (size_t i = 0; i < v.size(); i++)
    {
      write_bool(v.at(i));
    }
  }

  void write_string_vector(std::vector<std::string> v)
  {
    write_size_t(v.size());
    for (size_t i = 0; i < v.size(); i++)
    {
      write_string(v.at(i));
    }
  }

  /** Getter for binary representation of data */
  char *data()
  {
    return data_;
  }

  size_t length()
  {
    return length_;
  }
};

/**
 * Deserializer::
 *
 * The deserializer is the complement to the serializer class and
 * provides methods for deserializing primitives from the binary
 * data it is initialized with.
 */
class Deserializer
{
public:
  char *data_;    // binary representation of data
  size_t length_; // length of data
  size_t index_;  // index in the data to start deserializing at

  Deserializer(char *data, size_t length)
  {
    data_ = new char[length];
    memcpy(data_, data, length);
    length_ = length;
    index_ = 0;
  }

  ~Deserializer()
  {
    delete[] data_;
  }

  /** Used for "seeking" to the binary data, user must
   * know how long the proceeding data is. Common use case
   * is to reset the deserialization to 0 after learning of
   * the values in the first several fields */
  void set_index(size_t idx)
  {
    index_ = idx;
  }

  /** Deserialization methods for primitives */
  size_t read_size_t()
  {
    size_t v;
    memcpy(&v, data_ + index_, sizeof(size_t));
    index_ += sizeof(size_t);
    return v;
  }

  char *read_chars(size_t len)
  {
    char *res = new char[len + 1];
    memcpy(res, data_ + index_, len);
    index_ += len;
    res[len] = '\0';
    return res;
  }

  bool read_bool()
  {
    bool v;
    memcpy(&v, data_ + index_, sizeof(bool));
    index_ += sizeof(bool);
    return v;
  }

  int read_int()
  {
    int v;
    memcpy(&v, data_ + index_, sizeof(int));
    index_ += sizeof(int);
    return v;
  }

  double read_double()
  {
    double v;
    memcpy(&v, data_ + index_, sizeof(double));
    index_ += sizeof(double);
    return v;
  }

  std::string read_string()
  {
    size_t len = read_size_t();
    char *chars = read_chars(len);
    std::string res = std::string(chars);
    return res;
  }

  sockaddr_in read_sockaddr_in()
  {
    sockaddr_in res;
    //res.sin_len = (__uint8_t) read_size_t();
    res.sin_family = (sa_family_t) read_size_t();
    res.sin_port = (in_port_t) read_size_t();
    struct in_addr ia;
    ia.s_addr = (__uint32_t) read_size_t(); 
    res.sin_addr = ia;
    // res.sin_zero = read_chars(8);
    return res;
  }

  // Vectors of primitives
  std::vector<bool> read_bool_vector()
  {
    size_t vector_size = read_size_t();
    std::vector<bool> res;
    for (size_t i = 0; i < vector_size; i++)
    {
      res.push_back(read_bool());
    }
    return res;
  }

  std::vector<size_t> read_size_t_vector()
  {
    size_t vector_size = read_size_t();
    std::vector<size_t> res;
    for (size_t i = 0; i < vector_size; i++)
    {
      res.push_back(read_size_t());
    }
    return res;
  }

  std::vector<int> read_int_vector()
  {
    size_t vector_size = read_size_t();
    std::vector<int> res;
    for (size_t i = 0; i < vector_size; i++)
    {
      res.push_back(read_int());
    }
    return res;
  }

  std::vector<double> read_double_vector()
  {
    size_t vector_size = read_size_t();
    std::vector<double> res;
    for (size_t i = 0; i < vector_size; i++)
    {
      res.push_back(read_double());
    }
    return res;
  }

  std::vector<std::string> read_string_vector()
  {
    size_t vector_size = read_size_t();
    std::vector<std::string> res;
    for (size_t i = 0; i < vector_size; i++)
    {
      res.push_back(read_string());
    }
    return res;
  }

  size_t length()
  {
    return length_;
  }
};