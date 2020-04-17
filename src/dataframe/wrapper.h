/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <string>

/**
 * A basic wrapper class that is extended by Bool, Int, Double and String.
 * The inheriting classes only have the value and a flag indicating if the
 * value is missing or not.
 * 
 * This is used when reading data in from the data adapter. Because there is
 * no good way of indicating missing values with just primitive types (since we
 * cannot just set them to be nullptr), Row values will be set using these
 * to determine if the value is missing or not (printing the row will reveal
 * missing values)
 */
class Wrapper
{
public:
  bool missing_ = false; // Values are present by default.

public:
  Wrapper() = default;
  virtual ~Wrapper() = default;

  virtual void mark_missing() { missing_ = true; }
  virtual bool is_missing() { return missing_; }
};

/**
 * Thin wrapper for boolean values.
 */
class Bool : public Wrapper
{
public:
  bool val_;
  Bool(bool val) : val_(val) {}
  bool val() { return val_; }
};

/**
 * Thin wrapper for integer values.
 */
class Int : public Wrapper
{
public:
  int val_;
  Int(int val) : val_(val) {}
  int val() { return val_; }
};

/**
 * Thin wrapper for double values.
 */
class Double : public Wrapper
{
public:
  double val_;
  Double(double val) : val_(val) {}
  double val() { return val_; }
};

/**
 * Thin wrapper for string values.
 */
class String : public Wrapper
{
public:
  std::string val_;
  String(std::string val) : val_(val) {}
  std::string val() { return val_; }
};