/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 * CS4500 Assignment 4
 */

//lang::CwC

#pragma once
#include "object.h"
#include "string.h"

/**
 * Represents an FloatArray data structure in CwC.
 */
class FloatArray
{
public:
  // creates an FloatArray of the specified size, initializing all elements to be nullptr
  FloatArray(size_t size)
  {
    size_ = 10;
    vals_ = new float[size_];
    len_ = 0;
  }

  // Default constructor that can be used to initialize a 10-element FloatArray
  FloatArray() : FloatArray(10) {}

  // destructor that clears out the FloatArray of values without touching any
  // of the individual elements
  ~FloatArray()
  {
    delete[] vals_;
  }

  // Removes the idx-th value from the FloatArray and returns it
  float remove(size_t idx)
  {
    float ret = vals_[idx];
    for (size_t i = idx; i < len_ - 1; ++i)
    {
      vals_[i] = vals_[i + 1];
    }
    len_--;
    return ret;
  }

  // Doubles the size of the FloatArray and moves existing values over
  void _grow()
  {
    size_ = size_ * 2;
    float* new_vals = new float[size_];
    for (size_t i = 0; i < len_; ++i)
    {
      new_vals[i] = vals_[i];
    }
    delete[] vals_;
    vals_ = new_vals;
  }

  // Adds an Object o onto end of this FloatArray
  void push_back(float o)
  {
    if (len_ >= size_)
    {
      _grow();
    }

    vals_[len_] = o;
    len_ += 1;
  }

  // Compares o with this FloatArray for value equality.
  bool equals(Object *o)
  {
    FloatArray *a = dynamic_cast<FloatArray *>(o);
    if (a->length() != length())
    {
      return false;
    }
    for (size_t i = 0; i < len_; ++i)
    {
      if (a->get(i) != (vals_[i]))
      {
        return false;
      }
    }
    return true;
  }

  // Returns the element at given index
  float get(size_t index)
  {
    return vals_[index];
  }

  // Replaces the element at i with e.
  float set(size_t i, float e)
  {
    if (i == len_)
    {
      ++len_;
    }
    float old = vals_[i];
    vals_[i] = e;
    return old;
  }

  // Return the number of vals_ in the collection.
  size_t length()
  {
    return len_;
  }

  // ================================= FIELDS =================================
  float* vals_;
  size_t len_;
  size_t size_;
};