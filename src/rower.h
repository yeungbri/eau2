/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once
#include <iostream>
#include "row.h"
#include "fielder.h"

/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 */
class Rower
{
public:
  virtual ~Rower() { }

  /** This method is called once per row. The row object is on loan and
      should not be retained as it is likely going to be reused in the next
      call. The return value is used in filters to indicate that a row
      should be kept. */
  virtual bool accept(Row &r)
  {
    return true;
  }

  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  virtual void join_delete(std::shared_ptr<Rower> other)
  {
    return;
  }

  virtual std::shared_ptr<Rower> clone() {
    return nullptr;
  }
};

class PrintRower : public Rower
{
public:
  std::shared_ptr<Fielder> _fielder;

  PrintRower() 
  {
    _fielder = std::make_shared<PrintFielder>();
  }

  virtual ~PrintRower() = default;

  virtual bool accept(Row& r)
  {
    r.visit(*_fielder);
    std::cout << "\n";
    return true;
  }
};

// Returns true if search string is in the row
class StringSearchRower : public Rower
{
public:
  std::string _search_str;

  StringSearchRower(const char* search_str)
  {
    _search_str = std::string(search_str);
  }

  virtual ~StringSearchRower() { }

  virtual bool accept(Row& r)
  {
    for (size_t i = 0; i < r.width(); ++i)
    {
      if (r.col_type(i) == 'S') {
        if (r.get_string(i) == _search_str) {
          return true;
        }
      }
    }
    return false;
  }
};

// Returns true if int is in the row
class IntSumRower : public Rower
{
public:
  size_t _sum;

  IntSumRower()
  {
    _sum = 0;
  }

  virtual ~IntSumRower() = default;

  virtual bool accept(Row& r)
  {
    for (size_t i = 0; i < r.width(); ++i)
    {
      if (r.col_type(i) == 'I') {
        _sum += r.get_int(i);
      }
    }
    return true;
  }

  virtual std::shared_ptr<Rower> clone()
  {
    return std::make_shared<IntSumRower>();
  }

  virtual void join_delete(std::shared_ptr<Rower> other)
  {
    _sum = _sum + std::dynamic_pointer_cast<IntSumRower>(other)->_sum;
  }
};

// Returns true if search string is in the row
class CounterRower : public Rower
{
public:
  size_t _count = 0;

  CounterRower() = default;

  virtual ~CounterRower() = default;

  virtual bool accept(Row& r)
  {
    _count += r.width();
    return true;
  }

  virtual std::shared_ptr<Rower> clone()
  {
    return std::make_shared<CounterRower>();
  }

  virtual void join_delete(std::shared_ptr<Rower> other)
  {
    _count += std::dynamic_pointer_cast<CounterRower>(other)->_count;
  }
};

// Gets the count of the given char
class CharCountRower : public Rower
{
public:
  char _search_char;
  size_t _count;

  CharCountRower(char search_char)
  {
    _search_char = search_char;
    _count = 0;
  }

  virtual ~CharCountRower() {
  }

  virtual bool accept(Row& r)
  {
    for (size_t i = 0; i < r.width(); ++i)
    {
      if (r.col_type(i) == 'S') {
        std::string str = r.get_string(i);
        for (size_t j=0; j<str.length(); j++) {
          if (str[j] == _search_char) {
            _count++;
          }
        }
      }
    }
    return true;
  }

  virtual std::shared_ptr<Rower> clone()
  {
    return std::make_shared<CharCountRower>(_search_char);
  }

  virtual void join_delete(std::shared_ptr<Rower> other)
  {
    _count += std::dynamic_pointer_cast<CharCountRower>(other)->_count;
  }
};