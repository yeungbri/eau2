/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::CwC

#pragma once
#include "row.h"
#include "fielder.h"
#include "object.h"
#include "helper.h"

/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 */
class Rower : public Object
{
public:
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
  virtual void join_delete(Rower *other)
  {
    return;
  }
};

class PrintRower : public Rower
{
public:
  Fielder* _fielder;
  Sys _sys;

  PrintRower() : _sys() {
    _fielder = new PrintFielder();
  }

  virtual ~PrintRower() {
    delete _fielder;
  }

  virtual bool accept(Row& r)
  {
    for (size_t i = 0; i < r.width(); ++i)
    {
      r.visit(i, *_fielder);
      _sys.p("\n");
    }
    return true;
  }
};

// Returns true if search string is in the row
class StringSearchRower : public Rower
{
public:
  String* _search_str;
  Sys _sys;

  StringSearchRower(const char* search_str) : _sys() {
    _search_str = new String(search_str);
  }

  virtual ~StringSearchRower() {
    delete _search_str;
  }

  virtual bool accept(Row& r)
  {
    for (size_t i = 0; i < r.width(); ++i)
    {
      if (r.col_type(i) == 'S') {
        if (r.get_string(i)->equals(_search_str)) {
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
  Sys _sys;

  IntSumRower() : _sys() {
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

  virtual IntSumRower* clone()
  {
    return new IntSumRower();
  }

  virtual void join_delete(Rower* other)
  {
    _sum = _sum + dynamic_cast<IntSumRower*>(other)->_sum;
    delete other;
  }
};

// Returns true if search string is in the row
class CounterRower : public Rower
{
public:
  size_t _count = 0;
  Sys _sys;

  CounterRower() : _sys() {}

  virtual ~CounterRower() = default;

  virtual bool accept(Row& r)
  {
    _count += r.width();
    return true;
  }

  virtual CounterRower* clone()
  {
    return new CounterRower();
  }

  virtual void join_delete(Rower* other)
  {
    _count += dynamic_cast<CounterRower*>(other)->_count;
    delete other;
  }
};

// Gets the count of the given char
class CharCountRower : public Rower
{
public:
  char _search_char;
  size_t _count;
  Sys _sys;

  CharCountRower(char search_char) : _sys() {
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
        char* str = r.get_string(i)->c_str();
        for (size_t j=0; j<strlen(str); j++) {
          if (str[j] == _search_char) {
            _count++;
          }
        }
      }
    }
    return true;
  }

  virtual CharCountRower* clone()
  {
    return new CharCountRower(_search_char);
  }

  virtual void join_delete(Rower* other)
  {
    _count += dynamic_cast<CharCountRower*>(other)->_count;
    delete other;
  }
};