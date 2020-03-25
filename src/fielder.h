/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once

#include "helper.h"
#include <string>

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder
{
public:
  virtual ~Fielder() { }
  
  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r) = 0;

  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b) = 0;
  virtual void accept(double f) = 0;
  virtual void accept(int i) = 0;
  virtual void accept(std::string s) = 0;

  /** Called when all fields have been seen. */
  virtual void done() = 0;
};

class PrintFielder : public Fielder
{
public:
  Sys _sys;

  PrintFielder() : _sys() {}

  virtual ~PrintFielder() = default;

  virtual void start(size_t r)
  {
    _sys.p("<");
  }

  virtual void accept(bool b)
  {
    _sys.p(b);
  }

  virtual void accept(double f)
  {
    _sys.p(f);
  }

  virtual void accept(int i)
  {
    _sys.p(i);
  }

  virtual void accept(std::string s)
  {

    _sys.p(s.c_str());
  }

  virtual void done()
  {
    _sys.p(">");
  }
};