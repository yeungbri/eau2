/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::CwC

#pragma once

#include "helper.h"
#include "string.h"

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object
{
public:
  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r) = 0;

  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b) = 0;
  virtual void accept(float f) = 0;
  virtual void accept(int i) = 0;
  virtual void accept(String *s) = 0;

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

  virtual void accept(float f)
  {
    _sys.p(f);
  }

  virtual void accept(int i)
  {
    _sys.p(i);
  }

  virtual void accept(String *s)
  {
    _sys.p(s);
  }

  virtual void done()
  {
    _sys.p(">");
  }
};