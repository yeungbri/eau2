/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

//lang::Cpp

#pragma once

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

  PrintFielder() = default;

  virtual ~PrintFielder() = default;

  virtual void start(size_t r)
  {
    std::cout << "<";
  }

  virtual void accept(bool b)
  {
    std::cout << b;
  }

  virtual void accept(double f)
  {
    std::cout << f;
  }

  virtual void accept(int i)
  {
    std::cout << i;
  }

  virtual void accept(std::string s)
  {

    std::cout << s;
  }

  virtual void done()
  {
    std::cout << ">";
  }
};