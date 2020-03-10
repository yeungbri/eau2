/**
 * Provided code by CS4500 Instructors
 */

// Lang::CwC

#pragma once

#include <cstdlib>
#include <cstring>
#include <iostream>

/** Helper class providing some C++ functionality and convenience
 *  functions. This class has no data, constructors, destructors or
 *  virtual functions. Inheriting from it is zero cost.
 */
class Sys
{
public:
  // Printing functions
  Sys &p(char *c)
  {
    std::cout << c;
    return *this;
  }
  Sys &p(bool c)
  {
    std::cout << c;
    return *this;
  }
  Sys &p(float c)
  {
    std::cout << c;
    return *this;
  }
  Sys &p(int i)
  {
    std::cout << i;
    return *this;
  }
  Sys &p(size_t i)
  {
    std::cout << i;
    return *this;
  }
  Sys &p(const char *c)
  {
    std::cout << c;
    return *this;
  }
  Sys &p(char c)
  {
    std::cout << c;
    return *this;
  }
  Sys &pln()
  {
    std::cout << "\n";
    return *this;
  }
  Sys &pln(int i)
  {
    std::cout << i << "\n";
    return *this;
  }
  Sys &pln(char *c)
  {
    std::cout << c << "\n";
    return *this;
  }
  Sys &pln(bool c)
  {
    std::cout << c << "\n";
    return *this;
  }
  Sys &pln(char c)
  {
    std::cout << c << "\n";
    return *this;
  }
  Sys &pln(float x)
  {
    std::cout << x << "\n";
    return *this;
  }
  Sys &pln(size_t x)
  {
    std::cout << x << "\n";
    return *this;
  }
  Sys &pln(const char *c)
  {
    std::cout << c << "\n";
    return *this;
  }

  // Copying strings
  char *duplicate(const char *s)
  {
    char *res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
  }
  char *duplicate(char *s)
  {
    char *res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
  }

  // Function to terminate execution with a message
  void exit_if_not(bool b, char *c)
  {
    if (b)
      return;
    p("Exit message: ").pln(c);
    exit(-1);
  }

  // Definitely fail
  //  void FAIL() {
  void myfail()
  {
    pln("Failing");
    exit(1);
  }

  // Some utilities for lightweight testing
  void OK(const char *m) { pln(m); }
  void t_true(bool p)
  {
    if (!p)
      myfail();
  }
  void t_false(bool p)
  {
    if (p)
      myfail();
  }
};

/**
 * Because ports are passed in as char* arguments and must be ints to use in 
 * the C socket API, this method is used to convert char* ports to int ports.
 */
int portToInt(char *port)
{
  return std::stoi(port);
}

/**
 * Splits the given string at the given delimiter. 
 * Referenced from https://stackoverflow.com/a/9210560 on Thursday, 20FEB2020 at 12:09PM
 */
char **str_split(char *a_str, const char a_delim)
{
  char **result = 0;
  size_t count = 0;
  char *tmp = a_str;
  char *last_comma = 0;
  char delim[2];
  delim[0] = a_delim;
  delim[1] = 0;

  /* Count how many elements will be extracted. */
  while (*tmp)
  {
    if (a_delim == *tmp)
    {
      count++;
      last_comma = tmp;
    }
    tmp++;
  }

  /* Add space for trailing token. */
  count += last_comma < (a_str + strlen(a_str) - 1);

  /* Add space for terminating null string so caller
       knows where the list of returned strings ends. */
  count++;

  result = (char **)malloc(sizeof(char *) * count);

  if (result)
  {
    size_t idx = 0;
    char *token = strtok(a_str, delim);

    while (token)
    {
      assert(idx < count);
      *(result + idx++) = strdup(token);
      token = strtok(0, delim);
    }
    assert(idx == count - 1);
    *(result + idx) = 0;
  }

  return result;
}