/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include "../dataframe/row.h"

/**
 * Generic parent class that can be overwritten.
 */
class Writer
{
public:
  Writer() = default;
  Writer(const Writer &other) = default;
  virtual ~Writer() = default;
};

/**
 * A Reader that reads in a given file. Provided to us by Professor 
 * Vitek for Milestone 4.
 */
class FileReader : public Writer
{
public:
  char *buf_;
  size_t end_ = 0;
  size_t i_ = 0;
  FILE *file_;
  static const size_t BUFSIZE = 1024;

public:
  /** Creates the reader and opens the file for reading.  */
  FileReader(const char *filename)
  {
    file_ = fopen(filename, "r");
    if (file_ == nullptr)
      std::cout << "Cannot open file " << filename << std::endl;
    buf_ = new char[BUFSIZE + 1]; //  null terminator
    fillBuffer_();
    skipWhitespace_();
  }

  virtual ~FileReader() = default;

  /** Reads next word and stores it in the row. Actually read the word.
      While reading the word, we may have to re-fill the buffer  */
  void visit(Row &r)
  {
    assert(i_ < end_);
    assert(!isspace(buf_[i_]));
    size_t wStart = i_;
    while (true)
    {
      if (i_ == end_)
      {
        if (feof(file_))
        {
          ++i_;
          break;
        }
        i_ = wStart;
        wStart = 0;
        fillBuffer_();
      }
      if (isspace(buf_[i_]))
        break;
      ++i_;
    }
    buf_[i_] = 0;
    std::string word(buf_ + wStart, i_ - wStart);
    r.set(0, word);
    ++i_;
    skipWhitespace_();
  }

  /** Returns true when there are no more words to read.  There is nothing
     more to read if we are at the end of the buffer and the file has
     all been read.     */
  bool done() { return (i_ >= end_) && feof(file_); }

  /** Reads more data from the file. */
  void fillBuffer_()
  {
    size_t start = 0;
    // compact unprocessed stream
    if (i_ != end_)
    {
      start = end_ - i_;
      memcpy(buf_, buf_ + i_, start);
    }
    // read more contents
    end_ = start + fread(buf_ + start, sizeof(char), BUFSIZE - start, file_);
    i_ = start;
  }

  /** Skips spaces.  Note that this may need to fill the buffer if the
      last character of the buffer is space itself.  */
  void skipWhitespace_()
  {
    while (true)
    {
      if (i_ == end_)
      {
        if (feof(file_))
          return;
        fillBuffer_();
      }
      // if the current character is not whitespace, we are done
      if (!isspace(buf_[i_]))
        return;
      // otherwise skip it
      ++i_;
    }
  }
};

/***************************************************************************/
/**
 * A Writer visitor that, when called with a Row, will populate that row
 * with words and counts from its map. 
 * 
 * The user must not visit past the end of this map.
 */
class Summer : public Writer
{
public:
  std::map<std::string, int> map_;  // Stores words -> word counts
  size_t idx = 0;                   // Remembers which pair was written last

  Summer(std::map<std::string, int> map) : map_(map) {}
  virtual ~Summer() = default;

  /**
   * Writes the current word->word count pair into the given row.
   */
  void visit(Row &r)
  {
    if (!done())
    {
      auto it = map_.begin();
      for (size_t i = 0; i < idx; ++i)
      {
        it++;
      }
      std::string key = it->first;
      size_t value = it->second;
      r.set(0, key);
      r.set(1, (int)value);
    } else
    {
      std::cout << "Exceeded end of map!" << std::endl;
    }
  }

  /**
   * Returns true if the map has been completely written.
   */
  bool done()
  {
    return idx == map_.size();
  }
};
