/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once

/**
 * Generic Reader class that can be overridden;
 */
class Reader
{
public:
  Reader() = default;
  Reader(const Reader &other) = default;
  virtual ~Reader() = default;
  virtual bool visit(Row &row) { return false; }
};

/****************************************************************************/
/**
 * Populates its map with values, given a row that contains word->word-count
 * pairs.
 * 
 * Once this adder is "done", it's map contains a complete list of words and 
 * their counts that are local to this node.
 */
class Adder : public Reader
{
public:
  std::map<std::string, int> map_; // word to word-count pairs

public:
  Adder(std::map<std::string, int> map) : map_(map) {}
  virtual ~Adder() = default;

  /**
   * Updates this map with the pair present in the given Row. The 
   * row must not be malformed. Otherwise, there will be undefined
   * behavior.
   */
  bool visit(Row &r)
  {
    std::string word = r.get_string(0);
    assert(word != "");
    auto search = map_.find(word);
    if (search != map_.end())
    {
      int num = search->second;
      num++;
      map_.insert_or_assign(word, num);
    }
    else
    {
      map_.insert_or_assign(word, 0);
    }
    return true;
  }
};
