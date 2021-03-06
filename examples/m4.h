/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <cassert>
#include <map>
#include <string>
#include <vector>

#include "../src/util/reader.h"
#include "../src/util/writer.h"
#include "../src/application.h"
#include "../src/dataframe/dataframe.h"
#include "../src/util/parser.h"

/****************************************************************************
 * Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 **********************************************************author: pmaj ****/
class WordCount : public Application
{
public:
  static const size_t BUFSIZE = 1024;
  Key in;
  std::vector<std::shared_ptr<Key>> kbuf;
  std::map<std::string, int> all;

  /**
   * Create a word counter given:
   * @param idx the node index of this application instance
   * @param net a network interface to send and recv messages
   * @param num_nodes how many nodes are running in total (must be more than 1)
   */
  WordCount(size_t idx, std::shared_ptr<NetworkIfc> net, int num_nodes)
      : Application(idx, net, num_nodes), in("data", idx)
  {
    kbuf.push_back(std::make_shared<Key>("wc-map-", idx));
  }

  /** The master nodes reads the input, then all of the nodes count. */
  void run_() override
  {
    if (idx_ == 0)
    {
      FileReader fr("../data/100k.txt");
      auto key = std::make_shared<Key>(in.name_, in.home_);
      DataFrame::fromVisitor(key, kv, "S", fr);
    }
    local_count();
    reduce();
  }

  /** Returns a key for given node.  These keys are homed on master node
   *  which then joins them one by one. */
  std::shared_ptr<Key> mk_key(size_t idx)
  {
    std::shared_ptr<Key> k = kbuf.at(idx);
    std::cout << "Created key " << k->name_;
    return k;
  }

  /** Compute word counts on the local node and build a data frame. */
  void local_count()
  {
    Value words = kv->waitAndGet(in);
    std::cout << "Node " << idx_ << ": starting local count..." << std::endl;
    std::map<std::string, int> map;
    Adder add(map);
    Deserializer dser(words.data(), words.length());
    auto df = DataFrame::deserialize(dser);
    df->local_map(add, kv);
    Summer cnt(map);
    DataFrame::fromVisitor(mk_key(idx_), kv, "SI", cnt);
  }

  /** Merge the data frames of all nodes */
  void reduce()
  {
    if (idx_ != 0)
      return;
    std::cout << "Node 0: reducing counts..." << std::endl;
    std::map<std::string, int> map;
    std::shared_ptr<Key> own = mk_key(0);
    Value val = kv->get(*own);
    Deserializer dser(val.data(), val.length());
    auto df1 = DataFrame::deserialize(dser);
    merge(df1, map);
    for (size_t i = 1; i < kv->num_nodes(); ++i)
    { // merge other nodes
      std::shared_ptr<Key> ok = mk_key(i);
      Value val = kv->waitAndGet(*ok);
      Deserializer dser(val.data(), val.length());
      auto df2 = DataFrame::deserialize(dser);
      merge(df2, map);
    }
    std::cout << "Different words: " << map.size() << std::endl;
  }

  void merge(std::shared_ptr<DataFrame> df, std::map<std::string, int> m)
  {
    Adder add(m);
    df->local_map(add, kv);
  }
};

/**
 * Runs the Milestone 4 Demo.
 * 
 * NOTE: This demo only works on one node. We have not yet implemented multi-
 * node word-counting for this milestone.
 * 
 * However, Milestone3 demonstrates that multiple nodes can correctly work 
 * together. This is something we will implement in the future.
 */
class Milestone4
{
public:
  static void run()
  {
    int num_nodes = 1;
    auto net = std::make_shared<NetworkPseudo>(num_nodes);
    WordCount word_counter(0, net, num_nodes);
    word_counter.run_();
    std::cout << "SUCCESS" << std::endl;
  }
};