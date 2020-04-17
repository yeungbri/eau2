/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// Lang::Cpp

#pragma once
#include "../src/data_adapter/adapter.h"

/**
 * A basic example to demonstrate reading from a .sor file, storing it in a 
 * dataframe object, performing a basic operation, and printing the results.
 */
class Milestone1
{
public:
  static void run()
  {
    auto store = std::make_shared<KVStore>();
    auto df = getDataFrame("../data/test2.sor", store);
    df->print(store);
    CounterRower countRower;
    df->map(countRower, store);
    std::cout << "Number of elements (counted by CountRower):" << countRower._count << std::endl;
    std::cout << "SUCCESS" << std::endl;
  }
};