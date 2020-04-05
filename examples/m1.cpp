/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

#include "../src/data_adapter/adapter.h"

/**
 * A basic example to demonstrate reading from a .sor file, storing it in a 
 * dataframe object, performing a basic operation, and printing the results.
 */
int main()
{
    auto store = std::make_shared<KVStore>();
    auto df = getDataFrame("../data/test2.sor", store);
    //df->print();
    CounterRower countRower;
    //df->map(countRower);

    return 0;
}