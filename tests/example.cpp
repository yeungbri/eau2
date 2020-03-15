/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

#include "../src/data_adapter/adapter.h"

int main()
{
    DataFrame* df = getDataFrame("../data/test2.sor");
    df->print();
    CounterRower countRower;
    df->map(countRower);

    return 0;
}