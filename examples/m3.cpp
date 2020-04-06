/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

#include "../src/application.h"

/**
 * Runs the application on the specified thread.
 */
class DemoThread : public Thread {
 public:
  Demo d_;
  
  DemoThread(int node, std::shared_ptr<NetworkPseudo> net) : d_(node, net, 3) {};
  
  ~DemoThread() = default;

  void run() { d_.run_(); }
};

/**
 * Runs the demo code from the M3 specs. This program spawns 3 threads, one for
 * each of the application's tasks, and tests the dataframe's ability to 
 * distribute the load across 3 separate "nodes".
 */
int main() {
  auto net = std::make_shared<NetworkPseudo>(3);

  DemoThread t1(0, net);
  DemoThread t2(1, net); 
  DemoThread t3(2, net);

  t1.start();
  t2.start();
  t3.start();

  t1.join();
  t2.join();
  t3.join();

  return 0;
}