/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

#include "../src/application.h"

class DemoThread : public Thread {
 public:
  Demo d_;
  DemoThread(int node, std::shared_ptr<NetworkPseudo> net) : d_(node, net) {};

  void run() { d_.run_(); }
};

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