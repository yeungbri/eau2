/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

#include "../src/application.h"

class DemoThread : public Thread {
 public:
  Demo* d_;
  DemoThread(Demo* d) : d_(d) {}

  void run() { d_->run_(); }
};

int main() {
  NetworkPseudo net(3);

  Demo d0(0, &net);
  Demo d1(1, &net);
  Demo d2(2, &net);

  DemoThread t1(&d0);
  DemoThread t2(&d1);
  DemoThread t3(&d2);

  t1.start();
  t2.start();
  t3.start();

  t1.join();
  t2.join();
  t3.join();

  return 0;
}