/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#include "m1.h"
#include "m2.h"
#include "m3.h"
#include "m4.h"
#include "m5.h"

/**
 * Runs each of the 5 milestones in succession. Exits upon completion.
 * 
 * Status of milestones:
 * 
 * 1: COMPLETE
 * 2: COMPLETE
 * 3: COMPLETE
 * 4: MOSTLY COMPLETE
 * 5: PENDING
 */
int main()
{
  std::cout << "\nRunning Milestone 1..." << std::endl;
  Milestone1::run();
  std::cout << "\nRunning Milestone 2..." << std::endl;
  Milestone2::run();
  std::cout << "\nRunning Milestone 3..." << std::endl;
  Milestone3::run();
  std::cout << "\nRunning Milestone 4..." << std::endl;
  Milestone4::run();
  std::cout << "\nRunning Milestone 5..." << std::endl;
  Milestone5::run();
  return 0;
}