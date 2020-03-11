/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// Lang::CwC

#pragma once

#include <string>

// Possible types of messages that a client may send to a server
std::string REGISTER = "Register";
std::string DIRECTMSG = "DirectMsg";
std::string TEARDOWN = "TearDown";
std::string BROADCAST = "Broadcast";
std::string SHUTDOWN = "ShutDown";