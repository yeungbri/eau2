/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// Lang::CwC

#pragma once

// Possible types of messages that a client may send to a server
const char* REGISTER = "Register";
const char* DIRECTMSG = "DirectMsg";
const char* TEARDOWN = "TearDown";
const char* BROADCAST = "Broadcast";
const char* SHUTDOWN = "ShutDown";