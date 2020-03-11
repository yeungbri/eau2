/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// Lang::Cpp (<thread> library)

#include "util/parser.h"
#include "server.h"
#include <cstdio>

/**
 * Constantly listens for new connections and adds them to the server's list
 * of known sockets.
 */
void listenForRegistrations(Server* server)
{
  server->listenForRegistrations();
}

/**
 * Continually checks for messages received from other clients.
 */
void listenForMessages(Server* server)
{
  server->listenForMessages();
}

/**
 * Tests orderly shutdown after the server exits. Broadcasts a shutdown
 * message after 15 sseconds.
 */
void serverShutdownTest(Server* server)
{
  std::this_thread::sleep_for(std::chrono::milliseconds(15000));
  server->stop();
}

/**
 * Starts up a server that allows clients to register and communicate with each
 * other.
 */
int main(int argc, char** argv)
{
  Parser p;
  char* address = p.parseForFlagString("-ip", argc, argv);
  if (!address) {
    printf("Usage: ./server -ip <server ip>");
    return 1;
  }
  char** tokens = str_split(address, ':');
  char* ip = tokens[0];
  int port = std::stoi(tokens[1]);

  // ip is a const char*, and port is an int
  Server server(ip, port);

  std::thread t1(listenForRegistrations, &server);
  std::thread t2(listenForMessages, &server);
  std::thread t3(serverShutdownTest, &server);
  t1.join();
  t2.join();
  t3.join();
  return 0;
}