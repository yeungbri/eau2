/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// Lang::Cpp (<thread> library)

#include "util/parser.h"
#include "client.h"
#include <cstdio>

/**
 * Continously listens to the given client's server for broadcasts.
 */
void listenToServer(Client* c)
{
  c->listenToServer();
}

/**
 * Assumes 3 clients exist that live at 127.0.0.2, 127.0.0.3, and 127.0.0.4.
 * Send a sample DM to each of them that is not us.
 */
void sendPrototypeDMs(Client* client, const char* ip, int port)
{
  // Send 4 messages.
  int i = 0;
  while (i < 4)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    std::string dm = std::to_string(i);
    dm += " - Hello from ";
    dm += ip;
    dm += ":";
    dm += std::to_string(port);
    if (port != 8081)
    {
      client->sendDirectMessage("127.0.0.1", 8081, dm.c_str());
    }
    if (port != 8082)
    {
      client->sendDirectMessage("127.0.0.1", 8082, dm.c_str());
    }
    ++i;
  }
}

/**
 * Demonstrates the client teardown functionality. Closes 127.0.0.2 after
 * 10 seconds. Broadcasts change to other open clients.
 */
void teardownDemo(Client* client, const char* ip, int port)
{
  if (strcmp(ip, "127.0.0.1") == 0 && port == 8081)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    client->stop();
  }
}

/**
 * Opens a client connection to server, allowing for registration and direct
 * communication with the server's other clients.
 */
int main(int argc, char** argv)
{
  Parser p;
  char* address = p.parseForFlagString("-ip", argc, argv);
  if (!address) {
    printf("Usage: ./client -ip <server ip>");
    return 1;
  }
  char** tokens = str_split(address, ':');
  char* ip = tokens[0];
  int port = std::stoi(tokens[1]);
  
  Client client("127.0.0.1", 8080, ip, port);
  client.start();

  std::thread t1(listenToServer, &client);
  std::thread t2(sendPrototypeDMs, &client, ip, port);
  std::thread t3(teardownDemo, &client, ip, port);

  t1.join();
  t2.join();
  t3.join();
  return 0;
}