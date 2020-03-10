/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// Lang::CwC (<thread>, <chrono> libraries)

#include "util/network.h"
#include "util/message.h"
#include "util/string_array.h"
#include "util/int_array.h"
#include <thread>
#include <chrono>

/**
 * A server wrapper around C POSIX TCP functions. This server is able to
 * receive incoming client connections and provide directed communcations
 * between each of its clients.
 */
class Server
{
public:
  IntArray _clients;        // sockets of connected clients
  StringArray _client_adr;  // addresses of connected clients as "IP:PORT"
  int _server;              // this own server's socket
  struct pollfd _fds[1024]; // file descriptors. supports 1024 clients connecting to this server
  int _nfds = 0;            // number of file descriptors (clients)
  Network _n;               // contains network helper functions    
  bool _teardown = false;   // if true, tear this server down

public:
  /**
   * Default constructor.
   */
  Server() = default;

  /**
   * Initializes this server's socket given an ip and port.
   */
  Server(const char *ip, int port)
  {
    _server = _n.bindToSocket(ip ,port);
  }

  /**
   * Closes all open sockets.
   */
  ~Server()
  {
    stop();
    printf("Server exited.\n");
  }

  /**
   * Tears the server down by closing all connections and telling the clients
   * that it has shut down. This will trigger clients to all close as well.
   */
  void stop()
  {
    if (!_teardown)
    {
      _teardown = true;
      _client_adr.clear();
      _broadcastShutdown();
      for (size_t i = 0; i < _clients.length(); ++i)
      {
        close(_clients.get(i));
      }
      close(_server);
    }
  }
  
  /**
   * Given that tokens is a tokenized REGISTER message, process it and broadcast
   */
  void _processRegistration(char **tokens, int client)
  {
    StrBuff buff;
    buff = buff.c(tokens[1]);
    _client_adr.push_back(buff.get());
    _fds[_client_adr.length() - 1].fd = client;
    _fds[_client_adr.length() - 1].events = POLLIN;
    ++_nfds;
    _broadcastToClients();
  }

  /**
   * This method executes on its own thread and is responsibly for listening
   * for incoming client connections. Once it receives a connection, it notes
   * the associated socket, ip, and port of the client.
   */
  void listenForRegistrations()
  {
    while (!_teardown)
    {
      struct sockaddr_in adr;
      size_t adr_len = sizeof(adr);
      assert(listen(_server, 3) >= 0);
      int client = accept(_server, (struct sockaddr *)&adr, (socklen_t *)&adr_len);
      if (errno == ECONNABORTED)
      {
        return;
      }
      if (client < 0)
      {
        printf("Failed to accept client: %d\n", errno);
        assert(false);
      }
      else
      {
        _clients.push_back(client);
        size_t length = 0;
        _n.readForLength(client, &length, _teardown);
        char buffer[length];
        _n.readMsg(client, buffer, length, _teardown);
        // assumes client will correctly send registration message as "<ACTION>:IP:PORT"
        char **tokens = str_split(buffer, '\n'); // tokens[0] = <ACTION>, tokens[1] = IP, TOKENS[2] = PORT
        if (strcmp(tokens[0], REGISTER) == 0)
        {
          _processRegistration(tokens, client);
        }
      }
    }
  }

  /**
   * This method executes on its own thread and is responsible for
   * listening for direct messages from client->client and delivers them.
   */
  void listenForMessages()
  {
    int timeout = 5 * 1000; // 5 seconds
    while (!_teardown)
    {
      int ret = poll(_fds, _nfds, timeout);
      assert(ret >= 0);
      for (int i = 0; i < _nfds; i++)
      {
        if (_fds[i].revents == POLLIN)
        {
          size_t length = 0;
          _n.readForLength(_fds[i].fd, &length, _teardown);
          char buffer[length];
          _n.readMsg(_fds[i].fd, buffer, length, _teardown);
          char **tokens = str_split(buffer, '\n');
          if (strcmp(tokens[0], DIRECTMSG) == 0)
          {
            _processDM(tokens);
          }
          else if (strcmp(tokens[0], TEARDOWN) == 0)
          {
            _processTeardown(tokens);
          }
        }
      }
    }
  }

  /**
   * This method is responsible for broadcasting this server's list of 
   * connected clients' IPs and ports to their respective sockets.
   */
  void _broadcastToClients()
  {
    StrBuff buff;
    size_t numClients = _client_adr.length();
    buff = buff.c(BROADCAST);
    buff = buff.c("\n");
    buff = buff.c(numClients);
    buff = buff.c("\n");
    for (size_t j = 0; j < numClients; ++j)
    {
      buff = buff.c(_client_adr.get(j)->c_str());
      buff = buff.c("\n");
    }
    const char *msg = buff.get()->c_str();

    for (size_t i = 0; i < numClients; ++i)
    {
      printf("SENDING FROM SERVER:\n%s\n", msg);
      _n.sendMsg(_clients.get(i), msg, strlen(msg));
    }
  }

  /**
   * Broadcasts a shutdown message to every client, letting each know that the 
   * server is shutting down and that they should shut down too.
   */
  void _broadcastShutdown()
  {
    StrBuff buff;
    buff = buff.c(SHUTDOWN);
    buff = buff.c("\n");
    const char* msg = buff.get()->c_str();
    for (size_t i = 0; i < _clients.length(); ++i)
    {
      printf("SENDING FROM SERVER:\n%s\n", msg);
      _n.sendMsg(_clients.get(i), msg, strlen(msg));
    }
  }

  /**
   * Given a Direct Message from a client, parse it, and deliver it to 
   * the correct recipient.
   */
  void _processDM(char **tokens)
  {
    StrBuff buff;
    buff = buff.c(tokens[1]);
    String *address = buff.get();
    int idx = _client_adr.index_of(address);
    if (idx >= 0)
    {
      int sock = _clients.get(idx);
      StrBuff msg;
      msg = msg.c(tokens[0]);
      msg = msg.c("\n");
      msg = msg.c(tokens[2]);
      const char *payload = msg.get()->c_str();
      _n.sendMsg(sock, payload, strlen(payload) + 1); // +1 to account for null
    }
    else
    {
      printf("Server can't send to requested address because it does not exist.\n");
    }
  }

  /**
   * The server has received notice that some client has removed itself
   * from the registry. Close the connection, and re-broadcast the change
   * to all open clients.
   */
  void _processTeardown(char **tokens)
  {
    StrBuff buff;
    buff = buff.c(tokens[1]);
    String *address = buff.get();
    int idx = _client_adr.index_of(address);
    if (idx >= 0)
    {
      int sock = _clients.get(idx);
      assert(close(sock) == 0);
      _clients.remove(idx);
      _client_adr.remove(idx);
      _broadcastToClients();
    }
    else
    {
      printf("Server can't close requested address because it does not exist.\n");
    }
  }
};