/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// lang::Cpp

#include <thread>
#include <chrono>
#include <vector>
#include <string>
#include "util/network.h"
#include "util/message.h"
#include "util/helper.h"

/**
 * A server wrapper around C POSIX TCP functions. This server is able to
 * receive incoming client connections and provide directed communcations
 * between each of its clients.
 */
class Server
{
private:
  std::vector<int> _clients;            // sockets of connected clients
  std::vector<std::string> _client_adr; // addresses of connected clients as "IP:PORT"
  int _server;                          // this own server's socket
  struct pollfd _fds[1024];             // file descriptors. supports 1024 clients connecting to this server
  int _nfds = 0;                        // number of file descriptors (clients)
  Network _n;                           // contains network helper functions
  bool _teardown = false;               // if true, tear this server down

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
    _server = _n.bindToSocket(ip, port);
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
      for (size_t i = 0; i < _clients.size(); ++i)
      {
        close(_clients.at(i));
      }
      close(_server);
    }
  }

  /**
   * Given that tokens is a tokenized REGISTER message, process it and broadcast
   */
  void _processRegistration(char **tokens, int client)
  {
    _client_adr.push_back(std::string(tokens[1]));
    _fds[_client_adr.size() - 1].fd = client;
    _fds[_client_adr.size() - 1].events = POLLIN;
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
        if (REGISTER == tokens[0])
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
          if (DIRECTMSG == tokens[0])
          {
            _processDM(tokens);
          }
          else if (TEARDOWN == tokens[0])
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
    size_t numClients = _client_adr.size();
    std::string broadcast = BROADCAST;
    broadcast += "\n";
    broadcast += std::to_string(numClients);
    broadcast += "\n";
    for (size_t j = 0; j < numClients; ++j)
    {
      broadcast += _client_adr.at(j);
      broadcast += "\n";
    }

    for (size_t i = 0; i < numClients; ++i)
    {
      printf("SENDING FROM SERVER:\n%s\n", broadcast.c_str());
      _n.sendMsg(_clients.at(i), broadcast.c_str(), broadcast.length());
    }
  }

  /**
   * Broadcasts a shutdown message to every client, letting each know that the 
   * server is shutting down and that they should shut down too.
   */
  void _broadcastShutdown()
  {
    std::string shutdown = SHUTDOWN;
    shutdown += "\n";
    for (size_t i = 0; i < _clients.size(); ++i)
    {
      printf("SENDING FROM SERVER:\n%s\n", shutdown.c_str());
      _n.sendMsg(_clients.at(i), shutdown.c_str(), shutdown.length());
    }
  }

  /**
   * Given a Direct Message from a client, parse it, and deliver it to 
   * the correct recipient.
   */
  void _processDM(char **tokens)
  {
    std::string address = tokens[1];
    auto itr = std::find(_client_adr.begin(), _client_adr.end(), address);
    if (itr != _client_adr.cend())
    {
      int idx = std::distance(_client_adr.begin(), itr);
      int sock = _clients.at(idx);
      std::string msg = tokens[0];
      msg += "\n";
      msg += tokens[2];
      _n.sendMsg(sock, msg.c_str(), msg.length() + 1); // +1 to account for null
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
    std::string address = tokens[1];
    auto itr = std::find(_client_adr.begin(), _client_adr.end(), address);
    if (itr != _client_adr.cend())
    {
      int idx = std::distance(_client_adr.begin(), itr);
      int sock = _clients.at(idx);
      assert(close(sock) == 0);
      _clients.erase(_clients.begin() + idx);
      _client_adr.erase(itr);
      _broadcastToClients();
    }
    else
    {
      printf("Server can't close requested address because it does not exist.\n");
    }
  }
};