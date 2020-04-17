/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include <thread>
#include <chrono>
#include <vector>
#include <string>
#include "util/network.h"
#include "util/message.h"
#include "util/helper.h"

/**
 * A network client wrapper over C POSIX functions. This client is able to
 * connect to a server and register. Once registered, it is able to communicate
 * with other connected clients. It also receives the server's other clients
 * through regularly broadcasted messages.
 */
class Client
{
public:
  int _sock = -1;                       // The socket connected to by this client.
  std::vector<std::string> _client_adr; // addresses of connected clients as "IP:PORT"
  std::string _serverIp;                // the rendezvous server ip
  int _serverPort;                      // the rendezvous client ip
  std::string _ip;                      // this client's ip
  int _port;                            // this client's port
  Network _n;                           // contains network helper functions
  bool _teardown = false;               // if true, tear this client down

public:
  /**
   * Default constructor.
   */
  Client() = default;

  /**
   * Initializaes this client by connecting to the specified ip at the
   * specified port. Uses assert to check successful initialization. Sets
   * this client's _sock to the _sock obtained by this constructor.
   */
  Client(std::string serverIp, int serverPort, std::string ip, int port)
  {
    _ip = ip;
    _port = port;
    _serverIp = serverIp;
    _serverPort = serverPort;
    _sock = _n.connectToSocket(_serverIp, _serverPort);
  }

  /**
   * Default destructor. Stops the client.
   */
  ~Client()
  {
    stop();
    printf("Client %s:%d has exited.\n", _ip.c_str(), _port);
  }

  /**
   * Starts up this client. The user must still actively start listening.
   */
  void start()
  {
    _sendRegisterMsg();
  }

  /**
   * Shuts this client down. Closes server connection.
   */
  void stop()
  {
    _sendTeardownMsg();
    close(_sock);
  }

  /**
   * Sends a message to the server telling it that this client is about
   * to shut down. The server will close this client connection, and 
   * broadcast to the other clients that this client is closed.
   */
  int _sendTeardownMsg()
  {
    std::string teardown = TEARDOWN;
    teardown.append("\n");
    teardown.append(_ip);
    teardown.append(":");
    teardown.append(std::to_string(_port));
    teardown.append("\n");
    size_t length = teardown.length();
    _teardown = true;
    _n.sendMsg(_sock, teardown.c_str(), length);
    return 0;
  }

  /**
   * Sends a direct message to the client at the given ip and port. The message
   * is routed through the server that serves this client. Prints a message if
   * this fails - does not crash since the other client may be temporarily down.
   */
  void sendDirectMessage(std::string ip, int port, std::string msg)
  {
    std::string directMsg = DIRECTMSG;
    directMsg += "\n";
    directMsg += ip;
    directMsg += ":";
    directMsg += std::to_string(port);
    directMsg += "\n";
    directMsg += msg;
    directMsg += "\n";
    size_t length = directMsg.length();
    _n.sendMsg(_sock, directMsg.c_str(), length);
  }

  /**
   * Runs in its own thread, where it continously listens to the server to 
   * retrieve any and all broadcasts.
   */
  void listenToServer()
  {
    while (!_teardown)
    {
      size_t length = 0;
      _n.readForLength(_sock, &length, _teardown);
      if (length > 0)
      {
        char buffer[length];
        _n.readMsg(_sock, buffer, length, _teardown);
        _processMsg(buffer, length);
      }
    }
  }

  /**
   * On receipt of a broadcast of a client directory from the server, update
   * the client's list of other clients so it knows who is available for
   * communication.
   */
  void _processBroadcast(char **tokens)
  {
    // blow everything away
    _client_adr.clear();

    // re-init it all
    size_t numClients = std::stoi(tokens[1]);
    for (size_t i = 2; i < numClients + 2; ++i)
    {
      char **addr = str_split(tokens[i], ':');
      std::string ip = addr[0];
      int port = std::stoi(addr[1]);
      if (ip == _ip && port == _port)
      {
        continue;
      }
      std::string address = ip;
      address += ":";
      address += std::to_string(port);
      _client_adr.push_back(address);
    }
    printf("Available addresses:\n");
    for (size_t i = 0; i < _client_adr.size(); ++i)
    {
      printf("%s\n", _client_adr.at(i).c_str());
    }
  }

  /**
   * Sorts messages and sends them off to be processed based on message type.
   */
  void _processMsg(char *msg, size_t length)
  {
    char **tokens = str_split(msg, '\n');
    std::string msgType = tokens[0];
    if (msgType == DIRECTMSG)
    {
      printf("RECEIVED DM (CLIENT):\n%s\n", tokens[1]);
    }
    else if (msgType == BROADCAST)
    {
      _processBroadcast(tokens);
    }
    else if (msgType == SHUTDOWN)
    {
      _teardown = true;
      stop();
    }
    else
    {
      printf("Unknown message type received by client: %s\n", msgType.c_str());
    }
  }

  /**
   * Sends a message to the server requesting it to register this client in it.
   */
  void _sendRegisterMsg()
  {
    std::string registerMsg = REGISTER;
    registerMsg += "\n";
    registerMsg += _ip;
    registerMsg += ":";
    registerMsg += std::to_string(_port);
    registerMsg += "\n";
    size_t length = registerMsg.length();
    _n.sendMsg(_sock, registerMsg.c_str(), length);
  }
};