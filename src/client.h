/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// Lang::Cpp (<thread>, <chrono> libraries)

#pragma once
#include "util/network.h"
#include "util/string.h"
#include "util/string_array.h"
#include "util/int_array.h"
#include "util/message.h"
#include <thread>
#include <chrono>

/**
 * A network client wrapper over C POSIX functions. This client is able to
 * connect to a server and register. Once registered, it is able to communicate
 * with other connected clients. It also receives the server's other clients
 * through regularly broadcasted messages.
 */
class Client
{
public:
  int _sock = -1;                      // The socket connected to by this client.
  StringArray _client_adr;             // addresses of connected clients as "IP:PORT"
  const char* _serverIp;               // the rendezvous server ip
  int _serverPort;                     // the rendezvous client ip
  const char* _ip;                     // this client's ip
  int _port;                           // this client's port
  Network _n;                          // contains network helper functions
  bool _teardown = false;              // if true, tear this client down

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
  Client(const char* serverIp, int serverPort, const char* ip, int port)
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
    printf("Client exited.\n");
  }

  /**
   * Starts up this client. The user must still actively start listening.
   */
  void start()
  {
    assert(_sendRegisterMsg() > 0);
  }

  /**
   * Shuts this client down. Closes server connection.
   */
  void stop()
  {
    _sendTeardownMsg();
    assert(close(_sock) == 0);
  }

  /**
   * Sends a message to the server telling it that this client is about
   * to shut down. The server will close this client connection, and 
   * broadcast to the other clients that this client is closed.
   */
  int _sendTeardownMsg()
  {
    StrBuff buff;
    buff = buff.c(TEARDOWN);
    buff = buff.c("\n");
    buff = buff.c(_ip);
    buff = buff.c(":");
    buff = buff.c(_port);
    buff = buff.c("\n");
    String* tearDown = buff.get();
    size_t length = tearDown->size();
    _teardown = true;
    assert(_n.sendMsg(_sock, tearDown->c_str(), length) >= 0);
    printf("Client %s:%d has shut down.\n", _ip, _port);
    return 0;
  }

  /**
   * Sends a direct message to the client at the given ip and port. The message
   * is routed through the server that serves this client. Prints a message if
   * this fails - does not crash since the other client may be temporarily down.
   */
  int sendDirectMessage(const char* ip, int port, const char* msg)
  {
    StrBuff buff;
    buff = buff.c(DIRECTMSG);
    buff = buff.c("\n");
    buff = buff.c(ip);
    buff = buff.c(":");
    buff = buff.c(port);
    buff = buff.c("\n");
    buff = buff.c(msg);
    buff = buff.c("\n");
    String* directMsg = buff.get();
    size_t length = directMsg->size();
    return _n.sendMsg(_sock, directMsg->c_str(), length);
  }

  /**
   * Runs in its own thread, where it continously listens to the server to 
   * retrieve any and all broadcasts.
   */
  void listenToServer()
  {
    while(!_teardown)
    {
      size_t length = 0;
      _n.readForLength(_sock, &length, _teardown);
      if (length > 0) {
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
  void _processBroadcast(char** tokens)
  {
    // blow everything away
    _client_adr.clear();

    // re-init it all
    size_t numClients = portToInt(tokens[1]);
    for (size_t i = 2; i < numClients + 2; ++i)
    {
      char** addr = str_split(tokens[i], ':');
      char* ip = addr[0];
      int port = portToInt(addr[1]);
      if (strcmp(ip, _ip) == 0 && port == _port)
      {
        continue;
      }
      StrBuff addrBuff;
      addrBuff = addrBuff.c(ip);
      addrBuff = addrBuff.c(":");
      addrBuff = addrBuff.c(port);
      _client_adr.push_back(addrBuff.get());

    }
    printf("Available addresses:\n");
    for (size_t i = 0; i < _client_adr.length(); ++i)
    {
      printf("%s\n", _client_adr.get(i)->c_str());
    }
  }

  /**
   * Sorts messages and sends them off to be processed based on message type.
   */
  void _processMsg(char* msg, size_t length)
  {
    char** tokens = str_split(msg, '\n');
    const char* msgType = tokens[0];
    if (strcmp(msgType, DIRECTMSG) == 0)
    {
      printf("RECEIVED DM (CLIENT):\n%s\n", tokens[1]);
    } else if (strcmp(msgType, BROADCAST) == 0)
    {
      _processBroadcast(tokens);
    } else if (strcmp(msgType, SHUTDOWN) == 0)
    {
      _teardown = true;
      stop();
    }
    else
    {
      printf("Unknown message type received by client: %s\n", msgType);
    }
  }

  /**
   * Sends a message to the server requesting it to register this client in it.
   */
  int _sendRegisterMsg()
  {
    StrBuff buff;
    buff = buff.c(REGISTER);
    buff = buff.c("\n");
    buff = buff.c(_ip);
    buff = buff.c(":");
    buff = buff.c(_port);
    buff = buff.c("\n");
    String* registerMsg = buff.get();
    size_t length = registerMsg->size();
    return _n.sendMsg(_sock, registerMsg->c_str(), length);
  }
};