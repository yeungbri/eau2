/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// Lang:: Cpp

#pragma once
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <cassert>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/poll.h>

/**
 * A network utility class that servers and clients can use to send and receive
 * messages over TCP.
 */
class Network
{
public:
  /**
   * Sends the given message with the given length to the server of this client.
   */
  int sendMsg(int sock, const char* msg, size_t length)
  {
    int sendResult = send(sock, &length, sizeof(length), 0);
    if (sendResult < 0)
    {
      printf("Send Length of Message Failed: %s\n", strerror(errno));
      assert(false);
    }
    sendResult = send(sock, msg, length, 0);
    if (sendResult < 0)
    {
      printf("Send Message Failed: %s\n", strerror(errno));
      assert(false);
    }
    return sendResult;
  }

  /**
   * Reads the message at this socket to the provided buffer. Continues to
   * read until we have read the whole length.
   */
  int readMsg(int sock, char* buf, size_t len, bool& teardown)
  {
    int bytesRead = 0;
    int result = 0;
    while (bytesRead < len)
    {
      result = read(sock, buf + bytesRead, len - bytesRead);
      if (teardown)
      {
        return 0;
      }
      // EBADF means that the socket has closed... client probably closed conn.
      // Not a case to fail, since clients can close anytime.
      if (result < 0 && errno != EBADF)
      {
        printf("Error in receiving registrations: %s", strerror(errno));
        assert(false);
      } else
      {
        bytesRead += result;
      }
    }
    return result;
  }

  /**
   * Reads the server socket until it gets the whole length of the incoming message
   */
  void readForLength(int sock, size_t* length, bool& teardown)
  {
    size_t toRead = sizeof(size_t);
    int bytesRead = 0;
    int result = 0;
    while (bytesRead < toRead)
    {
      result = readMsg(sock, (char*)length, toRead - bytesRead, teardown);
      if (teardown)
      {
        return;
      }
      // EBADF means that the socket has closed... client probably closed conn.
      // Not a case to fail, since clients can close anytime.
      if (result < 0 && errno != EBADF)
      {
        printf("Error in receiving registrations: %s\n", strerror(errno));
        assert(false);
      } else
      {
        bytesRead += result;
      }
    }
  }

  /**
   * Connects to a socket and returns the socket.
   */
  int connectToSocket(const char* ip, int port)
  {
    struct sockaddr_in server;
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(socket_fd >= 0);
    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    assert(inet_pton(AF_INET, ip, &server.sin_addr) > 0);
    if (connect(socket_fd, (struct sockaddr *)&server, sizeof(server)) < 0)
    {
      printf("Connect failed for socket %d, error: %s\n", socket_fd, strerror(errno));
      assert(false);
    }
    return socket_fd;
  }

  /**
   * Binds to a socket and returns the socket bound.
   */
  int bindToSocket(const char* ip, int port)
  {
    struct sockaddr_in adr;
    int opt = 1;
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(server_fd >= 0);
    assert(setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)) != 0);
    adr.sin_family = AF_INET;
    assert(inet_pton(AF_INET, ip, &adr.sin_addr) > 0);
    adr.sin_port = htons(port);
    if (bind(server_fd, (struct sockaddr *)&adr, sizeof(adr)) < 0)
    {
      printf("Bind failed, error: %s\n", strerror(errno));
      assert(false);
    }
    return server_fd;
  }
};