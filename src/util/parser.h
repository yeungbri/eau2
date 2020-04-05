/**
 * Authors: gao.d@husky.neu.edu and yeung.bri@husky.neu.edu
 */

// Lang::CwC

#include <cstring>
#include <cstdlib>
#include <cctype>

/**
 * This class is a helper for parsing command line arguments.
 */
class Parser
{
public:
  /**
   * Checks if this string is convertible to an int.
   *
   * @param str  the string we are checking
   * @return true if the number is all digits, and false if the number is a double or string
   */
  bool isInt(char *str)
  {
    for (int i = 0; i < strlen(str); ++i)
    {
      if (!isdigit(str[i]))
      {
        return false;
      }
    }
    return true;
  }

  /**
   * Given an input flag and the entire command line input, this method parses 
   * the input for the flag, and retrieves the value if present. If present,
   * the value is type-checked to be an int. On error, it will return -1.
   *
   * @param flag   some command line option, such as -l, -f, etc.
   * @param argc   the number of arguments in input
   * @param input  the entire input from the command line
   *
   * @return the value of the specified flag. If DNE, returns empty string. If
   *         it is of the wrong type, returns -1.
   */
  int parseForFlagInt(const char *flag, int argc, char **input)
  {
    int ret = 0;
    bool found = false;
    for (int i = 0; i < argc; ++i)
    {
      if (strcmp(input[i], flag) == 0)
      {
        if (!found && i < argc - 1 && isInt(input[i + 1]))
        {
          ret = atoi(input[i + 1]);
          found = true;
        }
        else
        {
          // malformed!
          ret = -1;
          break;
        }
      }
    }
    return ret;
  }

  /**
   * Given an input flag and the entire command line input, this method parses
   * the input for the flag, and retrieves the value if present. If present,
   * the value is type-checked to be a string. On error, it will return null.
   *
   * @param flag  some command line option, such as -l, -f, etc.
   * @param argc  the number of arguments in input
   * @param input the entire input from the command line
   *
   * @return the value of the specified flag. If DNE, returns empty string. If
   *         it is of the wrong type, returns NULL.
   */
  char *parseForFlagString(const char *flag, int argc, char **input)
  {
    char *ret = strdup("");
    bool found = false;
    for (int i = 0; i < argc; ++i)
    {
      if (strcmp(input[i], flag) == 0)
      {
        if (!found && i < argc - 1)
        {
          ret = input[i + 1];
          found = true;
        }
        else
        {
          // malformed!
          ret = NULL;
          break;
        }
      }
    }
    return ret;
  }

  /**
   * Checks if the input has an optional string argument at the end and returns
   * it if it exists. Returns null if not.
   *
   * @param argc  the number of arguemnts in input
   * @param input the entire input from the command line
   *
   * @return the optional ending string argument if provided, null if not
   */
  char *parseForOptionalStr(int argc, char **input)
  {
    char *ret = strdup("");
    bool found = false;
    bool precededByFlag = false;
    for (int i = 1; i < argc; ++i)
    {
      char *curr = input[i];
      if (strcmp(curr, "-f") == 0 || strcmp(curr, "-i") == 0)
      {
        precededByFlag = true;
      }
      else
      {
        if (!precededByFlag && !found && i == argc - 1)
        {
          ret = curr;
          found = true;
        }
        else if (!precededByFlag)
        {
          ret = NULL;
          break;
        }
        else if (precededByFlag)
        {
          precededByFlag = false;
        }
      }
    }
    return ret;
  }
};