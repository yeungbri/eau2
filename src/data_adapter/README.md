# Assignment 1 Part 1
## Zhichao Chen and Bryce Russell-Benoit

The code for this part of the project has a very simple structure. We determined that we did not 
have any need for new classes as the data we would be working with was in easily-understood types
and formats. Therefore all the code for this part of the assignment can be found in main.cpp.

### **main**
Our program works by first parsing the command line to find the file, from and length flags.
We iterate through the first 500 lines by making our best guess of what data type we have at each
row with the given information (an integer type may start with 0's and 1's to look like a BOOl and
become an INT later on). Once we have determined the schema, we can pay attention to the command
line arguments that require an interpretation of the file like *is_missing_idx* or *print_col_idx*.

### **Helpers**
*getCmdOption* and *getCmdOption**s*** both work on parsing the command line instructions given.
The main difference between the two is that the latter works with commands that take in 2 arguments
like the *-is_missing_idx* whereas the singular method deals with the single-argument flags like 
-len.

*cmdOptionExists* is fairly self-explanatory. It checks if the given option flag can be found in
the given arguments.

*count_element* counts the number of elements that are on a given line.

*compute_type* determines what type of value is in the given string based on the criteria in the 
assignment. We ranked the types from basic to complex so that they could be moved up when we find
new information in the next row e.g. a value other than 1 or a 0 and a + or - make a BOOl and INT.
This method returns where on the scale the given value is. 
1 = BOOL, 2 = INT, 3 = FLOAT, 4 = STRING, and an empty string is -1.

*parse_type* returns an array of the types of the given row in order represented in the integer 
flags returned by *compute_type*.

*print_type* prints out what type the given integer flag represents.


