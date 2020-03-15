/*
Authors: Zhichao Chen and Bryce Russell-Benoit
*/

#include <iostream>
#include <string>
#include <fstream>
#include <algorithm> 
#include <vector>
#include <stdexcept>
#include "../dataframe.h"

using namespace std;

// credit to https://stackoverflow.com/questions/865668/how-to-parse-command-line-arguments-in-c
// find out the argument of given option
char* getCmdOption(char** begin, char** end, const string& option)
{
    char** itr = find(begin, end, option);

    if (itr != end && ++itr != end)
    {
        return *itr;
    }

    cout << "Insufficient number of arguments provided for flag " << option << "\n";
    exit(1);
}

// find out the arguments of given option and modify given value1 and value2
void getCmdOptions(char** begin, char** end, const string& option, size_t& value1, size_t& value2)
{
    char** itr = find(begin, end, option);
    if (itr != end && ++itr != end)
    {
        value1 = atol(*itr);

        if (++itr != end) {
            value2 = atol(*itr);
            return;
        }
    }
    cout << "Insufficient number of arguments provided for flag " << option << "\n";
    exit(1);
}

// determines if the option exist
bool cmdOptionExists(char** begin, char** end, const string& option)
{
    return find(begin, end, option) != end;
}

// count number of elements in the given line
int count_element(string line) {

    int count = 0;
    int leftBrackets = 0;

    for (int i = 0; i < line.length(); i++) {
        if (line.at(i) == '<') {
            leftBrackets += 1;
        }
        else if (line.at(i) == '>') {
            // increase count only when the outermost < is closed
            if (leftBrackets == 1) {
                count += 1;
            }
            leftBrackets -= 1;
        }
    }

    return count;
}

// find the type of the given input
// if BOOL or empty, return 1
// if INT, return 2
// if FLOAT, return 3
// if STRING, return 4
int compute_type(string input) {

    if (input.length() == 0 || input == "1" || input == "0") {
        return 1;
    }

    bool hasDecimalPoint = false;

    char firstChar = input.at(0);
    int index = firstChar == '+' || firstChar == '-' ? 1 : 0;
    while (index < input.length()) {
        char c = input.at(index);
        if (c == '.') {
            if (!hasDecimalPoint) {
                hasDecimalPoint = true;
            }
            else {
                // so that 1.2.3 is a string
                return 4;
            }
        }
        else if (!isdigit(c)) {
            // if not a digit, must be a string
            return 4;
        }
        index++;
    }

    if (!hasDecimalPoint) {
        return 2;
    }
    return 3;
}

// for a given line, replace invalid value with "" and return
// a list of values (don't care types)
vector<string> parse_line(string line) {
    vector<string> wordList;

    int leftDelimiters = 0;
    int leftDelimiterIndex = 0;
    int rightDelimiterIndex = 0;
    int start = 0;
    int end = 0;

    int quotation = 0;
    int spaceIndex = -1;

    for (int i = 0; i < line.length(); i++) {
        char character = line.at(i);
        if (character == '<') {
            // first time sees <, set index to outermost <
            if (leftDelimiterIndex <= rightDelimiterIndex) {
                leftDelimiterIndex = i;
            }
            leftDelimiters += 1;
        }
        else if (character == '>') {
            // if not outermost >, don't parse the value
            if (leftDelimiters > 1) {
                leftDelimiters -= 1;
                continue;
            }

            rightDelimiterIndex = i;

            // if no content in <>, push ""
            if (!(start > leftDelimiterIndex)) {
                wordList.push_back("");
            }
            else if (spaceIndex > 0) {
                // if <> has space in between characters and no "", push ""
                if (quotation != 2 && end > spaceIndex) {
                    wordList.push_back("");
                }
                else {
                    wordList.push_back(line.substr(start, end - start + 1));
                }
                spaceIndex = -1;
                quotation = 0;
            }
            else {
                wordList.push_back(line.substr(start, end - start + 1));
            }
            leftDelimiters = 0;
        }
        else if (character == '\"') {
            quotation += 1;
        }
        else if (character != ' ') {
            // if first character, set start to i
            if (!(start > leftDelimiterIndex)) {
                start = i;
            }
            end = i;
        }
        else {
            // if space in between characters, set first occurrence of space to i
            if (start > leftDelimiterIndex && rightDelimiterIndex < leftDelimiterIndex && spaceIndex < 0) {
                spaceIndex = i;
            }
        }
    }

    if (leftDelimiters != 0) {
        cout << "Given line is invalid " << line << endl;
        exit(1);
    }

    return wordList;
}

// return an int array of element types in the given line
vector<int> parse_type(string line) {
    // store types for given line
    vector<int> types;
    vector<string> wordList = parse_line(line);
    for (int i = 0; i < wordList.size(); i++) {
        types.push_back(compute_type(wordList[i]));
    }
    return types;
}

// return a list of values of the given line, if the value does
// not match its type, return ""
vector<string> parse_line(string input, vector<int>* types, int columns) {
    
    vector<string> wordList = parse_line(input);

    // if a line has more than columns 
    // (may be the case when this line is not in first 500 lines)
    if (wordList.size() > columns) {
        wordList.erase(wordList.begin() + columns, wordList.end());
    }

    for (int i = 0; i < wordList.size(); i++) {
        int type = compute_type(wordList[i]);
        // invalid type (i.e. a string value for desired type int)
        if (type > types->at(i)) {
            wordList[i] = "";
        }
        else if (type <= 3 && type > 1 && wordList[i].at(0) == '+' && types->at(i) != 4) {
            // erase + if it is float or int
            wordList[i].erase(0, 1);
        }
    }

    // fill up the vectors with given number of columns
    while (wordList.size() < columns) {
        wordList.push_back("");
    }
    return wordList;
}

// print the string format of given type number
void print_type(int type) {
    switch (type) {
    case 1:
        cout << "BOOL\n";
        break;
    case 2:
        cout << "INT\n";
        break;
    case 3:
        cout << "FLOAT\n";
        break;
    case 4:
        cout << "STRING\n";
        break;
    default:
        cout << "Undefined type\n";
        exit(1);
    }
}

DataFrame* getDataFrame(std::string filePath)
{
    // default from flag to 0 and len flag to max
    size_t from = 0;

    // set default len to max - from (avoid overflow) and read entire file
    size_t len = SIZE_MAX;

    // open file
    ifstream file(filePath);

    int lineNumber = 0;
    int columns = 0;
    vector<int> types;
    string line;

    // read either EOF or first 500, which either comes first
    while ((lineNumber < 500) && getline(file, line, '\n')) {

         vector<int> currentTypes = parse_type(line);
         int currentColumns = currentTypes.size();

        // compares current row type information with the record,
        // take the most lenient one (or, max of their values)
        if (columns >= currentColumns) {
            for (int i = 0; i < currentColumns; i++) {
                types[i] = max(types[i], currentTypes[i]);
            }
        }
        else {
            for (int i = 0; i < columns; i++) {
                currentTypes[i] = max(types[i], currentTypes[i]);
            }
            types = currentTypes;
            columns = currentColumns;
        }
        lineNumber += 1;
    }

    // move the pointer to the from flag user specified
    file.clear();
    file.seekg(from, file.beg);

    // to avoid reading partial file, ignore the first row
    // if from is not 0
    if (from != 0) {
        getline(file, line, '\n');
    }

    // Construct Schema object from types
    Schema* schema = new Schema();
    for (int type : types)
    {
        switch(type)
        {
            case 1:
                schema->add_column('B', "");
                break;
            case 2:
                schema->add_column('I', "");
                break;
            case 3:
                schema->add_column('F', "");
                break;
            case 4:
                schema->add_column('S', "");
                break;
            default:
                throw std::runtime_error("Unknown type!");
        }
    }

    // store the content of the file
    DataFrame* df = new DataFrame(*schema);

    while (getline(file, line, '\n')) {
        // if exceed reading length, break
        if (file.tellg() > (len + from)) {
            break;
        }
        Row* row = new Row(*schema, "");
        std::vector<std::string> fields = parse_line(line, &types, columns);
        for (size_t i = 0; i < fields.size(); ++i)
        {
            std::string field = fields.at(i);
            int type = types.at(i);
            if (field != "")
            {
                switch(type)
                {
                    case 1:
                        row->set(i, field == "1");
                        break;
                    case 2:
                        row->set(i, std::stoi(field));
                        break;
                    case 3:
                        row->set(i, std::stof(field));
                        break;
                    case 4:
                        row->set(i, field);
                        break;
                    default:
                        throw std::runtime_error("Unknown type!");
                }
            }
        }
        df->add_row(*row);
    }

    // close file
    file.close();
    return df;
}

/*
// entrance of the program
int main(int argc, char** argv)
{
    // default from flag to 0 and len flag to max
    size_t from = 0;

    // check f flag, if not exist, terminate the program
    if (!cmdOptionExists(argv, argv + argc, "-f")) {
        cout << "Must specify the file" << endl;
        exit(1);
    }

    char* filePath = getCmdOption(argv, argv + argc, "-f");

    // check from flag
    if (cmdOptionExists(argv, argv + argc, "-from")) {
        from = atoi(getCmdOption(argv, argv + argc, "-from"));
    }

    // set default len to max - from (avoid overflow) and read entire file
    size_t len = SIZE_MAX - from;

    // check len flag
    if (cmdOptionExists(argv, argv + argc, "-len")) {
        len = atol(getCmdOption(argv, argv + argc, "-len"));
    }

    // open file
    ifstream file(filePath);

    int lineNumber = 0;
    int columns = 0;
    vector<int> types;
    string line;

    // read either EOF or first 500, which either comes first
    while ((lineNumber < 500) && getline(file, line, '\n')) {

         vector<int> currentTypes = parse_type(line);
         int currentColumns = currentTypes.size();

        // compares current row type information with the record,
        // take the most lenient one (or, max of their values)
        if (columns >= currentColumns) {
            for (int i = 0; i < currentColumns; i++) {
                types[i] = max(types[i], currentTypes[i]);
            }
        }
        else {
            for (int i = 0; i < columns; i++) {
                currentTypes[i] = max(types[i], currentTypes[i]);
            }
            types = currentTypes;
            columns = currentColumns;
        }
        lineNumber += 1;
    }

    // move the pointer to the from flag user specified
    file.clear();
    file.seekg(from, file.beg);

    // to avoid reading partial file, ignore the first row
    // if from is not 0
    if (from != 0) {
        getline(file, line, '\n');
    }

    // store the content of the file
    vector<vector<string>> matrix;

    while (getline(file, line, '\n')) {
        // if exceed reading length, break
        if (file.tellg() > (len + from)) {
            break;
        }

        matrix.push_back(parse_line(line, &types, columns));
    }

    // close file
    file.close();

    // check for print_col_type flag
    if (cmdOptionExists(argv, argv + argc, "-print_col_type")) {
        int column = atoi(getCmdOption(argv, argv + argc, "-print_col_type"));

        if (column >= columns) {
            cout << "Given column out of bound\n";
            exit(1);
        }

        print_type(types[column]);
    }

    // check for print_col_idx flag
    if (cmdOptionExists(argv, argv + argc, "-print_col_idx")) {
        size_t column, offset;
        getCmdOptions(argv, argv + argc, "-print_col_idx", column, offset);

        // check that the columns and offset are in bounds
        if (column >= columns || offset >= matrix.size()) {
            cout << "Given column and/or offset out of bound\n";
            exit(1);
        }
        if (types[column] == 4) {
            cout << "\"" << matrix[offset][column] << "\"" << endl;
        }
        else {
            cout << matrix[offset][column] << endl;
        }

    }

    // check for is_missing_idx flag
    if (cmdOptionExists(argv, argv + argc, "-is_missing_idx")) {
        size_t column, offset;
        getCmdOptions(argv, argv + argc, "-is_missing_idx", column, offset);

        // check that the columns and offset are in bounds
        if (column >= columns || offset >= matrix.size()) {
            cout << "Given column and/or offset out of bound\n";
            exit(1);
        }

        int value = 0;

        if (matrix[offset][column] == "") {
            value = 1;
        }

        cout << value << endl;
    }
    return 0;
}
*/