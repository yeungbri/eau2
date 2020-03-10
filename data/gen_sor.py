"""
Generate various SoR files to benchmark SoRer implementations

Authors: gao.d@husky.neu.edu, yeung.bri@husky.neu.edu
"""

import enum
import random
import string

# Represents the type of the column
class Type(enum.Enum):
    MISSING = -1
    BOOL = 0
    INT = 1
    FLOAT = 2
    STRING = 3

    def __str__(self):
        return self.name

# Generates a random value for the given type
# returns  a string representation of that value
def gen_field(t):
    result = "<"
    if (t == Type.BOOL):
        result += str(int(random.random()))
    elif (t == Type.INT):
        result += str(int(random.uniform(-1, 1) * 100))
    elif (t == Type.FLOAT):
        result += str('%.3f'%(random.uniform(-10, 10)))
    else:
        strLength = int(random.uniform(1, 8))
        # Attribution: https://pynative.com/python-generate-random-string/ at: 2/4/20 11:18AM
        letters = string.ascii_lowercase
        result += ''.join(random.choice(letters) for i in range(strLength))
    result += ">"
    return result

# Generates a valid row
def gen_row(schema):
    result = ""
    for i in range(len(schema)):
        result += gen_field(schema[i])
    result += "\n"
    return result

def write_valid_data(filename, rows, schema):
    with open(filename, 'w') as f:
        for i in range(rows):
            line = gen_row(schema)
            f.write(line)

def gen_sor():
    rows = 2000000
    schema = [Type.FLOAT, Type.STRING, Type.INT, Type.BOOL, Type.FLOAT, Type.STRING, Type.INT, Type.BOOL, Type.FLOAT, Type.STRING]
    write_valid_data('datafile.txt', rows, schema)

def gen_tiny():
    rows = 100
    schema = [Type.FLOAT, Type.STRING, Type.INT, Type.BOOL, Type.FLOAT, Type.STRING, Type.INT, Type.BOOL, Type.FLOAT, Type.STRING]
    write_valid_data('tiny.txt', rows, schema)

def gen_med():
    rows = 10000
    schema = [Type.FLOAT, Type.STRING, Type.INT, Type.BOOL, Type.FLOAT, Type.STRING, Type.INT, Type.BOOL, Type.FLOAT, Type.STRING]
    write_valid_data('med.txt', rows, schema)

if __name__ == '__main__':
    gen_med()