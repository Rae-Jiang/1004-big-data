#!/usr/bin/env python
#Reduce function for computing matrix multiply A*B

import sys

# Create data structures to hold the current row/column values

###
# (if needed; your code goes here)
###
map_A = dict()
map_B = dict()

def calculate_matrix(map_A,map_B,key):
    res = 0
    for k in map_A.keys():
        if k in map_B:
            res += map_A[k] * map_B[k]
    key1,key2 = key.split(',')
    key1 = str(int(key1) - 1)
    key2 = str(int(key2) - 1)
    key = key1+','+key2
    print('{}\t{}'.format(key, res))

current_key = None

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

    # Remove leading and trailing whitespace
    line = line.strip()

    # Get key/value and split by tab
    key, value = line.split('\t', 1)

    # Parse key/value input (your code goes here)
    matrix, pos, val = value.split(',')


    # If we are still on the same key...
    if key == current_key:

        # Process key/value pair

        ###
        # (your code goes here)
        if matrix == 'A':
            map_A[pos] = float(val)
        else:
            map_B[pos] = float(val)
        ###

        pass

    # Otherwise, if this is a new key...
    else:
        # If this is a new key and not the first key we've seen
        if current_key:

            #compute/output result to STDOUT

            ###
            # (your code goes here)
            calculate_matrix(map_A,map_B,current_key) #calculate entry for this key in C
            map_A = dict() #clear map_A for the next key
            map_B = dict()
            ###

            pass

        current_key = key

        # Process input for new key

        ###
        # (your code goes here)
        if matrix == 'A':
            map_A[pos] = float(val)
        else:
            map_B[pos] = float(val)
        ###

        pass



#Compute/output result for the last key

###
# (your code goes here)
if current_key:
    calculate_matrix(map_A,map_B,current_key)
###
