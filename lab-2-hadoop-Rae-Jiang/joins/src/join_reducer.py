#!/usr/bin/env python
#Reduce function for joining tables on artist_id:
#artist_term: ARTIST_ID,term
#track: ARTIST_ID, TRACK_ID


import sys


###
# (if needed; your code goes here)
###


current_key = None
def res(key,terms, tracks):
    for track in tracks:
        for term in terms:
            print('{}\t{}\t{}'.format(key, track, term))

terms = []
tracks = []
# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

    # Remove leading and trailing whitespace
    line = line.strip()

    # Get key/value and split by tab
    key, value = line.split('\t', 1)

    # Parse key/value input (your code goes here)
    table,val = value.split(',')

    # If we are still on the same key...
    if key == current_key:
        if table=='T':
            track = val
            tracks.append(track)
        if table =="A":
            term = val
            terms.append(term)

    # Otherwise, if this is a new key...
    else:
        # If this is a new key and not the first key we've seen

        if current_key:
            res(current_key, terms, tracks)
            terms = []
            tracks = []

        if table=='T':
            track = val
            tracks.append(track)
        if table =="A":
            term = val
            terms.append(term)

        current_key = key




#Compute/output result for the last key

###
# (your code goes here)
if current_key:
    res(current_key,terms, tracks)
