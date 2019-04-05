#!/usr/bin/env python
"""Reduce function for left joining tables on artist_id:
SELECT 	track.artist_id, max(track.year), avg(track.duration), count(artist_term.term)
FROM	track LEFT JOIN artist_term
ON		track.artist_id = artist_term.artist_id
GROUP BY track.artist_id
""""""
#artist_term: ARTIST_ID,term
#track: ARTIST_ID, TRACK_ID
"""

import sys

current_key = None

def res(key,years, durs,terms):
    count_term = len(terms)*len(years)
    if durs == []:
        avg_dur = 0
    else:
        avg_dur = sum(durs)/len(durs)
    if years ==[]:
        max_year = 0
    else:
        max_year = max(years)
    print('{}\t{}\t{}\t{}'.format(key, max_year, avg_dur, count_term))

terms = []
years = []
durs = []
keys = []
# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

    # Remove leading and trailing whitespace
    line = line.strip()

    # Get key/value and split by tab
    key, value = line.split('\t', 1)

    # Parse key/value input (your code goes here)
    val = value.split(',')
    table = val[0]

    # If we are still on the same key...
    if key == current_key:
        if table =='T':
            year, dur = int(val[1]),float(val[2])
            years.append(year)
            durs.append(dur)
            if keys ==[]:
                keys.append(key)
        if table =="A":
            term = val[1]
            terms.append(term)

    # Otherwise, if this is a new key...
    else:
        # If this is a new key and not the first key we've seen
        if current_key:
            if keys !=[]:
                res(current_key,years,durs,terms)
            terms = []
            years = []
            durs = []
            keys = []

        if table =='T':
            year, dur = int(val[1]),float(val[2])
            years.append(year)
            durs.append(dur)
            if keys==[]:
                keys.append(key)
        if table =="A":
            term = val[1]
            terms.append(term)

        current_key = key

#Compute/output result for the last key

###
# (your code goes here)
if current_key:
    if keys !=[]:
        res(current_key, years, durs, terms)
