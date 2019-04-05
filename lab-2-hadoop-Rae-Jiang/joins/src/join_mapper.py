"""
you are given two data files in comma-separated value (CSV) format. These data files (joins/music_small/artist_term.csv and joins/music_small/track.csv) contain the same music data from the previous lab assignment on SQL and relational databases. Specifically, the file artist_term.csv contains data of the form
    ARTIST_ID,tag string
and track.csv contains data of the form
    TRACK_ID,title string,album string,year,duration,ARTIST_ID

SELECT 	artist_id, track_id, term
FROM	track INNER JOIN artist_term
ON 		track.artist_id = artist_term.artist_id
"""

import os
import sys

# are we reading from artist_term.csv or track.csv?
READING_A = False
READING_T = False

# Hadoop may break each input file into several small chunks for processing
# and the streaming mode only shows us one row (line of text) at a time.
#
# If we want to know what file the input data is coming from, this is
# stored in the environment variable `mapreduce_map_input_file`:
if 'artist_term' in os.environ['mapreduce_map_input_file']:
    READING_A = True
elif 'track' in os.environ['mapreduce_map_input_file']:
    READING_T = True
else:
    raise RuntimeError('Could not determine input file!')

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

    # Remove leading and trailing whitespace
    line = line.strip()

    # If this is data in artist_term.csv...
    if READING_A:

        # Split line into values for each attribute
        ARTIST_ID,tag_string = line.split(",")

        # Generate the necessary key-value pairs
        key = ARTIST_ID
        val = 'A' +','+ tag_string

        print('{}\t{}'.format(key, val))

    # If this is data in track.csv...
    if READING_T:
        # Split line into values for each attribute
        TRACK_ID,title_string,album_string,year,duration,ARTIST_ID = line.split(",")

        # Generate the necessary key-value pairs
        key = ARTIST_ID
        val = 'T' + ','+ TRACK_ID

        print('{}\t{}'.format(key, val))
