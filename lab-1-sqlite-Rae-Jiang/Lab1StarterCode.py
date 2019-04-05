#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# USAGE:
#   python Lab1.py Sample_Song_Dataset.db

import sys
import sqlite3


# The database file should be given as the first argument on the command line
db_file = sys.argv[1]

# We connect to the database using 
with sqlite3.connect('Sample_Song_Dataset.db') as conn:
    # We use a "cursor" to mark our place in the database.
    # We could use multiple cursors to keep track of multiple
    # queries simultaneously.
    cursor = conn.cursor()

    # This query counts the number of tracks from the year 1998
    year = ('1998',)
    cursor.execute('SELECT count(*) FROM tracks WHERE year=?', year)

    # Since there is no grouping here, the aggregation is over all rows
    # and there will only be one output row from the query, which we can
    # print as follows:
    print('Tracks from {}: {}'.format(year[0], cursor.fetchone()[0]))
    # The [0] bits here tell us to pull the first column out of the 'year' tuple
    # and query results, respectively.

    # ADD YOUR CODE STARTING HERE
    # 1.This query find id, name and term of the artist who played the track with id TRMMWLD128F9301BF2
    t = ('TRMMWLD128F9301BF2',)
    string = """
    SELECT
        artists.artist_id,
        artists.artist_name,
        artist_term.term
    FROM artists
    INNER JOIN tracks
    ON artists.artist_id = tracks.artist_id
    INNER JOIN artist_term
    ON tracks.artist_id = artist_term.artist_id
    WHERE tracks.track_id=?
    """
    cursor.execute(string, t)
    #because the condition is on the third table, and all three tables share the same artist_id, so we need to use INNER JOIN and then select attributes we want from the joined table based on the condtion on the third table

    for r in cursor:
        print(r)
    
    #2.This query Select all the unique tracks with the duration is strictly greater than 3020 seconds. We use distinct to select track_id in tracks
    t = (3020,)
    string = """
    SELECT 
        DISTINCT track_id,
        title,
        release,
        year,
        duration,
        artist_id
    FROM tracks
    WHERE duration > ?
    """
    cursor.execute(string, t)
    df = pd.DataFrame(cursor.fetchall())
    df.columns = ['track_id','title','release','year','duration','artist_id']
    print(df)
        
    #3.This query Find the ten shortest (by duration) 10 tracks released between 2010 and 2014 (inclusive), ordered by increasing duration.
    string = """
    SELECT *
    FROM tracks
    WHERE year>=2010 and year<=2014
    ORDER BY duration ASC
    LIMIT 10
    """
    cursor.execute(string)
    #find ten shortest so use ORDER BY and choose ascending, also limit to first ten tracks, then print the result:
    df = pd.DataFrame(cursor.fetchall())
    df.columns = ['track_id','title','release','year','duration','artist_id']
    print(df)
    #4.This query Find the top 20 most frequently used terms, ordered by decreasing usage.First group by term then count each term(group)'s usage. Finally get the count of each term in descending order.
    string = """
    SELECT 
        term
    FROM artist_term
    GROUP BY term
    ORDER BY Count(*) DESC
    LIMIT 20
    """
    cursor.execute(string)
    for r in cursor:
        print(r)
        
    #5.find the artist name associated with the longest track duration.
    #because name only appears in TABLE artists, and duration only appears in TABLE tracks. We should join the two table and use artist_id(which the two table share) to get the artist with the longest duration.
    #also we need to first select the record with the longest duration and find that artist_id.
    string = """
    SELECT 
        artist_name
    FROM artists
    INNER JOIN tracks
    ON artists.artist_id = tracks.artist_id
    WHERE duration = (SELECT MAX(duration) FROM tracks)
    """
    cursor.execute(string)
    for r in cursor:
        print(r)
        
    #6.Find the mean duration of all tracks.
    string = """
    SELECT 
        AVG(duration)
    FROM tracks
    """
    cursor.execute(string)
    print(cursor.fetchall())
    
    #7.Using only one query, count the number of tracks whose artists don't have any linked terms.
    #because we can only use one query and we need to acquire the artists that are in table tracks but not in table artist_term, so it's useful to use outer join. 
    string = """
    SELECT count(tracks.track_id)
    FROM tracks
    LEFT JOIN artist_term ON tracks.artist_id = artist_term.artist_id
    WHERE artist_term.artist_id IS NULL
    """
    cursor.execute(string)
    print(cursor.fetchall())
    
    #8.Index- Run Question 1 query in a loop for 100 times and note the minimum time taken. Now create an index on the column artist_id and compare the time. Share your findings in the report.
    #first, run Q1 in a loop for 100 times and note the time:
    t = ('TRMMWLD128F9301BF2',)
    string = """
    SELECT
        artists.artist_id,
        artists.artist_name,
        artist_term.term
    FROM artists
    INNER JOIN tracks
    ON artists.artist_id = tracks.artist_id
    INNER JOIN artist_term
    ON tracks.artist_id = artist_term.artist_id
    WHERE tracks.track_id=?
    """
    def func():
        for i in range (0, 100):
            c.execute(string, t)
    %timeit func()
    #it shows the total time it takes for 100 loops is: 1min 58s ± 3.31 s per loop (mean ± std. dev. of 7 runs, 1 loop each). average time for one loop is around 1.18s.
    #Then we create indices in turn for artist_id in each table.
    string = """
    CREATE INDEX index_artist_id
    ON tracks(artist_id)
    """
    cursor.execute(string)
    
    string = """
    CREATE INDEX index1
    ON artists(artist_id)
    """
    cursor.execute(string)
    
    string = """
    CREATE INDEX index2
    ON artist_term(artist_id)
    """
    cursor.execute(string)
    #at last we test the time it takes to run query after adding indices:
    t = ('TRMMWLD128F9301BF2',)
    string = """
    SELECT
        artists.artist_id,
        artists.artist_name,
        artist_term.term
    FROM artists
    INNER JOIN tracks
    ON artists.artist_id = tracks.artist_id
    INNER JOIN artist_term
    ON tracks.artist_id = artist_term.artist_id
    WHERE tracks.track_id=?
    """
    def func():
        for i in range (0, 100):
            cursor.execute(string, t)
    %timeit func()
    #it shows the total time is substantially decreased compared to the query without indices:1.86 ms ± 22.4 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)
    #So in conclusion, we know that an index can be used to speed up data retrieval.
    
    #9.Find all tracks associated with artists that have the tag eurovision winner and delete them from the database, then roll back this query using a transaction. Hint: you can select from the output of a select!
    #First, find all tracks associated with artists that have the tag eurovision winner 
    t = ('eurovision winner',)
    string = """
    SELECT * 
    FROM tracks
    WHERE artist_id IN
    (SELECT artist_id 
    FROM artist_term
    WHERE term =?)
    """
    cursor.execute(string,t)
    df= pd.DataFrame(cursor.fetchall())
    df.columns = ['track_id','title','release','year','duration','artist_id']
    print(df)
    #Then delete them from the database, then roll back this query using a transaction
    cursor.execute('BEGIN TRANSACTION;')
    cursor.execute('SAVEPOINT sp0; ')
    cursor.execute(
    'DELETE FROM tracks WHERE artist_id in (SELECT artist_id FROM artist_term WHERE term=?); ',t)
    cursor.execute('ROLLBACK TO sp0; ')
    cursor.execute('END TRANSACTION; ')
    

