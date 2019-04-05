# DSGA1004 - BIG DATA
## Lab 1: SQLite
- Prof Brian McFee (bm106)
- Mayank Lamba (mk5711)
- Saumya Goyal (sg5290)

					
*Handout date*: 2019-02-07

*Submission deadline*: 2019-02-21, 23:55 EST

### Goal of this exercise:
In this exercise, you will write queries to understand the concepts of SQLite and write Python scripts to execute those queries.	
			

Database Provided:  `Sample_Song_Dataset.db`.  This can be obtained through the
`resources` section of the newclasses.nyu.edu website.

The database schema contains three tables:

- tracks
    - track_id
    - title
    - release
    - year
    - duration
    - artist_id
- artists
    - artist_id
    - artist_name
- artist_terms
    - artist_id
    - term


Modify the given Python Script (`Lab1StarterCode.py`) to execute the following queries. Also, create a PDF file that includes for each question: the SQL query you constructed, its results (output) and an  explanation of the query when appropriate.

After you complete each step, commit your changes to git so that we have a log of your progress.
	
1. Find id, name and term of the artist who played the track with id `TRMMWLD128F9301BF2`.
2. Select all the unique tracks with the duration is strictly greater than `3020` seconds. 
3. Find the ten shortest (by duration) 10 tracks released between `2010` and `2014` (inclusive), ordered by increasing duration.
4. Find the top 20 most frequently used terms, ordered by decreasing usage.
5. Find the artist name associated with the longest track duration.
6. Find the mean duration of all tracks.
7. Using only one query, count the number of tracks whose artists don't have any linked terms.
8. Index- Run Question 1 query in a loop for 100 times and note the minimum time taken. Now create an index on the column artist_id and compare the time. Share your findings in the report.
9. Find all tracks associated with artists that have the tag `eurovision winner` and delete them from the database, then roll back this query using a transaction.  Hint: you can select from the output of a select!
