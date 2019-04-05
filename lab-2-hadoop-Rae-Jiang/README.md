# DSGA1004 - BIG DATA
## Lab 2: Hadoop
- Prof Brian McFee (bm106)
- Mayank Lamba (ml5711)
- Saumya Goyal (sg5290)

					
*Handout date*: 2019-02-21

*Submission deadline*: 2019-03-06, 23:55 EST

## 0. Requirements

Sections 1 and 2 of this assignment are designed to get you familiar with HPC and the work-flow of running Hadoop jobs.

For full credit on this lab assignment, you will need to provide working solutions for sections 3 (matrix multiplication) and 4 (table joins).
You are provided with small example inputs for testing these programs, but we will run your programs on larger data for grading purposes.
Be sure to commit your final working implementations to git (and push your changes) before the submission deadline!


## 1. High-performance Computing (HPC) at NYU

This lab assignment will require the use of the
Hadoop cluster run by the NYU high-performance
computing (HPC) center.  To learn more about HPC at
NYU, refer to the [HPC Wiki](https://wikis.nyu.edu/display/NYUHPC/High+Performance+Computing+at+NYU).

By now, you should have received notification at
your NYU email address that your HPC account is active. If you have not received this notification yet, please contact the instructor immediately.

If you're new to HPC, please read through the
[tutorial](https://wikis.nyu.edu/display/NYUHPC/Tutorials) section of the wiki, and in particular, the [Getting started on Dumbo](https://wikis.nyu.edu/display/NYUHPC/Clusters+-+Dumbo) section.

**Note**: Logging into the HPC from outside the NYU
network can be somewhat complicated.  Instructions
are given
[here](https://wikis.nyu.edu/display/NYUHPC/Logging+in+to+the+NYU+HPC+Clusters).



## 2. Hadoop and Hadoop-streaming

In lecture, we discussed the Map-Reduce paradigm in the abstract, and did not dive into the details of the Hadoop implementation.  Hadoop is an open-source implementation of map-reduce written in Java.
In this lab, you will be implementing map-reduce jobs using Hadoop's "streaming" mode, which allows mappers and reducers to be implemented in any programming language.  In our case, we'll be using Python.


### Environment setup

Before we get started, there are several commands and configuration variables that you will need to frequently set during this lab.  It will be easier if you configure your shell to provide some shortcuts.  This can be done by using your favorite text editor (e.g., `vi`, `emacs`, or `nano`) to modify the file `~/.bashrc`.  Specifically, you should add the following lines to your `~/.bashrc` file:
```bash
HADOOP_EXE='/usr/bin/hadoop'
HADOOP_LIBPATH='/opt/cloudera/parcels/CDH/lib'
HADOOP_STREAMING='hadoop-mapreduce/hadoop-streaming.jar'

alias hfs="$HADOOP_EXE fs"
alias hjs="$HADOOP_EXE jar $HADOOP_LIBPATH/$HADOOP_STREAMING"
```
After you have modified `.bashrc`, you can load the new aliases by executing the following command:
```bash
source ~/.bashrc
```
These modifications add shortcuts for interacting with the Hadoop distributed filesystem (`hfs`) and launching map-reduce jobs (`hjs`).

(*Note*: the modifications listed above are also included in `shell_setup.sh` given to you with this assignment.  Feel free to copy-paste the contents of that file into `.bashrc` in your home directory.)

### A first map-reduce project

Included within the repository under `word_count/` is a full implementation of the "word-counting" program, and an example input text file (`book.txt`).

The program consists of four files:
```
src/mapper.py
src/mapper.sh
src/reducer.py
src/reducer.sh
```
Why four files?  Clearly, two of them relate to the *mapper* and two relate to the *reducer*.
The reason for this is that we use the shell scripts (`.sh` extensions) to load dependencies on the worker nodes of the cluster before the mapper or reducer functions (`.py` extensions) are executed.
In this case, the only dependency is `Python`.
Once the dependency modules are loaded, the shell script (`mapper.sh` or `reducer.sh`) executes the mapper or reducer, which read from the standard input and write to the standard output.
Hadoop-streaming will coordinate the communication between mappers and reducers for us.


### Test-driving the mapper and reducer

Before we move on, it's a good idea to run these programs locally so we know what to expect.  (Hint: this is also an easy way to debug, as long as you have a small input on hand!)

You can run the mapper by going into the `word_count/src/` directory, and running the following commands:
```bash
module load python/gnu/3.6.5
cat ../book.txt | python3 mapper.py
```
The first command enables Python in your environment so that it matches what will run on the cluster.
The second command will run the contents of `book.txt` through `mapper.py`, resulting in an unordered list of `(key, value)` pairs.

These key-value pairs will be in the same order as they appear in `book.txt`, but Map-Reduce will want to sort them by key to make grouping easier.
To simulate this, run the same command followed by `sort`:
```bash
cat ../book.txt | python3 mapper.py | sort
```
This will produce the same output as above, but now you should see all repetitions of the same word grouped together.

Finally, you can run this through the `reducer` by adding one more step:
```bash
cat ../book.txt | python3 mapper.py | sort | python3 reducer.py
```
After running this command, you should see the total counts of each word in `book.txt`!
Remember, we did this all on one machine without using Hadoop, but you should now have a sense of what the inputs and outputs of these programs look like.


### Launching word-count on Hadoop

Before we can launch the word counter on the cluster, we will need to place the input file in the Hadoop distributed file system (HDFS).

To do this, issue the following command from inside the `word_count` directory:
```bash
hfs -put book.txt
```

Now, the example file is accessible from the Hadoop cluster, and we're ready to launch the job.
This is done by issuing the following command from inside the `word_count` directory:
```bash
hjs -file src/ -mapper src/mapper.sh -reducer src/reducer.sh -input book.txt -output word_count.out
```

The `hjs` command submits the job; the `-file` parameter indicates which files will be distributed to the worker nodes (i.e., the source code); the `-mapper` and `-reducer` parameters specify the paths to the mapper and reducer scripts; and the `-input` and `-output` paths specify the input file(s) and output file paths for the job.

After the job finishes, execute the command
```bash
hfs -ls
```
and you should see two files: `book.txt` and `word_count.out`, the latter being a directory.
If you run `hfs -ls word_count.out/` you will see several file "parts", each corresponding to a single reducer node.
To retrieve the results of the computation, run
```bash
hfs -get word_count.out 
```
to get all the partial outputs.
To get the complete output as one file, run:
```bash
hfs -getmerge word_count.out word_count_total.out
```
After running these commands, the results of the computation will be available to you through the usual unix file system.

At this point, you should now have successfully run your first Hadoop job!


## 3. Matrix products
In the next part of this assignment, you will develop a map-reduce algorithm for matrix multiplication.  Recall that the product of two matrices `A * B` where `A` has shape `(n, k)` and `B` has shape `(k, m)` is a matrix `C` of shape `(n, m)` where for each `i` and `j` coordinates,
```
C[i, j] = A[i, 1] * B[1, j] + A[i, 2] * B[2, j] + ... + A[i, k] * B[k, j]
```

In the `matmul/` directory, you will find skeleton code for implementing matrix multiplication `matmul/src/`.
Accompanying this is an example pair of matrices to multiply: `matmul/small/A.txt` and `matmul/small/B.txt`.
In this assignment, you can assume that `A` and `B` will always be of compatible dimension, and that `A` will be on the left side of the multiplication.
The mapper program should take as command-line arguments the dimensions of the product `C`, that is, the number of rows of `A` and the number of columns of `B`.


Your job in this part of the assignment is to fill in the skeleton code (`mapper.py` and `reducer.py`) by designing the intermediate key-value format, and implementing the rest of the program accordingly.

Note that the data now comes spread into two files (`A.txt` and `B.txt`), which must both be processed by the mapper. 
In Hadoop streaming, each file can be spread across multiple mappers, and if a directory is given as the `-input` option, then all files are processed.
Within the mapper, the identity of the file currently being processed can be found in the environment variable `mapreduce_map_input_file`, as noted in the code comments.  (Hint: this will be useful in part 4 below!)

To run the job, place the data on HDFS by issuing the command:
```bash
hfs -put small
```
from within the `matmul` directory.
Once you have implemented your mapper and reducer, the job should be launched by the following command:
```bash
hjs -files src/ -mapper src/mapper.sh -reducer src/reducer.sh -input small/ -output small_product
```
from within the `matmul` directory.
The output of the computation can be retrieved using `hfs -getmerge` like in the example section above.


**Note**: because this program will use multiple input files and environment variables to track which file is being processed, it is a bit more difficult to run locally.  You can test it as follows:
```bash
mapreduce_map_input_file=A.txt python3 src/mapper.py 50 50 < small/A.txt > tmp_A.txt
mapreduce_map_input_file=B.txt python3 src/mapper.py 50 50 < small/B.txt > tmp_B.txt
sort tmp_A.txt tmp_B.txt | python3 src/reducer.py
```
which will temporarily set the input file environment variable while running the mapper on each input (lines 1 and 2), and run the reducer on the sorted intermediate values.


## 4. Tabular data

In the final section of the lab, you are given two data files in comma-separated value (CSV) format.
These data files (`joins/music_small/artist_term.csv` and `joins/music_small/track.csv`) contain the same music data from the previous lab assignment on SQL and relational databases.  Specifically, the file `artist_term.csv` contains data of the form
```
ARTIST_ID,tag string
```
and `track.csv` contains data of the form
```
TRACK_ID,title string,album string,year,duration,ARTIST_ID
```

No skeleton code is provided for this part, but feel free to adapt any code from the previous sections that you've already completed.

### 4.1 Joining tables

For the first part, implement a map-reduce program which is equivalent to the following SQL query:
```
SELECT 	artist_id, track_id, term
FROM	track INNER JOIN artist_term 
ON 		track.artist_id = artist_term.artist_id
```

The program should be executable in a way similar to the matrix multiplication example, for example:
```bash
hjs -files src/ -mapper src/join_mapper.sh -reducer src/join_reducer.sh -input music_small/ -output join_query
```

### 4.2 Aggregation queries

For the last part, implement a map-reduce program which is equivalent to the following SQL query:
```
SELECT 	track.artist_id, max(track.year), avg(track.duration), count(artist_term.term)
FROM	track LEFT JOIN artist_term
ON		track.artist_id = artist_term.artist_id
GROUP BY track.artist_id
```
That is, for each artist ID, compute the maximum year of release, average track duration and the total number of terms matching the artist.  **Note**: the number of terms for an artist could be zero!

The program should be executable by the following command:
```bash
hjs -files src/ -mapper src/group_mapper.sh -reducer src/group_reducer.sh -input music_small/ -output group_query
```
