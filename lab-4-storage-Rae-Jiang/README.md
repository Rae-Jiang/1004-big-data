# DSGA1004 - BIG DATA
## Lab 4: Storage benchmarking
- Prof Brian McFee (bm106)
- Mayank Lamba (ml5711)
- Saumya Goyal (sg5290)

*Handout date*: 2019-03-28

*Submission deadline*: 2019-04-10


## Requirements

This lab assignment will require the use of Spark on the Dumbo cluster.

As in the previous assignment, remember to activate your environment as follows:
```
module load python/gnu/3.6.5
module load spark/2.4.0
```

## Part 0: Introduction

In this assignment, you will be comparing the speed of Spark queries against
dataframes backed be either CSV or Parquet file stores.

As reference data, you'll be using a synthetic dataset including names, annual
income, and 5-digit zip codes.  Three versions of the data are provided:

  - `hdfs:/user/bm106/pub/people_small.csv`: 5000 records (166 KB)
  - `hdfs:/user/bm106/pub/people_medium.csv`: 500,000 records (16.2 MB)
  - `hdfs:/user/bm106/pub/people_large.csv`: 50,000,000 records (1.6 GB)

The schema for these files is as follows:

  - `schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT'`

In each of the sections below, you will run a set of DataFrame queries against each
version of the data.  This will allow you to measure how the different analyses and
storage engines perform at different scales of data.

*Tip*: you may want to work through each part completely using just the small and
medium data before starting on the large data.  This will make it easier for you to 
debug your analysis and get familiar with the data.

### Getting started

In the project repository, you will find two python files: `bench.py` and
`queries.py`.

```python
>>> from imp import reload
>>> import queries
>>> import bench
```

The file `bench.py` contains one function `benchmark()` which is used to conduct
timing benchmarks on a spark DataFrame query.  Rather than provide the DataFrame
directly to the benchmark function, you will need to write a function that constructs
a new DataFrame when called.

An example of this is given in `queries.py` as `queries.basic_query()`.  This
function takes in the `spark` session object, as well as the path to a CSV file, and
returns a DataFrame of the first five people sorted by `last_name, first_name`.  For
example:

```python
>>> top5 = queries.basic_query(spark, 'hdfs:/user/bm106/pub/people_small.csv')
>>> top5
DataFrame[first_name: string, last_name: string, income: float, zipcode: int]
>>> top5.show()
+----------+---------+-------+-------+                                          
|first_name|last_name| income|zipcode|
+----------+---------+-------+-------+
|   Annette|   Abbott|72870.0|  14763|
|    Hailey|   Abbott|72182.0|  75097|
|   Jocelyn|   Abbott|56574.0|   3062|
|     Sheri|   Abbott|64952.0|  77663|
|     Sonya|   Abbott|86156.0|  79072|
+----------+---------+-------+-------+
```

Rather than benchmarking `top5` directly, instead benchmark it as follows:
```python
>>> times = bench.benchmark(spark, 5, queries.basic_query,
...                         'hdfs:/user/bm106/pub/people_small.csv')
>>> times
[6.869371175765991, 0.21157383918762207, 0.2251441478729248, 0.1284043788909912, 0.12465882301330566]
```
This constructs the query `5` times using the `people_small.csv` file, and returns a list of the time (in seconds) for each trial.



**NOTE**: If at any point you change `queries.py` (e.g. to add a new function), remember to reload the module in your spark session by saying:
```python
>>> reload(queries)
```


## Part 1: Benchmarking queries

Using the `basic_query` function as a template, create three new functions in `queries.py`:

  - `csv_avg_income`: returns a DataFrame which computes the average `income` grouped by `zipcode`.

  - `csv_max_income`: returns a DataFrame which computes the maximum `income` grouped by `last_name`.

  - `csv_sue`: returns a DataFrame which filters down to only include people with `first_name` of `'Sue'` and `income` at least `75000`.

All three of these functions should work similarly to the `basic_query` example: parameters are the `spark` session object and the path in HDFS to the CSV file.

Once you have all three queries written, benchmark their performance on each of the three data sets.  Each benchmark should contain 25 trials.  Record the minimum, median, and maximum time to complete each of the queries on each of the three data files.


## Part 2: CSV vs Parquet

For each of the three data files (small, medium, and large) convert the data to
Parquet format and store it in your own HDFS folder (e.g.,
`hdfs:/user/YOUR_NETID/people_small.parquet`).  The easiest way to do this is to load
the CSV file into a DataFrame in the pyspark console, and then write it out using the [`DataFrame.write.parquet()`](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.parquet) method.

Having translated the three data files from CSV to parquet, create three new functions in `queries.py` that operate on parquet-backed files rather than CSV:

  - `pq_avg_income`
  - `pq_max_income`
  - `pq_sue`

Now, repeat the benchmarking experiment from part 1 using these parquet-backed sources instead of the CSV sources.  Again, report the min, median, and max time for each query on each of the three files.  How do they compare to the results in part 1?


## Part 3: Optimizing Parquet

In the final part, your job is to try to make things go faster!  In this section, you are not allowed to change any of the *query* code that you wrote in the previous step, but you *are* allowed to change how the files are stored in HDFS.

Some things you may want to try:

  - Sort the DataFrame according to particular columns before writing out to parquet
  - Change the `partition` structure of the data.  This can be done in two ways:
  - `dataframe.repartition(...)` (as described in lecture) and then writing the resulting dataframe back to a new file
  - Explicitly setting the partition columns when writing out to parquet.  **WARNING**: this can be very slow!
  - Change the HDFS replication factor.

Each of the three queries may benefit from different types of optimization, so you may want to make multiple copies of each file.

*Hint*: you may want to look through the `explain()` output on each of your queries when choosing optimizations.

## What to turn in

  - The code for all of your queries, stored in `queries.py`
  - A brief report of your benchmarking results:
    - A summary of all numerical results for each query/size/storage combination.
    - How do the results in parts 1, 2, and 3 compare?
    - What did you try in part 3 to improve performance for each query?
    - What worked, and what didn't work?

Please save your report as a .pdf file and include it in the github repository when you submit the assignment.

