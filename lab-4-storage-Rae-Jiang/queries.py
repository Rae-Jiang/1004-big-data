#!/usr/bin/env python

def basic_query(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a dataframe corresponding to the
    first five people, ordered alphabetically by last_name, first_name.

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the CSV file, e.g.,
        `hdfs:/user/bm106/pub/people_small.csv`

    schema : string
        The CSV schema
    '''

    # This loads the CSV file with proper header decoding and schema
    people = spark.read.csv(file_path, header=True,
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    people.createOrReplaceTempView('people')

    top5 = spark.sql('SELECT * FROM people ORDER BY last_name, first_name ASC LIMIT 5')

    return top5

# --- ADD YOUR NEW QUERIES BELOW ---
#1.csv_avg_income: returns a DataFrame which computes the average income grouped by zipcode.
def csv_avg_income(spark, file_path):
    people = spark.read.csv(file_path, header=True,
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    people.createOrReplaceTempView('people')

    avg_income = spark.sql('SELECT zipcode,AVG(income) FROM people GROUP BY zipcode')

    return avg_income

#2.csv_max_income: returns a DataFrame which computes the maximum income grouped by last_name.
def csv_max_income(spark, file_path):
    people = spark.read.csv(file_path, header=True,
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    people.createOrReplaceTempView('people')

    max_income = spark.sql('SELECT last_name,MAX(income) FROM people GROUP BY last_name')

    return max_income

#3.csv_sue: returns a DataFrame which filters down to only include people with first_name of 'Sue' and income at least 75000.
def csv_sue(spark, file_path):
    people = spark.read.csv(file_path, header=True,
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    sue = people.filter((people.first_name=='Sue') & (people.income>=75000)).select('*')

    return sue

#
def pq_avg_income(spark, file_path):
    people = spark.read.parquet(file_path)

    people.createOrReplaceTempView('people')
    #
    avg_income = spark.sql('SELECT zipcode,AVG(income) FROM people GROUP BY zipcode')

    return avg_income
#
def pq_max_income(spark, file_path):
    people = spark.read.parquet(file_path)

    people.createOrReplaceTempView('people')

    max_income = spark.sql('SELECT last_name,MAX(income) FROM people GROUP BY last_name')

    return max_income
#
def pq_sue(spark, file_path):
    people = spark.read.parquet(file_path)

    people.createOrReplaceTempView('people')

    sue = people.filter((people.first_name=='Sue') & (people.income>=75000)).select('*')

    return sue
