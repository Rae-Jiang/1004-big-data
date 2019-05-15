#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''extension 1
Requirement:Conduct a thorough evaluation of different modification strategies
(e.g., log compression, or dropping low count values)
and their impact on overall accuracy.
Usage:

    $ spark-submit train.py hdfs:/path/to/data hdfs:/path/to/save/model strategy(none,log,drop)



'''

#imports
import sys
from pyspark.sql import SparkSession
#spark.ml packages
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import log

def main(spark, train_data_file, path2save_model, strategy):
	'''
    Args
    ---------
    train_data_file(parquet):
    	training data (string ID)

    path2save_model:
    	path to save the trained model (stringIndexers+ als)
    '''

    #Read train and val data
    train = spark.read.parquet(train_data_file).drop("__index_level_0__")

    #########################################################
    #                       Pipeline                        #
    #########################################################

    # StringIndexers
    indexer1 = StringIndexer(inputCol="user_id",outputCol="userId", handleInvalid="skip")
    indexer2 = StringIndexer(inputCol="track_id",outputCol="itemId", handleInvalid="skip")#handleInvalid = 'skip'/'keep'

    #different modification strategies on "count"
    #1.log compression (add column "log_count")
    ratingCol = 'count'
    if strategy == 'log':
        train = train.withColumn("log_count",log("count"))
        ratingCol = "log_count"
    #2.drop low count values
    elif strategy == 'drop':
        train = train.filter(train['count']>1)


    # Build the recommendation model using ALS
    als = ALS(maxIter=5, rank= 100, regParam=0.01, alpha = 1.0,
              userCol="userId", itemCol="itemId", ratingCol = ratingCol,
              coldStartStrategy = "drop",implicitPrefs = True).setSeed(0)

    # Combine into the pipeline
    pipeline = Pipeline(stages=[indexer1,indexer2, als])

    #########################################################
    #                       Training                        #
    #########################################################
    model = pipeline.fit(train)

    # Save the trained model
    model.write().overwrite().save(path2save_model)
    print("Trained model saved")

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('train').getOrCreate()

    #train data file
    train_data_file = sys.argv[1]

    # location to store the trained model
    model_file = sys.argv[2]

    #which strategy to modify the count data(choice: none,log,drop)
    strategy = sys.argv[3]

    # Call our main routine
    main(spark, train_data_file, model_file, strategy)
