#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''training
Usage:
    $ module load python/gnu/3.6.5
    $ module load spark/2.4.0
    $ spark-submit train.py hdfs:/user/zd415/project/subset002.parquet hdfs:/user/zd415/project/pipeline
    
'''

#imports
import sys
from pyspark.sql import SparkSession
#spark.ml packages
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.recommendation import ALS

def main(spark, train_data_file, path2save_model):
	'''
    Args
    ---------
    train_data_file(parquet):
    	training data (string ID)

    path2save_model:
    	path to save the trained pipeline model (stringIndexers+ als)
    '''

    #Read train and val data
    train = spark.read.parquet(train_data_file).drop("__index_level_0__")

    #########################################################
    #                       Pipeline                        #
    #########################################################

    # StringIndexers
    indexer1 = StringIndexer(inputCol="user_id",outputCol="userId", handleInvalid="skip")
    indexer2 = StringIndexer(inputCol="track_id",outputCol="itemId", handleInvalid="skip")
    #Choice: handleInvalid = 'skip'/'keep'

    # Build the recommendation model using ALS 
    als = ALS(maxIter=10, rank= 10, regParam=0.05, alpha = 1.0,
              userCol="userId", itemCol="itemId", ratingCol="count",
              coldStartStrategy="drop",implicitPrefs=True).setSeed(0)

    # Combine into the pipeline
    pipeline = Pipeline(stages=[indexer1,indexer2, als])

    #########################################################
    #                       Training                        #
    #########################################################
    print("Start to train...")
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

    # Call our main routine
    main(spark, train_data_file, model_file)

