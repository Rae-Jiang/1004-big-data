#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Part 1: unsupervised model training

Usage:

    $ spark-submit unsupervised_train.py hdfs:/path/to/file.parquet hdfs:/path/to/save/model

'''


# We need sys to get the command line arguments
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession

# TODO: you may need to add imports here
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

def main(spark, data_file, model_file):
    '''Main routine for unsupervised training

    Parameters
    ----------
    spark : SparkSession object

    data_file : string, path to the parquet file to load

    model_file : string, path to store the serialized model file
    '''

    ###
    # TODO: YOUR CODE GOES HERE
    # Load the dataframe
    df = spark.read.parquet(data_file)
    #select out the 20 attribute columns labeled mfcc_00, mfcc_01, ..., mfcc_19
    features = [c for c in df.columns if c[:4]=='mfcc']
    assembler= VectorAssembler(inputCols=features,outputCol='features')

    standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")

    kmeans = KMeans().setK(100).setFeaturesCol("features_scaled")
    # Build the pipeline with our assembler, standardScaler, and Kmeans stages
    pipeline = Pipeline(stages=[assembler, standardScaler, kmeans])

    model = pipeline.fit(df)
    result = model.transform(df)

    model.write().overwrite().save(model_file)

    ###

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('unsupervised_train').getOrCreate()

    # Get the filename from the command line
    data_file = sys.argv[1]

    # And the location to store the trained model
    model_file = sys.argv[2]

    # Call our main routine
    main(spark, data_file, model_file)

