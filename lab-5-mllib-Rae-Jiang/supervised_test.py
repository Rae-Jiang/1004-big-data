#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Part 1: supervised model testing

Usage:

    $ spark-submit supervised_test.py hdfs:/path/to/load/model.parquet hdfs:/path/to/file

'''


# We need sys to get the command line arguments
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession

# TODO: you may need to add imports here
from pyspark.ml import PipelineModel
from pyspark.mllib.evaluation import MulticlassMetrics

def main(spark, model_file, data_file):
    '''Main routine for supervised evaluation

    Parameters
    ----------
    spark : SparkSession object

    model_file : string, path to store the serialized model file

    data_file : string, path to the parquet file to load
    '''

    ###
    # TODO: YOUR CODE GOES HERE
    #load best lr model
    model = PipelineModel.load(model_file)
    # Load the test dataframe
    test = spark.read.parquet(data_file)

    predictions = model.transform(test)

    predictionAndLabels = predictions.rdd.map(lambda lp: (lp.prediction, lp.label))
    metrics = MulticlassMetrics(predictionAndLabels)

    # Overall statistics
    precision = metrics.precision()
    recall = metrics.recall()
    f1Score = metrics.fMeasure()
    print("Overall Stats:")
    print("Precision = %s" % precision)
    print("Recall = %s" % recall)
    print("F1 Score = %s" % f1Score)

    # Weighted stats
    print("Weighted precision = %s" % metrics.weightedPrecision)
    print("Weighted recall = %s" % metrics.weightedRecall)
    print("Weighted F1 Score = %s" % metrics.weightedFMeasure())

    # Statistics by class
    print("Stats by class")

    for (genre,label) in predictions.select('genre','label').distinct().collect():
        print("Class %s precision = %s" % (genre, metrics.precision(label)))
        print("Class %s recall = %s" % (genre, metrics.recall(label)))
        print("Class %s F1 Score = %s" % (genre, metrics.fMeasure(label, beta=1.0)))

    ###



# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('supervised_test').getOrCreate()

    # And the location to store the trained model
    model_file = sys.argv[1]

    # Get the filename from the command line
    data_file = sys.argv[2]

    # Call our main routine
    main(spark, model_file, data_file)

