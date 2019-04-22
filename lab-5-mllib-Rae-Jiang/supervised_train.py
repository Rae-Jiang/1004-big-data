#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Part 2: supervised model training

Usage:

    $ spark-submit supervised_train.py hdfs:/path/to/file.parquet hdfs:/path/to/save/model

'''


# We need sys to get the command line arguments
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession

# TODO: you may need to add imports here
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

def main(spark, data_file, model_file):
    '''Main routine for supervised training

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
    #select and standardize the mfcc_* features
    features = [c for c in df.columns if c[:4]=='mfcc']
    assembler= VectorAssembler(inputCols=features,outputCol='features')
    standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")
    #Encode the genre field as a target label using a StringIndexer and store the result as label.
    stringIndexer = StringIndexer(inputCol="genre", outputCol="label", handleInvalid='skip')
    #Define a multi-class logistic regression classifier
    lr = LogisticRegression(featuresCol="features_scaled", labelCol="label")

    pipeline = Pipeline(stages=[assembler, standardScaler, stringIndexer, lr])
    #Optimize the hyper-parameters by 5-fold cross-validation
    paramGrid = ParamGridBuilder().addGrid(lr.elasticNetParam, [0.01,0.1,0.2,0.5,0.8]).addGrid(lr.regParam, [0.001,0.01,0.05,0.1,0.5]).build()

    cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=MulticlassClassificationEvaluator(labelCol='label',predictionCol='prediction'))

    model = cv.fit(df)
    classifier = model.transform(df)

    best_lr = model.bestModel
    best_lr.write().overwrite().save(model_file)

    ###



# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('supervised_train').getOrCreate()

    # Get the filename from the command line
    data_file = sys.argv[1]

    # And the location to store the trained model
    model_file = sys.argv[2]

    # Call our main routine
    main(spark, data_file, model_file)

