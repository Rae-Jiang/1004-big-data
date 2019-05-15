#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''training

Usage:
    $ module load python/gnu/3.6.5
    $ module load spark/2.4.0
    $ spark-submit evaluate.py hdfs:/user/bm106/pub/project/cf_validation.parquet hdfs:/user/zd415/project/pipeline  
    

'''

import sys
from pyspark.sql import SparkSession

#spark.ml packages
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, expr
from pyspark.mllib.evaluation import RankingMetrics

def main(spark, val_data_file, model_file):
	'''
    Args
    -------
    val_data_file:
        validation data (string ID)
    
    model_file:
        path to the pipeline(stringIndexers + als) model
    '''

    # Read data
    val = spark.read.parquet(val_data_file).drop("__index_level_0__")

    print('Loading the trained model...')
    # Load the trained pipeline model
    model = PipelineModel.load(model_file)

    #########################################################
    #                       Evaluate                        #
    #########################################################
    print("Predicting...")
    # Run the model to create a prediction agains validation set
    preds = model.transform(val)
    
    print("Evaluating...")
    # Generate top 10 movie recommendations for each user (sorted??)
    # Returns a DataFrame of (userCol, recommendations), 
    # where recommendations are stored as an array of (itemCol, rating) Rows.
    perUserPredictions = model.stages[-1].recommendForAllUsers(500)\
                            .selectExpr("userId","recommendations.itemId as items_pred")
    # perUserPredictions.show(5)

    perUserActual = preds.select("userId","itemId").filter("count>0")\
                        .groupBy("userId")\
                        .agg(expr("collect_set(itemId) as items"))
    # perUserActual.show(5)

    
    print("Joining...")
    # perUserPredvActual: an RDD of (predicted ranking, ground
    # truth set) pairs
    # perUserPredvActual = perUserPredictions.join(perUserActual, ["userId"]).rdd\
    #                 .map(lambda row: (row.items_pred, row.items)).cache()

    perUserPredvActual = perUserPredictions.join(perUserActual, ["userId"])\
                    .select("items_pred","items").rdd

    # print(*perUserPredvActual.take(5),sep="\n")

    # Evaluate using MAP
    print("Calculating MAP...")
    ranks = RankingMetrics(perUserPredvActual)
    mean_avg_precision = ranks.meanAveragePrecision
    print("Mean Average Precision:", mean_avg_precision)

 # Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('evaluate').getOrCreate()

    # validation data file
    val_data_file = sys.argv[1]

    # location of the trained model
    model_file = sys.argv[2]

    # Call our main routine
    main(spark, val_data_file, model_file)

