from cassandra.cluster import Cluster
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import *
import pyspark_cassandra
import decimal
import re, string
import sys,operator, os, math
import itertools
from math import sqrt
from operator import add
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS

def map_string_to_int(lst):
    my_dict = {}
    for i in range(len(lst)):
        my_dict[i] = lst[i]
    inv_dict = dict((v, k) for k, v in my_dict.iteritems())
    return(inv_dict)

def main():
   outdir = sys.argv[1]

   cluster_seeds = ['199.60.17.136', '199.60.17.173']
   conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
   sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
   sqlContext = SQLContext(sc)

   # Loading yelp_review data
   yelp_review = sc.cassandraTable("bigbang", "yelp_review").select('user_id','business_id','stars')\
       .map(lambda r: (r['user_id'],r['business_id'],r['stars'])).cache()


   user_id_lst = list(set(yelp_review.map(lambda r: r[0]).collect()))
   user_dict = map_string_to_int(user_id_lst)

   business_id_lst = list(set(yelp_review.map(lambda r: r[1]).collect()))
   business_dict = map_string_to_int(business_id_lst)

   ratings = yelp_review.map(lambda r: (user_dict[r[0]],business_dict[r[1]],r[2])).cache()

   # Splitting Ratings data into Training set, Validation set and Test set.
   training_RDD, validation_RDD, test_RDD = ratings.randomSplit([6, 2, 2], seed=0L)
   validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1])).cache()
   test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1])).cache()

   # Training Phase
   seed = 5L
   iterations = [10,15,20]
   regularization_parameters = [1.0, 0.1, 0.01]
   ranks = [12, 16, 20]
   min_error = float('inf')
   best_rank = -1
   for rank in ranks:
       for iteration in iterations:
           for regularization_parameter in regularization_parameters:
               model = ALS.train(training_RDD, rank, iteration, lambda_=regularization_parameter, seed = seed)
               predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
               rates_and_preds = validation_RDD.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
               error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())
               print 'For rank %s , iteration %s , regularization parameter %s the RMSE is %s' % (rank,iteration,regularization_parameter,error)
               if error < min_error:
                   min_error = error
                   best_rank = rank
                   best_iteration = iteration
                   best_regularizer = regularization_parameter

   print 'The best model was trained with rank %s , iteration %s , regularization parameter %s' % (best_rank, best_iteration,best_regularizer)

   # Test the selected model
   model = ALS.train(training_RDD, best_rank, best_iteration, lambda_=best_regularizer, seed = seed)
   predictions = model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))

   rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
   error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())
   print 'For testing data the RMSE is %s' % (error)

   result = sc.parallelize([('best_rank', best_rank),('best_iteration', best_iteration),('best_regularizer', best_regularizer), ('train_error', min_error),('test_error', error)])
   outdata = result.map(lambda o_str: u"%s : %s" % (o_str[0],o_str[1])).coalesce(1)
   outdata.saveAsTextFile(outdir)

if __name__ == "__main__":
    main()