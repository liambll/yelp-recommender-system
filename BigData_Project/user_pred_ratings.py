from cassandra.cluster import Cluster
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import *
import pyspark_cassandra
import decimal
from decimal import *
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

def map_int_to_string(my_dict):
    inv_dict = dict((v, k) for k, v in my_dict.iteritems())
    return(inv_dict)

def main():
   in_path = sys.argv[1]
   #outdir = sys.argv[2]

   cluster_seeds = ['199.60.17.136', '199.60.17.173']
   conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds))
   sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
   sqlContext = SQLContext(sc)

   # Loading best parameters found in model_selection.py
   text = sc.textFile(in_path)
   text_processed = text.map(lambda line: line.split(":")).cache()
   best_rank = text_processed.filter(lambda w: str(w[0]).strip() == 'best_rank').map(lambda w: w[1]).collect()
   best_rank_int = int(best_rank[0])
   best_iteration = text_processed.filter(lambda w: str(w[0]).strip() == 'best_iteration')\
       .map(lambda w: w[1]).collect()
   best_iter_int = int(best_iteration[0])
   best_regularizer = text_processed.filter(lambda w: str(w[0]).strip() == 'best_regularizer')\
       .map(lambda w: w[1]).collect()
   best_reg_float = float(best_regularizer[0])

   # Loading yelp_business data
   yelp_business = sc.cassandraTable("bigbang", "yelp_business").\
       select('business_id', 'city', 'full_address', 'latitude', 'longitude', 'name', 'open', 'review_count', 'stars', 'state')\
       .cache()

   # Loading yelp_review data
   yelp_review = sc.cassandraTable("bigbang", "yelp_review").select('user_id','business_id','stars')\
       .map(lambda r: (r['user_id'],r['business_id'],r['stars'])).cache()

   # mapping string user_ids to int values
   user_id_lst = list(set(yelp_review.map(lambda r: r[0]).collect()))
   user_dict = map_string_to_int(user_id_lst)

   # mapping string business_ids to int values
   business_id_lst = list(set(yelp_review.map(lambda r: r[1]).collect()))
   business_dict = map_string_to_int(business_id_lst)

   #Converting the string user_id and business_id in ratings/review data into integer values using dictionaries
   ratings = yelp_review.map(lambda r: (user_dict[r[0]],business_dict[r[1]],r[2])).cache()


   # Train the model with the best parameters found in model_selection.py
   seed = 5L
   model = ALS.train(ratings, best_rank_int, best_iter_int, lambda_=best_reg_float, seed=seed)

   #Preparing distinct list of user_ids for predicting ratings for.
   ratings_df = ratings.toDF()
   ratings_df.registerTempTable('ratingsTable')
   user_distinct_df = sqlContext.sql("""SELECT distinct(_1) as user_id FROM ratingsTable LIMIT 10""")
   user_distinct_df.registerTempTable('userDistinctTable')

   # Preparing distinct list of business_ids for predicting ratings for.
   business_distinct_df = sqlContext.sql("""SELECT distinct(_2) as business_id FROM ratingsTable""")
   business_distinct_df.registerTempTable('businessDistinctTable')

   # Joining list of user_ids and business_ids.
   user_movie_for_pred_df = sqlContext.sql("""SELECT user_id, business_id from userDistinctTable , businessDistinctTable""")
   user_movie_for_pred_rdd = user_movie_for_pred_df.rdd.map(lambda p: (int(p[0]),int(p[1])))

   #Predicting ratings for the user_id and business_id combination retrieved above.
   predictions = model.predictAll(user_movie_for_pred_rdd).map(lambda r: (r[0], r[1], r[2])).cache()

   # mapping int user_ids back to their string values
   user_dict = map_int_to_string(user_dict)

   # mapping int business_ids back to their string values
   business_dict = map_int_to_string(business_dict)

   #Saving output predicted ratings to cassandra table 'user_pred_ratings'
   preds = predictions.map(lambda p: (str(user_dict[p[0]]),str(business_dict[p[1]]), Decimal(p[2]).quantize(decimal.Decimal('1.00'))))
   print(preds.take(5))
   #preds.saveAsTextFile(outdir)
   preds.saveToCassandra('bigbang', 'user_pred_ratings', columns=["user_id", "business_id", "user_pred_rating"], parallelism_level=1000)

if __name__ == "__main__":
    main()