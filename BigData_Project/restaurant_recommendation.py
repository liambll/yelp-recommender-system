from cassandra.cluster import Cluster
from pyspark import SparkConf, SparkContext, SQLContext, HiveContext
import pyspark.sql.functions as func
from pyspark.sql.functions import *
import pyspark_cassandra
from pyspark.sql import Window
import uuid

def main():
   cluster_seeds = ['199.60.17.136', '199.60.17.173']
   conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds))
   sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
   sqlContext = HiveContext(sc)

   # Loading yelp_business data
   yelp_business = sc.cassandraTable("bigbang", "yelp_business").\
       select('business_id', 'city', 'full_address', 'latitude', 'longitude', 'name', 'open', 'review_count', 'stars', 'state')\
       .toDF()
   yelp_business.registerTempTable('business')

   # Loading yelp_user data
   yelp_user = sc.cassandraTable("bigbang", "yelp_user").select('user_id','name','yelping_since').toDF()
   yelp_user.registerTempTable('user')

   # Loading user_pred_ratings data
   user_pred_ratings = sc.cassandraTable("bigbang", "user_pred_ratings").\
      select('user_id', 'business_id', 'user_pred_rating').toDF()
   user_pred_ratings.registerTempTable('userPredRatings')

   # Loading checkin data
   business_checkin = sc.cassandraTable("bigbang", "yelp_business_checkin")\
       .select('business_id', 'checkin','day','hour').toDF()
   business_checkin.registerTempTable('businessCheckin')

   # Joining user, business and ratings to get denormalized data
   user_res_pred_denorm_df = sqlContext.sql(
      """SELECT a.user_id as user_id, b.city as city, a.business_id as business_id, b.full_address as full_address,
       b.latitude as latitude, b.longitude as longitude, b.name as business_name, b.open as open,
       b.review_count as review_count, b.stars as stars, b.state as state, c.name as user_name,
       a.user_pred_rating as user_pred_rating
       from userPredRatings a inner join business b
       on a.business_id = b.business_id
       inner join user c
       on a.user_id = c.user_id""")

   # user_res_pred_denorm_df.registerTempTable('userResPredDenorm')
   #
   # recommendations_top10_df = sqlContext.sql(
   #    """SELECT user_id, city, business_id, full_address, latitude, longitude, business_name, open, review_count,
   #    stars, state, user_name, user_pred_rating FROM
   #     (SELECT user_id, city, business_id, full_address, latitude, longitude, business_name, open, review_count, stars,
   #     state, user_name, user_pred_rating, row_number() OVER (PARTITION BY user_id, state ORDER BY user_pred_rating desc) rn
   #     FROM userResPredDenorm) temp
   #     WHERE temp.rn <= 20""")

   #Retrieving top 20 restaurants per user per state using Window Analytic Functions
   windowSpec = Window.partitionBy(user_res_pred_denorm_df['user_id'],user_res_pred_denorm_df['state']).\
       orderBy(user_res_pred_denorm_df['user_pred_rating'].desc())

   rn_business = (func.rowNumber().over(windowSpec))
   recommendations_ranked_df = user_res_pred_denorm_df.select(
       user_res_pred_denorm_df['user_id'],
       user_res_pred_denorm_df['city'],
       user_res_pred_denorm_df['business_id'],
       user_res_pred_denorm_df['full_address'],
       user_res_pred_denorm_df['latitude'],
       user_res_pred_denorm_df['longitude'],
       user_res_pred_denorm_df['business_name'],
       user_res_pred_denorm_df['open'],
       user_res_pred_denorm_df['review_count'],
       user_res_pred_denorm_df['stars'],
       user_res_pred_denorm_df['state'],
       user_res_pred_denorm_df['user_name'],
       user_res_pred_denorm_df['user_pred_rating'],
       rn_business.alias("rn"))

   recommendations_top20_df = recommendations_ranked_df.filter(recommendations_ranked_df["rn"] <= 20).select(
       recommendations_ranked_df['user_id'],
       recommendations_ranked_df['city'],
       recommendations_ranked_df['business_id'],
       recommendations_ranked_df['full_address'],
       recommendations_ranked_df['latitude'],
       recommendations_ranked_df['longitude'],
       recommendations_ranked_df['business_name'],
       recommendations_ranked_df['open'],
       recommendations_ranked_df['review_count'],
       recommendations_ranked_df['stars'],
       recommendations_ranked_df['state'],
       recommendations_ranked_df['user_name'],
       recommendations_ranked_df['user_pred_rating'])
   recommendations_top20_df.registerTempTable('recommendations')

   recommendations_top20_rdd = recommendations_top20_df.rdd.map(lambda p: (str(uuid.uuid1()), p['user_id'],p['city'],p['business_id'],
                                                                           p['full_address'],p['latitude'],p['longitude'],
                                                                           p['business_name'],p['open'],p['review_count'],
                                                                           p['stars'],p['state'],p['user_name'],
                                                                           p['user_pred_rating']))
   # Loading the recommendations table in cassandra
   recommendations_top20_rdd.saveToCassandra('bigbang', 'recommendations',
                                              columns=["uid", "user_id", "business_city", "business_id", "business_full_address", "business_latitude","business_longitude",
                                                       "business_name","business_open", "business_review_count", "business_stars", "business_state", "user_name",
                                                       "user_pred_rating"] , parallelism_level=1000)



   # Joining recommendations and checkin data
   user_res_checkin_denorm_df = sqlContext.sql(
      """SELECT a.user_id as user_id, a.city as business_city, a.business_id as business_id,
       a.full_address as business_full_address, a.latitude as business_latitude,
       a.longitude as business_longitude, a.business_name as business_name, a.open as business_open,
       a.review_count as business_review_count, a.stars as business_stars,
       a.state as business_state, a.user_name as user_name,
       a.user_pred_rating as user_pred_rating, b.day as day, b.hour as hour, b.checkin as checkin
       from recommendations a inner join businessCheckin b
       on a.business_id = b.business_id""")

   recommendations_checkin_rdd = user_res_checkin_denorm_df.rdd.map(lambda p: (str(uuid.uuid1()),p['user_id'],p['business_city'],p['business_id'],
                                                                           p['business_full_address'],p['business_latitude'],
                                                                           p['business_longitude'], p['business_name'],
                                                                           p['business_open'],p['business_review_count'],
                                                                           p['business_stars'],p['business_state'],p['user_name'],
                                                                           p['user_pred_rating'], p['day'], p['hour'],
                                                                           p['checkin']))
   # Loading the recommendations_checkin table in cassandra
   recommendations_checkin_rdd.saveToCassandra('bigbang', 'recommendations_checkin',
                                              columns=["uid", "user_id", "business_city", "business_id", "business_full_address", "business_latitude","business_longitude",
                                                       "business_name","business_open", "business_review_count", "business_stars", "business_state", "user_name",
                                                       "user_pred_rating","day","hour","checkin"] , parallelism_level=1000)



if __name__ == "__main__":
    main()