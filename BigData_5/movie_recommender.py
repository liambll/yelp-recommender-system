from pyspark import SparkConf, SparkContext
import sys, os
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import levenshtein
from pyspark.sql import SQLContext
import itertools

def list_to_string(aList):
    aString = ""
    for item in aList:
        aString = aString + str(item) + " "
    aString = aString[:-1]
    return aString

def find_min((id1, title1, distance1), (id2, title2, distance2)):
    if distance1 < distance2:
        return (id1, title1, distance1)
    else:
        return (id2, title2, distance2)
    
def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        user = sys.argv[2]
        output = sys.argv[3]
    
    # Initialize Spark
    os.environ['PYSPARK_PYTHON'] = "python2"
    os.environ['PYTHONPATH'] = ':'.join(sys.path)
    
    conf = SparkConf().setAppName('movie_recommendation')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    
    # Load ratings data
    ratings_data = sc.textFile(inputs+"/ratings.dat")
    ratings = ratings_data.map(lambda l: l.split('::'))\
        .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
    
    # Load movies data
    movies_data = sc.textFile(inputs+"/movies.dat")
    movies = movies_data.map(lambda l: l.split('::'))\
        .map(lambda l: (int(l[0]), l[1].encode('utf-8').strip()))
    movies.cache()
    
    # Load user rating
    user_rate_data = sc.textFile(user)
    user_rate = user_rate_data.map(lambda l: l.split(' '))\
        .map(lambda l: (int(l[0]), list_to_string(l[1:]).encode('utf-8').strip()))
    user_rate_list = user_rate.collect()
    
    # Match movies name input by user to movie id in movies data
    user_rate_list_2 = []
    for item in user_rate_list:
        user_title = sc.broadcast(item[1])
        df_movie = sqlContext.createDataFrame(movies,['movieId','title'])
        df_movie.registerTempTable('movies')
        df_movie2 = sqlContext.sql("SELECT *, \"" + user_title.value + "\" as user_title FROM movies") \
            .select('movieId','title',levenshtein('title', 'user_title').alias('distance'))
        
        movie_id = df_movie2.rdd.map(lambda x: (x['movieId'], x['title'], x['distance'])) \
            .reduce(find_min)
        user_rate_list_2.append([0, movie_id[0], item[0]])
    user_rate_rdd = sc.parallelize(user_rate_list_2) \
        .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
    ratings_all = user_rate_rdd.union(ratings)
    ratings_all.cache()

    # Build the recommendation model using Alternating Least Squares
    ranks = [8, 20]
    numIters = [10, 20]
    bestModel = None
    bestMSE = float("inf")
    bestRank = 0
    bestNumIter = -1

    for rank, numIter in itertools.product(ranks, numIters):
        #Train Model
        model = ALS.train(ratings_all, rank, numIter)
        
        #Evaluate MSE
        testdata = ratings.map(lambda x: (x[0], x[1]))
        predictions = model.predictAll(testdata).map(lambda x: ((x[0], x[1]), x[2]))
        ratesAndPreds = ratings.map(lambda x: ((x[0], x[1]), x[2])).join(predictions)
        MSE = ratesAndPreds.map(lambda x: (x[1][0] - x[1][1])**2).mean()
        #print "MSE = %f for the model trained with " % MSE + \
        #      "rank = %d, and numIter = %d." % (rank, numIter)
        if (MSE < bestMSE):
            bestModel = model
            bestMSE = MSE
            bestRank = rank
            bestNumIter = numIter
   
    
    # Generate list recommended movies in descending order
    rated_movies = set([x[1] for x in user_rate_list_2])
    candidates = movies.filter(lambda x: x[0] not in rated_movies) #only recommend movie not rated yet
    predictions = bestModel.predictAll(candidates.map(lambda x: (0, x[0]))) \
        .map(lambda x: (x[1],x[2])).join(movies) \
        .sortBy(lambda (movieid,(score,title)): score, ascending=False)

    #write recommendation
    outdata = predictions.map(lambda (movieId, (score, title)): "Movies: %s - Score:%0.2f" % (title, score))
    outdata.saveAsTextFile(output)
    
if __name__ == "__main__":
    main()