from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SQLContext

def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        output = sys.argv[2]
        
    conf = SparkConf().setAppName('reddit_average_sql')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    comments = sqlContext.read.json(inputs)
    averages = comments.select('subreddit', 'score').groupby('subreddit').avg()

    # Output result to JSON text
    # The number of output is small enough
    averages.coalesce(1).write.save(output, format='json', mode='overwrite')

if __name__ == "__main__":
    main()