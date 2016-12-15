from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        output = sys.argv[2]
        
    conf = SparkConf().setAppName('reddit_average_sql')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    schema = StructType([
    StructField('archived', StringType(), False),
    StructField('author', StringType(), False),
    StructField('author_flair_css_class', StringType(), False),
    StructField('author_flair_text', StringType(), False),
    StructField('body', StringType(), False),
    StructField('controversiality', IntegerType(), False),
    StructField('created_utc', StringType(), False),
    StructField('distinguished', StringType(), False),
    StructField('downs', IntegerType(), False),
    StructField('edited', StringType(), False),
    StructField('gilded', IntegerType(), False),
    StructField('id', StringType(), False),
    StructField('link_id', StringType(), False),
    StructField('name', StringType(), False),
    StructField('parent_id', StringType(), False),
    StructField('retrieved_on', StringType(), False),
    StructField('score', IntegerType(), False),
    StructField('score_hidden', StringType(), False),
    StructField('subreddit', StringType(), False),
    StructField('ups', IntegerType(), False),
    ])

    comments = sqlContext.read.json(inputs,schema)
    averages = comments.select('subreddit', 'score').groupby('subreddit').avg()

    # Output result to JSON text
    # The number of output is small enough
    averages.coalesce(1).write.save(output, format='json', mode='overwrite')

def reddit_average_nostruct(argv=None):
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