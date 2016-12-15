from pyspark import SparkConf, SparkContext
import sys
import json

inputs = sys.argv[1]
output = sys.argv[2]

def add_pairs((x1,y1),(x2,y2)):
    return (x1+x2,y1+y2)

 
conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
 
text = sc.textFile(inputs)
reddit_json = text.map(lambda x: json.loads(x))
subreddit_json = reddit_json.map(lambda x: (x['subreddit'],(float(x['score']),1)))

subreddit = subreddit_json.reduceByKey(add_pairs)

subreddit2 = subreddit.map(lambda (sub,(total,count)): (sub,1.0*total/count))

# Output result to JSON text
outdata = subreddit2.map(lambda x: json.dumps(x))
# The number of output is small enough
outdata.coalesce(1).saveAsTextFile(output)