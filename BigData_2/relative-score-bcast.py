from pyspark import SparkConf, SparkContext
import sys
import json

inputs = sys.argv[1]
output = sys.argv[2]

def add_pairs((x1,y1),(x2,y2)):
    return (x1+x2,y1+y2)

 
conf = SparkConf().setAppName('relative-score-broadcast')
sc = SparkContext(conf=conf)
 
text = sc.textFile(inputs)
reddit_json = text.map(lambda x: json.loads(x))
subreddit_json = reddit_json.map(lambda x: (x['subreddit'],(float(x['score']),1)))

subreddit = subreddit_json.reduceByKey(add_pairs)
subreddit_avg = subreddit.map(lambda (sub,(total,count)): (sub,1.0*total/count)).filter(lambda (sub, avg): avg>0)
avg = sc.broadcast(dict(subreddit_avg.collect()))

commentbysub_withAvg = reddit_json.map(lambda x: (x['subreddit'], (x['author'], float(x['score'])))) \
    .map(lambda (subreddit,(author,score)): (1.0*score/avg.value[subreddit],author)) \
    .sortByKey(False)

# Output result
outdata = commentbysub_withAvg.map(lambda (w,c): u"%f %s" % (w, c))
outdata.saveAsTextFile(output)