from pyspark import SparkConf, SparkContext
import sys
from pyspark.streaming import StreamingContext
import datetime

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))
    
def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        output = sys.argv[2]
        if len(sys.argv) >3:
            batch_size = int(sys.argv[3])
        else:
            batch_size = 5
    
    # Initialize Spark Stream    
    conf = SparkConf().setAppName('streaming_points')
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, batch_size) #batch of 5 seconds
        
    lines = ssc.socketTextStream('cmpt732.csil.sfu.ca', int(inputs)) # read stream
    
    def regression(rdd):
        if (rdd.isEmpty() == False): #Only process non-empty stream
            data = rdd.map(lambda x: x.split(" ")).map(lambda (x, y): (float(x), float(y))). \
                map(lambda (x,y):(1, x*y, x*x, x, y)).reduce(add_tuples)
            
            # slope m
            m = (data[1]/data[0] - data[3]*data[4]/data[0]/data[0]) / \
            (data[2]/data[0] - data[3]*data[3]/data[0]/data[0])
            # intercept b
            b = data[4]/data[0] - m*data[3]/data[0]
            
            # save output to a file
            rdd = sc.parallelize([(m, b)], numSlices=1)
            rdd.saveAsTextFile(output + '/' + datetime.datetime.now().isoformat().replace(':', '-'))
    
    lines.foreachRDD(regression) # process stream
    ssc.start()
    ssc.awaitTermination(timeout=300) # listen for 5 minutes
    #ssc.stop()
if __name__ == "__main__":
    main()
    