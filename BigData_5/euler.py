from pyspark import SparkConf, SparkContext
import sys
import operator, random

def total_count(noIter):
    total_allIter = 0
    rand = random.Random()
    for i in range(noIter):        
        count = 0
        total = 0
        while (total < 1):
            total = total + rand.random()
            count = count + 1
        total_allIter = total_allIter + count
    return total_allIter
    
def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        if (len(sys.argv) > 2):
            noPartition = int(sys.argv[2])
        else:
            noPartition = 100
    
    # Initialize Spark Stream    
    conf = SparkConf().setAppName('euler_spark')
    sc = SparkContext(conf=conf)
    
    # Load ratings data
    iterations = int(inputs)
    
    max_size = iterations//noPartition
    array = [max_size] * noPartition
        
    rdd = sc.parallelize(array)
    total = rdd.map(lambda x: total_count(x)).reduce(operator.add)
    
    # Print Output
    print "=========== e value: %0.8f" %(total/float(max_size*noPartition))

if __name__ == "__main__":
    main()