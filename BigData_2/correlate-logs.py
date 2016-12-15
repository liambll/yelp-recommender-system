from pyspark import SparkConf, SparkContext
import sys
import re, math

inputs = sys.argv[1]
output = sys.argv[2]
 
def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))
    
conf = SparkConf().setAppName('nasa logs')
sc = SparkContext(conf=conf)
 
text = sc.textFile(inputs)

linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")

request = text.map(lambda line: linere.split(line)) \
    .filter(lambda lineSplit: len(lineSplit) > 4) \
    .map(lambda lineSplit: (lineSplit[1],(1,float(lineSplit[4]))))

request2 = request.reduceByKey(add_tuples).map(lambda (host,(count, byte)): ("r",(1, count, byte, count*count, byte*byte, count*byte)))
request3 = request2.reduceByKey(add_tuples)
r = request3.map(lambda (key, sixsum): (key, (sixsum[0]*sixsum[5] - sixsum[1]*sixsum[2])/(math.sqrt(sixsum[0]*sixsum[3]-sixsum[1]*sixsum[1]) * math.sqrt(sixsum[0]*sixsum[4]-sixsum[2]*sixsum[2])))) 
r2 = r.map(lambda (key, value): (key+'2',value*value))

# Result
outdata= r.union(r2)
# I have thought about combining this data onto one node. It is safe since the output data is small.
outdata.coalesce(1).saveAsTextFile(output)