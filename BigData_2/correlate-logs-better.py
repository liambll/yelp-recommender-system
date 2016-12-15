from pyspark import SparkConf, SparkContext
import sys, operator
import re, math

inputs = sys.argv[1]
output = sys.argv[2]
 
def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))
    
conf = SparkConf().setAppName('nasa logs better')
sc = SparkContext(conf=conf)
 
text = sc.textFile(inputs)

linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")

request = text.map(lambda line: linere.split(line)).filter(lambda lineSplit: len(lineSplit) > 4).map(lambda lineSplit: (lineSplit[1],(1,float(lineSplit[4]))))
request2 = request.reduceByKey(add_tuples)

# I have thought about combining this data onto one node. The data is a list of only 3 values
avg = request2.map(lambda (host,(count, byte)): (1, count, byte)).reduce(add_tuples)
avg_count = sc.broadcast(avg[1]/avg[0])
avg_byte = sc.broadcast(avg[2]/avg[0])

request3 = request2.map(lambda (host,(count, byte)): (count-avg_count.value, byte-avg_byte.value)).cache()

sum_countbyte = request3.map(lambda (count, byte): count*byte).reduce(operator.add)
sum_count2 = request3.map(lambda (count,byte): count*count).reduce(operator.add)
sum_byte2 = request3.map(lambda (count,byte): byte*byte).reduce(operator.add)

r = sum_countbyte/math.sqrt(sum_count2*sum_byte2)
r2 = r*r

# Result
outdata= sc.parallelize([('r',r),('r2',r2)])
# I have thought about combining this data onto one node. It is safe since the output data is small.
outdata.coalesce(1).saveAsTextFile(output)