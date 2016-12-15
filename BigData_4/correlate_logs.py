# -*- coding: utf-8 -*-

from pyspark import SparkConf
import sys
import pyspark_cassandra
import math

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))
       
def main(argv=None):
    if argv is None:
        keyspace = sys.argv[1]
        output = sys.argv[2]
    
    # initialize spark cassandra     
    cluster_seeds = ['199.60.17.136', '199.60.17.173']
    conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds))
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

    #Get RDD from Cassandra
    request = sc.cassandraTable(keyspace, 'nasalogs', row_format=pyspark_cassandra.RowFormat.DICT) \
        .map(lambda line: (line['host'],(1, line['bytes'])))
    
    request2 = request.reduceByKey(add_tuples) \
        .map(lambda (host,(count, byte)): ("r",(1, count, byte, count*count, byte*byte, count*byte)))
    request3 = request2.reduceByKey(add_tuples)
    r = request3.map(lambda (key, sixsum): \
        (key, (sixsum[0]*sixsum[5] - sixsum[1]*sixsum[2])/(math.sqrt(sixsum[0]*sixsum[3]-sixsum[1]*sixsum[1]) \
        * math.sqrt(sixsum[0]*sixsum[4]-sixsum[2]*sixsum[2])))) 
    r2 = r.map(lambda (key, value): (key+'2',value*value))

    #Save result
    outdata= r.union(r2)
    # I have thought about combining this data onto one node. It is safe since the output data is small.
    outdata.coalesce(1).saveAsTextFile(output)
    
if __name__ == "__main__":
    main()