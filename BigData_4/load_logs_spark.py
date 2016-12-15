# -*- coding: utf-8 -*-

from pyspark import SparkConf
import sys, re
import datetime
import pyspark_cassandra
import uuid

def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        keyspace = sys.argv[2]
    
    # initialize spark cassandra     
    cluster_seeds = ['199.60.17.136', '199.60.17.173']
    conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds))
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

    #Get RDD from input file
    text = sc.textFile(inputs)
    linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")    
    request = text.map(lambda line: linere.split(line)) \
        .filter(lambda lineSplit: len(lineSplit) > 4) \
        .map(lambda lineSplit: (str(uuid.uuid1()), lineSplit[1],datetime.datetime.strptime(lineSplit[2], '%d/%b/%Y:%H:%M:%S').isoformat(),\
            lineSplit[3], int(lineSplit[4])))
    
    #Save result to Cassandra
    request.saveToCassandra(keyspace, 'nasalogs', columns=["id", "host", "datetime", "path", "bytes"], batch_size=300, parallelism_level=1000 )          
        
if __name__ == "__main__":
    main()