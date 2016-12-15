# -*- coding: utf-8 -*-
from pyspark.sql import SQLContext
from pyspark import SparkConf
import sys
import pyspark_cassandra

# initialize spark cassandra      
cluster_seeds = ['199.60.17.136', '199.60.17.173']
conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
    .set('spark.dynamicAllocation.maxExecutors', 20)
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)

def append((price1, part1),(price2, part2)):
    price = price1
    part = part1 + ", " + part2
    return (price, part)
    
def rdd_for(keyspace, table, split_size=None):
    rdd = sc.cassandraTable(keyspace, table, split_size=split_size,
        row_format=pyspark_cassandra.RowFormat.DICT).setName(table)
    return rdd

def list_to_string(aList):
    aString = ""
    for item in aList:
        aString = aString + str(item) + ", "
    aString = aString[:-2]
    return aString
    
def main(argv=None):
    if argv is None:
        keyspace = sys.argv[1]
        output = sys.argv[2]
        orderkeys = sys.argv[3:]
        
    orderList = "("
    for orderkey in orderkeys:
        orderList = orderList + str(orderkey) + ","
    orderList = orderList[:-1] + ")"

    # get order, lineitem and part tables
    order = rdd_for(keyspace, 'orders_parts').where('orderkey in ' + orderList)   
    result = order.select('orderkey','totalprice','part_names')
    
    # Result
    outdata = result.map(lambda x: (x['orderkey'],x['totalprice'],list(x['part_names']))) \
        .map(lambda (order, price, part): "Order #%s $%0.2f: %s" % (order, price, list_to_string(part)))
    # I have thought about combining this data onto one node. It is safe since the output data is small.
    outdata.coalesce(1).saveAsTextFile(output)
        
if __name__ == "__main__":
    main()

