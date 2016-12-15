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

def df_for(keyspace, table, split_size=None):
    df = sqlContext.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
    #df.registerTempTable(table)
    return df

def append((price1, part1),(price2, part2)):
    price = price1
    part = part1 + ", " + part2
    return (price, part)

def main(argv=None):
    if argv is None:
        keyspace = sys.argv[1]
        output = sys.argv[2]
        orderkeys = sys.argv[3:]
    
    orderList = "("
    for orderkey in orderkeys:
        orderList = orderList + orderkey + ","
    orderList = orderList[:-1] + ")"

    # get dataframes for order, lineitem and part
    order = df_for(keyspace, 'orders').filter('orderkey in '+orderList)
    order.registerTempTable('orders')

    lineItem = df_for(keyspace, 'lineitem')
    lineItem.registerTempTable('lineitem')
    
    part = df_for(keyspace, 'part')   
    part.registerTempTable('part')

    #join
    result = sqlContext.sql("SELECT a.orderkey, a.totalprice, c.name " + \
        "FROM orders a JOIN lineitem b ON (a.orderkey = b.orderkey) " + \
        "JOIN part c on (b.partkey = c.partkey)")

    # Result
    outdata = result.rdd.map(lambda x: (x[0],(x[1],x[2]))) \
        .reduceByKey(append) \
        .map(lambda (order, (price, key)): "Order #%s $%.2f: %s" % (order, price, key))
    # I have thought about combining this data onto one node. It is safe since the output data is small.
    outdata.coalesce(1).saveAsTextFile(output)
        
if __name__ == "__main__":
    main()

