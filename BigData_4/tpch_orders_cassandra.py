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
    # Collect() is safe since the number of parts is probably small.
    order = rdd_for(keyspace, 'orders').where('orderkey in ' + orderList) \
        .select('orderkey','totalprice')
    dict_order = order.collect()
    
    lineitem = rdd_for(keyspace, 'lineitem').where('orderkey in ' + orderList) \
        .select('orderkey','partkey')
    dict_lineitem = lineitem.collect()
        
    partList = "("
    for aLineItem in dict_lineitem:
        partList = partList + str(aLineItem.get('partkey')) + ","
    partList = partList[:-1] + ")"
        
    part = rdd_for(keyspace, 'part').where('partkey in ' + partList) \
        .select('partkey','name')
    dict_part = part.collect() 
    
    #join
    for aLineItem in dict_lineitem:
        partkey = aLineItem.get('partkey')
        for aPart in dict_part:
            if aPart.get('partkey') == partkey:
                aLineItem['name'] = aPart.get('name')
                break
        
    for aOrder in dict_order:
        orderkey = aOrder.get('orderkey')
        part_names = ""
        for aLineItem in dict_lineitem:
            if aLineItem.get('orderkey') == orderkey:
                part_names = part_names + aLineItem.get('name') + ", "
        aOrder['part_names'] = part_names[:-2]
    

    # Result
    result = sc.parallelize(dict_order)
    outdata = result.map(lambda x: (x['orderkey'],x['totalprice'],x['part_names'])) \
        .map(lambda (order, price, part): "Order #%s $%.2f: %s" % (order, price, part))
    # I have thought about combining this data onto one node. It is safe since the output data is small.
    outdata.coalesce(1).saveAsTextFile(output)
        
if __name__ == "__main__":
    main()



