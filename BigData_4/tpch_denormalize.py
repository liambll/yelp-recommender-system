# -*- coding: utf-8 -*-
from pyspark.sql import SQLContext
from pyspark import SparkConf
import sys
import pyspark_cassandra
import uuid

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

def rows_to_list(key_vals, key_col, val_col, list_col):
    """
    Aggregate a DataFrame with key and value columns by joining the values into a list.
 
    >>> df = sqlContext.createDataFrame([{'a':1, 'b':927}, {'a':1, 'b':251}, {'a':1, 'b':396}, {'a':2, 'b':991}])
    >>> df.show()
    +---+---+
    |  a|  b|
    +---+---+
    |  1|927|
    |  1|251|
    |  1|396|
    |  2|991|
    +---+---+
    >>> rows_to_list(df, 'a', 'b', 'b_list')
    +---+---------------+
    |  a|         b_list|
    +---+---------------+
    |  1|[927, 251, 396]|
    |  2|          [991]|
    +---+---------------+
    """
    # this would be a lot easier if the UserDefinedAggregateFunction was available in the Python API.
    def listappend(lst, v):
        lst.append(v)
        return lst
    def listjoin(lst1, lst2):
        lst1.extend(lst2)
        return lst1
 
    assert key_vals.columns == [key_col, val_col], 'key_vals must have two columns: your key_col and val_col'
    key_val_rdd = key_vals.rdd.map(tuple)
    key_list_rdd = key_val_rdd.aggregateByKey([], listappend, listjoin)
    return sqlContext.createDataFrame(key_list_rdd, schema=[key_col, list_col])
    
def main(argv=None):
    if argv is None:
        keyspace = sys.argv[1]
        keyspace2 = sys.argv[2]

    # get order, lineitem and part tables
    order = df_for(keyspace, 'orders')
    order.cache()
    order.registerTempTable('orders')

    lineItem = df_for(keyspace, 'lineitem')
    lineItem.registerTempTable('lineitem')
    
    part = df_for(keyspace, 'part')   
    part.registerTempTable('part')
    
    #join
    order_part = sqlContext.sql("SELECT a.orderkey, c.name " + \
        "FROM orders a JOIN lineitem b ON (a.orderkey = b.orderkey) " + \
        "JOIN part c on (b.partkey = c.partkey)")
    
    order_part2 = rows_to_list(order_part, 'orderkey', 'name', 'part_names')
    order_part2.registerTempTable('order_part')
    
    result = sqlContext.sql("SELECT a.*, b.part_names " + \
        "FROM orders a JOIN order_part b ON (a.orderkey = b.orderkey)")
    
    #Save result to Cassandra
    result.rdd.map(lambda x: (x['orderkey'],x['custkey'],x['orderstatus'], \
        x['totalprice'], x['orderdate'], x['order_priority'], x['clerk'], 
        x['ship_priority'],x['comment'], x['part_names']))\
        .saveToCassandra(keyspace2, 'orders_parts', columns=["orderkey", "custkey", \
        "orderstatus", "totalprice", "orderdate", "order_priority", "clerk", \
        "ship_priority", "comment", "part_names"], batch_size=300, parallelism_level=1000 )         
        
if __name__ == "__main__":
    main()