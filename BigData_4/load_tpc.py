# Can be run with:
# spark-submit --master=yarn-client --packages TargetHolding/pyspark-cassandra:0.3.5 a4/tpc_ingest.py /courses/732/a4-tpch-2 keyspace
 
import sys, os
import decimal
import datetime
 
cluster_seeds = ['199.60.17.136', '199.60.17.173']
 
customer_types = [('custkey', 'int'), ('name', 'text'), ('address', 'text'), ('nationkey', 'int'), ('phone', 'text'), ('acctbal', 'decimal'), ('mktsegment', 'text'), ('comment', 'text')]
lineitem_types = [('orderkey', 'int'), ('partkey', 'int'), ('suppkey', 'int'), ('linenumber', 'int'), ('quantity', 'int'), ('extendedprice', 'decimal'), ('discount', 'decimal'), ('tax', 'decimal'), ('returnflag', 'text'), ('linestatus', 'text'), ('shipdate', 'date'), ('commitdate', 'date'), ('receiptdate', 'date'), ('shipinstruct', 'text'), ('shipmode', 'text'), ('comment', 'text')]
nation_types = [('nationkey', 'int'), ('name', 'text'), ('regionkey', 'int'), ('comment', 'text')]
orders_types = [('orderkey', 'int'), ('custkey', 'int'), ('orderstatus', 'text'), ('totalprice', 'decimal'), ('orderdate', 'date'), ('order_priority', 'text'), ('clerk', 'text'), ('ship_priority', 'int'), ('comment', 'text')]
partsupp_types = [('partkey', 'int'), ('suppkey', 'int'), ('availqty', 'int'), ('supplycost', 'decimal'), ('comment', 'text')]
part_types = [('partkey', 'int'), ('name', 'text'), ('mfgr', 'text'), ('brand', 'text'), ('type', 'text'), ('size', 'int'), ('container', 'text'), ('retailprice', 'decimal'), ('comment', 'text')]
region_types = [('regionkey', 'int'), ('name', 'text'), ('comment', 'text')]
supplier_types = [('suppkey', 'int'), ('name', 'text'), ('address', 'text'), ('nationkey', 'int'), ('phone', 'text'), ('acctbal', 'decimal'), ('comment', 'text')]
 
 
tables = [ # table name, field/type list, primary key
    ('customer', customer_types, 'custkey'),
    ('nation', nation_types, 'nationkey'),
    ('orders', orders_types, 'orderkey'),
    ('partsupp', partsupp_types, 'partkey, suppkey'),
    ('part', part_types, 'partkey'),
    ('region', region_types, 'regionkey'),
    ('supplier', supplier_types, 'suppkey'),
    ('lineitem', lineitem_types, 'linenumber, orderkey'),
]
 
 
def fix_type(d):
    v, (_,t) = d
    if t == 'int':
        return int(v)
    elif t == 'decimal':
        return decimal.Decimal(v)
    elif t == 'text':
        return v
    elif t == 'date':
        # storing dates as strings: pyspark-cassandra doesn't seem to support the date type
        return datetime.datetime.strptime(v, '%Y-%m-%d').date().isoformat()
    else:
        raise ValueError, t
 
 
def create_tables():
    for tbl, types, primarykey  in tables:
        fields = ',\n  '.join('%s %s' % (f, t if t!='date' else 'text') for f,t in types)
        print('CREATE TABLE %s (\n  %s,\n  PRIMARY KEY (%s)\n);' % (tbl, fields, primarykey))
 
 
def drop_tables():
    for tbl, types, primarykey  in tables:
        print('DROP TABLE %s;' % (tbl,))
 
 
def to_cassandra_data(line, types):
    fdata = zip(line.split('|'), types)
    data = map(fix_type, fdata)
    cdata = dict((f,v) for v,(f,t) in zip(data, types))
    return cdata
 
 
def read_table(keyspace, sc, input_dir, tbl, types):
    infile = os.path.join(input_dir, tbl+'.tbl.gz')
    cdata = sc.textFile(infile, minPartitions=100).map(lambda line: to_cassandra_data(line, types)).setName(tbl)
    cdata.saveToCassandra(keyspace, tbl, parallelism_level=16)
 
 
def read_tables(sc, input_dir, keyspace):
    for tbl, types, primarykey in tables:
        read_table(keyspace, sc, input_dir, tbl, types)
 
 
if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
 
    from pyspark import SparkConf
    import pyspark_cassandra
    conf = SparkConf().setAppName('TPC ingest') \
        .set("spark.cassandra.connection.host", ','.join(cluster_seeds)) \
        .set('spark.dynamicAllocation.maxExecutors', 20)
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
 
    read_tables(sc, input_dir, keyspace)