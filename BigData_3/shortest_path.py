# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        output = sys.argv[2]
        source = sys.argv[3]
        target = sys.argv[4]

    conf = SparkConf().setAppName('shortest-path')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    
    #read text input and store graph edges in a dataframe
    text = sc.textFile(inputs) 
    edges = text.map(lambda line: line.split(':')) \
        .filter(lambda lineSplit: len(lineSplit) > 1) \
        .map(lambda lineSplit: (lineSplit[0],lineSplit[1].split())) \
        .flatMapValues(lambda x: x)
    schema = StructType([
        StructField('from', StringType(), False),
        StructField('to', StringType(), False)
    ])
    edges = sqlContext.createDataFrame(edges,schema)
    edges.cache()
    
    
    # Check if source node exists
    if edges[edges['from']==source].count() == 0:
        outdata = ["Source not found"]
    else:    
        #initialize known path dataframe
        schema2 = StructType([
            StructField('node', StringType(), False),
            StructField('source', StringType(), False),
            StructField('distance', IntegerType(), False)
        ])
        knownpaths = sc.parallelize([[source,'No source',0]])
        knownpaths = sqlContext.createDataFrame(knownpaths,schema2)
        knownpaths.cache()
    
        schema3 = StructType([
            StructField('node', StringType(), False)
        ])
        tovisit = sc.parallelize([[source]])
        tovisit = sqlContext.createDataFrame(tovisit,schema3)
    
        #iterate through edges with limit of length 6
        for i in range(6):
            # get neighbours of node in tovisit
            neighbours = edges.join(tovisit, edges['from']==tovisit['node'], 'inner') \
                .drop('node')
            neighbours.cache()
        
            # get new paths for each neighbours
            newpaths = knownpaths.join(neighbours, knownpaths['node']==neighbours['from'], 'inner')
            newpaths =  newpaths.withColumn('new_dist', newpaths['distance']+1) \
                .select('to','from','new_dist')

            # union new paths and known paths     
            knownpaths = knownpaths.unionAll(newpaths).dropDuplicates()
            knownpaths.cache()

            # keep only path with minimum distance
            minpaths = knownpaths.groupby('node').min('distance') \
                .withColumnRenamed('node','min_node')   
            knownpaths2 = minpaths.join(knownpaths, (minpaths['min_node']==knownpaths['node']) \
                & (minpaths['min(distance)']==knownpaths['distance']), 'inner') \
                .drop('min_node').drop('min(distance)')
        
            knownpaths = knownpaths2
            knownpaths.cache()
            #the list of nodes to be visited next iteration
            tovisit = neighbours.select('to').withColumnRenamed('to','node')
        
            # output result in each iteration by simply writing the Row objects
            # Since the no. of entry is roughly the number of nodes, it is small enough to do coalesce      
            outdata = knownpaths.rdd.map(lambda x: "node %s: source %s, distance %i" \
                % (x[0], x[1], x[2]))
            outdata.coalesce(1).saveAsTextFile(output + '/iter-' + str(i))
        
            # Stop finding path if found the target
            if tovisit[tovisit['node']==target].count() > 0:
                break
    
        # Check if target is found
        if knownpaths[knownpaths['node']==target].count() == 0:
            outdata = ["Target not found"]
        else: 
            # Trace path from target back to source
            outdata=[]
            path = target
            outdata.insert(0,path)
            while (path <> source):
                path = knownpaths[knownpaths['node']==path].select('source').first()[0]
                outdata.insert(0,path)
        
    # Since the no. of entry is roughly the number of nodes, it is small enough to do coalesce      
    outdata = sc.parallelize(outdata)
    outdata.coalesce(1).saveAsTextFile(output + '/path')

if __name__ == "__main__":
    main()