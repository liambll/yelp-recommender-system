# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        output = sys.argv[2]

    conf = SparkConf().setAppName('weather-temp')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    
    #define schema for input
    schema = StructType([
        StructField('station', StringType(), False),
        StructField('date', StringType(), False),
        StructField('observation', StringType(), False),
        StructField('value', IntegerType(), False),
        StructField('field1', StringType(), False),
        StructField('field2', StringType(), False),
    ])
    
    #read input into a dataframe
    df = sqlContext.read.format('com.databricks.spark.csv') \
        .options(header='true').load(inputs, schema = schema)  
    
    #filter out record with quality issue 
    df = df.filter(df['field2'] == '')
    df.cache()    
    
    #get max temperature
    df_max = df.filter(df['observation']=='TMAX') \
        .withColumnRenamed('value', 'max') \
        .select('station','date','max')

    #get min temperature
    df_min = df.filter(df['observation']=='TMIN') \
        .withColumnRenamed('value', 'min') \
        .select('station','date','min')
    df.unpersist()

    #derive temperature range
    df_max_min = df_max.join(df_min,['station','date'],how='inner')
    df_range = df_max_min.withColumn('range', df_max_min['max'] - df_max_min['min'])
    df_range.cache()
    
    #get maximum temperature range
    df_range2 = df_range.groupby('date').max('range')
    df_range3 = df_range2.join(df_range,(df_range['date']==df_range2['date']) \
        & ( df_range['range']==df_range2['max(range)']),how='inner') \
        .drop(df_range['date'])
    df_range4 = df_range3.select('date','station','max(range)') \
        .withColumnRenamed('max(range)', 'range').sort('date')

    # Output result
    # Each entry is one date, so it is small enough to use coalesce(1)
    outdata = df_range4.rdd.map(lambda x: "%s %s %s" % (x[0], x[1], x[2]))
    outdata.coalesce(1).saveAsTextFile(output)


if __name__ == "__main__":
    main()
    
    
    
        