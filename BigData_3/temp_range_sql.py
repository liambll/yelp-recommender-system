# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        output = sys.argv[2]

    conf = SparkConf().setAppName('weather-temp-sql')
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
        .options(header='true') \
        .load(inputs, schema = schema) 
        
    #filter out record with quality issue
    df = df.filter(df['field2'] == '')
    sqlContext.registerDataFrameAsTable(df, 'temperature')

    #get max and min temperature    
    sqlContext.cacheTable('temperature')
    df_max = sqlContext.sql('SELECT date, station, value AS max ' \
        + 'FROM temperature WHERE observation="TMAX"')
    df_min = sqlContext.sql('SELECT date, station, value AS min ' \
        + 'FROM temperature WHERE observation="TMIN"')
    sqlContext.uncacheTable('temperature')

    #get temperature range
    sqlContext.registerDataFrameAsTable(df_max, 'tb_max')
    sqlContext.registerDataFrameAsTable(df_min, 'tb_min')
    df_max_min = sqlContext.sql('SELECT tb_max.date, tb_max.station, max, min, ' \
        + '(max-min) AS range FROM tb_max inner join tb_min ' \
        + 'on tb_max.date=tb_min.date and tb_max.station=tb_min.station')
    sqlContext.registerDataFrameAsTable(df_max_min, 'tb_max_min')
    sqlContext.cacheTable('tb_max_min')
    
    #get max range
    df_range = sqlContext.sql('SELECT date, max(range) AS range ' \
        + 'from tb_max_min GROUP BY date')
    sqlContext.registerDataFrameAsTable(df_range, 'tb_range')
    df_range2 = sqlContext.sql('SELECT tb_range.date, station, tb_range.range ' \
        + 'FROM tb_range inner join tb_max_min ' \
        + 'on tb_range.date=tb_max_min.date and tb_range.range = tb_max_min.range ' \
        + 'ORDER BY date')

    # Result
    # Each entry is one date, so it is small enough to use coalesce(1)
    outdata = df_range2.rdd.map(lambda x: "%s %s %s" % (x[0], x[1], x[2]))
    outdata.coalesce(1).saveAsTextFile(output)


if __name__ == "__main__":
    main()
    
    
    
        