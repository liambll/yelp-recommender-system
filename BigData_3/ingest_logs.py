# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
import sys
import re, datetime
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        output = sys.argv[2]

    conf = SparkConf().setAppName('nasa-log-ingest')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    text = sc.textFile(inputs) 

    linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")

    request = text.map(lambda line: linere.split(line)).filter(lambda lineSplit: len(lineSplit) > 4) \
        .map(lambda lineSplit: Row(hostname=lineSplit[1],timestamp=datetime.datetime.strptime(lineSplit[2], '%d/%b/%Y:%H:%M:%S'),path=lineSplit[3],size=float(lineSplit[4])))
    
    schema = StructType([
    StructField('hostname', StringType(), False),
    StructField('path', StringType(), False),
    StructField('size', FloatType(), False),
    StructField('timestamp', TimestampType(), False)
    ])
    
    request2 = sqlContext.createDataFrame(request,schema)
    
    # Result
    request2.write.format('parquet').save(output)


if __name__ == "__main__":
    main()
    # -*- coding: utf-8 -*-

