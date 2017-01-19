from pyspark import SparkConf, SparkContext
import sys, operator
import re, string, unicodedata

inputs = 'C:\\Users\\linhb\\a1-wordcount-small'
output = 'C:\\Users\\linhb\\a1-output'
 
conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
 
text = sc.textFile(inputs)

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

words = text.flatMap(lambda line: wordsep.split(line.lower())).map(lambda word: unicodedata.normalize('NFD',word)).filter(lambda x: len(x) > 0).map(lambda w: (w, 1))
 
wordcount = words.reduceByKey(operator.add).cache()

# Result sorted by Alphabetical order
outdata = wordcount.sortBy(lambda (w,c): w).map(lambda (w,c): u"%s %i" % (w, c))
# I have thought about combining this data onto one node. It is safe since the output data is small.
outdata.coalesce(1).saveAsTextFile(output+"/by-word")

# Result sorted by Frequency and then by Alphabetical order
outdata = wordcount.sortBy(lambda (w,c): w).sortBy(lambda (w,c): c, ascending=False).map(lambda (w,c): u"%s %i" % (w, c))
# I have thought about combining this data onto one node. It is safe since the output data is small.
outdata.coalesce(1).saveAsTextFile(output+"/by-freq")