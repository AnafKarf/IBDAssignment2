#!/usr/bin/env python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext
import sys


conf = SparkConf().setAppName("context").setMaster("local")
sc = SparkContext(conf=conf)
sqlc = SQLContext(sc)

input_name = "/" + sys.argv[1]
textFile = sc.wholeTextFiles(input_name)
files = []
for path, content in textFile.collect():
    id = path.split("/")[-1].split('_')[0]
    title = " ".join(path.split("/")[-1].split('_')[1:])
    files.append(Row(id=id, title=title, text=content))

df = sqlc.createDataFrame(files)

df.write.mode('overwrite').csv("/processed", sep = "\t")
