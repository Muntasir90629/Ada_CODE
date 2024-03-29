pkg_list=com.databricks:spark-avro_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.1
pyspark --packages $pkg_list --driver-memory 30G --driver-cores 5 --num-executors 29 --executor-memory 30G --executor-cores 5 --conf spark.driver.memoryOverhead=512 --conf spark.debug.maxToStringFields=100 --conf spark.driver.maxResultSize=0 --conf spark.yarn.maxAppAttempts=1 --conf s7park.ui.port=10045


from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
import csv
import pandas as pd
import numpy as np
import sys
from pyspark.sql import Window
from pyspark.sql.functions import rank, col
import geohash2 as geohash
import pygeohash as pgh
from functools import reduce
from pyspark.sql import *

df=spark.read.csv('s3a://ada-bd-emr/muntasir/REACH/output/fullview-geofence/*.csv',header=True)

df.printSchema()

df=df.select('dev_ifa','poi_name')



df.printSchema()




df=df.withColumnRenamed("dev_ifa","ifa")

df.printSchema()






df=df.withColumnRenamed("poi_name","name")


df.printSchema()


df2=df.select('ifa')


df2.distinct().count()




>>> df.printSchema()
root
 |-- ifa: string (nullable = true)
 |-- name: string (nullable = true)

>>> df2=df.select('ifa')
>>> 
>>> 
>>> df2.distinct().count()
24083594                                                                        
>>> 
