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


dhaka_uh = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/UH/*.csv',header=True)







df = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/2022{04,05,06,07,08,09}/*.parquet')

df = df.select('ifa',F.explode('req_connection.req_brq_count').alias('brq_count'))

# output=df.groupBy('brq_count').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)
# output.show(200, False)

# row1 = df.agg({"brq_count": "max"}).collect()[0]

df=dhaka_uh.join(df,'ifa','left')

df=df.dropna()



# df=df.select('ifa')

# df=df.distinct()

# df.count()




brq_avg = df.agg({'brq_count': 'avg'})
c = brq_avg.collect()[0][0]

print(c)
final_df = df.withColumn("data_usage", when(df.brq_count > c+520,"ULTRA HIGH")\
.when(df.brq_count >= c+502,"HIGH")\
.when(df.brq_count < c-10,"LOW")\
.when(df.brq_count < c+500,"MID")\
.otherwise(df.brq_count))
output=final_df.groupBy('data_usage').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)
output.show(4, False)