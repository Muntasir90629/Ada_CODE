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


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/home-office/monthly/BD/2020{05}/*.parquet')


brq=brq.select('ifa','user.age')


brq=brq.filter(brq.age >45 )

brq=brq.select('ifa')

brq=brq.distinct()

brq.count()
# brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210301/*.parquet')

brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')

brq2.printSchema()


ifa=brq2.select('ifa','geohash')



ifa=ifa.withColumn("new_geohash", substring(col("geohash"),1,7))


ifa=ifa.drop(ifa.geohash)

ifa=ifa.withColumnRenamed("new_geohash","geohash")

ifa.show()


master_poi=spark.read.csv('s3://ada-dev/BD-DataScience/muntasir/POI_7/bd_poi_master_geo9_v7.0.csv',header=True)




master_poi=master_poi.select('level_id','name','geohash9')


master_poi=master_poi.withColumn("geohash", substring(col("geohash9"),1,7))


master_poi=master_poi.drop(master_poi.geohash9)


master_poi=master_poi.withColumn("LD", substring(col("level_id"),1,9))



master_poi=master_poi.filter(master_poi.LD =='RET_09_01')


master_poi.show()




df_in=ifa.join(master_poi,'geohash','inner')

df2=df_in.select('ifa')

df2.printSchema()

df2=df2.distinct()


df2.count()








