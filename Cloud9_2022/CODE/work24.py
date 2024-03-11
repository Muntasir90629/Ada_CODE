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

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2021{11}/*.parquet')

df.show()

df.printSchema()
df.select('gps.first_seen','gps.last_seen').show()


df.select('ifa',F.explode('gps.geohash').alias('geohash'),'gps.first_seen','gps.last_seen').show()


df.withColumn("new_geohash", substring(col("geohash"),-4,4))

 ifa_data=df.select('ifa',F.explode('gps.geohash').alias('geohash'),'gps.first_seen','gps.last_seen')
ifa_data.show()

ifa_data.withColumn("new_geohash", substring(col("geohash"),1,6))


>>> ifa_new=ifa_data.withColumn("new_geohash", substring(col("geohash"),1,6))
>>> ifa_new.show()
ifa_new.drop(ifa_new.geohash)


ifa_new.printSchema()

>>> ifa_new=ifa_new.drop(ifa_new.geohash)
>>> ifa_new.show()

ifa_new.withColumnRenamed("new_geohash","geohash").printSchema()

ifa_new.withColumnRenamed("gps.first_seen","first_seen")
ifa_new.withColumnRenamed("gps.last_seen","last_seen")

master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_master_v7.0.csv',header=True)

master.filter(master.level_id=='FNB_05_01_074').show()

bfc=master.filter(master.level_id=='FNB_05_01_074')

bfc=bfc.select('name','geohash')