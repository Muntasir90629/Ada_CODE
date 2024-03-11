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



# zone = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/zone/NE/*.csv',header=True)


geo=spark.read.csv('s3a://ada-bd-emr/muntasir/unilever_local_store/output/fullview-geofence/*.csv',header=True)
geo.printSchema()
df=geo.select("dev_ifa")
df=df.withColumnRenamed("dev_ifa","ifa")
df=df.distinct()
df.count()
df.printSchema()



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{04,05,07,08,09,10}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select('ifa','geohash')
main=brq2.withColumn("new_geohash", substring(col("geohash"),1,6))
main=main.drop(main.geohash)
main=main.withColumnRenamed("new_geohash","geohash")








common_ifa=df.join(main,'ifa','left')

# common_ifa=common_ifa.select("ifa")

# common_ifa=common_ifa.distinct()

# common_ifa.count()




look_up=spark.read.csv('s3a://ada-bd-emr/muntasir/BD_LOOKUP/BD_lookup_table.csv',header=True)

cifa=look_up.join(common_ifa,'geohash','left')




freq_beh=cifa.groupBy('division').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)


freq_beh.show(100,truncate=False)

