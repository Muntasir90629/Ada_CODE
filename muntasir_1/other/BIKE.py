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
from functools import reduce
from pyspark.sql import *



df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/202205{02}/*.parquet')



ifa_data=df.select('ifa',F.explode('gps.geohash').alias('geohash'))

ifa_new=ifa_data.withColumn("new_geohash", substring(col("geohash"),1,7))

# ifa_new.show()

ifa_new=ifa_new.drop(ifa_new.geohash)

ifa_new=ifa_new.withColumnRenamed("new_geohash","geohash")


bike=spark.read.csv('s3a://ada-bd-emr/muntasir/bike/yah_showroom.csv',header=True)

bike=bike.select('name','geohash9')

bike=bike.withColumn("geohash", substring(col("geohash9"),1,7))

bike=bike.drop(bike.geohash9)

# bike.show()

bike_cnt=bike.join(ifa_new,'geohash','left')

# bike_cnt.show()

df2=bike_cnt.select('ifa')

df2.distinct().count()

