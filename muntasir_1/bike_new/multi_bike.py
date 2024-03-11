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


df=spark.read.csv('s3a://ada-bd-emr/muntasir/bike_june/output/fullview-geofence/*.csv',header=True)

df.printSchema()

df=df.withColumnRenamed("dev_ifa","ifa")

df=df.withColumnRenamed("poi_name","name")



df2=df.select('ifa','name','dev_lat','dev_lon','distance')



main=spark.read.csv('s3a://ada-bd-emr/muntasir/bike_june_19/bike_showroom_19.csv',header=True)



join_df=main.join(df2,'name','inner')

count=join_df.groupBy('ifa').agg(countDistinct('brands').alias('ifa count per carrier')).sort('ifa count per carrier',ascending=False)









# df=df.withColumnRenamed("dev_lat","latitude")

# df=df.withColumnRenamed("dev_lon","longitude")



