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

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/202{203,204,205,206}/*.parquet')

# df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/202{20630,20701,20702,20703,20704,20705,20706,20707,20708,20709,20710,20711,20712}/*.parquet')




df2=df.select('ifa',explode('gps')).select('ifa','col.*')

df2=df2.withColumn("new_geohash", substring(col("geohash"),1,8))

df2=df2.drop(df2.geohash)

df2=df2.withColumnRenamed("new_geohash","geohash")




df3=df2.filter(df2.geohash=='w5cr1cf9')


mob=df.select('ifa','device.device_vendor','device.device_manufacturer','user.gender','user.age')



ctg_ifa=df3.join(mob,'ifa','inner')

ctg_ifa=ctg_ifa.select('ifa','device_vendor','age')


ctg=ctg_ifa.filter(ctg_ifa.age==23)

ctg.select('ifa')

dhaka=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/202{201,202,203,204,205}/*.parquet')



ifa=dhaka.join(ctg,'ifa','inner')


dhaka_airport=main_airport.filter(main_airport.name == 'Hazrat Shahjalal International Airport')

from pyspark.sql.functions import date_format

home_ctg=ctg_ifa.withColumn("hour", date_format('last_seen', 'HH'))


home_ctg=home_ctg.select('ifa','state_name','city','state','country','device_vendor','device_manufacturer','age','gender','hour')

from pyspark.sql.types import IntegerType

home_ctg = home_ctg.withColumn("hour", home_ctg["hour"].cast(IntegerType()))


# df4=df.select('ifa','device.device_vendor','device.device_manufacturer','user.gender','user.age')

# ctg_ifa=df3.join(df4,'ifa','left')

# ctg_ifa.coalesce(1).write.csv("my.csv")



# df5=df2.filter(df2.ifa=='f0b518f9-a2a1-429b-96eb-30c7dd50c3e3')