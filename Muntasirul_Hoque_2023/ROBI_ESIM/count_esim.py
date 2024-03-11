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


df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2023{05}/*.parquet')

df=df.select('ifa','device.device_name','device.device_vendor','device.device_year_of_release','device.major_os','device.device_manufacturer','device.device_model')

# list1=df.select("device_vendor")
# list1=list1.distinct()

# list1.show(100,truncate=False)


iphone=df.filter(df.device_vendor== 'Apple')

iphone=iphone.withColumn("year", substring(col("device_year_of_release"),0,4))

iphone=iphone.filter(~F.col('major_os').startswith('iPadOS'))
iphone=iphone.filter(~F.col('major_os').startswith('Android'))
iphone=iphone.filter(~F.col('major_os').startswith('TV'))


iphone=iphone.withColumn("version", substring(col("major_os"),5,4))

from pyspark.sql.types import IntegerType

iphone =iphone.withColumn("version", iphone["version"].cast(FloatType()))

iphone=iphone.filter(iphone.version >= 14.0)
iphone=iphone.select('ifa')
iphone=iphone.distinct()
iphone.count()


df2=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/2022{04}/*.parquet')
# connection = df2.select('ifa', F.explode('req_connection.req_carrier_name').alias('telco'))


# iphone_sim=iphone.join(connection,"ifa","left")

# freq_beh=iphone_sim.groupBy('telco').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)

# freq_beh.show(10,truncate=False)

# # iphone_sim=iphone_sim.select('ifa')
# # iphone_sim=iphone_sim.distinct()
# # iphone_sim.count()



# freq_beh=iphone.groupBy('year').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)

# freq_beh.show(10,truncate=False)



# l2=["2018","2019","2020","2021","2022","2023"]

# iphone=iphone.filter(iphone.year.isin(l2))

# iphone.show(100,truncate=False)



# df=df.select('ifa')

# df=df.distinct()

# df.count()
# 47219888

li=["iPhone 14",	"iPhone 14 Plus",	"iPhone 14 Pro Max",	"iPhone 14 Pro",	"iPhone 13",	"iPhone 13 Pro",	"iPhone 13 Pro Max",	"iPhone 13 Mini",	"iPhone 12",	"iPhone 12 Pro",	"iPhone 12 Pro Max",	"iPhone 12 Mini",	"iPhone SE",	"iPhone 11",	"iPhone 11 Pro",	"iPhone 11 Pro Max",	"iPhone XS",	"iPhone XS Max",	"iPhone XR",	"Google Pixel 7 Pro",	"Google Pixel 7",	"Google Pixel 6 Pro",	"Google Pixel 6",	"Google Pixel 5a 5G",	"Google Pixel 5",	"Google Pixel 4a",	"Google Pixel 4",	"Google Pixel 3",	"Google Pixel 3XL",	"Google Pixel 2",	"Samsung Galaxy S22 5G",	"Samsung Galaxy S22 Ultra 5G",	"Samsung Galaxy S22",	"Samsung Fold LTE model",	"Samsung Z Flip 4",	"Samsung Z Fold 4",	"Samsung Galaxy Z Fold 3 5G",	"Samsung Galaxy Z Flip 5G",	"Samsung Galaxy Z Flip",	"Samsung Galaxy Z Fold2 5G",	"Samsung Galaxy Fold",	"Samsung Galaxy S21+ 5G",	"Samsung Galaxy S21 Ultra 5G",	"Samsung Galaxy Note 20 Ultra",	"Samsung Galaxy Note 20 Ultra 5G",	"Samsung Galaxy Note 20 FE 5G",	"Samsung Galaxy Note 20 FE",	"Samsung Galaxy S20",	"Samsung Galaxy S20+",	"Samsung Galaxy S20 Ultra",	"Vivo X80",	"Huawei P40",	"Huawei P40 Pro",	"Huawei Mate 40",	"Oppo Find X3",	"Oppo Find X3 Pro",	"Oppo Find X5",	"Oppo Find X5 Pro"]

esim=df.filter(df.device_name.isin(li))

esim=esim.select('ifa')

esim=esim.distinct()

esim.count()

df2=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/2022{04,05,06,07,08,09,10,11,12}/*.parquet')
connection = df2.select('ifa', F.explode('req_connection.req_carrier_name').alias('telco'))


iphone_sim=esim.join(connection,"ifa","left")

freq_beh=iphone_sim.groupBy('telco').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)

freq_beh.show(10,truncate=False)











connection = df.select('ifa', F.explode('req_connection.req_carrier_name').alias('telco'))
