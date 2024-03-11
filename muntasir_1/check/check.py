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

s3://ada-prod-data/etl/data/brq/sub/age/monthly/BD/

# s3://ada-prod-data/etl/data/brq/sub/gender/monthly/BD/

# s3://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/

# s3://ada-prod-data/etl/data/brq/sub/device/monthly/BD/

# s3://ada-prod-data/etl/data/brq/sub/home-office/monthly/BD/

# s3://ada-prod-data/etl/data/brq/sub/app/monthly/BD/

# df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{06}/*.parquet')

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/app/monthly/BD/2022{07}/*.parquet')

df.printSchema()



con=df.select('ifa',explode('app')).select('ifa','col.*')

con2=con.select('ifa',explode('genre')).select('ifa','col.*')



con1=con.select('ifa','genre','segment')

# con.printSchema()

for col in con1.dtypes:
    c=col[0]
    print(c)
    ifa_null=con1.groupBy(c).agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
    ifa_null.show(200, False)











# device=df.select('ifa','device.device_vendor','device.device_name','device.device_manufacturer','device.device_model','device.device_year_of_release','device.platform','device.major_os','device.device_category','device.price','device.pricegrade')

# device=device.select('ifa','device_category','price','pricegrade')
# for col in device.dtypes:
#     c=col[0]
#     print(c)
#     ifa_null=device.groupBy(c).agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
#     ifa_null.show(100, False)


# ifa_null=df.groupBy('prediction').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
