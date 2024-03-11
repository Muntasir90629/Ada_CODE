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

df = feb.select('ifa',F.explode('connection.mm_con_type_desc').alias('type'))
df2 = df.groupBy("type").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)


df =df.select('ifa',F.explode('connection.mm_con_type_desc').alias('type'))
df2 = df.groupBy("type").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)


df2=df.select('ifa','device.major_os')
df3= df2.groupBy("device_vendor").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
df3.show()



df4=df.select('ifa','device.major_os')
df5=df4.groupBy("major_os").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
df5.show()


df1= df.select('ifa',F.explode('connection.mm_con_type_desc').alias('type'))
df2 = df1.groupBy("type").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
df2.show()


df1=df.select('ifa',F.explode('connection.req_con_type_desc').alias('type'))
df2 = df1.groupBy("type").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
df2.show()


df=df.select('ifa',F.explode('connection.user_agent').alias('type'))
df2 = df.groupBy("type").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
df2.show()


df1=df.select('ifa',F.explode('app.bundle').alias('bunddle'))
df2 = df1.groupBy("bunddle").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
df2.show()


df1=df.select('ifa',F.explode('app.asn').alias('asn'))
df2 = df1.groupBy("asn").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
df2.show()

df1=df.select('ifa',F.explode('maxmind.city').alias('city'))
df2 = df1.groupBy("city").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
df2.show()


df1=df.select('ifa',F.explode('gps.state').alias('state'))
df2 = df1.groupBy("state").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
df2.show()