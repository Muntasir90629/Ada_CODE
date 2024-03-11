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


#Daily dataframe
Df5 = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210301/*.parquet')




device_vendor=Df5.select('ifa',col('device.device_vendor').alias('device vendor'))

top_device_vendor=device_vendor.groupBy('device vendor').agg(F.countDistinct('ifa').alias('ifa_count')).sort(col('ifa_count').desc())

columns=['vendor','model','category']
for c in columns:
    print('device_'+c)


# 20210301 Daily Top 10
top_10_device_vendor=Df5.select('ifa',col('device.device_vendor').alias('device vendor')).groupBy('device vendor').agg(F.countDistinct('ifa').alias('ifa_count')).sort(col('ifa_count').desc()).limit(10).show()
top_10_device_model=Df5.select('ifa',col('device.device_model').alias('device model')).groupBy('device model').agg(F.countDistinct('ifa').alias('ifa_count')).sort(col('ifa_count').desc()).limit(10).show()
top_10_device_category=Df5.select('ifa',col('device.device_category').alias('device category')).groupBy('device category').agg(F.countDistinct('ifa').alias('ifa_count')).sort(col('ifa_count').desc()).limit(10).show()


# 2022{01}/ jan 2022 top 10 vendor
df_202201=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{01}/*.parquet')
top_10_device_vendor_202201=df_202201.select('ifa',col('device.device_vendor').alias('device vendor')).groupBy('device vendor').agg(F.countDistinct('ifa').alias('ifa_count')).sort(col('ifa_count').desc()).limit(10).show()


#Top n numbers of daily device vendor(1), device model(2) & device category(3). a --> which device column,m is the month string
def top_n_device(n,a,m):
  
    
    path='s3a://ada-prod-data/etl/data/brq/agg/agg_brq/'
    #Applicable for data from 2020 - 2021.NOT 2022!! 
    Df5=spark.read.parquet(path+'monthly/BD/'+str(m)+'/*.parquet') if len(str(m))==6 else spark.read.parquet(path+'daily/BD/'+str(m)+'/*.parquet')
    
    if(a==1):
        return Df5.select('ifa',col('device.device_vendor').alias('device vendor')).groupBy('device vendor').agg(F.countDistinct('ifa').alias('ifa_count')).sort(col('ifa_count').desc()).limit(n).show()
    elif(a==2):
        return Df5.select('ifa',col('device.device_model').alias('device model')).groupBy('device model').agg(F.countDistinct('ifa').alias('ifa_count')).sort(col('ifa_count').desc()).limit(n).show()
    elif(a==3):
        return Df5.select('ifa',col('device.device_category').alias('device category')).groupBy('device category').agg(F.countDistinct('ifa').alias('ifa_count')).sort(col('ifa_count').desc()).limit(n).show()
    
    
#top 10 device category        
top_n_device(10,3,202103)

+----------------+---------+                                                    
| device category|ifa_count|
+----------------+---------+
|      Smartphone| 35810448|
|   Feature Phone|  2002048|
|          Tablet|   579411|
|        Smart-TV|    12789|
|         Desktop|     8279|
|           Robot|     7169|
|    Other Mobile|     1030|
|Other Non-Mobile|      259|
|            null|       36|
+----------------+---------+


top_n_device(10,3,20210301)
+----------------+---------+                                                    
| device category|ifa_count|
+----------------+---------+
|      Smartphone|  9280034|
|   Feature Phone|   356160|
|          Tablet|   109086|
|         Desktop|     1683|
|           Robot|     1209|
|        Smart-TV|     1053|
|    Other Mobile|      253|
|Other Non-Mobile|       32|
|            null|        5|
+----------------+---------+
        
path='s3a://ada-prod-data/etl/data/brq/agg/agg_brq/'
Df5=spark.read.parquet(path+'monthly/BD/'+str(m)+'/*.parquet') if len(str(m))==6 else spark.read.parquet(path+'daily/BD/'+str(m)+'/*.parquet')


columns=['vendor','model','category']
for c in columns:
    print('device_'+c)
    
function=
    
return Df5.select('ifa',col('device.device_vendor').alias('device vendor')).groupBy('device vendor').agg(F.countDistinct('ifa').alias('ifa_count')).sort(col('ifa_count').desc()).limit(n).show()