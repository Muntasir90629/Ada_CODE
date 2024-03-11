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
from pyspark.sql import SparkSession

from pyspark.sql.functions import date_format



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/202212{09}/*.parquet')


brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(10,truncate=False)


df1=baluka.filter(baluka.Name == 'The Westin Dhaka')
df1_event=df1.join( gps_df,'geohash','inner')
df1_event=df1_event.select('ifa')
df1_event=df1_event.distinct()
df1_event.count()




#AISD


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221205,20221201,20221023,20220910,20221206}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(10,truncate=False)
df1=baluka.filter(baluka.Name == 'AISD')
df1_AISD=df1.join( gps_df,'geohash','inner')
df1_AISD=df1_AISD.select('ifa')
df0=df1_AISD.distinct()
df0.count()

df0.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df1', mode='overwrite', header=True)

#  Grace International School Bangladesh


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221128,20220923,20220822,20220823,20220824}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(10,truncate=False)



df1=baluka.filter(baluka.Name == 'Grace International School Bangladesh')
df1.show(10,truncate=False)
df1_Grace=df1.join( gps_df,'geohash','inner')
df1_Grace=df1_Grace.select('ifa')
df2=df1_Grace.distinct()
df2.count()

df2.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df2', mode='overwrite', header=True)
#ISD Campus 


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221025,20220928,20220830,20220131,20220619}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)



df1=baluka.filter(baluka.Name == 'ISD Campus')
df1.show(10,truncate=False)
df1_ISD=df1.join( gps_df,'geohash','inner')
df1_ISD=df1_Grace.select('ifa')
df3=df1_Grace.distinct()
df3.count()

df3.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df3', mode='overwrite', header=True)


#Radisson


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221114,20221115,20221123,20221124,20221203,20221207}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)



df1=baluka.filter(baluka.Name == 'Radisson Blu Hotel DHAKA')
df1.show(10,truncate=False)
df1_Radisson=df1.join( gps_df,'geohash','inner')
df1_Radisson=df1_Radisson.select('ifa')
df4=df1_Radisson.distinct()
df4.count()

df4.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df4', mode='overwrite', header=True)




#Silchar


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221203,20221203}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)



df1=baluka.filter(baluka.Name == 'Silchar, Police Parade Ground')
df1.show(10,truncate=False)
df1_Silchar=df1.join( gps_df,'geohash','inner')
df1_Silchar=df1_Silchar.select('ifa')
df5=df1_Silchar.distinct()
df5.count()

df5.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df5', mode='overwrite', header=True)

#bpc 


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221204}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)



df1=baluka.filter(baluka.Name == 'BPC')
df1.show(10,truncate=False)
df1_bpc=df1.join( gps_df,'geohash','inner')
df1_bpc=df1_bpc.select('ifa')
df6=df1_bpc.distinct()
df6.count()



df6.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df6', mode='overwrite', header=True)


# Grand Palace Sylhet



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20220922}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)



df1=baluka.filter(baluka.Name == 'Grand Palace Sylhet')
df1.show(10,truncate=False)
df1_grand=df1.join( gps_df,'geohash','inner')
df1_grand=df1_grand.select('ifa')
df7=df1_grand.distinct()
df7.count()

df7.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df7', mode='overwrite', header=True)


# The Westin Dhaka


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221209}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)



df1=baluka.filter(baluka.Name == 'The Westin Dhaka')
df1.show(10,truncate=False)
df1_Westin=df1.join( gps_df,'geohash','inner')
df1_Westin=df1_Westin.select('ifa')
df8=df1_Westin.distinct()
df8.count()



df8.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df8', mode='overwrite', header=True)


# Kurmitola Golf Club


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221113}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)



df1=baluka.filter(baluka.Name == 'Kurmitola Golf Club')
df1.show(10,truncate=False)
df1_kgf=df1.join( gps_df,'geohash','inner')
df1_kgf=df1_kgf.select('ifa')
df9=df1_kgf.distinct()
df9.count()

df9.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df9', mode='overwrite', header=True)


#Bangabandhu International Conference Center


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221114}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)



df1=baluka.filter(baluka.Name == 'Bangabandhu International Conference Center')
df1.show(10,truncate=False)
df1_bicc=df1.join( gps_df,'geohash','inner')
df1_bicc=df1_bicc.select('ifa')
df10=df1_bicc.distinct()
df10.count()

df10.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df10', mode='overwrite', header=True)




#Bangabandhu International Conference Center


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221114,20230311,20230312,20230313}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)



df1=baluka.filter(baluka.Name == 'Bangabandhu International Conference Center')
df1.show(10,truncate=False)
df1_bicc=df1.join( gps_df,'geohash','inner')
df1_bicc=df1_bicc.select('ifa')
df11=df1_bicc.distinct()
df11.count()


df11.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df11', mode='overwrite', header=True)

#FBCCI


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221114,20230311,20230312,20230313}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)



df1=baluka.filter(baluka.geohash == 'wh0qcw)
df1.show(10,truncate=False)
df1_FBCCI=df1.join( gps_df,'geohash','inner')
df1_FBCCI=df1_FBCCI.select('ifa')
df12=df1_FBCCI.distinct()
df12.count()

df12.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df12', mode='overwrite', header=True)

#ICCB

brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221115,20221116}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)

li=["Hall 2  ICCB","Hall 1  ICCB","Hall 3  ICCB"]
df1=baluka.filter(baluka.Name.isin(li))
df1.show(10,truncate=False)
df1_ICCB=df1.join( gps_df,'geohash','inner')
df1_ICCB=df1_ICCB.select('ifa')
df13=df1_ICCB.distinct()
df13.count()


df13.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/df13', mode='overwrite', header=True)

#GEC


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/{20221117,20221118,20221119}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
gps_df=brq2.withColumn("day", date_format('last_seen', 'dd'))
gps_df=gps_df.withColumn("month", date_format('last_seen', 'MM'))
gps_df=gps_df.drop(gps_df.last_seen)
gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))
gps_df=gps_df.drop(gps_df.geohash)
gps_df=gps_df.withColumnRenamed("new_geohash","geohash")
brq2.show(10)
gps_df.show(10)
baluka=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/baluka_geo.csv',header=True)
baluka=baluka.withColumn("new_geohash", substring(col("geohash"),1,6))
baluka=baluka.drop(baluka.geohash)
baluka=baluka.withColumnRenamed("new_geohash","geohash")
baluka.show(50,truncate=False)

df1=baluka.filter(baluka.Name == 'GEC Convention Centre')
df1.show(10,truncate=False)
df1_bicc=df1.join( gps_df,'geohash','inner')
df1_bicc=df1_bicc.select('ifa')
df14=df1_bicc.distinct()
df14.count()


df14.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/ifa_baluka/uka/df14', mode='overwrite', header=True)




from functools import reduce

from pyspark.sql import DataFrame



dfs = [df0,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14]


dfs=dfs.select('ifa')

dfs=dfs.distinct()

dfs.count()



dfs.coalesce(1).write.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/total_ifa/', mode='overwrite', header=True)












s3a://ada-business-insights/prod/affluence/affluence_result/BD/202206/


brq = spark.read.parquet('s3a://ada-business-insights/prod/affluence/affluence_result/BD/202206/*.parquet')