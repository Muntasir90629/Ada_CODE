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


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{07,08,09,10,11,12}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select('ifa','state','city')
dhaka=brq2.filter(brq2.state == 'Dhaka')
brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{10,11,12}/*.parquet')
gender_df=brq.select('ifa','user.gender')
gender_female=gender_df.filter(gender_df.gender == 'F')
common_ifa=dhaka.join(gender_female,'ifa','inner')
affluence=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/2022{07,08,09,10,11,12}/*.parquet')
affluence=affluence.select('ifa','final_affluence')
affluence=affluence.withColumnRenamed("final_affluence","affluence")
c_ifa=common_ifa.join(affluence,'ifa','inner')
ifa=c_ifa.groupBy('affluence').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
ifa.show(20, False)
c_ifa=c_ifa.filter(c_ifa.affluence == 'Low')
c_ifa=c_ifa.select('ifa')
c_ifa=c_ifa.distinct()
c_ifa.count()


geo=spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/geo_3/output/fullview-geofence/*.csv',header=True)
geo.printSchema()
df=geo.select('dev_ifa','poi_name')
df=df.withColumnRenamed("dev_ifa","ifa")
df=df.withColumnRenamed("poi_name","name")
df.printSchema()
match=c_ifa.join(df,"ifa","inner")
match.printSchema()





brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{07,08,09,10,11,12}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select("ifa","geohash")
brq2=brq2.withColumn("new_geohash", substring(col("geohash"),1,8))
brq2=brq2.drop(brq2.geohash)
brq2=brq2.withColumnRenamed("new_geohash","geohash")
brq2.printSchema()


match=match.join(brq2,"ifa","inner")

match=match.dropna()





master=spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/master clean.csv',header=True)
master=master.select("level_id","name","geohash")
# master=master.withColumn("new_geohash9", substring(col("geohash9"),1,8))
# master=master.drop(master.geohash9)
# master=master.withColumnRenamed("new_geohash9","geohash")
master.show()


master.select('level_id').distinct().show()



tax=spark.read.csv('s3a://ada-bd-emr/muntasir/POI_7/bd_poi_taxonomy_v7.0.csv',header=True)
tax=tax.select('level_id','l1_name','l2_name','l3_name','l4_name')
# tax_df=tax.withColumn("new_level_id", substring(col("level_id"),1,9))
# tax_df=tax_df.drop(tax_df.level_id)
# tax_df=tax_df.withColumnRenamed("new_level_id","level_id")
# tax_df.select('level_id').show()

tax.select('level_id').distinct().show()


POI_TAX=tax.join(master,"level_id","left")

POI_TAX.printSchema()




match_df=POI_TAX.join(match,"name","left")

match_df.printSchema()







l3=match_df.groupBy("l1_name").agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)

match_df=match_df.filter(match_df.l1_name=='Beauty And Wellness')

l4=match_df.groupBy("l4_name").agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)

l3.show(20,truncate=False)

l4.show(11,truncate=False)

match_df=match_df.select('ifa')
match_df=match_df.distinct()
match.count()





