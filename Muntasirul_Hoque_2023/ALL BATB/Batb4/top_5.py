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


geo=spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/geo_3/output/fullview-geofence/*.csv',header=True)
geo.printSchema()


df=geo.select('dev_ifa','poi_name')
df=df.withColumnRenamed("dev_ifa","ifa")
df=df.withColumnRenamed("poi_name","name")
df.printSchema()


# df2=df.select('ifa')
# df2=df2.distinct()
# df2.count()


dhaka_zone = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/zone/C/*.csv',header=True)
dhaka_zone.count()



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{10,11,12}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')

match=dhaka_zone.join(df,"ifa","left")




match.printSchema()





brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{04,05,06,07,08,09}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select("ifa","geohash")
brq2=brq2.withColumn("new_geohash", substring(col("geohash"),1,8))
brq2=brq2.drop(brq2.geohash)
brq2=brq2.withColumnRenamed("new_geohash","geohash")
brq2.printSchema()


match=match.join(brq2,"ifa","left")

match=match.dropna()
# match=match.select('ifa')
# match=match.distinct()
# match.count()

# match.printSchema()


# match=match.select('ifa')
# match=match.distinct()
# match.count()



master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_master_geo9_v7.0.csv',header=True)
master=master.select("level_id","name","geohash9")
master=master.withColumn("new_geohash9", substring(col("geohash9"),1,8))
master=master.drop(master.geohash9)
master=master.withColumnRenamed("new_geohash9","geohash")
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




match_df=POI_TAX.join(match,"geohash","left")
match=match.dropna()
match_df.printSchema()


match_df=match_df.filter(match_df.l1_name=='Food And Beverages')

# l1=match_df.groupBy("l1_name").agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)

# l2=match_df.groupBy("l2_name").agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)

# l3=match_df.groupBy("l3_name").agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)

l4=match_df.groupBy("l4_name").agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)




l4.show(11,truncate=False)

