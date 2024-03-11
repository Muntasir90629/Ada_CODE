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



dhaka_uh = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/zone/C/*.csv',header=True)

dhaka_uh.count()



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{04,05,06,07,08,09}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select('ifa','geohash')



cat=dhaka_uh.join(brq2,"ifa","left")


cat=cat.withColumn("new_geohash", substring(col("geohash"),1,8))
cat=cat.drop(cat.geohash)
cat=cat.withColumnRenamed("new_geohash","geohash")


master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI_7/bd_poi_master_geo9_v7.0.csv',header=True)

master=master.drop(master.geohash)


master=master.withColumnRenamed("geohash9","geohash")


master=master.withColumn("new_geohash", substring(col("geohash"),1,8))
master=master.drop(master.geohash)
master=master.withColumnRenamed("new_geohash","geohash")










master=master.withColumn("new_level_id", substring(col("level_id"),1,3))


master=master.drop(master.level_id)


master=master.withColumnRenamed("new_level_id","level_id")



master.select('level_id').show()


tax=spark.read.csv('s3a://ada-bd-emr/muntasir/POI_7/bd_poi_taxonomy_v7.0.csv',header=True)


tax_df=tax.withColumn("new_level_id", substring(col("level_id"),1,3))


tax_df=tax_df.drop(tax_df.level_id)


tax_df=tax_df.withColumnRenamed("new_level_id","level_id")


tax_df.select('level_id').show()



POI_TAX=tax_df.join(master,"level_id","left")


cat_l1=POI_TAX.join(cat,"geohash","left")




df_count=cat_l1.groupBy('l2_name').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
df_count.show(10,truncate=False)








