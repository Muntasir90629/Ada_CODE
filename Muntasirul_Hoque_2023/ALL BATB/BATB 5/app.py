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

dhaka_zone = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/zone/NE/*.csv',header=True)

dhaka_zone.count()


#load app , life stage reffrence data
master_df = spark.read.csv('s3a://ada-bd-emr/app_ref/master_df/*', header=True)

master_df.printSchema()

level_df = spark.read.csv('s3a://ada-bd-emr/app_ref/level_df/*', header=True)

level_df.printSchema()

lifestage_df = spark.read.csv('s3a://ada-bd-emr/app_ref/lifestage_df/*', header=True)

lifestage_df.printSchema()

#joining table
join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()

select_columns = ['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name', 'lifestage_name']
finalapp_df = join_df2.select(*select_columns)


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{06,07,08,09}/*.parquet')
brq2 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')
app = brq2.join(finalapp_df, on='bundle', how='left').cache()

#Joining persona with App Details
persona_app =dhaka_zone.join(app, on = 'ifa').cache()


freq_beh=persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)

freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
freq_beh1.show(10,truncate=False)


# Group by 'app_l1_name' and 'asn'
top_app_per_cat =persona_app.groupBy('app_l1_name','asn').agg(F.countDistinct('ifa').alias('freq'))
top_app_per_cat = top_app_per_cat.filter(top_app_per_cat['app_l1_name'] != 'null')
# top_app_per_cat.show(10,0)

# convertig a single column of a df into list

app_cat_list = top_app_per_cat.select('app_l1_name').distinct()
# app_cat_list = list(app_cat_list.select('app_l1_name').toPandas()['app_l1_name'])
app_cat_list=app_cat_list.rdd.map(lambda x: x[0]).collect()

# Top 20 apps per category

for cat in app_cat_list:
    print(cat)
    top_app_per_cat.filter(top_app_per_cat.app_l1_name == cat).sort(col('freq').desc()).show(20,0)
