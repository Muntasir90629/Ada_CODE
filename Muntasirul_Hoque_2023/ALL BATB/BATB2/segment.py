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



zone = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/zone/C/*.csv',header=True)

zone.count()





#load app , life stage reffrence data
master_df = spark.read.csv('s3a://ada-bd-emr/app_ref/master_df/*', header=True)

# master_df.printSchema()
level_df = spark.read.csv('s3a://ada-bd-emr/app_ref/level_df/*', header=True)

# level_df.printSchema()
lifestage_df = spark.read.csv('s3a://ada-bd-emr/app_ref/lifestage_df/*', header=True)

# lifestage_df.printSchema()

#joining table
join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()

select_columns = ['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name', 'lifestage_name']
finalapp_df = join_df2.select(*select_columns)


# lis=finalapp_df.select('app_l1_name')
# lis=lis.distinct()

# lis.show()




finalapp_df=finalapp_df.filter(finalapp_df.app_l1_name=='Games')

# finalapp_df=finalapp_df.filter(finalapp_df.app_l1_name=='News')


# finalapp_df=finalapp_df.filter(finalapp_df.app_l1_name=='Social App Accessories')


finalapp_df.show()







#taking time line data
brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{04,05,06,07,08,09}/*.parquet')

# brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210301/*.parquet')

brq2 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')

# brq2.count()



app=finalapp_df.join(brq2,'bundle','inner')


# app.count()





#Joining persona with App Details
persona_app=zone.join(app,'ifa','inner')


# persona_app.count()


persona_app_ifa=persona_app.select('ifa')


persona_app_ifa=persona_app_ifa.distinct()


persona_app_ifa.count()

