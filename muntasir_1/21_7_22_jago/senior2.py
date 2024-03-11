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



affluence=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/2022{06}/*.parquet')
affluence=affluence.select('ifa','final_affluence')
affluence=affluence.filter(affluence.final_affluence != 'Low')
affluence=affluence.filter(affluence.final_affluence != 'Mid')



age_df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{06}/*.parquet')
age_df=age_df.select('ifa','user.age')
age_df=age_df.filter(age_df.age>=35)


affluence_age_df=age_df.join(affluence, on='ifa', how='inner')
affluence_age_df=affluence_age_df.select('ifa')
affluence_age_df=affluence_age_df.distinct()
affluence_age_df.count()


# affluence_age_df=affluence_age_df.select('ifa')

# affluence_age_df=affluence_age_df.distinct()

# affluence_age_df.count()

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
select_columns = ['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name','lifestage_name']
finalapp_df = join_df2.select(*select_columns)



#taking time line data
brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{04,05,06}/*.parquet')
# brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210301/*.parquet')
brq2 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')



app = brq2.join(finalapp_df, on='bundle', how='left')
app_aff=affluence_age_df.join(app, on='ifa', how='left')
app_aff2=app_aff.select('ifa','final_affluence','lifestage_name','age')
app_job=app_aff2.filter((app.lifestage_name == 'Working Adults') |(app.lifestage_name == 'First Jobber'))



# app_job.show()

app_job=app_job.select('ifa')
app_job=app_job.distinct()
app_job.count()


# df2_aff_H_ifa=app_job.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
# df2_aff_H_ifa.show(20, False)