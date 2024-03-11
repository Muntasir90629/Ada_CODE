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



#Here we are taking Dhaka District IFA to analysis persona
df=spark.read.csv('s3a://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase3/dhk_ctg_six_month/*.csv',header=True)
# df=spark.read.csv('s3a://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase3/dhk_raj_six_months/*.csv',header=True)
df=df.withColumnRenamed("count","freq")
df=df.filter(df.freq!="1")
df=df.select('ifa')
# df=df.distinct()



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

#taking time line data
brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{03,04}/*.parquet')
# brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210301/*.parquet')

brq2 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')
app = brq2.join(finalapp_df, on='bundle', how='left').cache()

#Joining persona with App Details
persona_app = df.join(app, on = 'ifa').cache()

freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('freq')).sort('lifestage_name', ascending =False)
freq_ls=freq_ls.filter(freq_ls.lifestage_name !='null')
freq_ls.show(50, False)