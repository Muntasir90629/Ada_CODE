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
# brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{08}/*.parquet')

brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20220301/*.parquet')
brq2 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')
app = brq2.join(finalapp_df, on='bundle', how='left').cache()



#Count Freq on life_stage for analysis
freq_ls =app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('freq')).sort('lifestage_name', ascending = True)
freq_ls=freq_ls.filter(freq_ls.lifestage_name !='null')
freq_ls.show(10, False)
# +-------------------------+----+                                                
# |lifestage_name           |freq|
# +-------------------------+----+
# |null                     |1059|
# |In a Relationship/Married|1   |
# |Parents with Kids (3-6)  |16  |
# |Single                   |29  |
# |Working Adults           |46  |
# +-------------------------+----+

########   2. Top 10 Behaviour    ########
freq_beh=app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('app_l1_name', ascending = True)
freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
freq_beh1.show(20,truncate=False)


freq_beh=app.groupBy('app_l2_name').agg(F.countDistinct('ifa').alias('freq')).sort('app_l2_name', ascending = False)
freq_beh1 = freq_beh.filter(freq_beh['app_l2_name'] != 'null')
freq_beh1.show(20,truncate=False)

freq_beh=app.groupBy('app_l3_name').agg(F.countDistinct('ifa').alias('freq')).sort('app_l1_name', ascending = True)
freq_beh1 = freq_beh.filter(freq_beh['app_l3_name'] != 'null')
freq_beh1.show(20,truncate=False)







# +----------------------+----+
# |app_l1_name           |freq|
# +----------------------+----+
# |Call and Chat         |877 |
# |Social App Accessories|699 |
# |Photo Video           |289 |
# |Music                 |190 |
# |Video Streaming       |189 |
# |Games                 |162 |
# |Sports and Fitness    |87  |
# |Personal Productivity |76  |
# |Career                |42  |
# |Finance               |34  |
# +----------------------+----+


########   3. Top App Used    ########
#Please change the list (list1 and list2) according to persona (it is available for all region)
list1 = ['Shopping', 'Video Streaming'] #change list, should be the same as list used in persona.py to build persona
list2= ['Investment', 'Currency Converter'] #change list, should be the same as list used in persona.py to build persona
app_df = app.filter((app.app_l2_name.isin(list2)) | (app.app_l1_name.isin(list1)))



#Joining persona with App Details
persona_app = persona.join(app_df, on = 'ifa').cache()

#Count Freq on top app for analysis
top_app = persona_app.groupBy('asn').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
top_app = top_app.filter(top_app['asn'] != 'null')
top_app.show(10,0)