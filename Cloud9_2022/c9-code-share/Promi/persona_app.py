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

path = 's3a://ada-bd-emr/poi/fuad_tanvir/output/dhaka_fields/fullview-geofence/*.csv'

dhaka_fields = spark.read.csv(path,header=True)
persona = dhaka_fields

# Load Dataframes

master_df = spark.read.csv('s3a://ada-bd-emr/app_ref/master_df/*', header=True)
level_df = spark.read.csv('s3a://ada-bd-emr/app_ref/level_df/*', header=True)
lifestage_df = spark.read.csv('s3a://ada-bd-emr/app_ref/lifestage_df/*', header=True)

# Select necessary columns for optimization

master_df = master_df.select(['bundle','app_level_id','app_lifestage_id'])
level_df = level_df.select(['app_level_id','app_l1_name','app_l2_name','app_l3_name'])
lifestage_df = lifestage_df.select(['app_lifestage_id','lifestage_name'])

# Joining the tables

join_df1 = master_df.join(level_df, on='app_level_id', how='left')
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()

# Overwrite the prev join_df1 variable and replace it as a final df

join_df1 = join_df2.select(['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name', 'lifestage_name'])

#taking time line data
brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210301/*.parquet')

app = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')
app = app.join(join_df1, on='bundle', how='left').cache()


#count freq on Top 10 Category
count_df = app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
count_df = count_df.filter(count_df['app_l1_name'] != 'null')
count_df.show(10,0)


# Group by 'app_l1_name' and 'asn'
top_app_per_cat = app.groupBy('app_l1_name','asn').agg(F.countDistinct('ifa').alias('freq'))
top_app_per_cat = top_app_per_cat.filter(top_app_per_cat['app_l1_name'] != 'null')
top_app_per_cat.show(10,0)

# convertig a single column of a df into list

app_cat_list = top_app_per_cat.select('app_l1_name').distinct()
app_cat_list = list(app_cat_list.select('app_l1_name').toPandas()['app_l1_name'])

# Top 20 apps per category

for cat in app_cat_list:
    print(cat)
    top_app_per_cat.filter(top_app_per_cat.app_l1_name == cat).sort(col('freq').desc()).show(20,0)
    