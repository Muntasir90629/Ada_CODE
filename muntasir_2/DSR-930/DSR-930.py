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



master_df = spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/app/master_df/*', header=True)

# master_df.printSchema()
level_df = spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/app/level_df/*', header=True)

# level_df.printSchema()
lifestage_df = spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/app/lifestage_df/*', header=True)

# lifestage_df.printSchema()

#joining table
join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()

select_columns = ['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name', 'lifestage_name']
finalapp_df = join_df2.select(*select_columns)


#taking time line data
brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2023{04}/*.parquet')


brq2 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')

app =finalapp_df.join( brq2,'bundle','inner')


freq_ls = app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('freq')).sort('lifestage_name', ascending = True)
freq_ls=freq_ls.filter(freq_ls.lifestage_name !='null')
freq_ls.show(10, False)


segment=app.select('app_l1_name')

segment=segment.distinct()

segment.show(100,truncate=False)



li=["Social App Accessories","Couple App","Dating App"]



app=app.filter(app.app_l1_name.isin(li))


app=app.select('ifa')


app=app.distinct()

app.count()





# app=app.select('app_l1_name')





# app =app.filter(app.app_l1_name=='Sports and Fitness')

# app=app.select('ifa')


# app=app.distinct()

# app.count()


app=app.filter(app.lifestage_name !='Single')

app=app.filter(app.lifestage_name !='First Jobber')

app=app.filter(app.lifestage_name !='College/University Student')





app=app.select('ifa')


app=app.distinct()

app.count()





# freq_ls = app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('freq')).sort('lifestage_name', ascending = True)
# freq_ls=freq_ls.filter(freq_ls.lifestage_name !='null')
# freq_ls.show(10, False)








df_age = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/age/monthly/BD/2023{04}/*.parquet')




df_age = df_gender.withColumnRenamed('prediction', 'gender')
df_gender_segment=zone.join(df_gender ,'ifa','inner')
df_count=df_gender_segment.groupBy('gender').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
df_count.show(50)

