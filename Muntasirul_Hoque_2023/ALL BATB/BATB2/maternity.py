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


dhaka_uh = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/UH/*.csv',header=True)

df1=dhaka_uh
df1.count()






df2=spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/Mall/output/fullview-geofence/*.csv',header=True)
df2=df2.withColumnRenamed("dev_ifa","ifa")
df2.printSchema()
df2=df2.select('ifa')
df2=df2.distinct()
df2.count()


affluence=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/2022{04,05,06,07,08,09}/*.parquet')

affluence=affluence.select('ifa','final_affluence')

affluence=affluence.filter(affluence.final_affluence == 'Ultra High')


common_ifa=affluence.join(df2,'ifa','inner')

common_ifa=common_ifa.select('ifa')

common_ifa=common_ifa.distinct()

common_ifa.count()




from functools import reduce

from pyspark.sql import DataFrame






dfs = [df1,df2]

# create merged dataframe
df_complete = reduce(DataFrame.unionAll, dfs)

df_complete.count()


df_complete=df_complete.distinct()


df_complete.count()

persona=df_complete

persona.count()

persona.printSchema()



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

# segment=finalapp_df.select('app_l1_name')

# segment=segment.distinct()

# segment.show(100,truncate=False)


finalapp_df =finalapp_df .filter(finalapp_df.app_l1_name=='eCommerce')

finalapp_df.show()



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{04,05,o6,07,08,09}/*.parquet')

# brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210301/*.parquet')

brq2 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')

brq2.count()


app =finalapp_df.join( brq2,'bundle','inner')

app.count()


persona_app =persona.join(app,'ifa','inner')

persona_app=persona_app.select('ifa')


persona_app=persona_app.distinct()



persona_app.count()


















