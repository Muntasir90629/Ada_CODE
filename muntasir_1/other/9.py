
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



# Taking monthly brq data wifi and cellular
brq=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{04}/*.parquet')



# Selecting connection and state data
df= brq.select('ifa', F.explode('connection')).select('ifa', 'col.req_con_type_desc', 'col.mm_con_type_desc')
df_state=brq.select('ifa', F.explode('gps')).select('ifa','col.state')



# Overall total count
wifi_df=df.select('ifa','req_con_type_desc').filter(df['req_con_type_desc']=='WIFI')
wifi_df.select('ifa').distinct().count()



cellular_df=df.select('ifa','mm_con_type_desc').filter(df['mm_con_type_desc']=='Cellular')
cellular_df.select('ifa').distinct().count()



overlapping_ifa=wifi_df.join(cellular_df, on='ifa', how='left')
overlapping_ifa=overlapping_ifa.na.drop()
overlapping_ifa.select('ifa').distinct().count()



# Divisional data
final_df=df_state.join(df, on='ifa', how='left')





# Division wise count
wifi_df=final_df.select('ifa', 'state','req_con_type_desc').filter(df['req_con_type_desc']=='WIFI')
wifi_df=wifi_df.groupBy('state').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
wifi_df.show(20, False)



cellular_df=final_df.select('ifa','state','mm_con_type_desc').filter(df['mm_con_type_desc']=='Cellular')
cellular_df=cellular_df.groupBy('state').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
cellular_df.show(20, False)



wifi_df=final_df.select('ifa', 'state','req_con_type_desc').filter(df['req_con_type_desc']=='WIFI')
cellular_df=final_df.select('ifa','mm_con_type_desc').filter(df['mm_con_type_desc']=='Cellular')
overlapping_ifa=wifi_df.join(cellular_df, on='ifa', how='left')
overlapping_ifa=overlapping_ifa.na.drop()
overlapping_ifa=overlapping_ifa.groupBy('state').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
overlapping_ifa.show(20, False)