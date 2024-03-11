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

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{01}/*.parquet')


df2=df.select('ifa','device.device_vendor','device.device_name','device.device_manufacturer','device.device_model','device.device_year_of_release','device.platform','device.major_os','device.device_category')


for d in  df2.columns:
    
    
    print(d)
    
    d1=df2.select(d)
    c=d1.count()
    
    print("%s : %d" %(d,c))
    

null_cnt = df2.select([count(when(col(c).isNull(),c)).alias(c) for c in df2.columns])                                                            
null_cnt.show()



con_df=df.select('ifa',explode('connection')).select('ifa','col.*')

for d in  df2.columns:
    
    
    print(d)
    
    d1=df2.select(d)
    c=d1.count()
    
    print("%s : %d" %(d,c))
    

null_cnt =con_df.select([count(when(col(c).isNull(),c)).alias(c) for c in con_df.columns])                                                            
null_cnt.show()

