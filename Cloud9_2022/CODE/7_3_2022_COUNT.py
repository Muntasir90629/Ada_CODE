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



country='BD'
time='monthly'



months =['202001',
'202002',
'202003',
'202004',
'202005',
'202006',
'202007',
'202008',
'202009',
'202010',
'202011',
'202012',
'202101',
'202102',
'202103',
'202104',
'202105',
'202106',
'202107',
'202108',
'202109',
'202110',
'202111',
'202112',
'202201',
'202202',


]




path = 's3a://ada-bd-emr/result/2022/Robi/DSR-623/raw/'



# for m in months:
    
#     df = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/'+time+'/'+country+'/'+m+'/*.parquet')
    
#     c=df.count()
    
#     print(m,c)
    
    
# df = df.select('ifa',F.explode('req_connection.req_brq_count').alias('brq_count'))
# brq_avg = df.agg({'brq_count': 'avg'})
# c = brq_avg.collect()[0][0]
# final_df = df.withColumn("data_usage", when(df.brq_count > c+1500,"ULTRA HIGH")\
# .when(df.brq_count >= c+100,"HIGH")\
# .when(df.brq_count < c-10,"LOW")\
# .when(df.brq_count < c+1000,"MID")\
# .otherwise(df.brq_count))
# output=final_df.groupBy('data_usage').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)
# output.show(4, False)
# print("================ "+m+" DONE" + "=======================")

df = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/'+time+'/'+country+'/'+2022{02}+'/*.parquet')

202001 34804218                                                                 
202002 35226677                                                                 
202003 39220824                                                                 
202004 37547287                                                                 
202005 34531483                                                                 
202006 31102559                                                                 
202007 31400451                                                                 
202008 34599941                                                                 
202009 33294292                                                                 
202010 36008137                                                                 
202011 38487919                                                                 
202012 39480369                                                                 
202101 36421184                                                                 
202102 36222248                                                                 
202103 38421469                                                                 
202104 37423174                                                                 
202105 38905667                                                                 
202106 36579400                                                                 
202107 39982260                                                                 
202108 41866450                                                                 
202109 42903058                                                                 
202110 42180389                                                                 
202111 40407753                                                                 
202112 41004458                                                                 
202201 43308365                                                                 
202202 46145701    

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{02}/*.parquet')