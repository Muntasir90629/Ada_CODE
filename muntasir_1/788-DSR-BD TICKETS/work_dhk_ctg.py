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
from pyspark.sql.functions import date_format
from pyspark.sql import SparkSession


#creating  empty DataFrame
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([StructField('ifa', StringType(), True),])
df_em = spark.createDataFrame(emptyRDD,schema)
df_em.printSchema()

months=[
'02',
'03',
'04',
'05',
'06',
'07'
]

days=[
'01',
'02',
'03',
'04',
'05',
'06',
'07',
'08',
'09',
'10',
'11',
'12',
'13',
'14',
'15',
'16',
'17',
'18',
'19',
'20',
'21',
'22',
'23',
'24',
'25',
'26',
'27',
'28',
'29',
'30',
]


path='s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD'



# day1_ctg=spark.read.parquet(path+'2022{'+month+'}{'+day1+'}/*.parquet')

m=len(months)
d=len(days)


for i in range(0,m,1):
    
    month=months[i]
    
    for j in range(0,d,1):
        
        
        if days[j]=="30":
            
            break
    
        day1=days[j]
        
        s=path+'/2022{'+month+'}{'+day1+'}/*.parquet'
        
        print(s)
        
        day1_ctg=spark.read.parquet(path+'/2022{'+month+'}{'+day1+'}/*.parquet')
        
        day1_ctg.count()








day1_dhaka=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20220601/*.parquet')
day1_dhaka=day1_dhaka.select('ifa',explode('gps')).select('ifa','col.*')
day1_dhaka=day1_dhaka.select('ifa','city')
day1_dhaka=day1_dhaka.filter(day1_dhaka.city=='Dhaka')
day1_dhaka=day1_dhaka.select('ifa')
day1_dhaka=day1_dhaka.distinct()


day2_Ctg=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20220602/*.parquet')
day2_Ctg=day2_Ctg.select('ifa',explode('gps')).select('ifa','col.*')
day2_Ctg=day2_Ctg.select('ifa','city')
day2_Ctg=day2_Ctg.filter(day1_dhaka.city=='Chittagong')
day2_Ctg=day2_Ctg.select('ifa')
day2_Ctg=day2_Ctg.distinct()



common_ifa=day2_Ctg.join(day1_dhaka,'ifa','inner')
common_ifa=common_ifa.distinct()




