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
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([StructField('ifa', StringType(), True),])
df_em = spark.createDataFrame(emptyRDD,schema)
df_em.printSchema()



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

path='s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/'



c=len(days)


from functools import reduce

from pyspark.sql import DataFrame




for i in range(0,c,1):
    
    
    if days[i]=="30":
        
        break
    
    day1=days[i]
    
    day2=days[i+1]
    
    day2=str(day2)
    
    #dhaka to chittagong-------------------------------------------------------
    
    day1_dhk=spark.read.parquet(path+'/202205{'+day1+'}/*.parquet')
    
    day1_dhk=day1_dhk.select('ifa',explode('gps')).select('ifa','col.*')
    
    day1_dhk=day1_dhk.select('ifa','city')
    
    day1_dhk=day1_dhk.filter(day1_dhk.city=='Dhaka')
    
    day1_dhk=day1_dhk.select('ifa')
    
    day1_dhk=day1_dhk.distinct()
    
    # day1_dhk.show()
    
    # day1_dhk.count()
    
    day2_ctg=spark.read.parquet(path+'/202205{'+day2+'}/*.parquet')
    
    day2_ctg=day2_ctg.select('ifa',explode('gps')).select('ifa','col.*')
    
    day2_ctg=day2_ctg.select('ifa','city')
    
    day2_ctg=day2_ctg.filter(day2_ctg.city=='Chittagong')
    
    day2_ctg=day2_ctg.select('ifa')
    
    day2_ctg=day2_ctg.distinct()
    
    # day2_ctg.show()
    
    # day2_ctg.count()
    common_ifa=day1_dhk.join(day2_ctg,'ifa','inner')
    
    common_ifa=common_ifa.distinct()
    
    
    
    # common_ifa.count()
    #---------------------------------------------------------------------------
    #chittagong to dhaka 
    day1_ctg=spark.read.parquet(path+'/202205{'+day1+'}/*.parquet')
    
    day1_ctg=day1_ctg.select('ifa',explode('gps')).select('ifa','col.*')
    
    day1_ctg=day1_ctg.select('ifa','city')
    
    day1_ctg=day1_ctg.filter(day1_ctg.city=='Chittagong')
    
    day1_ctg=day1_ctg.select('ifa')
    
    day1_ctg=day1_ctg.distinct()
    day2_dhk=spark.read.parquet(path+'/202205{'+day1+'}/*.parquet')
    
    day2_dhk=day2_dhk.select('ifa',explode('gps')).select('ifa','col.*')
    
    day2_dhk=day2_dhk.select('ifa','city')
    
    day2_dhk=day2_dhk.filter(day2_dhk.city=='Dhaka')
    
    day2_dhk=day2_dhk.select('ifa')
    
    day2_dhk=day2_dhk.distinct()
    
    common_ifa2=day1_ctg.join(day2_dhk,'ifa','inner')
    
    common_ifa2=common_ifa2.distinct()
    
    # common_ifa2.count()
    dfs = [common_ifa,common_ifa2]
    
    
    # create merged dataframe
    
    df_complete = reduce(DataFrame.unionAll, dfs)
    
    dfs2=[df_em,df_complete]
    
    df_em=reduce(DataFrame.unionAll, dfs2)
    
    # df_em.count()


df=df_em.groupBy('ifa').count()

df=df.orderBy('count', ascending=False)

i=2

i=str(i)

s='s3a://ada-bd-emr/muntasir/final2'

path=s+'/'+i+'/'

print(path)


df.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/final2/', mode='overwrite', header=True)

    
    