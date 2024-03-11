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


from pyspark.sql import SparkSession
import functools
 
# explicit function
def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)
 

months=[
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
'28'
# '29',
# '30',
]

# path='s3a://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/test/'
m=len(months)
print(m)
d=len(days)
for i in range(0,m,1):
    month=months[i]
    print(month)
    for j in range(0,d,1):
        # if days[j]=="03":
        #     break
        s='s3a://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase1/Dhk_Raj'
        mn=str(month)
        dy=str(j)
        path2=s+'/'+mn+'/'+dy
        print(path2)
        df=spark.read.csv(path2+'/*.csv',header=True)
        print("df:")
        df.count()
        print("merge :")
        df_em= unionAll([df_em,df])
        df_em.count()

df_em=df_em.groupBy("ifa").count()
df_em.show()
df_em.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase2/dhk_Raj/07', mode='overwrite', header=True)

    
    
