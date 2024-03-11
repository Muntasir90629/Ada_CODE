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

from functools import reduce



from pyspark.sql import DataFrame



from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)


from pyspark.sql.types import StructType,StructField, StringType

schema = StructType([
  StructField('ifa', StringType(), True),
  ])
  
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




for day in days:
    
    df=spark.read.parquet(path+'/202205{'+day+'}/*.parquet')
    
    df2=df.select('ifa',explode('gps')).select('ifa','col.*')
    
    df3=df2.select('ifa','geohash','last_seen')
    df3= df3.withColumn("date", date_format('last_seen', 'dd'))
        
    df3=df3.withColumn("new_geohash", substring(col("geohash"),1,6))
        
    # ifa_new.show()
        
    df3=df3.drop(df3.geohash)
        
    df3=df3.withColumnRenamed("new_geohash","geohash")
        
    # df3.printSchema()
    
    main_airport=spark.read.csv('s3a://ada-bd-emr/muntasir/bus/bus_stop.csv',header=True)
    
    # main_airport=main_airport.drop(main_airport.geohash)
    # main_airport=main_airport.withColumnRenamed("geohash9","geohash")
    # main_airport.show(truncate=False)
    # main_airport=main_airport.select('name','geohash')
    
    
    #cox bazar -dhaka
    #----------------------------------------------------------------------
    
    dhaka_airport=main_airport.filter(main_airport.name == 'dhaka bus stop')
    
    dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')
    
    dhaka_air_ifa=dhaka_airport_visitors.select('ifa')
    
    
    
    cox_airport=main_airport.filter(main_airport.name == 'cox bus stop')
    
    cox_airport__visitors=cox_airport.join(df3,'geohash','left')
    
    cox_air_ifa=cox_airport__visitors.select('ifa')
    
    
    common_ifa=dhaka_air_ifa.join(cox_air_ifa,'ifa','inner')
    
    cox_ifa=common_ifa.distinct()
    
    
    # df_em=df_em.union(ctg_ifa)
    
    
    #dhaka -ctg ifa 
    
    
    
    dhaka_airport=main_airport.filter(main_airport.name == 'dhaka bus stop')
    
    dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')
    
    dhaka_air_ifa=dhaka_airport_visitors.select('ifa')
    
    
    
    
    ctg_airport=main_airport.filter(main_airport.name == 'ctg bus stop')
    
    ctg_airport__visitors=ctg_airport.join(df3,'geohash','left')
    
    ctg_air_ifa=ctg_airport__visitors.select('ifa')
    
    
    # ctg_air_ifa.distinct()
    
    
    common_ifa=dhaka_air_ifa.join(ctg_air_ifa,'ifa','inner')
    
    ctg_ifa=common_ifa.distinct()
    
    
    
    
    
    #dhaka- jessore ifa
    
    #-------------------------------------------------------------------------------
    
    
    
    
    dhaka_airport=main_airport.filter(main_airport.name == 'dhaka bus stop')
    
    dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')
    
    dhaka_air_ifa=dhaka_airport_visitors.select('ifa')
    
    
    
    
    Jashor_airport=main_airport.filter(main_airport.name == 'Jashore bus stop ')
    
    Jashor_airport__visitors=Jashor_airport.join(df3,'geohash','left')
    
    Jashor_air_ifa=Jashor_airport__visitors.select('ifa')
    
    common_ifa=dhaka_air_ifa.join(Jashor_air_ifa,'ifa','inner')
    
    Jashor_ifa=common_ifa.distinct()
    
    
    
    cox_ctg_ifa=cox_ifa.join(ctg_ifa,'ifa','outer')
    
    Jashor_cox_ctg_ifa=cox_ctg_ifa.join(Jashor_ifa,'ifa','outer')
    
    
    
    
    df_em=Jashor_cox_ctg_ifa.join(df_em,'ifa','outer')



df_em=df_em.distinct()

df_em.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/bus/bus4/april/', mode='overwrite', header=True)

df_em.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/bus/bus4/may2/', mode='overwrite', header=True)