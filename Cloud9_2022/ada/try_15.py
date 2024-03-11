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



def top_vendor_mobile(c,d):
    
    df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{01}/*.parquet')
    #Needed  columns
    #for vendor
    mv=df.select('ifa','device.device_vendor','device.device_model')
    #for device
    mv2=df.select('ifa','device.device_model')
    #Top 10 vendor
    vendor=mv.groupBy("device_vendor").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show(c)
    #Top 20 Mobile
    mobile=mv2.groupBy("device_model").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show(d)
    


def top_app(c):
    
    df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210301/*.parquet')
    df2=df.select('ifa',explode('app')).select('ifa','col.*')
    df3=df2.select('ifa','asn')
    #Top 20 app
    app=df3.groupBy("asn").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show(c)
    

def demo_gender_age(c):
    
    df= spark.read.parquet('s3a://ada-platform-components/demographics/output/BD/age/202111/*.parquet')
    #age count 
    age=df.groupBy("prediction").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show(c)
    
    #gender data read
    df2 = spark.read.parquet('s3a://ada-platform-components/demographics/output/BD/gender/202111/*.parquet')
    #gender count 
    gender=df2.groupBy("prediction").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show(c)


def lifestage():
    
    master_df = spark.read.csv('s3a://ada-prod-data/reference/app/master_all/all/all/all/app.csv', header=True)
    level_df = spark.read.csv('s3a://ada-prod-data/reference/app/app_level/all/all/all/app_level.csv', header=True)
    lifestage_df = spark.read.csv('s3a://ada-prod-data/reference/app/lifestage/all/all/all/app_lifestage.csv', header=True)
    join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
    join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()
    select_columns = ['bundle','lifestage_name']
    finalapp_df = join_df2.select(*select_columns)
    app_df_daily = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210301/*.parquet')
    app_daily=app_df_daily.select('ifa', explode('app')).select('ifa', 'col.*')
    app =app_daily.join(finalapp_df, on='bundle', how='left').cache()
    lifestage_ifa=app.select('ifa','lifestage_name')
    lifestage_ifa=lifestage_ifa.filter(lifestage_ifa['lifestage_name'] != 'null')
    lifestage_ifa_count=lifestage_ifa.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa per lifestages')).sort(col('ifa per lifestages').desc()).limit(10).show(truncate=False)
        
    


top_vendor_mobile(5,20)

top_app(20)

demo_gender_age(5)



lifestage()

