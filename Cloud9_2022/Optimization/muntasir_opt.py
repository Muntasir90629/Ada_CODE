
# File name     : muntasir_opt.py
# Author        : Md. Muntasirul Hoque
# Date          : 24 March 2022
# Country       : BD
# Description   : Top 10 device vendor, Top 20 handset model, Telco Distribution, Data usage



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
from functools import reduce
from pyspark.sql import *


#Top 10 device vendor code

#reading Dataset
df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2021{01}/*.parquet')
#for vendor selecting columns
vendor=df.select('ifa','device.device_vendor')
vendor=vendor.filter(vendor.device_vendor !='null')
vendor=vendor.withColumnRenamed("device_vendor","Vendor")
#Top 10 vendor
vendor_10=vendor.groupBy("Vendor").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show(10)


#Top 20 handset model

#reading Dataset
df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2021{01}/*.parquet')
#for device model selecting important columns 
mobile=df.select('ifa','device.device_name')
#Removeing null values
mobile=mobile.filter(mobile.device_name !='null')
mobile=mobile.withColumnRenamed("device_name","Device Name")
#Top 20 Mobile
mobile_20=mobile.groupBy("Device Name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show(20)




#telco distribution

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/2021{01}/*.parquet')
connection = df.select('ifa', F.explode('req_connection.req_carrier_name').alias('telco'))
#removing null values
connection=connection.filter(connection.telco != 'null')
#finding  telco distribution and showing it 
telco_cnt=connection.groupBy("telco").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show(20)



#data usage 

#reading the dataframe 
df = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/2021{01}/*.parquet')
#selecting the important columns
df = df.select('ifa',F.explode('req_connection.req_brq_count').alias('brq_count'))
#finding the value of average 
brq_avg = df.agg({'brq_count': 'avg'})
#converting pyspark dataframe into variable 
c = brq_avg.collect()[0][0]
#new column generating 
final_df = df.withColumn("data_usage", when(df.brq_count > c+1500,"ULTRA HIGH").when(df.brq_count >= c+100,"HIGH").when(df.brq_count < c-10,"LOW").when(df.brq_count < c+1000,"MID").otherwise(df.brq_count))
#grouping according to data usage 
output=final_df.groupBy('data_usage').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)
#showing output
output.show(4, False)