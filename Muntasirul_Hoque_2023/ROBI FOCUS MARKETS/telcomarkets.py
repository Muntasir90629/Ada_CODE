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

brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{08,09,10}/*.parquet')



con=brq.select('ifa', F.explode('connection')).select('ifa', 'col.*')

con=con.select('ifa','mm_carrier_name')

l=['GrameenPhone','Robi','Banglalink','Teletalk Bangladesh']

con=con.filter(con.mm_carrier_name.isin(l))






brq2 = brq.select('ifa', F.explode('maxmind')).select('ifa', 'col.*')
brq2=brq2.select('ifa','city')

#Chittagong Noakhali Comilla  Khulna Kushtia  Rajshahi Rangpur City Sylhet Dhaka Mymensingh

region=brq2.filter(brq2.city == 'Mymensingh')
region.show()
# region.count()




region_ifa=region.join(con,'ifa','left')
# region_ifa.count()

region_ifa=region_ifa.dropna()
# region_ifa.count()

# region_ifa.show()

telco=region_ifa.groupBy('mm_carrier_name').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
telco.show(50,truncate=False)









