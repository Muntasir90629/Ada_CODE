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




path='s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/'

days=[

['01','02'],

['02','03'],

['03','04'],

['04','05'],

['05','06'],

['06','07'],

]

c=len(days)

for i in range(0,c,1):
  for j in range(0,2,1):
    if j==0:
      dayone=days[i][j]
      # print(dayone)
      # print(daytwo)
    else :
      daytwo=days[i][j]
      # print(dayone)
  s=path+'/202204{'+dayone+','+daytwo+'}/*.parquet'
  
  
  
  print(s)
  
  
  
  
  