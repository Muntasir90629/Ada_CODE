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


df=spark.read.csv('s3a://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase2/dhk_Raj/{02,03,04,05,06,07}/*.csv',header=True)

df.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase3/dhk_raj_six_months/', mode='overwrite', header=True)



df_em.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase3/dhk_ctg_six_month/', mode='overwrite', header=True)

# s3://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase2/dhaka_ctg/

# 19358+17631

df=spark.read.csv('s3a://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase2/dhaka_ctg/{02,03,04,05,06,07}/*.csv',header=True)

df.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase3/dhk_ctg_six_month/', mode='overwrite', header=True)


df=spark.read.csv('s3a://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase3/dhk_ctg_six_month/*.csv',header=True)
s3://ada-bd-emr/muntasir/DSR_788_BD_TICKETS/phase3/dhk_raj_six_months/