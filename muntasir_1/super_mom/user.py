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


df1_aware=spark.read.csv("s3a://ada-bd-emr/muntasir/Super_mom/User/awareness.csv",header=True)
df1_aware=df1_aware.select("Display Name","Email","awareness","Phone")


df2_inf=spark.read.csv("s3a://ada-bd-emr/muntasir/Super_mom/User/influence.csv",header=True)
df2_inf=df2_inf.select("Phone","influence")


df3_pre=spark.read.csv("s3a://ada-bd-emr/muntasir/Super_mom/User/Preference.csv",header=True)
df3_pre=df3_pre.select("Phone","Preference")


df4_refer=spark.read.csv("s3a://ada-bd-emr/muntasir/Super_mom/User/refer.csv",header=True)
df4_refer=df4_refer.select("Phone","refere")



df5_sat=spark.read.csv("s3a://ada-bd-emr/muntasir/Super_mom/User/satisfaction.csv",header=True)
df5_sat=df5_sat.select("Phone","statisfaction ")




df6_choice=spark.read.csv("s3a://ada-bd-emr/muntasir/Super_mom/User/User_choice.csv",header=True)
df6_choice=df6_choice.select("Phone","Choice")



df7=df1_aware.join(df2_inf,'Phone','inner')

df8=df7.join(df3_pre,'Phone','inner')

df9=df8.join(df4_refer,'Phone','inner')

df10=df9.join(df5_sat,'Phone','inner')

df11=df10.join(df6_choice,'Phone','inner')


df11 =df11.dropDuplicates()



df11.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/Super_mom/User/final/', mode='overwrite', header=True)
