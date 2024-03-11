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



df1_comp=spark.read.csv("s3a://ada-bd-emr/muntasir/Super_mom/Non_User/competitor.csv",header=True)



df1_comp=df1_comp.select("Display Name","Email","Phone","Competitor")


df2_inf=spark.read.csv("s3a://ada-bd-emr/muntasir/Super_mom/Non_User/influence.csv",header=True)

df2_inf=df2_inf.select("Phone",'influence_to_buy')


df3_place=spark.read.csv("s3a://ada-bd-emr/muntasir/Super_mom/Non_User/place_to_buy.csv",header=True)


df3_place=df3_place.select("Phone","Place_to_buy")


df4_type=spark.read.csv("s3a://ada-bd-emr/muntasir/Super_mom/Non_User/type.csv",header=True)

df4_type=df4_type.select("Phone","type")



df5=df1_comp.join(df2_inf,'Phone','inner')

df6=df5.join(df3_place,'Phone','inner')


df6=df6.join(df4_type,'Phone','inner')




df6 = df6.dropDuplicates()


df6.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/Super_mom/Non_User/final/', mode='overwrite', header=True)


df7=df6.select('Phone','Display Name','Email','Competitor','influence_to_buy','Place_to_buy','type')