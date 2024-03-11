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


user=spark.read.csv("s3a://ada-bd-emr/muntasir/Super_mom/final2/Non_user_Detail.csv",header=True)


from pyspark.sql.types import IntegerType
user=user.withColumn("Age", user["Age"].cast(IntegerType()))

final_df =user.withColumn("Age Group", when(user.Age >50,"50+")\
.when(user.Age>=35,"35-49")\
.when(user.Age>=25,"25-34")\
.when(user.Age>=18,"18-24")\
.otherwise(user.Age))

final_df.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/Super_mom/final3/non_user_detail/', mode='overwrite', header=True)
