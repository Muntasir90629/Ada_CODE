
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



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2023{04}/*.parquet')
df=brq.select('ifa','brq_count')
brq_avg =df.agg({'brq_count': 'avg'})
c = brq_avg.collect()[0][0]
print(c)



final_df = df.withColumn("data_usage", when(df.brq_count > c+290,"ULTRA HIGH")\
.when(df.brq_count >= c+100,"HIGH")\
.when(df.brq_count < c-70,"LOW")\
.when(df.brq_count < c+130,"MID")\
.otherwise(df.brq_count))
output=final_df.groupBy('data_usage').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)
output.show(4, False)



