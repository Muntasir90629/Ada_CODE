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
from pyspark.sql import SparkSession

from pyspark.sql.functions import date_format



df1=spark.read.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/GEO_JAN_MARCH_23/output/fullview-geofence/*.csv',header=True)
df1.count()
df1.printSchema()


df2=spark.read.csv('s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/2022/output/fullview-geofence/*.csv',header=True)
df2.count()
df2.printSchema()


from functools import reduce
from pyspark.sql import DataFrame



dfs = [df1, df2]

# create merged dataframe
df_complete = reduce(DataFrame.unionAll, dfs)

df_complete =df_complete.distinct()

df_complete.count()


df_complete.coalesce(1).write.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/full_geo/', mode='overwrite', header=True)


s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/full_geo/


df=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/full_geo/*.csv',header=True)

df.count()









