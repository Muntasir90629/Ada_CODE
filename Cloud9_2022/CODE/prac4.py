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


df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{01}/*.parquet')






df2=df.select('ifa',explode('connection')).select('ifa','col.*')



df3=df2.select('ifa','state')


dis=df3.groupBy('state').agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)


df4=df.select('ifa',explode('maxmind')).select('ifa','col.*')



dev=df.select('ifa',explode('device')).select('ifa','col.*')


df6=df.select('ifa',explode('device')).select('ifa','col.*')



df7=df.select('ifa','device.device_name','device.device_category')