#Author - Md. Muntasirul Hoque
#importing module 
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
#reading Dataset
df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210301/*.parquet')
df2=df.select('ifa',explode('app')).select('ifa','col.*')
df3=df2.select('ifa','asn')
#Top 20 app
app=df3.groupBy("asn").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show(20)
