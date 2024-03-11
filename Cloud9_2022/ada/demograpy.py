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
#age data read
df= spark.read.parquet('s3a://ada-platform-components/demographics/output/BD/age/202111/*.parquet')
#age count 
age=df.groupBy("prediction").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show()

#gender data read
df2 = spark.read.parquet('s3a://ada-platform-components/demographics/output/BD/gender/202111/*.parquet')
#gender count 
gender=df2.groupBy("prediction").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show()





