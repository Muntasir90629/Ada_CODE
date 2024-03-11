

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



# zone = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/zone/NE/*.csv',header=True)


geo=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/ROBI_MOSQUE_842/output2/fullview-geofence/*.csv',header=True)


geo.printSchema()


df=geo.select('dev_ifa','poi_name')
df=df.withColumnRenamed("dev_ifa","ifa")
df=df.withColumnRenamed("poi_name","name")
df.printSchema()


df2=df.select('ifa')
df2=df2.distinct()
df2.count()
