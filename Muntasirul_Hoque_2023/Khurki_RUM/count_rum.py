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


rum=spark.read.csv('s3a://ada-bd-emr/nepal/omi/khukri/older/fullview-geofence/*.csv',header=True)
rum=rum.withColumnRenamed("dev_ifa","ifa")
rum=rum.select('ifa')
rum=rum.distinct()
rum.count()

age=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/age/monthly/BD/2022{02}/*.parquet')



