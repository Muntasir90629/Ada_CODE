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



df=spark.read.csv('s3a://ada-bd-emr/omi/markets/output2/fullview-geofence/*.csv',header=True)


df2=df.select('dev_ifa')


df2.distinct().count()