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

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2021{11}/*.parquet')
>>> df.printSchema()
root
 |-- ifa: string (nullable = true)
 |-- device: struct (nullable = true)
 |    |-- device_vendor: string (nullable = true)
 |    |-- device_name: string (nullable = true)
 |    |-- device_manufacturer: string (nullable = true)
 |    |-- device_model: string (nullable = true)
 |    |-- device_year_of_release: string (nullable = true)
 |    |-- platform: string (nullable = true)
 |    |-- major_os: string (nullable = true)
 |    |-- device_category: string (nullable = true)
 |-- connection: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- mm_con_type_desc: string (nullable = true)
 |    |    |-- mm_carrier_name: string (nullable = true)
 |    |    |-- req_con_type: integer (nullable = true)
 |    |    |-- req_carrier_mnc: string (nullable = true)
 |    |    |-- req_carrier_name: string (nullable = true)
 |    |    |-- req_carrier_code: string (nullable = true)
 |    |    |-- req_carrier_mcc: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- user_agent: string (nullable = true)
 |    |    |-- req_con_type_desc: string (nullable = true)
 |    |    |-- ndays: long (nullable = true)
 |-- app: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- bundle: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- platform: string (nullable = true)
 |    |    |-- asn: string (nullable = true)
 |    |    |-- ndays: long (nullable = true)
 |-- maxmind: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- geohash: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- longitude: float (nullable = true)
 |    |    |-- state_name: string (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- latitude: float (nullable = true)
 |    |    |-- country: string (nullable = true)
 |    |    |-- ndays: long (nullable = true)
 |-- gps: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- geohash: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- longitude: float (nullable = true)
 |    |    |-- state_name: string (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- latitude: float (nullable = true)
 |    |    |-- country: string (nullable = true)
 |    |    |-- ndays: long (nullable = true)
 |-- user: struct (nullable = true)
 |    |-- gender: string (nullable = true)
 |    |-- yob: integer (nullable = true)
 |    |-- age: integer (nullable = true)
 |-- brq_count: long (nullable = true)

>>> df.select('app.bundle').show()
+--------------------+                                                          
|              bundle|
+--------------------+
|         [382617920]|
|[com.imo.android....|
|[com.mxtech.video...|
|[com.imo.android....|
|[com.lenovo.anysh...|
|[com.outfit7.myta...|
|[com.playit.video...|
|[com.mxtech.video...|
|[com.lenovo.anysh...|
|[com.lenovo.anysh...|
|[com.hippogames.l...|
|[com.camerasideas...|
|[com.cricbuzz.and...|
|[com.imo.android....|
|[com.candywriter....|
|[com.lenovo.anysh...|
|[com.miui.player,...|
|[com.linecorp.b61...|
|[com.cricbuzz.and...|
|[com.dencreak.dlc...|
+--------------------+
only showing top 20 rows

>>> df.select('app.asn').show()
+--------------------+                                                          
|                 asn|
+--------------------+
|[Viber Messenger:...|
|[imo beta free ca...|
|         [MX Player]|
|[imo HD-Free Vide...|
|[SHAREit - Transf...|
|[My Talking Tom F...|
|[HD Video Player ...|
|         [MX Player]|
|[SHAREit - Transf...|
|[SHAREit - Transf...|
|[Ludo Master - Ne...|
|[Video Editor & V...|
|[Cricbuzz - Live ...|
|[imo free video c...|
|[BitLife - Life S...|
|[SHAREit - Transf...|
|[Mi Music, B612 -...|
|[B612 - Beauty & ...|
|[Cricbuzz - Live ...|
|[ClevCalc - Calcu...|
+--------------------+
only showing top 20 rows
