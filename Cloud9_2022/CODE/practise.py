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

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/202111/*.parquet')

df.printSchema()


df2=df.select('mm_connection')



req=df.select('ifa',explode('req_connection')).select('ifa','col.*')


>>> req=df.select('ifa',explode('req_connection')).select('ifa','col.*')
>>> req.show()


+--------------------+---------------+---------------+----------------+------------------+------------+------------------+-------------+-----------+
|                 ifa|req_carrier_mcc|req_carrier_mnc|req_carrier_code|  req_carrier_name|req_con_type| req_con_type_desc|req_brq_count|req_country|
+--------------------+---------------+---------------+----------------+------------------+------------+------------------+-------------+-----------+
|00005507-013a-48d...|            470|             01|          470-01|      GrameenPhone|           3|Unknown Generation|           23|         bd|
|0000576b-0006-43c...|            470|             02|          470-02|        Robi/Aktel|           3|Unknown Generation|           13|         bd|
|0000576b-0006-43c...|            470|             02|          470-02|        Robi/Aktel|           5|                3G|            1|         bd|
|0000576b-0006-43c...|            470|             02|          470-02|        Robi/Aktel|           2|              WIFI|            1|         bd|
|0000576b-0006-43c...|            470|             02|          470-02|        Robi/Aktel|           6|                4G|         2409|         bd|
|00005aa3-ded2-45e...|            470|             02|          470-02|        Robi/Aktel|           2|              WIFI|           34|         bd|
|00005aa3-ded2-45e...|            470|             02|          470-02|        Robi/Aktel|           6|                4G|           97|         bd|
|00007739-1015-4ae...|            470|             03|          470-03|Orascom/Banglalink|           3|Unknown Generation|            1|         bd|
|0000d7c0-8c2d-440...|            470|             02|          470-02|        Robi/Aktel|           3|Unknown Generation|            9|         bd|
|000105e6-84a0-494...|            470|             01|          470-01|      GrameenPhone|           3|Unknown Generation|            2|         bd|
|000105e6-84a0-494...|            470|             01|          470-01|      GrameenPhone|           6|                4G|          294|         bd|
|00013779-d721-4fd...|            470|             03|          470-03|Orascom/Banglalink|           6|                4G|           14|         bd|
|00013779-d721-4fd...|            470|             02|          470-02|        Robi/Aktel|           2|              WIFI|           45|         bd|
|000248e6-77c8-430...|            470|             02|          470-02|        Robi/Aktel|           6|                4G|           71|         bd|
|000248e6-77c8-430...|            470|             02|          470-02|        Robi/Aktel|           2|              WIFI|            4|         bd|
|000274ba-aa78-465...|            470|             03|          470-03|Orascom/Banglalink|           6|                4G|            2|         bd|
|00032775-be8f-4da...|            470|             03|          470-03|Orascom/Banglalink|           6|                4G|           35|         bd|
|00032775-be8f-4da...|            470|             03|          470-03|Orascom/Banglalink|           3|Unknown Generation|            1|         bd|
|00032775-be8f-4da...|            470|             03|          470-03|Orascom/Banglalink|           5|                3G|            6|         bd|
|0003adf0-f7b0-4c6...|            470|             01|          470-01|      GrameenPhone|           6|                4G|            4|         bd|
+--------------------+---------------+---------------+----------------+------------------+------------+------------------+-------------+-----------+



cnt=req.select('ifa','req_con_type_desc')

a=cnt.groupBy("req_con_type_desc").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

+------------------+-----------+                                                
| req_con_type_desc|ifa_numbers|
+------------------+-----------+
|                4G|   20829247|
|Unknown Generation|   16467854|
|                3G|    8012250|
|              WIFI|    6421307|
|                2G|    2679608|
|           Unknown|     443019|
|          Ethernet|     124679|
|               NaN|          7|
+------------------+-----------+



operator=req.select('ifa','req_carrier_name')


op_cnt=operator.groupBy('req_carrier_name').agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)


op_cnt.show()

+------------------+-----------+                                                
|  req_carrier_name|ifa_numbers|
+------------------+-----------+
|      GrameenPhone|   13571840|
|        Robi/Aktel|   11341131|
|Orascom/Banglalink|    5960975|
|          TeleTalk|     627185|
+------------------+-----------+

df3=df.select('ifa',explode('mm_connection')).select('ifa','col.*')

df3.printSchema()



l1 =df3.groupBy("mm_con_type_desc").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

+----------------+-----------+                                                  
|mm_con_type_desc|ifa_numbers|
+----------------+-----------+
|        Cellular|   29293656|
|       Cable/DSL|   21459357|
|       Corporate|     954710|
+----------------+-----------+




from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys


country='BD'
time='monthly'
m='202001'



df = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/'+country+'/'+m+'/*.parquet')


df2 = df.select('ifa', explode('mm_connection')).select('ifa', 'col.*')


df3 = df2.select('ifa', 'mm_con_type_desc', 'mm_carrier_name')

geo = spark.read.parquet('s3a://ada-bd-emr/result/2022/Robi/DSR-623/raw/bl/'+m+'/*.parquet')

geo.printSchema()

df4=geo.join(df3,on='ifa',how='left')

dual_simmers = dual_simmers.groupBy('ifa').agg(countDistinct('mm_carrier_name').alias('sims'))



dual_simmers = dual_simmers.filter(col('sims') > 1).withColumn('dual_sim', F.lit(1))



dual_simmers = df4.filter(col('mm_con_type_desc') == 'Cellular').select('ifa', 'mm_carrier_name').distinct()
 
carrier_name=df4.filter(col('mm_con_type_desc') == 'Cellular').select('ifa', 'mm_carrier_name').distinct()
dual_simmers = dual_simmers.groupBy('ifa').agg(countDistinct('mm_carrier_name').alias('sims'))
dual_simmers = dual_simmers.filter(col('sims') > 1).withColumn('dual_sim', F.lit(1))
sims_12=dual_simmers.filter(col('sims')==12).withColumn('sims_12',F.lit(12))







