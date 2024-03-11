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


df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{01}/*.parquet')
df.show()


df.select('ifa',F.explode('gps.geohash').alias('geohash')).show()


ifa_data=df.select('ifa',F.explode('gps.geohash').alias('geohash'))

ifa_data.withColumn("new_geohash", substring(col("geohash"),1,6))

ifa_new=ifa_data.withColumn("new_geohash", substring(col("geohash"),1,6))

>>> ifa_new.show()

+--------------------+---------+-----------+                                    
|                 ifa|  geohash|new_geohash|
+--------------------+---------+-----------+
|0000fa9c-ef42-435...|wh0ub55k7|     wh0ub5|
|0000fa9c-ef42-435...|wh0tb7s21|     wh0tb7|
|0000fa9c-ef42-435...|wh0twyzxn|     wh0twy|
|00011557-8e66-4ed...|wh0r0vyz8|     wh0r0v|
|000227b4-8100-4e9...|tuxu8b1x3|     tuxu8b|
|000227b4-8100-4e9...|tuxu8b1p0|     tuxu8b|
|00028a5e-aa41-465...|turs7fcj6|     turs7f|
|00028a5e-aa41-465...|turs6u2rb|     turs6u|
|00028a5e-aa41-465...|turs568j4|     turs56|
|00028a5e-aa41-465...|turs6ddf1|     turs6d|
|00028a5e-aa41-465...|turs5rjfj|     turs5r|
|0002b564-dffe-44e...|wh0qcknpr|     wh0qck|
|0002b564-dffe-44e...|wh0qce13h|     wh0qce|
|0002b564-dffe-44e...|wh0r36gpq|     wh0r36|
|00035693-cd5c-43d...|wh0ub55k7|     wh0ub5|
|00035693-cd5c-43d...|wh0qbdb2z|     wh0qbd|
|00037bf4-b8c8-402...|wh0qbdb2z|     wh0qbd|
|0003de19-6241-4f8...|tux5mxyph|     tux5mx|
|0003de19-6241-4f8...|tux5w03rz|     tux5w0|
|0003f4d2-5483-40f...|wh0pwmt0s|     wh0pwm|
+--------------------+---------+-----------+
only showing top 20 rows


ifa_new.drop(ifa_new.geohash)

ifa_new=ifa_new.drop(ifa_new.geohash)

ifa_new.printSchema()


root
 |-- ifa: string (nullable = true)
 |-- new_geohash: string (nullable = true)
 
ifa_new.withColumnRenamed("new_geohash","geohash").printSchema()

root
 |-- ifa: string (nullable = true)
 |-- geohash: string (nullable = true)

ifa_new=ifa_new.withColumnRenamed("new_geohash","geohash")
>>> ifa_new.show()
+--------------------+-------+                                                  
|                 ifa|geohash|
+--------------------+-------+
|0000fa9c-ef42-435...| wh0ub5|
|0000fa9c-ef42-435...| wh0tb7|
|0000fa9c-ef42-435...| wh0twy|
|00011557-8e66-4ed...| wh0r0v|
|000227b4-8100-4e9...| tuxu8b|
|000227b4-8100-4e9...| tuxu8b|
|00028a5e-aa41-465...| turs7f|
|00028a5e-aa41-465...| turs6u|
|00028a5e-aa41-465...| turs56|
|00028a5e-aa41-465...| turs6d|
|00028a5e-aa41-465...| turs5r|
|0002b564-dffe-44e...| wh0qck|
|0002b564-dffe-44e...| wh0qce|
|0002b564-dffe-44e...| wh0r36|
|00035693-cd5c-43d...| wh0ub5|
|00035693-cd5c-43d...| wh0qbd|
|00037bf4-b8c8-402...| wh0qbd|
|0003de19-6241-4f8...| tux5mx|
|0003de19-6241-4f8...| tux5w0|
|0003f4d2-5483-40f...| wh0pwm|
+--------------------+-------+
only showing top 20 rows

master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_master_v7.0.csv',header=True)

>>> master.printSchema()
root
 |-- country: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- updated_date: string (nullable = true)
 |-- created_date: string (nullable = true)
 |-- level_id: string (nullable = true)
 |-- affluence_id: string (nullable = true)
 |-- unique_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- radius: string (nullable = true)
 |-- geohash: string (nullable = true)


>>> master.select('level_id','name','geohash').show()


+-------------+--------------------+-------+
|     level_id|                name|geohash|
+-------------+--------------------+-------+
|ADV_03_09_001|Sahebganj Footbal...| tuxswf|
|ADV_03_09_001|Katakhali Playground| wh2nbb|
|ADV_03_09_001|Cheuria Mondolpar...| tupruy|
|ADV_03_09_001|SAHEB GANJ VC FOO...| tuxswf|
|ADV_03_09_001| BPL Ground, Bhaluka| wh2p35|
|ADV_03_09_001|Aklash Shibpur Sh...| turqhf|
|ADV_03_09_001|Shafia Sharif Spo...| wh0h10|
|ADV_03_09_001|Manikgonj Hock St...| tup6uc|
|ADV_03_09_001| Playground Abdullah| tursqz|
|ADV_03_09_001|Khailsindur play ...| wh20c5|
|ADV_03_09_001| Batason Play Ground| tux3sg|
|ADV_03_09_001|Hatshira Purbopar...| tursnz|
|ADV_03_09_001|          Balur math| wh00cp|
|ADV_03_09_001|Bakshigonj Primar...| tux7hz|
|ADV_03_09_001|Halipad playgroun...| tgzxyf|
|ADV_03_09_001|Bukabunia School ...| w5bncn|
|ADV_03_09_001|Churamankathi Hig...| tupkj4|
|ADV_03_09_001|      EPZ Playground| tgzxyv|
|ADV_03_09_001|Bhandaria Shishu ...| w5bpcm|
|ADV_03_09_001|       CP Playground| turkj1|
+-------------+--------------------+-------+
only showing top 20 rows

>>> master_new=master.select('level_id','name','geohash')
>>> master_show()

>>> master_new.show()
+-------------+--------------------+-------+
|     level_id|                name|geohash|
+-------------+--------------------+-------+
|ADV_03_09_001|Sahebganj Footbal...| tuxswf|
|ADV_03_09_001|Katakhali Playground| wh2nbb|
|ADV_03_09_001|Cheuria Mondolpar...| tupruy|
|ADV_03_09_001|SAHEB GANJ VC FOO...| tuxswf|
|ADV_03_09_001| BPL Ground, Bhaluka| wh2p35|
|ADV_03_09_001|Aklash Shibpur Sh...| turqhf|
|ADV_03_09_001|Shafia Sharif Spo...| wh0h10|
|ADV_03_09_001|Manikgonj Hock St...| tup6uc|
|ADV_03_09_001| Playground Abdullah| tursqz|
|ADV_03_09_001|Khailsindur play ...| wh20c5|
|ADV_03_09_001| Batason Play Ground| tux3sg|
|ADV_03_09_001|Hatshira Purbopar...| tursnz|
|ADV_03_09_001|          Balur math| wh00cp|
|ADV_03_09_001|Bakshigonj Primar...| tux7hz|
|ADV_03_09_001|Halipad playgroun...| tgzxyf|
|ADV_03_09_001|Bukabunia School ...| w5bncn|
|ADV_03_09_001|Churamankathi Hig...| tupkj4|
|ADV_03_09_001|      EPZ Playground| tgzxyv|
|ADV_03_09_001|Bhandaria Shishu ...| w5bpcm|
|ADV_03_09_001|       CP Playground| turkj1|
+-------------+--------------------+-------+
only showing top 20 rows


master_new=master_new.withColumn("level_id_new", substring(col("level_id"),1,3))

master_new.show

+-------------+--------------------+-------+------------+
|     level_id|                name|geohash|level_id_new|
+-------------+--------------------+-------+------------+
|ADV_03_09_001|Sahebganj Footbal...| tuxswf|         ADV|
|ADV_03_09_001|Katakhali Playground| wh2nbb|         ADV|
|ADV_03_09_001|Cheuria Mondolpar...| tupruy|         ADV|
|ADV_03_09_001|SAHEB GANJ VC FOO...| tuxswf|         ADV|
|ADV_03_09_001| BPL Ground, Bhaluka| wh2p35|         ADV|
|ADV_03_09_001|Aklash Shibpur Sh...| turqhf|         ADV|
|ADV_03_09_001|Shafia Sharif Spo...| wh0h10|         ADV|
|ADV_03_09_001|Manikgonj Hock St...| tup6uc|         ADV|
|ADV_03_09_001| Playground Abdullah| tursqz|         ADV|
|ADV_03_09_001|Khailsindur play ...| wh20c5|         ADV|
|ADV_03_09_001| Batason Play Ground| tux3sg|         ADV|
|ADV_03_09_001|Hatshira Purbopar...| tursnz|         ADV|
|ADV_03_09_001|          Balur math| wh00cp|         ADV|
|ADV_03_09_001|Bakshigonj Primar...| tux7hz|         ADV|
|ADV_03_09_001|Halipad playgroun...| tgzxyf|         ADV|
|ADV_03_09_001|Bukabunia School ...| w5bncn|         ADV|
|ADV_03_09_001|Churamankathi Hig...| tupkj4|         ADV|
|ADV_03_09_001|      EPZ Playground| tgzxyv|         ADV|
|ADV_03_09_001|Bhandaria Shishu ...| w5bpcm|         ADV|
|ADV_03_09_001|       CP Playground| turkj1|         ADV|
+-------------+--------------------+-------+------------+
only showing top 20 rows


master_new.groupby("level_id_new").count().show(200)

+------------+-----+                                                            
|level_id_new|count|
+------------+-----+
|         ENT| 1595|
|         REL|18959|
|         STY|   17|
|         TEL| 2643|
|         HOT| 2139|
|         BTY|  449|
|         EDU| 9571|
|         FIN|22108|
|         RET|21530|
|         AUT| 3669|
|         FIT| 1584|
|         WOR|   34|
|         FNB|10343|
|         HLT| 7125|
|         OIL| 2657|
|         ADV| 2239|
|         APT|    8|
+------------+-----+

master_new.filter(master_new.level_id_new=='APT').show()

>>> master_new.filter(master_new.level_id_new=='APT').show()
+-------------+--------------------+-------+------------+
|     level_id|                name|geohash|level_id_new|
+-------------+--------------------+-------+------------+
|APT_01_01_010|     Barisal Airport| wh0ctw|         APT|
|APT_01_01_019|     Jashore Airport| tup7ug|         APT|
|APT_01_01_043|     Saidpur Airport| tux4x4|         APT|
|APT_01_01_045|Shah Makhdum Airport| tur506|         APT|
|APT_02_01_012| Cox's Bazar Airport| w5c6hb|         APT|
|APT_02_01_022|Hazrat Shahjalal ...| wh0r9h|         APT|
|APT_02_01_044|Osmani Internatio...| wh3mfx|         APT|
|APT_02_01_052|Shah Amanat Inter...| w5cq93|         APT|
+-------------+--------------------+-------+------------+

airport=master_new.filter(master_new.level_id_new=='APT')
airport.show()

+-------------+--------------------+-------+------------+
|     level_id|                name|geohash|level_id_new|
+-------------+--------------------+-------+------------+
|APT_01_01_010|     Barisal Airport| wh0ctw|         APT|
|APT_01_01_019|     Jashore Airport| tup7ug|         APT|
|APT_01_01_043|     Saidpur Airport| tux4x4|         APT|
|APT_01_01_045|Shah Makhdum Airport| tur506|         APT|
|APT_02_01_012| Cox's Bazar Airport| w5c6hb|         APT|
|APT_02_01_022|Hazrat Shahjalal ...| wh0r9h|         APT|
|APT_02_01_044|Osmani Internatio...| wh3mfx|         APT|
|APT_02_01_052|Shah Amanat Inter...| w5cq93|         APT|
+-------------+--------------------+-------+------------+


APT=ifa_new.join(airport,"geohash","left")

APT=airport.join(ifa_new,"geohash","left")

APT.distinct().count()

APT.count()

#AIRPORT

23907      

ADV=master_new.filter(master_new.level_id_new=='ADV')

adv_join=ADV.join(ifa_new,"geohash","left")

adv_join.distinct().count()


adv_join.count()


OIL=master_new.filter(master_new.level_id_new=='OIL')

oil_join=OIL.join(ifa_new,"geohash","left")

oil_join.distinct().count()

oil_join.count()


HLT=master_new.filter(master_new.level_id_new=='HLT')

HLT_join=HLT.join(ifa_new,"geohash","left")

HLT_join.distinct().count()

HLT_join.count()


FNB=master_new.filter(master_new.level_id_new=='FNB')

FNB_join=FNB.join(ifa_new,"geohash","left")

FNB_join.distinct().count()

FNB_join.count()



WOR=master_new.filter(master_new.level_id_new=='WOR')

WOR_join=WOR.join(ifa_new,"geohash","left")

WOR_join.distinct().count()

WOR_join.count()



FIT=master_new.filter(master_new.level_id_new=='FIT')

FIT_join=FIT.join(ifa_new,"geohash","left")

FIT_join.distinct().count()

FIT_join.count()



AUT=master_new.filter(master_new.level_id_new=='AUT')

AUT_join=WOR.join(ifa_new,"geohash","left")

AUT_join.distinct().count()

AUT_join.count()


RET=master_new.filter(master_new.level_id_new=='RET')

RET_join=RET.join(ifa_new,"geohash","left")

RET_join.distinct().count()

RET_join.count()



FIN=master_new.filter(master_new.level_id_new=='FIN')

FIN_join=FIN.join(ifa_new,"geohash","left")

FIT_join.distinct().count()

FIN_join.count()




EDU=master_new.filter(master_new.level_id_new=='FIN')

EDU_join=EDU.join(ifa_new,"geohash","left")

EDU_join.distinct().count()

EDU_join.count()



BTY=master_new.filter(master_new.level_id_new=='BTY')

BTY_join=BTY.join(ifa_new,"geohash","left")

BTY_join.distinct().count()

BTY_join.count()



HOT=master_new.filter(master_new.level_id_new=='HOT')

HOT_join=HOT.join(ifa_new,"geohash","left")

HOT_join.distinct().count()

HOT_join.count()


TEL=master_new.filter(master_new.level_id_new=='TEL')

TEL_join=TEL.join(ifa_new,"geohash","left")

TEL_join.distinct().count()

TEL_join.count()



STY=master_new.filter(master_new.level_id_new=='STY')

STY_join=STY.join(ifa_new,"geohash","left")

STY_join.distinct().count()

STY_join.count()



REL=master_new.filter(master_new.level_id_new=='REL')

REL_join=REL.join(ifa_new,"geohash","left")

REL_join.distinct().count()

REL_join.count()




ENT=master_new.filter(master_new.level_id_new=='ENT')

ENT_join=ENT.join(ifa_new,"geohash","left")

ENT_join.distinct().count()


ENT_join.count()



+------------+-----+                                                            
|level_id_new|count|
+------------+-----+
|         ENT| 1595|
|         REL|18959|
|         STY|   17|
|         TEL| 2643|
|         HOT| 2139|
|         BTY|  449|
|         EDU| 9571|
|         FIN|22108|
|         RET|21530|
|         AUT| 3669|
|         FIT| 1584|
|         WOR|   34|
|         FNB|10343|
|         HLT| 7125|
|         OIL| 2657|
|         ADV| 2239|
|         APT|    8|
+------------+-----+