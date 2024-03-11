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

master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_master_v7.0.csv',header=True)

master.show()
+-------+-------+------------+------------+-------------+------------+---------+--------------------+----------+----------+------+-------+
|country|user_id|updated_date|created_date|     level_id|affluence_id|unique_id|                name|  latitude| longitude|radius|geohash|
+-------+-------+------------+------------+-------------+------------+---------+--------------------+----------+----------+------+-------+
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100034|Sahebganj Footbal...|26.1179737|89.5973039|   100| tuxswf|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000| BD_10015|Katakhali Playground|25.0977799|90.0435437|   100| wh2nbb|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100154|Cheuria Mondolpar...|23.8993615|89.1558883|   100| tupruy|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100156|SAHEB GANJ VC FOO...|26.1178379|89.5973737|   100| tuxswf|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000| BD_10027| BPL Ground, Bhaluka|25.2011427|90.0475593|   100| wh2p35|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100277|Aklash Shibpur Sh...|24.9753439|89.1580129|   100| turqhf|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000| BD_10039|Shafia Sharif Spo...|23.2066833|90.0489902|   100| wh0h10|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100401|Manikgonj Hock St...|22.9909934|89.1581027|   100| tup6uc|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100411| Playground Abdullah|24.6969635|  89.59962|   100| tursqz|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000| BD_10051|Khailsindur play ...|24.0557201|90.0531832|   100| wh20c5|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100524| Batason Play Ground|25.5963602|89.1598026|   100| tux3sg|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100533|Hatshira Purbopar...|24.6487075|89.5999274|   100| tursnz|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000| BD_10063|          Balur math| 22.675755|90.0532368|   100| wh00cp|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100647|Bakshigonj Primar...| 25.878916|89.1598564|   100| tux7hz|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100655|Halipad playgroun...|22.4680179|89.6007398|   100| tgzxyf|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000| BD_10075|Bukabunia School ...|22.3176519|90.0538534|   100| w5bncn|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100770|Churamankathi Hig...| 23.215737|89.1661988|   100| tupkj4|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100777|      EPZ Playground|22.4878132|89.6009812|   100| tgzxyv|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000| BD_10088|Bhandaria Shishu ...|22.4869274|90.0587943|   100| w5bpcm|
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|      AFF000|BD_100893|       CP Playground| 24.616249| 89.167022|   100| turkj1|
+-------+-------+------------+------------+-------------+------------+---------+--------------------+----------+----------+------+-------+



tax=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_taxonomy_v7.0.csv',header=True)
tax.show()
+-------+-------+------------+------------+-------------+--------------------+--------------------+----------+--------------------+
|country|user_id|updated_date|created_date|     level_id|             l1_name|             l2_name|   l3_name|             l4_name|
+-------+-------+------------+------------+-------------+--------------------+--------------------+----------+--------------------+
|     BD|  admin|  21/02/2022|  18/01/2022|ADV_03_09_001|Adventure And Lei...|                Park|Playground|              others|
|     BD|  admin|  21/02/2022|  18/01/2022|APT_01_01_010|             Airport|    Domestic Airport|    others|     Barisal Airport|
|     BD|  admin|  21/02/2022|  18/01/2022|APT_01_01_019|             Airport|    Domestic Airport|    others|     Jashore Airport|
|     BD|  admin|  21/02/2022|  18/01/2022|APT_01_01_043|             Airport|    Domestic Airport|    others|     Saidpur Airport|
|     BD|  admin|  21/02/2022|  18/01/2022|APT_01_01_045|             Airport|    Domestic Airport|    others|Shah Makhdum Airport|
|     BD|  admin|  21/02/2022|  18/01/2022|APT_02_01_012|             Airport|International Air...|    others|Cox's Bazar Airport.|
|     BD|  admin|  21/02/2022|  18/01/2022|APT_02_01_022|             Airport|International Air...|    others|Hazrat Shahjalal ...|
|     BD|  admin|  21/02/2022|  18/01/2022|APT_02_01_044|             Airport|International Air...|    others|Osmani Internatio...|
|     BD|  admin|  21/02/2022|  18/01/2022|APT_02_01_052|             Airport|International Air...|    others|Shah Amanat Inter...|
|     BD|  admin|  21/02/2022|  18/01/2022|AUT_01_01_001|          Automotive|  Car Repair Service|    others|              others|
|     BD|  admin|  21/02/2022|  18/01/2022|AUT_01_01_004|          Automotive|  Car Repair Service|    others|                Audi|
|     BD|  admin|  21/02/2022|  18/01/2022|AUT_01_01_007|          Automotive|  Car Repair Service|    others|                 BMW|
|     BD|  admin|  21/02/2022|  18/01/2022|AUT_01_01_016|          Automotive|  Car Repair Service|    others|               Honda|
|     BD|  admin|  21/02/2022|  18/01/2022|AUT_01_01_017|          Automotive|  Car Repair Service|    others|             Hyundai|
|     BD|  admin|  21/02/2022|  18/01/2022|AUT_01_01_021|          Automotive|  Car Repair Service|    others|                 Kia|
|     BD|  admin|  21/02/2022|  18/01/2022|AUT_01_01_032|          Automotive|  Car Repair Service|    others|       Mercedes-Benz|
|     BD|  admin|  21/02/2022|  18/01/2022|AUT_01_01_034|          Automotive|  Car Repair Service|    others|          Mitsubishi|
|     BD|  admin|  21/02/2022|  18/01/2022|AUT_01_01_035|          Automotive|  Car Repair Service|    others|              Nissan|
|     BD|  admin|  21/02/2022|  18/01/2022|AUT_01_01_039|          Automotive|  Car Repair Service|    others|              Proton|
|     BD|  admin|  21/02/2022|  18/01/2022|AUT_01_01_040|          Automotive|  Car Repair Service|    others|             Renault|
+-------+-------+------------+------------+-------------+--------------------+--------------------+----------+--------------------+
only showing top 20 rows


 master.printSchema()

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


tax.printSchema()

root
 |-- country: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- updated_date: string (nullable = true)
 |-- created_date: string (nullable = true)
 |-- level_id: string (nullable = true)
 |-- l1_name: string (nullable = true)
 |-- l2_name: string (nullable = true)
 |-- l3_name: string (nullable = true)
 |-- l4_name: string (nullable = true)
 

POI_TAX=master.join(tax,"level_id","left")

POI_TAX.printSchema()


root
 |-- level_id: string (nullable = true)
 |-- country: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- updated_date: string (nullable = true)
 |-- created_date: string (nullable = true)
 |-- affluence_id: string (nullable = true)
 |-- unique_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- radius: string (nullable = true)
 |-- geohash: string (nullable = true)
 |-- country: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- updated_date: string (nullable = true)
 |-- created_date: string (nullable = true)
 |-- l1_name: string (nullable = true)
 |-- l2_name: string (nullable = true)
 |-- l3_name: string (nullable = true)
 |-- l4_name: string (nullable = true)
 

print((POI_TAX.count(), len(POI_TAX.columns)))

(106670, 20)

>>> POI_TAX.select('level_id','l1_name','l2_name','l3_name','l4_name','geohash').show()
+-------------+--------------------+-------+----------+-------+-------+
|     level_id|             l1_name|l2_name|   l3_name|l4_name|geohash|
+-------------+--------------------+-------+----------+-------+-------+
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| tuxswf|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| wh2nbb|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| tupruy|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| tuxswf|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| wh2p35|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| turqhf|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| wh0h10|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| tup6uc|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| tursqz|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| wh20c5|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| tux3sg|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| tursnz|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| wh00cp|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| tux7hz|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| tgzxyf|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| w5bncn|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| tupkj4|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| tgzxyv|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| w5bpcm|
|ADV_03_09_001|Adventure And Lei...|   Park|Playground| others| turkj1|
+-------------+--------------------+-------+----------+-------+-------+
only showing top 20 rows



POI_TAX.groupby("level_id").count().show(200)


+-------------+-----+
|     level_id|count|
+-------------+-----+
|EDU_01_09_125|    1|
|FIN_01_01_117|  130|
|FNB_05_01_023|    6|
|EDU_02_05_001|  138|
|RET_11_01_037|   25|
|TEL_01_01_045|  366|
|APT_02_01_022|    1|
|EDU_03_07_144|    2|
|AUT_02_01_028|  148|
|FIN_01_02_004|  451|
|FIN_01_02_041|  172|
|RET_02_01_039|    6|
|EDU_01_09_129|    1|
|STY_01_01_008|    3|
|RET_05_03_001| 1320|
|FIN_01_01_119|  357|
|FIN_01_01_126|  129|
|HLT_01_01_160|    1|
|FIN_01_01_166|    1|
|FIN_01_02_049|  649|
|BTY_01_01_056|    1|
|FIN_01_02_045|  743|
|RET_02_01_001| 3427|
|AUT_01_01_032|    2|
|FIN_01_01_083|   12|
|RET_02_01_065|   42|
|FNB_09_06_001|   25|
|FIN_01_02_003|  374|
|STY_01_01_009|    3|
|ADV_03_09_001| 2239|
|FIN_01_02_013|   51|
|FIN_01_02_008|  183|
|EDU_03_07_150|    1|
|REL_03_01_001| 4235|
|RET_02_01_064|    5|
|RET_11_01_039|   17|
|FIN_01_02_026|  214|
|FIN_01_02_046|   83|
|HLT_01_03_065|    1|
|AUT_02_01_063|   11|
|FIN_01_01_077|  171|
|FIN_01_01_127|  121|
|FIN_01_02_023|  403|
|FNB_09_08_001|  332|
|RET_08_01_002|   11|
|FNB_05_01_011|    5|
|FNB_05_01_074|   10|
|EDU_01_09_128|    1|
|EDU_03_07_161|    1|
|ENT_02_01_001|  259|
|FNB_05_01_072|   31|
|FIN_01_01_128|   76|
|FIN_01_02_017|   53|
|FIN_01_02_020|   16|
|FNB_03_01_066|    2|
|HLT_02_02_001| 2014|
|EDU_02_07_001|   91|
|ENT_01_01_004|    8|
|FNB_05_01_073|    5|
|HLT_02_01_001| 1754|
|RET_14_02_001|   27|
|REL_06_01_001|    3|
|RET_05_03_002|  273|
|FNB_09_03_001| 2371|
|RET_03_01_001|   67|
|FNB_03_01_001|  677|
|FIN_01_02_001|  294|
|WOR_01_01_028|    7|
|APT_01_01_043|    1|
|AUT_01_01_017|    6|
|RET_05_03_018|  114|
|TEL_01_01_027|  395|
|EDU_03_07_137|    1|
|FIN_01_01_091|  449|
|APT_01_01_019|    1|
|RET_08_01_001| 1974|
|FIN_01_01_065|  119|
|FIN_01_02_015|  227|
|RET_05_01_015|   57|
|FIN_01_01_114|   70|
|EDU_01_09_131|    1|
|FIN_01_01_112|   44|
|FIN_01_02_044|   38|
|FNB_03_01_017|    3|
|AUT_02_01_048|    7|
|EDU_01_09_123|    1|
|FNB_01_01_032|   14|
|RET_12_01_009|    1|
|AUT_01_01_001| 1355|
|EDU_01_09_120|    1|
|FIN_01_01_026|  276|
|FIN_01_01_087|   13|
|AUT_01_01_039|    3|
|EDU_03_07_154|    1|
|RET_02_01_060|    7|
|FIN_01_02_019|  143|
|FNB_05_01_034|    8|
|RET_04_01_009|    8|
|EDU_03_07_139|    1|
|FIN_02_01_021|   30|
|OIL_01_03_001|  240|
|AUT_01_01_061|    9|
|FIN_01_01_115|  105|
|WOR_01_01_005|    1|
|AUT_02_01_034|    6|
|RET_05_01_088|   20|
|RET_02_01_023|    4|
|FIN_01_01_086|  159|
|FNB_03_01_069|    2|
|FIN_01_01_089|   67|
|REL_05_01_001|13321|
|FIN_01_02_028|  259|
|AUT_02_01_059|  129|
|FIN_01_01_122|  122|
|FNB_03_01_067|    3|
|RET_05_01_091|   13|
|EDU_03_07_162|    1|
|FIN_01_02_043|  157|
|FIN_01_01_165|  375|
|FNB_03_01_065|    2|
|FNB_07_01_001|  862|
|EDU_03_07_164|    1|
|FIN_01_01_106|  135|
|FNB_06_01_001| 1777|
|HOT_01_05_001|   47|
|FNB_03_01_028|    4|
|BTY_01_01_001|  439|
|FIN_01_01_100|   31|
|FIN_01_01_164|  786|
|RET_09_01_298|    1|
|FIN_01_02_033|   43|
|FNB_01_01_034|    7|
|AUT_01_01_004|    4|
|FIN_01_02_031|    6|
|HLT_01_03_066|    1|
|FIN_01_02_030|   27|
|FIN_01_02_050|   53|
|HLT_01_01_158|    1|
|FIN_01_01_041|  166|
|RET_02_01_063|   11|
|TEL_01_01_011| 1151|
|FIN_01_01_095|  107|
|RET_09_01_001| 2147|
|AUT_01_01_021|    2|
|FNB_09_02_001|  653|
|FIN_01_01_110|    8|
|HLT_01_03_063|    1|
|AUT_01_01_044|   28|
|EDU_01_09_126|    1|
|EDU_02_09_001|   11|
|FIN_01_02_034|    4|
|HLT_01_03_073|    1|
|FIN_01_01_084|   76|
|FNB_01_01_001|  472|
|FNB_05_01_075|   19|
|EDU_01_09_122|    1|
|FIT_02_01_001| 1125|
|FIN_01_02_021|  106|
|FNB_05_01_050|   10|
|APT_02_01_012|    1|
|RET_12_01_058|  255|
|AUT_02_01_044|    2|
|EDU_03_07_163|    1|
|FIN_01_01_130|  241|
|ENT_01_01_003|    2|
|EDU_01_09_119|    1|
|FIN_01_02_038|   34|
|EDU_03_07_153|    1|
|FIN_01_01_093|  221|
|FIN_01_02_048|  423|
|HOT_01_03_001|   76|
|FIN_01_02_010|  201|
|RET_05_01_092|   12|
|ENT_05_01_001|  540|
|FIN_01_02_009|  198|
|FNB_03_01_063|    2|
|FIN_01_01_099|    8|
|FIN_01_02_006|  125|
|FIN_01_02_018|  134|
|EDU_03_07_001|  108|
|FIN_01_01_104|  308|
|FNB_05_01_039|    5|
|EDU_01_07_001| 2032|
|FIN_01_02_042|   79|
|HLT_01_03_001| 1783|
|RET_02_01_062|    7|
|EDU_03_07_146|    1|
|FIN_01_02_014|   40|
|FIN_01_02_029|    3|
|RET_09_01_299|    1|
|AUT_01_01_040|    2|
|FIN_01_02_025|  193|
|EDU_03_07_138|    1|
|FIN_01_01_094|   19|
|EDU_03_07_152|    1|
|AUT_02_01_047|    1|
|FIN_01_01_034|   87|
|EDU_03_07_147|    1|
|RET_05_01_087|   14|
|FIN_01_02_022|  208|
+-------------+-----+
only showing top 200 rows




POI_TAX.groupby("l1_name").count().show(200)


+--------------------+-----+
|             l1_name|count|
+--------------------+-----+
|           Education| 9571|
|         Oil And Gas| 2657|
|       Entertainment| 1595|
|          Healthcare| 7125|
|Adventure And Lei...| 2239|
|          Automotive| 3669|
|  Food And Beverages|10343|
|  Telecommunications| 2643|
| Beauty And Wellness|  449|
|         Hospitality| 2139|
|  Sports And Fitness| 1584|
|             Airport|    8|
|           Financial|22108|
|              Retail|21530|
|       Place Of Stay|   17|
|    Religious Places|18959|
|       Place Of Work|   34|
+--------------------+-----+

l2=POI_TAX.groupby("l2_name").count()
l2.show(200)

+--------------------+-----+
|             l2_name|count|
+--------------------+-----+
|              Market| 3996|
|         Fine Dining| 1777|
|    Domestic Airport|    4|
|                Bank|21876|
|             Devices|    8|
|              Bakery|  605|
|                 Bar|  796|
|           Christian| 1090|
|       Optical Store|  795|
|    Foreign Exchange|  232|
|         Golf Course|   43|
|             Fashion| 1964|
|International Air...|    4|
|Government Education| 4032|
|                Mall| 2689|
|   Private Education| 5278|
|         Supermarket| 1539|
|     Beauty Retailer|  601|
|         Home Living|   56|
|   Business District|   34|
|            Buddhist|  310|
|         Electronics|  104|
|   Food Court Hawker|  862|
|           Fast Food| 2434|
|    Department Store|   67|
|       Telco Dealers| 2643|
|     Gym And Fitness| 1125|
|            Jeweller| 1991|
|   Convenience Store| 3522|
|                Sikh|    3|
|           Beverages|   10|
|               Hotel| 2139|
|              Muslim|13321|
|              Cinema|  259|
|       Swimming Pool|  416|
|           Nightclub|  540|
|               Hindu| 4235|
|     Retail Pharmacy| 4198|
|            Hospital| 3357|
|          Restaurant| 3937|
|              Clinic| 3768|
|            Showroom| 1955|
|    Aesthetic Clinic|  449|
|Affluent Neighbou...|   17|
|International Edu...|  261|
|            Car Wash|  250|
|                Park| 2239|
|                Cafe|  718|
|      Petrol Station| 2657|
|  Car Repair Service| 1464|
+--------------------+-----+


>>> l3=POI_TAX.groupby("l3_name").count()
>>> l3.show(200)
+------------------+-----+
|           l3_name|count|
+------------------+-----+
|  Secondary School| 6109|
|     Japanese Food|    5|
|          Tertiary|  216|
|    Primary School| 2169|
|               ATM|10241|
|        Playground| 2239|
|            1 Star|  332|
|      Italian Food|   84|
|Mobile Phone Store|   27|
|            2 Star|   47|
|             Plaza|  539|
|       French Food|   25|
|      Chinese Food|  140|
|        Government| 1588|
|International Food|  332|
|           Private| 1792|
|          Footwear| 1772|
|            others|57031|
|            Branch|11635|
|            Dental| 2014|
|            4 Star| 1135|
|            3 Star|  549|
|               CNG|  517|
|        Asian Food| 2371|
|         Furniture|   48|
|        Pre School| 1077|
|               LPG|  240|
|     American Food|  653|
|               OIL| 1667|
|            5 Star|   76|
+------------------+-----+


>>> l3.write.csv('l3_grp.csv')
>>> l2.write.csv('l2_grp.csv')                                                  
>>> l1.write.csv('l1_grp.csv')                                                  

>>> l1=POI_TAX.groupby("l1_name").count()
>>> l1.write.csv('l1_grp.csv')
