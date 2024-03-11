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



master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_master_v7.0.csv',header=True)
    
master.printSchema()
    
    
tax=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_taxonomy_v7.0.csv',header=True)
    
tax.printSchema()
#making dataset for 24-34

df_brq=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20220101/*.parquet')

df_brq.printSchema()

gps_df=df_brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')

app_df=df_brq.select('ifa','app.bundle')

app_df=app_df.select('ifa',F.explode('bundle').alias('bundle'))

gps_app_df=app_df.join(gps_df,"ifa","left")

gps_df=gps_app_df.select('ifa','geohash','last_seen','bundle')

gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))

gps_df=gps_df.drop(gps_df.geohash)

gps_df=gps_df.withColumnRenamed("new_geohash","geohash")


# gps_df.show()


from pyspark.sql.functions import date_format

gps_df=gps_df.withColumn("time", date_format('last_seen', 'HH'))

gps_df=gps_df.drop(gps_df.last_seen)

gps_df.show()


df_gender = spark.read.parquet('s3a://ada-prod-data/etl/table/brq/sub/demographics/monthly/BD/202101/gender/*.parquet')

df_gender.printSchema()

df_gender=df_gender.select('ifa','label')

df_gender=df_gender.withColumnRenamed('label','gender')

df_gender.show()

df_age = spark.read.parquet('s3a://ada-prod-data/etl/table/brq/sub/demographics/monthly/BD/202101//age/*.parquet')
df_age=df_age.select('ifa','label')
df_age=df_age.withColumnRenamed('label','age')
df_age.show()
df_gender_age=df_gender.join(df_age,"ifa","left")
df_gps_gender_age=df_gender_age.join(gps_df,"ifa","left")
df_gps_gender_age=df_gps_gender_age.filter((df_gps_gender_age.geohash != 'null')|(df_gps_gender_age.time !='null'))
df_gps_gender_age_25_34=df_gps_gender_age.filter(df_gps_gender_age.age=='25-34')

# device = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/device/monthly/BD/202201/*.parquet')

# device = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/device/daily/BD/20210101/*.parquet')

# device.printSchema()

device = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/device/monthly/BD/202101/*.parquet')
device = device.select('ifa', 'device.pricegrade')
device= device.withColumn("affluence", F.lit(None))
device= device.withColumn('affluence', F.when((device.pricegrade ==1) , 'high').otherwise(device.affluence))
device= device.withColumn('affluence', F.when((device.pricegrade ==2) , 'mid').otherwise(device.affluence))
device= device.withColumn('affluence', F.when((device.pricegrade == 3), 'low').otherwise(device.affluence))
device=device.drop(device.pricegrade)
df_gender_age_affluence=device.join(df_gps_gender_age_25_34,"ifa","left")

#df_gender_age_affluence=df_gender_age_affluence.filter((df_gender_age_affluence.affluence=='mid')|(df_gender_age_affluence.gender!='null')|(df_gender_age_affluence.age!='null')|(df_gender_age_affluence.geohash !='null')|(df_gender_age_affluence.time!='null'))

df_gender_age_affluence=df_gender_age_affluence.filter((df_gender_age_affluence.affluence=='mid')&(df_gender_age_affluence.gender!='null')&(df_gender_age_affluence.age!='null')&(df_gender_age_affluence.geohash !='null')&(df_gender_age_affluence.time!='null'))

df_gender_age_affluence_6_22=df_gender_age_affluence.filter((df_gender_age_affluence.time > 6 ) & (df_gender_age_affluence.time <= 22))


df_gender_age_affluence_6_22.show()


+--------------------+---------+------+-----+--------------------+-------+----+ 
|                 ifa|affluence|gender|  age|              bundle|geohash|time|
+--------------------+---------+------+-----+--------------------+-------+----+
|00b1a7f6-8c84-4a0...|      mid|     M|25-34|      com.viber.voip| wh28s3|  17|
|00da76ed-bde3-4d1...|      mid|     M|25-34|com.lenovo.anysha...| wh0puw|  13|
|00da76ed-bde3-4d1...|      mid|     M|25-34|com.playit.videop...| wh0puw|  13|
|011700da-e310-4ce...|      mid|     F|25-34|com.emojimatch.pu...| wh3mfd|  14|
|01219240-c6df-44f...|      mid|     M|25-34|com.imo.android.i...| wh0r27|  20|
|013380ae-08d6-469...|      mid|     M|25-34|com.lenovo.anysha...| wh0nqb|  18|
|01b7c838-3b8d-420...|      mid|     M|25-34|com.lenovo.anysha...| wh0pqu|  07|
|01b7c838-3b8d-420...|      mid|     M|25-34|com.mxtech.videop...| wh0pqu|  07|
|026afd07-71e9-491...|      mid|     M|25-34|com.imo.android.i...| wh0qbd|  15|
|029f65cf-2583-4cd...|      mid|     M|25-34|      com.viber.voip| wh0nyf|  15|
|02b33054-eaf5-4f8...|      mid|     F|25-34|com.mxtech.videop...| wh0qk4|  14|
|02b33054-eaf5-4f8...|      mid|     F|25-34|com.mxtech.videop...| wh0qk4|  14|
|02cd91ff-fd15-450...|      mid|     F|25-34|com.camerasideas....| w5cr3s|  22|
|03510acb-ab3b-4ca...|      mid|     M|25-34|   com.eyecon.global| wh0pks|  19|
|03510acb-ab3b-4ca...|      mid|     M|25-34|      com.viber.voip| wh0pks|  19|
|0377f605-6dc8-433...|      mid|     M|25-34|      com.viber.voip| wh0qc4|  21|
|0377f605-6dc8-433...|      mid|     M|25-34|      com.viber.voip| tuqsu4|  14|
|0377f605-6dc8-433...|      mid|     M|25-34|com.teenpatti.hd....| wh0qc4|  21|
|0377f605-6dc8-433...|      mid|     M|25-34|com.teenpatti.hd....| tuqsu4|  14|
|03cadb18-621a-484...|      mid|     M|25-34|com.merge.cube.wi...| tur487|  10|
+--------------------+---------+------+-----+--------------------+-------+----+
only showing top 20 rows




###persona_app+++++++++++++++++++++++++++++++++++++++++++++++++++++++++==



#load app , life stage reffrence data
master_df = spark.read.csv('s3a://ada-bd-emr/app_ref/master_df/*', header=True)
master_df.printSchema()
level_df = spark.read.csv('s3a://ada-bd-emr/app_ref/level_df/*', header=True)
level_df.printSchema()
lifestage_df = spark.read.csv('s3a://ada-bd-emr/app_ref/lifestage_df/*', header=True)
lifestage_df.printSchema()



#joining table
join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()
select_columns = ['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name', 'lifestage_name']
finalapp_df = join_df2.select(*select_columns)
persona_app= finalapp_df.join(df_gender_age_affluence_6_22, on='bundle', how='left').cache()
#Count Freq on life_stage for analysis
freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('freq')).sort('lifestage_name', ascending = True)
freq_ls=freq_ls.filter(freq_ls.lifestage_name !='null')
freq_ls.show(10, False)
# +-------------------------+----+                                                
# |lifestage_name           |freq|
# +-------------------------+----+
# |null                     |1059|
# |In a Relationship/Married|1   |
# |Parents with Kids (3-6)  |16  |
# |Single                   |29  |
# |Working Adults           |46  |
# +-------------------------+----+

+--------------------------+----+                                               
|lifestage_name            |freq|
+--------------------------+----+
|College/University Student|304 |
|Expecting Parents         |4   |
|First Jobber              |0   |
|In a Relationship/Married |1   |
|Parents with Kids (0-3)   |5   |
|Parents with Kids (3-6)   |1893|
|Single                    |554 |
|Working Adults            |205 |
+--------------------------+----+

########   2. Top 10 Behaviour    ########
freq_beh=persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('app_l1_name', ascending = True)

freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
freq_beh1.show(10,0)

# +----------------------+----+
# |app_l1_name           |freq|
# +----------------------+----+
# |Call and Chat         |877 |
# |Social App Accessories|699 |
# |Photo Video           |289 |
# |Music                 |190 |
# |Video Streaming       |189 |
# |Games                 |162 |
# |Sports and Fitness    |87  |
# |Personal Productivity |76  |
# |Career                |42  |
# |Finance               |34  |
# +----------------------+----+

+-----------------+-----+                                                       
|app_l1_name      |freq |
+-----------------+-----+
|Auto Vehicles    |1    |
|Beauty           |24   |
|Call and Chat    |82616|
|Career           |153  |
|Couple App       |1    |
|Dating App       |554  |
|Education        |1705 |
|Events           |1    |
|Finance          |1439 |
|Food and Beverage|4    |
+-----------------+-----+
only showing top 10 rows



#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==



# VERSION 7 POI DATA WORK

master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_master_v7.0.csv',header=True)

master.printSchema()

master.select('level_id','name','geohash').show()




master=master.select('level_id','name','geohash')

tax=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_taxonomy_v7.0.csv',header=True)

tax.printSchema()

tax=tax.select('level_id','l1_name','l2_name','l3_name','l4_name')


POI_TAX=master.join(tax,"level_id","left")

#per hour behaviour 24-34

for i in range (7,8,1):
    
    
    df_gender_age_affluence_new=df_gender_age_affluence_6_22.filter(df_gender_age_affluence_6_22.time==i)
    
    # df_gender_age_affluence_new.show()
    
    new=df_gender_age_affluence_new.select('ifa','geohash')
    
    # res=new.groupBy("geohash").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
    
    # res.show(200)
    
    # master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_master_v7.0.csv',header=True)
    
    # master.printSchema()
    
    
    # tax=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_taxonomy_v7.0.csv',header=True)
    
    # tax.printSchema()
    
    # master=master.select('name','geohash')
    
    visited=new.join(POI_TAX, on='geohash', how='left')
    
    
    # visited.show(20)
    
    l1 =visited.groupBy("l1_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
    l1.show()
    l2=visited.groupBy("l2_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
    l2.show()

    # l3=REACH.groupBy("l3_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)      
    
    # l1=visited.groupBy("l1_name").sum('ifa_numbers').alias('ifa_numbers')
    
    # l1.show(200)
    
    
    
    # l2=visited.groupBy("l2_name").sum("ifa_numbers").alias('ifa_numbers')
    
    # l2.show(200)











+-------+-----------+--------------------+                                      
|geohash|ifa_numbers|                name|
+-------+-----------+--------------------+
| wh0qcr|       1304|Business District...|
| wh0qcr|       1304|Business District...|
| wh0qcr|       1304|Business District...|
| wh0qcr|       1304|Business District...|
| wh0qcr|       1304|Robi Airtel Custo...|
| wh0qcr|       1304|Robi & Airtel Int...|
| wh0qcr|       1304|         Robi Paltan|
| wh0qcr|       1304|Grameenphone Oppo...|
| wh0qcr|       1304|Grameenphone Oppo...|
| wh0qcr|       1304|        Suman Optics|
| wh0qcr|       1304|       Opticman Ltd.|
| wh0qcr|       1304|   Optical Show-Room|
| wh0qcr|       1304|    Pabna Homeo Hall|
| wh0qcr|       1304| Imdadiya Drug House|
| wh0qcr|       1304|Baitul Mukarram M...|
| wh0qcr|       1304| Stadium Market, ...|
| wh0qcr|       1304| Ramna Bhaban Market|
| wh0qcr|       1304|kazal enterprise,...|
| wh0qcr|       1304|         Camera Zone|
| wh0qcr|       1304|      Apan Jewellers|
| wh0qcr|       1304|    Fariha Jewellers|
| wh0qcr|       1304|   Kalpana Jewellers|
| wh0qcr|       1304|       Rina Jewelers|
| wh0qcr|       1304|Romana Silver Jew...|
| wh0qcr|       1304|BD online jewelle...|
| wh0qcr|       1304|Lia Stone & Jewel...|
| wh0qcr|       1304|  Venus Jewelers Ltd|
| wh0qcr|       1304|Aarong, 41, AK Fa...|
| wh0qcr|       1304|     A.S Corporation|
| wh0qcr|       1304|   Golap shah Mosque|
| wh0qcr|       1304|Baitur Rauf Jame ...|
| wh0qcr|       1304|Bandhu CNG & Fill...|
| wh0qcr|       1304|Hotel Purbani Int...|
| wh0qcr|       1304|       Hotel Pacific|
| wh0qcr|       1304|Health Engineerin...|
| wh0qcr|       1304|Appayon Hotel, Ba...|
| wh0qcr|       1304|KFC Paltan, Bangl...|
| wh0qcr|       1304|           The Beast|
| wh0qcr|       1304|       THE WAY Dhaka|
| wh0qcr|       1304|           Food Fast|
| wh0qcr|       1304|Express cafe food...|
| wh0qcr|       1304|Ivy Rahman Swimmi...|
| wh0qcr|       1304|Power Sports And ...|
| wh0qcr|       1304|Power Sports And ...|
| wh0qcr|       1304|York Money Exchan...|
| wh0qcr|       1304|Ornate Money Exch...|
| wh0qcr|       1304|Maatrik Money Exc...|
| wh0qcr|       1304|Himalaya Dollar A...|
| wh0qcr|       1304|Haque Money Exchange|
| wh0qcr|       1304|Eastern Union Mon...|
| wh0qcr|       1304|Chawk Bazar Money...|
| wh0qcr|       1304|Bakaul Money Exch...|
| wh0qcr|       1304|Agrani Bank Limit...|
| wh0qcr|       1304|MoneyGram Jiban B...|
| wh0qcr|       1304|  MoneyGram Dilkusha|
| wh0qcr|       1304|       SBAC Bank ATM|
| wh0qcr|       1304| Sonali Bank Limited|
| wh0qcr|       1304|Sonali Bank Limit...|
| wh0qcr|       1304|Sonali Bank Limit...|
| wh0qcr|       1304|Rupali Bank Ltd. ...|
| wh0qcr|       1304|Rupali Bank Limit...|
| wh0qcr|       1304| Rupali Bank Limited|
| wh0qcr|       1304|Janata Bank Limit...|
| wh0qcr|       1304|Janata Bank Limit...|
| wh0qcr|       1304| Janata Bank Limited|
| wh0qcr|       1304|ATM Booth, Janata...|
| wh0qcr|       1304|Janata Bank Limit...|
| wh0qcr|       1304|     Janata Bank ATM|
| wh0qcr|       1304|BASIC Bank Limite...|
| wh0qcr|       1304| Agrani Bank Limited|
| wh0qcr|       1304|Agrani Bank Limit...|
| wh0qcr|       1304|Agrani Bank Limit...|
| wh0qcr|       1304|Agrani Bank Limit...|
| wh0qcr|       1304|Standard Bank Lim...|
| wh0qcr|       1304|Standard Bank Lim...|
| wh0qcr|       1304|     Union Bank Ltd.|
| wh0qcr|       1304|Social Islami Ban...|
| wh0qcr|       1304|Shahjalal Islami ...|
| wh0qcr|       1304|Shahjalal Islami ...|
| wh0qcr|       1304|Islami Bank Bangl...|
| wh0qcr|       1304|Islami Bank Bangl...|
| wh0qcr|       1304|Islami Bank Bangl...|
| wh0qcr|       1304|First Security Is...|
| wh0qcr|       1304|   EXIM Bank Limited|
| wh0qcr|       1304|EXIM Bank Limited...|
| wh0qcr|       1304|Al-Arafah Islami ...|
| wh0qcr|       1304|Al-Arafah Islami ...|
| wh0qcr|       1304|Al-Arafah Islami ...|
| wh0qcr|       1304|Al-Arafah Islami ...|
| wh0qcr|       1304|Commercial Bank o...|
| wh0qcr|       1304|Uttara Bank Limit...|
| wh0qcr|       1304|Uttara Bank Ltd.,...|
| wh0qcr|       1304|Southeast Bank Li...|
| wh0qcr|       1304|PUBALI BANK LIMIT...|
| wh0qcr|       1304|Prime Bank Ltd, H...|
| wh0qcr|       1304|Prime Bank Limite...|
| wh0qcr|       1304|Premier Bank Ltd....|
| wh0qcr|       1304|NRB Commercial Ba...|
| wh0qcr|       1304|   NRBC Bank Limited|
| wh0qcr|       1304|NRB Commercial Ba...|
| wh0qcr|       1304|    NRB Bank Limited|
| wh0qcr|       1304|     NRB Bank My ATM|
| wh0qcr|       1304|National Bank Lim...|
| wh0qcr|       1304| MTB Dilkusha Branch|
| wh0qcr|       1304|Mutual Trust Bank...|
| wh0qcr|       1304|Midland Bank Limited|
| wh0qcr|       1304|Mercantile Bank L...|
| wh0qcr|       1304|Mercantile Bank A...|
| wh0qcr|       1304|Mercantile Bank L...|
| wh0qcr|       1304|   IFIC Bank Limited|
| wh0qcr|       1304|IFIC Bank Limited...|
| wh0qcr|       1304|Eastern Bank Limi...|
| wh0qcr|       1304|Dutch-Bangla Bank...|
| wh0qcr|       1304|City Bank Limited...|
| wh0qcr|       1304|City Bank Limited...|
| wh0qcr|       1304|The City Bank Lim...|
| wh0qcr|       1304|City Bank Limited...|
| wh0qcr|       1304|BRAC Bank Limited...|
| wh0qcr|       1304|Bank Asia Limited...|
| wh0qcr|       1304|Bank Asia Limited...|
| wh0qcr|       1304|   Bank Asia Limited|
| wh0qcr|       1304|Bangladesh Commer...|
| wh0qcr|       1304|Bangladesh Commer...|
| wh0qcr|       1304|Bangladesh Commer...|
| wh0qcr|       1304|AB Bank Limited A...|
| wh0qcr|       1304|Uttara Bank Ramna...|
| wh0qcr|       1304|Uttara Bank Local...|
| wh0qcr|       1304|Union Bank Dilkus...|
| wh0qcr|       1304|Standard Bank Top...|
| wh0qcr|       1304|Standard Bank Pri...|
| wh0qcr|       1304|Social Islami Ban...|
| wh0qcr|       1304|Shahjalal Islami ...|
| wh0qcr|       1304|SBAC Bank Motijhe...|
| wh0qcr|       1304|Prime Bank Motijh...|
| wh0qcr|       1304|Prime Bank Islami...|
| wh0qcr|       1304|NRB Commercial Ba...|
| wh0qcr|       1304|NRB Bank Dilkusha...|
| wh0qcr|       1304|Mutual Trust Bank...|
| wh0qcr|       1304|Midland Bank Corp...|
| wh0qcr|       1304|Mercantile Bank M...|
| wh0qcr|       1304|Janata Bank Nawab...|
| wh0qcr|       1304|Jamuna Bank Motij...|
| wh0qcr|       1304|Islami Bank Under...|
| wh0qcr|       1304|Islami Bank Palta...|
| wh0qcr|       1304|Islami Bank Baitu...|
| wh0qcr|       1304|IFIC Bank Motijhe...|
| wh0qcr|       1304|IFIC Bank Local O...|
| wh0qcr|       1304|First Security Is...|
| wh0qcr|       1304|Dutch Bangla Loca...|
| wh0qcr|       1304|Dutch Bangla Corp...|
| wh0qcr|       1304|Dutch Bangla BB A...|
| wh0qcr|       1304|Dhaka Bank Local ...|
| wh0qcr|       1304|Dhaka Bank Foreig...|
| wh0qcr|       1304|Commercial Bank o...|
| wh0qcr|       1304|BRAC Bank Motijhe...|
| wh0qcr|       1304|BASIC Bank Dilkus...|
| wh0qcr|       1304|Bank Asia Princip...|
| wh0qcr|       1304|Bank Asia Paltan ...|
| wh0qcr|       1304|Bank Asia MCB Dil...|
| wh0qcr|       1304|Bangladesh Develo...|
| wh0qcr|       1304|Bangladesh Develo...|
| wh0qcr|       1304|Bangladesh Commer...|
| wh0qcr|       1304|Bangladesh Commer...|
| wh0qcr|       1304|Alfalah Bank Moti...|
| wh0qcr|       1304|Al Arafah Islami ...|
| wh0qcr|       1304|Al Arafah Islami ...|
| wh0qcr|       1304|Rupali Bank Puran...|
| wh0qcr|       1304|Rupali Bank Naya ...|
| wh0qcr|       1304|Rupali Bank Banga...|
| wh0qcr|       1304|National Bank Lim...|
| wh0qcr|       1304|National Bank Lim...|
| wh0qcr|       1304|EXIM Bank Rajuk A...|
| wh0qcr|       1304|Southeast Bank Pr...|
| wh0qcr|       1304|Southeast Bank Mo...|
| wh0qcr|       1304|Southeast Bank Co...|
| wh0qcr|       1304|City Bank Princip...|
| wh0qcr|       1304|City Bank B.B. Av...|
| wh0qcr|       1304| National Press Club|
| wh0qcr|       1304|New Purnima Snack...|
| wh0qcr|       1304|Autometic Car Was...|
| wh0qcr|       1304|Aftab Automobiles...|
| wh0qcr|       1304|       Outer Stadium|
| wh0qbd|        777|Grameenphone Maye...|
| wh0qbd|        777|    Aduri Super Shop|
| wh0qbd|        777|Radha Krishna Temple|
| wh0qbd|        777| CNG filling station|
| wh0qbd|        777|Shoili Bakery,  ,...|
| wh0qbd|        777|       The Paris Gym|
| wh0qbd|        777|Kalindi Girls Hig...|
| wh0qbd|        777|Keraniganj Girl's...|
| wh0qbd|        777|Parjowar Kalindi ...|
| wh0qbd|        777|Keraniganj Girl's...|
| wh0qbd|        777|PLAYGROUND OF KAL...|
| wh0qbd|        777|Kalindi Big Play ...|
| wh0r36|        709|    Gulshan Avenue 1|
| wh0r36|        709|Office of Operati...|
| wh0r36|        709|Teletalk 5G Bangl...|
| wh0r36|        709|Grameenphone Loun...|
| wh0r36|        709|           Gulshan 1|
| wh0r36|        709|Dr Saleh Ahmed Ey...|
+-------+-----------+--------------------+
only showing top 200 rows

>>> 




