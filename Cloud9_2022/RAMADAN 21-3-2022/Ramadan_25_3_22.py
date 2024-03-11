# File name     : Ramadan dhaka.py

# Author        : Md.Muntasirul Hoque

# Date          : 25 March 2021

# Country       : BD

# Description   : Script to prepare analysis for built persona, for each result on each item has to be pasted on Persona Deck Template excel

# Persona Name  : UH,H,MID - INCOME



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
from functools import reduce
from pyspark.sql import *

#Making dataframe for 25- 35 age people with Affluence of UH,H ,MID


df_brq=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2021{03}/*.parquet')

df_brq.printSchema()

gps_df=df_brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')

app_df=df_brq.select('ifa', F.explode('app')).select('ifa', 'col.*')

app_df=app_df.select('ifa','bundle','asn')

gps_app_df=app_df.join(gps_df,"ifa","left")

gps_df=gps_app_df.select('ifa','geohash','last_seen','bundle','asn')

gps_df=gps_df.withColumn("new_geohash", substring(col("geohash"),1,6))

gps_df=gps_df.drop(gps_df.geohash)

gps_df=gps_df.withColumnRenamed("new_geohash","geohash")

from pyspark.sql.functions import date_format

gps_df=gps_df.withColumn("time", date_format('last_seen', 'HH'))

gps_df=gps_df.drop(gps_df.last_seen)


#adding age and gender to dataframe 


df_gender = spark.read.parquet('s3a://ada-prod-data/etl/table/brq/sub/demographics/monthly/BD/2021{03}/gender/*.parquet')
df_gender.printSchema()
df_gender=df_gender.select('ifa','label')
df_gender=df_gender.withColumnRenamed('label','gender')
df_gender.show()
df_age = spark.read.parquet('s3a://ada-prod-data/etl/table/brq/sub/demographics/monthly/BD/2021{03}//age/*.parquet')
df_age=df_age.select('ifa','label')
df_age=df_age.withColumnRenamed('label','age')
df_age.show()
df_gender_age=df_gender.join(df_age,"ifa","left")
df_gps_gender_age=df_gender_age.join(gps_df,"ifa","left")

#filtering out 25-35 age people removing null values from important columns 

df_gps_gender_age=df_gps_gender_age.filter((df_gps_gender_age.geohash != 'null')&(df_gps_gender_age.time !='null') & (df_gps_gender_age.bundle !='null')& (df_gps_gender_age.bundle !='asn'))
df_gps_gender_age_25_34=df_gps_gender_age.filter(df_gps_gender_age.age=='25-34')



#Adding affluence according from -( affluence dataset) present dataframe 


affluence=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/2021{03}/*.parquet')


#taking important columns for affluence

affluence=affluence.select('ifa','final_affluence')

# l1=affluence.groupBy("final_affluence").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

# l1.show()


#Removing Null values and low affluence data 
affluence=affluence.withColumnRenamed("final_affluence","affluence")

affluence_final=affluence.filter(affluence.affluence!='Low')

# affluence_final=affluence.filter((affluence.affluence=='High') & (affluence.affluence == 'Ultra High') & (affluence.affluence =='Mid') )



#-------------------------------MERGING AFFLUENCE WITH MAIN DATA -------------------------------


df_gender_age_affluence=affluence_final.join(df_gps_gender_age_25_34,"ifa","left")

#removing null values 


df_gender_age_affluence=df_gender_age_affluence.filter((df_gender_age_affluence.affluence!='null')&(df_gender_age_affluence.gender!='null')&(df_gender_age_affluence.age!='null')&(df_gender_age_affluence.geohash !='null')&(df_gender_age_affluence.time!='null'))




#filtering out time according to situation 


#here i am filtering out 4 hour time duration 6am - 10am 

df_gender_age_affluence_6_22=df_gender_age_affluence.filter((df_gender_age_affluence.time >=22 ) & (df_gender_age_affluence.time <=24))

#df_gender_age_affluence_6_22=df_gender_age_affluence.filter((df_gender_age_affluence.time >= 22 ) & (df_gender_age_affluence.time <=24) & (df_gender_age_affluence.time ==1) & (df_gender_age_affluence.time ==2))

# filtering out dhaka district ifa


dhaka=spark.read.csv('s3a://ada-bd-emr/muntasir/location/dhaka.csv',header=True)


df_gender_age_affluence_6_22_dhaka=dhaka.join(df_gender_age_affluence_6_22,"geohash","left")



df_gender_age_affluence_6_22=df_gender_age_affluence_6_22_dhaka.select('geohash','ifa','affluence','gender','age','time','bundle','asn')



#cleaning out null values

df_gender_age_affluence_6_22=df_gender_age_affluence_6_22.filter((df_gender_age_affluence_6_22.bundle !='null')&(df_gender_age_affluence_6_22.ifa !='null')&(df_gender_age_affluence_6_22.affluence !='null') & (df_gender_age_affluence_6_22.gender !='null') & (df_gender_age_affluence_6_22.age !='null')&(df_gender_age_affluence_6_22.gender !='null') & (df_gender_age_affluence_6_22.time !='null'))




#filter for male or female if you want both dont filter it




df_gender_age_affluence_6_22=df_gender_age_affluence_6_22.filter(df_gender_age_affluence_6_22.gender=='M')


#persona




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
########   2. Top 10 Behaviour    ########
freq_beh=persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('app_l1_name', ascending = True)
freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
freq_beh1.show(10,0)


# Group by 'app_l1_name' and 'asn'
top_app_per_cat =persona_app.groupBy('app_l1_name','asn').agg(F.countDistinct('ifa').alias('freq'))
top_app_per_cat = top_app_per_cat.filter(top_app_per_cat['app_l1_name'] != 'null')
top_app_per_cat.show(10,0)

# convertig a single column of a df into list

app_cat_list = top_app_per_cat.select('app_l1_name').distinct()
# app_cat_list = list(app_cat_list.select('app_l1_name').toPandas()['app_l1_name'])

app_cat_list=app_cat_list.rdd.map(lambda x: x[0]).collect()

# Top 20 apps per category

for cat in app_cat_list:
    print(cat)
    top_app_per_cat.filter(top_app_per_cat.app_l1_name == cat).sort(col('freq').desc()).show(20,0)





