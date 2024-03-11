
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
from pyspark.sql import *# ------------------Objective---------------------------------
# age group, lifestage group, personal lifestage, professional lifestage
########### 1.IFA count per age group #######################
age= spark.read.parquet('s3a://ada-platform-components/demographics/output/BD/age/202104/*.parquet')#rename the 'prediction' column to 'age'
age=age.withColumnRenamed('prediction','age')
age_count=age.groupBy('age').agg(F.countDistinct('ifa').alias('ifa count per age group')).sort(col('ifa count per age group').desc())
age_count.show()




# +----------+-----------------------+
# | age |ifa count per age group|
# +----------+-----------------------+
# | 18-24| 20475590|
# | 25-34| 12641348|
# | 35-49| 3942999|
# | 50+| 363237|
# +----------+-----------------------+#load app , life stage reference 

m='202001'
country='BD'
time='monthly'

geo = spark.read.parquet('s3a://ada-bd-emr/result/2022/Robi/DSR-623/raw/bl/'+m+'/*.parquet')
df_gender = spark.read.parquet('s3a://ada-prod-data/etl/table/brq/sub/demographics/monthly/'+country+'/'+m+'/gender/*.parquet')
df_gender = df_gender.withColumnRenamed('prediction', 'gender')

df_gender=df_gender.drop(df_gender.gender)

df_gender=df_gender.withColumnRenamed("label","gender")
df_gender_segment = geo.join(df_gender, ['ifa'], how='left')
df_gender_segment.groupBy('gender').agg(F.countDistinct('ifa')).orderBy('gender').show(200,0)


master_df = spark.read.csv('s3a://ada-bd-emr/app_ref/master_df/*', header=True)
level_df = spark.read.csv('s3a://ada-bd-emr/app_ref/level_df/*', header=True)

level_df.groupBy('app_l1_name').count()
lifestage_df = spark.read.csv('s3a://ada-bd-emr/app_ref/lifestage_df/*', header=True)#joining table
join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()#selecting columns related lifestages
select_columns = ['bundle', 'lifestage_status','lifestage_type', 'lifestage_name']
finalapp_df = join_df2.select(*select_columns)#taking timeline data
brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/202104{01}/*.parquet')#exploding the app array to extract the app info
brq2 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')
app = brq2.join(finalapp_df, on='bundle', how='left').cache()
############### 2.IFA count per lifestage name #####################count the ifa on lifestage name,no null value present
lifestage_ifa=app.select('ifa','lifestage_name')
lifestage_ifa=lifestage_ifa.filter(lifestage_ifa['lifestage_name'] != 'null')
lifestage_ifa_count=lifestage_ifa.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa per lifestages')).sort(col('ifa per lifestages').desc())
lifestage_ifa_count.limit(20).show(truncate=False)
############### 3.IFA count per lifestage name & lifestage type #####################filter by lifestage type(personal & professional)
personal_lifestage=app.select('ifa','lifestage_type','lifestage_name').filter(col('lifestage_type') =='personal')

professional_lifestage=app.select('ifa','lifestage_type','lifestage_name').filter(col('lifestage_type') =='professional')
#count the ifa on the lifestage name for "personal" lifestage type
personal_ifa=personal_lifestage.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('personal ifa')).sort(col('personal ifa').desc())

personal_ifa.show(truncate=False)# +--------------------+------------+
# | lifestage_name|personal ifa|
# +--------------------+------------+
# |Parents with Kids...| 60150|
# | Single| 13814|
# | Expecting Parents| 482|
# |Parents with Kids...| 84|
# |In a Relationship...| 45|
# +--------------------+------------+#count the ifa on the lifestage name for "professional" lifestage type
professional_ifa=professional_lifestage.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('professional ifa')).sort(col('professional ifa').desc())
professional_ifa.show(truncate=False)# +--------------------------+----------------+
# |lifestage_name |professional ifa|
# +--------------------------+----------------+
# |Working Adults |19610 |
# |College/University Student|1480 |
# |First Jobber |322 |
# +--------------------------+----------------

