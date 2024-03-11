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

def count_functions(path,a):
    
    if a==1:
        
        
        
        def top_vendor_mobile():
        
    
            df=spark.read.parquet(f'{path}')
            #Needed  columns
            #for vendor
            mv=df.select('ifa','device.device_vendor','device.device_model')
            #for device
            mv2=df.select('ifa','device.device_model')
            #Top 10 vendor
            vendor=mv.groupBy("device_vendor").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show()
            #Top 20 Mobile
            mobile=mv2.groupBy("device_model").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show()
            
        top_vendor_mobile()
            
    elif a==2 :
        
      
        def top_app():
            df=spark.read.parquet(f'{path}')
            df2=df.select('ifa',explode('app')).select('ifa','col.*')
            df3=df2.select('ifa','asn')
            #Top 20 app
            app=df3.groupBy("asn").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show()
            
        top_app()
        
    elif a==3:
        
        def demo_gender_age():
            df= spark.read.parquet('s3a://ada-platform-components/demographics/output/BD/age/202111/*.parquet')
            #age count 
            age=df.groupBy("prediction").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show()
            #gender data read
            df2 = spark.read.parquet('s3a://ada-platform-components/demographics/output/BD/gender/202111/*.parquet')
            #gender count 
            gender=df2.groupBy("prediction").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False).show()
        
        demo_gender_age()
        
        
        
    elif a==4:
        
        def lifestage():
            master_df = spark.read.csv('s3a://ada-prod-data/reference/app/master_all/all/all/all/app.csv', header=True)
            level_df = spark.read.csv('s3a://ada-prod-data/reference/app/app_level/all/all/all/app_level.csv', header=True)
            lifestage_df = spark.read.csv('s3a://ada-prod-data/reference/app/lifestage/all/all/all/app_lifestage.csv', header=True)
            join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
            join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()
            select_columns = ['bundle','lifestage_name']
            finalapp_df = join_df2.select(*select_columns)
            app_df_daily = spark.read.parquet(f'{path}')
            app_daily=app_df_daily.select('ifa', explode('app')).select('ifa', 'col.*')
            app =app_daily.join(finalapp_df, on='bundle', how='left').cache()
            lifestage_ifa=app.select('ifa','lifestage_name')
            lifestage_ifa=lifestage_ifa.filter(lifestage_ifa['lifestage_name'] != 'null')
            lifestage_ifa_count=lifestage_ifa.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa per lifestages')).sort(col('ifa per lifestages').desc()).limit(10).show(truncate=False)
        
        lifestage()
    
    
    else :
        
        
        
        print('programme file error')




print('Pls enter your command')

a=input()

b=int(a)

path='s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{01}/*.parquet'

count_functions(path,b)



# a="abc"


# b=f'{a}'

# df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/202201/*.parquet')

# connection = df.select('ifa', F.explode('req_connection.req_carrier_name').alias('telco'))

# robi_user = connection.filter(connection['telco'] == 'Robi/Aktel').distinct()
# print('Robi User {}'.format(robi_user.count()))


# gp_user = connection.filter(connection['telco'] == 'GrameenPhone').distinct()
# print('GrameenPhone User {}'.format(gp_user.count()))

# bl_user = connection.filter(connection['telco'] == 'Orascom/Banglalink').distinct()
# print('Banglalink User {}'.format(bl_user.count()))


# geo = spark.read.parquet('s3a://ada-bd-emr/result/2022/Banglalink/DSR-623/banglalink/b/202101/*.parquet')


# geo = spark.read.parquet('s3a://ada-bd-emr/result/2022/Robi/DSR-623/raw/robi/202101/*.parquet')
# dev = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/device/monthly/BD/202101/*.parquet')
# device = device.select('ifa', 'device.pricegrade')
# dev = geo.join(device, on='ifa')

# aff_co = dev.withColumn("affluence", F.lit(None))
# new = aff_co.withColumn('affluence', F.when((aff_co.pricegrade ==1) , 'high').otherwise(aff_co.affluence))
# new = new.withColumn('affluence', F.when((aff_co.pricegrade ==2) , 'mid').otherwise(new.affluence))
# new = new.withColumn('affluence', F.when((aff_co.pricegrade == 3), 'low').otherwise(new.affluence))
# final = new.na.fill('Unknow', 'affluence')
# final = final.groupBy('affluence').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
# final.show(20,0)




# brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/202102/*.parquet')
# brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
# app = brq2.join(finalapp_df, on='bundle', how='left').cache()
# geo = spark.read.parquet('s3a://ada-bd-emr/result/2022/Robi/DSR-623/raw/robi/'+m+'/*.parquet')
# persona_app = geo.join(app, on ='ifa')
# freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
# freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
# dev.select('ifa','device.pricegrade','device.price')


# master_df = spark.read.csv('s3a://ada-prod-data/reference/app/master_all/all/all/all/app.csv', header=True)
# level_df = spark.read.csv('s3a://ada-prod-data/reference/app/app_level/all/all/all/app_level.csv', header=True)
# lifestage_df = spark.read.csv('s3a://ada-prod-data/reference/app/lifestage/all/all/all/app_lifestage.csv', header=True)
# join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
# join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()
# select_columns = ['bundle','app_l1_name','app_l2_name','app_l3_name','lifestage_name']
# finalapp_df = join_df2.select(*select_columns)

# brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/202102/*.parquet')
# brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
# app = brq2.join(finalapp_df, on='bundle', how='left').cache()
# geo = spark.read.parquet('s3a://ada-bd-emr/result/2022/Robi/DSR-623/raw/robi/'+m+'/*.parquet')
# persona_app = geo.join(app, on ='ifa')
# freq_beh = persona_app.groupBy('app_l1_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)
# freq_beh1 = freq_beh.filter(freq_beh['app_l1_name'] != 'null')
# freq_beh1.show(20,0)


# brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210101/*.parquet')
# brq2 = brq.select('ifa', explode('app')).select('ifa', 'col.*')
# app = brq2.join(finalapp_df, on='bundle', how='left').cache()
# geo = spark.read.parquet('s3a://ada-bd-emr/result/2022/Robi/DSR-623/raw/robi/202101/*.parquet')
# persona_app = geo.join(app, on='ifa').cache()
# freq_ls = persona_app.groupBy('lifestage_name').agg(F.countDistinct('ifa').alias('ifa')).sort('lifestage_name',ascending=True)
# freq_ls.show(10,0)








