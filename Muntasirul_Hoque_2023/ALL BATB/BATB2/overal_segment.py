from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
import csv
import pandas as pd
import numpy as np

########   1. Re-read Persona IFA   ########
#Please change the path according to saved persona
dhaka_uh = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/UH/*.csv',header=True)

persona=dhaka_uh

persona.count()

persona.printSchema()


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

segment=finalapp_df.select('app_l1_name')

segment=segment.distinct()

segment.show(100,truncate=False)


finalapp_df =finalapp_df .filter(finalapp_df.app_l1_name=='Travel')

finalapp_df.show()



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{04,05,o6,07,08,09}/*.parquet')

# brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20210301/*.parquet')

brq2 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')

brq2.count()


app =finalapp_df.join( brq2,'bundle','inner')

app.count()


persona_app =persona.join(app,'ifa','inner')

persona_app=persona_app.select('ifa')


persona_app=persona_app.distinct()



persona_app.count()







segement=finalapp_df.select('app_l1_name')

segement=segement.distinct()

segement.show(100,truncate=False)

