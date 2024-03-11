


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{09}/*.parquet')



device=brq.select('ifa','device.device_vendor','device.device_name','device.device_manufacturer','device.device_model','device.platform','device.major_os','device.device_category')


device=device.select('ifa','device_category')


smart_tv=device.filter(device.device_category=='Smart-TV')


smart_tv=smart_tv.select('ifa')

smart_tv=smart_tv.distinct()

# smart_tv.count()



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{09}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select('ifa','state','city','geohash','latitude','longitude')


common_ifa=smart_tv.join(brq2,'ifa','left')





common_ifa=common_ifa.withColumn("new_geohash", substring(col("geohash"),1,6))
common_ifa=common_ifa.drop(common_ifa.geohash)
common_ifa=common_ifa.withColumnRenamed("new_geohash","geohash")




district=spark.read.csv('s3a://ada-bd-emr/muntasir/BD_LOOKUP/BD_lookup_table.csv',header=True)





device_df=common_ifa.join(district,'geohash','inner')










freq_beh=device_df.groupBy('district').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)


freq_beh.show(100,truncate=False)







district=spark.read.csv('s3a://ada-bd-emr/muntasir/BD_LOOKUP/BD_lookup_table.csv',header=True)

