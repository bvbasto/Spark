# Databricks notebook source
# MAGIC %md
# MAGIC se necessário fazer o mount novamente

# COMMAND ----------

# dbutils.fs.unmount("/mnt/adlsg2bvbRO")

# COMMAND ----------

############## READONLY #################
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "APPID",
           "fs.azure.account.oauth2.client.secret": "SECRET",
           "fs.azure.account.oauth2.client.endpoint": "ENDPOINT"}


dbutils.fs.mount(
  source = "abfss://main@adlsg2bvb.dfs.core.windows.net/",
  mount_point = "/mnt/adlsg2bvbRO",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ####a partir de agora é possivel aceder ao Data Lake a partir de /mnt/adlsg2bvbRO

# COMMAND ----------

mntADLS = "/mnt/adlsg2bvbRO/old_training/nyctaxi/_curatedDir"

dbutils.fs.ls(mntADLS)

# COMMAND ----------

dbutils.fs.rm('/nyc',True)

# COMMAND ----------

dbutils.fs.cp("/mnt/adlsg2bvbRO/old_training/nyctaxi/_curatedDir/materialized-view-b-2015Green/","/nyc",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database taxi_db

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS  taxi_db.taxi_trips_mat_view(
# MAGIC taxi_type STRING,
# MAGIC vendor_id INT,
# MAGIC pickup_datetime TIMESTAMP,
# MAGIC dropoff_datetime TIMESTAMP,
# MAGIC store_and_fwd_flag STRING,
# MAGIC rate_code_id INT,
# MAGIC pickup_location_id INT,
# MAGIC dropoff_location_id INT,
# MAGIC pickup_longitude STRING,
# MAGIC pickup_latitude STRING,
# MAGIC dropoff_longitude STRING,
# MAGIC dropoff_latitude STRING,
# MAGIC passenger_count INT,
# MAGIC trip_distance DOUBLE,
# MAGIC fare_amount DOUBLE,
# MAGIC extra DOUBLE,
# MAGIC mta_tax DOUBLE,
# MAGIC tip_amount DOUBLE,
# MAGIC tolls_amount DOUBLE,
# MAGIC ehail_fee DOUBLE,
# MAGIC improvement_surcharge DOUBLE,
# MAGIC total_amount DOUBLE,
# MAGIC payment_type INT,
# MAGIC trip_type INT,
# MAGIC trip_year STRING,
# MAGIC trip_month STRING,
# MAGIC vendor_abbreviation STRING,
# MAGIC vendor_description STRING,
# MAGIC trip_type_description STRING,
# MAGIC month_name_short STRING,
# MAGIC month_name_full STRING,
# MAGIC payment_type_description STRING,
# MAGIC rate_code_description STRING,
# MAGIC pickup_borough STRING,
# MAGIC pickup_zone STRING,
# MAGIC pickup_service_zone STRING,
# MAGIC dropoff_borough STRING,
# MAGIC dropoff_zone STRING,
# MAGIC dropoff_service_zone STRING,
# MAGIC pickup_year INT,
# MAGIC pickup_month INT,
# MAGIC pickup_day INT,
# MAGIC pickup_hour INT,
# MAGIC pickup_minute INT,
# MAGIC pickup_second INT,
# MAGIC dropoff_year INT,
# MAGIC dropoff_month INT,
# MAGIC dropoff_day INT,
# MAGIC dropoff_hour INT,
# MAGIC dropoff_minute INT,
# MAGIC dropoff_second INT)
# MAGIC USING parquet
# MAGIC partitioned by (taxi_type,trip_year,trip_month)
# MAGIC LOCATION '/nyc';

# COMMAND ----------

spark.sql("MSCK REPAIR TABLE  taxi_db.taxi_trips_mat_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from taxi_db.taxi_trips_mat_view
# MAGIC --   19.233.634
# MAGIC --1.600.000.000
