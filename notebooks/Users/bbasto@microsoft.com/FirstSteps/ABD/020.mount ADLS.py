# Databricks notebook source
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "28dd86ad-d24a-4183-9dc7-d42790a1116c",
           "dfs.adls.oauth2.credential": "Eez/ykW2aVVQrzFKAr1KwB44yHt1xGFpUOjhKFZtpM0=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

dbutils.fs.mount(
  source = "adl://adlsfordatabricksbvb01.azuredatalakestore.net/training",
  mount_point = "/mnt/MyADLS_training",
  extra_configs = configs)

# COMMAND ----------

configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "28dd86ad-d24a-4183-9dc7-d42790a1116c",
           "dfs.adls.oauth2.credential": "Eez/ykW2aVVQrzFKAr1KwB44yHt1xGFpUOjhKFZtpM0=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

dbutils.fs.mount(
  source = "adl://adlsfordatabricksbvb01.azuredatalakestore.net/nyctaxi",
  mount_point = "/mnt/MyADLS_nyctaxi",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs ls /mnt/MyADLS/

# COMMAND ----------

dbutils.fs.unmount("/mnt/MyADLS_nyctaxi")

# COMMAND ----------

dbutils.fs.ls("/mnt/MyADLS_training")

# COMMAND ----------

dbutils.fs.ls("/mnt/MyADLS_nyctaxi")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY TABLE taxis
# MAGIC USING parquet
# MAGIC OPTIONS (path "/mnt/MyADLS_nyctaxi/curatedDir/materialized-view/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(*) as trip_count
# MAGIC from 
# MAGIC   taxis

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE taxis 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY TABLE taxis
# MAGIC USING parquet
# MAGIC OPTIONS (path "/mnt/MyADLS_nyctaxi/curatedDir/materialized-view-2015Green/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(*) as trip_count
# MAGIC from 
# MAGIC   taxis

# COMMAND ----------

