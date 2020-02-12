# Databricks notebook source
dbutils.fs.ls("/mnt")

# COMMAND ----------

spark

# COMMAND ----------

############## READONLY #################
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "a32e3374-4070-4af4-a125-007ab1162cb6",
           "fs.azure.account.oauth2.client.secret": "Q/Q:x66Az?n.NnugjIYwDb9JZJ2cRVgi",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}


dbutils.fs.mount(
  source = "abfss://main@adlsg2bvb.dfs.core.windows.net/",
  mount_point = "/mnt/adlsg2bvbRO",
  extra_configs = configs)


# COMMAND ----------

dbutils.fs.ls("/mnt")

# COMMAND ----------

dbutils.fs.put("/mnt/adlsg2bvbRO/abc.txt","frwgr")

# COMMAND ----------

# MAGIC %fs ls /mnt/adlsg2bvbRO

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs ls /mnt/adlsg2bvbRO/training/nyctaxi

# COMMAND ----------

dbutils.fs.ls("/mnt/adlsg2bvbRO/training/nyctaxi/curatedDir/")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/adlsg2bvbRO/training/nyctaxi/curatedDir/"))

# COMMAND ----------

spark

# COMMAND ----------

parquetFile = "/mnt/adlsg2bvbRO/training/nyctaxi/curatedDir/materialized-view/"
df = spark.read.parquet(parquetFile)
df

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df2 = df.filter("passenger_count = 1")

# COMMAND ----------

df2.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY TABLE taxis
# MAGIC USING parquet
# MAGIC OPTIONS (path "/mnt/adlsg2bvbRO/training/nyctaxi/curatedDir/materialized-view/")

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT count(*) from taxis

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) from taxis where passenger_count = 1