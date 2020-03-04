# Databricks notebook source
# MAGIC %md alguns testes para verificar existencia de folders

# COMMAND ----------

dbutils.fs.help()1 

# COMMAND ----------

dbutils.fs.ls("/mnt/adlsg2bvbRO")

# COMMAND ----------

dbutils.fs.unmount("/mnt/adlsg2bvbRO")

# COMMAND ----------

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

# MAGIC %fs ls /mnt/

# COMMAND ----------

# MAGIC %fs ls /mnt/adlsg2bvb/

# COMMAND ----------

# MAGIC %fs ls /mnt

# COMMAND ----------

# MAGIC %md copiar ficheiros entre diretorios

# COMMAND ----------

dbutils.fs.cp("/mnt/adlsg2bvb/training/_/f1","/f1",True)

# COMMAND ----------

# MAGIC %fs ls /f1

# COMMAND ----------

# MAGIC %md display seasons.csv

# COMMAND ----------

# MAGIC %fs head  "/f1/seasons.csv"

# COMMAND ----------

fileNameSeasons = "/f1/seasons.csv"

s = spark.read \
  .option("inferSchema", "true") \
  .csv(fileNameSeasons)   

display(s)

# COMMAND ----------

# MAGIC %md quantas colunas tem o driver.csv

# COMMAND ----------

len(d.columns)

# COMMAND ----------

fileNameDriver = "/f1/driver.csv"

d = spark.read \
  .option("inferSchema", "true") \
  .csv(fileNameDriver)   

display(d)

# COMMAND ----------

# MAGIC %fs head  /f1/driver.csv

# COMMAND ----------

d = spark.read \
  .option("inferSchema", "true") \
  .csv(fileNameDriver)   

display(d)

# COMMAND ----------

# são 9 colunas mas se quiser contar programaticamente tenho que ir buscar o objeto columns

len(d.columns)

# COMMAND ----------

# MAGIC %md quantas linhas tem o results.csv

# COMMAND ----------

# MAGIC %fs head /f1/results.csv

# COMMAND ----------

fileNameResults = "/f1/results.csv"

r = spark.read \
  .option("inferSchema", "true") \
  .csv(fileNameResults)   

display(r)

# COMMAND ----------

r.count()

# COMMAND ----------

# MAGIC %md crie a tabela temp driver_standings

# COMMAND ----------

fileNameResults = "/f1/driver_standings.csv"

ds = spark.read \
  .option("inferSchema", "true") \
  .csv(fileNameResults)   

display(ds)



# COMMAND ----------

ds.createOrReplaceTempView("driver_standings1")

# COMMAND ----------

# MAGIC %sql
# MAGIC --ou em sql 
# MAGIC 
# MAGIC CREATE TEMPORARY TABLE driver_standings2
# MAGIC USING csv
# MAGIC OPTIONS (path "/f1/driver_standings.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select (Select count(*) from driver_standings1),(Select count(*) from driver_standings2)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists driver_standings3
# MAGIC USING CSV
# MAGIC LOCATION "/f1/driver_standings.csv"

# COMMAND ----------

# MAGIC %md  ----

# COMMAND ----------

# MAGIC %fs head /f1/f1db_schema.txt
