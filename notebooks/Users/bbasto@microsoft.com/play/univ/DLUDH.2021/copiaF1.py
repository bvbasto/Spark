# Databricks notebook source
# MAGIC %md alguns testes para verificar existencia de folders

# COMMAND ----------

dbutils.fs.help()

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

dbutils.fs.cp("/mnt/adlsg2bvbRO/old_training/_/f1","/f1",True)

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

# COMMAND ----------

display(r)

# COMMAND ----------

fileNameLT = "/f1/lap_times.csv"

lt = spark.read \
  .option("inferSchema", "true") \
  .csv(fileNameLT)   

lt.createOrReplaceTempView("lapTimes")

fileNameRaces = "/f1/races.csv"

rc = spark.read \
  .option("inferSchema", "true") \
  .csv(fileNameRaces)   

rc.createOrReplaceTempView("races")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from laptimes 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from races

# COMMAND ----------

# MAGIC %sql
# MAGIC select lapTimes._c0 as raceID, lapTimes._c1 as driverID,lapTimes._c4 as time,races._c1 as year 
# MAGIC   from lapTimes 
# MAGIC     inner join races on races._c0 = lapTimes._c0 

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view lapYear as 
# MAGIC select lapTimes._c0 as raceID, lapTimes._c1 as driverID,lapTimes._c4 as time,races._c1 as year from lapTimes inner join races on races._c0 = lapTimes._c0 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lapYear

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct  year from lapYear

# COMMAND ----------

 ly = spark.read.table("lapYear")

# COMMAND ----------

display(ly.select("year").distinct())

# COMMAND ----------

ly.write \
  .mode("overwrite") \
  .csv("/f1/out/lapYear1.csv")

# COMMAND ----------

ly.coalesce(1).write \
  .mode("overwrite") \
  .csv("/f1/out/lapYear10.csv")

# COMMAND ----------

# MAGIC %fs ls /f1/out/lapYear10.csv

# COMMAND ----------

# MAGIC %fs head dbfs:/f1/out/lapYear1.csv/part-00000-tid-9214450874229200042-6b04cc12-9647-4f2c-8a79-f9309b43a4d5-758-1-c000.csv

# COMMAND ----------

ly.coalesce(1).write.partitionBy("year") \
  .mode("overwrite") \
  .csv("/f1/out/lapYear100.csv")

# COMMAND ----------

# MAGIC %fs ls /f1/out/lapYear100.csv/year=2000

# COMMAND ----------

x = spark.read.csv("/f1/out/lapYear1.csv")
x.count()

# COMMAND ----------

ly.write \
  .option("compression", "snappy") \
  .mode("overwrite") \
  .parquet("/f1/out/lapYear2.parquet")

# COMMAND ----------

# MAGIC %fs ls /f1/out/lapYear1.csv/

# COMMAND ----------

# MAGIC %fs ls /f1/out/lapYear2.parquet/

# COMMAND ----------

ly.coalesce(1).write \
  .mode("overwrite") \
  .csv("/f1/out/lapYear3.csv")

# COMMAND ----------

ly.coalesce(1).write \
  .option("compression", "snappy") \
  .mode("overwrite") \
  .parquet("/f1/out/lapYear4.parquet")

# COMMAND ----------

# MAGIC %fs ls /f1/out/lapYear3.csv/

# COMMAND ----------

# MAGIC %fs ls /f1/out/lapYear4.parquet/

# COMMAND ----------

ly.coalesce(1).write \
  .partitionBy("year") \
  .mode("overwrite") \
  .csv("/f1/out/lapYear5.csv")


# COMMAND ----------

# MAGIC %fs ls /f1/out/lapYear5.csv/

# COMMAND ----------

lyT = spark.read \
  .option("inferSchema", "true") \
  .csv("/f1/out/lapYear5.csv")   
lyT.count()

# COMMAND ----------

ly.coalesce(1).write \
  .mode("overwrite") \
  .csv("/f1/out/lapYear10.csv")

# COMMAND ----------

# MAGIC %fs ls /f1/out/lapYear10.csv

# COMMAND ----------

# MAGIC %fs ls /f1/out/lapYear9999.parquet

# COMMAND ----------

ly.coalesce(1).write \
  .mode("overwrite") \
  .parquet("/f1/out/lapYear9999.parquet")
