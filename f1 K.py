# Databricks notebook source
# MAGIC %sh pip install kaggle

# COMMAND ----------

# MAGIC %md login to kaggle and get api key from 

# COMMAND ----------

# MAGIC %sh
# MAGIC export KAGGLE_USERNAME=bvbasto75
# MAGIC export KAGGLE_KEY=7899776dd920e31e03059f00c0ca1148
# MAGIC kaggle datasets download rohanrao/formula-1-world-championship-1950-2020 --force

# COMMAND ----------

# MAGIC %sh 
# MAGIC mkdir /databricks/driver/f15020/

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /databricks/driver/formula-1-world-championship-1950-2020.zip /databricks/driver/f15020/formula-1-world-championship-1950-2020.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /databricks/driver/f15020/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /databricks/driver/formula-1-world-championship-1950-2020.zip

# COMMAND ----------

# dbutils.fs.rm("/f1",True)
dbutils.fs.mkdirs("/f1")

# COMMAND ----------

dbutils.fs.cp("file:/databricks/driver/circuits.csv", "/f1/circuits.csv")  
dbutils.fs.cp("file:/databricks/driver/constructor_results.csv", "/f1/constructor_results.csv")  
dbutils.fs.cp("file:/databricks/driver/constructor_standings.csv", "/f1/constructor_standings.csv")
dbutils.fs.cp("file:/databricks/driver/constructors.csv", "/f1/constructors.csv") 
dbutils.fs.cp("file:/databricks/driver/driver_standings.csv", "/f1/driver_standings.csv") 
dbutils.fs.cp("file:/databricks/driver/drivers.csv", "/f1/drivers.csv") 
dbutils.fs.cp("file:/databricks/driver/lap_times.csv", "/f1/lap_times.csv") 
dbutils.fs.cp("file:/databricks/driver/pit_stops.csv", "/f1/pit_stops.csv")
dbutils.fs.cp("file:/databricks/driver/qualifying.csv", "/f1/qualifying.csv") 
dbutils.fs.cp("file:/databricks/driver/races.csv", "/f1/races.csv") 
dbutils.fs.cp("file:/databricks/driver/results.csv", "/f1/results.csv")
dbutils.fs.cp("file:/databricks/driver/seasons.csv", "/f1/seasons.csv")
dbutils.fs.cp("file:/databricks/driver/status.csv", "/f1/status.csv")

# COMMAND ----------

display(dbutils.fs.ls("/f1"))

# COMMAND ----------

# MAGIC %md quantas seasons estao presentes?

# COMMAND ----------

fileNameSeasons = "/f1/seasons.csv"

s = spark.read \
  .option("inferSchema", "true") \
  .option("header","true") \
  .csv(fileNameSeasons)   

display(s)

# COMMAND ----------

s.count()

# COMMAND ----------

# MAGIC %md quantas colunas tem o ficheiro drivers ?

# COMMAND ----------

d = spark.read \
  .option("inferSchema", "true") \
  .option("header","true") \
  .csv("/f1/drivers.csv")   

display(d)

# COMMAND ----------

d.printSchema()

# COMMAND ----------

d.columns

# COMMAND ----------

len(d.columns)

# COMMAND ----------

# MAGIC %md crie a tabela temp driver_standings

# COMMAND ----------

fileNameResults = "/f1/driver_standings.csv"

ds = spark.read \
  .option("inferSchema", "true") \
  .option("header","true") \
  .csv(fileNameResults)   

display(ds)

# COMMAND ----------

ds.createOrReplaceTempView("driver_standings1")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from driver_standings1 

# COMMAND ----------

# MAGIC %md crie as tabelas lap times e races

# COMMAND ----------

fileNameLT = "/f1/lap_times.csv"

lt = spark.read \
  .option("inferSchema", "true") \
  .option("header","true") \
  .csv(fileNameLT)   

lt.createOrReplaceTempView("lapTimes")

fileNameRaces = "/f1/races.csv"

rc = spark.read \
  .option("inferSchema", "true") \
  .option("header","true") \
  .csv(fileNameRaces)   

rc.createOrReplaceTempView("races")

# COMMAND ----------

# MAGIC %md em sql quantas corridas estao no ficheiro?

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from races

# COMMAND ----------

# MAGIC %md liste as corridas e o nome do piloto por ordem temporal

# COMMAND ----------


ct = spark.read \
  .option("inferSchema", "true") \
  .option("header","true") \
  .csv("/f1/driver_standings.csv")   

ct.createOrReplaceTempView("driverS")

d = spark.read \
  .option("inferSchema", "true") \
  .option("header","true") \
  .csv("/f1/drivers.csv")   

d.createOrReplaceTempView("driver")

r = spark.read \
  .option("inferSchema", "true") \
  .option("header","true") \
  .csv("/f1/races.csv")   

r.createOrReplaceTempView("races")


display(ct)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from driverS  where position = 1 order by raceid 

# COMMAND ----------

# MAGIC %sql select * from driver

# COMMAND ----------

# MAGIC %sql 
# MAGIC select r.date,r.name, d.surname driver  
# MAGIC   from races r 
# MAGIC     inner join driverS ds on r.raceID = ds.raceId
# MAGIC     inner join driver d on ds.driverId = d.driverId
# MAGIC   where ds.position = 1 order by 1 asc 
