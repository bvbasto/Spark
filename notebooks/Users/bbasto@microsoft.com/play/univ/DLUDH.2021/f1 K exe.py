# Databricks notebook source
# MAGIC %sh pip install kaggle

# COMMAND ----------

# MAGIC %md login to kaggle and get api key from 

# COMMAND ----------

# MAGIC %sh
# MAGIC export KAGGLE_USERNAME=
# MAGIC export KAGGLE_KEY=
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

# MAGIC %md quantas colunas tem o ficheiro drivers ?

# COMMAND ----------

# MAGIC %md crie a tabela temp driver_standings

# COMMAND ----------

# MAGIC %md crie as tabelas lap times e races

# COMMAND ----------

# MAGIC %md em sql quantas corridas estao no ficheiro?

# COMMAND ----------

# MAGIC %md liste as corridas e o nome do piloto por ordem temporal
