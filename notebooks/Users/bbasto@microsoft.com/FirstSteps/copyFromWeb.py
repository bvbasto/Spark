# Databricks notebook source
# MAGIC %sh
# MAGIC 
# MAGIC wget https://github.com/bvbasto/Spark/blob/master/notebooks/Users/bbasto%40microsoft.com/FirstSteps/Chicago-Crimes-2018.csv

# COMMAND ----------

# MAGIC %sh ls 

# COMMAND ----------

# MAGIC %sh ls /

# COMMAND ----------

dbutils.fs.ls("file:/")

# COMMAND ----------

dbutils.fs.ls("file:/databricks/driver/")

# COMMAND ----------

dbutils.fs.cp("file:/databricks/driver/Chicago-Crimes-2018.csv","dbfs:/Chicago-Crimes-2018.csv")

# COMMAND ----------

dbutils.fs.ls("/")