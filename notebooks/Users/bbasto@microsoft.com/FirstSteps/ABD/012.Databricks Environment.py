# Databricks notebook source
sqlContext.range(5).toDF("value").createOrReplaceGlobalTempView("my_data")
dbutils.notebook.exit("my_data")

# COMMAND ----------

