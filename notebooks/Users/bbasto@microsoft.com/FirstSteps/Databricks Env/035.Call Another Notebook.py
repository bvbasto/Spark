# Databricks notebook source
## In caller notebook
returned_table = dbutils.notebook.run("./036.Notebook to be called", 60)
global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
display(table(global_temp_db + "." + returned_table))

# COMMAND ----------

