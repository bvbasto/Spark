# Databricks notebook source
# MAGIC %fs ls /mnt/MyADLS_training/_bvb/q1

# COMMAND ----------

df = (spark.read                      
   .option("header", "true")               
   .option("inferSchema", "true")  
   .csv("/mnt/MyADLS_training/_bvb/q1")                   
)

# COMMAND ----------

display(df)