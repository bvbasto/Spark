# Databricks notebook source
# MAGIC %md 
# MAGIC ####Copiar os ficheiros para a conta local 
# MAGIC ####Esta não deve ser a pratica a seguir!

# COMMAND ----------

dbutils.fs.rm('/WorldDataBank/',True)

# COMMAND ----------

dbutils.fs.ls('/mnt/adlsg2bvb/training/_bvb/WorldDataBank/WorldDevelopmentIndicators')

# COMMAND ----------

dbutils.fs.cp("/mnt/adlsg2bvb/training/_bvb/WorldDataBank/WorldDevelopmentIndicators/WDIData.csv",
             "/WorldDataBank/WDIData.csv")

# COMMAND ----------

dbutils.fs.cp("/mnt/adlsg2bvb/training/_bvb/WorldDataBank/WorldDevelopmentIndicators/WDICountry.csv",
             "/WorldDataBank/WDICountry.csv")

# COMMAND ----------

dbutils.fs.ls("/WorldDataBank")
