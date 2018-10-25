# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.dropdown("X", "1", [str(x) for x in range(1, 10)], "hello this is a widget 2")

# COMMAND ----------

dbutils.widgets.get("X")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

