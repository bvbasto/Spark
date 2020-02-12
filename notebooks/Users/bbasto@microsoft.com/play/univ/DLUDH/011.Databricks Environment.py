# Databricks notebook source
# MAGIC %scala
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC spark.conf.set("com.databricks.training.username", username)

# COMMAND ----------

username = spark.conf.get("com.databricks.training.username")

# COMMAND ----------

myname2 = "coisasdt5y65y76u"