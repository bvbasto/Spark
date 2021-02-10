# Databricks notebook source
############## READONLY #################
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

# MAGIC %fs ls /mnt/adlsg2bvbRO/old_training/covid/ww 

# COMMAND ----------

# MAGIC %md quantos dias estão no ficheiro ?
# MAGIC 
# MAGIC para passar a data para tipo date (criar uma nova coluna) ver aqui
# MAGIC --https://medium.com/expedia-group-tech/deep-dive-into-apache-spark-datetime-functions-b66de737950a

# COMMAND ----------

# MAGIC %md qual o valor para os sintomas no dia mais recente, mostre os 5 sintomas e respetivo valor?

# COMMAND ----------

# MAGIC %md crie um grafico do tipo barras com a % de cresciment diário dos casos confirmados
# MAGIC --para adicionar dias ver aqui
# MAGIC --https://spark.apache.org/docs/2.3.0/api/sql/index.html#date
