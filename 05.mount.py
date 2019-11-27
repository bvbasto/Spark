# Databricks notebook source
# MAGIC %md # Versão de notebook feita para a community edition
# MAGIC 
# MAGIC O que queremos habitualmente é trabalhar dados com armazenamento externo ao próprio databricks. 
# MAGIC No cenário BigData em que tenho outros serviços a partilharem os mesmo dados, a gravação local não é o ideal. Queremos um armazenamento externo.
# MAGIC 
# MAGIC Neste exercício, para facilitar o acesso vamos utilizar os dados na conta local databricks.
# MAGIC 
# MAGIC Os ficheiros iniciais estão num Data Lake Storage, vamos fazer mount para copiar os ficheiros necessários.
# MAGIC 
# MAGIC nota:
# MAGIC Os ficheiros têm origem neste url: https://www.kaggle.com/theworldbank/world-development-indicators

# COMMAND ----------

############## READONLY #################
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "a32e3374-4070-4af4-a125-007ab1162cb6",
           "fs.azure.account.oauth2.client.secret": "Q/Q:x66Az?n.NnugjIYwDb9JZJ2cRVgi",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://main@adlsg2bvb.dfs.core.windows.net/",
  mount_point = "/mnt/adlsg2bvb",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md ####a partir de agora é possivel aceder ao Data Lake a partir de /mnt/MyADLS_training

# COMMAND ----------

dbutils.fs.ls('/mnt/adlsg2bvb/training')

# COMMAND ----------

# MAGIC %md
# MAGIC ## mas sem acesso de escrita

# COMMAND ----------

dbutils.fs.put('/mnt/adlsg2bvb/abc.txt','deffe')
