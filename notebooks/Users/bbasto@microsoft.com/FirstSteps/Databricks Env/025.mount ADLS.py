# Databricks notebook source
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "9f8dca3f-3641-407e-94fa-54e056a0e080",
           "dfs.adls.oauth2.credential": "VOyn7QwSjaq0o0hrALBYUlh5n2XnF77aEaAUn5BLR4w=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

dbutils.fs.mount(
  source = "adl://adlsfordatabricksbvb01.azuredatalakestore.net/",
  mount_point = "/mnt/MyADLS",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/MyADLS/

# COMMAND ----------

