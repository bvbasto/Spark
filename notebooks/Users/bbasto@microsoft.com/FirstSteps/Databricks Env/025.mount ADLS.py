# Databricks notebook source
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "28dd86ad-d24a-4183-9dc7-d42790a1116c",
           "dfs.adls.oauth2.credential": "Eez/ykW2aVVQrzFKAr1KwB44yHt1xGFpUOjhKFZtpM0=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

dbutils.fs.mount(
  source = "adl://adlsfordatabricksbvb01.azuredatalakestore.net/nyc-taxi",
  mount_point = "/mnt/MyADLS",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/MyADLS/
# MAGIC #dbutils.fs.ls("/mnt/MyADLS")

# COMMAND ----------

dbutils.fs.unmount("/mnt/MyADLS")