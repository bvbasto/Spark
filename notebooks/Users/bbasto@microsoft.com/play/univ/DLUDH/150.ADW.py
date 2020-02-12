# Databricks notebook source
spark.conf.set(
   "spark.sql.parquet.writeLegacyFormat",
   "true")

spark.conf.set(
  "fs.azure.account.key.bvbdemodb001.blob.core.windows.net",
  "T+D3g5yZZpUO6/pH4+2Dw49ju6ZwxJOpxh9X5U/96IqpgKzTTzq7saehWRvJ9XcLIYg3B/qJT2kBJTBlRidt+Q==")

# Get some data from a SQL DW table.
df = spark.read \
  .format("com.databricks.spark.sqldw")\
  .option("forward_spark_azure_storage_credentials", "true")\
  .option("url", "jdbc:sqlserver://demoiseg.database.windows.net:1433;database=demoiseg;user=bvbasto@demoiseg;password=Qwerty654321")\
  .option("tempDir", "wasbs://ct1@bvbdemodb001.blob.core.windows.net/tempDirs")\
  .option("dbTable", "dbo.DimProduct")\
  .load()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE dimProduct

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dimProduct
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://demoiseg.database.windows.net:1433;database=demoiseg;user=bvbasto@demoiseg;password=Qwerty654321;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;",
# MAGIC   dbtable "dbo.DimProduct",
# MAGIC   user "bvbasto",
# MAGIC   password "Qwerty654321"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from  dimProduct

# COMMAND ----------


df = sqlContext.range(5).toDF("value")

df.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://demoiseg.database.windows.net:1433;database=demoiseg;user=bvbasto@demoiseg;password=Qwerty654321") \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("dbTable", "dbo.aaaDB") \
  .option("tempDir", "wasbs://ct1@bvbdemodb001.blob.core.windows.net/tempDirs")\
  .mode("overwrite")\
  .save()

# COMMAND ----------

display(sqlContext.range(5).toDF("value"))