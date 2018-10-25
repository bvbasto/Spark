// Databricks notebook source
// MAGIC %md 
// MAGIC # Ligação ao ADLS

// COMMAND ----------

spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "9f8dca3f-3641-407e-94fa-54e056a0e080")
spark.conf.set("dfs.adls.oauth2.credential", "VOyn7QwSjaq0o0hrALBYUlh5n2XnF77aEaAUn5BLR4w=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

// COMMAND ----------

val df = spark.read.json("adl://adlsfordatabricksbvb01.azuredatalakestore.net/files/small_radio_json.json")

// COMMAND ----------

df.show()


// COMMAND ----------

val specificColumnsDf = df.select("firstname", "lastname", "gender", "location", "level")
 specificColumnsDf.show()

// COMMAND ----------

val renamedColumnsDf = specificColumnsDf.withColumnRenamed("level", "subscription_type")
renamedColumnsDf.show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Ligação ao SQL DW

// COMMAND ----------

 val blobStorage = "bvbdemodb001.blob.core.windows.net"
 val blobContainer = "ct1"
 val blobAccessKey =  "T+D3g5yZZpUO6/pH4+2Dw49ju6ZwxJOpxh9X5U/96IqpgKzTTzq7saehWRvJ9XcLIYg3B/qJT2kBJTBlRidt+Q=="

// COMMAND ----------

val tempDir = "wasbs://ct1@bvbdemodb001.blob.core.windows.net/tempDirs"

// COMMAND ----------

val acntInfo = "fs.azure.account.key."+ blobStorage
 sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

// COMMAND ----------

//SQL Data Warehouse related settings
 val dwDatabase = "adwfordatabricksbvb01"
 val dwServer = "adwfordatabricksbvb01.database.windows.net" 
 val dwUser = "testDataBricks"
 val dwPass = "Qwerty123456"
 val dwJdbcPort =  "1433"
 val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
 val sqlDwUrl = "jdbc:sqlserver://adwfordatabricksbvb01.database.windows.net:1433;database=adwfordatabricksbvb01;user=testDataBricks@adwfordatabricksbvb01;password=Qwerty123456;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
 val sqlDwUrlSmall = "jdbc:sqlserver://adwfordatabricksbvb01.database.windows.net:1433;database=adwfordatabricksbvb01;user=testDataBricks@adwfordatabricksbvb01;password=Qwerty123456"

// COMMAND ----------

 spark.conf.set(
   "spark.sql.parquet.writeLegacyFormat",
   "true")

 renamedColumnsDf.write
     .format("com.databricks.spark.sqldw")
     .option("forward_spark_azure_storage_credentials","True")
     .option("url", sqlDwUrlSmall) 
     .option("dbtable", "SampleTable3")
     .option("tempdir", tempDir)
     .mode("overwrite")
     .save()

// COMMAND ----------

// MAGIC %md # teste Ligação ao SQL DB

// COMMAND ----------

//Using SQLDatabase driver
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

// COMMAND ----------

val tempDir = "wasbs://ct1@bvbdemodb001/tempDirs2"
val jdbcUsername = dbutils.secrets.get(scope = "jdbc", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "jdbc", key = "password")

// COMMAND ----------

val jdbcHostname = "adwfordatabricksbvb01.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "adwfordatabricksbvb01"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = "jdbc:sqlserver://adwfordatabricksbvb01.database.windows.net:1433;database=adwfordatabricksbvb01;user=testDataBricks;password=Qwerty123456;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


// COMMAND ----------

import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", "testDataBricks")
connectionProperties.put("password", "Qwerty123456")

// COMMAND ----------

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

val employees_table = spark.read.jdbc(jdbcUrl, "DimReseller", connectionProperties)

// COMMAND ----------

employees_table.printSchema

// COMMAND ----------

employees_table.take(10)

// COMMAND ----------

display(employees_table.select("AnnualSales", "YearOpened").groupBy("YearOpened").avg("AnnualSales"))

// COMMAND ----------

renamedColumnsDf.withColumnRenamed("subscription_type", "type")
     .write
     .mode(SaveMode.Append)
     .jdbc(jdbcUrl, "Mytable", connectionProperties)

// COMMAND ----------

renamedColumnsDf.show()

// COMMAND ----------

renamedColumnsDf.printSchema

// COMMAND ----------

// MAGIC %md #ligação com SparkSQL

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS jdbcTable

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE jdbcTable
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url "jdbc:sqlserver://adwfordatabricksbvb01.database.windows.net:1433;database=adwfordatabricksbvb01;user=testDataBricks;password=Qwerty123456;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;",
// MAGIC   dbtable "dbo.Mytable",
// MAGIC   user "testDataBricks",
// MAGIC   password "Qwerty123456"
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC Select * from jdbcTable

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO jdbcTable
// MAGIC SELECT * FROM jdbcTable LIMIT 10 

// COMMAND ----------

