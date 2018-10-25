# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

dbutils.fs.mkdirs("/foobar/")

# COMMAND ----------

dbutils.fs.put("/foobar/baz.txt", "Hello, World!")
dbutils.fs.head("/foobar/baz.txt")
dbutils.fs.rm("/foobar/baz.txt")
dbutils.fs.ls("dbfs:/")

# COMMAND ----------

dbutils.fs.ls("file:/")

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.io.Source
# MAGIC 
# MAGIC val filename = "/dbfs/tmp/test_dbfs.txt"
# MAGIC for (line <- Source.fromFile(filename).getLines()) {
# MAGIC   println(line)
# MAGIC }

# COMMAND ----------

#write a file to DBFS using python i/o apis
with open("/dbfs/tmp/test_dbfs.txt", 'w') as f:
  f.write("Apache Spark is awesome!\n")
  f.write("End of example!")

# read the file
with open("/dbfs/tmp/test_dbfs.txt", "r") as f_read:
  for line in f_read:
    print line

# COMMAND ----------

