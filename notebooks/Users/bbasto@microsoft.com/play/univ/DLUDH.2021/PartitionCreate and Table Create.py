# Databricks notebook source
# MAGIC %md
# MAGIC # Writing Data
# MAGIC 
# MAGIC Just as there are many ways to read data, we have just as many ways to write data.
# MAGIC 
# MAGIC In this notebook, we will take a quick peek at how to write data back out to Parquet files.
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC - Writing data to Parquet files

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Writing Data
# MAGIC 
# MAGIC Let's start with one of our original CSV data sources, **pageviews_by_second.tsv**:

# COMMAND ----------

from pyspark.sql.types import *

csvSchema = StructType([
  StructField("timestamp", StringType(), False),
  StructField("site", StringType(), False),
  StructField("requests", IntegerType(), False)
])

csvFile = "/mnt/adlsg2bvbRO/old_training/wikipedia/pageviews/pageviews_by_second.tsv"

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', "\t")
  .schema(csvSchema)
  .csv(csvFile)
)

# COMMAND ----------

display(csvDF)

# COMMAND ----------

csvDF.count()

# COMMAND ----------

csvDF2 = csvDF.select("site","requests")

# COMMAND ----------

csvDF2.show()

# COMMAND ----------

csvDF2.write.csv("/pageviews_by_second20.csv")

# COMMAND ----------

# MAGIC %fs ls /pageviews_by_second20.csv

# COMMAND ----------

csvDF2.write.parquet("/pageviews_by_second30")

# COMMAND ----------

# MAGIC %md partitions

# COMMAND ----------

display(csvDF)

# COMMAND ----------

csvD3 = csvDF.selectExpr("*","year(timestamp) as y","month(timestamp) as m","day(timestamp) as d")
display(csvD3)

# COMMAND ----------

csvD3.write.partitionBy("y", "m", "d").parquet("/pageviews_by_second_partitioned")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /pageviews_by_second_partitioned

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /pageviews_by_second_partitioned/y=2015

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /pageviews_by_second_partitioned/y=2015/m=3

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /pageviews_by_second_partitioned/y=2015/m=3/d=16

# COMMAND ----------

from pyspark.sql.types import *

pFile = "/pageviews_by_second_partitioned/y=2015/m=*/d=16"

df = spark.read \
  .parquet(pFile)


# COMMAND ----------

display(df.selectExpr("*","year(timestamp) as y","month(timestamp) as m","day(timestamp) as d"))

# COMMAND ----------

display(df.selectExpr("month(timestamp) as m","day(timestamp) as d").distinct())

# COMMAND ----------

# MAGIC %md 
# MAGIC # local and global tables
# MAGIC ## https://docs.databricks.com/data/tables.html
# MAGIC 
# MAGIC There are two types of tables: global and local. A global table is available across all clusters. Databricks registers global tables either to the Databricks Hive metastore or to an external Hive metastore. For details about Hive support, see Apache Hive compatibility. A local table is not accessible from other clusters and is not registered in the Hive metastore. This is also known as a temporary view.

# COMMAND ----------

display(csvDF)

# COMMAND ----------

#save as global table python
csvDF.write.saveAsTable("pageviews_by_second_FROMPH")

# COMMAND ----------

# MAGIC %sql 
# MAGIC --save as global table sql
# MAGIC 
# MAGIC create table pageviews_by_second_FROMSQL
# MAGIC USING PARQUET
# MAGIC LOCATION '/pageviews_by_second30'

# COMMAND ----------

#save as temp table python
csvDF.createOrReplaceTempView("pageviews_by_second_tv")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pageviews_by_second_FROMPH
# MAGIC UNION ALL
# MAGIC select count(*) from pageviews_by_second_FROMSQL
# MAGIC UNION ALL
# MAGIC select count(*) from pageviews_by_second_tv
