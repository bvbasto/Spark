// Databricks notebook source
displayHTML(s"""
<h3>
  <img width="200px" src="https://spark.apache.org/images/spark-logo-trademark.png"/> 
  + 
  <img src="http://training.databricks.com/databricks_guide/databricks_logo_400px.png"/>
</h3>
""")

// COMMAND ----------

// MAGIC %md # Introduction to Apache Spark

// COMMAND ----------

// MAGIC %md Spark is a unified processing engine that can analyze big data using SQL, machine learning, graph processing or real time stream analysis:
// MAGIC 
// MAGIC ![Spark Engines](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_4engines.png)
// MAGIC 
// MAGIC We will mostly focus on SparkSessions, DataFrames/Datasets and a bit on Structured Streaming this evening.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Apache Spark
// MAGIC Apache Spark is a powerful open-source processing engine built around speed, ease of use, and sophisticated analytics, with APIs in Java, Scala, Python, R, and SQL. Spark runs programs up to 100x faster than Hadoop MapReduce in memory, or 10x faster on disk. It can be used to build data applications as a library, or to perform ad-hoc data analysis interactively. Spark powers a stack of libraries including SQL, DataFrames, and Datasets, MLlib for machine learning, GraphX for graph processing, and Spark Streaming. You can combine these libraries seamlessly in the same application

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 2. RDD
// MAGIC At the core of Apache Spark is the notion of data abstraction as distributed collection of objects. This data abstraction, called Resilient Distributed Dataset (RDD), allows you to write programs that transform these distributed datasets.
// MAGIC RDDs are immutable distributed collection of elements of your data that can be stored in memory or disk across a cluster of machines. 

// COMMAND ----------

// MAGIC %md
// MAGIC # Open textFile for Spark Context RDD
// MAGIC 
// MAGIC text_file = spark.textFile("hdfs://…")
// MAGIC # Execute word count
// MAGIC 
// MAGIC text_file.flatMap(lambda line: line.split())
// MAGIC 
// MAGIC     .map(lambda word: (word, 1))
// MAGIC 
// MAGIC     .reduceByKey(lambda a, b: a+b)

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. DataFrame
// MAGIC Like an RDD, a DataFrame is an immutable distributed collection of data. Unlike an RDD, data is organized into named columns, like a table in a relational database. Designed to make large data sets processing even easier, DataFrame allows developers to impose a structure onto a distributed collection of data, allowing higher-level abstraction; it provides a domain specific language API to manipulate your distributed data; and makes Spark accessible to a wider audience, beyond specialized data engineers.

// COMMAND ----------

// MAGIC %md 
// MAGIC # Read JSON file and register temp view
// MAGIC 
// MAGIC context.jsonFile("s3n://…").createOrReplaceTempView("json")
// MAGIC # Execute SQL query
// MAGIC 
// MAGIC results = context.sql("""SELECT * FROM people JOIN json …""")

// COMMAND ----------

// MAGIC %md A Spark cluster is made of one Driver and many Executor JVMs (java virtual machines):

// COMMAND ----------

// MAGIC %md ![Spark Physical Cluster, slots](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_cluster_slots.png)

// COMMAND ----------

// MAGIC %md The Driver sends Tasks to the empty slots on the Executors when work has to be done:

// COMMAND ----------

// MAGIC %md ![Spark Physical Cluster, tasks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_cluster_tasks.png)

// COMMAND ----------

// MAGIC %md In Databricks Community Edition, everyone gets a local mode cluster, where the Driver and Executor code run in the same JVM. Local mode clusters are typically used for prototyping and learning Spark:

// COMMAND ----------

// MAGIC %md ![Notebook + Micro Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/notebook_microcluster.png)

// COMMAND ----------

// MAGIC %md ####![Spark Operations](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/spark_ta.png)

// COMMAND ----------

// MAGIC %md DataFrames support two types of operations: *transformations* and *actions*.
// MAGIC 
// MAGIC Transformations, like `select()` or `filter()` create a new DataFrame from an existing one, resulting into another immutable DataFrame. All transformations are lazy. That is, they are not executed until an `action` is invoked or performed.
// MAGIC 
// MAGIC Actions, like `show()` or `count()`, return a value with results to the user. Other actions like `save()` write the DataFrame to distributed storage (like S3 or HDFS).

// COMMAND ----------

// MAGIC %md Transformations contribute to a query plan,  but  nothing is executed until an action is called. 
// MAGIC 
// MAGIC We will get into DataFrames & Datasets more deeply in Workshop 2 but here is a good place to get a feel for it...

// COMMAND ----------

// MAGIC %md ####![Spark T/A](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pagecounts/trans_and_actions.png)

// COMMAND ----------

// MAGIC %md #How to Use SparkSession - A Unified Entry Point in Apache Spark 2.0#
// MAGIC In Spark 2.0, we introduced SparkSession, a new entry point that subsumes SparkContext, SQLContext, StreamingContext, and HiveContext. For backward compatibiilty, they are preserved. SparkSession has many features, and in this notebook we expatiate, by way of simple code examples, some of the more important ones, using data to illustrate its access to underlying Spark functionality. Even though, this notebook is written in Scala, similar functionality and APIs exist in Python and Java.
// MAGIC 
// MAGIC In Databricks notebooks and Spark REPL, the SparkSession is created for you, stored in a variable called *spark.*
// MAGIC 
// MAGIC It subsumes SparkContext, HiveContext, SparkConf, and StreamingContext

// COMMAND ----------

// MAGIC %md Keep this URL handy in your browser tab the [SparkSession APIs](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession).
// MAGIC Let's use it to try some API calls

// COMMAND ----------

// MAGIC %md ##PART 1: Exploring SparkSession##
// MAGIC For backward compatibility, you can access `SparkContext`, `SQLContext`, and `SparkConf`. To autocomplete the instance use "." + "tab" at the end of the instance variable

// COMMAND ----------

// MAGIC %md ## Executing Shell or Linux commands
// MAGIC %sh free -mh
// MAGIC 
// MAGIC %sh jps
// MAGIC 
// MAGIC %sh jps –v
// MAGIC 
// MAGIC %sh ps -fe
// MAGIC 
// MAGIC %sh ls -l /tmp

// COMMAND ----------

// MAGIC %sh ps -fe

// COMMAND ----------

// MAGIC %md **Q1**: What version of Spark we are running? 

// COMMAND ----------

spark

// COMMAND ----------

spark.version

// COMMAND ----------

spark.sparkContext

// COMMAND ----------

// MAGIC %md ###SparkContext as part of SparkSession###
// MAGIC Preserved as part of SparkSession for backward compatibility. 

// COMMAND ----------

spark.sparkContext.version

// COMMAND ----------

// MAGIC %md ###sqlContext as part of SparkSession### 
// MAGIC Preserved as part of SparkSession for backward compatibility

// COMMAND ----------

spark.sqlContext

// COMMAND ----------

// MAGIC %md ## Configuring Spark's runtime configuration parameters ##

// COMMAND ----------

// MAGIC %md ** Q2 **: What variables can be set and accessed?

// COMMAND ----------

// MAGIC %md ###SparkConf as part of SparkSession###
// MAGIC Through *spark.conf*, You manipulate Spark's runtime configruation parameters. Note that all configuration options set are automatically propagated over to Spark and Hadoop during I/O.
// MAGIC 
// MAGIC Unlike Spark 1.6, you had to create an instance of `SparkConf`, using `SparkContext`, whereas in Spark 2.0 that same level of functionality is offered via `SparkSession`, and the instance variable in Notebook and REPL is *`spark`*

// COMMAND ----------

spark.conf.set("spark.notebook.name", "SparkSessionWKSH_1")

// COMMAND ----------

spark.conf.get("spark.notebook.name")

// COMMAND ----------

spark.conf.get("spark.sql.warehouse.dir")

// COMMAND ----------

// MAGIC %md ** Q3 **: How do I enumerate over all default Spark Runtime Config Variables?

// COMMAND ----------

// returns a Scala Map. Using Scala's collection APIs you can iterate over each (key, value)
val configMap = spark.conf.getAll
configMap.foreach(println)

// COMMAND ----------

// MAGIC %md Spark config variables set can be accessed via SQL with variable subsitution

// COMMAND ----------

// MAGIC %sql select "${spark.notebook.name}, ${spark.sql.warehouse.dir}, ${spark.akka.frameSize}"

// COMMAND ----------

// MAGIC %md ###Working and Accessing Catalog metadata###

// COMMAND ----------

// MAGIC %md Through the `SparkSession.catalog` field instance you can access all the Catalog metadata information about your tables, database, UDFs etc.
// MAGIC 
// MAGIC Note: `spark.catalog.<func>` returns a Dataset and can be display as table

// COMMAND ----------

spark.catalog.listDatabases.show(false)

// COMMAND ----------

// MAGIC %md `display()` is a Databricks Noteboobk method that displays DataFrames/Datasets in a tabular form.

// COMMAND ----------

display(spark.catalog.listDatabases)

// COMMAND ----------

display(spark.catalog.listTables)

// COMMAND ----------

// MAGIC %md ###PART 2: Creating DataFrames and Datasets with SparkSession###
// MAGIC There are a number of ways to create DataFrames and Datasets using the [SparkSession APIs](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession). Once either a DataFrame or [Dataset](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset) is created, you can manipulate your data. For example, for quick exploration of Datasets, you can use the `spark.range.` Documentation of [Spark SQL functions](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$).

// COMMAND ----------

//imports some built in SQL aggregate and other functions
import org.apache.spark.sql.functions._

// COMMAND ----------

//create a dataset of numbers, in increments of 5 starting at 5
val numDS = spark.range(0, 100000, 5)

// COMMAND ----------

display(numDS)

// COMMAND ----------

// MAGIC %python 
// MAGIC numDS2 = spark.range(0, 100000, 5)

// COMMAND ----------



// COMMAND ----------

display(numDS.orderBy(desc("id")))

// COMMAND ----------

// MAGIC %md ### Create Summary Descriptive Stats###

// COMMAND ----------

display(numDS.describe())

// COMMAND ----------

// MAGIC %md ### Creating a DataFrame from a collection with SparkSession###

// COMMAND ----------

// MAGIC %md Inferes the schema of the DataFrame from the type of the collection values

// COMMAND ----------

val langPercentDF = spark.createDataFrame(List(("Scala", 35), ("SQL", 15), ("Python", 25), ("R", 5), ("Java", 20))).toDF("language", "percent")


// COMMAND ----------

display(langPercentDF.orderBy(desc("percent")))

// COMMAND ----------

// MAGIC %md **Q5**: Can you save this as permanent table and list the table?
// MAGIC 
// MAGIC Once you have created the table, go to the Tabels Navigation on the left, and see if the table is created, and check its schema.

// COMMAND ----------

langPercentDF.write.saveAsTable("languages")

// COMMAND ----------

display(spark.catalog.listTables)

// COMMAND ----------

// MAGIC %md ###Challenge: 
// MAGIC Create couple of simple datafames or datasets using `SparkSession`, save them as a temporary tables, and run some transformations on it
// MAGIC 
// MAGIC Consult the [SparkSession API Documentation](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession)

// COMMAND ----------

// range always returns at Dataset
val employeeDS = spark.range(0, 500000).
                select($"id".as("employee_id"), 
                (rand() * 3).cast("int").as("dep_id"), 
                (rand() * 40 + 20).cast("int").as("age"))
                .cache()
// create a temporary table in memory
employeeDS.createOrReplaceTempView("employees")

// COMMAND ----------

// MAGIC %md At this point the cache is realized. Look at Spark UI to see how it's cached.

// COMMAND ----------

employeeDS.count()

// COMMAND ----------

display(employeeDS)

// COMMAND ----------

// MAGIC %md Look at the Spark UI to see what's cached

// COMMAND ----------

// MAGIC %md get a list of tables names using `SparkSession.catalog` methods

// COMMAND ----------

display(spark.catalog.listTables)

// COMMAND ----------

// MAGIC %sql select employee_id, age from employees where age > 50

// COMMAND ----------

// MAGIC %md ## Challenge:
// MAGIC 
// MAGIC Use Dataset API or SQL to make some queries on the `employees` table or `employeesDS`

// COMMAND ----------

// MAGIC %md ##Thank you & have fun & try other things in your free time...!