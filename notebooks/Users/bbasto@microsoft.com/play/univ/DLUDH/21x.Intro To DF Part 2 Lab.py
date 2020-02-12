# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to DataFrames, Lab #2
# MAGIC ## Washingtons and Adams

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Instructions
# MAGIC 
# MAGIC This data was captured in the August before the 2016th presidential election.
# MAGIC 
# MAGIC As a result, articles about the candidates were very popular.
# MAGIC 
# MAGIC For this exercise, you will...
# MAGIC 0. Filter the result to the **en** Wikipedia project.
# MAGIC 0. Find all the articles where the name of the article ends with **_Washington** (presumably "George Washington", "Martha Washington, etc)
# MAGIC 0. Return all records as an array to the Driver.
# MAGIC 0. Total the requests for the Washingtons and assign it to the variable `totalWashingtons`.
# MAGIC 0. Assign your array of Washingtons (the return value of your action) to the variable `washingtons`.
# MAGIC 
# MAGIC ** Bonus **
# MAGIC 
# MAGIC Repeat the exercise for the Adams
# MAGIC 0. Filter the result to the **en** Wikipedia project.
# MAGIC 0. Find all the articles where the name of the article ends with **_Adams** (presumably "John Adams", "John Quincy Adams", etc)
# MAGIC 0. Return all records as an array to the Driver.
# MAGIC 0. Total the requests for the Adamas and assign it to the variable `totalAdams`.
# MAGIC 0. Assign your array of Adamas (the return value of your action) to the variable `adams`.
# MAGIC 0. But you cannot do it the same way twice:
# MAGIC    * In the filter, don't use the same conditional method as the one used for the Washingtons.
# MAGIC    * Don't use the same action as used for the Washingtons.
# MAGIC 
# MAGIC **Testing**
# MAGIC 
# MAGIC Run the last cell to verify that your results are correct.
# MAGIC 
# MAGIC **Hints**
# MAGIC * The actions we've explored for extracting data include:
# MAGIC   * `first()`
# MAGIC   * `collect()`
# MAGIC   * `head()`
# MAGIC   * `take(n)`
# MAGIC * The conditional methods used with a `filter(..)` include:
# MAGIC   * equals
# MAGIC   * not-equals
# MAGIC   * starts-with
# MAGIC   * and there are others - remember, the `DataFrames` API is built upon an SQL engine.
# MAGIC * There shouldn't be more than 1000 records for either the Washingtons or the Adams

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Show Your Work

# COMMAND ----------

# TODO
#
# Replace <<FILL_IN>> with your code. You will probably need multiple
# lines of code for this problem.

parquetDir = "/mnt/MyADLS_training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

washingtons = <<FILL_IN>>

totalWashingtons = 0

for washington in washingtons:
  totalWashingtons += <<FILL_IN>>
  
print("Total Washingtons: {0:,}".format( len(washingtons) ))
print("Total Washington Requests: {0:,}".format( totalWashingtons ))

# COMMAND ----------

# TODO
#
# Replace <<FILL_IN>> with your code. You will probably need multiple
# lines of code for this problem.

adams = <<FILL_IN>>

totalAdams = 0

for adam in adams:
  totalAdams += <<FILL_IN>>

print("Total Adams: {0:,}".format( len(adams) ))
print("Total Adam Requests: {0:,}".format( totalAdams ))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Verify Your Work
# MAGIC Run the following cell to verify that your `DataFrame` was created properly.

# COMMAND ----------

print("Total Washingtons: {0:,}".format( len(washingtons) ))
print("Total Washington Requests: {0:,}".format( totalWashingtons ))

expectedCount = 466
assert len(washingtons) == expectedCount, "Expected " + str(expectedCount) + " articles but found " + str( len(washingtons) )

expectedTotal = 3266
assert totalWashingtons == expectedTotal, "Expected " + str(expectedTotal) + " requests but found " + str(totalWashingtons)

# COMMAND ----------

print("Total Adams: {0:,}".format( len(adams) ))
print("Total Adam Requests: {0:,}".format( totalAdams ))

expectedCount = 235
assert len(adams) == expectedCount, "Expected " + str(expectedCount) + " articles but found " + str( len(adams) )

expectedTotal = 3126
assert totalAdams == expectedTotal, "Expected " + str(expectedTotal) + " requests but found " + str(totalAdams)