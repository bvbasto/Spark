# Databricks notebook source
# MAGIC %python
# MAGIC # ****************************************************************************
# MAGIC # Utility method to count & print the number of records in each partition.
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC def printRecordsPerPartition(df):
# MAGIC   print("Per-Partition Counts:")
# MAGIC   def countInPartition(iterator): yield __builtin__.sum(1 for _ in iterator)
# MAGIC   results = (df.rdd                   # Convert to an RDD
# MAGIC     .mapPartitions(countInPartition)  # For each partition, count
# MAGIC     .collect()                        # Return the counts to the driver
# MAGIC   )
# MAGIC   for result in results: print("* " + str(result))
# MAGIC   
# MAGIC # ****************************************************************************
# MAGIC # Utility to count the number of files in and size of a directory
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC def computeFileStats(path):
# MAGIC   bytes = 0
# MAGIC   count = 0
# MAGIC 
# MAGIC   files = dbutils.fs.ls(path)
# MAGIC   
# MAGIC   while (len(files) > 0):
# MAGIC     fileInfo = files.pop(0)
# MAGIC     if (fileInfo.isDir() == False):               # isDir() is a method on the fileInfo object
# MAGIC       count += 1
# MAGIC       bytes += fileInfo.size                      # size is a parameter on the fileInfo object
# MAGIC     else:
# MAGIC       files.extend(dbutils.fs.ls(fileInfo.path))  # append multiple object to files
# MAGIC       
# MAGIC   return (count, bytes)
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Utility method to cache a table with a specific name
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC def cacheAs(df, name, level):
# MAGIC   print("WARNING: The PySpark API currently does not allow specification of the storage level - using MEMORY-ONLY")
# MAGIC   
# MAGIC   try: spark.catalog.uncacheTable(name)
# MAGIC   except AnalysisException: None
# MAGIC   
# MAGIC   df.createOrReplaceTempView(name)
# MAGIC   spark.catalog.cacheTable(name)
# MAGIC   #spark.catalog.cacheTable(name, level)
# MAGIC   return df
# MAGIC 
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Simplified benchmark of count()
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC def benchmarkCount(func):
# MAGIC   import time
# MAGIC   start = float(time.time() * 1000)                    # Start the clock
# MAGIC   df = func()
# MAGIC   total = df.count()                                   # Count the records
# MAGIC   duration = float(time.time() * 1000) - start         # Stop the clock
# MAGIC   return (df, total, duration)
# MAGIC 
# MAGIC None