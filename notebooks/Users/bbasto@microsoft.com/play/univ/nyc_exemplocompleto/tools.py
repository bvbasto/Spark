# Databricks notebook source
# MAGIC %scala 
# MAGIC 
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC import java.text.SimpleDateFormat
# MAGIC import java.util.{Calendar, Date}
# MAGIC 
# MAGIC //val path = dbutils.widgets.get("Path")
# MAGIC 
# MAGIC def computeFileStats(path:String):(Long,Long) = {
# MAGIC   var bytes = 0L
# MAGIC   var count = 0L
# MAGIC 
# MAGIC   import scala.collection.mutable.ArrayBuffer
# MAGIC   var files=ArrayBuffer(dbutils.fs.ls(path):_ *)
# MAGIC 
# MAGIC   while (files.isEmpty == false) {
# MAGIC     val fileInfo = files.remove(0)
# MAGIC     if (fileInfo.isDir == false) {
# MAGIC       count += 1
# MAGIC       bytes += fileInfo.size
# MAGIC     } else {
# MAGIC       files.append(dbutils.fs.ls(fileInfo.path):_ *)
# MAGIC     }
# MAGIC   }
# MAGIC   (count, bytes)
# MAGIC }
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC 
# MAGIC def getFSObjects(path: String):DataFrame ={
# MAGIC   var myDF = spark.createDataFrame(
# MAGIC     dbutils.fs.ls(path).map { info =>
# MAGIC       (info.name,info.path,info.size,info.isDir)
# MAGIC      }).toDF("name", "path", "size","isDir").orderBy("name")
# MAGIC   myDF
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get FS Stats - Python
# MAGIC 
# MAGIC only root file size
# MAGIC     display(bvbFSGetRootOnly("/mnt/adlsg2bvb/training/WorldDataBank/XindicatorsAPAGAR/"))
# MAGIC 
# MAGIC show only root, full size
# MAGIC     display(bvbFSGetRootWithCompleteStats("/mnt/adlsg2bvb/training/WorldDataBank/XindicatorsAPAGAR/"))
# MAGIC     
# MAGIC show full, full size
# MAGIC   display(bvbFSGetRootWithCompleteStatsFull("/mnt/adlsg2bvb/training/WorldDataBank/XindicatorsAPAGAR/"))
# MAGIC   
# MAGIC l = [list(row) for row in df.collect()]
# MAGIC i = 0
# MAGIC while (len(l) > 0):
# MAGIC   r = l.pop(0)
# MAGIC   i += 1
# MAGIC   if(r[3]==True):
# MAGIC     runFull(r[0],r[1])
# MAGIC     print(datetime.now(),i,r[1])

# COMMAND ----------

def bvbFSGetRootOnly(path):
  x = dbutils.fs.ls(path)
  l = list()
  while (len(x) > 0):
    fileInfo = x.pop(0)
    l0 = list((fileInfo.path,fileInfo.name,fileInfo.size,fileInfo.isDir()))
    l.append(l0)
  
  df = spark.createDataFrame(l,("path","name","size","isDir"))
  return df

def bvbFSGetRootWithCompleteStats(path):
  l = list()
  r = bvbFSGetRootWithCompleteStatsFullX(l,path,0,0,0)
  df = spark.createDataFrame(l,("level","path","name","size","isDir","dirsN1","filesN1","sizeN1","dirsTot","filesTot","sizeTot"))
  return df.orderBy("path","level")

def bvbFSGetRootWithCompleteStatsFull(path):
  l = list()
  r = bvbFSGetRootWithCompleteStatsFullX(l,path,0,1,0)
  df = spark.createDataFrame(l,("level","path","name","size","isDir","dirsN1","filesN1","sizeN1","dirsTot","filesTot","sizeTot"))
  return df.orderBy("path","level")

def bvbFSGetRootWithCompleteStatsFullX(l,path,level,full,i):
  #path = "/mnt/adlsg2bvb/training/WorldDataBank/XindicatorsAPAGAR/"
  Xfiles = dbutils.fs.ls(path)

  bytes1 = 0
  count1 = 0
  dirs1 = 0
  bytes = 0
  count = 0
  dirs = 0
  lv = level + 1
  
  while (len(Xfiles) > 0):
    fileInfo = Xfiles.pop(0)
    i += 1
    l0 = list((lv,fileInfo.path,fileInfo.name,fileInfo.size,fileInfo.isDir()))
    if (fileInfo.isDir() == False):               # isDir() is a method on the fileInfo object
      count1 += 1
      bytes1 += fileInfo.size                      # size is a parameter on the fileInfo object
      l1 = list((0,0,0,0,0,0))
    else:
      dirs1 += 1
      r1 = bvbFSGetRootWithCompleteStatsFullX(l,fileInfo.path,lv,full,i)
      l1 = list((r1[1],r1[2],r1[3],r1[4],r1[5],r1[6]))
      dirs +=  r1[4]
      count +=  r1[5]
      bytes +=  r1[6]  
    l0.extend(l1)
    if(lv == 1 or full == 1):
      l.append(l0)
    #if(i % 100 == 0):
    #  print("#",i)
    
  return list((0,dirs1,count1,bytes1, dirs + dirs1, count + count1, bytes+bytes1))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Get csv from URL - Python

# COMMAND ----------

import urllib.request

def bvbFSGet_File_FromURL(fullUrl,destinationPath,filename):
  urllib.request.urlretrieve(fullUrl,"/tmp/" + filename)
  dbutils.fs.mv("file:/tmp/" + filename,destinationPath + filename)

def bvbFSGet_DF_FromURL(fullUrl,destinationPath,filename,header,inferSchema):
  urllib.request.urlretrieve(fullUrl,"/tmp/" + filename)
  dbutils.fs.mv("file:/tmp/" + filename,destinationPath + filename)
  df = sqlContext.read.option("header",header).option("inferSchema",inferSchema).csv(destinationPath + filename)
  return df
  
  
#df = bvbFSGet_File_FromURL("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-06.csv","dbfs:/","yellow_tripdata_2019-06.csv",True,True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### log time
# MAGIC 
# MAGIC #####  from datetime import datetime
# MAGIC #####  print(datetime.now())

# COMMAND ----------

from datetime import datetime

def bvbLog(text):
  print(text,datetime.now())
