# Databricks notebook source
# MAGIC %md
# MAGIC # Versão de notebook feita para a community edition
# MAGIC 
# MAGIC O que queremos habitualmente é trabalhar dados com armazenamento externo ao próprio databricks. 
# MAGIC No cenário BigData em que tenho outros serviços a partilharem os mesmo dados, a gravação local não é o ideal. Queremos um armazenamento externo.
# MAGIC 
# MAGIC Neste exercício, para facilitar o acesso vamos utilizar os dados na conta local databricks.
# MAGIC 
# MAGIC Os ficheiros iniciais estão num Data Lake Storage, vamos fazer mount para copiar os ficheiros necessários.
# MAGIC 
# MAGIC nota:
# MAGIC Os ficheiros têm origem neste url: https://www.kaggle.com/theworldbank/world-development-indicators

# COMMAND ----------

dbutils.fs.unmount("/mnt/adlsg2bvbRO")

# COMMAND ----------

############## READONLY #################
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "a32e3374-4070-4af4-a125-007ab1162cb6",
           "fs.azure.account.oauth2.client.secret": "Q/Q:x66Az?n.NnugjIYwDb9JZJ2cRVgi",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}


dbutils.fs.mount(
  source = "abfss://main@adlsg2bvb.dfs.core.windows.net/",
  mount_point = "/mnt/adlsg2bvbRO",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md 
# MAGIC ####a partir de agora é possivel aceder ao Data Lake a partir de /mnt/adlsg2bvbRO

# COMMAND ----------

mntADLS = "/mnt/adlsg2bvbRO"

dbutils.fs.ls(mntADLS)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ####Copiar os ficheiros para a conta local

# COMMAND ----------

dbutils.fs.cp(mntADLS + "/training/_bvb/WorldDataBank/WorldDevelopmentIndicators/WDIData.csv",
             "/WorldDataBank/WDIData.csv")

# COMMAND ----------

dbutils.fs.cp(mntADLS + "/training/_bvb/WorldDataBank/WorldDevelopmentIndicators/WDICountry.csv",
             "/WorldDataBank/WDICountry.csv")

# COMMAND ----------

dbutils.fs.ls("/WorldDataBank")

# COMMAND ----------

# MAGIC %md
# MAGIC ####a partir daqui o exercício é idêntico mas utilizando os ficheiros locais

# COMMAND ----------

csvFile = "/WorldDataBank/WDIData.csv"

df = (spark.read           
  .option("header", "true") 
  .option("sep", ",") 
  .option("inferSchema", "true")
  .csv(csvFile)
)

df.printSchema()

# COMMAND ----------

#display(df)
df.count()

# COMMAND ----------

#cls = ('Indicator Code','Indicator Name')
df.select('Indicator Code').distinct().count()
# display(df.select([c for c in df.columns if c in cls]).distinct())

# COMMAND ----------

import pyspark.sql.functions as f
df2 = df.filter(f.col('Indicator Code')=='NE.RSB.GNFS.KN')
display(df2)

# COMMAND ----------

df

# COMMAND ----------

def squared(s):
  return s * s
spark.udf.register("squaredWithPython", squared)

# COMMAND ----------

spark.range(1, 20).registerTempTable("test")

# COMMAND ----------

# MAGIC %sql select id, squaredWithPython(id) as id_squared from test

# COMMAND ----------

from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame

def melt_df(df,id_vars, value_vars,var_name,value_name):
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)
  
#import pandas as pd

#pdf = pd.DataFrame({'A': {0: 'a', 1: 'b', 2: 'c'},
#                   'B': {0: 1, 1: 3, 2: 5},
#                   'C': {0: 2, 1: 4, 2: 6}})

#pd.melt(pdf, id_vars=['A'], value_vars=['B', 'C'])
#sdf = spark.createDataFrame(pdf)
#melt_df(sdf, id_vars=['A'], value_vars=['B', 'C'],var_name='Col_Name',value_name='NA').show()

# COMMAND ----------

df2 = melt_df(df, id_vars=['Country Code','Indicator Code'], value_vars=['1963','1964','1965','1966','1967','1968','1969','1970','1971','1972','1973','1974','1975','1976','1977','1978','1979','1980','1981','1982','1983','1984','1985','1986','1987','1988','1989','1990','1991','1992','1993','1994','1995','1996','1997','1998','1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015','2016','2017'],var_name='Ano',value_name='Valor')
df2.count()
#display(df2)

# COMMAND ----------

from pyspark.sql.types import IntegerType

df3 = df2 \
  .withColumnRenamed("Country Code", "idCountry") \
  .withColumnRenamed("Indicator Code", "idIndicator") \
  .withColumn("Ano", df2["Ano"].cast(IntegerType())) \
  .na.drop(subset=["Valor"]) 
df3.printSchema()
#df3.count()
#display(df3)

# COMMAND ----------

destDataDir = "/WorldDataBank/parquet/data_csv"
df3.repartition(1).write.mode('overwrite').option("header", "true").csv(destDataDir)

# COMMAND ----------

destDataDir = "/WorldDataBank/parquet/data_part"
df3.repartition('idIndicator').write.mode('overwrite').partitionBy('idIndicator').parquet(destDataDir)

# COMMAND ----------

destDataDir = "/WorldDataBank/parquet/data"
df3.repartition(1).write.mode('overwrite').parquet(destDataDir)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database WorldDataBank;

# COMMAND ----------

# MAGIC %sql 
# MAGIC use WorldDataBank;
# MAGIC 
# MAGIC DROP table if exists wdb_data;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS wdb_data(
# MAGIC idCountry STRING,
# MAGIC  idIndicator STRING,
# MAGIC  Ano INT,
# MAGIC  Valor DOUBLE)
# MAGIC USING parquet
# MAGIC --partitioned by (y,m)
# MAGIC LOCATION '/WorldDataBank/parquet/data';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM wdb_data limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT Ano,sum(Valor) FROM wdb_data where idIndicator = 'SP.POP.TOTL' GROUP BY ano ORDER  BY ano;

# COMMAND ----------

cls = ('Indicator Code','Indicator Name')
destDataDir = "/WorldDataBank/parquet/indicators"

dfIndicators = df \
  .select([c for c in df.columns if c in cls]) \
  .distinct() \
  .withColumnRenamed("Indicator Code", "idIndicator") \
  .withColumnRenamed("Indicator Name", "name") 

dfIndicators.repartition(1).write.mode('overwrite').parquet(destDataDir)
dfIndicators.printSchema()
  

# COMMAND ----------

# MAGIC %sql 
# MAGIC use WorldDataBank;
# MAGIC 
# MAGIC drop table if exists wdb_indicators;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS wdb_indicators(
# MAGIC  idIndicator STRING,
# MAGIC  name STRING)
# MAGIC USING parquet
# MAGIC --partitioned by (y,m)
# MAGIC LOCATION '/WorldDataBank/parquet/indicators';
# MAGIC 
# MAGIC SELECT * FROM wdb_indicators limit 10;

# COMMAND ----------

csvFileC = "/WorldDataBank/WDICountry.csv"
destDataDir = "/WorldDataBank/parquet/countries"

dfC = (spark.read           
  .option("header", "true") 
  .option("sep", ",") 
  .option("inferSchema", "true")
  .csv(csvFileC)
)

dfC = dfC \
  .withColumnRenamed("Country Code"   , "idCountry") \
  .withColumnRenamed("Short Name"     , "name") \
  .withColumnRenamed("Table Name"     , "name2") \
  .withColumnRenamed("2-alpha code"   , "alphaCode") \
  .withColumnRenamed("Currency Unit"  , "currency") \
  .withColumnRenamed("Region"         , "region") \
  .withColumnRenamed("Income Group"   , "income") \
  .withColumnRenamed("Lending category", "lendingCat") \
  .withColumnRenamed("Other groups"   , "otherGroups") 

dfC = dfC.select("idCountry","name","name2","alphaCode","currency","region","income","lendingCat","otherGroups")

dfC.repartition(1).write.mode('overwrite').parquet(destDataDir)

dfC.printSchema()

# COMMAND ----------

# MAGIC %sql 
# MAGIC use WorldDataBank;
# MAGIC 
# MAGIC drop table if exists wdb_countries;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS wdb_countries(
# MAGIC  idCountry STRING,
# MAGIC  name STRING,
# MAGIC  name2 STRING,
# MAGIC  alphaCode STRING,
# MAGIC  currency STRING,
# MAGIC  region STRING,
# MAGIC  income STRING,
# MAGIC  lendingCat STRING,
# MAGIC  otherGroups STRING
# MAGIC  )
# MAGIC USING parquet
# MAGIC LOCATION '/WorldDataBank/parquet/countries';
# MAGIC 
# MAGIC SELECT * FROM wdb_countries limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists wdb_country;
# MAGIC 
# MAGIC CREATE VIEW wdb_country
# MAGIC as
# MAGIC SELECT * from wdb_countries WHERE region is not  null 
# MAGIC UNION
# MAGIC SELECT 'ARG' , 'Argentina' , 'Argentina' , alphaCode , 'Argentine peso' , 'Latin America & Caribbean' , 'High income' , 'IBRD' , null 
# MAGIC from wdb_countries WHERE region is not  null and alphaCode = 'AR'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists wdb_region;
# MAGIC 
# MAGIC CREATE VIEW wdb_region
# MAGIC as
# MAGIC SELECT idCountry,name,name2,alphaCode from wdb_countries WHERE region is  null and idCountry != 'ARG'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT d.Ano,sum(d.Valor) FROM wdb_data d
# MAGIC                             LEFT JOIN wdb_country c on d.idCountry = c.idCountry
# MAGIC                           where d.idIndicator = 'SP.POP.TOTL' and c.idCountry is null
# MAGIC                           GROUP BY d.ano 
# MAGIC                           ORDER  BY d.ano;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT d.Ano,sum(d.Valor) FROM wdb_data d
# MAGIC                             JOIN wdb_region r on d.idCountry = r.idCountry
# MAGIC                           where d.idIndicator = 'SP.POP.TOTL' and r.idCountry = 'WLD'
# MAGIC                           GROUP BY d.ano 
# MAGIC                           ORDER  BY d.ano;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wdb_indicators 
# MAGIC where idIndicator in ('SI.DST.05TH.20','SI.DST.04TH.20','SI.DST.03RD.20','SI.DST.02ND.20','SI.DST.FRST.20')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.Ano,n5.n5,n4.n4 ,n3.n3,n2.n2,n1.n1 FROM 
# MAGIC (
# MAGIC   SELECT distinct  Ano  
# MAGIC     FROM wdb_data 
# MAGIC     where idIndicator in ('SI.DST.05TH.20','SI.DST.04TH.20','SI.DST.03RD.20','SI.DST.02ND.20','SI.DST.FRST.20')  and idCountry = 'PRT'
# MAGIC ) a
# MAGIC   left join (SELECT Ano,valor as n5 FROM wdb_data  where idIndicator = 'SI.DST.05TH.20' and idCountry = 'PRT') n5 ON a.ano = n5.ano
# MAGIC   left join (SELECT Ano,valor as n4 FROM wdb_data  where idIndicator = 'SI.DST.04TH.20' and idCountry = 'PRT') n4 ON a.ano = n4.ano
# MAGIC   left join (SELECT Ano,valor as n3 FROM wdb_data  where idIndicator = 'SI.DST.03RD.20' and idCountry = 'PRT') n3 ON a.ano = n3.ano
# MAGIC   left join (SELECT Ano,valor as n2 FROM wdb_data  where idIndicator = 'SI.DST.02ND.20' and idCountry = 'PRT') n2 ON a.ano = n2.ano
# MAGIC   left join (SELECT Ano,valor as n1 FROM wdb_data  where idIndicator = 'SI.DST.FRST.20' and idCountry = 'PRT') n1 ON a.ano = n1.ano
# MAGIC   order by ano

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.Ano,n5.n5,n4.n4 ,n3.n3,n2.n2,n1.n1 FROM 
# MAGIC (
# MAGIC   SELECT distinct  Ano  
# MAGIC     FROM wdb_data 
# MAGIC     where idIndicator in ('SI.DST.05TH.20','SI.DST.04TH.20','SI.DST.03RD.20','SI.DST.02ND.20','SI.DST.FRST.20')  and idCountry = 'USA'
# MAGIC ) a
# MAGIC   left join (SELECT Ano,valor as n5 FROM wdb_data  where idIndicator = 'SI.DST.05TH.20' and idCountry = 'USA') n5 ON a.ano = n5.ano
# MAGIC   left join (SELECT Ano,valor as n4 FROM wdb_data  where idIndicator = 'SI.DST.04TH.20' and idCountry = 'USA') n4 ON a.ano = n4.ano
# MAGIC   left join (SELECT Ano,valor as n3 FROM wdb_data  where idIndicator = 'SI.DST.03RD.20' and idCountry = 'USA') n3 ON a.ano = n3.ano
# MAGIC   left join (SELECT Ano,valor as n2 FROM wdb_data  where idIndicator = 'SI.DST.02ND.20' and idCountry = 'USA') n2 ON a.ano = n2.ano
# MAGIC   left join (SELECT Ano,valor as n1 FROM wdb_data  where idIndicator = 'SI.DST.FRST.20' and idCountry = 'USA') n1 ON a.ano = n1.ano
# MAGIC   order by ano

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.name,c.region, valor as n5 FROM wdb_data d LEFT JOIN wdb_country c on d.idCountry = c.idCountry                  
# MAGIC     where idIndicator = 'SI.DST.05TH.20' and Ano = 2015

# COMMAND ----------

# MAGIC %sql
# MAGIC --SI.DST.10TH.10
# MAGIC select d.ano,c.name,c.region,d.valor from wdb_data d
# MAGIC           join (select max(ano) ano,idCountry FROM wdb_data  where idIndicator = 'SI.DST.05TH.20' and valor >0 group by idCountry order by 1)x
# MAGIC             on d.ano = x.ano and  d.idCountry = x.idCountry
# MAGIC           left join wdb_country c
# MAGIC             on d.idCountry = c.idCountry
# MAGIC           where idIndicator = 'SI.DST.05TH.20' 
# MAGIC           order by valor

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select d.ano,c.name,c.region,d.valor from wdb_data d
# MAGIC           join (select max(ano) ano,idCountry FROM wdb_data  where idIndicator = 'SP.DYN.LE00.IN' and valor >0 group by idCountry order by 1)x
# MAGIC             on d.ano = x.ano and  d.idCountry = x.idCountry
# MAGIC           left join wdb_country c
# MAGIC             on d.idCountry = c.idCountry
# MAGIC           where idIndicator = 'SP.DYN.LE00.IN' 
# MAGIC           order by valor
# MAGIC           

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC drop view if exists  exercicio_lf_n5;
# MAGIC create temporary view exercicio_lf_n5
# MAGIC as
# MAGIC select life.ano anolf,life.name,life.region,lifeExpect,n5.ano anon5,n5.n5,n10.ano anon10,n10.n10 from
# MAGIC (
# MAGIC     select c.idCountry, d.ano,c.name,c.region,d.valor lifeExpect from wdb_data d
# MAGIC           join (select max(ano) ano,idCountry FROM wdb_data  where idIndicator = 'SP.DYN.LE00.IN' and valor >0 group by idCountry order by 1)x
# MAGIC             on d.ano = x.ano and  d.idCountry = x.idCountry
# MAGIC           left join wdb_country c
# MAGIC             on d.idCountry = c.idCountry
# MAGIC           where idIndicator = 'SP.DYN.LE00.IN' 
# MAGIC ) life
# MAGIC inner join
# MAGIC (
# MAGIC     select c.idCountry, d.ano,c.name,c.region,d.valor n5 from wdb_data d
# MAGIC           join (select max(ano) ano,idCountry FROM wdb_data  where idIndicator = 'SI.DST.05TH.20' and valor >0 group by idCountry order by 1)x
# MAGIC             on d.ano = x.ano and  d.idCountry = x.idCountry
# MAGIC           left join wdb_country c
# MAGIC             on d.idCountry = c.idCountry
# MAGIC           where idIndicator = 'SI.DST.05TH.20' 
# MAGIC ) n5
# MAGIC on life.idCountry = n5.idCountry
# MAGIC inner join
# MAGIC (
# MAGIC     select c.idCountry, d.ano,c.name,c.region,d.valor n10 from wdb_data d
# MAGIC           join (select max(ano) ano,idCountry FROM wdb_data  where idIndicator = 'SI.DST.10TH.10' and valor >0 group by idCountry order by 1)x
# MAGIC             on d.ano = x.ano and  d.idCountry = x.idCountry
# MAGIC           left join wdb_country c
# MAGIC             on d.idCountry = c.idCountry
# MAGIC           where idIndicator = 'SI.DST.10TH.10' 
# MAGIC ) n10
# MAGIC on life.idCountry = n10.idCountry
# MAGIC order by lifeExpect
# MAGIC     

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from exercicio_lf_n5
# MAGIC --drop view exercicio_lf_n5

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC library(ggplot2)
# MAGIC tbl<-sql("SELECT * FROM exercicio_lf_n5")
# MAGIC 
# MAGIC tbl_local <- collect(tbl)
# MAGIC #display(tbl_local)
# MAGIC ggplot(tbl_local, aes(lifeExpect, n10, color = region, group = 1)) + geom_point(alpha = 0.3) + stat_smooth()

# COMMAND ----------

# MAGIC %r
# MAGIC library(lattice)
# MAGIC xyplot(lifeExpect ~ n10 | region , tbl_local,  type = c("p", "g", "smooth"))
