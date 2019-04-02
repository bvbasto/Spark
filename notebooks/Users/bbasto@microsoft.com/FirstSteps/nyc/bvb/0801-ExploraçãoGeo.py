# Databricks notebook source
# DBTITLE 1,View Consolidada
# MAGIC %sql
# MAGIC CREATE TEMPORARY TABLE taxis
# MAGIC USING parquet
# MAGIC OPTIONS (path "/mnt/MyADLS/nyctaxi/curatedDir/materialized-view/")

# COMMAND ----------

# DBTITLE 1,Viagens com coordenadas em 2015 (sem outliers) - Pickups
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pickups AS
# MAGIC SELECT ROUND(pickup_latitude, 4) AS lat,
# MAGIC ROUND(pickup_longitude, 4) AS long,
# MAGIC COUNT(*) AS num_trips,
# MAGIC SUM(fare_amount) AS total_revenue
# MAGIC FROM taxis
# MAGIC WHERE trip_year="2015" AND fare_amount/trip_distance BETWEEN 2 AND 10
# MAGIC GROUP BY lat, long

# COMMAND ----------

# DBTITLE 1,Viagens com coordenadas em 2015 (sem outliers) - Dropoffs
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dropoffs AS
# MAGIC SELECT ROUND(dropoff_latitude, 4) AS lat,
# MAGIC ROUND(dropoff_longitude, 4) AS long,
# MAGIC COUNT(*) AS num_trips,
# MAGIC SUM(fare_amount) AS total_revenue
# MAGIC FROM taxis
# MAGIC WHERE trip_year="2015" AND fare_amount/trip_distance BETWEEN 2 AND 10
# MAGIC GROUP BY lat, long

# COMMAND ----------

# DBTITLE 1,Materializar a view numa DataFrame - Pickups
from ggplot import *
from pyspark.sql import *
from pyspark.sql.functions import col, asc

pickups = spark.table('pickups').filter(col('num_trips') > 10).toPandas()
pickups.shape

# COMMAND ----------

# DBTITLE 1,Materializar a view numa DataFrame - Dropoffs
from ggplot import *
from pyspark.sql import *
from pyspark.sql.functions import col, asc

dropoffs = spark.table('dropoffs').filter(col('num_trips') > 10).toPandas()
dropoffs.shape

# COMMAND ----------

# DBTITLE 1,Inícios de Viagem
import matplotlib.pyplot as plt
fig, ax = plt.subplots(figsize=(30,24), dpi=300)

# Crop
ax.set_xlim(-74.15, -73.70)
ax.set_ylim(40.5774, 40.9176)

# remove axis
plt.axis('off')
# remove margin
fig.tight_layout()

ax.scatter(pickups['long'], pickups['lat'], marker=',', s=0.5, c='black', edgecolor='none', alpha=0.75)

display(fig)

# COMMAND ----------

# DBTITLE 1,Fins de Viagem
import matplotlib.pyplot as plt
fig, ax = plt.subplots(figsize=(30,24), dpi=300)

# Crop
ax.set_xlim(-74.15, -73.70)
ax.set_ylim(40.5774, 40.9176)

# remove axis
plt.axis('off')
# remove margin
fig.tight_layout()

ax.scatter(dropoffs['long'], dropoffs['lat'], marker=',', s=0.5, c='black', edgecolor='none', alpha=0.75)

display(fig)

# COMMAND ----------

# DBTITLE 1,Número e Custo das Viagens - Pickups
import matplotlib.pyplot as plt
import pandas as pd
from sklearn import preprocessing

# Human-friendly scaling
scaled = preprocessing.minmax_scale(pickups['total_revenue'], feature_range=(0,100))

# Bin the scaled data and assign color values
bins = [0, 1, 5, 10, 15, 25, 50, 100]
labels = [0, 8, 12, 14, 16, 18, 20]
scaled = pd.cut(scaled, bins=bins, labels=labels).astype('double')

fig, ax = plt.subplots(figsize=(30,24), dpi=200)

# Original boundaries
ax.set_xlim(-74.15, -73.70)
ax.set_ylim(40.5774, 40.9176)

# remove axis
plt.axis('off')
# remove margin
fig.tight_layout()

ax.scatter(pickups['long'], pickups['lat'], marker='.', s=pickups['num_trips'], c=scaled, cmap='hot', edgecolor='white', alpha=0.2)
display(fig)

# COMMAND ----------

# DBTITLE 1,Dropoffs
import matplotlib.pyplot as plt
import pandas as pd
from sklearn import preprocessing

# Human-friendly scaling
scaled = preprocessing.minmax_scale(dropoffs['total_revenue'], feature_range=(0,100))

# Bin the scaled data and assign color values
bins = [0, 1, 5, 10, 15, 25, 50, 100]
labels = [0, 8, 12, 14, 16, 18, 20]
scaled = pd.cut(scaled, bins=bins, labels=labels).astype('double')

fig, ax = plt.subplots(figsize=(30,24), dpi=200)

# Original boundaries
ax.set_xlim(-74.15, -73.70)
ax.set_ylim(40.5774, 40.9176)

# remove axis
plt.axis('off')
# remove margin
fig.tight_layout()

ax.scatter(dropoffs['long'], dropoffs['lat'], marker='.', s=dropoffs['num_trips'], c=scaled, cmap='hot', edgecolor='white', alpha=0.2)
display(fig)

# COMMAND ----------

