# Databricks notebook source
# MAGIC %md # Dashboards
# MAGIC 
# MAGIC Dashboards allow you to publish graphs and visualizations and share them in a presentation format with your organization. This notebook shows how to create, edit, and delete dashboards.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Dashboards are composed of elements. These elements are created by output from notebook cells. Let's create some elements (and cells) for the dashboard we're going to be building. The first cell creates a dashboard title using the ``displayHTML()`` function.

# COMMAND ----------

displayHTML("""<font size="6" color="red" face="sans-serif">Bike Sharing Data Analysis Dashboard</font>""")

# COMMAND ----------

# MAGIC %md You can also create a title or label using Markdown.
# MAGIC 
# MAGIC ```
# MAGIC %md ## Dashboard label
# MAGIC ```
# MAGIC 
# MAGIC which renders as:
# MAGIC 
# MAGIC ## Dashboard label

# COMMAND ----------

# MAGIC %md Create a dashboard that displays a bikesharing dataset available as a Databricks hosted dataset.

# COMMAND ----------

# For this example, we use the bike sharing dataset available on `dbfs`.
df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv")

df.registerTempTable("bikeshare")

# COMMAND ----------

# MAGIC %md 
# MAGIC Create a graph of bike conditions across each of the seasons. Also create a cell with this label to correctly label the graph.

# COMMAND ----------

# MAGIC %md **Biking Conditions Across the Seasons**

# COMMAND ----------

display(spark.sql("SELECT season, MAX(temp) as temperature, MAX(hum) as humidity, MAX(windspeed) as windspeed FROM bikeshare GROUP BY season ORDER BY SEASON"))

# COMMAND ----------

# MAGIC %md ## Create a dashboard
# MAGIC 
# MAGIC Now that we have some elements to display, create a dashboard from them.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Navigate to the **View** menu and select **+ New Dashboard**.
# MAGIC 
# MAGIC ![dashboard demo](https://docs.databricks.com/_static/images/dashboards/dashboard-demo-0.png)
# MAGIC 
# MAGIC Give your dashboard a name.

# COMMAND ----------

# MAGIC %md 
# MAGIC By default the new dashboard includes all cells that you've created thus far. You can rearrange and reshape each cell as you see fit. Navigate back to the notebook code by selecting the **Code** option from the **View** menu.
# MAGIC 
# MAGIC ![something](https://docs.databricks.com/_static/images/dashboards/dashboard-demo-2.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC New cells do not automatically appear on your dashboard. You add them manually by clicking the dashboard icon at the far right and selecting the checkbox next to the dashboard name.
# MAGIC 
# MAGIC ![something](https://docs.databricks.com/_static/images/dashboards/dashboard-demo-3.png)
# MAGIC 
# MAGIC You can add and remove cells in the same manner.

# COMMAND ----------

# MAGIC %md ## Add graphs to a dashboard
# MAGIC 
# MAGIC Create more graphs to add to the dashboard. First, add a graph of biking conditions across all months in our dataset. Also create a cell with a label. Add the cells to the dashboard.

# COMMAND ----------

# MAGIC %md **Average Biking Conditions**

# COMMAND ----------

display(spark.sql("SELECT mnth as month, AVG(temp) as temperature, AVG(hum) as humidity, AVG(windspeed) as windspeed FROM bikeshare GROUP BY month ORDER BY month"))

# COMMAND ----------

# MAGIC %md While the average is nice to see, it's likely we're more worried about the extreme conditions. Create a graph of the extreme bike conditions across the months. Also create a cell with a label. Add the cells to the dashboard.

# COMMAND ----------

# MAGIC %md 
# MAGIC **Extreme Biking Conditions**

# COMMAND ----------

# MAGIC %sql SELECT mnth as month, MAX(temp) as max_temperature, MAX(hum) as max_humidity, MAX(windspeed) as max_windspeed FROM bikeshare GROUP BY mnth ORDER BY mnth

# COMMAND ----------

# MAGIC %md Now that you've created a dashboard you can organize it. Navigate to the top of new dashboard. Select the bottom left or right corners to resize and reshape each tile in the dashboard. Markdown cells become labels for each section of the dashboard.
# MAGIC 
# MAGIC ![something](https://docs.databricks.com/_static/images/dashboards/dashboard-demo-4.png)

# COMMAND ----------

# MAGIC %md ## Present a dashboard
# MAGIC 
# MAGIC You can present a dashboard by selecting the **Present Dashboard** button on the right.
# MAGIC 
# MAGIC ![something](https://docs.databricks.com/_static/images/dashboards/dashboard-demo-5.png)

# COMMAND ----------

# MAGIC %md ## Edit a dashboard
# MAGIC 
# MAGIC Edit a dashboard from within the Dashboard view. Open the dashboard and edit it as instructed above.

# COMMAND ----------

# MAGIC %md ## Delete a dashboard
# MAGIC 
# MAGIC Delete a dashboard from within the Dashboard view.  Open the dashboard and press the **Delete this dashboard** button on the right.