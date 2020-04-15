// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC We will run various reports and visualize                          

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from  taxi_db.taxi_trips_mat_view 
// MAGIC -- 1.545.219.315   
// MAGIC -- 2.89m 2n
// MAGIC -- 29.24s 9n 1x
// MAGIC -- 14.22s 9n 2x
// MAGIC -- 19.233.634  18s community

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from  taxi_db.taxi_trips_mat_view limit 100

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select y.trip_year,g.g,y.y from
// MAGIC (select trip_year,count(*) g from  taxi_db.taxi_trips_mat_view where taxi_type = 'yellow'  group by trip_year) g
// MAGIC inner join 
// MAGIC (select trip_year,count(*) y from  taxi_db.taxi_trips_mat_view where taxi_type = 'green'  group by trip_year) y
// MAGIC on g.trip_year = y.trip_year
// MAGIC order by 1
// MAGIC -- 3.36m 2n a #1
// MAGIC -- 1.67m 2n a #2
// MAGIC --14.86s 9n a #1

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   taxi_type,trip_month,
// MAGIC   count(*) as trip_count
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view
// MAGIC group by taxi_type,trip_month

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 
// MAGIC   taxi_type,trip_month,
// MAGIC   count(*) as trip_count
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view
// MAGIC group by taxi_type,trip_month
// MAGIC 
// MAGIC -- 1.76 2n

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.  Revenue share by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   taxi_type,trip_month, sum(total_amount) revenue
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view
// MAGIC group by taxi_type,trip_month

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.  Trip count trend between 2013 and 2016

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   taxi_type,trip_month, sum(total_amount) revenue
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view
// MAGIC group by taxi_type,trip_month

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   taxi_type,
// MAGIC   trip_month,
// MAGIC   count(*) as trip_count
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view
// MAGIC where trip_year between 2014 and 2016
// MAGIC group by taxi_type, trip_month
// MAGIC order by trip_month asc

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5.  Trip count trend by month, by taxi type, for 2015

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 
// MAGIC   taxi_type,
// MAGIC   trip_month as month,
// MAGIC   count(*) as trip_count
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view
// MAGIC where 
// MAGIC   trip_year=2015
// MAGIC group by taxi_type,trip_month
// MAGIC order by trip_month 

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6.  Average trip distance by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 
// MAGIC   taxi_type,trip_month, round(avg(trip_distance),2) as trip_distance_miles
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view
// MAGIC group by taxi_type,trip_month

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.  Average trip amount by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 
// MAGIC   taxi_type,trip_month, round(avg(total_amount),2) as avg_total_amount
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view
// MAGIC group by taxi_type,trip_month

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8.  Trips with no tip, by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 
// MAGIC   taxi_type,trip_month,trip_month, count(*) tipless_count
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view
// MAGIC where tip_amount=0
// MAGIC group by taxi_type,trip_month

// COMMAND ----------

// MAGIC %md
// MAGIC ### 9.  Trips with no charge, by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 
// MAGIC   taxi_type,trip_month, count(*) as transactions
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view
// MAGIC where
// MAGIC   payment_type_description='No charge'
// MAGIC   and total_amount=0.0
// MAGIC group by taxi_type,trip_month

// COMMAND ----------

// MAGIC %md
// MAGIC ### 10.  Trips by payment type

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 
// MAGIC   payment_type_description as Payment_type, count(*) as transactions
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view
// MAGIC group by payment_type_description

// COMMAND ----------

// MAGIC %md
// MAGIC ### 11.  Trip trend by pickup hour 

// COMMAND ----------

// MAGIC %sql
// MAGIC select hour(pickup_datetime),count(*) 
// MAGIC from taxi_db.taxi_trips_mat_view
// MAGIC where trip_year=2015
// MAGIC group by hour(pickup_datetime)
// MAGIC order by hour(pickup_datetime)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 12.  Top 3 yellow taxi pickup-dropoff zones for 2016

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from 
// MAGIC   (
// MAGIC   select 
// MAGIC     pickup_zone,dropoff_zone,count(*) as trip_count
// MAGIC   from 
// MAGIC     taxi_db.taxi_trips_mat_view
// MAGIC   group by pickup_zone,dropoff_zone
// MAGIC   order by trip_count desc
// MAGIC   ) x
// MAGIC limit 3
