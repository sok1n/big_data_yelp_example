# Databricks notebook source
# MAGIC %md
# MAGIC # Example 2 - Assessment-aligned guided practice in Databricks
# MAGIC
# MAGIC This notebook is designed to mirror the workflow students will need in the final portfolio assessment, while still using a different dataset combination from the assessed task.
# MAGIC
# MAGIC **Classroom dataset combination:**
# MAGIC - `business.json`
# MAGIC - `review.json`
# MAGIC - `user.json`
# MAGIC
# MAGIC **Important modelling point**
# MAGIC - `business.json` and `user.json` should **not** be joined directly.
# MAGIC - `review.json` is the bridge table because it contains both `business_id` and `user_id`.
# MAGIC

# COMMAND ----------

# Raw data stored in a volume 
# load data

catalog_name = "workspace"  # choose the catalog and the schema 
schema_name = "default"

business_path = "/Volumes/workspace/default/yelpdataset/yelp_academic_dataset_business.json"  
review_path = "/Volumes/workspace/default/yelpdataset/yelp_academic_dataset_review.json"
user_path = "/Volumes/workspace/default/yelpdataset/yelp_academic_dataset_user.json"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")




# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Tip: Before defining the catalog name and the schema name, copy and paste the path, then check the path so that /Volumes/**catalog_name**/**schema_name**/data.....

# COMMAND ----------

# Bronze layer

business_raw = spark.read.json(business_path) # read json into Spark 
review_raw = spark.read.json(review_path)

#load bronze data into Delta tables
business_raw.write.mode("overwrite").format("delta").saveAsTable("bronze_business")
review_raw.write.mode("overwrite").format("delta").saveAsTable("bronze_review")



# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks is the workspace, Apache Spark is the engine 
# MAGIC 1. in the bronze level and after inserting json raw data into Delta tables, do I still have raw data?
# MAGIC > no, as the data are stored in Delta tables
# MAGIC 2. What Delta tables are?
# MAGIC > Delta table is a table stored using the Delta lake format, which adds structure on top of data files. 
# MAGIC
# MAGIC

# COMMAND ----------

# Silver layer

from pyspark.sql import functions as F 

silver_business = business_raw.select("business_id", "name", "state", "stars", "review_count", "categories").withColumnRenamed("stars", "business_stars")


silver_review = review_raw.select("business_id", "stars", "date").withColumn("new_date", F.year("date")).withColumnRenamed("stars", "review_stars")
silver_business.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_business")
silver_review.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_review")

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774688714519.png](./image_1774688714519.png "image_1774688714519.png")
# MAGIC trying to use from review_raw the date attribute, we received the error above, that we were not able to write the data in the silver layer.
# MAGIC checking the schema, data is string so we should convert to numerical 

# COMMAND ----------

# Gold layer 

gold_business_review = silver_review.join(silver_business, on="business_id", how="inner") # inner join so that the joined table will include only the common rows

gold_summary = gold_business_review.groupBy("state").agg(
  F.countDistinct("business_id").alias("total_businesses"),
  F.countDistinct("review_count").alias("total_reviews"),
  F.round(F.avg("business_stars"),2).alias("avg_business_stars"),
  F.round(F.avg("review_stars"),2).alias("avg_review_stars")
)

gold_summary.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("gold_summary")

# COMMAND ----------

# print the schema
gold_summary.printSchema()

# COMMAND ----------

gold_summary.show(5)

# COMMAND ----------

silver_business.printSchema()
silver_review.printSchema()
gold_business_review.printSchema()

# COMMAND ----------

silver_business.show(5)
business_raw.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TEMP VIEW gold_summary AS
# MAGIC SELECT *
# MAGIC FROM gold_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     state,
# MAGIC     total_businesses,
# MAGIC     total_reviews,
# MAGIC     avg_business_stars,
# MAGIC     avg_review_stars
# MAGIC FROM gold_summary
# MAGIC ORDER BY total_reviews DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
