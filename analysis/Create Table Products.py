# Databricks notebook source
# MAGIC %run "/Workspace/Users/n01606417@humber.ca/etl_and_ml_project/utils/Common Variables"

# COMMAND ----------

products_df = spark.read.format("delta").load(f"{processed_folder_path}/products")
display(products_df)

# COMMAND ----------

products_df.count()

# COMMAND ----------

data = spark.read.format("delta").load(f"{processed_folder_path}/products")
data.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS amazon_products_db.products (
# MAGIC   is_best_seller INT,
# MAGIC   is_prime INT,
# MAGIC   offers_count INT,
# MAGIC   num_ratings DOUBLE,
# MAGIC   original_price DOUBLE,
# MAGIC   price DOUBLE,
# MAGIC   star_rating DOUBLE,
# MAGIC   SALES_VOLUME DOUBLE,
# MAGIC   DISCOUNT FLOAT,
# MAGIC   PROFIT FLOAT,
# MAGIC   DISCOUNT_PERCENTAGE FLOAT,
# MAGIC   PROFIT_PERCENTAGE FLOAT,
# MAGIC   product_title_Hanes INT,
# MAGIC   product_title_adidas INT,
# MAGIC   product_title_Amazon_Essentials INT,
# MAGIC   product_title_Dokotoo INT,
# MAGIC   product_title_Carhartt INT,
# MAGIC   product_title_SOJOS INT,
# MAGIC   product_title_AUTOMET INT,
# MAGIC   product_title_Fruit_of_the_Loom INT,
# MAGIC   product_title_Skechers INT,
# MAGIC   product_title_Trendy_Queen INT,
# MAGIC   product_title_Wrangler_Authentics INT,
# MAGIC   product_title_Levi_s INT,
# MAGIC   product_title_PRETTYGARDEN INT,
# MAGIC   product_title_Gildan INT,
# MAGIC   product_title_Lee INT,
# MAGIC   product_title_others INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

data.write.mode("append").insertInto("amazon_products_db.products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM amazon_products_db.products;