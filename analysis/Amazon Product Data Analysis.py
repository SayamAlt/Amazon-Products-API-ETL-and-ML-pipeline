# Databricks notebook source
# MAGIC %run "/Workspace/Users/n01606417@humber.ca/etl_and_ml_project/utils/Common Variables"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM amazon_products_db.products;

# COMMAND ----------

df = spark.read.option("header","true").format("delta").load(f"{transformed_folder_path}/products")
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------


df.createOrReplaceTempView('products')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM products;