# Databricks notebook source
dbutils.fs.ls("abfss://raw@amazonproductsdl.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS amazon_products_db
# MAGIC LOCATION 'abfss://raw@amazonproductsdl.dfs.core.windows.net/';