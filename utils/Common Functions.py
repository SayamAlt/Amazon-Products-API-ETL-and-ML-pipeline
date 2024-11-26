# Databricks notebook source
from pyspark.sql.functions import col, when, mean, median
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.pipeline import Pipeline

# COMMAND ----------

def fill_missing_with_mean(data,col_name):
    mean_val = data.select(round(mean(col_name)).alias('mean_val')).collect()[0].mean_val
    data = data.withColumn(col_name,when(col(col_name).isNull(), mean_val).otherwise(col(col_name)))
    return data

# COMMAND ----------

def fill_missing_with_median(data,col_name):
    mean_val = data.select(round(median(col_name)).alias('median_val')).collect()[0].median_val
    data = data.withColumn(col_name,when(col(col_name).isNull(), mean_val).otherwise(col(col_name)))
    return data

# COMMAND ----------

def create_end_to_end_pipeline(model,labelCol):
    assembler = VectorAssembler(inputCols=['is_best_seller',
    'is_prime',
    'offers_count',
    'num_ratings',
    'original_price',
    'price',
    'sales_volume',
    'discount',
    'profit',
    'discount_percentage',
    'profit_percentage',
    'product_title_Hanes',
    'product_title_adidas',
    'product_title_Amazon_Essentials',
    'product_title_Dokotoo',
    'product_title_Carhartt',
    'product_title_SOJOS',
    'product_title_AUTOMET',
    'product_title_Fruit_of_the_Loom',
    'product_title_Skechers',
    'product_title_Trendy_Queen',
    'product_title_Wrangler_Authentics',
    "product_title_Levi's",
    'product_title_PRETTYGARDEN',
    'product_title_Gildan',
    'product_title_Lee',
    'product_title_others'],outputCol='features',handleInvalid='skip')
    scaler = StandardScaler(inputCol='features',outputCol='scaledFeatures')
    regressor = model.setFeaturesCol('scaledFeatures').setLabelCol(labelCol)

    pipeline = Pipeline(stages=[
        assembler,scaler,regressor
    ])
    return pipeline