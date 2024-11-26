# Databricks notebook source
import json
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/etl_and_ml_project/utils/Common Variables"

# COMMAND ----------

model = PipelineModel.load(f"{model_folder_path}/ml_pipeline")
model

# COMMAND ----------

def make_prediction(model, raw_data):
    data = json.loads(raw_data)
    schema = StructType([
        StructField('is_best_seller', IntegerType(), True),
        StructField('is_prime', IntegerType(), True),
        StructField('offers_count', IntegerType(), True),
        StructField('num_ratings', DoubleType(), True),
        StructField('original_price', DoubleType(), True),
        StructField('price', DoubleType(), True),
        StructField('star_rating', DoubleType(), True),
        StructField('sales_volume', IntegerType(), True),
        StructField('discount', DoubleType(), True),
        StructField('profit', DoubleType(), True),
        StructField('discount_percentage', DoubleType(), True),
        StructField('profit_percentage', DoubleType(), True),
        StructField('product_title_Hanes', IntegerType(), True),
        StructField('product_title_adidas', IntegerType(), True),
        StructField('product_title_Amazon_Essentials', IntegerType(), True),
        StructField('product_title_Dokotoo', IntegerType(), True),
        StructField('product_title_Carhartt', IntegerType(), True),
        StructField('product_title_SOJOS', IntegerType(), True),
        StructField('product_title_AUTOMET', IntegerType(), True),
        StructField('product_title_Fruit_of_the_Loom', IntegerType(), True),
        StructField('product_title_Skechers', IntegerType(), True),
        StructField('product_title_Trendy_Queen', IntegerType(), True),
        StructField('product_title_Wrangler_Authentics', IntegerType(), True),
        StructField("product_title_Levi's", IntegerType(), True),
        StructField('product_title_PRETTYGARDEN', IntegerType(), True),
        StructField('product_title_Gildan', IntegerType(), True),
        StructField('product_title_Lee', IntegerType(), True),
        StructField('product_title_others', IntegerType(), True)
    ])
    input_df = spark.createDataFrame(data, schema=schema)
    predictions = model.transform(input_df)
    return predictions.select("prediction").toPandas().to_json(orient="records")

# COMMAND ----------

# raw_data = {
#     "is_best_seller": 1,
#     "is_prime": 1,
#     "offers_count": 1,
#     "num_ratings": 4.5,
#     "original_price": 100.0,
#     "price": 50.0,
#     "star_rating": 4.5,
#     "sales_volume": 1000,
#     "discount": 50.0,
#     "profit": 30.0,
#     "discount_percentage": 50.0,
#     "profit_percentage": 50.0,
#     "product_title_Hanes": 1,
#     "product_title_adidas": 0,
#     "product_title_Amazon_Essentials": 0,
#     "product_title_Dokotoo": 0,
#     "product_title_Carhartt": 0,
#     "product_title_SOJOS": 0,
#     "product_title_AUTOMET": 0,
#     "product_title_Fruit_of_the_Loom": 0,
#     "product_title_Skechers": 0,
#     "product_title_Trendy_Queen": 0,
#     "product_title_Wrangler_Authentics": 0,
#     "product_title_Levi's": 0,
#     "product_title_PRETTYGARDEN": 0,
#     "product_title_Gildan": 0,
#     "product_title_Lee": 0,
#     "product_title_others": 1   
# }

# raw_data = json.dumps([raw_data])
# make_prediction(model,raw_data)