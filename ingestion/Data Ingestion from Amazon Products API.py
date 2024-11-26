# Databricks notebook source
import requests, json, re
import numpy as np
import pandas as pd
from pyspark.sql.functions import current_timestamp, col, udf
from pyspark.sql.types import IntegerType, StringType

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/etl_and_ml_project/utils/Common Variables"

# COMMAND ----------

dbutils.secrets().listScopes()

# COMMAND ----------

rapid_api_key = str(dbutils.secrets.get(scope='amazon-products-secret-scope',key='amazon-products-rapidapi-key'))
rapid_api_key

# COMMAND ----------

def get_data(query):
  url = "https://amazon-online-data-api.p.rapidapi.com/search"

  querystring = {"query": query,
                 "page":"1",
                 "geo":"US"}
  headers = {
    "x-rapidapi-key": rapid_api_key,
    "x-rapidapi-host": "amazon-online-data-api.p.rapidapi.com"
  }

  response = requests.get(url, headers=headers, params=querystring)
  return json.loads(response.text)

# COMMAND ----------

categories = ['phone', 'laptop', 'tablet', 'camera', 'headphones', 'tv', 'airpods', 'charger', 'earbuds', 'speaker', 'keyboard', 'mouse', 'monitor', 'mousepad', 'pendrive', 'shared_drive', 'memory_card', 'powerbank', 'charging_case', 'charging_stand', 'charging_cable', 'charging_kit', 'charging_stand', 'charging_cable', 'shampoo', 'conditioner', 'hair_gel', 'hair_spray', 'hair_conditioner', 'hair_oil', 'hair_serum', 'hair_mask', 'hair_treatment', 'hair_color', 'hair_dryer', 'iphone_case', 'iphone_cover', 'iphone_charger', 'iphone_cable', 'iphone_earbuds', 'iphone_headphones', 'iphone_speaker', 'iphone_screen_protector', 'iphone_earpiece', 'iphone_earpiece_case', 'iphone_earpiece_stand', 'bottle', 'water', 'juice', 'shampoo', 'conditioner', 'hair_gel', 'hair_spray', 'hair_conditioner', 'hair_oil', 'basket', 'baby_bottle', 'baby_water', 'baby_shampoo', 'baby_conditioner', 'baby_hair_gel', 'baby_hair_spray', 'baby_hair_conditioner', 'baby_hair_oil', 'baby_hair_serum', 'baby_hair_mask', 'baby_hair_treatment', 'baby_hair_color', 'wardrobe', 'wardrobe_shoes', 'wardrobe_clothing', 'wardrobe_jewellery', 'wardrobe_bags', 'wardrobe_hats']
categories = list(set(categories))
len(categories)

# COMMAND ----------

data = get_data('phone')

# COMMAND ----------

data.keys()

# COMMAND ----------

pd.json_normalize(data['products']).head()

# COMMAND ----------

products_df = pd.DataFrame()

for category in categories:
  data = get_data(category)
  df = pd.json_normalize(data['products'])
  products_df = pd.concat([products_df,df],axis=0)
  if products_df.shape[0] == 0:
    break
  print(f"Category {category} done.")

# COMMAND ----------

for category in list(set(['apparel','electronics','furniture','sports','hoodies','jackets','jeans','shoes','sneakers','watches','accessories','shirts','t-shirts','tops','trousers','skirts','blazers','pants','shorts','dresses','bags','hats','sunglasses','scarves','socks','underwear','swimwear','sweaters','leggings','sweaters'])):
  if category not in categories:
    data = get_data(category)
    df = pd.json_normalize(data['products'])
    products_df = pd.concat([products_df,df],axis=0)
    if df.shape[0] == 0:
      break
    print(f"Category {category} done.")

# COMMAND ----------

products_df.shape

# COMMAND ----------

products_df.head()

# COMMAND ----------

products_df = spark.createDataFrame(products_df)
products_df.printSchema()

# COMMAND ----------

products_df.count()

# COMMAND ----------

products_df.describe().show()

# COMMAND ----------

display(products_df)

# COMMAND ----------

products_df.write.format("csv").option("header", "true").mode("overwrite").save(f'{raw_folder_path}/products.csv')

# COMMAND ----------

products_df = spark.read.csv(f"{raw_folder_path}/products.csv",header=True,inferSchema=True)
display(products_df)