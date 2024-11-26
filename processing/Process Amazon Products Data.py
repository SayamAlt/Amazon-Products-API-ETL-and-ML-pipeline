# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, when, udf, round, desc, mean
from pyspark.sql.window import Window
import re
from pyspark.sql.types import StringType, IntegerType

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/etl_and_ml_project/utils/Common Variables"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/etl_and_ml_project/utils/Common Functions"

# COMMAND ----------

products_df = spark.read.csv(f"{raw_folder_path}/products.csv", header=True, inferSchema=True)
display(products_df)

# COMMAND ----------

products_df.count()

# COMMAND ----------

products_df.printSchema()

# COMMAND ----------

products_df = products_df.withColumn("discount", when(col("product_original_price") - col("product_price")>0, col("product_original_price") - col("product_price")).otherwise(0)) \
                        .withColumn("profit", when(col("product_price") - col("product_original_price")>0, col("product_price") - col("product_original_price")).otherwise(0)) \
                        .withColumn("discount_percentage",when(col("product_original_price") - col("product_price")>0, (col("product_original_price")-col("product_price"))/col("product_original_price")*100).otherwise(0)) \
                        .withColumn("profit_percentage",when(col("product_price") - col("product_original_price")>0, (col("product_price")-col("product_original_price"))/col("product_price")*100).otherwise(0))
display(products_df)

# COMMAND ----------

def get_digit(text):
    if text is None:
        return None
    elif 'K' in text:
        return int(''.join(re.findall(r'\d',text)))*1000
    else:
        return int(''.join(re.findall(r'\d',text)))

# COMMAND ----------

get_digit_udf = udf(get_digit, IntegerType())
get_digit_udf

# COMMAND ----------

products_df = products_df.withColumn('sales_volume',get_digit_udf(col('sales_volume')))
display(products_df)

# COMMAND ----------

products_df = products_df.withColumn('profit',round(col('profit'),2).cast('float')) \
                         .withColumn('discount',round(col('discount'),2).cast('float')) \
                         .withColumn('profit_percentage',round(col('profit_percentage'),2).cast('float')) \
                         .withColumn('discount_percentage',round(col('discount_percentage'),2).cast('float'))
products_df.printSchema()

# COMMAND ----------

products_df.withColumn('climate_pledge',col('climate_pledge').cast('int')).select('climate_pledge').distinct().collect()

# COMMAND ----------

products_df.withColumn('is_amazon_choice',col('is_amazon_choice').cast('int')).select('is_amazon_choice').distinct().collect()

# COMMAND ----------

products_df = products_df.drop('climate_pledge','is_amazon_choice')
products_df.printSchema()

# COMMAND ----------

products_df.withColumn('is_best_seller',col('is_best_seller').cast('int')).select('is_best_seller').distinct().collect()

# COMMAND ----------

products_df.withColumn('is_prime',col('is_prime').cast('int')).select('is_prime').distinct().collect()

# COMMAND ----------

products_df = products_df.withColumn('is_best_seller',col('is_best_seller').cast('int')) \
                         .withColumn('is_prime',col('is_prime').cast('int')) \
                         .withColumn('product_num_ratings',col('product_num_ratings').cast('int')) \
                         .withColumn('product_original_price',col('product_original_price').cast('double'))
products_df.printSchema()

# COMMAND ----------

products_df = products_df.drop('product_photo','product_delivery_info','product_stock_message','product_url')
products_df.printSchema()

# COMMAND ----------

products_df.select('asin').distinct().count() / products_df.count()

# COMMAND ----------

products_df = products_df.drop('asin')
products_df.printSchema()

# COMMAND ----------

products_df = products_df.withColumn('product_star_rating',col('product_star_rating').cast('float'))

# COMMAND ----------

display(products_df)

# COMMAND ----------

products_df.select('product_title').distinct().count()

# COMMAND ----------

products_df.groupBy('product_title').count().sort('count',ascending=False).show()

# COMMAND ----------

products_df.write.format("delta").mode("overwrite").save(f"{transformed_folder_path}/products")

# COMMAND ----------

# Get the top 15 most frequent product titles
top_15_titles = products_df.groupBy('product_title').count().orderBy(desc('count')).limit(15).select('product_title').rdd.flatMap(lambda x: x).collect()

products_df = products_df.withColumn('product_title', when(col('product_title').isin(top_15_titles), col('product_title')).otherwise('others'))

# One hot encode the 'product_title' column
categories = top_15_titles + ['others']
for category in categories:
    products_df = products_df.withColumn(f'product_title_{category}', when(col('product_title') == category, 1).otherwise(0))

display(products_df)

# COMMAND ----------

products_df.groupBy('product_title').count().sort(desc('count')).show()

# COMMAND ----------

products_df = products_df.drop('product_title')
products_df.printSchema()

# COMMAND ----------

display(products_df)

# COMMAND ----------

products_df = fill_missing_with_mean(products_df,'product_num_ratings')

# COMMAND ----------

# mean_products_rating = products_df.select(round(mean('product_num_ratings')).alias('mean_ratings')).collect()[0].mean_ratings
# mean_products_rating

# COMMAND ----------

# products_df = products_df.withColumn(
#     'product_num_ratings',
#     when(col('product_num_ratings').isNull(), mean_products_rating).otherwise(col('product_num_ratings'))
# )
# display(products_df.select('product_num_ratings'))

# COMMAND ----------

products_df.select(col('product_num_ratings')).describe().show()

# COMMAND ----------

products_df = fill_missing_with_median(products_df,'product_original_price')

# COMMAND ----------

products_df = fill_missing_with_mean(products_df,'sales_volume')

# COMMAND ----------

display(products_df)

# COMMAND ----------

null_columns = [col for col in products_df.columns if products_df.filter(products_df[col].isNull()).count() > 0]
null_columns

# COMMAND ----------

products_df.select('product_price').describe().show()

# COMMAND ----------

products_df.select('product_star_rating').describe().show()

# COMMAND ----------

products_df = fill_missing_with_mean(products_df,'product_star_rating')

# COMMAND ----------

products_df = fill_missing_with_median(products_df,'product_price')

# COMMAND ----------

null_columns = [col for col in products_df.columns if products_df.filter(products_df[col].isNull()).count() > 0]
null_columns

# COMMAND ----------

display(products_df)

# COMMAND ----------

products_df = products_df.withColumnRenamed('product_num_ratings','num_ratings') \
                         .withColumnRenamed('product_original_price','original_price') \
                         .withColumnRenamed('product_price','price') \
                         .withColumnRenamed('product_star_rating','star_rating') 
products_df.printSchema()    

# COMMAND ----------

products_df = products_df.withColumnRenamed('product_title_Fruit of the Loom','product_title_Fruit_of_the_Loom') \
                         .withColumnRenamed('product_title_Trendy Queen','product_title_Trendy_Queen') \
                         .withColumnRenamed('product_title_Wrangler Authentics','product_title_Wrangler_Authentics') \
                         .withColumnRenamed('product_title_Amazon Essentials','product_title_Amazon_Essentials')
products_df.printSchema()

# COMMAND ----------

products_df.write.format("delta").mode("overwrite").save(f"{processed_folder_path}/products")