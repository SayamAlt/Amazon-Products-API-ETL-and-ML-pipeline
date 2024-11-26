# Databricks notebook source
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor, FMRegressor, GeneralizedLinearRegression, IsotonicRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.pipeline import Pipeline, PipelineModel
import joblib
import numpy as np

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/etl_and_ml_project/utils/Common Variables"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/etl_and_ml_project/utils/Common Functions"

# COMMAND ----------

df = spark.read.format("delta").load(f"{processed_folder_path}/products")
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy(df.columns).count().filter("count > 1").count()

# COMMAND ----------

df = df.dropDuplicates()
df.count()

# COMMAND ----------

train_df, test_df = df.randomSplit([0.7,0.3],seed=84)

# COMMAND ----------

train_df.count(), test_df.count()

# COMMAND ----------

train_df.columns

# COMMAND ----------

lr_pipeline = create_end_to_end_pipeline(model=LinearRegression(), labelCol='price')
lr_pipeline

# COMMAND ----------

dt_pipeline = create_end_to_end_pipeline(DecisionTreeRegressor(),'price')
dt_pipeline

# COMMAND ----------

rf_pipeline = create_end_to_end_pipeline(RandomForestRegressor(),'price')
rf_pipeline

# COMMAND ----------

fm_pipeline = create_end_to_end_pipeline(FMRegressor(),'price')
fm_pipeline

# COMMAND ----------

gr_pipeline = create_end_to_end_pipeline(GeneralizedLinearRegression(family='gaussian'),'price')
gr_pipeline

# COMMAND ----------

pr_pipeline = create_end_to_end_pipeline(GeneralizedLinearRegression(family='poisson'),'price')
pr_pipeline

# COMMAND ----------

gamma_pipeline = create_end_to_end_pipeline(GeneralizedLinearRegression(family='gamma'),'price')
gamma_pipeline

# COMMAND ----------

tweedie_pipeline = create_end_to_end_pipeline(GeneralizedLinearRegression(family='tweedie'),'price')
tweedie_pipeline

# COMMAND ----------

ir_pipeline = create_end_to_end_pipeline(IsotonicRegression(),'price')
ir_pipeline

# COMMAND ----------

gbt_pipeline = create_end_to_end_pipeline(GBTRegressor(),'price')
gbt_pipeline

# COMMAND ----------

fitted_pipelines = []
r2_scores = []
mae_scores = []
rmse_scores = []
mse_scores = []
explained_variance_scores = []

# COMMAND ----------

def train_and_evaluate_model(pipeline: Pipeline, labelCol: str) -> None:
    fitted_pipeline = pipeline.fit(train_df)
    predictions = fitted_pipeline.transform(test_df)
    r2_eval = RegressionEvaluator(labelCol=labelCol,predictionCol='prediction',metricName='r2')
    mae_eval = RegressionEvaluator(labelCol=labelCol,predictionCol='prediction',metricName='mae')
    rmse_eval = RegressionEvaluator(labelCol=labelCol,predictionCol='prediction',metricName='rmse')
    mse_eval = RegressionEvaluator(labelCol=labelCol,predictionCol='prediction',metricName='mse')
    explained_variance_eval = RegressionEvaluator(labelCol=labelCol,predictionCol='prediction',metricName='var')
    r2 = r2_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    rmse = rmse_eval.evaluate(predictions)
    mse = mse_eval.evaluate(predictions)
    explained_variance = explained_variance_eval.evaluate(predictions)
    print(f"R2 Score: {r2}")
    print(f"MAE: {mae}")
    print(f"RMSE: {rmse}")
    print(f"MSE: {mse}")
    print(f"Explained Variance: {explained_variance}")
    fitted_pipelines.append(fitted_pipeline)
    r2_scores.append(r2)
    mae_scores.append(mae)
    rmse_scores.append(rmse)
    mse_scores.append(mse)
    explained_variance_scores.append(explained_variance)

# COMMAND ----------

train_and_evaluate_model(lr_pipeline,'price')

# COMMAND ----------

train_and_evaluate_model(dt_pipeline, 'price')

# COMMAND ----------

train_and_evaluate_model(rf_pipeline, 'price')

# COMMAND ----------

train_and_evaluate_model(fm_pipeline, 'price')

# COMMAND ----------

train_and_evaluate_model(gbt_pipeline, 'price')

# COMMAND ----------

train_and_evaluate_model(ir_pipeline, 'price')

# COMMAND ----------

train_and_evaluate_model(gr_pipeline, 'price')

# COMMAND ----------

train_and_evaluate_model(pr_pipeline, 'price')

# COMMAND ----------

train_and_evaluate_model(gamma_pipeline, 'price')

# COMMAND ----------

train_and_evaluate_model(tweedie_pipeline, 'price')

# COMMAND ----------

model_pipeline = lr_pipeline.fit(train_df)
model_pipeline.transform(test_df)

# COMMAND ----------

model_pipeline.write().overwrite().save(f"{model_folder_path}/ml_pipeline")

# COMMAND ----------

loaded_pipeline = PipelineModel.load(f"{model_folder_path}/ml_pipeline")
loaded_pipeline

# COMMAND ----------

display(loaded_pipeline.transform(test_df).select('price','prediction'))

# COMMAND ----------

train_df.printSchema()

# COMMAND ----------

lr_model = loaded_pipeline.stages[-1]
lr_model

# COMMAND ----------

from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

# COMMAND ----------

sklearn_model = LinearRegression()
sklearn_model.coef_ = np.array(lr_model.coefficients.toArray())
sklearn_model.intercept_ = lr_model.intercept

# COMMAND ----------

joblib.dump(sklearn_model,'amazon_products_price_predictor.pkl')

# COMMAND ----------

scaler_model = loaded_pipeline.stages[1]
scaler_model

# COMMAND ----------

sklearn_scaler = StandardScaler()
sklearn_scaler.mean_ = np.array(scaler_model.mean.toArray())
sklearn_scaler.scale_ = np.array(scaler_model.std.toArray()) 

# COMMAND ----------

joblib.dump(sklearn_scaler,'scaler.pkl')