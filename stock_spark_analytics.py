import os
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Create a Spark session
spark = SparkSession.builder.appName("StockPriceProcessing").getOrCreate()

# Define the schema for the CSV data
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True)
])

# Read the CSV data into a Spark DataFrame
data_dir = "data"
stock_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".csv")]

dfs = []
for stock_file in stock_files:
    stock_name = os.path.splitext(os.path.basename(stock_file))[0]
    df = spark.read.csv(stock_file, header=True, schema=schema)
    df = df.withColumn("Stock", lit(stock_name))
    dfs.append(df)

combined_df = dfs[0]
for i in range(1, len(dfs)):
    combined_df = combined_df.union(dfs[i])

# Handle missing values
combined_df = combined_df.na.fill(0.0, subset=["Open", "High", "Low", "Close", "Volume"])

# Limit values to certain decimal places
combined_df = combined_df.withColumn("Open", combined_df["Open"].cast("decimal(10,2)"))
combined_df = combined_df.withColumn("High", combined_df["High"].cast("decimal(10,2)"))
combined_df = combined_df.withColumn("Low", combined_df["Low"].cast("decimal(10,2)"))
combined_df = combined_df.withColumn("Close", combined_df["Close"].cast("decimal(10,2)"))
combined_df = combined_df.withColumn("Volume", combined_df["Volume"].cast("decimal(10,2)"))

# Handle closed market days
market_closed_condition = combined_df["Open"].isNull() & combined_df["High"].isNull() & combined_df["Low"].isNull() & combined_df["Close"].isNull() & combined_df["Volume"].isNull()
combined_df = combined_df.withColumn("MarketClosed", market_closed_condition.cast("integer"))

def create_line_plot(stock_name):
    stock_df = combined_df.filter(combined_df["Stock"] == stock_name).toPandas()
    if stock_df.empty:
        print(f"No data found for {stock_name}")
        return
    stock_df.set_index("Date", inplace=True)
    stock_df["Close"].plot(figsize=(12, 6), title=f"{stock_name} Closing Prices")
    plt.show()


# Create a candlestick plot for a specific stock
def create_candlestick_plot(stock_name):
    stock_df = combined_df.filter(combined_df["Stock"] == stock_name).toPandas()
    stock_df.set_index("Date", inplace=True)
    stock_df["Close"].plot(kind="candlestick", figsize=(12, 6), title=f"{stock_name} Candlestick Plot")
    plt.show()

create_line_plot("AAPL")
create_candlestick_plot("GOOGL")

# Prepare the data for modeling
feature_cols = ["Open", "High", "Low", "Volume"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
model_data = vector_assembler.transform(combined_df)
model_data = model_data.select("features", "Close")

# Split the data into training and testing sets
(training_data, test_data) = model_data.randomSplit([0.7, 0.3], seed=42)

# Train the linear regression model
lr = LinearRegression(featuresCol="features", labelCol="Close")
lr_model = lr.fit(training_data)

# Evaluate the model
predictions = lr_model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="Close", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")

# Predict future stock prices
future_data = spark.createDataFrame([
    (170.0, 172.0, 168.0, 1000000.0)
], ["Open", "High", "Low", "Volume"])

future_data = vector_assembler.transform(future_data)
predictions = lr_model.transform(future_data)
predictions.select("prediction").show()