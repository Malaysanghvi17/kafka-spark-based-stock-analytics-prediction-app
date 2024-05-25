import streamlit as st
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import time
from bs4 import BeautifulSoup
import requests
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

def load_csv_spark(stock_name):
    # Read the CSV data into a Spark DataFrame
    data_dir = "data"
    stock_file = stock_name + ".csv"
    stock_file = os.path.join(data_dir, stock_file)
    combined_df = spark.read.csv(stock_file, header=True, schema=schema)
    return combined_df

def plot_stock_data(stock_name, selected_chart_types):
    combined_df = load_csv_spark(stock_name)

    time_df = combined_df.select("Date").toPandas()
    open_prices_df = combined_df.select("Open").toPandas()
    high_prices_df = combined_df.select("High").toPandas()
    low_prices_df = combined_df.select("Low").toPandas()
    close_prices_df = combined_df.select("Close").toPandas()

    st.subheader('Raw Data {}'.format(stock_name))
    st.write(combined_df)  # Display raw data

    st.subheader('Charts')
    if not selected_chart_types:
        st.warning("Please select at least one chart type.")
        return

    with st.spinner('Loading...'):
        time.sleep(3)

    st.subheader('Relative Returns {}'.format(stock_name))

    if 'Line Chart' in selected_chart_types:
        st.line_chart(close_prices_df)
        st.write("### High Prices of {}".format(stock_name))
        st.line_chart(high_prices_df)

        st.write("### Low Prices of {}".format(stock_name))
        st.line_chart(low_prices_df)

    if 'Area Chart' in selected_chart_types:
        st.area_chart(close_prices_df)
        st.write("### High Prices of {}".format(stock_name))
        st.area_chart(high_prices_df)

        st.write("### Low Prices of {}".format(stock_name))
        st.area_chart(low_prices_df)

    if 'Bar Chart' in selected_chart_types:
        st.bar_chart(close_prices_df)
        st.write("### High Prices of {}".format(stock_name))
        st.bar_chart(high_prices_df)

        st.write("### Low Prices of {}".format(stock_name))
        st.bar_chart(low_prices_df)

def predict_stock_prices(stock_name):
    combined_df = load_csv_spark(stock_name)

    # Drop rows with null values
    combined_df = combined_df.dropna()

    # Select relevant features
    features_df = combined_df.select("Open", "High", "Low", "Volume", "Close")

    # Split data into training and testing sets
    train_data, test_data = features_df.randomSplit([0.8, 0.2], seed=123)

    # Prepare features for modeling
    assembler = VectorAssembler(inputCols=["Open", "High", "Low", "Volume"], outputCol="features")
    train_features = assembler.transform(train_data)
    test_features = assembler.transform(test_data)

    # Create and train the model
    lr = LinearRegression(featuresCol="features", labelCol="Close")
    lr_model = lr.fit(train_features)

    # Make predictions
    predictions = lr_model.transform(test_features)

    # Convert predictions to Pandas DataFrame for display in Streamlit
    predictions_df = predictions.select("features", "prediction", "Close").toPandas()
    predictions_df["features"] = predictions_df["features"].apply(lambda x: x.toArray())

    # Evaluate the model
    evaluator = RegressionEvaluator(labelCol="Close", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    
    return predictions_df, rmse
        
def get_stock_data(company_name):
    company_name = company_name.replace('.NS', '')
    
    f_url = "https://www.google.com/finance/quote/"
    e_url = ":NASDAQ"
    
    url = f_url + company_name + e_url
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')

    price, pcp, drp, yrp, mcp = "No Data Available", "No Data Available", "No Data Available", "No Data Available", "No Data Available"
    about = "No Data Available"
    np1, n1, np2, n2 = "No Data Available", "No Data Available", "No Data Available", "No Data Available"
    
    try:
        price = soup.find('div', class_="YMlKec fxKbKc").text
    except Exception:
        pass

    try:
        pcp = soup.find('div', class_="P6K39c").text
    except Exception:
        pass

    try:
        drp = soup.find_all('div', class_="P6K39c")[1].text
    except Exception:
        pass

    try:
        yrp = soup.find_all('div', class_="P6K39c")[2].text
    except Exception:
        pass

    try:
        mcp = soup.find_all('div', class_="P6K39c")[3].text
    except Exception:
        pass
        
    try:
        about = soup.find('div', class_="bLLb2d").text
    except Exception:
        pass

    try:
        np1 = soup.find_all('div', class_="sfyJob")[0].text
        n1 = soup.find_all('div', class_="Yfwt5")[0].text
    except Exception:
        pass

    try:
        np2 = soup.find_all('div', class_="sfyJob")[1].text
        n2 = soup.find_all('div', class_="Yfwt5")[1].text
    except Exception:
        pass

    return price, pcp, drp, yrp, mcp, about, np1, n1, np2, n2

# Main Streamlit app
def main():
    st.title("Stock Analysis App")

    # Dropdown to select stock
    stock_name = st.selectbox("Select Stock", ["AAPL", "MSFT", "GOOGL"])

    # Multiselect to pick chart types
    chart_types = ('Line Chart', 'Area Chart', 'Bar Chart')
    selected_chart_types = st.multiselect('Pick your chart', chart_types)

    # Button to trigger analysis
    if st.button("Show Analysis"):
        st.write("### Analysis for", stock_name)
        # Plot stock data
        plot_stock_data(stock_name, selected_chart_types)

        # Get and display real-time stock data
        price, pcp, drp, yrp, mcp, about, np1, n1, np2, n2 = get_stock_data(stock_name)

        # Display the real-time stock data
        st.subheader('Real-Time Stock Data for {}'.format(stock_name))
        st.write("Current Price:", price)
        st.write("Percentage Change:", pcp)
        st.write("Day's Range Percentage:", drp)
        st.write("52-Week Range Percentage:", yrp)
        st.write("Market Cap Percentage:", mcp)
        
        st.subheader("About the Company")
        st.markdown(about, unsafe_allow_html=True)
                
        st.subheader("NEWS About the Company")
        st.markdown(np1, unsafe_allow_html=True)
        st.markdown(n1, unsafe_allow_html=True)
        st.markdown(np2, unsafe_allow_html=True)
        st.markdown(n2, unsafe_allow_html=True)

    # Button to trigger ML model for prediction
    if st.button("Predict Stock Prices"):
        st.write("Prediction for", stock_name)
        predictions, rmse = predict_stock_prices(stock_name)
        st.write("Predictions by our model:")
        st.dataframe(predictions)
        st.write("Root Mean Squared Error (RMSE):", rmse)

if __name__ == "__main__":
    main()

# import streamlit as st
# import os
# import pandas as pd
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# import time
# from bs4 import BeautifulSoup
# import requests

# # Add import for ML related libraries
# from pyspark.ml.feature import VectorAssembler
# from pyspark.ml.regression import LinearRegression
# from pyspark.ml.evaluation import RegressionEvaluator

# # Create a Spark session
# spark = SparkSession.builder.appName("StockPriceProcessing").getOrCreate()

# # Define the schema for the CSV data
# schema = StructType([
#     StructField("Date", StringType(), True),
#     StructField("Open", DoubleType(), True),
#     StructField("High", DoubleType(), True),
#     StructField("Low", DoubleType(), True),
#     StructField("Close", DoubleType(), True),
#     StructField("Volume", DoubleType(), True)
# ])

# def load_csv_spark(stock_name):
#     # Read the CSV data into a Spark DataFrame
#     data_dir = "data"
    
#     stock_file = stock_name + ".csv"
#     stock_file = os.path.join(data_dir, stock_file)
#     combined_df = spark.read.csv(stock_file, header=True, schema=schema)
#     return combined_df

# def plot_stock_data(stock_name, selected_chart_types):
#     combined_df = load_csv_spark(stock_name)

#     time_df = combined_df.select("Date").toPandas()
#     open_prices_df = combined_df.select("Open").toPandas()
#     high_prices_df = combined_df.select("High").toPandas()
#     low_prices_df = combined_df.select("Low").toPandas()
#     close_prices_df = combined_df.select("Close").toPandas()

#     st.subheader('Raw Data {}'.format(stock_name))
#     st.write(combined_df)  # Display raw data

#     st.subheader('Charts')
#     if not selected_chart_types:
#         st.warning("Please select at least one chart type.")
#         return

#     with st.spinner('Loading...'):
#         time.sleep(3)

#     st.subheader('Relative Returns {}'.format(stock_name))

#     if 'Line Chart' in selected_chart_types:
#         st.line_chart(close_prices_df)
#         st.write("### High Prices of {}".format(stock_name))
#         st.line_chart(high_prices_df)

#         st.write("### Low Prices of {}".format(stock_name))
#         st.line_chart(low_prices_df)

#     if 'Area Chart' in selected_chart_types:
#         st.area_chart(close_prices_df)
#         st.write("### High Prices of {}".format(stock_name))
#         st.area_chart(high_prices_df)

#         st.write("### Low Prices of {}".format(stock_name))
#         st.area_chart(low_prices_df)

#     if 'Bar Chart' in selected_chart_types:
#         st.bar_chart(close_prices_df)
#         st.write("### High Prices of {}".format(stock_name))
#         st.bar_chart(high_prices_df)

#         st.write("### Low Prices of {}".format(stock_name))
#         st.bar_chart(low_prices_df)

# def predict_stock_prices(stock_name):
#     combined_df = load_csv_spark(stock_name)

#     # Drop rows with null values
#     combined_df = combined_df.dropna()

#     # Select relevant features
#     features_df = combined_df.select("Open", "High", "Low", "Volume", "Close")

#     # Split data into training and testing sets
#     train_data, test_data = features_df.randomSplit([0.8, 0.2], seed=123)

#     # Prepare features for modeling
#     assembler = VectorAssembler(inputCols=["Open", "High", "Low", "Volume"], outputCol="features")
#     train_features = assembler.transform(train_data)
#     test_features = assembler.transform(test_data)

#     # Create and train the model
#     lr = LinearRegression(featuresCol="features", labelCol="Close")
#     lr_model = lr.fit(train_features)

#     # Make predictions
#     predictions = lr_model.transform(test_features)

#     # Evaluate the model
#     evaluator = RegressionEvaluator(labelCol="Close", predictionCol="prediction", metricName="rmse")
#     rmse = evaluator.evaluate(predictions)
    
#     return predictions, rmse
        
# def get_stock_data(company_name):
#     company_name = company_name.replace('.NS', '')
    
#     f_url = "https://www.google.com/finance/quote/"
#     e_url = ":NASDAQ"
    
#     url = f_url + company_name + e_url
#     r = requests.get(url)
#     soup = BeautifulSoup(r.text, 'lxml')

#     price, pcp, drp, yrp, mcp = "No Data Available", "No Data Available", "No Data Available", "No Data Available", "No Data Available"
#     about = "No Data Available"
#     np1, n1, np2, n2 = "No Data Available", "No Data Available", "No Data Available", "No Data Available"
    
#     try:
#         price = soup.find('div', class_="YMlKec fxKbKc").text
#     except Exception:
#         pass

#     try:
#         pcp = soup.find('div', class_="P6K39c").text
#     except Exception:
#         pass

#     try:
#         drp = soup.find_all('div', class_="P6K39c")[1].text
#     except Exception:
#         pass

#     try:
#         yrp = soup.find_all('div', class_="P6K39c")[2].text
#     except Exception:
#         pass

#     try:
#         mcp = soup.find_all('div', class_="P6K39c")[3].text
#     except Exception:
#         pass
        
#     try:
#         about = soup.find('div', class_="bLLb2d").text
#     except Exception:
#         pass

#     try:
#         np1 = soup.find_all('div', class_="sfyJob")[0].text
#         n1 = soup.find_all('div', class_="Yfwt5")[0].text
#     except Exception:
#         pass

#     try:
#         np2 = soup.find_all('div', class_="sfyJob")[1].text
#         n2 = soup.find_all('div', class_="Yfwt5")[1].text
#     except Exception:
#         pass

#     return price, pcp, drp, yrp, mcp, about, np1, n1, np2, n2

# # Main Streamlit app
# def main():
#     st.title("Stock Analysis App")

#     # Dropdown to select stock
#     stock_name = st.selectbox("Select Stock", ["AAPL", "MSFT", "GOOGL"])

#     # Multiselect to pick chart types
#     chart_types = ('Line Chart', 'Area Chart', 'Bar Chart')
#     selected_chart_types = st.multiselect('Pick your chart', chart_types)

#     # Button to trigger analysis
#     if st.button("Show Analysis"):
#         st.write("### Analysis for", stock_name)
#         # Plot stock data
#         plot_stock_data(stock_name, selected_chart_types)

#         # Get and display real-time stock data
#         price, pcp, drp, yrp, mcp, about, np1, n1, np2, n2 = get_stock_data(stock_name)

#         # Display the real-time stock data
#         st.subheader('Real-Time Stock Data for {}'.format(stock_name))
#         st.write("Current Price:", price)
#         st.write("Percentage Change:", pcp)
#         st.write("Day's Range Percentage:", drp)
#         st.write("52-Week Range Percentage:", yrp)
#         st.write("Market Cap Percentage:", mcp)
        
#         st.subheader("About the Company")
#         st.markdown(about, unsafe_allow_html=True)
                
#         st.subheader("NEWS About the Company")
#         st.markdown(np1, unsafe_allow_html=True)
#         st.markdown(n1, unsafe_allow_html=True)
#         st.markdown(np2, unsafe_allow_html=True)
#         st.markdown(n2, unsafe_allow_html=True)

#     # Button to trigger ML model for prediction
#     if st.button("Predict Stock Prices"):
#         st.write("Prediction for", stock_name)
#         predictions, rmse = predict_stock_prices(stock_name)
#         st.write("Predictions by our model: ", predictions)
#         st.write("Predictions by our model: ", rmse)
# if __name__ == "__main__":
#     main()


# import streamlit as st
# import os
# import pandas as pd
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# import time
# # Create a Spark session
# spark = SparkSession.builder.appName("StockPriceProcessing").getOrCreate()

# # Define the schema for the CSV data
# schema = StructType([
#     StructField("Date", StringType(), True),
#     StructField("Open", DoubleType(), True),
#     StructField("High", DoubleType(), True),
#     StructField("Low", DoubleType(), True),
#     StructField("Close", DoubleType(), True),
#     StructField("Volume", DoubleType(), True)
# ])

# def load_csv_spark(stock_name):
#     # Read the CSV data into a Spark DataFrame
#     data_dir = "data"
    
#     stock_file = stock_name + ".csv"
#     stock_file = os.path.join(data_dir, stock_file)
#     combined_df = spark.read.csv(stock_file, header=True, schema=schema)
#     return combined_df

# def plot_stock_data(stock_name):
#     combined_df = load_csv_spark(stock_name)

#     time_df = combined_df.select("Date").toPandas()
#     open_prices_df = combined_df.select("Open").toPandas()
#     high_prices_df = combined_df.select("High").toPandas()
#     low_prices_df = combined_df.select("Low").toPandas()
#     close_prices_df = combined_df.select("Close").toPandas()

#     st.subheader('Raw Data {}'.format(stock_name))
#     st.write(combined_df)  # Display raw data

#     # chart_types = ('Line Chart', 'Area Chart', 'Bar Chart')
#     # dropdown1 = st.multiselect('Pick your chart', chart_types,)
#     # print(dropdown1)

#     # while (len(dropdown1) == 0):
#     #     time.sleep(10)
#     dropdown1 = 'Line Chart'
#     with st.spinner('Loading...'):
#         time.sleep(5)

#     st.subheader('Relative Returns {}'.format(stock_name))

#     if (dropdown1) == 'Line Chart':
#         st.line_chart(close_prices_df)
#         st.write("### High Prices of {}".format(stock_name))
#         st.line_chart(high_prices_df)

#         st.write("### Low Prices of {}".format(stock_name))
#         st.line_chart(low_prices_df)

#     # elif (dropdown1) == 'Area Chart':
#         st.area_chart(close_prices_df)
#         st.write("### High Prices of {}".format(stock_name))
#         st.area_chart(high_prices_df)

#         st.write("### Low Prices of {}".format(stock_name))
#         st.area_chart(low_prices_df)

#     # elif (dropdown1) == 'Bar Chart':
#         st.bar_chart(close_prices_df)
#         st.write("### High Prices of {}".format(stock_name))
#         st.bar_chart(high_prices_df)

#         st.write("### Low Prices of {}".format(stock_name))
#         st.bar_chart(low_prices_df)

#     else:
#         st.line_chart(close_prices_df, width=1000, height=800, use_container_width=False)
#         st.write("### High Prices of {}".format(stock_name))
#         st.line_chart(high_prices_df)

#         st.write("### Low Prices of {}".format(stock_name))
#         st.line_chart(low_prices_df)

# # Main Streamlit app
# def main():
#     st.title("Stock Analysis App")

#     # Dropdown to select stock
#     stock_name = st.selectbox("Select Stock", ["AAPL", "MSFT", "GOOGL"])

#     # Button to trigger analysis
#     if st.button("Show Analysis"):
#         st.write("### Analysis for", stock_name)
#         # Code to fetch and display news about the selected stock from Yahoo Finance or any other source

#         # Plot stock data
#         plot_stock_data(stock_name)

# if __name__ == "__main__":
#     main()
