kafka-spark-based-stock-analytics-prediction-app:

This application uses spark(sql, ml), kafka, python, streamlit, flutter, etc to create a stock price analytics and prediction app.
The pipeline of the project is as follows:
1. kafka producer gets historical, as well as live data(of all the stocks i have listed) from yfinance library.
2. kafka producer writes data to a kafka topic.
3. from there kafka consumer reads(consumes) the data and write only the updates to the csv file(i know this is not very efficient, i could have used influxdb or cassandra, etc).
4. The data from the csv file will be used by the spark apis to preprocess the data and then the preprocessed data is used to plot graphs, as well as i have leveraged spark.sql library to predict the various prices of stocks in near future.
5. I have used Streamlit app(extremely easy to create a webpage) to display graphs and machine learning based predictions to the users based on the selection of the stock by the user.
6. also implemented webscraping using beautiful soup libarary, so users can get all the latest info of the stock right there in the webapp.
7. i have also create a flutter app that uses webview to display streamlit app inside mobile app.

project working photos:
![image](https://github.com/Malaysanghvi17/kafka-spark-based-stock-analytics-prediction-app/assets/127402092/e6746d65-a6f2-4aaf-8974-6804f5fb5324)
![image](https://github.com/Malaysanghvi17/kafka-spark-based-stock-analytics-prediction-app/assets/127402092/d66413f0-c918-468e-9cf9-5ee22251b3a6)
![image](https://github.com/Malaysanghvi17/kafka-spark-based-stock-analytics-prediction-app/assets/127402092/71922175-7466-43cc-9311-3daa235bdf26)
![image](https://github.com/Malaysanghvi17/kafka-spark-based-stock-analytics-prediction-app/assets/127402092/0a6ff4f7-dfc1-4e12-b8c8-31eee3f521c5)

machine learning based prediction(spark.ml):
![image](https://github.com/Malaysanghvi17/kafka-spark-based-stock-analytics-prediction-app/assets/127402092/8214aed4-872d-4ab4-97a3-94f4c98e7359)

      flutter web view:
![image](https://github.com/Malaysanghvi17/kafka-spark-based-stock-analytics-prediction-app/assets/127402092/b4bfa4f5-6b6f-4c57-b055-4e719deea694)

Also i would like to thank Jawahar Ramis - ( https://github.com/JawaharRamis/stock-price-analysis-kafka-spark-influxdb-grafana ) repository for inspiration of the project.
