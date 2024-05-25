import sys
import yfinance as yf
import json
from confluent_kafka import Producer
from datetime import datetime, timedelta, time, timezone
import time as t
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_to_kafka(producer, topic, key, message):
    print("sent to kafka", message)
    producer.produce(topic, key=key, value=json.dumps(message).encode("utf-8"))
    producer.flush()

def retrieve_historical_data(producer, stock_symbol, kafka_topic, logger):
    stock_symbols = stock_symbol.split(",") if stock_symbol else []
    if not stock_symbols:
        logger.error(f"No stock symbols provided.")
        return

    end_date = datetime.now()
    start_date = (yf.Ticker(stock_symbols[0]).history(period="1mo").index[0]).strftime('%Y-%m-%d')
    for stock_symbol in stock_symbols:
        historical_data = yf.download(stock_symbol, start=start_date, end=end_date, interval="2m", prepost=True)
        historical_data = historical_data.dropna(subset=["Open", "High", "Low", "Close", "Adj Close", "Volume"])

        for index, row in historical_data.iterrows():
            historical_data_point = {
                'stock': stock_symbol,
                'date': row.name.isoformat(),
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume']
            }
            send_to_kafka(producer, kafka_topic, stock_symbol, historical_data_point)

def retrieve_real_time_data(producer, stock_symbol, kafka_topic, logger):
    retrieve_historical_data(producer, stock_symbol, kafka_topic, logger)
    stock_symbols = stock_symbol.split(",") if stock_symbol else []
    if not stock_symbols:
        logger.error(f"No stock symbols provided.")
        return

    while True:
        current_time = datetime.now()
        if is_stock_market_open(current_time):
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=1)
            for stock_symbol in stock_symbols:
                real_time_data = yf.download(stock_symbol, start=start_time, end=end_time, interval="2m")
                if not real_time_data.empty:
                    latest_data_point = real_time_data.iloc[-1]
                    real_time_data_point = {
                        'stock': stock_symbol,
                        'date': latest_data_point.name.isoformat(),
                        'open': latest_data_point['Open'],
                        'high': latest_data_point['High'],
                        'low': latest_data_point['Low'],
                        'close': latest_data_point['Close'],
                        'volume': latest_data_point['Volume']
                    }
                    send_to_kafka(producer, kafka_topic, stock_symbol, real_time_data_point)
                    logger.info(f"Stock value retrieved and pushed to kafka topic {kafka_topic}")
        else:
            for stock_symbol in stock_symbols:
                null_data_point = {
                    'stock': stock_symbol,
                    'date': current_time.isoformat(),
                    'open': None,
                    'high': None,
                    'low': None,
                    'close': None,
                    'volume': None
                }
                send_to_kafka(producer, kafka_topic, stock_symbol, null_data_point)
        t.sleep(10)

def get_stock_details(stock_symbol, logger):
    stock_symbols = stock_symbol.split(",") if stock_symbol else []
    logger.info(stock_symbols)
    if not stock_symbols:
        logger.error(f"No stock symbols provided.")
        return []

    stock_details = []
    for stock_symbol in stock_symbols:
        try:
            ticker = yf.Ticker(stock_symbol)
            stock_info = {
                'Date': datetime.now().strftime('%Y-%m-%d'),
                'Symbol': stock_symbol,
                'ShortName': ticker.info.get('shortName', ''),
                'LongName': ticker.info.get('longName', ''),
                'Industry': ticker.info.get('industry', ''),
                'Sector': ticker.info.get('sector', ''),
                'MarketCap': ticker.info.get('marketCap', 0),
                'ForwardPE': ticker.info.get('forwardPE', 0),
                'TrailingPE': ticker.info.get('trailingPE', 0),
                'Currency': ticker.info.get('currency', ''),
                'FiftyTwoWeekHigh': ticker.info.get('fiftyTwoWeekHigh', 0),
                'FiftyTwoWeekLow': ticker.info.get('fiftyTwoWeekLow', 0),
                'FiftyDayAverage': ticker.info.get('fiftyDayAverage', 0),
                'Exchange': ticker.info.get('exchange', ''),
                'ShortRatio': ticker.info.get('shortRatio', 0)
            }
            stock_details.append(stock_info)
        except Exception as e:
            logger.error(f"Error fetching stock details for {stock_symbol}: {str(e)}")

    return stock_details

def is_stock_market_open(current_datetime=None):
    if current_datetime is None:
        current_datetime = datetime.now()

    market_open_time = time(9, 30)
    market_close_time = time(16, 0)

    current_time_et = current_datetime.astimezone(timezone(timedelta(hours=-4)))

    if current_time_et.weekday() < 5 and market_open_time <= current_time_et.time() < market_close_time:
        return True
    else:
        return False

kafka_bootstrap_servers = "localhost:9092"
kafka_config = {
    "bootstrap.servers": kafka_bootstrap_servers,
}
producer = Producer(kafka_config)

if __name__ == '__main__':
    stock_symbol = "AAPL,MSFT,GOOGL"
    kafka_topic = "stock_prices"

    try:
        retrieve_real_time_data(producer, stock_symbol, kafka_topic, logger)
    except Exception as e:
        logger.error(f"An error occurred: {e}")

# import sys
# import yfinance as yf
# import pandas as pd
# import numpy as np
# import json
# from confluent_kafka import Producer
# from datetime import datetime, timedelta, time, timezone
# import time
# from pathlib import Path

# path_to_utils = Path(__file__).parent.parent
# sys.path.insert(0, str(path_to_utils))
# sys.path.append("./")

# def send_to_kafka(producer, topic, key, message):
#     print("sent to kafka", message)
#     producer.produce(topic, key=key, value=json.dumps(message).encode("utf-8"))
#     producer.flush()

# def retrieve_historical_data(producer, stock_symbol, kafka_topic, logger):
#     # Define the date range for historical data
#     stock_symbols = stock_symbol.split(",") if stock_symbol else []
#     if not stock_symbols:
#         logger.error(f"No stock symbols provided in the environment variable.")
#         return  # Exit the function if no stock symbols are provided

#     # Fetch historical data
#     end_date = datetime.now()
#     start_date = (yf.Ticker(stock_symbols[0]).history(period="1mo").index[0]).strftime('%Y-%m-%d')
#     for stock_symbol in stock_symbols:
#         historical_data = yf.download(stock_symbol, start=start_date, end=end_date, interval="2m", prepost=True)
#         historical_data = historical_data.dropna(subset=["Open", "High", "Low", "Close", "Adj Close", "Volume"])

#         # Convert and send historical data to Kafka
#         for index, row in historical_data.iterrows():
#             historical_data_point = {
#                 'stock': stock_symbol,
#                 'date': row.name.isoformat(),
#                 'open': row['Open'],
#                 'high': row['High'],
#                 'low': row['Low'],
#                 'close': row['Close'],
#                 'volume': row['Volume']
#             }
#             send_to_kafka(producer, kafka_topic, stock_symbol, historical_data_point)

# def retrieve_real_time_data(producer, stock_symbol, kafka_topic, logger):
#     # Define the stock symbol for real-time data
#     retrieve_historical_data(producer, stock_symbol, kafka_topic, logger)
#     stock_symbols = stock_symbol.split(",") if stock_symbol else []
#     if not stock_symbols:
#         logger.error(f"No stock symbols provided in the environment variable.")
#         return  # Exit the function if no stock symbols are provided

#     while True:
#         # Fetch real-time data for the last 1 minute
#         current_time = datetime.now()
#         is_market_open_bool = is_stock_market_open(current_time)
#         if is_market_open_bool:
#             end_time = datetime.now()
#             start_time = end_time - timedelta(minutes=1)
#             for stock_symbol in stock_symbols:
#                 real_time_data = yf.download(stock_symbol, start=start_time, end=end_time, interval="2m")
#                 if not real_time_data.empty:
#                     # Convert and send the latest real-time data point to Kafka
#                     latest_data_point = real_time_data.iloc[-1]
#                     real_time_data_point = {
#                         'stock': stock_symbol,
#                         'date': latest_data_point.name.isoformat(),
#                         'open': latest_data_point['Open'],
#                         'high': latest_data_point['High'],
#                         'low': latest_data_point['Low'],
#                         'close': latest_data_point['Close'],
#                         'volume': latest_data_point['Volume']
#                     }
#                     send_to_kafka(producer, kafka_topic, stock_symbol, real_time_data_point)
#                     logger.info(f"Stock value retrieved and pushed to kafka topic {kafka_topic}")
#         else:
#             for stock_symbol in stock_symbols:
#                 null_data_point = {
#                     'stock': stock_symbol,
#                     'date': current_time.isoformat(),
#                     'open': None,
#                     'high': None,
#                     'low': None,
#                     'close': None,
#                     'volume': None
#                 }
#                 send_to_kafka(producer, kafka_topic, stock_symbol, null_data_point)
#         time.sleep(20)  # Wait for (20 seconds)

# def get_stock_details(stock_symbol, logger):
#     stock_symbols = stock_symbol.split(",") if stock_symbol else []
#     print(stock_symbols)
#     logger.info(stock_symbols)
#     if not stock_symbols:
#         logger.error(f"No stock symbols provided in the environment variable.")
#         return []  # Return an empty list if no stock symbols are provided

#     stock_details = []
#     for stock_symbol in stock_symbols:
#         try:
#             ticker = yf.Ticker(stock_symbol)

#             # Retrieve general stock information
#             stock_info = {
#                 'Date': datetime.now().strftime('%Y-%m-%d'),
#                 'Symbol': stock_symbol,
#                 'ShortName': ticker.info['shortName'],
#                 'LongName': ticker.info['longName'],
#                 'Industry': ticker.info['industry'],
#                 'Sector': ticker.info['sector'],
#                 'MarketCap': ticker.info['marketCap'],
#                 'ForwardPE': ticker.info['forwardPE'],
#                 'TrailingPE': ticker.info['trailingPE'],
#                 'Currency': ticker.info['currency'],
#                 'FiftyTwoWeekHigh': ticker.info['fiftyTwoWeekHigh'],
#                 'FiftyTwoWeekLow': ticker.info['fiftyTwoWeekLow'],
#                 'FiftyDayAverage': ticker.info['fiftyDayAverage'],
#                 'Exchange': ticker.info['exchange'],
#                 'ShortRatio': ticker.info['shortRatio']
#             }
#             stock_details.append(stock_info)
#         except Exception as e:
#             logger.error(f"Error fetching stock details for {stock_symbol}: {str(e)}")

#     return stock_details

# def is_stock_market_open(current_datetime=None):
#     # If no datetime is provided, use the current datetime
#     if current_datetime is None:
#         current_datetime = datetime.now()

#     # Define NYSE trading hours in Eastern Time Zone
#     market_open_time = time(9, 30)
#     market_close_time = time(16, 0)

#     # Convert current_datetime to Eastern Time Zone
#     current_time_et = current_datetime.astimezone(timezone(timedelta(hours=-4)))  # EDT (UTC-4)

#     # Check if it's a weekday and within trading hours
#     if current_time_et.weekday() < 5 and market_open_time <= current_time_et.time() < market_close_time:
#         return True
#     else:
#         return False

# kafka_bootstrap_servers = "localhost:9092"
# kafka_config = {
#     "bootstrap.servers": kafka_bootstrap_servers,
# }
# producer = Producer(kafka_config)

# if __name__ == '__main__':
#     stock_symbol = "AAPL,MSFT,GOOGL"  # Replace with the desired stock symbols
#     kafka_topic = "stock_prices"  # Replace with the desired Kafka topic
#     logger = None  # Provide a logger instance if needed

#     try:
#         retrieve_real_time_data(producer, stock_symbol, kafka_topic, logger)
#     except Exception as e:
#         print(f"An error occurred: {e}")