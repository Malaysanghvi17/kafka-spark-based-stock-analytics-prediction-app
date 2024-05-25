# Imports
import yfinance as yf

ticker = "AAPL"  # Replace with the ticker symbol of the company you want to analyze
start_date = "2010-01-01"
# end_date = "2020-12-31"

stock_prices_data = yf.download(ticker, start=start_date)

stock_prices_data.to_csv("stock_prices_data.csv")