import yfinance as yf
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import os

# Define the SPDR sector ETF ticker symbols
spdr_etfs = [
    "XLC", "XLY", "XLP", "XLE", "XLF", "XLV",
    "XLI", "XLB", "XLRE", "XLK", "XLU", "SPY"
]

# Define the date range
start_date = "2022-03-01"
end_date = datetime.today().strftime('%Y-%m-%d')

# Create a folder to store the ETF data
folder_name = "AA SPDR_ETF_Data"
os.makedirs(folder_name, exist_ok=True)

# Function to fetch historical data for a given ETF and save to CSV
def fetch_and_save_etf_data(ticker):
    print(f"Fetching data for {ticker}")
    etf = yf.Ticker(ticker)
    hist = etf.history(start=start_date, end=end_date)
    if not hist.empty:
        hist = hist[['Open','Close', 'Volume']]
        hist['Ticker'] = ticker
        file_name = os.path.join(folder_name, f"{ticker}_data.csv")
        hist.to_csv(file_name)
        print(f"Data for {ticker} saved to {file_name}")
    return hist

# Use ThreadPoolExecutor to fetch data in parallel
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(fetch_and_save_etf_data, ticker) for ticker in spdr_etfs]
    for future in futures:
        future.result()

print("Data fetching and saving complete.")
