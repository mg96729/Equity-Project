import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import os

# Function to prepare the dataset
def prepare_dataset(file_path):
    # Load the CSV file from local filesystem
    try:
        index_event_data = pd.read_csv(file_path)
    except Exception as e:
        print(f"Failed to read CSV file: {e}")
        raise SystemExit(e)

    index_event_data['Announced'] = pd.to_datetime(index_event_data['Announced']).dt.tz_localize(None)
    index_event_data['Trade Date'] = pd.to_datetime(index_event_data['Trade Date']).dt.tz_localize(None)
    index_event_data['Start Date'] = index_event_data['Announced'] - pd.DateOffset(months=2)
    index_event_data['End Date'] = index_event_data['Trade Date'] + pd.DateOffset(months=2)

    return index_event_data


# Function to fetch historical data for a given stock ticker and add additional information
def fetch_historical_data(idx, row):
    ticker = row['Ticker'].split(' ')[0]
    start_date = row['Start Date'].strftime('%Y-%m-%d')
    end_date = row['End Date'].strftime('%Y-%m-%d')
    action = row['Action']
    index = row['Index Change']
    shares_to_trade = row['Shs to Trade']
    adv_to_trade = row['ADV to Trade']

    print(f"Fetching data for {ticker} from {start_date} to {end_date}")
    stock = yf.Ticker(ticker)
    try:
        hist = stock.history(start=start_date, end=end_date)
    except ValueError as e:
        if 'Period' in str(e):
            print(f"{ticker}: {e}")
            return idx, None
        else:
            raise

    if hist.empty:
        print(f"{ticker}: possibly delisted; No timezone found")
        return idx, None

    # Ensure historical data is tz-naive
    hist.index = hist.index.tz_localize(None)

    hist = hist[['Open', 'Close', 'Volume']]
    hist['Date'] = hist.index
    hist['Ticker'] = ticker
    hist['Sector'] = row['Sector']
    hist['Action'] = action
    hist['Index'] = index
    hist['Shares Traded'] = shares_to_trade
    hist['ADV to Trade'] = adv_to_trade

    # Calculate days relative to announcement and trade dates
    hist['Days from Announce'] = (hist['Date'] - row['Announced']).dt.days
    hist['Days from Trade'] = (hist['Date'] - row['Trade Date']).dt.days

    return idx, hist


# Function to process a file and save fetched data
def process_file(file_path, output_file):
    index_event_data = prepare_dataset(file_path)

    # Initialize a list to store all event data
    all_event_data = []

    # Use ThreadPoolExecutor to fetch data in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_historical_data, idx, row) for idx, row in index_event_data.iterrows()]
        for future in futures:
            idx, result = future.result()
            if result is not None and not result.empty:
                all_event_data.append(result)
            else:
                print(f"No data fetched for row index: {idx}")

    # Combine all event data into a single DataFrame
    if all_event_data:
        combined_event_data = pd.concat(all_event_data, ignore_index=True)

        # Save combined data to a single CSV file
        combined_event_data.to_csv(output_file, index=False)

        # Display a sample of the fetched data
        print(f"Sample data for {output_file}:")
        print(combined_event_data.head())

        print(f"Data fetching and saving complete. Data saved to {output_file}")
    else:
        print("No valid data fetched. No objects to concatenate.")


# File paths
before_file_path = 'Index_Event_Data_Before_June_2023.csv'
after_file_path = 'Index_Event_Data_After_June_2023.csv'

# Output files
before_output_file = 'Price_data_Before_June_2023.csv'
after_output_file = 'Price_data_After_June_2023.csv'

# Process files
process_file(before_file_path, before_output_file)
process_file(after_file_path, after_output_file)
