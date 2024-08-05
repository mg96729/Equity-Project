import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import os

# Local file path for the event data
file_path = 'D:\GitHub\Equity Project\Index Event Data.xlsx'  # Adjust this path to your actual file location

# Load the Excel file from local filesystem
try:
    index_event_data = pd.read_excel(file_path, sheet_name='Data', engine='openpyxl')
except Exception as e:
    print(f"Failed to read Excel file: {e}")
    raise SystemExit(e)

# Prepare the dataset
index_event_data['Announced'] = pd.to_datetime(index_event_data['Announced']).dt.tz_localize(None)
index_event_data['Trade Date'] = pd.to_datetime(index_event_data['Trade Date']).dt.tz_localize(None)
index_event_data['Start Date'] = index_event_data['Announced'] - pd.DateOffset(months=2)
index_event_data['End Date'] = index_event_data['Trade Date'] + pd.DateOffset(months=2)

# Initialize lists to store delisted tickers and period invalid entries for verification
delisted_tickers = []
period_invalid_entries = []


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
            period_invalid_entries.append((ticker, index, row['Announced'], row['Trade Date'], str(e)))
            print(f"{ticker}: {e}")
            return idx, None
        else:
            raise

    if hist.empty:
        print(f"{ticker}: possibly delisted; No timezone found")
        delisted_tickers.append((ticker, index, row['Announced'], row['Trade Date'], action))
        return idx, None

    # Ensure historical data is tz-naive
    hist.index = hist.index.tz_localize(None)

    hist = hist[['Open', 'Close', 'Volume']]
    hist['Date'] = hist.index
    hist['Ticker'] = ticker
    hist['Action'] = action
    hist['Index'] = index
    hist['Shares Traded'] = shares_to_trade
    hist['ADV to Trade'] = adv_to_trade

    # Calculate days relative to announcement and trade dates
    hist['Days from Announce'] = hist.index.to_series().apply(
        lambda x: f"A-{(row['Announced'] - x).days}" if x < row['Announced'] else (
            f"A+{(x - row['Announced']).days}" if x >= row['Announced'] else()
        )
    )

    hist['Days from Trade'] = hist.index.to_series().apply(
        lambda x: f"T-{(row['Trade Date'] - x).days}" if x < row['Trade Date'] else (
            f"T+{(x - row['Announced']).days}" if x >= row['Trade Date'] else()
        )
    )

    return idx, hist


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
    output_file = 'event_data_with_details.csv'
    combined_event_data.to_csv(output_file, index=False)

    # Display a sample of the fetched data
    print("Sample data:")
    print(combined_event_data.head())

    print(f"Data fetching and saving complete. Data saved to {output_file}")
else:
    print("No valid data fetched. No objects to concatenate.")

# Save period invalid entries to a separate CSV file
if period_invalid_entries:
    period_invalid_entries_df = pd.DataFrame(period_invalid_entries,
                                             columns=['Ticker', 'Index', 'Announced', 'Trade Date', 'Error'])
    period_invalid_entries_file = 'period_invalid_entries.csv'
    period_invalid_entries_df.to_csv(period_invalid_entries_file, index=False)
    print(f"Period invalid entries saved to {period_invalid_entries_file}")

# Save delisted tickers to a separate CSV file
if delisted_tickers:
    delisted_tickers_df = pd.DataFrame(delisted_tickers,
                                       columns=['Ticker', 'Index', 'Announced', 'Trade Date', 'Action'])
    delisted_tickers_file = 'delisted_tickers.csv'
    delisted_tickers_df.to_csv(delisted_tickers_file, index=False)
    print(f"Delisted tickers saved to {delisted_tickers_file}")

# Verify that delisted tickers are associated with the action type "Delete"
delisted_verification = [(ticker, action) for ticker, index, announced, trade_date, action in delisted_tickers if
                         action == "Delete"]

print(f"Total delisted tickers found: {len(delisted_tickers)}")
print(f"Total delisted tickers with action 'Delete': {len(delisted_verification)}")
