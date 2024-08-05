import pandas as pd

# Load the delisted_tickers.csv file
file_path = 'D:\GitHub\Equity Project\A Data Retrieval and Evaluation\delisted_tickers.csv'  # Replace with your actual file path
delisted_tickers_df = pd.read_csv(file_path)

# Get a list of unique tickers
unique_tickers = delisted_tickers_df['Ticker'].unique()

# Get a list of tickers with at least one delete action associated with it
tickers_with_delete_action = delisted_tickers_df[delisted_tickers_df['Action'] == 'Delete']['Ticker'].unique()

# Convert to lists
unique_tickers_list = unique_tickers.tolist()
tickers_with_delete_action_list = tickers_with_delete_action.tolist()

tickers_without_list = unique_tickers_list - tickers_with_delete_action_list


# Print the results
print("Number of Unique Tickers:")

print(len(unique_tickers_list))
#255

print("\nTickers with at least one delete action:")
print(len(tickers_with_delete_action_list))
#219

# This may be of some concern ...
