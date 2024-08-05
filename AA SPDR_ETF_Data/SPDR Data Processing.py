import pandas as pd
import os

# List of SPDR ETFs
spdr_etfs = ["XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLB", "XLRE", "XLK", "XLU", "SPY"]

# Process each ETF file
for etf in spdr_etfs:
    file_path = f"{etf}_data.csv"

    # Check if the file exists
    if os.path.exists(file_path):
        # Load the CSV file
        data = pd.read_csv(file_path)

        # Convert 'Date' to datetime format
        data['Date'] = pd.to_datetime(data['Date'])

        # Calculate daily returns
        data['Return'] = data['Close'].pct_change()

        # Save the updated dataframe to a new CSV file
        output_file_path =  f"{etf}_data.csv"
        data.to_csv(output_file_path, index=False)

        print(f"Updated CSV file saved to {output_file_path}")
    else:
        print(f"File {file_path} does not exist.")
