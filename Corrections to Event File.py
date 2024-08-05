import pandas as pd
from datetime import timedelta

# Local file path for the event data
file_path = 'D:\GitHub\Equity Project\Index Event Data.xlsx'  # Adjust this path to your actual file location

# Load the Excel file from local filesystem
try:
    index_event_data = pd.read_excel(file_path, sheet_name='Data', engine='openpyxl')
except Exception as e:
    print(f"Failed to read Excel file: {e}")
    raise SystemExit(e)


# Function to correct the "Announced" dates
def correct_announced_dates(data):
    # Identify problematic entries where the "Announced" date is one year ahead
    problematic_entries = data[(data['Announced'] > data['Trade Date'])]

    # Correct the "Announced" date by subtracting one year
    data.loc[problematic_entries.index, 'Announced'] = data.loc[problematic_entries.index, 'Announced'] - pd.DateOffset(
        years=1)

    return data


# Correct the dates
corrected_data = correct_announced_dates(index_event_data)

# Save the corrected data to a new Excel file
corrected_file_path = 'D:\GitHub\Equity Project\Index Event Data.xlsx'  # Adjust this path as needed
corrected_data.to_excel(corrected_file_path, index=False, sheet_name='Data')

print(f"Corrected data saved to {corrected_file_path}")
