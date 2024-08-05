import pandas as pd

# Load the original Index Event Data file
index_event_data_file_path = 'D:\GitHub\Equity Project\Index Event Data.xlsx'

# Load the Excel file
index_event_data = pd.read_excel(index_event_data_file_path, sheet_name='Data', engine='openpyxl')

# Convert 'Announced' column to datetime if it's not already
index_event_data['Announced'] = pd.to_datetime(index_event_data['Announced'])

# Separate the data into two DataFrames based on the date
date_cutoff = pd.Timestamp('2023-06-01')
before_june_2023 = index_event_data[index_event_data['Announced'] < date_cutoff]
after_june_2023 = index_event_data[index_event_data['Announced'] >= date_cutoff]

# Save the two DataFrames to separate Excel files
before_june_2023_file_path = 'Index_Event_Data_Before_June_2023.csv'
after_june_2023_file_path = 'Index_Event_Data_After_June_2023.csv'

before_june_2023.to_csv(before_june_2023_file_path, index=False)
after_june_2023.to_csv(after_june_2023_file_path, index=False)
