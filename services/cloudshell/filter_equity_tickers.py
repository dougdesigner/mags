import pandas as pd

# File path for the input CSV
file_path = 'IVV_holdings.csv'

# Read the raw content of the file to inspect structure
with open(file_path, 'r') as file:
    raw_content = file.readlines()

# Identify the start of actual data (header line)
header_line_index = next(
    (index for index, line in enumerate(raw_content) if "Ticker,Name," in line), None
)

# Load the actual data into a DataFrame, skipping unnecessary lines
if header_line_index is not None:
    cleaned_data = pd.read_csv(file_path, skiprows=header_line_index)

    # Filter the data: keep only rows where 'Asset Class' is 'Equity'
    # and extract the 'Ticker' column
    if 'Asset Class' in cleaned_data.columns and 'Ticker' in cleaned_data.columns:
        filtered_data = cleaned_data[cleaned_data['Asset Class'] == 'Equity']['Ticker']

        # Save the cleaned data to a new CSV file
        output_csv_path = 'filtered_tickers.csv'
        filtered_data.to_csv(output_csv_path, index=False)

        # Save the tickers as a Python array in a .py file
        tickers_list = filtered_data.tolist()
        output_py_path = 'tickers_array.py'
        with open(output_py_path, 'w') as py_file:
            py_file.write(f"tickers = {tickers_list}")
else:
    print("Header line with expected columns was not found in the file.")