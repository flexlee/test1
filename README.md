```
import os
import pandas as pd

def combine_csv_with_selected_headers_memory_efficient(root_folder, selected_headers, output_file):
    with open(output_file, 'w') as outfile:
        header_written = False
        for dirpath, dirnames, filenames in os.walk(root_folder):
            for filename in filenames:
                if filename.endswith('.csv'):
                    filepath = os.path.join(dirpath, filename)
                    try:
                        for chunk in pd.read_csv(
                            filepath, 
                            usecols=lambda col: col.strip() in selected_headers, 
                            chunksize=50000
                        ):
                            chunk = chunk[selected_headers]  # Ensure column order
                            if not header_written:
                                chunk.to_csv(outfile, index=False, header=True, mode='a')
                                header_written = True
                            else:
                                chunk.to_csv(outfile, index=False, header=False, mode='a')
                    except Exception as e:
                        print(f"Error reading {filepath}: {e}")
    print(f"Memory efficient combined CSV saved to {output_file} with headers {selected_headers}")

# Change these variables as needed
root_folder = '/path/to/csv/folder'  # Set this to your folder
selected_headers = ["tradeID", "quantity"]
output_file = 'combined_selected_headers_efficient.csv'

combine_csv_with_selected_headers_memory_efficient(root_folder, selected_headers, output_file)


```


# test1

```
import pandas as pd

# Load the CSV file
df = pd.read_csv('trade_blotter.csv', parse_dates=['trade time'])

# Ensure columns are correct
required_cols = ['trade time', 'Symbol', 'Quantity', 'Account', 'Price', 'Side', 'Execution ID']
assert all(col in df.columns for col in required_cols), "CSV missing required columns."

# Filter out rows with blank Execution ID
df = df[df['Execution ID'].notna() & (df['Execution ID'].astype(str).str.strip() != '')]

# Consider trades within 1 minute as possible wash trades
time_window = pd.Timedelta('1 minute')

# Create a DataFrame of buys and sells
buys = df[df['Side'].str.lower() == 'buy']
sells = df[df['Side'].str.lower() == 'sell']

# Merge buys and sells on Account, Symbol, Price, within time window
merged = pd.merge(
    buys,
    sells,
    on=['Account', 'Symbol', 'Price'],
    suffixes=('_buy', '_sell')
)

# Filter pairs within the time window and match quantity
wash_trades = merged[
    (abs(merged['trade time_buy'] - merged['trade time_sell']) <= time_window) &
    (merged['Quantity_buy'] == merged['Quantity_sell'])
]

# Show results or save to file
if not wash_trades.empty:
    print("Possible wash trades found:")
    print(wash_trades[['trade time_buy', 'trade time_sell', 'Symbol', 'Account', 'Quantity_buy', 'Price', 'Execution ID_buy', 'Execution ID_sell']])
else:
    print("No wash trades detected.")

# Optionally, save to CSV
wash_trades.to_csv('wash_trades_detected.csv', index=False)

```



```
import pandas as pd
import os

def detect_wash_trades(csv_path):
    df = pd.read_csv(csv_path, parse_dates=['trade time'])
    required_cols = ['trade time', 'Symbol', 'Quantity', 'Account', 'Price', 'Side', 'Execution ID']
    if not all(col in df.columns for col in required_cols):
        print(f"File {csv_path} skipped: missing required columns.")
        return None

    # Filter out rows with blank Execution ID
    df = df[df['Execution ID'].notna() & (df['Execution ID'].astype(str).str.strip() != '')]

    time_window = pd.Timedelta('1 minute')
    buys = df[df['Side'].str.lower() == 'buy']
    sells = df[df['Side'].str.lower() == 'sell']

    merged = pd.merge(
        buys,
        sells,
        on=['Account', 'Symbol', 'Price'],
        suffixes=('_buy', '_sell')
    )

    wash_trades = merged[
        (abs(merged['trade time_buy'] - merged['trade time_sell']) <= time_window) &
        (merged['Quantity_buy'] == merged['Quantity_sell'])
    ]

    return wash_trades

def process_directory(root_folder):
    for foldername, subfolders, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.lower().endswith('.csv'):
                filepath = os.path.join(foldername, filename)
                print(f"Processing {filepath} ...")
                wash_trades = detect_wash_trades(filepath)
                if wash_trades is not None and not wash_trades.empty:
                    wash_filename = os.path.join(foldername, f'wash_trade_{filename}')
                    wash_trades.to_csv(wash_filename, index=False)
                    print(f"Wash trades saved to {wash_filename}.")
                else:
                    print("No wash trades detected or file skipped.")

# Usage
process_directory('path/to/folder')  # Change to your folder path
```
