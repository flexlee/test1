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
required_cols = ['trade time', 'Symbol', 'Quantity', 'Account', 'Price', 'Side']
assert all(col in df.columns for col in required_cols), "CSV missing required columns."

# Consider trades within 1 minute as possible wash trades
time_window = pd.Timedelta('1 minute')

# Create a DataFrame of buys and sells
buys = df[df['Side'].str.lower() == 'buy']
sells = df[df['Side'].str.lower() == 'sell']

# Merge buys and sells on Account, Symbol, Price, and within time window
merged = pd.merge(
    buys,
    sells,
    on=['Account', 'Symbol', 'Price'],
    suffixes=('_buy', '_sell')
)

# Filter pairs within the time window and match quantity (optional)
wash_trades = merged[
    (abs(merged['trade time_buy'] - merged['trade time_sell']) <= time_window) &
    (merged['Quantity_buy'] == merged['Quantity_sell'])
]

# Output wash trades or save to file
if not wash_trades.empty:
    print("Possible wash trades found:")
    print(wash_trades[['trade time_buy', 'trade time_sell', 'Symbol', 'Account', 'Quantity_buy', 'Price']])
else:
    print("No wash trades detected.")

# Optionally, save to CSV
wash_trades.to_csv('wash_trades_detected.csv', index=False)
```
