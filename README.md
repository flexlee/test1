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
