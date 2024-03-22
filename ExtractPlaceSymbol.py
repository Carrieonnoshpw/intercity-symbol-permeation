import re
import os
import glob
import pandas as pd
import concurrent.futures
from typing import List, Tuple, Dict
import gc
import time

starttime = time.time()

def check_and_extract(row: pd.Series, placesymbol_dict: Dict[str, int]) -> Tuple[str, str, str, str, int]:
    """
        Check if the 'name' field in the row contains any of the placesymbols.
        If it does, return the relevant fields after removing parentheses and their contents.
        Prefer the placesymbol that appears first in the 'name' field.
    """
    name_modified = re.sub(r'\([^)]*\)', '', row['name'])
    
    # Find the position of each placesymbol in the name
    symbol_positions = {symbol: name_modified.find(symbol) for symbol, code in placesymbol_dict.items() if symbol in name_modified}
    
    # Filter out symbols that were not found (-1) and find the one that appears first
    symbol_positions = {symbol: pos for symbol, pos in symbol_positions.items() if pos != -1}
    if symbol_positions:
        first_symbol = min(symbol_positions, key=symbol_positions.get)
        return name_modified, row['_id'], row['adcode'], first_symbol, placesymbol_dict[first_symbol]
    return None

def process_csv(file_path: str, placesymbol_dict: Dict[str, int], output_dir: str, max_rows_per_file: int):
    skip_count = 0 # Initializes the counter for skipping rows
    def warn_bad_line(msg):
        nonlocal skip_count
        skip_count += 1  # 对跳过的行进行计数
    try:
        data = pd.read_csv(file_path, encoding='GBK',on_bad_lines=warn_bad_line,engine='python')

        # Function to apply
        def apply_check_and_extract(row):
            if isinstance(row['name'], str):
                result = check_and_extract(row, placesymbol_dict)
                return pd.Series(result) if result is not None else pd.Series([None]*5)
            else:
                return pd.Series([None]*5)

        results = data.apply(apply_check_and_extract, axis=1, result_type='expand')
        results.columns = ['name', '_id', 'adcode', 'placesymbol', 'placecode']
        results.dropna(inplace=True)

        for start_idx in range(0, len(results), max_rows_per_file):
            output_file = os.path.join(output_dir, f'output_{os.path.basename(file_path).split(".")[0]}_{start_idx//max_rows_per_file + 1}.csv')
            results.iloc[start_idx:start_idx + max_rows_per_file].to_csv(output_file, index=False, header=True, encoding='utf-8')
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
    # Print the number of lines skipped
    if skip_count > 0:
        print(f"Skipped {skip_count} bad lines in file {file_path}")

def batch_process_csv(file_paths: List[str], batch_size: int, placesymbol_dict: Dict[str, int], output_dir: str, max_rows_per_file: int):
    for i in range(0, len(file_paths), batch_size):
        batch = file_paths[i:i + batch_size]
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(batch_size, os.cpu_count())) as executor:
            futures = [executor.submit(process_csv, file_path, placesymbol_dict, output_dir, max_rows_per_file) for file_path in batch]
            concurrent.futures.wait(futures)

        gc.collect()

# Load placesymbol_code.csv and create a dictionary
placesymbol_code_file_path = 'data/output/placesymbol_code.csv'  # Update with actual path
placesymbol_code_df = pd.read_csv(placesymbol_code_file_path)
placesymbol_dict = placesymbol_code_df.set_index('placesymbol')['placecode'].to_dict()

# Directory where the CSV files are located
csv_directory = r'C:\Users\jsj\Downloads\2018-POICSV-3'  # Update with the actual directory path
output_dir = 'data/output/extractresult'  # Update with the actual output directory path

# List of file paths to process
file_paths = glob.glob(os.path.join(csv_directory, '*.csv'))

# Process the files in batches
batch_size = 5  # Adjust based on your system's capability
max_rows_per_file = 1000000  # One million rows per file

batch_process_csv(file_paths, batch_size, placesymbol_dict, output_dir, max_rows_per_file)

end = time.time()
print(f"Total time: {end - starttime} seconds")
