# This file parses from CSVs and stores them in a CSV/Parquet file.

import pandas as pd
import sys


input_file = '/Users/rakshita/Desktop/gatech/fall25/8903/code/scripts/output/all_roas_data_pd.parquet'


ipxo_uri = 'rsync://r.magellan.ipxo.com/repo/'

print(f"Loading dataset from '{input_file}'")

try:
    all_df = pd.read_parquet(input_file)
    print(f"Loaded {len(all_df)} total records")
except FileNotFoundError:
    print(f"!!ERROR: The file '{input_file}' was not found.")
    sys.exit()

print(f"\nFiltering records for URI: '{ipxo_uri}'")
ipxo_df = all_df[all_df['uri'].str.contains(ipxo_uri, na=False)]
if ipxo_df.empty:
    print("\nNo records found matching that URI.")
else:
    print(f"\nFound {len(ipxo_df)} records matching the IPXO URI.")
    print("First 5 matching rows:")
    print(ipxo_df.head())
