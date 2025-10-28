# Fetches all ROAs associated to prefixes once associated to ASN 834 or Magellan Repo.

import argparse
import pandas as pd
import os

def main(prefix_details, fdata_file, output_file):
    print("\n*************************************************************************************")
    print("\n------------------- RPKI ROA CHURNED PREFIX HISTORY EXTRACTOR ----------------------")
    print("\n*************************************************************************************")

    print(f"Loading churned prefix list from: {prefix_details}")
    try:
        details_df = pd.read_csv(prefix_details)
        churned_prefixes = set(details_df['prefix'])
        print(f" * Found {len(churned_prefixes)} unique prefixes that churned.")
    except Exception as e:
        print(f"!!ERROR: Could not read the detail file '{prefix_details}'.")
        print(e)
        return

    print(f"Loading full dataset from: {fdata_file}")
    try:
        df = pd.read_parquet(fdata_file)
        print(f" * Successfully loaded {len(df):,} total ROA records.")
    except Exception as e:
        print(f"!!ERROR: Could not read the full data file '{fdata_file}'.")
        print(e)
        return

    print(f"\nFiltering for the {len(churned_prefixes)} churned prefixes.")
    history_df = df[df['prefix'].isin(churned_prefixes)].copy()
    print(f" * Found {len(history_df):,} total ROA records for all churned prefixes.")
    unique_cols = ['prefix', 'asn', 'max_len', 'not_before', 'not_after']
    unique_history_df = history_df.drop_duplicates(subset=unique_cols)
    print(f" ** Found {len(unique_history_df):,} total unique ROA records for all churned prefixes.")

    try:
        output_dir = os.path.dirname(output_file)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        history_df.to_parquet(output_file, index=False)
        print(f"\nSuccessfully saved churned prefix history to: {output_file}\n")
    except Exception as e:
        print(f"!!ERROR: Could not save the output file '{output_file}'.")
        print(e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract and save full ROA history for churned prefixes.")
    
    parser.add_argument(
        '--prefix_details', 
        type=str, 
        required=True,
        help="Path to the CSV (TBD) details file having churned prefix (output of roa-analyzer-*.py)."
    )
    
    parser.add_argument(
        '--data_file', 
        type=str, 
        required=True,
        help="Path to the parquet file having the complete record of all ROAs."
    )
    
    parser.add_argument(
        '--output_file', 
        type=str, 
        required=True,
        help="Path for the output Parquet file having all ROA associated to the churned prefix."
    )

    args = parser.parse_args()
    main(args.prefix_details, args.data_file, args.output_file)