# This file parses from CSVs and stores them in a CSV/Parquet file.

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import glob
import os
import argparse

considered_columns = {
    'URI': 'uri',
    'ASN': 'asn',
    'IP Prefix': 'prefix',
    'Max Length': 'max_len',
    'Not Before': 'not_before',
    'Not After': 'not_after'
}

def parse_csvs_and_save(csvs, output_dir, output_filename, output_type):

    output_filename = output_filename + "." + output_type
    output_filepath = os.path.join(output_dir, output_filename)

    final_data = 0
    os.makedirs(output_dir, exist_ok=True)
    writer = None
    for csv in csvs:
        data = pd.read_csv(csv)
        data = data[list(considered_columns.keys())].rename(columns=considered_columns)
        base_name = os.path.basename(csv)
        date_part = base_name.split('_')[0]
        snapshot_date = pd.to_datetime(date_part, format='%Y%m%d')
        for chunk in pd.read_csv(csv, usecols=considered_columns.keys(), chunksize=100000):
            chunk = chunk.rename(columns=considered_columns)
            chunk["snapshot_date"] = snapshot_date
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(output_filepath, table.schema)
            writer.write_table(table)
            final_data += len(chunk)
        print(f" ** Processed {csv}")

    if writer:
        writer.close()

    print(f"\nCompleted parsing and combined {final_data} records. Saved the parsed data to {output_filepath}.\n")


def main(csv_directory, csv_name, output_dir, output_filename, output_type):

    if csv_directory is None and csv_name is None:
        print("!!ERROR: Neither directory nor file path specified. Try again with either one of them.")
    if output_type not in ['csv', 'parquet'] :
        print("!!ERROR: Invalid output type. Please try again.")
        print(output_type)
        return
    
    os.makedirs(output_dir, exist_ok=True)
        
    print("\n*************************************************************************************")
    print("\n--------------------------- RPKI ROA CSV Parser ----------------------------------")
    print("\n*************************************************************************************")

    csvs = []

    if csv_name is not None:
        print(f"\nStarting ROAs parsing from CSV {csv_name}")
        csvs = glob.glob(csv_name) # TODO
    else:
        print(f"\nStarting ROAs parsing from CSVs in {csv_directory}\n")
        csvs = glob.glob(os.path.join(csv_directory, "*.csv"))
        if len(csvs) == 0:
            print(f"\nFound no records in {csv_directory}. Check if the directory is correct")
            return
        print(f" * In {csv_directory}, found {len(csvs)} files. Parsing now\n")
    
    
    parse_csvs_and_save(csvs,output_dir,output_filename,output_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parsing RPKI ROA CSVs.")
    
    parser.add_argument(
        '--dir', 
        type=str, 
        default=None,
        help="The directory which contains the CSVs to be parsed."
    )

    parser.add_argument(
        '--file_path', 
        type=str, 
        default=None,
        help="The file path of the CSV to be parsed."
    )

    parser.add_argument(
        '--output_dir', 
        type=str, 
        required=True,
        help="The output directory of the parsed CSVs."
    )

    parser.add_argument(
        '--output_filename', 
        type=str, 
        default='all_roas_data_pd',
        help="The output file name of the parsed CSVs (no need of extension)."
    )

    parser.add_argument(
        '--output_type', 
        type=str, 
        default='csv',
        help="The output type of the file. Possible options are csv and parquet."
    )

    args = parser.parse_args()
    main(args.dir, args.file_path, args.output_dir, args.output_filename, args.output_type)
