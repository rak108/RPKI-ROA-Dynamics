# This file unzips and parses from ROA CSVs and stores them in a CSV/Parquet file.

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import glob
import os
import argparse
import lzma

considered_columns = {
    'URI': 'uri',
    'ASN': 'asn',
    'IP Prefix': 'prefix',
    'Max Length': 'max_len',
    'Not Before': 'not_before',
    'Not After': 'not_after'
}

def parse_csvs_and_save(zips, output_dir, output_filename, output_type, clean):

    output_filename = output_filename + "." + output_type
    output_filepath = os.path.join(output_dir, output_filename)

    final_data = 0
    os.makedirs(output_dir, exist_ok=True)
    writer = None
    processed_files = []
    for zip in zips:
        print(f" ** Processing {zip}")
        try:
            base_name = os.path.basename(zip)
            date_part = base_name.split('_')[0]
            snapshot_date = pd.to_datetime(date_part, format='%Y%m%d')
            # Open the compressed file in text-reading mode ('rt')
            with lzma.open(zip, 'rt') as csv:
                for chunk in pd.read_csv(csv, usecols=considered_columns.keys(), chunksize=100000):
                    chunk = chunk.rename(columns=considered_columns)
                    chunk["snapshot_date"] = snapshot_date
                    table = pa.Table.from_pandas(chunk, preserve_index=False)
                    if writer is None:
                        writer = pq.ParquetWriter(output_filepath, table.schema)
                    writer.write_table(table)
                    final_data += len(chunk)
            print(f" ** Processed {zip}")
            processed_files.append(zip)
        except (lzma.LZMAError, pd.errors.EmptyDataError, EOFError) as e:
            print(f"!!ERROR: Failed to process {zip}. Error: {e}. Skipping file.")
        except Exception as e:
            print(f"!!ERROR: An unexpected error occurred with {zip}. Error: {e}. Skipping file.")

    if writer:
        writer.close()
        print(f"\nCompleted parsing and combined {final_data} records. Saved the parsed data to {output_filepath}.\n")
        if clean:
            print("Cleaning up original .csv.xz files.")
            for f_to_delete in processed_files:
                try:
                    os.remove(f_to_delete)
                    print(f" * Deleted {f_to_delete}")
                except OSError as e:
                    print(f"!!ERROR: Could not delete {f_to_delete}. Error: {e}")
        else:
            print("Original .csv.xz files were not deleted as per --clean flag.")
    
    else:
        print("\nNo data was written (writer was not initialized). No files will be deleted.")


def main(file_directory, file_name, output_dir, output_filename, output_type, clean):

    if file_directory is None and file_name is None:
        print("!!ERROR: Neither directory nor file path specified. Try again with either one of them.")
    if output_type not in ['csv', 'parquet'] :
        print("!!ERROR: Invalid output type. Please try again.")
        print(output_type)
        return
    
    os.makedirs(output_dir, exist_ok=True)
        
    print("\n*************************************************************************************")
    print("\n--------------------------- RPKI ROA File Parser ----------------------------------")
    print("\n*************************************************************************************")

    zips = []

    if file_name is not None:
        print(f"\nStarting ROAs parsing from zip {file_name}")
        zips = glob.glob(file_name) # TODO
    else:
        print(f"\nStarting ROAs unzipping and parsing from ZIPs in {file_directory}\n")
        zips = glob.glob(os.path.join(file_directory, "*.csv.xz"))
        if len(zips) == 0:
            print(f"\nFound no records in {file_directory}. Check if the directory is correct")
            return
        print(f" * In {file_directory}, found {len(zips)} files. Parsing now\n")
    
    
    parse_csvs_and_save(zips,output_dir,output_filename,output_type, clean)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parsing RPKI ROA files.")
    
    parser.add_argument(
        '--dir', 
        type=str, 
        default=None,
        help="The directory which contains the Zip files to be parsed."
    )

    parser.add_argument(
        '--file_path', 
        type=str, 
        default=None,
        help="The file path of the Zip file to be parsed."
    )

    parser.add_argument(
        '--output_dir', 
        type=str, 
        required=True,
        help="The output directory of the parsed records."
    )

    parser.add_argument(
        '--output_filename', 
        type=str, 
        default='all_roas_data_pd',
        help="The output file name of the parsed files (no need of extension)."
    )

    parser.add_argument(
        '--output_type', 
        type=str, 
        default='parquet',
        help="The output type of the file. Possible options are csv and parquet."
    )

    parser.add_argument(
        '--clean',
        action='store_true',
        help="If set, it deletes the original .csv.xz files after successful parsing."
    )

    args = parser.parse_args()
    main(args.dir, args.file_path, args.output_dir, args.output_filename, args.output_type, args.clean)
