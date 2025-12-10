#!/bin/bash
#
# This script runs the RPKI data pipeline:
# 1. Downloads X amount of ROA data.
# 2. Parses the data into a single Parquet file (and cleans up the zip files).
# 3. The parquet file is read to create an event file of all ROA updates around IPXO
# 4. The event file is read to extract prefixes associated to IPXO, and then all ROAs are fetched for these prefixes from the main file.

# Python Scripts Path
DOWNLOADER_SCRIPT="roa-scripts/roa-csv-fetch.py"
PARSER_SCRIPT="roa-scripts/roa-csv-parser.py"
IPXO_SCRIPT="roa-scripts/roa-analyzer-834.py"
PREFIX_SCRIPT="roa-scripts/roa-collection-prefix-match.py"

# Timeframe to be downloaded: can specify month, date and year.
TARGET_YEAR=2025

# Temp Download Directory
DOWNLOAD_DIR="./zip_downloads"

# Where the Singular Parquet file needs to be saved
OUTPUT_DIR="./output"
OUTPUT_FILENAME="all_roas_${TARGET_YEAR}"
OUTPUT_PATH="${OUTPUT_DIR}/${OUTPUT_FILENAME}.parquet"

# Output of 834
EVENT_PATH="./output/event_details.csv"
SUMMARY_PATH="./output/summary_details.csv"

IPXO_PARQUET="./output/ipxo_roas_${TARGET_YEAR}.parquet"

set -e

echo "\n============= [Step 1: Downloading RPKI Data for ${TARGET_YEAR}] =============="

python3 ${DOWNLOADER_SCRIPT} \
    --year ${TARGET_YEAR} \
    --month 12 \
    --dir ${DOWNLOAD_DIR} \
    --repo ripencc.tal

echo "\n==================== [Step 2: Parsing data into Parquet] ======================="

python3 ${PARSER_SCRIPT} \
    --dir ${DOWNLOAD_DIR} \
    --output_dir ${OUTPUT_DIR} \
    --output_filename ${OUTPUT_FILENAME} \
    --output_type parquet \
    --clean

rm -rf ${DOWNLOAD_DIR}
echo "Successfully deleted temporary directory: ${DOWNLOAD_DIR}"

echo "\n==================== [Step 3: Fetching all events associated to IPXO] ====================="

python3 ${IPXO_SCRIPT} \
    --file ${OUTPUT_PATH} \
    --summary_output_file_path ${SUMMARY_PATH} \
    --detail_output_file_path ${EVENT_PATH}

echo "\n==================== [Step 4: Fetching all prefixes ever associated to IPXO] ====================="

python3 ${PREFIX_SCRIPT} \
    --prefix_details ${EVENT_PATH} \
    --data_file ${OUTPUT_PATH} \
    --output_file ${IPXO_PARQUET}


echo "Pipeline finished successfully!"