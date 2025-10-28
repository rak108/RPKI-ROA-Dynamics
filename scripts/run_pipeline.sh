#!/bin/bash
#
# This script runs the RPKI data pipeline:
# 1. Downloads X amount of ROA data.
# 2. Parses the data into a single Parquet file (and cleans up the zip files).

# Python Scripts Path
DOWNLOADER_SCRIPT="python-scripts/roa-csv-fetch.py"
PARSER_SCRIPT="python-scripts/roa-csv-parser.py"

# Timeframe to be downloaded: can specify month, date and year.
TARGET_YEAR=2025

# Temp Download Directory
DOWNLOAD_DIR="./zip_downloads"

# Where the Singular Parquet file needs to be saved
OUTPUT_DIR="./output"
OUTPUT_FILENAME="all_roas_${TARGET_YEAR}"

set -e

echo "\n============= [Step 1: Downloading RPKI Data for ${TARGET_YEAR}] =============="

python3 ${DOWNLOADER_SCRIPT} \
    --year ${TARGET_YEAR} \
    --dir ${DOWNLOAD_DIR} \
    --repo ripencc.tal

echo "\n==================== [Step 2: Parsing data into Parquet] ======================="

python3 ${PARSER_SCRIPT} \
    --dir ${DOWNLOAD_DIR} \
    --output_dir ${OUTPUT_DIR} \
    --output_filename ${OUTPUT_FILENAME} \
    --output_type parquet \
    --clean

echo "\n==================== [Step 3: Cleaning up temp directory] ====================="

rm -rf ${DOWNLOAD_DIR}
echo "Successfully deleted temporary directory: ${DOWNLOAD_DIR}"


echo "Pipeline finished successfully! Output file: ${OUTPUT_DIR}/${OUTPUT_FILENAME}.parquet"