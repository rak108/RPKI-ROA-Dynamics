# This file downloads and unzips ROA CSVs for year/month/date specified.

import os
import requests
import lzma
import shutil
import calendar
import argparse
from datetime import date

roas_url = "https://ftp.ripe.net/rpki/"
rir_repos = ['ripencc.tal'] #for now only ripe

def save_roas_csv(url, output_filepath):

    zip_path = output_filepath + ".xz"
    try:
        print(f"\n ----- Downloading from {url} -----")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        with open(zip_path, 'wb') as f:
            f.write(response.content)

        print(f"\nUnzipping to {output_filepath}")
        with lzma.open(zip_path) as f_in:
            with open(output_filepath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"\n=======")

        return True

    except requests.exceptions.RequestException as e:
        print(f"!!ERROR: Failed downloading ROA CSV from {url}. Error: {e}")
        return False
    finally:
        if os.path.exists(zip_path):
            os.remove(zip_path)


def main(repos, year, month, day, directory):
        
    print("\n*************************************************************************************")
    print("\n--------------------------- RPKI ROA CSV Downloads ----------------------------------")
    print("\n*************************************************************************************")
    print(f"\nDownloading RPKI ROAs to {directory}")
    os.makedirs(directory, exist_ok=True)

    print(" * Checking which repositories to process...")

    if len(repos) == 0:
        print("\n!!ERROR: No repos passed. Please try again.")
    elif 'all' in repos:
        all_repos = repos
    else:
        all_repos = []
        for repo in repos:
            if repo in rir_repos:
                all_repos.append(repo)
            else:
                print(f"!WARNING: Unknown repo '{repo}'.")
    
    print(f" ** Selected Repositories: {', '.join(all_repos)}")
    print(f" ** Selected Year: {year}")
    if month:
        print(f" ** Target Month: {month}")
        if day:
            print(f" ** Target Day: {day}")
    if day is not None and month is None:
        print("\n!!ERROR: Target Day specified but no month specified. Please try again.")

    files_count = 0

    month_first = month if month else 1
    month_last = month if month else 12
    
    for repo in all_repos:
        for current_month in range(month_first, month_last + 1):
            valid_days = calendar.monthrange(year, current_month)[1]
            num_days = []
            if day is not None:
                if day in range(1, valid_days + 1):
                    num_days.append(int(day))
                else:
                    print("\n!!ERROR: Day does not occur in the specified month.")
            else:
                for current_day in range(1, valid_days + 1):
                    if date(year, current_month, current_day) > date.today():
                        continue
                    num_days.append(current_day)

            for current_day in num_days:
                month_str = str(current_month).zfill(2)
                day_str = str(current_day).zfill(2)

                url = f"{roas_url}{repo}/{year}/{month_str}/{day_str}/roas.csv.xz"
                output_filename = f"{year}{month_str}{day_str}_roas.csv"
                output_filepath = os.path.join(directory, output_filename)

                print(f"\n=======")
                print(f"Starting download of {repo} for {year}/{month_str}/{day_str}")

                if os.path.exists(output_filepath):
                    print(f"!WARNING: {output_filepath} already exists. Skipping.")
                    continue

                if save_roas_csv(url, output_filepath):
                    files_count += 1
                else:
                    if os.path.exists(output_filepath):
                        os.remove(output_filepath)

    print(f"\n\nDownload complete. Total ROA CSVs downloaded are {files_count}.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download RPKI ROA CSV snapshots from ftp.ripe.net.")
    
    parser.add_argument(
        '--repo', 
        nargs='+', 
        default=['ripencc.tal'],
        help=f"Specify one or more repos to download from (e.g., ripencc.tal). Use 'all' for all repos. Defaults to 'ripe.net'. Pick from: {', '.join(rir_repos)}"
    )
    parser.add_argument(
        '--year', 
        type=int, 
        required=True,
        help="The year to download data for."
    )
    parser.add_argument(
        '--month', 
        type=int, 
        default=None,
        choices=range(1, 13),
        help="Optional: The month to download data for (1-12). If omitted, the entire year will be downloaded."
    )
    parser.add_argument(
        '--day', 
        type=int, 
        default=None,
        choices=range(1, 32),
        help="Optional: The day to download data for (1-31). If omitted, the entire year will be downloaded."
    )
    parser.add_argument(
        '--dir', 
        type=str, 
        required=True,
        help="The directory where the downloaded CSV files will be saved."
    )

    args = parser.parse_args()
    main(args.repo, args.year, args.month, args.day, args.dir)
