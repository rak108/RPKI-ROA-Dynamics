# This file logs events on prefixes considering only Magellan IPXO.

import argparse
import pandas as pd

SUMMARY_CSV = '/Users/rakshita/Desktop/gatech/fall25/8903/code/output/ipxo_roa_event_summary_uri.csv'
DETAIL_CSV = '/Users/rakshita/Desktop/gatech/fall25/8903/code/output/ipxo_roa_event_details_uri.csv'

IPXO_REPO_URI = 'rsync://r.magellan.ipxo.com'

def is_in_ipxo_repo(uri_set):
    if not uri_set:
        return False
    for uri in uri_set:
        if isinstance(uri, str) and uri.startswith(IPXO_REPO_URI):
            return True
    return False

def main(input_file):
    print("\n*************************************************************************************")
    print("\n----------------- RPKI ROA IPXO ANALYSIS - IPXO Magellan Repo URI ------------------")
    print("\n*************************************************************************************")
    print(f"\nLoading data from {input_file}...")
    try:
        df = pd.read_parquet(input_file)
        print(f" * Successfully loaded {len(df):,} total ROA records.")
    except Exception as e:
        print(f"!!ERROR: Could not read the input file '{input_file}'. Please check the path.")
        print(e)
        return
    
    df['snapshot_date'] = pd.to_datetime(df['snapshot_date']).dt.date
    sorted_dates = sorted(df['snapshot_date'].unique())
    print(f" * Loaded {len(sorted_dates)} snapshot days.")

    daily_count = []
    detailed_log = []

    for i in range(1, len(sorted_dates)):
        print(f" ** For {sorted_dates[i]} ")
        
        prev_date = sorted_dates[i - 1]
        current_date = sorted_dates[i]
        prev_date_roas = df[df['snapshot_date'] == prev_date]
        curr_date_roas = df[df['snapshot_date'] == current_date]
        prev_date_prefix = set(prev_date_roas['prefix'])
        curr_date_prefix = set(curr_date_roas['prefix'])
        all_prefixes = prev_date_prefix.union(curr_date_prefix)
        prev_uri_map = prev_date_roas.groupby('prefix')['uri'].apply(set)
        curr_uri_map = curr_date_roas.groupby('prefix')['uri'].apply(set)

        creations, deletions, updates_to_ipxo, updates_from_ipxo = set(), set(), set(), set()

        for prefix in all_prefixes:
            # print(prefix)
            prev_date_uris = prev_uri_map.get(prefix, set())
            curr_date_uris = curr_uri_map.get(prefix, set())

            in_ipxo_prev = is_in_ipxo_repo(prev_date_uris)
            in_ipxo_curr = is_in_ipxo_repo(curr_date_uris)

            # CREATION: prefix newly appeared with Magellan URI (none before)
            if in_ipxo_curr and len(prev_date_uris) == 0:
                creations.add(prefix)
                detailed_log.append({
                    'date': current_date,
                    'prefix': prefix,
                    'event': 'creation',
                    'prev_date_uris': list(prev_date_uris),
                    'curr_date_uris': list(curr_date_uris)
                })

            # DELETION: prefix had Magellan URI, now disappeared
            elif in_ipxo_prev and len(curr_date_uris) == 0:
                deletions.add(prefix)
                detailed_log.append({
                    'date': current_date,
                    'prefix': prefix,
                    'event': 'deletion',
                    'prev_date_uris': list(prev_date_uris),
                    'curr_date_uris': list(curr_date_uris)
                })

            else:
                # UPDATE TO IPXO: switched from another URI to Magellan URI
                if in_ipxo_curr and not in_ipxo_prev and len(prev_date_uris) > 0:
                    updates_to_ipxo.add(prefix)
                    detailed_log.append({
                        'date': current_date,
                        'prefix': prefix,
                        'event': 'update_to_AS834',
                        'prev_date_uris': list(prev_date_uris),
                        'curr_date_uris': list(curr_date_uris)
                    })

                # UPDATE FROM IPXO: switched from Magellan URI to another URI
                if in_ipxo_prev and not in_ipxo_curr and len(curr_date_uris) > 0:
                    updates_from_ipxo.add(prefix)
                    detailed_log.append({
                        'date': current_date,
                        'prefix': prefix,
                        'event': 'update_from_AS834',
                        'prev_date_uris': list(prev_date_uris),
                        'curr_date_uris': list(curr_date_uris)
                    })

        daily_count.append({
            'date': current_date,
            'creations': len(creations),
            'deletions': len(deletions),
            'updates_to_Magellan_URI': len(updates_to_ipxo),
            'updates_from_Magellan_URI': len(updates_from_ipxo)
        })

        print(f" *** +{len(creations)}, -{len(deletions)}, to->{len(updates_to_ipxo)}, from<-{len(updates_from_ipxo)}")

    summary_df = pd.DataFrame(daily_count)
    details_df = pd.DataFrame(detailed_log)
    summary_df.to_csv(SUMMARY_CSV, index=False)
    details_df.to_csv(DETAIL_CSV, index=False)

    print(f"\nSaved summary (event count) to {SUMMARY_CSV}")
    print(f"Saved detailed events to {DETAIL_CSV}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyzing RPKI ROA CSVs - IPXO Magellan Repo specific")
    

    parser.add_argument(
        '--file', 
        type=str, 
        default=None,
        help="The file path of the Parquet file which is to be analyzer."
    )

    args = parser.parse_args()
    main(args.file)

