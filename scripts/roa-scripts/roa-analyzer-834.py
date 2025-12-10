# This file logs events on prefixes considering only ASN 834.

import argparse
import pandas as pd
import os

# SUMMARY_CSV = '/Users/rakshita/Desktop/gatech/fall25/8903/code/output/final1/ipxo_roa_event_summary_834.csv'
# DETAIL_CSV = '/Users/rakshita/Desktop/gatech/fall25/8903/code/output/final1/ipxo_roa_event_details_834.csv'

IPXO_ASN = 'AS834'
IPXO_REPO_URI = 'r.magellan.ipxo.com'

def main(input_file, summary_file, event_file):
    print("\n*************************************************************************************")
    print("\n-------------------------- RPKI ROA IPXO ANALYSIS - ASN 834 -------------------------")
    print("\n*************************************************************************************")

    try:
        print("Fetching date range.")
        all_dates = pd.read_parquet(input_file, columns=['snapshot_date'])['snapshot_date'].drop_duplicates()
        sorted_dates = sorted(pd.to_datetime(all_dates).dt.date)
        print(f" * Found {len(sorted_dates)} snapshot days to process.")
    except Exception as e:
        print(f"!!ERROR: Could not read the input file '{input_file}' to get dates.")
        print(e)
        return

    daily_count = []
    detailed_log = []

    try:
        print(f" * Loading initial data for {sorted_dates[0]}")
        prev_date = sorted_dates[0]
        prev_date_roas = pd.read_parquet(input_file, filters=[('snapshot_date', '=', prev_date)])
        prev_asns_map = prev_date_roas.groupby('prefix')['asn'].apply(set)

        all_ipxo_prefixes = set(prev_date_roas[prev_date_roas['asn'] == IPXO_ASN]['prefix'])
    except Exception as e:
        print(f"!!ERROR: Could not load initial data for {sorted_dates[0]}. {e}")
        return
    
    for i in range(1, len(sorted_dates)):
        current_date = sorted_dates[i]
        print(f" ** For {sorted_dates[i]} ")
        
        try:
            curr_date_roas = pd.read_parquet(input_file, filters=[('snapshot_date', '=', current_date)])
        except Exception as e:
            print(f"!!ERROR: Could not load data for {current_date}. Skipping day. {e}")
            continue

        curr_asns_map = curr_date_roas.groupby('prefix')['asn'].apply(set)
        all_ipxo_prefixes.update(set(curr_date_roas[curr_date_roas['asn'] == IPXO_ASN]['prefix']))
        all_prefixes = set(prev_asns_map.index).union(set(curr_asns_map.index))
        
        creations, deletions, updates_to_ipxo, updates_from_ipxo = set(), set(), set(), set()


        for prefix in all_prefixes:
            # print(prefix)
            prev_date_asns = prev_asns_map.get(prefix, set())
            curr_date_asns = curr_asns_map.get(prefix, set())

            # CREATION: prefix newly appeared with AS834 (none before)
            if IPXO_ASN in curr_date_asns and len(prev_date_asns) == 0:
                creations.add(prefix)
                detailed_log.append({
                    'date': current_date,
                    'prefix': prefix,
                    'event': 'creation',
                    'prev_date_asns': list(prev_date_asns),
                    'curr_date_asns': list(curr_date_asns)
                })

            # DELETION: prefix had AS834, now disappeared
            elif IPXO_ASN in prev_date_asns and len(curr_date_asns) == 0:
                deletions.add(prefix)
                detailed_log.append({
                    'date': current_date,
                    'prefix': prefix,
                    'event': 'deletion',
                    'prev_date_asns': list(prev_date_asns),
                    'curr_date_asns': list(curr_date_asns)
                })

            else:
                # UPDATE TO IPXO: switched from another ASN to AS834
                if IPXO_ASN in curr_date_asns and IPXO_ASN not in prev_date_asns and len(prev_date_asns) > 0:
                    updates_to_ipxo.add(prefix)
                    detailed_log.append({
                        'date': current_date,
                        'prefix': prefix,
                        'event': 'update_to_AS834',
                        'prev_date_asns': list(prev_date_asns),
                        'curr_date_asns': list(curr_date_asns)
                    })

                # UPDATE FROM IPXO: switched from AS834 to another ASN
                if IPXO_ASN in prev_date_asns and IPXO_ASN not in curr_date_asns and len(curr_date_asns) > 0:
                    updates_from_ipxo.add(prefix)
                    detailed_log.append({
                        'date': current_date,
                        'prefix': prefix,
                        'event': 'update_from_AS834',
                        'prev_date_asns': list(prev_date_asns),
                        'curr_date_asns': list(curr_date_asns)
                    })

        daily_count.append({
            'date': current_date,
            'creations': len(creations),
            'deletions': len(deletions),
            'updates_to_AS834': len(updates_to_ipxo),
            'updates_from_AS834': len(updates_from_ipxo)
        })

        print(f" *** +{len(creations)}, -{len(deletions)}, to->{len(updates_to_ipxo)}, from<-{len(updates_from_ipxo)}")
        prev_asns_map = curr_asns_map

    summary_df = pd.DataFrame(daily_count)
    details_df = pd.DataFrame(detailed_log)

    output_dir = os.path.dirname(summary_file)
    os.makedirs(output_dir, exist_ok=True)
    summary_df.to_csv(summary_file, index=False)
    output_dir = os.path.dirname(event_file)
    os.makedirs(output_dir, exist_ok=True)
    details_df.to_csv(event_file, index=False)

    print(f"\nSaved summary (event count) to {summary_file}")
    print(f"Saved detailed events to {event_file}")

    print("\nFinding 'permanent' prefixes (associated with AS834 but never churned)")
    churned_prefixes = set(details_df['prefix'])
    print(f" * Found {len(churned_prefixes)} total prefixes that churned.")
    print(f" * Found {len(all_ipxo_prefixes)} total prefixes ever associated with AS834.")
    permanent_prefixes = all_ipxo_prefixes - churned_prefixes
    print(f" * Found {len(permanent_prefixes)} permanent (non-churning) prefixes.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyzing RPKI ROA CSVs - ASN 834 specific")
    
    # Output of roa-csv-parser file
    parser.add_argument(
        '--file', 
        type=str, 
        default=None,
        help="The file path of the Parquet file which is to be analyzer."
    )

    parser.add_argument(
        '--summary_output_file_path', 
        type=str, 
        default=None,
        help="The file path of the summary file to be saved."
    )

    parser.add_argument(
        '--detail_output_file_path', 
        type=str, 
        default=None,
        help="The file path of the detailed file to be saved."
    )

    args = parser.parse_args()
    main(args.file, args.summary_output_file_path, args.detail_output_file_path)

