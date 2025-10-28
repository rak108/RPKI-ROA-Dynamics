import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import os

def scatter_plot_for_prefix(df, output_dir):
    print("\n ** Printing Scatter Plot for ROAs showing ASN & IP Prefix Relationship")
    unique_pairs = df[['prefix', 'asn']].drop_duplicates()
    print(f" *** Found {len(unique_pairs)} unique prefix-ASN associations.")
    asn_counts = unique_pairs['prefix'].value_counts()
    best_prefix = asn_counts.index[0]
    example_df = df[df['prefix'] == best_prefix].copy()
    print(" *** Generating plot...")
    plt.figure(figsize=(15, 8))
    sns.scatterplot(
        data=example_df,
        x='snapshot_date',
        y='asn',
        hue='asn',
        s=100
    )
    ax = plt.gca()
    ax.set_yticklabels(ax.get_yticklabels())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gcf().autofmt_xdate()
    
    plt.title(f"Observed ROA Timeline for: {best_prefix}")
    plt.xlabel('Snapshot Date (Day Observed)')
    plt.ylabel('Origin ASN')
    plt.legend(title='ASNs', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(axis='x', linestyle='--', alpha=0.6)
    plt.tight_layout()

    safe_prefix_name = best_prefix.replace('/', '_')
    timeline_plot_path = os.path.join(output_dir, f'snapshot_timeline_{safe_prefix_name}.png')
    plt.savefig(timeline_plot_path)
    print(" *** Plot Generated!")

def observed_distribution_lifetime(df, output_dir):
    print("\n ** Histogram Plot for lifetime duration of ROAs")
    unique_cols = ['prefix', 'asn', 'max_len', 'not_before', 'not_after']
    lifetime_df = (df.groupby(unique_cols)['snapshot_date'].agg(['min', 'max']).reset_index())
    lifetime_df['min'] = pd.to_datetime(lifetime_df['min'])
    lifetime_df['max'] = pd.to_datetime(lifetime_df['max'])
    lifetime_df['lifetime_days'] = (lifetime_df['max'] - lifetime_df['min']).dt.days + 1

    print(f" *** Found {len(lifetime_df)} distinct ROA configurations.")
    print(" *** Generating plot")
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(15,8))
    plt.hist(lifetime_df['lifetime_days'], bins=50, color='steelblue', edgecolor='black', alpha=0.7)
    plt.title("Distribution of ROA Lifetimes (Derived from Snapshot Dates)")
    plt.xlabel("Lifetime (days between first and last seen)")
    plt.ylabel("Number of ROAs")
    plt.grid(True, linestyle='--', alpha=0.6)
    
    plot_path = os.path.join(output_dir, 'observed_lifetime_distribution.png')
    plt.savefig(plot_path)
    print(" *** Plot Generated!")

def unique_asns_per_prefix(df, output_dir):
    print("\n ** Histogram Plot for lifetime duration of ROAs")
    asn_per_prefix = (
        df.groupby('prefix')['asn']
          .nunique()
          .reset_index(name='asn_count')
          .sort_values('asn_count', ascending=False)
    )

    print(f" *** Processed {len(asn_per_prefix)} unique prefixes.")

    # Histogram plot
    plt.figure(figsize=(12, 6))
    plt.hist(asn_per_prefix['asn_count'], bins=range(1, asn_per_prefix['asn_count'].max() + 2),
             color='steelblue', edgecolor='black', alpha=0.7)
    plt.title("Distribution of Unique Origin ASNs per Prefix")
    plt.xlabel("Number of Distinct ASNs Observed")
    plt.ylabel("Count of Prefixes")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "unique_asns_per_prefix.png")
    plt.savefig(path)
    print(" *** Plot Generated!")

def unique_roas_over_time(df, output_dir):
    active_counts = (
        df.groupby('snapshot_date')['uri']
          .nunique()
          .reset_index(name='distinct_roas')
          .sort_values('snapshot_date')
    )

    plt.figure(figsize=(12, 6))
    plt.plot(active_counts['snapshot_date'], active_counts['distinct_roas'],
             marker='o', color='steelblue')
    plt.title("Total Active ROAs Over Time")
    plt.xlabel("Snapshot Date")
    plt.ylabel("Number of Distinct ROAs")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "unique_roas_over_time.png")
    plt.savefig(path)
    print(f" *** Plot Generated!")

def plot_churn_timeline_from_events(event_csv, output_dir):
    print(" ** Plotting Churn Timeline from Event Log")

    event_df = pd.read_csv(event_csv)

    event_df['date'] = pd.to_datetime(event_df['date'], errors='coerce')
    churn_summary = (
        event_df.groupby(['date', 'event'])
                .size()
                .unstack(fill_value=0)
                .sort_index()
    )

    plt.figure(figsize=(14, 7))
    for col in churn_summary.columns:
        plt.plot(churn_summary.index, churn_summary[col], marker='o', label=col)

    plt.title("ROA Churn Events Over Time")
    plt.xlabel("Date")
    plt.ylabel("Number of Events")
    plt.legend(title="Event Type", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "churn_timeline_from_events.png")
    plt.savefig(path)
    print(f" *** Plot saved to {path}")


def main(history_file, output_dir):
    print("\n*************************************************************************************")
    print("\n-------------------------- RPKI ROA HISTORY VISUALIZER ----------------------------")
    print("\n*************************************************************************************")
    print(f"Loading data from: {history_file}")
    try:
        df = pd.read_parquet(history_file)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date']).dt.date
        print(f" * Successfully loaded {len(df):,} total historical records.")
    except Exception as e:
        print(f"!!ERROR: Could not read the history file '{history_file}'.")
        print(e)
        return
    
    os.makedirs(output_dir, exist_ok=True)
    print(f" * Plots will be saved to: {output_dir}")

    event_csv = "/Users/rakshita/Desktop/gatech/fall25/8903/code/output/ipxo_roa_event_details_834.csv"

    # Plots
    scatter_plot_for_prefix(df, output_dir)
    observed_distribution_lifetime(df, output_dir)
    unique_asns_per_prefix(df, output_dir)
    unique_roas_over_time(df, output_dir)
    plot_churn_timeline_from_events(event_csv, output_dir)

    print("\nAnalysis complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze and visualize ROA history.")
    
    parser.add_argument(
        '--history_file', 
        type=str, 
        required=True,
        help="Path to the Parquet file from 'extract_churn_history.py' (with all records)."
    )
    
    parser.add_argument(
        '--output_dir', 
        type=str, 
        required=True,
        help="Directory to save the output .png plot files."
    )

    args = parser.parse_args()
    main(args.history_file, args.output_dir)