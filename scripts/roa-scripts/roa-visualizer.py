import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import os

def compute_intervals(example_df):
    # Given per-day ROA snapshot rows, compute continuous intervals per ASN
    example_df = example_df.sort_values("snapshot_date")
    intervals = []
    for asn, group in example_df.groupby("asn"):
        group = group.sort_values("snapshot_date")

        start = group['snapshot_date'].iloc[0]
        prev = start

        for current in group['snapshot_date'].iloc[1:]:
            if (current - prev).days > 1:
                intervals.append((asn, start, prev))
                start = current
            prev = current
        intervals.append((asn, start, prev))

    return pd.DataFrame(intervals, columns=["asn", "start", "end"])

def timeline_plot(df, output_dir, highlight_asn=834):
    print("\n ** Generating ROA timeline with merged intervals")

    unique_pairs = df[['prefix', 'asn']].drop_duplicates()
    most_common_prefix = unique_pairs['prefix'].value_counts().index[0]
    example_df = df[df['prefix'] == most_common_prefix].copy()

    # Ensure datetime
    example_df['snapshot_date'] = pd.to_datetime(example_df['snapshot_date'])

    intervals = compute_intervals(example_df)
    print(f"\n--- Computed Intervals for {most_common_prefix} ---")
    print(intervals.sort_values("start").to_string(index=False)) 
    print("---------------------------------------------------\n")

    plt.figure(figsize=(16, 9))
    ax = plt.gca()

    # Color palette by ASN
    asns = sorted(example_df['asn'].unique())
    palette = sns.color_palette("tab20", len(asns))
    color_map = {asn: palette[i] for i, asn in enumerate(asns)}

    for _, row in intervals.iterrows():
        ax.hlines(
            y=row['asn'],
            xmin=row['start'],
            xmax=row['end'],
            color=color_map[row['asn']],
            linewidth=6,
            alpha=0.85
        )

    sns.scatterplot(
        data=example_df,
        x='snapshot_date',
        y='asn',
        hue='asn',
        palette=color_map,
        s=10,
        alpha=0.8,
        legend=True
    )


    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gcf().autofmt_xdate()


    plt.title(f"ROA Timeline (Merged Intervals) for Prefix: {most_common_prefix}", fontsize=14)
    plt.xlabel("Snapshot Date (Daily Observed)", fontsize=12)
    plt.ylabel("Origin ASN", fontsize=12)

    plt.grid(axis='x', linestyle='--', alpha=0.4)
    plt.tight_layout()


    safe_prefix = most_common_prefix.replace('/', '_')
    out_path = os.path.join(output_dir, f"timeline_intervals_{safe_prefix}.png")
    plt.savefig(out_path, dpi=200)
    print(f" *** Saved interval timeline to: {out_path}\n")

def observed_distribution_lifetime(df, output_dir):
    print("\n ** Histogram Plot for Continuous ROA Lifetimes")

    unique_cols = ['prefix', 'asn', 'max_len']
    df_sorted = df[unique_cols + ['snapshot_date']].drop_duplicates().sort_values(by=unique_cols + ['snapshot_date'])
    print(" *** Finding continuous active blocks (gaps and islands)...")
    df_sorted['snapshot_date'] = pd.to_datetime(df_sorted['snapshot_date'])
    df_sorted['prev_date'] = df_sorted.groupby(unique_cols)['snapshot_date'].shift(1)
    df_sorted['gap'] = (df_sorted['snapshot_date'] - df_sorted['prev_date']).dt.days
    df_sorted['new_island'] = ((df_sorted['gap'] > 1) | (df_sorted['gap'].isna())).astype(int)
    df_sorted['island_id'] = df_sorted.groupby(unique_cols)['new_island'].cumsum()

    island_lifetimes = df_sorted.groupby(unique_cols + ['island_id'])['snapshot_date'].agg(['min', 'max']).reset_index()
    island_lifetimes['lifetime_days'] = (island_lifetimes['max'] - island_lifetimes['min']).dt.days + 1
    
    print(f" *** Found {len(island_lifetimes)} continuous ROA blocks.")
    print(" *** Generating plot")
    
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(15,8))
    plt.hist(island_lifetimes['lifetime_days'], bins=50, color='steelblue', edgecolor='black', alpha=0.7)
    plt.title("Distribution of *Continuous* Observed ROA Lifetimes")
    plt.xlabel("Lifetime of Continuous Block (days)")
    plt.ylabel("Number of Continuous Blocks")
    plt.grid(True, linestyle='--', alpha=0.6)
    
    plot_path = os.path.join(output_dir, 'observed_continuous_lifetime_distribution.png')
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
        df[['snapshot_date', 'uri', 'asn', 'prefix']]
        .drop_duplicates()  # ensures unique composite ROAs
        .groupby('snapshot_date')
        .size()
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

def cdf_roas_per_prefix(df, output_dir):
    print("\n ** Generating CDF for Number of ROAs per Prefix")

    # Compute number of ROAs observed per prefix
    roas_per_prefix = (
        df.groupby('prefix')['uri']
          .nunique()
          .reset_index(name='roa_count')
          .sort_values('roa_count')
    )

    print(f" *** Processed {len(roas_per_prefix)} prefixes across dataset.")
    print(" *** Median number of ROAs per prefix:",
          roas_per_prefix['roa_count'].median())

    # Compute CDF
    sorted_counts = roas_per_prefix['roa_count'].sort_values().values
    cdf = pd.Series(range(1, len(sorted_counts)+1)) / len(sorted_counts)

    plt.figure(figsize=(10, 6))
    plt.plot(sorted_counts, cdf, marker='.', linestyle='none', color='steelblue')
    plt.title("CDF of Number of ROAs per Prefix")
    plt.xlabel("Number of Distinct ROAs Observed (per prefix)")
    plt.ylabel("Cumulative Fraction of Prefixes")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "cdf_roas_per_prefix.png")
    plt.savefig(path)
    print(f" *** CDF Plot saved to {path}")

def cdf_median_roas_per_prefix(df, output_dir):
    print("\n ** Generating CDF for Median Number of ROAs per Prefix (across time)")

    roa_counts = (
        df.groupby(['snapshot_date', 'prefix'])['uri']
          .nunique()
          .reset_index(name='roa_count')
    )

    # Take median ROA count across all snapshots for each prefix
    medians = roa_counts.groupby('prefix')['roa_count'].median().reset_index()

    sorted_vals = medians['roa_count'].sort_values().values
    cdf = pd.Series(range(1, len(sorted_vals)+1)) / len(sorted_vals)

    plt.figure(figsize=(10, 6))
    plt.plot(sorted_vals, cdf, marker='.', linestyle='none', color='seagreen')
    plt.title("CDF of Median Number of ROAs per Prefix (across time)")
    plt.xlabel("Median Number of Active ROAs per Prefix")
    plt.ylabel("Cumulative Fraction of Prefixes")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "cdf_median_roas_per_prefix.png")
    plt.savefig(path)
    print(f" *** CDF Plot saved to {path}")

def avg_roa_duration_per_asn(df, output_dir):
    print("\n ** Plot: Average ROA Duration per Origin ASN")
    unique_cols = ['prefix', 'asn', 'max_len']
    lifetime_df = (
        df.groupby(unique_cols)['snapshot_date']
          .agg(['min', 'max'])
          .reset_index()
    )
    lifetime_df['min'] = pd.to_datetime(lifetime_df['min'])
    lifetime_df['max'] = pd.to_datetime(lifetime_df['max'])
    lifetime_df['lifetime_days'] = (lifetime_df['max'] - lifetime_df['min']).dt.days + 1

    avg_lifetime_per_asn = (
        lifetime_df.groupby('asn')['lifetime_days']
                   .mean()
                   .reset_index(name='avg_lifetime_days')
                   .sort_values('avg_lifetime_days', ascending=False)
    )

    print(f" *** Computed average lifetime for {len(avg_lifetime_per_asn)} ASNs.")

    # Step 3: plot
    plt.figure(figsize=(12, 6))
    plt.hist(
        avg_lifetime_per_asn['avg_lifetime_days'],
        bins=50,
        color='darkorange',
        edgecolor='black',
        alpha=0.7
    )
    plt.title("Distribution of Average ROA Duration per Origin ASN")
    plt.xlabel("Average ROA Lifetime (days)")
    plt.ylabel("Number of Origin ASNs")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "avg_roa_duration_per_asn.png")
    plt.savefig(path)
    print(f" *** Plot saved to {path}")


def main(history_file, output_dir, event_csv):
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

    # event_csv = "/Users/rakshita/Desktop/gatech/fall25/8903/code/year-long/final/ipxo_roa_event_details_834.csv"

    # Plots
    # scatter_plot_for_prefix(df, output_dir)
    observed_distribution_lifetime(df, output_dir)
    unique_asns_per_prefix(df, output_dir)
    unique_roas_over_time(df, output_dir)
    plot_churn_timeline_from_events(event_csv, output_dir)
    cdf_roas_per_prefix(df, output_dir)
    cdf_median_roas_per_prefix(df, output_dir)
    avg_roa_duration_per_asn(df, output_dir)
    timeline_plot(df, output_dir)

    print("\nAnalysis complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze and visualize ROA history.")
    
    parser.add_argument(
        '--history_file', 
        type=str, 
        required=True,
        help="Path to the Parquet file from 'roa-collection-prefix-match' (with all records)."
    )

    parser.add_argument(
        '--event_file', 
        type=str, 
        required=True,
        help="Event file having all IPXO related ROA dated."
    )
    
    parser.add_argument(
        '--output_dir', 
        type=str, 
        required=True,
        help="Directory to save the output .png plot files."
    )

    args = parser.parse_args()
    main(args.history_file, args.output_dir, args.event_file)