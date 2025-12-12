import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import os
from matplotlib.backends.backend_pdf import PdfPages

START_DATE = pd.Timestamp("2024-09-01")
END_DATE = pd.Timestamp("2025-09-23")

def compute_intervals(df):

    if df.empty:
        return pd.DataFrame(columns=["asn", "start", "end"])

    all_intervals = []
    
    for asn, group in df.groupby('asn'):
        group = group.sort_values('snapshot_date')
        
        group['prev_date'] = group['snapshot_date'].shift(1)
        group['gap'] = (group['snapshot_date'] - group['prev_date']).dt.days
        
        group['new_island'] = ((group['gap'] > 1) | (group['gap'].isna())).astype(int)
        group['island_id'] = group['new_island'].cumsum()
        
        intervals = group.groupby(['island_id'])['snapshot_date'].agg(['min', 'max']).reset_index()
        intervals['asn'] = asn
        intervals.rename(columns={'min': 'start', 'max': 'end'}, inplace=True)
        all_intervals.append(intervals)
    
    if all_intervals:
        return pd.concat(all_intervals)[['asn', 'start', 'end']]
    else:
        return pd.DataFrame(columns=["asn", "start", "end"])

def plot_prefix_timeline(df, prefix, ax):
    subset = df[df['prefix'] == prefix].copy()
    subset['snapshot_date'] = pd.to_datetime(subset['snapshot_date'])

    asns = sorted(subset['asn'].unique())
    
    y_map = {asn: i for i, asn in enumerate(asns)}
    
    intervals = compute_intervals(subset)

    # Colors
    palette = sns.color_palette("tab10", len(asns)) 
    color_map = {asn: palette[i % len(palette)] for i, asn in enumerate(asns)}

    for _, row in intervals.iterrows():
        ax.hlines(
            y=y_map[row['asn']], # Use mapped index
            xmin=row['start'],
            xmax=row['end'],
            color=color_map[row['asn']],
            linewidth=10, 
            alpha=0.85
        )

    subset['y_index'] = subset['asn'].map(y_map)
    sns.scatterplot(
        data=subset,
        x='snapshot_date',
        y='y_index',
        hue='asn',
        palette=color_map,
        s=15,
        alpha=0.9,
        ax=ax,
        legend=False,
        edgecolor=None
    )

    ax.set_xlim(START_DATE, END_DATE)
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    
    ax.set_yticks(range(len(asns)))
    ax.set_yticklabels(asns)

    ax.set_title(f"Prefix: {prefix}", fontsize=12, fontweight='bold')
    ax.set_ylabel("Origin ASN")
    
    ax.grid(axis='x', linestyle='--', alpha=0.4)
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right')


def generate_timeline_pdf(df, output_pdf, per_page=3):
    prefixes = df['prefix'].unique()
    print(f"\nFound {len(prefixes)} unique prefixes.")
    os.makedirs(os.path.dirname(output_pdf), exist_ok=True)

    with PdfPages(output_pdf) as pdf:
        for i in range(0, len(prefixes), per_page):
            batch = prefixes[i:i+per_page]
            n_plots = len(batch)
            
            fig, axes = plt.subplots(
                per_page, 1, 
                figsize=(14, 4 * per_page),
                constrained_layout=True
            )

            if per_page == 1:
                axes = [axes]
            
            if n_plots < per_page:
                for j in range(n_plots, per_page):
                    axes[j].axis('off')

            for j, prefix in enumerate(batch):
                print(f"Generating timeline for {prefix}...")
                plot_prefix_timeline(df, prefix, axes[j])

            fig.suptitle(f"ROA Timelines ({START_DATE.date()} to {END_DATE.date()})", fontsize=16, y=1.02)
            pdf.savefig(fig, bbox_inches='tight') 
            plt.close(fig)

    print(f"\nPDF successfully generated at: {output_pdf}")

def main(history_file, output_pdf):
    print("\nLoading data...")
    try:
        df = pd.read_parquet(history_file)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date']).dt.normalize()
        print(f" * Loaded {len(df):,} ROA records.")
    except Exception as e:
        print(f"!! ERROR: Could not read file '{history_file}'")
        print(e)
        return

    generate_timeline_pdf(df, output_pdf, per_page=5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--history_file', type=str, required=True)
    parser.add_argument('--output_pdf', type=str, required=True)
    args = parser.parse_args()
    
    main(args.history_file, args.output_pdf)