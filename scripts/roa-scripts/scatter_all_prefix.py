import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import os
from matplotlib.backends.backend_pdf import PdfPages


def compute_intervals(df):
    df = df.sort_values("snapshot_date")
    intervals = []

    for asn, group in df.groupby("asn"):
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

def plot_prefix_timeline(df, prefix, ax):

    subset = df[df['prefix'] == prefix].copy()
    subset['snapshot_date'] = pd.to_datetime(subset['snapshot_date'])

    intervals = compute_intervals(subset)

    asns = sorted(subset['asn'].unique())
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
        data=subset,
        x='snapshot_date',
        y='asn',
        hue='asn',
        palette=color_map,
        s=10,
        alpha=0.8,
        ax=ax,
        legend=False
    )

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())

    ax.set_title(f"{prefix}", fontsize=12)
    ax.set_xlabel("Snapshot Date")
    ax.set_ylabel("Origin ASN")

    ax.grid(axis='x', linestyle='--', alpha=0.4)

    for label in ax.get_xticklabels():
        label.set_rotation(45)
        label.set_ha('right')


def generate_timeline_pdf(df, output_pdf, per_page=3):
    prefixes = df['prefix'].unique()
    print(f"\nFound {len(prefixes)} unique prefixes.")
    os.makedirs(os.path.dirname(output_pdf), exist_ok=True)

    with PdfPages(output_pdf) as pdf:
        for i in range(0, len(prefixes), per_page):
            fig, axes = plt.subplots(
                per_page, 1,
                figsize=(14, 4 * per_page),
                constrained_layout=True
            )

            if per_page == 1:
                axes = [axes]

            batch = prefixes[i:i+per_page]

            for ax, prefix in zip(axes, batch):
                print(f"Generating timeline for {prefix} ")
                plot_prefix_timeline(df, prefix, ax)

            fig.suptitle("ROA Timelines (Merged Intervals per Prefix)", fontsize=16)
            pdf.savefig(fig)
            plt.close(fig)

    print(f"\nPDF successfully generated at: {output_pdf}")


def main(history_file, output_pdf):
    print("\n*************************************************************************************")
    print("\n-------------------------- MULTI-PREFIX ROA TIMELINE REPORT -------------------------")
    print("\n*************************************************************************************")

    print(f"Loading data from: {history_file}")
    try:
        df = pd.read_parquet(history_file)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        print(f" * Loaded {len(df):,} ROA records.")
    except Exception as e:
        print(f"!! ERROR: Could not read file '{history_file}'")
        print(e)
        return

    generate_timeline_pdf(df, output_pdf, per_page=5)
    print("\nAll prefix timeline plots successfully compiled into PDF.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate merged-interval ROA timelines for all prefixes into a PDF."
    )
    parser.add_argument(
        '--history_file',
        type=str,
        required=True,
        help="Path to the Parquet file containing full ROA history (prefix, asn, snapshot_date)."
    )
    parser.add_argument(
        '--output_pdf',
        type=str,
        required=True,
        help="Path for the output PDF report."
    )
    args = parser.parse_args()
    main(args.history_file, args.output_pdf)
