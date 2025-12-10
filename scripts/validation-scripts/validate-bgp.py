import requests
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.lines import Line2D

def fetch_bgp_history(prefix, starttime, endtime):
    url = "https://stat.ripe.net/data/routing-history/data.json"
    params = {
        "resource": prefix,
        "starttime": starttime,
        "endtime": endtime,
        "min_peers": 10
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def parse_bgp_intervals(bgp_json):
    rows = []
    for origin_entry in bgp_json["data"]["by_origin"]:
        asn = origin_entry["origin"]
        for pfx in origin_entry["prefixes"]:
            for tl in pfx["timelines"]:
                start = datetime.datetime.fromisoformat(tl["starttime"].replace("Z", "")).date()
                end = datetime.datetime.fromisoformat(tl["endtime"].replace("Z", "")).date()
                rows.append([asn, start, end])

    df = pd.DataFrame(rows, columns=["asn", "start", "end"])
    df = df.sort_values(["start", "asn"])
    return df

def compute_intervals(example_df):
    example_df = example_df.sort_values("snapshot_date")

    intervals = []
    for asn, group in example_df.groupby("asn"):
        group = group.sort_values("snapshot_date")

        start = group["snapshot_date"].iloc[0]
        prev = start

        for current in group["snapshot_date"].iloc[1:]:
            if (current - prev).days > 1:
                intervals.append((asn, start, prev))
                start = current
            prev = current

        intervals.append((asn, start, prev))

    return pd.DataFrame(intervals, columns=["asn", "start", "end"])

def load_roa_intervals(df, prefix):
    print("\n ** Computing ROA timeline intervals")
    example_df = df[df["prefix"] == prefix].copy()

    example_df["snapshot_date"] = pd.to_datetime(example_df["snapshot_date"]).dt.date

    intervals = compute_intervals(example_df)

    print(f"\n--- Computed ROA Intervals for {prefix} ---")
    print(intervals.sort_values("start").to_string(index=False))
    print("---------------------------------------------------\n")

    return intervals.sort_values(["start", "asn"])

def intervals_overlap(s1, e1, s2, e2):
    return not (e1 < s2 or e2 < s1)

def compare_intervals(bgp_df, roa_df):
    bgp_valid = []
    bgp_invalid = []

    roa_df["used"] = False

    for _, b in bgp_df.iterrows():
        asn = "AS" + str(b["asn"])
        bs, be = b["start"], b["end"]

        matches = roa_df[roa_df["asn"] == asn]

        ok = False
        for ridx, r in matches.iterrows():
            if intervals_overlap(bs, be, r["start"], r["end"]):
                ok = True
                roa_df.at[ridx, "used"] = True
                bgp_valid.append([asn, bs, be, r["start"], r["end"]])
                break

        if not ok:
            bgp_invalid.append([asn, bs, be])

    roa_unused = []
    for _, r in roa_df[roa_df["used"] == False].iterrows():
        roa_unused.append([r["asn"], r["start"], r["end"]])

    return bgp_valid, bgp_invalid, roa_unused


def plot_clean_roa_bgp_timeline(bgp_df, roa_df, prefix, output_file="clean_roa_bgp_timeline.png"):
    print("\n📊 Generating clean ROA + BGP timeline plot\n")

    bgp_df["asn_norm"] = bgp_df["asn"].apply(lambda x: "AS" + str(x))
    roa_df["asn_norm"] = roa_df["asn"]

    all_asns = sorted(set(bgp_df["asn_norm"]) | set(roa_df["asn_norm"]))
    asn_index = {asn: i for i, asn in enumerate(all_asns)}

    _, ax = plt.subplots(figsize=(18, 10))

    for _, r in roa_df.iterrows():
        y = asn_index[r["asn_norm"]]
        ax.hlines(
            y=y,
            xmin=r["start"],
            xmax=r["end"],
            color="tab:blue",
            linewidth=10,
            alpha=0.8
        )

    for _, b in bgp_df.iterrows():
        asn = "AS" + str(b["asn"])
        y = asn_index[asn]
        ax.hlines(
            y=y,
            xmin=b["start"],
            xmax=b["end"],
            color="tab:red",
            linewidth=4,
            alpha=0.9
        )

    ax.set_yticks(list(asn_index.values()))
    ax.set_yticklabels(list(asn_index.keys()), fontsize=12)

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    ax.grid(axis="x", linestyle="--", alpha=0.3)

    ax.set_title(f"BGP vs ROA Timeline for {prefix}", fontsize=20)
    ax.set_xlabel("Date", fontsize=14)
    ax.set_ylabel("Origin ASN", fontsize=14)

    legend_items = [
        Line2D([0], [0], color="tab:blue", lw=10, label="ROA Interval"),
        Line2D([0], [0], color="tab:red", lw=4,  label="BGP Announcement"),
    ]
    ax.legend(
        handles=legend_items,
        title="Legend",
        fontsize=12,
        loc="upper left",
        frameon=True
    )

    plt.tight_layout()
    plt.savefig(output_file, dpi=230)
    plt.close()

    print(f"Saved timeline to {output_file}")


def main():
    prefix = "31.56.67.0/24"
    starttime = "2024-09-01T00:00:00"
    endtime   = "2025-09-23T00:00:00"

    print("\nFetching BGP history")
    bgp_json = fetch_bgp_history(prefix, starttime, endtime)
    bgp_df = parse_bgp_intervals(bgp_json)

    print("\nLoading ROA Data")
    # history_file = "/Users/rakshita/Desktop/gatech/fall25/8903/code/output/all-roas-834-prefix.parquet"
    history_file = "/Users/rakshita/Desktop/gatech/fall25/8903/code/year-long/final/year-long-834-prefix-roas.parquet"
    df = pd.read_parquet(history_file)

    roa_df = load_roa_intervals(df, prefix)

    print("\nComparing intervals...")
    bgp_valid, bgp_invalid, roa_unused = compare_intervals(bgp_df, roa_df)

    print("\n========== VALID BGP INTERVALS (covered by ROA) ==========") 
    for row in bgp_valid: 
        print(f"ASN {row[0]} BGP:{row[1]}-{row[2]} covered by ROA:{row[3]}-{row[4]}") 
    
    print("\n========== INVALID BGP INTERVALS (NO ROA COVERAGE) ==========") 
    for asn, s, e in bgp_invalid: 
        print(f"ASN {asn} {s}-{e} No matching ROA interval") 
    
    print("\n========== UNUSED ROA INTERVALS (never seen in BGP) ==========") 
    for asn, s, e in roa_unused: 
        print(f"{asn} {s}-{e} Never used in BGP")   
    
    print("\nDone.\n")

    plot_clean_roa_bgp_timeline(bgp_df, roa_df, prefix)


if __name__ == "__main__":
    main()
