import pandas as pd


DETAIL_FILE = "ipxo_roa_event_details.csv" 
OUTPUT_FILE = "ipxo_roa_lifetimes.csv"

print("Loading detailed event log...")
events = pd.read_csv(DETAIL_FILE, parse_dates=["date"])
events = events.sort_values(["prefix", "date"])

lifetimes = []

for prefix, group in events.groupby("prefix"):
    group = group.sort_values("date")
    
    active = False
    start_date = None

    for _, row in group.iterrows():
        event = row["event"].lower()
        date = row["date"]

        if event in ["creation", "update_to_as834"] and not active:
            active = True
            start_date = date

        elif event in ["deletion", "update_from_as834"] and active:
            end_date = date
            duration = (end_date - start_date).days
            lifetimes.append({
                "prefix": prefix,
                "start_date": start_date.date(),
                "end_date": end_date.date(),
                "duration_days": duration
            })
            active = False
            start_date = None

    if active and start_date is not None:
        lifetimes.append({
            "prefix": prefix,
            "start_date": start_date.date(),
            "end_date": None,
            "duration_days": None
        })

lifetimes_df = pd.DataFrame(lifetimes)
lifetimes_df = lifetimes_df.sort_values(["prefix", "start_date"]).reset_index(drop=True)

lifetimes_df.to_csv(OUTPUT_FILE, index=False)

print(f"\nSaved prefix lifetimes → {OUTPUT_FILE}")
print("\nExample of computed lifetimes:")
print(lifetimes_df.head(20))
