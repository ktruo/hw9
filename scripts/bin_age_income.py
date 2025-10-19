import csv
from collections import defaultdict
from pathlib import Path

SRC = Path("data/age_income_by_state.csv")
DEST = Path("data/age_income_binned.csv")

# Mapping for age groups to bins
AGE_BIN_MAP = {
    "15-19 years": "15-24",
    "20-24 years": "15-24",
    "25-34 years": "25-44",
    "35-44 years": "25-44",
    "45-54 years": "45-64",
    "55-64 years": "45-64",
    "65-74 years": "65-84",
    "75-84 years": "65-84",
    "85 years and over": "85 years and over",
}

# Mapping for income brackets to bins
INCOME_BIN_MAP = {
    "Negative/Nil income": "Negative/Nil to $649",
    "$1-$149": "Negative/Nil to $649",
    "$150-$299": "Negative/Nil to $649",
    "$300-$399": "Negative/Nil to $649",
    "$400-$499": "Negative/Nil to $649",
    "$500-$649": "Negative/Nil to $649",

    "$650-$799": "$650-$1,499",
    "$800-$999": "$650-$1,499",
    "$1,000-$1,249": "$650-$1,499",
    "$1,250-$1,499": "$650-$1,499",

    "$1,500-$1,749": "$1,500-$2,999",
    "$1,750-$1,999": "$1,500-$2,999",
    "$2,000-$2,999 more": "$1,500-$2,999",

    "$3,000-$3,499": "$3,000 and above",
    "$3,500 or more": "$3,000 and above",
}

def main():
    if not SRC.exists():
        raise SystemExit(f"Source CSV not found: {SRC}")

    agg = defaultdict(int)  # key: (State, AgeBin, IncomeBin) -> sum Count

    with SRC.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            state = row.get('State')
            age = row.get('AgeGroup')
            inc = row.get('IncomeBracket')
            if not state or not age or not inc:
                continue

            # Skip totals and not stated
            if age == 'Total' or inc == 'Total' or inc == 'Personal income not stated':
                continue

            age_bin = AGE_BIN_MAP.get(age)
            inc_bin = INCOME_BIN_MAP.get(inc)
            if not age_bin or not inc_bin:
                # Unmapped category; skip defensively
                continue

            try:
                count = int(row.get('Count', '0').replace(',', '').strip())
            except Exception:
                # If Count cannot be parsed, skip
                continue

            agg[(state, age_bin, inc_bin)] += count

    # Add national aggregate as AUS
    total = defaultdict(int)
    for (state, ageb, incb), cnt in agg.items():
        total[(ageb, incb)] += cnt
    for (ageb, incb), cnt in total.items():
        agg[("AUS", ageb, incb)] = cnt

    DEST.parent.mkdir(parents=True, exist_ok=True)
    with DEST.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['State', 'AgeBin', 'IncomeBin', 'Count'])
        # Sort for stable output using explicit state order
        age_order = ["15-24", "25-44", "45-64", "65-84", "85 years and over"]
        inc_order = [
            "Negative/Nil to $649",
            "$650-$1,499",
            "$1,500-$2,999",
            "$3,000 and above",
        ]
        state_order = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT", "AUS"]
        def sort_key(item):
            (state, ageb, incb), _ = item
            return (
                state_order.index(state) if state in state_order else 998,
                age_order.index(ageb) if ageb in age_order else 999,
                inc_order.index(incb) if incb in inc_order else 999,
            )
        rows = sorted(agg.items(), key=sort_key)
        for (state, ageb, incb), cnt in rows:
            writer.writerow([state, ageb, incb, cnt])

    print(f"Wrote {DEST} with {len(rows)} rows (aggregated incl. AUS)")

if __name__ == '__main__':
    main()
