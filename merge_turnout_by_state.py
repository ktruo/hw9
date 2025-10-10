# merge_turnout_by_state.py  — robust year detection
import csv, glob, os, re, io

RAW_DIR = os.path.join("data", "raw")
OUT_PATH = os.path.join("data", "turnout_by_state.csv")
os.makedirs("data", exist_ok=True)

# Accept these header names
STATE_KEYS = {"StateAb", "State", "State/Territory", "State or Territory", "StateNm"}
TURNOUT_KEYS = {
    "TurnoutPercentage", "Turnout (%)", "Turnout Percentage",
    "Turnout %", "Turnout percentage"
}

# Map known event IDs → election year (AEC)
EVENT_TO_YEAR = {
    "15508": 2010,
    "17496": 2013,
    "20499": 2016,
    "24310": 2019,
    "27966": 2022,
}

def read_aec_csv_skip_metadata(path):
    """Return metadata_line, DictReader that skips the first metadata line if present."""
    with open(path, "r", encoding="utf-8-sig") as f:
        text = f.read()
    lines = text.splitlines()
    meta = lines[0] if lines else ""
    if lines and ("Federal Election" in lines[0] or "House of Representatives" in lines[0]):
        csv_text = "\n".join(lines[1:])
    else:
        csv_text = text
    return meta, csv.DictReader(io.StringIO(csv_text))

def infer_year(path_basename: str, metadata_line: str):
    """Get the true election year from metadata, else from known event id map."""
    # 1) Try metadata first line: e.g. '2010 Federal Election House of Representatives ...'
    m = re.search(r"(20\d{2})\s+Federal Election", metadata_line)
    if m:
        return int(m.group(1))
    # 2) Try known event id mapping from filename
    m2 = re.search(r"(15508|17496|20499|24310|27966)", path_basename)
    if m2:
        return EVENT_TO_YEAR[m2.group(1)]
    return None

rows = []
files = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))
if not files:
    print(f"⚠️  No CSVs found in {RAW_DIR}. Put the AEC files there and re-run.")
else:
    for path in files:
        base = os.path.basename(path)
        meta, reader = read_aec_csv_skip_metadata(path)
        year = infer_year(base, meta)

        headers = [h.strip() for h in (reader.fieldnames or [])]
        header_map = {h.strip(): h for h in headers}

        state_col = next((header_map[h] for h in headers if h.strip() in STATE_KEYS), None)
        turn_col  = next((header_map[h] for h in headers if h.strip() in TURNOUT_KEYS), None)

        if not state_col or not turn_col:
            print(f"Skipping (columns not found) → {base}")
            print(f"  Headers seen: {headers}")
            continue

        added = 0
        for rec in reader:
            st = (rec.get(state_col) or "").strip()
            if not st:
                continue
            # Prefer abbreviations if we got full names
            if state_col == header_map.get("StateNm") and "StateAb" in header_map:
                st = (rec.get(header_map["StateAb"]) or st).strip()

            # Normalise state to abbreviations
            mapping = {
                "New South Wales":"NSW","Victoria":"VIC","Queensland":"QLD",
                "Western Australia":"WA","South Australia":"SA","Tasmania":"TAS",
                "Australian Capital Territory":"ACT","Northern Territory":"NT"
            }
            st = mapping.get(st, st)

            if st not in {"NSW","VIC","QLD","WA","SA","TAS","ACT","NT"}:
                continue

            t = (rec.get(turn_col) or "").replace("%", "").replace(",", "").strip()
            try:
                t_pct = float(t)
            except ValueError:
                continue

            y = year
            # last-ditch: scan row for a year (rare)
            if y is None:
                m = re.search(r"(20\d{2})", " ".join(rec.values()))
                y = int(m.group(1)) if m else None
            if y is None:
                continue

            rows.append({"Year": y, "State": st, "TurnoutPct": t_pct})
            added += 1

        print(f"✓ Parsed {base} → {added} rows (Year={year})")

rows.sort(key=lambda r: (r["Year"], r["State"]))
with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["Year","State","TurnoutPct"])
    w.writeheader()
    w.writerows(rows)

print(f"\nSaved {OUT_PATH} with {len(rows)} rows.")
