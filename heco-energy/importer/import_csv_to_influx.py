import os
import glob
import time
import shutil
import re
from datetime import datetime

import pandas as pd
from dateutil import tz
from influxdb_client import InfluxDBClient, Point, WritePrecision

# -----------------------------
# Config (env + folders)
# -----------------------------
IN_DIR = "/incoming"
PROCESSED = "/processed"
FAILED = "/failed"

INFLUX_URL = os.environ["INFLUX_URL"]
INFLUX_TOKEN = os.environ["INFLUX_TOKEN"]
INFLUX_ORG = os.environ["INFLUX_ORG"]
INFLUX_BUCKET = os.environ["INFLUX_BUCKET"]

HST = tz.gettz("Pacific/Honolulu")

MEASUREMENT = "heco_interval"
TAG_SOURCE = "heco"

# -----------------------------
# Fallback parser for "collapsed" content
# Example: StartkWh12/1/2025 12:00:00 AM0.1986...
# -----------------------------
DT_RE = re.compile(
    r"(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+(?:AM|PM))"
)
NUM_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")

def parse_collapsed_text(text: str):
    cleaned = text.replace("\r", " ").replace("\n", " ")
    cleaned = cleaned.replace("StartkWh", " ").replace("Start kWh", " ")

    dts = list(DT_RE.finditer(cleaned))
    rows = []

    for i, m in enumerate(dts):
        dt_str = m.group(1)

        start = m.end()
        end = dts[i + 1].start() if i + 1 < len(dts) else len(cleaned)
        between = cleaned[start:end].strip()

        nm = NUM_RE.search(between)
        if not nm:
            continue

        kwh = float(nm.group(0))

        dt_naive = datetime.strptime(dt_str, "%m/%d/%Y %I:%M:%S %p")
        dt_local = dt_naive.replace(tzinfo=HST)

        rows.append((dt_local, kwh))

    return rows

# -----------------------------
# Preferred parser for real CSV: Start,kWh
# -----------------------------
def parse_start_kwh_csv(path: str):
    """
    Expects two columns named something like Start,kWh (case/whitespace tolerant).
    Handles comma-separated, tab-separated, or Excel-ish exports with BOM.
    """
    # Try common delimiters (comma first, then tab)
    last_err = None
    for sep in [",", "\t", ";"]:
        try:
            df = pd.read_csv(path, sep=sep, engine="python")
            if df.shape[1] >= 2:
                break
        except Exception as e:
            last_err = e
            df = None

    if df is None:
        raise last_err or RuntimeError("Failed to read CSV.")

    # Normalize column names
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "start" not in df.columns or "kwh" not in df.columns:
        raise ValueError(f"Expected columns Start,kWh but got: {df.columns}")

    # Parse time + localize to Hawaii
    df["start"] = pd.to_datetime(
        df["start"],
        format="%m/%d/%Y %I:%M:%S %p",
        errors="raise",
    )
    df["start"] = df["start"].dt.tz_localize(HST)

    # Parse numeric kWh
    df["kwh"] = pd.to_numeric(df["kwh"], errors="raise")

    rows = list(zip(df["start"].to_list(), df["kwh"].to_list()))
    return rows

def parse_file(path: str):
    """
    Try proper CSV first; if it doesn't look like Start/kWh CSV,
    fall back to collapsed-text parsing.
    """
    try:
        return parse_start_kwh_csv(path)
    except Exception:
        # Fallback: read as text and parse collapsed format
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        rows = parse_collapsed_text(content)
        if not rows:
            raise  # re-raise original CSV exception if fallback yields nothing
        return rows

# -----------------------------
# Influx write
# -----------------------------
def write_to_influx(rows):
    if not rows:
        return 0

    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        write_api = client.write_api()

        points = []
        for dt_local, kwh in rows:
            p = (
                Point(MEASUREMENT)
                .tag("source", TAG_SOURCE)
                .field("kwh", float(kwh))
                .time(dt_local, WritePrecision.S)
            )
            points.append(p)

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

    return len(rows)

# -----------------------------
# File watcher loop
# -----------------------------
def ensure_dirs():
    os.makedirs(PROCESSED, exist_ok=True)
    os.makedirs(FAILED, exist_ok=True)

def should_process(path: str) -> bool:
    if os.path.isdir(path):
        return False
    base = os.path.basename(path).lower()
    return base.endswith(".csv") or base.endswith(".txt")

def main():
    ensure_dirs()
    print("HECO importer started. Watching /incoming ...")

    while True:
        for path in glob.glob(os.path.join(IN_DIR, "*")):
            if not should_process(path):
                continue

            base = os.path.basename(path)
            try:
                rows = parse_file(path)
                n = write_to_influx(rows)

                dest = os.path.join(PROCESSED, base)
                shutil.move(path, dest)
                print(f"Imported {n} points from {base} -> processed/")
            except Exception as e:
                dest = os.path.join(FAILED, base)
                try:
                    shutil.move(path, dest)
                except Exception:
                    pass
                print(f"FAILED importing {base}: {e} -> failed/")

        time.sleep(10)

if __name__ == "__main__":
    main()

