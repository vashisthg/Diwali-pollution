"""Fetch hourly Delhi station data (CPCB / DPCC / IMD) from the public OpenAQ archive.

The archive (s3://openaq-data-archive) needs no API key. It is published with a
lag of a few days, so re-run to pick up the most recent days.
"""
import csv
import gzip
import io
import os
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import pandas as pd

ARCHIVE = "https://openaq-data-archive.s3.amazonaws.com/records/csv.gz"

# OpenAQ location id -> station name (Delhi NCT stations active in 2026)
DELHI_STATIONS = {
    17: "R K Puram",
    50: "Punjabi Bagh",
    235: "Anand Vihar",
    5404: "Pusa",
    5570: "Aya Nagar",
    5586: "Sirifort",
    5610: "North Campus, DU",
    5613: "ITO",
    5622: "NSIT Dwarka",
    5626: "DTU",
    5627: "CRRI Mathura Road",
    5630: "Shadipur",
    5634: "Lodhi Road",
    5650: "IGI Airport (T3)",
    8118: "US Embassy",  # Chanakyapuri, AirNow; PM2.5 only
}

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "openaq_cache")


def _fetch_day(location_id, day):
    os.makedirs(CACHE_DIR, exist_ok=True)
    stamp = day.strftime("%Y%m%d")
    cache = os.path.join(CACHE_DIR, f"{location_id}-{stamp}.csv")
    if os.path.exists(cache):
        with open(cache) as f:
            return f.read()
    url = (f"{ARCHIVE}/locationid={location_id}/year={day:%Y}/month={day:%m}/"
           f"location-{location_id}-{stamp}.csv.gz")
    try:
        text = gzip.decompress(urllib.request.urlopen(url, timeout=30).read()).decode()
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):  # day not published (yet) or station offline
            return ""
        raise
    with open(cache, "w") as f:
        f.write(text)
    return text


def get_hourly(start, end, stations=DELHI_STATIONS, parameters=("pm25", "pm10", "no2")):
    """Hourly readings as a long DataFrame: station, datetime, parameter, value."""
    days = [start + timedelta(d) for d in range((end - start).days + 1)]
    jobs = [(loc, d) for loc in stations for d in days]
    with ThreadPoolExecutor(32) as ex:
        texts = list(ex.map(lambda j: _fetch_day(*j), jobs))
    rows = []
    for (loc, _), text in zip(jobs, texts):
        for r in csv.DictReader(io.StringIO(text)):
            if r["parameter"] in parameters:
                rows.append((stations[loc], r["datetime"], r["parameter"], float(r["value"])))
    df = pd.DataFrame(rows, columns=["station", "datetime", "parameter", "value"])
    # timestamps are local (+05:30); drop the offset so plots read in IST
    df["datetime"] = pd.to_datetime(df["datetime"].str[:19])
    # drop sensor glitches (negative or absurd values)
    df = df[(df["value"] >= 0) & (df["value"] < 2000)]
    return df


def daily_means(hourly, min_hours=16):
    """Daily average per station/pollutant (like the CPCB 24-h values in the 2015 data).

    Days with fewer than `min_hours` hourly readings are dropped.
    """
    h = hourly.assign(date=hourly["datetime"].dt.normalize())
    g = h.groupby(["station", "parameter", "date"])["value"].agg(["mean", "count"]).reset_index()
    g = g[g["count"] >= min_hours]
    return g.pivot_table(index=["station", "date"], columns="parameter", values="mean").reset_index()


if __name__ == "__main__":
    df = get_hourly(date(2026, 9, 1), date.today())
    print(df.groupby("parameter")["datetime"].agg(["min", "max", "count"]))
