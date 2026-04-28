"""
Loads Zillow housing data for Massachusetts municipalities.

Sources (national CSVs filtered to StateName == 'MA'):

  PRIMARY — ZHVI (Zillow Home Value Index):
    files.zillowstatic.com/.../zhvi/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv
    Covers 356 MA towns (estimate-based, not transaction-based — full small-town coverage).
    Monthly, smoothed + seasonally adjusted.  → zhvi column

  SUPPLEMENTAL — Median sale price & days to pending:
    files.zillowstatic.com/.../median_sale_price/City_median_sale_price_uc_sfrcondo_month.csv
    files.zillowstatic.com/.../mean_doz_pending/City_mean_doz_pending_uc_sfrcondo_month.csv
    Transaction-based — only 23 major MA cities have enough volume for Zillow to report.
    → median_sale_price, mean_days_to_pending columns (NULL for small towns)

All three are national files (~30MB) downloaded and filtered to MA.
Aggregate in SQL as needed:
  SELECT region_name, data_year, AVG(zhvi) AS avg_home_value
  FROM municipal_zillow_housing
  WHERE data_year = 2024 GROUP BY 1, 2

Run: python scrapers/zillow_housing.py [--start-year 2014]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import io
import argparse
import requests
import pandas as pd
from sqlalchemy import text
from config import get_engine

ZHVI_URL = (
    "https://files.zillowstatic.com/research/public_csvs/zhvi/"
    "City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
)
SALE_PRICE_URL = (
    "https://files.zillowstatic.com/research/public_csvs/median_sale_price/"
    "City_median_sale_price_uc_sfrcondo_month.csv"
)
DAYS_PENDING_URL = (
    "https://files.zillowstatic.com/research/public_csvs/mean_doz_pending/"
    "City_mean_doz_pending_uc_sfrcondo_month.csv"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Fixed ID columns in every Zillow city-level CSV
ID_COLS = ["RegionID", "SizeRank", "RegionName", "RegionType",
           "StateName", "State", "Metro", "CountyName"]

UPSERT = text("""
    INSERT INTO municipal_zillow_housing
        (region_name, county_name, data_year, data_month,
         zhvi, median_sale_price, mean_days_to_pending)
    VALUES
        (:region_name, :county_name, :data_year, :data_month,
         :zhvi, :median_sale_price, :mean_days_to_pending)
    ON CONFLICT (region_name, data_year, data_month) DO UPDATE SET
        county_name          = COALESCE(EXCLUDED.county_name,          municipal_zillow_housing.county_name),
        zhvi                 = COALESCE(EXCLUDED.zhvi,                 municipal_zillow_housing.zhvi),
        median_sale_price    = COALESCE(EXCLUDED.median_sale_price,    municipal_zillow_housing.median_sale_price),
        mean_days_to_pending = COALESCE(EXCLUDED.mean_days_to_pending, municipal_zillow_housing.mean_days_to_pending),
        loaded_at            = NOW()
""")


def _download(url: str, label: str) -> pd.DataFrame | None:
    print(f"[zillow] Downloading {label} ({url.split('/')[-1]})...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=120)
        r.raise_for_status()
        raw = r.content
        print(f"[zillow]   {len(raw)/1_048_576:.1f} MB downloaded")
    except Exception as e:
        print(f"[zillow] ERROR downloading {label}: {e}")
        return None

    df = pd.read_csv(io.BytesIO(raw), dtype=str)
    # StateName is the 2-letter abbreviation (e.g. "MA"), not full name
    ma = df[df["StateName"] == "MA"].copy()
    print(f"[zillow]   {len(ma)} Massachusetts cities/towns found")
    return ma


def _melt(df: pd.DataFrame, value_name: str, start_year: int) -> pd.DataFrame:
    """Melt wide-format Zillow CSV to long format, filtering to start_year onward."""
    date_cols = [c for c in df.columns if c not in ID_COLS]
    long = df.melt(
        id_vars=["RegionName", "CountyName"],
        value_vars=date_cols,
        var_name="date_str",
        value_name=value_name,
    )
    long["date_str"] = pd.to_datetime(long["date_str"], errors="coerce")
    long = long.dropna(subset=["date_str"])
    long["data_year"]  = long["date_str"].dt.year
    long["data_month"] = long["date_str"].dt.month
    long = long[long["data_year"] >= start_year].copy()
    long[value_name] = pd.to_numeric(long[value_name], errors="coerce")
    return long[["RegionName", "CountyName", "data_year", "data_month", value_name]]


def run(start_year: int = 2014):
    engine = get_engine()

    zhvi_raw  = _download(ZHVI_URL,         "ZHVI home value index")
    sale_raw  = _download(SALE_PRICE_URL,   "median sale price")
    days_raw  = _download(DAYS_PENDING_URL, "mean days to pending")

    if zhvi_raw is None and sale_raw is None and days_raw is None:
        print("[zillow] All downloads failed — nothing to load.")
        return

    # Melt each source to long format
    long_frames = []
    key_cols = ["RegionName", "CountyName", "data_year", "data_month"]

    if zhvi_raw is not None:
        long_frames.append(_melt(zhvi_raw, "zhvi", start_year))
    if sale_raw is not None:
        long_frames.append(_melt(sale_raw, "median_sale_price", start_year))
    if days_raw is not None:
        long_frames.append(_melt(days_raw, "mean_days_to_pending", start_year))

    # Outer-join all three on town + year + month
    combined = long_frames[0]
    for frame in long_frames[1:]:
        combined = combined.merge(frame, on=key_cols, how="outer")

    # Fill value columns that may be missing after merge
    for col in ["zhvi", "median_sale_price", "mean_days_to_pending"]:
        if col not in combined.columns:
            combined[col] = None

    print(f"[zillow]   Combined: {len(combined):,} town-months "
          f"(years {int(combined['data_year'].min())}–{int(combined['data_year'].max())})")

    records = []
    for _, row in combined.iterrows():
        zhvi  = row.get("zhvi")
        price = row.get("median_sale_price")
        days  = row.get("mean_days_to_pending")
        records.append({
            "region_name":          str(row["RegionName"]).strip(),
            "county_name":          str(row["CountyName"]).strip() if pd.notna(row.get("CountyName")) else None,
            "data_year":            int(row["data_year"]),
            "data_month":           int(row["data_month"]),
            "zhvi":                 float(zhvi)  if pd.notna(zhvi)  else None,
            "median_sale_price":    float(price) if pd.notna(price) else None,
            "mean_days_to_pending": float(days)  if pd.notna(days)  else None,
        })

    if not records:
        print("[zillow] No records to load.")
        return

    BATCH = 1000
    total = 0
    with engine.begin() as conn:
        for i in range(0, len(records), BATCH):
            conn.execute(UPSERT, records[i:i+BATCH])
            total += min(BATCH, len(records) - i)
        conn.execute(text("""
            INSERT INTO ingest_log (source, rows_loaded, status)
            VALUES ('zillow_housing', :n, 'ok')
        """), {"n": total})

    print(f"[zillow] ✓ Loaded {total:,} town-month rows into municipal_zillow_housing")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Zillow housing data for MA municipalities")
    parser.add_argument("--start-year", type=int, default=2014,
                        help="Earliest year to keep (default 2014)")
    args = parser.parse_args()
    run(start_year=args.start_year)
