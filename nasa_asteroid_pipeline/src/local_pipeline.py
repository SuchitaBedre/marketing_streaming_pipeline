from pathlib import Path
import pandas as pd
import argparse
import datetime
import os
import numpy as np


def read_csvs(input_dir: Path):
    files = sorted(input_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {input_dir}")
    df_list = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df['__source_file'] = f.name
            df_list.append(df)
        except Exception as e:
            print(f"Warning: failed reading {f}: {e}")
    if not df_list:
        raise RuntimeError("No CSVs could be read successfully")
    return pd.concat(df_list, ignore_index=True)


def ensure_columns(df: pd.DataFrame):
    # Normalise expected columns
    expected = ['id', 'name', 'absolute_magnitude_h', 'estimated_diameter_km', 'is_potentially_hazardous_asteroid']
    for c in expected:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def write_parquet(df: pd.DataFrame, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = int(datetime.datetime.utcnow().timestamp())
    run_dir = out_dir / f"snapshot_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    out_file = run_dir / "data.parquet"
    try:
        df.to_parquet(out_file, index=False)
    except Exception:
        # fallback to CSV if parquet engine not available
        out_csv = run_dir / "data.csv"
        df.to_csv(out_csv, index=False)
        return run_dir
    return run_dir


def main(input_dir: str, out_root: str):
    input_path = Path(input_dir)
    out_root = Path(out_root)
    print(f"Reading CSVs from {input_path}")
    df = read_csvs(input_path)
    df = ensure_columns(df)

    # Add ingestion metadata
    df['ingestion_timestamp'] = pd.Timestamp.utcnow()
    df['source'] = 'local_csv'

    # Bronze: raw write
    bronze_dir = out_root / 'delta' / 'asteroid_bronze'
    bronze_run = write_parquet(df, bronze_dir)

    # Silver: clean and dedupe
    df_silver = df.copy()
    df_silver['id'] = df_silver['id'].astype(str)
    df_silver['absolute_magnitude'] = pd.to_numeric(df_silver['absolute_magnitude_h'], errors='coerce')
    # handle hazardous boolean-like
    def map_hazard(x):
        if pd.isna(x):
            return 0
        if isinstance(x, (bool, np.bool_)):
            return int(x)
        if isinstance(x, (int, np.integer)):
            return 1 if x != 0 else 0
        xs = str(x).strip().lower()
        if xs in ('true', '1', 'yes'):
            return 1
        return 0

    df_silver['is_hazardous'] = df_silver['is_potentially_hazardous_asteroid'].apply(map_hazard)
    df_silver = df_silver.dropna(subset=['id', 'name', 'absolute_magnitude'])
    # keep latest ingestion per id
    df_silver = df_silver.sort_values('ingestion_timestamp').drop_duplicates(subset=['id'], keep='last')

    silver_dir = out_root / 'delta' / 'asteroid_silver'
    silver_run = write_parquet(df_silver[['id', 'name', 'absolute_magnitude', 'is_hazardous', 'ingestion_timestamp', 'source']], silver_dir)

    # Gold: feature engineering
    df_gold = df_silver.copy()
    def mag_cat(m):
        try:
            m = float(m)
        except Exception:
            return 'unknown'
        if m < 18:
            return 'very_bright'
        if m < 22:
            return 'bright'
        if m < 25:
            return 'moderate'
        return 'dim'

    df_gold['magnitude_category'] = df_gold['absolute_magnitude'].apply(mag_cat)
    df_gold['risk_score'] = df_gold.apply(lambda r: (100 - r['absolute_magnitude'] * 3) if r['is_hazardous'] == 1 else 0, axis=1)

    gold_dir = out_root / 'delta' / 'asteroid_gold'
    gold_run = write_parquet(df_gold, gold_dir)

    print(f"Processed {len(df)} records into Bronze: {bronze_run}")
    print(f"Silver rows: {len(df_silver)} -> {silver_run}")
    print(f"Gold rows: {len(df_gold)} -> {gold_run}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Local pipeline to process CSVs into bronze/silver/gold parquet snapshots')
    parser.add_argument('--input', default='data/raw_csv_landing', help='Input folder with CSVs')
    parser.add_argument('--out', default='data', help='Output root folder')
    args = parser.parse_args()
    # detect common input path if default not present
    input_path = Path(args.input)
    if not input_path.exists():
        # try nasa_raw_data
        alt = Path('data') / 'nasa_raw_data'
        if alt.exists():
            input_path = alt
        else:
            print(f"Input folder {args.input} not found and no alternative found.")
            raise SystemExit(1)
    main(str(input_path), args.out)
