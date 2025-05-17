import pandas as pd
import re
import os
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import datetime

def clean_column_name(col_name):
    col_name = col_name.strip()
    col_name = col_name.replace('%', 'percent')
    col_name = col_name.replace('[', '').replace(']', '')
    col_name = col_name.replace('.', '')
    col_name = re.sub(r'\s+', '_', col_name)
    return col_name.lower()

def read_device1_file(path):
    df = pd.read_csv(path, sep='\t', skiprows=1, engine="python", on_bad_lines='skip')
    df["source_file"] = path.name  # Track source file
    return df

def upload_dumas_to_postgres(folder_path, db_uri, log_path="logs/dumas_loader.log"):
    log_lines = []

    def log(msg):
        timestamp = f"{datetime.now().isoformat()} - {msg}"
        print(timestamp)
        log_lines.append(timestamp)

    log_dir = os.path.dirname(log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    folder = Path(folder_path)
    file_paths = sorted([f for f in folder.iterdir() if f.name.lower().startswith("device1") and f.name.lower().endswith(".csv") and f.is_file()])

    if not file_paths:
        log(f"❌ No files matching 'Device1*.csv' found in: {folder_path}")
        return

    log(f"📥 Found {len(file_paths)} file(s): {[f.name for f in file_paths]}")

    dfs = []
    for path in file_paths:
        try:
            df = read_device1_file(path)
            dfs.append(df)
            log(f"✅ Loaded: {path.name} ({df.shape[0]} rows)")
        except Exception as e:
            log(f"❌ Failed to read {path.name}: {e}")

    if not dfs:
        log("❌ No valid Dumas files to process.")
        return

    df = pd.concat(dfs, ignore_index=True).drop_duplicates()
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    df.columns = [clean_column_name(col) for col in df.columns]
    df = df.rename(columns={"name": "title"})

    log(f"📊 Combined rows after cleaning: {df.shape[0]}")

    # === Validations ===
    required_cols = ["no", "hole_pos", "weight_mg", "title", "method",
        "n_area", "n_percent", "n_mg", "n_factor", "n_blank",
        "protein_percent", "protein_mg", "protein_factor",
        "moisture_percent", "memo", "info", "date_time"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        log(f"❌ Missing required columns: {missing_cols}")
        return

    if df["title"].isna().any():
        log(f"⚠️ Found {df['title'].isna().sum()} rows with missing title")
        df = df.dropna(subset=["title"])

    dupes = df["title"].duplicated().sum()
    if dupes > 0:
        log(f"⚠️ Found {dupes} duplicate title rows")

    if (df["n_percent"] < 0).any():
        log("❌ Negative nitrogen % detected")

    if (df["protein_percent"] > 100).any():
        log("⚠️ Protein % over 100 detected")

    if "weight_mg" in df.columns and (df["weight_mg"] <= 0).any():
        log("❌ Non-positive weights found")

    # Parse datetime
    if "date_time" in df.columns:
        dt = pd.to_datetime(df["date_time"], errors="coerce", dayfirst=True)
    elif "date" in df.columns and "time" in df.columns:
        dt = pd.to_datetime(df["date"] + " " + df["time"], errors="coerce", dayfirst=True)
    else:
        dt = pd.NaT

    df["datetime"] = dt
    df["date"] = df["datetime"].dt.date
    df["time"] = df["datetime"].dt.time

    if df["datetime"].isna().sum() > 0:
        log("⚠️ Some rows missing parsed datetime")

    

    engine = create_engine(db_uri)

    ddl = '''
    DROP TABLE IF EXISTS dumas_results CASCADE;

    CREATE TABLE dumas_results (
        id SERIAL PRIMARY KEY,
        no INTEGER,
        hole_pos TEXT,
        weight_mg FLOAT,
        title TEXT,
        method TEXT,
        n_area FLOAT,
        n_percent FLOAT,
        n_mg FLOAT,
        n_factor FLOAT,
        n_blank FLOAT,
        protein_percent FLOAT,
        protein_mg FLOAT,
        protein_factor FLOAT,
        moisture_percent FLOAT,
        memo TEXT,
        info TEXT,
        date DATE,
        time TIME,
        datetime TIMESTAMP,
        source_file TEXT
    );
    '''

    with engine.connect() as conn:
        for stmt in ddl.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt + ";"))
        conn.commit()
        log("✅ dumas_results table created")

    upload_cols = [
        "no", "hole_pos", "weight_mg", "title", "method",
        "n_area", "n_percent", "n_mg", "n_factor", "n_blank",
        "protein_percent", "protein_mg", "protein_factor",
        "moisture_percent", "memo", "info", "date", "time", "datetime", "source_file"
    ]

    df_to_upload = df[upload_cols]
    df_to_upload.to_sql("dumas_results", engine, if_exists="append", index=False)
    log(f"✅ Uploaded {len(df_to_upload)} records to dumas_results")

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))
    log(f"📝 Log saved to {log_path}")