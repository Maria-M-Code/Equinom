import pandas as pd
import re
from sqlalchemy import create_engine, text
from datetime import datetime
import os

def clean_column_name(col_name):
    col_name = col_name.strip()
    col_name = col_name.replace('%', 'percent')
    col_name = col_name.replace('[', '').replace(']', '')
    col_name = col_name.replace('.', '')
    col_name = re.sub(r'\s+', '_', col_name)
    return col_name.lower()

def read_device1_file(path):
    return pd.read_csv(path, sep='\t', skiprows=1, engine="python", on_bad_lines='skip')

def upload_dumas_to_postgres(file_paths, db_uri, log_path="dumas_loader.log"):
    log_lines = []

    def log(msg):
        print(msg)
        log_lines.append(f"{datetime.now().isoformat()} - {msg}")

    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # 🧾 קריאה מרשימת קבצים
    dfs = []
    for path in file_paths:
        try:
            df = read_device1_file(path)
            dfs.append(df)
            log(f"✅ Read file: {path} ({df.shape[0]} rows)")
        except Exception as e:
            log(f"❌ Failed to read {path}: {e}")

    if not dfs:
        log("❌ No valid Dumas files provided.")
        return

    df = pd.concat(dfs, ignore_index=True).drop_duplicates()
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    df.columns = [clean_column_name(col) for col in df.columns]

    log(f"📊 Total combined rows after cleaning: {df.shape[0]}")

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

    df = df.rename(columns={"name": "title"})

    missing_titles = df["title"].isna().sum()
    if missing_titles > 0:
        log(f"⚠️ {missing_titles} rows missing 'title' — dropped")
        df = df.dropna(subset=["title"])

    # Connect to PostgreSQL
    engine = create_engine(db_uri)

    ddl = """
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
        datetime TIMESTAMP
    );
    """

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
        "moisture_percent", "memo", "info", "date", "time", "datetime"
    ]

    df_to_upload = df[upload_cols]
    df_to_upload.to_sql("dumas_results", engine, if_exists="append", index=False)
    log(f"✅ Uploaded {len(df_to_upload)} records to dumas_results")

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))
    log(f"📝 Log saved to {log_path}")
