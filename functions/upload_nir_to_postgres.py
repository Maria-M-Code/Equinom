# upload_to_postgres.py

import json
import uuid
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os

def upload_json_to_postgres(json_path, db_uri, log_path="logs/nir_loader.log"):
    log_lines = []

    def log(msg):
        print(msg)
        log_lines.append(f"{datetime.now().isoformat()} - {msg}")

    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    engine = create_engine(db_uri)

    # === Create tables ===
    ddl_samples = """
    DROP TABLE IF EXISTS samples CASCADE;
    CREATE TABLE samples (
        sample_id UUID PRIMARY KEY,
        title VARCHAR(50),
        instrument_sn VARCHAR(50),
        spectrometer_sn VARCHAR(50),
        instrument_type VARCHAR(50),
        sample_description VARCHAR(50),
        date TIMESTAMP,
        smoothed VARCHAR(50),
        xunits VARCHAR(50),
        yunits VARCHAR(50),
        concentrations VARCHAR(50),
        perten_types VARCHAR(50),
        perten_repack VARCHAR(50),
        perten_repeat VARCHAR(50),
        perten_subscan VARCHAR(50),
        perten_goodrepacks VARCHAR(50),
        perten_totalrepacks VARCHAR(50),
        perten_rejected VARCHAR(50),
        perten_sampleinfo VARCHAR(50),
        xfactor FLOAT,
        yfactor FLOAT,
        firstx FLOAT,
        lastx FLOAT,
        npoints INTEGER,
        deltax FLOAT,
        xydata VARCHAR(50),
        raw_json JSONB,
        UNIQUE (title, date)
    );
    """

    ddl_nir_spectra = """
    DROP TABLE IF EXISTS nir_spectra CASCADE;
    CREATE TABLE nir_spectra (
        id SERIAL PRIMARY KEY,
        sample_id UUID REFERENCES samples(sample_id),
        title VARCHAR(50),
        date TIMESTAMP,
        x FLOAT,
        y FLOAT,
        y_index INTEGER
    );
    CREATE INDEX idx_nir_spectra_title_date_x ON nir_spectra (title, date, x);
    """

    with engine.connect() as conn:
        conn.execute(text(ddl_samples))
        log("✅ Ensured samples table exists")

        for stmt in ddl_nir_spectra.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt + ";"))

        conn.commit()
        log("✅ nir_spectra table created with index")

    # === Load existing samples ===
    with engine.connect() as conn:
        sample_map = dict(conn.execute(
            text("SELECT title || '|' || date::text AS key, sample_id FROM samples")
        ).fetchall())

    samples_to_insert = []
    spectra = []

    for block in data:
        title = block.get("TITLE")
        long_date = block.get("LONG DATE")
        if not title or not long_date:
            continue

        key = f"{title}|{long_date}"

        if key not in sample_map:
            sample_id = str(uuid.uuid4())
            sample_map[key] = sample_id

            samples_to_insert.append({
                "sample_id": sample_id,
                "title": title,
                "instrument_sn": block.get("INSTRUMENT S/N"),
                "spectrometer_sn": block.get("SPECTROMETER S/N"),
                "instrument_type": block.get("INSTRUMENT TYPE"),
                "sample_description": block.get("SAMPLE DESCRIPTION"),
                "date": long_date,
                "smoothed": block.get("SMOOTHED"),
                "xunits": block.get("XUNITS"),
                "yunits": block.get("YUNITS"),
                "concentrations": block.get("CONCENTRATIONS"),
                "perten_types": block.get("PERTEN-TYPES"),
                "perten_repack": block.get("PERTEN-REPACK"),
                "perten_repeat": block.get("PERTEN-REPEAT"),
                "perten_subscan": block.get("PERTEN-SUBSCAN"),
                "perten_goodrepacks": block.get("PERTEN-GOODREPACKS"),
                "perten_totalrepacks": block.get("PERTEN-TOTALREPACKS"),
                "perten_rejected": block.get("PERTEN-REJECTED"),
                "perten_sampleinfo": block.get("PERTEN-SAMPLEINFO"),
                "xfactor": float(block.get("XFACTOR", 0)),
                "yfactor": float(block.get("YFACTOR", 0)),
                "firstx": float(block.get("FIRSTX", 0)),
                "lastx": float(block.get("LASTX", 0)),
                "npoints": int(block.get("NPOINTS", 0)),
                "deltax": float(block.get("DELTAX", 0)),
                "xydata": block.get("XYDATA"),
                "raw_json": json.dumps(block),
            })
        else:
            sample_id = sample_map[key]

        for xy in block.get("XY", []):
            x = xy["X"]
            for idx, y_val in enumerate(xy["Y"]):
                spectra.append({
                    "sample_id": sample_id,
                    "title": title,
                    "date": long_date,
                    "x": x,
                    "y": y_val,
                    "y_index": idx
                })

    # === Insert data ===
    if samples_to_insert:
        df_samples = pd.DataFrame(samples_to_insert)
        df_samples.to_sql("samples", engine, if_exists="append", index=False)
        log(f"✅ Inserted {len(df_samples)} new samples into `samples`")

    if spectra:
        df_spectra = pd.DataFrame(spectra)
        df_spectra.to_sql("nir_spectra", engine, if_exists="append", index=False)
        log(f"✅ Uploaded {len(df_spectra)} NIR spectra points to PostgreSQL")
    else:
        log("⚠️ No NIR spectra uploaded")

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))
    log(f"📝 Log saved to {log_path}")
