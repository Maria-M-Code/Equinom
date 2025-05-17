import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from hashlib import md5
import os

def normalize_block(block):
    return {
        "instrument_sn": block.get("INSTRUMENT S/N"),
        "spectrometer_sn": block.get("SPECTROMETER S/N"),
        "instrument_type": block.get("INSTRUMENT TYPE"),
        "sample_description": block.get("SAMPLE DESCRIPTION"),
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
        "source_file": block.get("SOURCE_FILE")
    }

def row_fingerprint(block_dict):
    content = json.dumps(block_dict, sort_keys=True)
    return md5(content.encode()).hexdigest()

def upload_json_to_postgres(json_path, db_uri, log_path="logs/nir_loader.log"):
    log_lines = []
    def log(msg):
        print(msg)
        log_lines.append(f"{datetime.now().isoformat()} - {msg}")

    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    engine = create_engine(db_uri)

    ddl_nir_headers = """
    CREATE TABLE IF NOT EXISTS nir_headers (
        title VARCHAR(50),
        date TIMESTAMP,
        instrument_sn VARCHAR(50),
        spectrometer_sn VARCHAR(50),
        instrument_type VARCHAR(50),
        sample_description VARCHAR(50),
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
        source_file VARCHAR(100),
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN DEFAULT TRUE,
        UNIQUE (title, valid_from)
    );
    """

    ddl_nir_spectra = """
    DROP TABLE IF EXISTS nir_spectra CASCADE;
    CREATE TABLE nir_spectra (
        id SERIAL PRIMARY KEY,
        title VARCHAR(50),
        date TIMESTAMP,
        x FLOAT,
        y FLOAT,
        y_index INTEGER
    );
    CREATE INDEX idx_nir_spectra_title_date_x ON nir_spectra (title, date, x);
    """

    with engine.connect() as conn:
        conn.execute(text(ddl_nir_headers))
        log("✅ Created nir_headers table (SCD2)")

        for stmt in ddl_nir_spectra.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt + ";"))
        conn.commit()
        log("✅ Created nir_spectra table with index")

    spectra = []

    with engine.connect() as conn:
        for block in data:
            title = block.get("TITLE")
            long_date = block.get("LONG DATE")
            if not title or not long_date:
                continue

            block_dict = normalize_block(block)
            fingerprint = row_fingerprint(block_dict)

            existing = conn.execute(text("""
                SELECT instrument_sn, spectrometer_sn, instrument_type, sample_description,
                       smoothed, xunits, yunits, concentrations, perten_types,
                       perten_repack, perten_repeat, perten_subscan,
                       perten_goodrepacks, perten_totalrepacks, perten_rejected,
                       perten_sampleinfo, xfactor, yfactor, firstx, lastx, npoints, deltax, source_file
                FROM nir_headers
                WHERE title = :title AND is_current = true
            """), {"title": title}).fetchone()

            insert_new = False

            if existing:
                existing_dict = dict(existing._mapping)
                existing_fp = row_fingerprint(existing_dict)
                if fingerprint != existing_fp:
                    conn.execute(text("""
                        UPDATE nir_headers
                        SET is_current = false, valid_to = :date
                        WHERE title = :title AND is_current = true
                    """), {"title": title, "date": long_date})
                    insert_new = True
                else:
                    log(f"⏩ Skipped unchanged: {title} ({long_date})")
            else:
                insert_new = True

            if insert_new:
                conn.execute(text("""
                    INSERT INTO nir_headers (
                        title, date, instrument_sn, spectrometer_sn,
                        instrument_type, sample_description, smoothed,
                        xunits, yunits, concentrations, perten_types,
                        perten_repack, perten_repeat, perten_subscan,
                        perten_goodrepacks, perten_totalrepacks, perten_rejected,
                        perten_sampleinfo, xfactor, yfactor, firstx, lastx,
                        npoints, deltax, source_file, valid_from, valid_to, is_current
                    ) VALUES (
                        :title, :date, :instrument_sn, :spectrometer_sn,
                        :instrument_type, :sample_description, :smoothed,
                        :xunits, :yunits, :concentrations, :perten_types,
                        :perten_repack, :perten_repeat, :perten_subscan,
                        :perten_goodrepacks, :perten_totalrepacks, :perten_rejected,
                        :perten_sampleinfo, :xfactor, :yfactor, :firstx, :lastx,
                        :npoints, :deltax, :source_file, :valid_from, :valid_to, true
                    )
                """), {
                    **block_dict,
                    "title": title,
                    "date": long_date,
                    "valid_from": long_date,
                    "valid_to": "2099-01-01 00:00:00"
                })
                log(f"🆕 Inserted new version for: {title} ({long_date})")

            for xy in block.get("XY", []):
                x = xy["X"]
                for idx, y_val in enumerate(xy["Y"]):
                    spectra.append({
                        "title": title,
                        "date": long_date,
                        "x": x,
                        "y": y_val,
                        "y_index": idx
                    })

        conn.commit()

    if spectra:
        df_spectra = pd.DataFrame(spectra)
        df_spectra.to_sql("nir_spectra", engine, if_exists="append", index=False)
        log(f"✅ Uploaded {len(df_spectra)} NIR spectra points")
    else:
        log("⚠️ No NIR spectra points found")

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))
    log(f"📝 Log saved to {log_path}")