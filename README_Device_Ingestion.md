# Structured Device Data Ingestion and Versioning

## 1. Overview

This project handles ingestion, transformation, and storage of data from two analytical devices:
- **Device 1 – Dumas**: Produces biochemical analysis (e.g., protein and moisture).
- **Device 2 – NIR (Near-Infrared Reflectance)**: Produces absorption spectrum.

Device 2 outputs are parsed from `.dx` format into structured JSON and stored in a PostgreSQL database with historical versioning (SCD Type 2) using `nir_headers` and `nir_spectra` tables.

---

## 2. Database Schema

### `nir_headers`

Stores one row per `title` per version. Historical changes are tracked using SCD2 strategy.

```sql
CREATE TABLE nir_headers (
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
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    UNIQUE (title, valid_from)
);
```

### `nir_spectra`

Stores spectrum points for each sample, linked via `title` and `date`.

```sql
CREATE TABLE nir_spectra (
    id SERIAL PRIMARY KEY,
    title VARCHAR(50),
    date TIMESTAMP,
    x FLOAT,
    y FLOAT,
    y_index INTEGER
);
```

---

## 3. Change Detection (SCD Type 2)

- Changes are tracked using a fingerprint hash of specific fields.
- When a new record differs from the latest version:
  - The old version is marked `is_current = false` and `valid_to = current record's date`.
  - A new row is inserted with `valid_from = date`, `valid_to = 2099-01-01`.
- If there are no changes, no new row is inserted.

---

## 4. Data Parsing

- `.dx` files are split into blocks by `##TITLE=`.
- Metadata is parsed line by line.
- `XYDATA` values are extracted and structured as an array of `{X, Y[]}`.

---

## 5. Fingerprint Fields

Only the following fields are considered when comparing changes:

```
instrument_sn, spectrometer_sn, instrument_type, sample_description, smoothed,
xunits, yunits, concentrations, perten_types, perten_repack, perten_repeat,
perten_subscan, perten_goodrepacks, perten_totalrepacks, perten_rejected,
perten_sampleinfo, xfactor, yfactor, firstx, lastx, npoints, deltax
```

---

## 6. Logging

Each step is logged:
- Input files, number of blocks parsed
- Skipped blocks
- New versions inserted or identical entries skipped
- Output paths are logged for traceability

Logs are saved to:
- `logs/create_json.log`
- `logs/nir_loader.log`

---

## 7. Extensibility

This structure supports:
- Integration with Airflow or other orchestration tools
- Ingestion of additional device types
- Downstream analytics or visualization tools