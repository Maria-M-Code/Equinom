# Equinom Data Pipeline

This repository contains scripts to process and validate data from two lab devices:
- **Device 1** (Dumas) – Biochemical analysis in `.csv` format.
- **Device 2** (NIR Spectra) – Spectroscopy data in `.dx` (JCAMP-DX) format.

---

## ✅ Features

- Automatic folder scanning and file processing
- Parsing `.dx` files into structured JSON
- Data validation for both devices
- PostgreSQL schema creation and data loading
- Logging for every step
- SCD Type 2 versioning for metadata
- Minimal dependencies with Python

---

## 📦 Installation

Make sure you have Python 3.8 or higher.

Install all required packages with:

```bash
pip install -r requirements.txt
```

---

## ⚙️ Configuration

Edit the `params.json` file before running:

```json
{
  "file_name": "Device2_20241230",
  "dx_file": "input_files/Device2_20241230.dx",
  "json_file": "output_files/Device2_20241230.json",
  "dumas_input_dir": "input_files",
  "postgres": {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "your_password"
  }
}
```

---

## 🚀 How to Run

```bash
python main.py
```

This will:
- Parse NIR `.dx` file → JSON
- Upload NIR data to PostgreSQL (headers + spectra)
- Upload Dumas results from all `Device1*.csv` files in `dumas_input_dir`

---

## 🧪 Validations

### Dumas CSV:
- Required fields: `title`, `weight_mg`, `n_percent`, `protein_percent`
- No missing or duplicate titles
- Negative nitrogen %, protein > 100%, or invalid weights are flagged
- Date/time parsed and normalized

### NIR DX:
- Required header fields (e.g., `TITLE`, `LONG DATE`)
- XYDATA block validated and parsed
- Duplicate entries prevented using fingerprinting (MD5)

---

## 📂 Output Tables

- `nir_headers` – NIR sample metadata (SCD2 logic)
- `nir_spectra` – Spectral values by X,Y,index
- `dumas_results` – Biochemical values from Device1

---

## 📝 Logs

Logs are saved to:
- `logs/create_json.log`
- `logs/nir_loader.log`
- `logs/dumas_loader.log`