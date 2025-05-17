# Dumas Data Loader with Validations

This script loads biochemical data from Device1-generated `.csv` files and stores the results into a PostgreSQL table named `dumas_results`.

## ✅ Key Features
- Automatic scanning of all files in a given folder matching `Device1*.csv`
- Data cleaning and normalization
- Date and time parsing
- Appends source file name to each record
- **Built-in data validation** to ensure consistency and data integrity
- Logs all steps into a log file

---

## 🔍 Validations Performed

Before loading the data into the database, the following validations are applied:

### 1. Required Columns
Ensures the presence of critical columns:
- `title`
- `weight_mg`
- `n_percent`
- `protein_percent`

If any of these are missing, the process is stopped.

### 2. Empty or Duplicate Titles
- Rows missing a `title` are dropped and logged.
- Duplicate titles are counted and logged (but not dropped).

### 3. Range and Value Checks
- Flags **negative values** in `n_percent`
- Flags **non-positive values** in `weight_mg`
- Flags `protein_percent` values **above 100**

### 4. DateTime Parsing
- Parses either `date_time` column, or combination of `date` + `time`
- Logs any rows where `datetime` could not be parsed

---

## 💾 Output Table Schema: `dumas_results`
Includes all standard Dumas result fields plus a `source_file` column.

---

## 📝 Logging
All steps, including errors, warnings, and loaded file counts are saved to a log file (default: `logs/dumas_loader.log`).

---

## ✨ Example Call
```python
upload_dumas_to_postgres("input_files", postgres_uri)
```

This will:
- Read all `Device1*.csv` files from `input_files/`
- Validate and clean the data
- Load into PostgreSQL