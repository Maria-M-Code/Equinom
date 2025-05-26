from jcamp import jcamp_readfile
import json
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# קריאת הקובץ
data = jcamp_readfile("input_files/Device2_20241230.dx")

# המרת ndarray לרשימות רגילות כדי שיהיה אפשר להדפיס/לשמור כ-JSON
def convert_ndarrays(obj):
    if isinstance(obj, dict):
        return {k: convert_ndarrays(v) for k, v in obj.items()}
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, list):
        return [convert_ndarrays(i) for i in obj]
    else:
        return obj

clean_data = convert_ndarrays(data)

#print(clean_data["children"])

df = pd.DataFrame(clean_data["children"])
print(df["y"][0])


import pandas as pd

# נניח שה-DataFrame שלך נקרא df
# לדוגמה: df = pd.read_csv("nir_samples.csv")

import pandas as pd

def row_to_wide(row):
    y_vals = row["y"]

    try:
        first_x = float(row.get("firstx"))
        delta_x = float(row.get("deltax"))
        npoints = int(row.get("npoints", len(y_vals)))
    except Exception as e:
        print(f"Missing x generation parameters for row {row.get('title')}: {e}")
        return pd.Series(name=(row.get("title"), row.get("long date")))

    # אם יש יותר מדי y – לחתוך
    y_vals = y_vals[:npoints]

    # יצירת x_vals לפי FIRSTX + DELTAX
    x_vals = [first_x + i * delta_x for i in range(len(y_vals))]

    return pd.Series(data=y_vals, index=[str(int(x)) for x in x_vals], name=(row["title"], row["long date"]))


# יצירת wide format
spectra_df = df.apply(row_to_wide, axis=1)

# הגדרת MultiIndex מהעמודות המקוריות
spectra_df.index = pd.MultiIndex.from_frame(df[["title", "long date"]])
spectra_df.index.names = ["title", "long_date"]

# הוספת מטא-דאטה (לא כולל title/date)
meta_cols = [ "title", "long date", "instrument s/n", "instrument type", "spectrometer s/n"]
meta_df = df[ meta_cols].set_index(["title", "long date"])

# איחוד עם הספקטרום
final_df = meta_df.join(spectra_df).reset_index()

# תוצאה
print(final_df.columns)

def load_params(param_file="params/params.json"):
    with open(param_file, "r", encoding="utf-8") as f:
        return json.load(f)


params = load_params()
pg = params["postgres"]
postgres_uri = f"postgresql+psycopg2://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}"


engine = create_engine(postgres_uri)


final_df.reset_index().drop("long_date", axis=1).to_sql("final_df", engine, if_exists="replace", index=False)