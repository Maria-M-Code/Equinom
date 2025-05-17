import json
from functions.create_nir_json import create_structured_json
from functions.upload_nir_to_postgres import upload_json_to_postgres
from functions.upload_dumas_to_postgres import upload_dumas_to_postgres

def load_params(param_file="params/params.json"):
    with open(param_file, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    params = load_params()

    dx_file = params["dx_file"]
    json_file = params["json_file"]
    

    pg = params["postgres"]
    postgres_uri = f"postgresql+psycopg2://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}"

    print("📦 Step 1: Creating structured NIR JSON...")
    create_structured_json(dx_file, json_file)

    print("🛢 Step 2: Uploading NIR data to PostgreSQL...")
    upload_json_to_postgres(json_file, postgres_uri, log_path="logs/upload_json_to_postgres.log")

    print("🧪 Step 3: Uploading Dumas biochemical results...")
    upload_dumas_to_postgres(
    file_paths=params["dumas_files"],
    db_uri=postgres_uri,
    log_path="logs/upload_dumas_to_postgres.log"
)


if __name__ == "__main__":
    main()
