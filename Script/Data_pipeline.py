import polars as pl
import mysql.connector
from mysql.connector import Error
import os
import kagglehub
import polars as pl

DATASET = "likithagedipudi/genz-slang-evolution-tracker-2020-2025"
DOWNLOAD_PATH = "data/raw"
OUTPUT_PATH = "data/processed"

# 1️⃣ Download dataset
os.makedirs(DOWNLOAD_PATH, exist_ok=True)
path = kagglehub.dataset_download(DATASET)

# 2️⃣ Find CSV files
csv_files = [f for f in os.listdir(path) if f.endswith(".csv")]
if not csv_files:
    raise FileNotFoundError("No CSV files found in the dataset")
csv_file = csv_files[0]

file_path = os.path.join(path, csv_file)

# 3️⃣ Read CSV using Polars
df = pl.read_csv(file_path)
print("Dataset loaded:")
print(df.head(5))
print(f"Columns: {df.columns}")

# 4️⃣ Save processed CSV
os.makedirs(OUTPUT_PATH, exist_ok=True)
output_file = os.path.join(OUTPUT_PATH, "genz_slang.csv")
df.write_csv(output_file)
print(f"Dataset saved as {output_file}")


# Lead the data into Database
# =========================
# CONFIG
# =========================
CSV_FILE = "data/processed/genz_slang.csv"  # Path to your processed CSV

DB_HOST = "localhost"
DB_USER = "root"            # Your MySQL username
DB_PASSWORD = "asad."       # Your MySQL password
DB_NAME = "genz_data"       # Database name (auto-created)
TABLE_NAME = "genz_slang"   # Table name (auto-created)
BATCH_SIZE = 1000           # Number of rows per batch insert If the data is not big you can remove the batch limit 

# =========================
# STEP 1 — READ CSV USING POLARS
# =========================
df = pl.read_csv(CSV_FILE)
print(f"Loaded {df.height} rows from CSV.")
print(f"Columns: {df.columns}")

# =========================
# STEP 2 — CONNECT TO MYSQL AND CREATE DATABASE
# =========================
conn = None
cursor = None

try:
    # Connect without database to create it if needed
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    conn.commit()
    print(f"✅ Database '{DB_NAME}' is ready")
    cursor.close()
    conn.close()

    # Reconnect with the database
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()
    print(f"✅ Connected to database '{DB_NAME}'")

    # =========================
    # STEP 3 — COMPUTE MAX LENGTHS FOR STRING COLUMNS
    # =========================
    max_lengths = {}
    for col, dtype in zip(df.columns, df.dtypes):
        if "Utf8" in str(dtype):
            max_len = df[col].apply(lambda x: len(
                str(x)) if x is not None else 0).max()
            max_lengths[col] = max_len + 10  # add buffer

    # =========================
    # STEP 4 — CREATE TABLE DYNAMICALLY
    # =========================
    def polars_to_mysql_dtype(polars_dtype, col_name):
        if "Int" in str(polars_dtype):
            return "INT"
        elif "Float" in str(polars_dtype):
            return "FLOAT"
        elif "Utf8" in str(polars_dtype):
            length = max_lengths.get(col_name, 255)
            return f"VARCHAR({length})"
        elif "Bool" in str(polars_dtype):
            return "TINYINT(1)"
        elif "Date" in str(polars_dtype) or "Datetime" in str(polars_dtype):
            return "DATETIME"
        else:
            return "VARCHAR(255)"

    columns_with_types = [
        f"`{col}` {polars_to_mysql_dtype(dtype, col)}"
        for col, dtype in zip(df.columns, df.dtypes)
    ]
    columns_sql = ",\n    ".join(columns_with_types)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {columns_sql}
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print(
        f"✅ Table '{TABLE_NAME}' created dynamically with smart VARCHAR lengths")

    # =========================
    # STEP 5 — INSERT DATA IN BATCHES
    # =========================
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {TABLE_NAME} ({', '.join(df.columns)}) VALUES ({placeholders})"
    data_to_insert = [tuple(row) for row in df.to_numpy()]

    for i in range(0, len(data_to_insert), BATCH_SIZE):
        batch = data_to_insert[i:i+BATCH_SIZE]
        cursor.executemany(insert_query, batch)
        conn.commit()
        print(f"✅ Inserted rows {i+1} to {i+len(batch)}")

except Error as e:
    print("❌ MySQL Error:", e)

finally:
    if cursor:
        cursor.close()
    if conn is not None and conn.is_connected():
        conn.close()
        print("✅ MySQL connection closed")

print("✅ The data has been succesfully inseted into Database ")