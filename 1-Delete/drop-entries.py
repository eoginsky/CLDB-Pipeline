"""
This script deletes entries from the database. How it works:
1. It goes through the 'drop' folder
2. Opens each Excel file from there, identifies the DB table with the same name as the Excel file
3. Searches for 'id' column, deletes entries in the DB table with the same ids
"""

import os
import pandas as pd
import psycopg2
from psycopg2 import sql

# Get DB connection parameters
with open(r'C:\Users\Public\CLDB\cldb_params.txt', 'r', encoding='utf-8') as f:  # --- ToDo: change to your path
    lines = [line.strip() for line in f]
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD = lines

# Excel files folder
EXCEL_FILES_DIR = r"C:\Users\Public\CLDB\drop"  # --- ToDo: change to your path


# Function that gets ids from the file
def get_ids_from_excel(file_path):
    df = pd.read_excel(file_path)
    if 'id' not in df.columns:
        raise ValueError(f"There is no 'id' columns in {os.path.basename(file_path)}.")
    ids = df['id'].dropna().tolist()
    return ids


# Function that deletes rows from DB tables
def delete_ids_from_table(conn, table_name, ids):
    with conn.cursor() as cur:
        query = sql.SQL("DELETE FROM {table} WHERE id = ANY(%s)").format(
            table=sql.Identifier(table_name)
        )
        try:
            cur.execute(query, (ids,))
            print(f"{cur.rowcount} rows are deleted from '{table_name}'.")
        except Exception as e:
            print(f"Error while deleting from '{table_name}': {e}")

# Main script
# Connect to the DB
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    print("Connection established.")
except Exception as e:
    print(f"Error while connecting to the DB: {e}")
    exit(1)

# List Excel files
excel_files = [f for f in os.listdir(EXCEL_FILES_DIR) if f.endswith(('.xlsx', '.xls'))]
if not excel_files:
    print("No Excel files found in the folder.")

for excel_file in excel_files:
    file_path = os.path.join(EXCEL_FILES_DIR, excel_file)
    table_name = os.path.splitext(excel_file)[0]  # table name without extension
    print(f"\nProcessing '{excel_file}' for the table '{table_name}'.")

    try:
        ids = get_ids_from_excel(file_path)
        if not ids:
            print(f"There is no ids in '{excel_file}'. Skipping.")
            continue
        delete_ids_from_table(conn, table_name, ids)
    except Exception as e:
        print(f"Error while processing '{excel_file}': {e}")

conn.commit()
conn.close()
print("\nConnection closed.")
