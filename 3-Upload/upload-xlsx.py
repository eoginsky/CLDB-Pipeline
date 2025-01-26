"""
This script uploads data from the Excel files found in the '4-final' folder
into the DB-tables with the same names.
"""

import os
import psycopg2
import pandas as pd
from io import StringIO

# Read the connection parameters from a text file
with open(r'C:\Users\Public\CLDB\cldb_params.txt', 'r', encoding='utf-8') as f:  # --- ToDo: change to your path
    lines = [line.strip() for line in f]
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD = lines

# Connection mode
DB_OPTIONS = "target_session_attrs=read-write"

# Directory containing the .xlsx files
xlsx_directory = r"C:\Users\Public\CLDB\upload\4-final"  # --- ToDo: change to your path


def copy_xlsx_to_table(conn, xlsx_file, table_name):
    df = pd.read_excel(xlsx_file)

    # Convert float columns with integer values to nullable integer types
    for col in df.columns:
        if pd.api.types.is_float_dtype(df[col]):
            if df[col].dropna().apply(float.is_integer).all():
                df[col] = df[col].astype('Int64')

    # Convert DataFrame to CSV format in memory
    csv_data = StringIO()
    df.to_csv(csv_data, index=False, header=True, sep=';', encoding='utf-8', na_rep='\\N')
    csv_data.seek(0)

    # Use the COPY command to upload the data
    sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ';' NULL '\\N' ENCODING 'UTF8'"
    with conn.cursor() as cur:
        cur.copy_expert(sql, csv_data)
        conn.commit()
        print(f"Data from {xlsx_file} has been successfully copied to {table_name}.")

# Main script
try:
    # Establish a database connection
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        sslmode="verify-full",
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        options=f"-c {DB_OPTIONS}"
    )
    print("Database connection established.")

    # Iterate through each .xlsx file in the directory
    for filename in os.listdir(xlsx_directory):
        if filename.endswith(".xlsx"):
            xlsx_file_path = os.path.join(xlsx_directory, filename)
            table_name = os.path.splitext(filename)[0]  # Assumes table name matches file name
            copy_xlsx_to_table(conn, xlsx_file_path, table_name)

    # Close the database connection
    conn.close()
    print("Database connection closed.")

except Exception as e:
    print(f"An error occurred: {e}")
