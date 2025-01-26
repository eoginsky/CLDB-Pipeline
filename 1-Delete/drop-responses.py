"""
This script takes the respondents' ids from 'delete-list.xlsx' file and delete
those responses from all the tables of the database, that contain 'response_id' field.
"""

import pandas as pd
import psycopg2
from psycopg2 import sql


# Read DB connection parameters
with open(r'C:\Users\Public\CLDB\cldb_params.txt', 'r', encoding='utf-8') as f:  # --- ToDo: change to your path
    lines = [line.strip() for line in f]
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD = lines

# Path to the Excel-file with the response_id
EXCEL_FILE_PATH = r"C:\Users\Public\CLDB\delete_list.xlsx"  # --- ToDo: change to your path


# Function that gets response_id to delete from DB
def get_response_ids(excel_file):
    df = pd.read_excel(excel_file)
    if 'response_id' not in df.columns:
        raise ValueError("Excel-file 'delete_list.xlsx' has no 'response_id' column.")
    response_ids = df['response_id'].dropna().unique().tolist()
    return response_ids


# Function that lists all tables that contain response_id
def get_tables_with_response_id(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.columns
            WHERE column_name = 'response_id'
            AND table_schema NOT IN ('information_schema', 'pg_catalog')
        """)
        tables = cur.fetchall()
    return tables


# Function to delete the rows with the response_ids
def delete_response_ids_from_tables(conn, tables, response_ids):
    with conn.cursor() as cur:
        for schema, table in tables:
            print(f"Deleting from {schema}.{table}...")
            query = sql.SQL("DELETE FROM {}.{} WHERE response_id = ANY(%s)").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
            try:
                cur.execute(query, (response_ids,))
                print(f"{cur.rowcount} rows have been deleted from {schema}.{table}.")
            except Exception as e:
                print(f"Error while deleting from {schema}.{table}: {e}")
        conn.commit()

# Main script
try:
    # Reading response_id from the Excel-file
    response_ids = get_response_ids(EXCEL_FILE_PATH)
    if not response_ids:
        print("There are no response_id in the Excel-file.")
    print(f"{len(response_ids)} response_id are found to be deleted.")

    # Connecting to the database
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    print("Connection  established.")

    # Reading all tables with response_id
    tables = get_tables_with_response_id(conn)
    if not tables:
        print("Tables with 'response_id' not found.")

    print(f"{len(tables)} tables with response_id found.")

    # Dropping response_id from tables
    delete_response_ids_from_tables(conn, tables, response_ids)
    print("Deletion completed.")

# What to do if there is an error
except Exception as e:
    print(f"Error: {e}")

# Close connection
finally:
    if 'conn' in locals() and conn:
        conn.close()
        print("Connection closed.")
