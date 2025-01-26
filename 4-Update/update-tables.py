"""
This script updated the data in the DB using the data from '5-update' folder.
"""


import os
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

# --- Minimal change: Read connection parameters from a text file ---
with open(r'C:\Users\Public\CLDB\cldb_params.txt', 'r', encoding='utf-8') as f:  # --- ToDo: change to your path
    lines = [line.strip() for line in f]
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD = lines

# Excel-file path
xlsx_directory = r"C:\Users\Public\CLDB\upload\5-update"  # --- ToDo: change to your path


def update_table_from_excel(conn, xlsx_file, table_name):
    df = pd.read_excel(xlsx_file)
    df.columns = [col.lower() for col in df.columns]

    # Getting datatypes
    with conn.cursor() as cur:
        # Looking for id column
        cur.execute(f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY';
        """)
        pk_column = cur.fetchone()[0]

        # Getting column names
        cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """)
        columns_info = cur.fetchall()
    columns_data_types = dict(columns_info)

    # Converting data types
    for col in df.columns:
        db_type = columns_data_types.get(col)
        if db_type:
            if db_type.startswith('smallint'):
                df[col] = df[col].astype('Int16')
            elif db_type.startswith('integer'):
                df[col] = df[col].astype('Int32')
            elif db_type.startswith('bigint'):
                df[col] = df[col].astype('Int64')
            elif db_type.startswith('numeric') or db_type.startswith('double') or db_type.startswith('real'):
                df[col] = df[col].astype(float)
            elif db_type.startswith('character') or db_type.startswith('text'):
                df[col] = df[col].astype(str)
            elif db_type.startswith('boolean'):
                df[col] = df[col].astype(bool)
            elif db_type.startswith('date'):
                df[col] = pd.to_datetime(df[col]).dt.date
            elif db_type.startswith('timestamp'):
                df[col] = pd.to_datetime(df[col])
            # Add more types if needed

    # Replace pd.NA with None
    df = df.replace({pd.NA: None})

    # Checking smallint columns
    for col in df.columns:
        db_type = columns_data_types.get(col)
        if db_type and db_type.startswith('smallint'):
            min_val = df[col].min()
            max_val = df[col].max()
            print(f"Column '{col}' — min: {min_val}, max: {max_val}")
            if min_val is not None and min_val < -32768:
                print(f"Warning: Some values in '{col}' are lower than min limit for smallint.")
            if max_val is not None and max_val > 32767:
                print(f"Warning: Some values in '{col}' are higher than max limit for smallint.")

    # Preparing data for update
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ', '.join(list(df.columns))

    # Building the INSERT with ON CONFLICT
    conflict_target = pk_column
    update_cols = [col for col in df.columns if col != conflict_target]
    update_stmt = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])

    query = f"""
        INSERT INTO {table_name} ({cols}) VALUES %s
        ON CONFLICT ({conflict_target}) DO UPDATE SET {update_stmt};
    """

    # Data update
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, tuples)
            conn.commit()
            print(f"Data from {xlsx_file} updated in table {table_name}.")
    except Exception as e:
        print(f"Error while updating data: {e}")
        raise


try:
    # Connection to the DB
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    print("Connection established.")

    # Reading the files in the directory
    for filename in os.listdir(xlsx_directory):
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            xlsx_file_path = os.path.join(xlsx_directory, filename)
            table_name = os.path.splitext(filename)[0]  # File name without extension
            print(f"Processing: {filename}")

            # Updating the table from Excel file
            update_table_from_excel(conn, xlsx_file_path, table_name)

    # Closing connection
    conn.close()
    print("Connection closed.")

except Exception as e:
    print(f"Error: {e}")
