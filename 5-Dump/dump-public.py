"""
This script reads all the  data from the public schema of the database
and saves it as separate Excel-tables.
If a DB table has more than 1 M rows, it will be split into 2 Excel files
"""

import os
import pandas as pd
from sqlalchemy import create_engine


# Read connection parameters
with open(r'C:\Users\Public\CLDB\cldb_params.txt', 'r', encoding='utf-8') as f:  # --- ToDo: change to your path
    lines = [line.strip() for line in f]
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD = lines

# Additional connection options
DB_OPTIONS = "target_session_attrs=read-write"

# Directory to save the .xlsx files
output_dir = r"C:\Users\Public\CLDB\dump"  # --- ToDo: change to your path


def dump_table_to_excel(engine, table_name, output_dir):
    chunk_size = 1_000_000  # Number of rows per chunk
    offset = 0
    part = 1
    first_iteration = True  # First iteration flag

    while True:
        # Construct chunk query
        chunk_query = f"SELECT * FROM public.{table_name} OFFSET {offset} LIMIT {chunk_size};"

        # Read data into DataFrame
        df = pd.read_sql(chunk_query, engine)

        if df.empty:
            if first_iteration:
                # Creating an empty Excel file
                columns_query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '{table_name}';
                """
                columns_df = pd.read_sql(columns_query, engine)
                empty_df = pd.DataFrame(columns=columns_df['column_name'])
                output_file = os.path.join(output_dir, f"{table_name}.xlsx")
                empty_df.to_excel(output_file, index=False, engine='openpyxl')
                print(f"Empty table {table_name} has been dumped to {output_file}")
            break

        # Define the output file path
        if part == 1:
            output_file = os.path.join(output_dir, f"{table_name}.xlsx")
        else:
            output_file = os.path.join(output_dir, f"{table_name}_part{part}.xlsx")

        # Write the DataFrame to an Excel file
        df.to_excel(output_file, index=False, engine='openpyxl')
        print(f"Part {part} of table {table_name} has been dumped to {output_file}")

        offset += chunk_size
        part += 1
        first_iteration = False  # First iteration flag


# Main part of the script
try:
    # Create SQLAlchemy engine
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?{DB_OPTIONS}"
    )
    print("Database connection established.")

    # Fetch the list of tables in the public schema
    query = """
    SELECT tablename 
    FROM pg_tables 
    WHERE schemaname = 'public';
    """
    tables = pd.read_sql(query, engine)

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Dump each table to a separate Excel file
    for table in tables['tablename']:
        dump_table_to_excel(engine, table, output_dir)

    print("All tables have been successfully dumped.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    print("Database connection closed.")
