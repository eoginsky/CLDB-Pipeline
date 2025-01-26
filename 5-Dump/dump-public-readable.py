"""
This script performs custom SQL queries and save results as separate Excel files.
"""


import pandas as pd
import sqlalchemy
from pathlib import Path

# Read connection parameters
with open(r'C:\Users\Public\CLDB\cldb_params.txt', 'r', encoding='utf-8') as f:  # --- ToDo: change to your path
    lines = [line.strip() for line in f]
host, port, database, username, password = lines

# Keep db_type as is (not in the config file)
db_type = 'postgresql'

# Input and output folders
queries_folder = Path(r"C:\Users\Public\CLDB\human_dump\queries")  # --- ToDo: change to your path
results_folder = Path(r"C:\Users\Public\CLDB\human_dump\dump")  # --- ToDo: change to your path

# Maximum rows per Excel file (Excel limit is ~1,048,576 for .xlsx)
max_rows_per_file = 1_000_000

# Concatenating connection string
connection_string = f'{db_type}+psycopg2://{username}:{password}@{host}:{port}/{database}'

# Creating connection object
engine = sqlalchemy.create_engine(connection_string)

# Identifying all SQL-scripts in the folder
sql_files = list(queries_folder.glob('*.sql'))

# Performing the queries
for sql_file in sql_files:
    try:
        with sql_file.open('r', encoding='utf-8') as file:
            sql_query = file.read()
        df = pd.read_sql_query(sql_query, engine)

        # Check if dataframe fits in a single Excel file
        if len(df) <= max_rows_per_file:
            result_file = results_folder / (sql_file.stem + '.xlsx')
            df.to_excel(result_file, index=False)
            print(f"Query results for '{sql_file.name}' saved in '{result_file}'")
        else:
            # Split into multiple Excel files
            for i in range(0, len(df), max_rows_per_file):
                part_df = df.iloc[i:i + max_rows_per_file]
                part_num = (i // max_rows_per_file) + 1
                part_file = results_folder / f"{sql_file.stem}_part{part_num}.xlsx"
                part_df.to_excel(part_file, index=False)
                print(
                    f"Query results for '{sql_file.name}' (rows {i + 1} to {i + len(part_df)}) "
                    f"saved in '{part_file}'"
                )

    except Exception as e:
        print(f"Error while performing '{sql_file.name}': {e}")

print("All queries done.")
