import pandas as pd
import os

# Specify the source and destination directories
source_folder = r'C:\Users\Public\CLDB\dump'   # --- ToDo: change to your path
destination_folder = r'C:\Users\Public\CLDB\templates'  # --- ToDo: change to your path

# Create the destination folder if it doesn't exist
os.makedirs(destination_folder, exist_ok=True)

# Iterate over each Excel file in the source folder
for filename in os.listdir(source_folder):
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        print(f"Processing file: {filename}")
        file_path = os.path.join(source_folder, filename)

        try:
            # Read the Excel file
            df = pd.read_excel(file_path)

            # Keep only headers
            df = df.iloc[:0]

            # Check if 'response_id' column exists
            if 'response_id' in df.columns:
                idx = df.columns.get_loc('response_id')
                # Drop 'response_id' column
                df = df.drop(columns=['response_id'])
                # Insert 'survey_id' and 'sid' columns at the position of 'response_id'
                new_columns = df.columns.tolist()
                new_columns[idx:idx] = ['survey_id', 'sid']
                df = df.reindex(columns=new_columns)
                # Initialize the new columns with empty strings
                df['survey_id'] = ''
                df['sid'] = ''

            # Save the modified DataFrame to the destination folder
            destination_path = os.path.join(destination_folder, filename)
            df.to_excel(destination_path, index=False)

        except Exception as e:
            print(f"An error occurred while processing {filename}: {e}")
