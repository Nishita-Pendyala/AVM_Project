import logging
import os
import snowflake.connector
import pandas as pd
import numpy as np
import argparse

# Configuration
SNOWFLAKE_CONFIG = {
    'user': 'NISHITA27',
    'password': 'Chinnuepapro@27',
    'account': 'mcvpuud-yr28894',
    'warehouse': 'COMPUTE_WH',
    'database': 'P01_EMP',
    'schema': 'EMP_RAW',
}

class DataIngestion:
    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect(self):
        try:
            self.conn = snowflake.connector.connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema']
            )
            logging.info("Connected to Snowflake successfully.")
        except Exception as e:
            logging.exception("Error connecting to Snowflake.")
            exit(1)

    def create_table(self, table_name, columns):
        cursor = self.conn.cursor()
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.config['schema']}.{table_name} ({', '.join(columns)})
            """
            cursor.execute(create_table_query)
            logging.info(f"Table {table_name} created successfully.")
        except Exception as e:
            logging.exception(f"Error creating table {table_name}.")
            exit(1)
        finally:
            cursor.close()

    def ingest_data(self, file_path, table_name):
        try:
            if os.path.exists(file_path):
                data = pd.read_csv(file_path, delimiter='|', header=None, dtype=str)
                data.replace('NULL', np.nan, inplace=True)

                # Debug: Print the DataFrame to ensure it's read correctly
                print("DataFrame:")
                print(data)

                columns = [f'"COLUMN{i+1}" VARCHAR' for i in range(len(data.columns))]
                self.create_table(table_name, columns)

                cursor = self.conn.cursor()
                insert_query = f"INSERT INTO {self.config['schema']}.{table_name} VALUES ({', '.join(['%s' for _ in range(len(columns))])})"

                for row in data.itertuples(index=False, name=None):
                    # Replace NaN with None
                    cleaned_row = [None if pd.isna(value) else value for value in row]
                    print(f"Inserting row: {cleaned_row}")  # Debug line to print each row
                    cursor.execute(insert_query, cleaned_row)

                self.conn.commit()
                logging.info(f"Ingested {len(data)} rows into table {table_name}.")
            else:
                logging.error(f"File {file_path} does not exist.")
        except Exception as e:
            logging.exception(f"Error ingesting data from file {file_path} to table {table_name}.")
            exit(1)

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Snowflake connection closed.")

if __name__ == "__main__":
    logging.basicConfig(filename='logs/ingestion.log', level=logging.INFO)

    parser = argparse.ArgumentParser(description="Ingest data into Snowflake")
    parser.add_argument('input_dir', type=str, help='Directory containing input data files')

    args = parser.parse_args()
    input_dir = args.input_dir

    files_to_tables = {
        'dependent.txt': 'DEPENDENT',
        'department.txt': 'DEPARTMENT',
        'employee.txt': 'EMPLOYEE',
        'project.txt': 'PROJECT',
        'dept_locations.txt': 'DEPT_LOCATIONS',
        'works_on.txt': 'WORKS_ON',
    }

    ingestion = DataIngestion(SNOWFLAKE_CONFIG)
    ingestion.connect()

    for file_name, table_name in files_to_tables.items():
        file_path = os.path.join(input_dir, file_name)
        ingestion.ingest_data(file_path, table_name)

    ingestion.close()