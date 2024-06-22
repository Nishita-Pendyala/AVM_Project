import snowflake.connector
import logging
import sys
import os

# Ensure the logs directory exists before setting up logging
if not os.path.exists('logs'):
    os.makedirs('logs')

# Logging configuration
logging.basicConfig(filename='logs/ddl_execution.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Print current working directory for debugging
print("Current Directory:", os.getcwd())

def execute_ddl(sql_path):
    try:
        # Read SQL file
        with open(sql_path, 'r') as file:
            sql_statements = file.read().split(';')

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user='NISHITA27',
            password='Chinnuepapro@27',
            account='mcvpuud-yr28894',
            warehouse='COMPUTE_WH',
            database='P01_EMP',
            schema='EMP_RAW'
        )
        cursor = conn.cursor()

        # Execute DDL statements
        for statement in sql_statements:
            if statement.strip():  # Ensure the statement is not empty
                cursor.execute(statement)

        conn.commit()
        conn.close()
        logging.info("DDL execution successful.")

    except Exception as e:
        logging.error(f"Error executing DDL: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python emp_create_tables.py <sql_path>")
        sys.exit(1)
    sql_path = sys.argv[1]
    if not os.path.exists(sql_path):
        print("Error: SQL file not found.")
        sys.exit(1)
    execute_ddl(sql_path)

# Print message indicating script completion
print("DDL execution completed.")
