import logging
import os
import snowflake.connector
import pandas as pd

# Configuration
SNOWFLAKE_CONFIG = {
    'user': 'NISHITA27',
    'password': 'Chinnuepapro@27',
    'account': 'mcvpuud-yr28894',
    'warehouse': 'COMPUTE_WH',
    'database': 'P01_EMP',
    'schema_raw': 'EMP_RAW',
    'schema_proc': 'EMP_PROC'
}

# Setup logging
log_dir = os.path.abspath('logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, 'emp_data_processing.log'),
    level=logging.DEBUG,  # Change to DEBUG level to capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Ensure extracts directory exists
extracts_dir = os.path.abspath('../extracts')
if not os.path.exists(extracts_dir):
    os.makedirs(extracts_dir)

class DataProcessing:
    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect(self):
        try:
            logging.debug("Attempting to connect to Snowflake")
            self.conn = snowflake.connector.connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema_raw']
            )
            logging.info("Connected to Snowflake successfully.")
        except Exception as e:
            logging.error(f"Error connecting to Snowflake: {e}")
            exit(1)

    def execute_query(self, query):
        try:
            logging.debug(f"Executing query: {query}")
            cursor = self.conn.cursor()
            cursor.execute(query)
            logging.info(f"Executed query: {query}")
            return cursor
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            return None

    def generate_report(self, query, file_name):
        try:
            logging.debug(f"Generating report for query: {query}")
            cursor = self.execute_query(query)
            if cursor:
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                file_path = os.path.join(extracts_dir, file_name)
                df.to_csv(file_path, sep='|', index=False)
                logging.info(f"Report generated: {file_name}")
        except Exception as e:
            logging.error(f"Error generating report {file_name}: {e}")

    def process_data(self):
        try:
            logging.debug("Starting data processing")
            # Create EMP_PROC schema
            self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {self.config['schema_proc']}")
            self.conn.cursor().execute(f"USE SCHEMA {self.config['schema_proc']}")

            # Query 1: Employees with salaries greater than their managers
            self.execute_query("""
                CREATE OR REPLACE TABLE emp_sal_greater_mngr AS
                SELECT e1.essn, e1.salary AS emp_salary, e1.super_ssn, e2.salary AS mngr_salary
                FROM EMP_RAW.EMPLOYEE e1
                JOIN EMP_RAW.EMPLOYEE e2 ON e1.super_ssn = e2.essn
                WHERE e1.salary > e2.salary;
            """)
            self.generate_report("SELECT * FROM emp_sal_greater_mngr", "emp_sal_greater_mngr.txt")

            # Query 2: Employees working on projects of other departments
            self.execute_query("""
                CREATE OR REPLACE TABLE emp_project_dept AS
                SELECT e.essn, p.pname, ed.dname AS emp_dept_name, pd.dname AS proj_dept_name
                FROM EMP_RAW.EMPLOYEE e
                JOIN EMP_RAW.DEPARTMENT ed ON e.dno = ed.dnumber
                JOIN EMP_RAW.WORKS_ON w ON e.essn = w.essn
                JOIN EMP_RAW.PROJECT p ON w.pno = p.pnumber
                JOIN EMP_RAW.DEPARTMENT pd ON p.dnum = pd.dnumber
                WHERE e.dno <> p.dnum;
            """)
            self.generate_report("SELECT * FROM emp_project_dept", "emp_project_dept.txt")

            # Query 3: Department with the least number of employees
            self.execute_query("""
                CREATE OR REPLACE TABLE emp_dept_least AS
                SELECT d.dname, d.dnumber, COUNT(e.essn) AS no_of_emp
                FROM EMP_RAW.DEPARTMENT d
                LEFT JOIN EMP_RAW.EMPLOYEE e ON d.dnumber = e.dno
                GROUP BY d.dname, d.dnumber
                ORDER BY no_of_emp ASC
                LIMIT 1;
            """)
            self.generate_report("SELECT * FROM emp_dept_least", "emp_dept_least.txt")

            # Query 4: Total number of hours spent on projects by employees with dependents
            self.execute_query("""
                CREATE OR REPLACE TABLE emp_tot_hrs_spent AS
                SELECT e.essn, d.dependent_name, w.pno, SUM(w.hours) AS total_hours_spent
                FROM EMP_RAW.EMPLOYEE e
                JOIN EMP_RAW.DEPENDENT d ON e.essn = d.essn
                JOIN EMP_RAW.WORKS_ON w ON e.essn = w.essn
                GROUP BY e.essn, d.dependent_name, w.pno;
            """)
            self.generate_report("SELECT * FROM emp_tot_hrs_spent", "emp_tot_hrs_spent.txt")

            # Query 5: Full employee details with projects, departments, and dependents
            self.execute_query("""
                CREATE OR REPLACE TABLE emp_full_details AS
                SELECT 
                    e.essn, e.fname, e.minit, e.lname, e.bdate, e.address, e.sex, e.salary, e.super_ssn, 
                    d.dnumber AS dno, d.dname, dl.dlocation, 
                    p.pname, p.pnumber, p.plocation, 
                    SUM(w.hours) AS total_hours, 
                    dep.dependent_name, dep.sex AS dependent_sex, dep.relationship AS dependent_relation
                FROM P01_EMP.EMP_RAW.EMPLOYEE e
                JOIN P01_EMP.EMP_RAW.DEPARTMENT d ON e.dno = d.dnumber
                JOIN P01_EMP.EMP_RAW.DEPT_LOCATIONS dl ON d.dnumber = dl.dnumber
                JOIN P01_EMP.EMP_RAW.WORKS_ON w ON e.essn = w.essn
                JOIN P01_EMP.EMP_RAW.PROJECT p ON w.pno = p.pnumber
                LEFT JOIN dependent dep ON e.essn = dep.essn
                GROUP BY e.essn, e.fname, e.minit, e.lname, e.bdate, e.address, e.sex, e.salary, e.super_ssn, 
                         d.dnumber, d.dname, dl.dlocation, 
                         p.pname, p.pnumber, p.plocation, 
                         dep.dependent_name, dep.sex, dep.relationship;
            """)
            self.generate_report("SELECT * FROM emp_full_details", "emp_full_details.txt")

            logging.info("Data processing completed successfully.")
        except Exception as e:
            logging.error(f"Error processing data: {e}")

if __name__ == "__main__":
    logging.debug("Starting the script")
    processor = DataProcessing(SNOWFLAKE_CONFIG)
    processor.connect()
    processor.process_data()
    processor.conn.close()
    logging.debug("Script finished")