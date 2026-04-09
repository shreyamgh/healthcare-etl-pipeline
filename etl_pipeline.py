import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import urllib
import logging
import smtplib
from email.message import EmailMessage

# 1.Logging SETUP
logging.basicConfig(
    filename='pipeline_monitor.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 2. Database Connection
server = 'localhost'
database = 'HealthcareETL'
username = 'sa'
password = 'Shreyam@170898' 
driver = '{ODBC Driver 18 for SQL Server}'

# 3. Email Alert Setup
SENDER_EMAIL = "singhshreyam00@gmail.com" 
SENDER_APP_PASSWORD = "rudl vuku gstv budl" 
RECEIVER_EMAIL = "singhshreyam00@gmail.com" 

params = urllib.parse.quote_plus(f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;")
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def send_failure_email(error_msg):
    """Sends an automated email if the pipeline crashes."""
    try:
        msg = EmailMessage()
        msg['Subject'] = "CRITICAL ALERT: ETL Pipeline Failed"
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECEIVER_EMAIL
        msg.set_content(f"The Healthcare Revenue ETL pipeline failed to run.\n\nError Details:\n{error_msg}\n\nPlease check the pipeline_monitor.log file immediately.")

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(SENDER_EMAIL, SENDER_APP_PASSWORD)
            smtp.send_message(msg)
        logging.info("Failure alert email sent successfully.")
    except Exception as email_e:
        logging.error(f"Failed to send alert email: {str(email_e)}")

def extract_and_load():
    try:
        logging.info("Starting ETL Pipeline...")
        
        # --- PROCESS PATIENTS ---
        df_patients = pd.read_csv('patients.csv', low_memory=False)
        df_patients.rename(columns={'FIRST': 'FIRST_NAME', 'LAST': 'LAST_NAME'}, inplace=True)
        df_patients.to_sql('Patients', con=engine, schema='Staging', if_exists='replace', index=False)

        # --- PROCESS ENCOUNTERS ---
        df_encounters = pd.read_csv('encounters.csv', low_memory=False)
        df_encounters.rename(columns={'START': 'START_DATE', 'STOP': 'STOP_DATE'}, inplace=True)
        df_encounters.to_sql('Encounters', con=engine, schema='Staging', if_exists='replace', index=False)
        
        # --- TRIGGER SQL TRANSFORMATION ---
        with engine.begin() as conn:
            conn.execute(text("EXEC Core.usp_ProcessRevenueLeakage"))
            # --- TRIGGER SQL TRANSFORMATION ---
        with engine.begin() as conn:
            conn.execute(text("EXEC Core.usp_ProcessRevenueLeakage"))
            
        # --- EXPORTING CLEAN DATA FOR TABLEAU PUBLIC ---
        logging.info("Exporting clean data to CSV for Tableau...")
        df_dashboard = pd.read_sql("SELECT * FROM Core.Fact_Revenue_Leakage", con=engine)
        df_dashboard.to_csv('tableau_dashboard_data.csv', index=False)
        logging.info("Tableau export complete!")

        logging.info("Pipeline Execution Completed Successfully.")
        print("Success! Check pipeline_monitor.log for details.")

        logging.info("Pipeline Execution Completed Successfully.")
        print("Success! Check pipeline_monitor.log for details.")

    except Exception as e:
        error_string = str(e)
        logging.error(f"PIPELINE CRASHED: {error_string}")
        print(f"Error: {error_string}. Sending alert email...")
        send_failure_email(error_string) # <-- THIS TRIGGERS THE EMAIL IF IT BREAKS!

if __name__ == "__main__":
    extract_and_load()