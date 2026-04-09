# healthcare-etl-pipeline
# Enterprise Healthcare ETL Pipeline 🏥
**Automated Data Engineering Pipeline for Revenue Leakage Analysis**

### Overview
This project is an end-to-end Data Engineering pipeline built to identify hospital revenue leakage (denied or partially paid insurance claims). It automatically extracts 10,000+ raw healthcare records, profiles the data for anomalies, loads it into a containerized SQL Server data warehouse, and triggers a SQL Stored Procedure to perform the final financial calculations.

### Tech Stack
* **Extraction & Automation:** Python (Pandas, SQLAlchemy)
* **Database:** Microsoft SQL Server (Containerized via Docker on Apple Silicon)
* **Transformation:** Advanced SQL (Stored Procedures, Joins, Case Logic)
* **Monitoring:** Python Logging & Automated SMTP Email Alerts
* **Visualization:** Tableau

### Pipeline Architecture
1. **Extract & Profile:** Python scripts ingest messy `.csv` data (Synthea healthcare dataset) and perform quality assurance checks for negative claim costs and missing IDs.
2. **Load:** Data is pushed to the `Staging` schema in SQL Server via PyODBC.
3. **Transform:** Python triggers a Stored Procedure to join Patient and Encounter tables, calculating the exact `Revenue_Leakage` dollar amount and flagging `Is_Total_Denial`.
4. **Alerts & Export:** The pipeline features self-monitoring email alerts for failures and automatically exports the clean `Core.Fact_Revenue_Leakage` dataset for Tableau integration.

### Live Dashboard
The finalized pipeline feeds directly into this executive-level dashboard to visualize the financial impact:
👉 **[https://public.tableau.com/app/profile/shreyam.singh5190/viz/Healthcare_Revenue_Leakage_Analysis/Dashboard1?publish=yes]**
