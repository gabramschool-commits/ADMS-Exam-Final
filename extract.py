import pandas as pd
import sqlite3
from pathlib import Path

STAGING_DB = Path("data/Staging/staging.db")
STAGING_DB.parent.mkdir(parents=True, exist_ok=True)

def run_extract():
    """Load CSV source files into the staging database."""
    with sqlite3.connect(STAGING_DB) as conn:
        # Japan
        japan_files = {
            "japan_store_sales_data": "data/Source/japan_store/sales_data.csv",
            "japan_store_japan_Customers": "data/Source/japan_store/japan_Customers.csv",
            "japan_store_japan_items": "data/Source/japan_store/japan_items.csv",
            "japan_store_japan_branch": "data/Source/japan_store/japan_branch.csv",
            "japan_store_japan_payment": "data/Source/japan_store/japan_payment.csv",
        }
        for table, file in japan_files.items():
            df = pd.read_csv(file)
            df.to_sql(table, conn, if_exists="replace", index=False)
            print(f"[EXTRACT] {table} loaded")

        # Myanmar
        myanmar_files = {
            "myanmar_store_sales_data": "data/Source/myanmar_store/sales_data.csv",
            "myanmar_store_myanmar_customers": "data/Source/myanmar_store/myanmar_customers.csv",
            "myanmar_store_myanmar_items": "data/Source/myanmar_store/myanmar_items.csv",
            "myanmar_store_myanmar_branch": "data/Source/myanmar_store/myanmar_branch.csv",
            "myanmar_store_myanmar_payment": "data/Source/myanmar_store/myanmar_payment.csv",
        }
        for table, file in myanmar_files.items():
            df = pd.read_csv(file)
            df.to_sql(table, conn, if_exists="replace", index=False)
            print(f"[EXTRACT] {table} loaded")
