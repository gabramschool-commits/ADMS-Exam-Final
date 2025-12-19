import sqlite3
from pathlib import Path
import pandas as pd

TRANSFORM_DIR = Path("data/Transformation")
TRANSFORM_DB = TRANSFORM_DIR / "transformation.db"
TARGET_DB = Path("data/Presentation/analytics.db")

def load_big_table():
    TARGET_DB.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(TRANSFORM_DB) as t_conn, sqlite3.connect(TARGET_DB) as target_conn:
        df = pd.read_sql("SELECT * FROM all_sales_big_table", t_conn)
        df.to_sql("all_sales_big_table", target_conn, if_exists="replace", index=False)
        print("[LOAD] all_sales_big_table loaded into target database")
