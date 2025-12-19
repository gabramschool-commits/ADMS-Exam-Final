import sqlite3
from pathlib import Path

STAGING_DB = Path("data/Staging/staging.db")

def diagnose():
    print("\n🧪 STAGING DATABASE DIAGNOSTICS\n")

    with sqlite3.connect(STAGING_DB) as conn:
        cursor = conn.cursor()

        # Get all tables
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table'
            ORDER BY name;
        """)
        tables = cursor.fetchall()

        for (table,) in tables:
            print(f"📦 TABLE: {table}")

            # Get columns
            cursor.execute(f"PRAGMA table_info({table});")
            columns = cursor.fetchall()

            print("Columns:")
            for col in columns:
                print(f"  - {col[1]}")

            # Row count
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = cursor.fetchone()[0]
            print(f"Row count: {count}\n")

if __name__ == "__main__":
    diagnose()
