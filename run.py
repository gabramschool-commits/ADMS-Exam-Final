# =============================
# run_etlprogram.py — Orchestrate ETL pipeline
# =============================
from extract import run_extract
from transform import transform_data
from load import load_big_table


def main():
    print(">>> Starting ETL Pipeline <<<\n")

    print("Step 1: Extracting data…")
    run_extract()
    print("Step 1 Complete.\n")

    print("Step 2: Transforming data…")
    transform_data()
    print("Step 2 Complete.\n")

    print("Step 3: Loading data…")
    load_big_table()
    print("Step 3 Complete.\n")

    print(">>> ETL Pipeline Finished Successfully! <<<")


if __name__ == "__main__":
    main()
