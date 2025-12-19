import pandas as pd
import sqlite3
from pathlib import Path

STAGING_DB = Path("data/Staging/staging.db")
TRANSFORM_DIR = Path("data/Transformation")
TRANSFORM_DB = TRANSFORM_DIR / "transformation.db"

# Currency conversion
JPY_TO_USD = 0.0064
USD_TO_USD = 1


def clean_columns(df):
    """Remove quotes and whitespace from column names."""
    df.columns = df.columns.str.replace("'", "").str.strip()
    return df


def clean_data(df):
    """Drop fully empty rows and fill missing values."""
    df = df.dropna(how="all")
    numeric_cols = df.select_dtypes(include=['number']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    cat_cols = df.select_dtypes(include=['object']).columns
    df[cat_cols] = df[cat_cols].fillna("Unknown")
    return df


def convert_currency(df, price_col, rate):
    """Convert price column to standard currency."""
    if price_col in df.columns:
        df[price_col] = (
            pd.to_numeric(df[price_col], errors='coerce')
            .fillna(0) * rate
        )
    return df


def map_japan(s_conn):
    # ❌ REMOVED DISTINCT from sales table
    sales = clean_columns(
        pd.read_sql("SELECT * FROM japan_store_sales_data", s_conn)
    )
    customers = clean_columns(
        pd.read_sql("SELECT DISTINCT * FROM japan_store_japan_Customers", s_conn)
    )
    items = clean_columns(
        pd.read_sql("SELECT DISTINCT * FROM japan_store_japan_items", s_conn)
    )
    branches = clean_columns(
        pd.read_sql("SELECT DISTINCT * FROM japan_store_japan_branch", s_conn)
    )
    payments = clean_columns(
        pd.read_sql("SELECT DISTINCT * FROM japan_store_japan_payment", s_conn)
    )

    df = (
        sales
        .merge(customers, left_on="customer_id", right_on="id", how="left")
        .merge(items, left_on="product_id", right_on="id", how="left", suffixes=("", "_item"))
        .merge(branches, left_on="branch_id", right_on="id", how="left", suffixes=("", "_branch"))
        .merge(payments, left_on="payment", right_on="id", how="left", suffixes=("", "_payment"))
    )

    df = clean_data(df)

    # Add store column BEFORE conversion
    df["store"] = "Japan"

    # ✅ SAFE currency conversion (JPY → USD once)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df[df["price"] < 100000]   # sanity guard against already-USD values
    df["price"] = (df["price"] * JPY_TO_USD).round(2)

    # Standardize column names
    df = df.rename(columns={
        "membership": "customer_type",
        "product_name": "product_name",
        "category": "category",
        "name_branch": "branch_name",
        "name_payment": "payment_method"
    })

    # Strip and standardize string columns
    str_cols = [
        "customer_type", "product_name", "category",
        "branch_name", "payment_method", "gender"
    ]
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip().str.title()

    # Ensure numeric columns
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").fillna(0)

    # Remove unrealistic rows
    df = df[(df["quantity"] > 0) & (df["price"] > 0)]

    # Keep only relevant columns
    df = df[[
        "invoice_id", "date", "time", "store",
        "customer_id", "customer_type", "gender",
        "product_id", "product_name", "category",
        "price", "quantity",
        "branch_name", "payment_method", "rating"
    ]]

    return df


def map_myanmar(s_conn):
    sales = clean_columns(
        pd.read_sql("SELECT * FROM myanmar_store_sales_data", s_conn)
    )
    customers = clean_columns(
        pd.read_sql("SELECT * FROM myanmar_store_myanmar_customers", s_conn)
    )
    items = clean_columns(
        pd.read_sql("SELECT * FROM myanmar_store_myanmar_items", s_conn)
    )
    branches = clean_columns(
        pd.read_sql("SELECT * FROM myanmar_store_myanmar_branch", s_conn)
    )
    payments = clean_columns(
        pd.read_sql("SELECT * FROM myanmar_store_myanmar_payment", s_conn)
    )

    df = (
        sales
        .merge(customers, left_on="customer_id", right_on="id", how="left")
        .merge(items, left_on="product_id", right_on="id", how="left", suffixes=("", "_item"))
        .merge(branches, left_on="branch_id", right_on="id", how="left", suffixes=("", "_branch"))
        .merge(payments, left_on="payment", right_on="id", how="left", suffixes=("", "_payment"))
    )

    # ✅ CRITICAL FIX — explicit disambiguation
    df = df.rename(columns={
        "name_item": "product_name",     # correct product name
        "type_item": "category",
        "type": "customer_type",         # customer type
        "name_branch": "branch_name",
        "name_payment": "payment_method"
    })

    # ❌ REMOVE customer name to prevent pollution
    if "name" in df.columns:
        df = df.drop(columns=["name"])

    df = clean_data(df)

    # Add store column
    df["store"] = "Myanmar"

    # Numeric enforcement
    df["price"] = pd.to_numeric(df["price"], errors="coerce").round(2)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").fillna(0)

    # Standardize strings
    for col in [
        "customer_type", "product_name", "category",
        "branch_name", "payment_method", "gender"
    ]:
        df[col] = df[col].astype(str).str.strip().str.title()

    # Remove invalid rows
    df = df[(df["quantity"] > 0) & (df["price"] > 0)]

    return df[[ 
        "invoice_id", "date", "time", "store",
        "customer_id", "customer_type", "gender",
        "product_id", "product_name", "category",
        "price", "quantity",
        "branch_name", "payment_method", "rating"
    ]]


def transform_data():
    TRANSFORM_DIR.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(STAGING_DB) as s_conn, sqlite3.connect(TRANSFORM_DB) as t_conn:
        japan_df = map_japan(s_conn)
        myanmar_df = map_myanmar(s_conn)

        big_table = pd.concat([japan_df, myanmar_df], ignore_index=True)

        # ✅ FIX: store-scoped deduplication
        big_table = big_table.drop_duplicates(
            subset=["store", "invoice_id", "customer_id", "product_id"]
        )

        big_table = big_table.dropna(
            subset=["invoice_id", "customer_id", "product_id", "branch_name"]
        ).reset_index(drop=True)

        print(f"[TRANSFORM] Rows after cleaning: {len(big_table)}")

        big_table.to_sql(
            "all_sales_big_table",
            t_conn,
            if_exists="replace",
            index=False
        )

        print("[TRANSFORM] all_sales_big_table created")
