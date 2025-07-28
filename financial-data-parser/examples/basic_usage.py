"""
End-to-end demo – Phase-1 (Excel info) + Phase-2 (type detection)
             + Phase-3 (format parsing) + Phase-4 (DataStorage)
PERFORMANCE OPTIMIZED VERSION WITH FIXES
"""

import sys
import pandas as pd
from pathlib import Path
from decimal import Decimal
import time
from typing import List, Dict, Tuple

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.core.excel_processor import ExcelProcessor
from src.core.type_detector import TypeDetector
from src.core.format_parser import FormatParser
from src.core.data_storage import DataStorage

def timed(message):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            print(f"\n⏱️  {message} took: {end - start:.4f} seconds")
            return result
        return wrapper
    return decorator

@timed("Phase-1: Excel info extraction")
def phase1_excel_info(file_list: List[str]) -> Tuple[ExcelProcessor, Dict]:
    ep = ExcelProcessor()
    ep.load_files(file_list)  # This now loads and parses all data
    
    # Get sheet info (now includes dtypes)
    info = ep.get_sheet_info()
    
    # Print file info
    for file_path, sheets in info.items():
        print(f"\n📁 File: {file_path}")
        for sheet_name, sheet_info in sheets.items():
            print(f"   📄 Sheet: {sheet_name}  rows={sheet_info['rows']}  cols={sheet_info['cols']}")
            
            # Get preview using the fixed preview_data method
            preview = ep.preview_data(file_path, sheet_name, rows=3)
            print("      preview:")
            print(preview.to_string())
            print("-"*60)
    
    return ep, info

@timed("Phase-2: Type detection")
def phase2_type_detection(ep, info):
    print("\n" + "=" * 70)
    print("PHASE-2  –  Column type detection")
    print("=" * 70)

    for file_path, sheets in info.items():
        for sheet, _ in sheets.items():
            df_preview = ep.preview_data(file_path, sheet, rows=200)
            results = TypeDetector.detect_all(df_preview)

            print(f"\n📊 {Path(file_path).name}  |  sheet: {sheet}")
            for col, (dtype, conf) in results.items():
                print(f"   {col:<35}  ->  {dtype:<8}  ({conf:.2f})")

@timed("Phase-3: FormatParser tests")
def phase3_format_parser_tests():
    print("\n" + "=" * 70)
    print("PHASE-3  –  FormatParser smoke test")
    print("=" * 70)

    test_amounts = ["$1,234.56", "€1.234,56", "₹1,23,456.78", "(1,234.56)", 
                   "1234.56-", "1.23K", "2.5M", "1.2B"]
    test_dates = ["12/31/2023", "31/12/2023", "2023-12-31", "31-Dec-2023", 
                 "Q1-24", "Quarter 1 2024", "Mar 2024", "March 2024", "44927"]

    # Amount parsing
    for val in test_amounts:
        parsed = FormatParser.parse_amount(val)
        print(f"{val:<15} -> {parsed}")

    # Date parsing
    for val in test_dates:
        parsed = FormatParser.parse_date(val)
        print(f"{val:<18} -> {parsed}")

@timed("Phase-4: DataStorage operations")
def phase4_datastorage_demo(ep, ledger_file):
    print("\n" + "=" * 70)
    print("PHASE-4  –  DataStorage demo")
    print("=" * 70)

    sheet = ep._workbooks[ledger_file].sheet_names[0]
    
    # Load only necessary columns
    columns_to_load = ["Posting Date", "Amount", "Currency Code", "Customer Name"]
    df_demo = ep.preview_data(ledger_file, sheet, rows=500, columns=columns_to_load)
    
    # Clean column names
    df_demo.columns = [col.lower().replace(" ", "_") for col in df_demo.columns]
    print(f"Loaded {len(df_demo)} records")

    # Parse amounts and dates
    print("\nParsing amounts and dates...")
    df_demo["amount_num"] = FormatParser.parse_amount_vectorized(df_demo["amount"])
    df_demo["posting_date"] = pd.to_datetime(df_demo["posting_date"], errors="coerce")

    # Clean data
    initial_count = len(df_demo)
    df_demo = df_demo.dropna(subset=["amount_num", "posting_date"])
    print(f"Cleaned data: {initial_count} -> {len(df_demo)} valid records")

    # Store data
    print("Storing data...")
    store = DataStorage()
    store.store(
        name="ledger",
        df=df_demo,
        column_types={"amount_num": "number", "posting_date": "datetime"},
        index_cols=["posting_date", "amount_num"]  # Add index for performance
    )

    # Query 1: Amount range
    print("\n--- Amount range query: 5,000 – 100,000 ---")
    result = store.query("ledger", amount_range=(Decimal("5000"), Decimal("100000")))
    print(f"Found {len(result)} records")
    if not result.empty:
        # FIX: Reset index to access amount_num as column
        print(result.reset_index()[["customer_name", "amount_num"]].head(10))

    # Query 2: Aggregation
    print("\n--- Aggregation: sum(amount) by currency code ---")
    agg = store.aggregate(
        "ledger",
        group_by=["currency_code"],
        measures={"amount_num": "sum"}
    )
    print(agg)

    # Query 3: Date range
    print("\n--- Date range query: Jan 2024 transactions ---")
    result = store.query("ledger", date_range=("2024-01-01", "2024-01-31"))
    print(f"Found {len(result)} records")
    if not result.empty:
        # FIX: Reset index to access columns
        print(result.reset_index()[["customer_name", "posting_date", "amount_num"]].head())

    # Query 4: SQL fallback
    print("\n--- SQL fallback: top 3 customers by total amount ---")
    sql = """
    SELECT customer_name, SUM(amount_num) AS total
    FROM ledger
    GROUP BY customer_name
    ORDER BY total DESC
    LIMIT 3
    """
    result = store.sql(sql)
    print(result)
    
    return store

if __name__ == "__main__":
    file_list = [
        "data/sample/KH_Bank.XLSX",
        "data/sample/Customer_Ledger_Entries_FULL.xlsx",
    ]

    ep, info = phase1_excel_info(file_list)
    phase2_type_detection(ep, info)
    phase3_format_parser_tests()
    
    ledger_file = file_list[1]
    store = phase4_datastorage_demo(ep, ledger_file)