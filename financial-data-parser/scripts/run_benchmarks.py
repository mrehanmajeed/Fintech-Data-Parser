#!/usr/bin/env python3
"""
Optimized Performance Benchmark for Financial Data Pipeline
Measures all critical components with vectorized operation support
"""

import sys
import time
import pandas as pd
from pathlib import Path
from decimal import Decimal
from typing import List, Dict, Tuple
import matplotlib.pyplot as plt

sys.path.append(str(Path(__file__).resolve().parent.parent))

# Import YOUR implementations
from src.core.excel_processor import ExcelProcessor
from src.core.type_detector import TypeDetector
from src.core.format_parser import FormatParser
from src.core.data_storage import DataStorage

class BenchmarkRunner:
    def __init__(self):
        self.results = {}
        self.test_files = [
            "data/sample/KH_Bank.XLSX",
            "data/sample/Customer_Ledger_Entries_FULL.xlsx",
        ]
        
    def _timed(self, operation: str):
        """Timing decorator with automatic result capture"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                start = time.time()
                result = func(*args, **kwargs)
                duration = time.time() - start
                self.results[operation] = duration
                print(f"\n⏱️  {operation}: {duration:.4f} seconds")
                return result
            return wrapper
        return decorator

    def benchmark_excel_loading(self) -> ExcelProcessor:
        """Benchmark Excel file loading"""
        ep = ExcelProcessor()
        start = time.time()
        ep.load_files(self.test_files)
        duration = time.time() - start
        self.results["ExcelProcessor.load_files"] = duration
        print(f"\n⏱️  ExcelProcessor.load_files: {duration:.4f} seconds")
        return ep

    def benchmark_metadata_extraction(self, ep: ExcelProcessor):
        """Benchmark metadata extraction"""
        start = time.time()
        info = ep.get_sheet_info()
        duration = time.time() - start
        self.results["ExcelProcessor.get_sheet_info"] = duration
        print(f"\n⏱️  ExcelProcessor.get_sheet_info: {duration:.4f} seconds")
        return info

    def benchmark_type_detection(self, ep: ExcelProcessor, info: dict):
        """Benchmark type detection performance"""
        start = time.time()
        for file_path, sheets in info.items():
            for sheet in sheets.keys():
                df = ep.preview_data(file_path, sheet, rows=200)
                TypeDetector.detect_all(df)
        duration = time.time() - start
        self.results["TypeDetector.detect_all"] = duration
        print(f"\n⏱️  TypeDetector.detect_all: {duration:.4f} seconds")

    def benchmark_vectorized_parsing(self, ep: ExcelProcessor):
        """Benchmark vectorized amount parsing"""
        ledger = self.test_files[1]
        sheet = ep._workbooks[ledger].sheet_names[0]
        df = ep.preview_data(ledger, sheet, rows=1000)
        
        start = time.time()
        FormatParser.parse_amount_vectorized(df["Amount"])
        duration = time.time() - start
        self.results["FormatParser.parse_amount_vectorized"] = duration
        print(f"\n⏱️  FormatParser.parse_amount_vectorized: {duration:.4f} seconds")

    def benchmark_data_storage(self, ep: ExcelProcessor):
        """Benchmark data storage operations"""
        ledger = self.test_files[1]
        sheet = ep._workbooks[ledger].sheet_names[0]
        df = ep.preview_data(ledger, sheet, rows=500, 
                            columns=["Posting Date", "Amount"])
        
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]
        df["amount_num"] = FormatParser.parse_amount_vectorized(df["amount"])
        df["posting_date"] = pd.to_datetime(df["posting_date"], errors="coerce")
        df = df.dropna(subset=["amount_num", "posting_date"])
        
        store = DataStorage()
        
        start = time.time()
        store.store(
            "benchmark", 
            df,
            {"amount_num": "number", "posting_date": "datetime"},
            index_cols=["posting_date", "amount_num"]
        )
        duration = time.time() - start
        self.results["DataStorage.store"] = duration
        print(f"\n⏱️  DataStorage.store: {duration:.4f} seconds")
        return store

    def benchmark_queries(self, store: DataStorage):
        """Benchmark query performance"""
        start = time.time()
        # Amount range query
        store.query("benchmark", amount_range=(Decimal("1000"), Decimal("5000")))
        # Date range query
        store.query("benchmark", date_range=("2023-01-01", "2023-12-31"))
        duration = time.time() - start
        self.results["DataStorage.query_range"] = duration
        print(f"\n⏱️  DataStorage.query_range: {duration:.4f} seconds")

    def benchmark_full_pipeline(self):
        """Benchmark end-to-end pipeline"""
        print("\n⏱️  Starting End-to-End Pipeline Benchmark")
        start = time.time()
        
        ep = ExcelProcessor()
        ep.load_files(self.test_files)
        info = ep.get_sheet_info()
        
        # Type detection
        for file_path, sheets in info.items():
            for sheet in sheets.keys():
                df = ep.preview_data(file_path, sheet, rows=200)
                TypeDetector.detect_all(df)
        
        # Storage demo
        ledger = self.test_files[1]
        sheet = ep._workbooks[ledger].sheet_names[0]
        df = ep.preview_data(ledger, sheet, rows=500, 
                            columns=["Posting Date", "Amount"])
        
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]
        df["amount_num"] = FormatParser.parse_amount_vectorized(df["amount"])
        df["posting_date"] = pd.to_datetime(df["posting_date"], errors="coerce")
        df = df.dropna(subset=["amount_num", "posting_date"])
        
        store = DataStorage()
        store.store(
            "ledger", 
            df,
            {"amount_num": "number", "posting_date": "datetime"},
            index_cols=["posting_date", "amount_num"]
        )
        
        # Run queries
        store.query("ledger", amount_range=(Decimal("1000"), Decimal("5000")))
        store.query("ledger", date_range=("2023-01-01", "2023-12-31"))
        
        duration = time.time() - start
        self.results["End-to-End Pipeline"] = duration
        print(f"\n⏱️  End-to-End Pipeline: {duration:.4f} seconds")

    def generate_report(self):
        """Create visual performance report"""
        # Convert results to DataFrame
        df = pd.DataFrame({
            "Operation": list(self.results.keys()),
            "Time (s)": list(self.results.values())
        }).sort_values("Time (s)", ascending=False)
        
        # Plot results
        plt.figure(figsize=(10, 6))
        plt.barh(df["Operation"], df["Time (s)"], color='skyblue')
        plt.xlabel('Time (seconds)')
        plt.title('Financial Pipeline Performance')
        plt.tight_layout()
        plt.savefig('performance_report.png')
        
        # Save raw data
        df.to_csv("performance_results.csv", index=False)
        print("\n✅ Report generated: performance_report.png")

    def run(self):
        print("🚀 Starting Financial Pipeline Benchmark")
        print("=" * 50)
        
        # Component benchmarks
        ep = self.benchmark_excel_loading()
        info = self.benchmark_metadata_extraction(ep)
        self.benchmark_type_detection(ep, info)
        self.benchmark_vectorized_parsing(ep)
        store = self.benchmark_data_storage(ep)
        self.benchmark_queries(store)
        
        # Full system test
        self.benchmark_full_pipeline()
        
        # Reporting
        self.generate_report()
        print("\n🔥 Benchmark Completed!")

if __name__ == "__main__":
    benchmark = BenchmarkRunner()
    benchmark.run()