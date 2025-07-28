"""
DataStorage – Phase-4
Fast, memory-efficient storage with range & aggregation support.
FIXED AGGREGATION METHOD
"""

from __future__ import annotations

import sqlite3
import threading  # Add this import
from decimal import Decimal
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd

class DataStorage:
    def __init__(self) -> None:
        self.dataframes: Dict[str, pd.DataFrame] = {}
        self.indexes: Dict[str, Dict[str, Any]] = {}
        # Thread-local storage for SQLite connections
        self.local = threading.local()
    
    def _get_sqlite_conn(self):
        """Get thread-specific SQLite connection"""
        if not hasattr(self.local, "conn"):
            self.local.conn = sqlite3.connect(":memory:")
            self.local.conn.row_factory = sqlite3.Row
        return self.local.conn

    def store(
        self,
        name: str,
        df: pd.DataFrame,
        column_types: Dict[str, str],
        index_cols: Optional[List[str]] = None,
    ) -> None:
        self.dataframes[name] = df.copy()
        self.indexes[name] = column_types
        
        if index_cols:
            self.dataframes[name] = df.set_index(index_cols).sort_index()

        # Get thread-specific connection
        conn = self._get_sqlite_conn()
        
        df_sql = df.copy()
        for col in df_sql.columns:
            if df_sql[col].dtype == 'object' and any(isinstance(x, Decimal) for x in df_sql[col].dropna()):
                df_sql[col] = df_sql[col].apply(
                    lambda x: float(x) if isinstance(x, Decimal) else x
                )
            elif pd.api.types.is_datetime64_any_dtype(df_sql[col]):
                df_sql[col] = df_sql[col].dt.strftime('%Y-%m-%d')
            elif pd.api.types.is_numeric_dtype(df_sql[col]):
                df_sql[col] = pd.to_numeric(df_sql[col], errors="coerce")

        df_sql.to_sql(name, conn, if_exists="replace", index=False)  # Use thread-specific connection

    def query(
        self,
        dataset: str,
        filters: Dict[str, Any] = None,
        date_range: Optional[Tuple[str, str]] = None,
        amount_range: Optional[Tuple[Decimal, Decimal]] = None,
    ) -> pd.DataFrame:
        filters = filters or {}
        df = self.dataframes[dataset].copy()
        
        # Apply exact filters
        for col, val in filters.items():
            if col in df.columns:
                df = df[df[col] == val]

        # Date range filtering
        if date_range:
            start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
            found = False
            
            # Check index levels
            if df.index.nlevels >= 1:
                for level in df.index.names:
                    if level and ('date' in level.lower() or 'time' in level.lower()):
                        mask = (df.index.get_level_values(level) >= start) & (
                            df.index.get_level_values(level) <= end
                        )
                        df = df[mask]
                        found = True
                        break
            
            # Check columns if not found in index
            if not found:
                for col in df.columns:
                    if col and ('date' in col.lower() or 'time' in col.lower()):
                        mask = (df[col] >= start) & (df[col] <= end)
                        df = df[mask]
                        break

        # Amount range filtering
        if amount_range:
            low, high = float(amount_range[0]), float(amount_range[1])
            found = False
            
            # Check index levels
            if df.index.nlevels >= 1:
                for level in df.index.names:
                    if level and ('amount' in level.lower() or 'value' in level.lower()):
                        mask = (df.index.get_level_values(level) >= low) & (
                            df.index.get_level_values(level) <= high
                        )
                        df = df[mask]
                        found = True
                        break
            
            # Check columns if not found in index
            if not found:
                for col in df.columns:
                    if col and ('amount' in col.lower() or 'value' in col.lower()):
                        mask = (df[col] >= low) & (df[col] <= high)
                        df = df[mask]
                        break

        return df

    def aggregate(
        self,
        dataset: str,
        group_by: List[str],
        measures: Dict[str, str],
    ) -> pd.DataFrame:
        df = self.dataframes[dataset]
        
        # Reset index to access all columns
        df = df.reset_index()
        
        return df.groupby(group_by)[list(measures.keys())].agg(measures)

    def sql(self, sql: str) -> pd.DataFrame:
        conn = self._get_sqlite_conn()  # Use thread-specific connection
        return pd.read_sql(sql, conn)