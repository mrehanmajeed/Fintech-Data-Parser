from __future__ import annotations

import re
import warnings
from decimal import Decimal, InvalidOperation
from typing import Dict, Tuple

import pandas as pd
import numpy as np


class TypeDetector:
    """
    Public API:
        detect(df, column) -> ("str"|"number"|"datetime", confidence)
        detect_all(df)     -> dict {col: (dtype, confidence)}
    """

    MIN_CONFIDENCE = 0.6  # threshold for acceptance
    
    # Common date formats for financial data
    COMMON_DATE_FORMATS = [
        # ISO formats
        '%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M',
        # European formats
        '%d/%m/%Y', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M',
        '%d.%m.%Y', '%d.%m.%Y %H:%M:%S', '%d %b %Y', '%d %B %Y',
        # US formats
        '%m/%d/%Y', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %H:%M',
        '%m-%d-%Y', '%b %d, %Y', '%B %d, %Y',
        # Compact formats
        '%Y%m%d', '%d%m%Y', '%m%d%Y',
        # Special financial formats
        'Q%q-%y', 'Quarter %q %Y', '%b-%Y', '%B %Y',
        # Time formats
        '%H:%M:%S', '%H:%M'
    ]

    # ---------- public ----------

    @staticmethod
    def detect(series: pd.Series) -> Tuple[str, float]:
        """
        Detect the most probable type of a pandas Series.
        Returns: (dtype, confidence)
        """
        s = series.dropna()

        if s.empty:
            return "str", 1.0

        # 1. Date attempt -----------------------------------------------------
        dt_succ, dt_conf = TypeDetector._try_datetime(s)
        if dt_succ:
            return "datetime", dt_conf

        # 2. Number attempt ---------------------------------------------------
        num_succ, num_conf = TypeDetector._try_number(s)
        if num_succ:
            return "number", num_conf

        # 3. Default ----------------------------------------------------------
        return "str", 1.0

    @staticmethod
    def detect_all(df: pd.DataFrame) -> Dict[str, Tuple[str, float]]:
        """Run detect() on every column."""
        return {col: TypeDetector.detect(df[col]) for col in df.columns}

    # ---------- internal helpers ----------

    @staticmethod
    def _try_datetime(s: pd.Series) -> Tuple[bool, float]:
        """Enhanced datetime detection with format inference and Excel support"""
        # Handle Excel serial dates first (numeric values in date range)
        if pd.api.types.is_numeric_dtype(s):
            if s.min() > 1000 and s.max() < 1000000:
                try:
                    # Convert Excel serial numbers to dates
                    converted = pd.to_datetime(
                        s, 
                        unit='D', 
                        origin='1899-12-30', 
                        errors='coerce'
                    )
                    success_rate = converted.notnull().mean()
                    if success_rate >= 0.95:
                        return True, success_rate
                except Exception:
                    pass

        # Try common explicit formats
        max_success = 0.0
        s_str = s.astype(str)
        
        for fmt in TypeDetector.COMMON_DATE_FORMATS:
            try:
                converted = pd.to_datetime(s_str, format=fmt, errors='coerce')
                success_rate = converted.notnull().mean()
                if success_rate >= 0.95:
                    return True, success_rate
                max_success = max(max_success, success_rate)
            except Exception:
                continue

        # Final fallback with warnings suppressed
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            try:
                converted = pd.to_datetime(s_str, errors='coerce')
                success_rate = converted.notnull().mean()
                if success_rate >= 0.95:
                    return True, success_rate
                max_success = max(max_success, success_rate)
            except Exception:
                pass

        return False, max_success

    @staticmethod
    def _try_number(s: pd.Series) -> Tuple[bool, float]:
        # Remove common currency symbols & thousand separators
        cleaned = (
            s.astype(str)
            .str.replace(r"[€$₹£¥,]", "", regex=True)
            .str.replace(r"\s+", "", regex=True)
        )

        # Allow negative numbers in parentheses  (1 234)  or  1 234-
        cleaned = cleaned.str.replace(r"\(([^)]+)\)", r"-\1", regex=True)
        cleaned = cleaned.str.replace(r"(\d)-$", r"-\1", regex=True)

        # Accept K, M, B suffixes (1.2K → 1200)
        suffix_map = {"K": 1e3, "M": 1e6, "B": 1e9}
        pattern = re.compile(r"^([+-]?\d+\.?\d*)([KkMmBb])$")

        def parse(x: str) -> bool:
            # Abbreviated amounts
            m = pattern.match(x)
            if m:
                return True
            # Exact decimals
            try:
                Decimal(x)
                return True
            except InvalidOperation:
                return False

        success_mask = cleaned.apply(parse)
        conf = success_mask.mean()
        return conf >= TypeDetector.MIN_CONFIDENCE, conf