import pandas as pd
from src.core.type_detector import TypeDetector

def test_detect_string():
    series = pd.Series(["abc", "def", None])
    dtype, conf = TypeDetector.detect(series)
    assert dtype == "str"

def test_detect_number():
    series = pd.Series(["1.2", "3.4", None])
    dtype, conf = TypeDetector.detect(series)
    assert dtype == "number"

def test_detect_date():
    series = pd.Series(["2023-12-31", "2024-01-01", None])
    dtype, conf = TypeDetector.detect(series)
    assert dtype == "datetime"