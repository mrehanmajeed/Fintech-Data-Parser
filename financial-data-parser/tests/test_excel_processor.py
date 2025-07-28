import pytest
from pathlib import Path
from src.core.excel_processor import ExcelProcessor

@pytest.fixture
def ep():
    return ExcelProcessor()

@pytest.fixture
def sample_files():
    return [
        "data/sample/KH_Bank.XLSX",
        "data/sample/Customer_Ledger_Entries_FULL.xlsx"
    ]

def test_load_files(ep, sample_files):
    ep.load_files(sample_files)
    assert len(ep._workbooks) == 2

def test_get_sheet_info(ep, sample_files):
    ep.load_files(sample_files)
    info = ep.get_sheet_info()
    # keys are full paths, so use `in`
    assert any("KH_Bank.XLSX" in k for k in info.keys())