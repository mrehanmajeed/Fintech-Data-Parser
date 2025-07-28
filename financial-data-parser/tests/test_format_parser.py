from decimal import Decimal
from src.core.format_parser import FormatParser

def test_parse_us_currency():
    assert FormatParser.parse_amount("$1,234.56") == Decimal("1234.56")

def test_parse_euro_comma():
    assert FormatParser.parse_amount("€1.234,56") == Decimal("1234.56")

def test_parse_excel_date():
    assert FormatParser.parse_date("44927") == FormatParser.parse_date("2023-01-01")