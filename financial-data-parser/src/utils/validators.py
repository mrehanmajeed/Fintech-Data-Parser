"""Data validation functions"""

def validate_date(date_str: str) -> bool:
    """Check if string is valid date"""
    try:
        pd.to_datetime(date_str)
        return True
    except ValueError:
        return False

def validate_amount(value) -> bool:
    """Check if value is numeric financial amount"""
    return isinstance(value, (int, float, Decimal)) 