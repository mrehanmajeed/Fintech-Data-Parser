 """Generic helper functions"""

def format_currency(value: float) -> str:
    """Format number as currency string"""
    return f"${value:,.2f}"

def clean_column_name(name: str) -> str:
    """Standardize column names for processing"""
    return name.lower().replace(" ", "_").replace("-", "_")