import pandas as pd
from decimal import Decimal
from src.core.data_storage import DataStorage

def test_store_and_query():
    df = pd.DataFrame({
        "customer": ["A", "B", "C"],
        "amount_num": [100, 200, 300],
        "posting_date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"])
    })
    store = DataStorage()
    
    store.store(
        "test", 
        df, 
        column_types={
            "amount_num": "number",
            "posting_date": "datetime"
        }
    )
    
    # Test 1: Range 200-250 should return only 200
    result1 = store.query("test", amount_range=(Decimal("200"), Decimal("250")))
    assert len(result1) == 1
    assert result1.iloc[0]["amount_num"] == 200
    
    # Test 1.5: Exact frame comparison for first query
    expected1 = df[df["amount_num"].between(200, 250)]
    pd.testing.assert_frame_equal(
        result1.reset_index(drop=True), 
        expected1.reset_index(drop=True),
        check_dtype=False
    )
    
    # Test 2: Range 150-300 should return 200 and 300
    result2 = store.query("test", amount_range=(Decimal("150"), Decimal("300")))
    assert len(result2) == 2
    assert set(result2["amount_num"]) == {200, 300}
    
    # Test 2.5: Exact frame comparison for second query
    expected2 = df[df["amount_num"].between(150, 300)]
    pd.testing.assert_frame_equal(
        result2.reset_index(drop=True), 
        expected2.reset_index(drop=True),
        check_dtype=False
    )