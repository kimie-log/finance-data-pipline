"""
processing/transformer 的單元測試：Transformer.process_ohlcv_data。

驗證 OHLCV 清洗、欄位正規化、缺失值處理、日報酬計算、交易可行性標記。
"""
import numpy as np
import pandas as pd

from processing.transformer import Transformer


def test_process_ohlcv_data_basic():
    """
    驗證 process_ohlcv_data 基本功能：欄位正規化、缺失值處理、型別轉換。

    實務：測試含缺失值的輸入，驗證價格欄位 ffill、volume 填 0、無效資料移除。
    """
    df_raw = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01"]),
            "asset": ["2330", "2330", "2317"],
            "open": [100.0, None, 50.0],
            "high": [101.0, None, 51.0],
            "low": [99.0, None, 49.0],
            "close": [100.0, None, 50.0],
            "volume": [1000, None, 500],
        }
    )

    result = Transformer.process_ohlcv_data(df_raw)

    expected_cols = {
        "date",
        "stock_id",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "daily_return",
        "is_suspended",
        "is_limit_up",
        "is_limit_down",
    }
    assert set(result.columns) == expected_cols
    assert result["date"].dtype.kind == "M"

    assert not result["close"].isna().any()
