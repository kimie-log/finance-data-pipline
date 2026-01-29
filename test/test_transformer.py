import numpy as np
import pandas as pd

from processing.transformer import Transformer


def test_process_market_data_basic():
    # 準備：建立含缺失值的 wide format 資料
    index = pd.date_range("2024-01-01", periods=2, freq="D", name="Date")
    df_raw = pd.DataFrame(
        {
            "2330": [100.0, np.nan],
            "2317": [np.nan, 50.0],
        },
        index=index,
    )

    # 執行：轉換為 long format 並計算報酬
    result = Transformer.process_market_data(df_raw)

    # 驗證：欄位完整且型別正確
    assert set(result.columns) == {"date", "stock_id", "close", "daily_return"}
    assert result["close"].dtype == np.float32
    assert result["date"].dtype.kind == "M"
    assert len(result) == 3

    # 驗證：ffill 後 2330 的價格被補齊
    close_2330 = result[result["stock_id"] == "2330"].sort_values("date")["close"].tolist()
    assert close_2330 == [100.0, 100.0]
