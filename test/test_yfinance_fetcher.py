"""
ingestion/yfinance_fetcher 的單元測試：YFinanceFetcher.fetch_daily_ohlcv_data、fetch_benchmark_daily。

驗證 yfinance 抓取：欄位正規化（MultiIndex 處理、小寫命名）、台股 .TW 後綴、基準指數 index_id 處理。
"""
from unittest import mock

import pandas as pd

from conftest import require_module


def _get_fetcher():
    """
    取得 YFinanceFetcher class，確保 yfinance 套件已安裝。

    Returns:
        YFinanceFetcher class。
    """
    require_module("yfinance", "pip install -r requirements.txt")
    from ingestion.yfinance_fetcher import YFinanceFetcher

    return YFinanceFetcher


def test_fetch_daily_ohlcv_data_long_format():
    """
    驗證 fetch_daily_ohlcv_data：MultiIndex 欄位處理、小寫命名、long format、台股 .TW 後綴。

    實務：yfinance 單一 ticker 回傳 MultiIndex，需 droplevel；欄位統一為小寫以利 BigQuery；
    long format 符合後續 Transformer.process_ohlcv_data 需求。
    """
    YFinanceFetcher = _get_fetcher()
    with mock.patch("ingestion.yfinance_fetcher.yf.download") as mock_download:
        index = pd.date_range("2024-01-01", periods=2, freq="D", name="Date")
        cols = pd.MultiIndex.from_product(
            [["Open", "High", "Low", "Close", "Volume"], ["2330.TW"]],
            names=[None, "Ticker"],
        )
        data = [
            [100, 101, 99, 100, 1000],
            [101, 102, 100, 101, 1100],
        ]
        mock_download.return_value = pd.DataFrame(data, index=index, columns=cols)

        result = YFinanceFetcher.fetch_daily_ohlcv_data(
            stock_symbols=["2330"],
            start_date="2024-01-01",
            end_date="2024-01-03",
            is_tw_stock=True,
        )

        assert set(result.columns) == {
            "open",
            "high",
            "low",
            "close",
            "volume",
            "datetime",
            "asset",
        }
        assert set(result["asset"]) == {"2330"}


def test_fetch_benchmark_daily():
    """
    驗證 fetch_benchmark_daily：欄位為 date, index_id, close, daily_return；index_id 去掉前綴 ^。

    實務：基準指數用於回測績效比較；index_id 去掉 ^ 以利 BigQuery 查詢；daily_return 供回測計算使用。
    """
    YFinanceFetcher = _get_fetcher()
    index = pd.date_range("2024-01-01", periods=2, freq="D", name="Date")
    raw = pd.DataFrame(
        {"Close": [100.0, 101.0]},
        index=index,
    )
    with mock.patch("ingestion.yfinance_fetcher.yf.download", return_value=raw):
        result = YFinanceFetcher.fetch_benchmark_daily(
            index_ids=["^TWII"],
            start_date="2024-01-01",
            end_date="2024-01-03",
        )
    assert set(result.columns) == {"date", "index_id", "close", "daily_return"}
    assert result["index_id"].iloc[0] == "TWII"
    assert len(result) == 2
