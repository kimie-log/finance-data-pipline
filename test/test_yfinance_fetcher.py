from unittest import mock

import pandas as pd

from conftest import require_module


def _get_fetcher():
    # 確保 yfinance 已安裝，避免測試在缺依賴時直接壞掉
    require_module("yfinance", "pip install -r requirements.txt")
    from ingestion.yfinance_fetcher import YFinanceFetcher

    return YFinanceFetcher


def test_fetch_daily_close_prices_multiindex():
    # 準備：建立 MultiIndex 欄位資料，模擬多檔下載
    YFinanceFetcher = _get_fetcher()
    with mock.patch("ingestion.yfinance_fetcher.yf.download") as mock_download:
        index = pd.date_range("2024-01-01", periods=2, freq="D")
        columns = pd.MultiIndex.from_product(
            [["Close", "Open"], ["2330.TW", "2317.TW"]]
        )
        data = [
            [100, 200, 99, 199],
            [101, 201, 98, 198],
        ]
        mock_download.return_value = pd.DataFrame(data, index=index, columns=columns)

        # 執行：抓取並轉換
        result = YFinanceFetcher.fetch_daily_close_prices(
            stock_symbols=["2330", "2317"],
            start_date="2024-01-01",
            end_date="2024-01-03",
            is_tw_stock=True,
        )

        # 驗證：欄位去除 .TW 且資料維度正確
        assert list(result.columns) == ["2330", "2317"]
        assert result.shape == (2, 2)


def test_fetch_daily_close_prices_series():
    # 準備：模擬單檔下載回傳 Series
    YFinanceFetcher = _get_fetcher()
    with mock.patch("ingestion.yfinance_fetcher.yf.download") as mock_download:
        index = pd.date_range("2024-01-01", periods=2, freq="D")
        mock_download.return_value = pd.Series([10, 11], index=index, name="Close")

        # 執行：抓取並轉換
        result = YFinanceFetcher.fetch_daily_close_prices(
            stock_symbols=["2330"],
            start_date="2024-01-01",
            end_date="2024-01-03",
            is_tw_stock=False,
        )

        # 驗證：轉為單欄 DataFrame
        assert list(result.columns) == ["2330"]
        assert result.shape == (2, 1)
