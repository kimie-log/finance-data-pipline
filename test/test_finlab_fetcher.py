from unittest import mock

import pandas as pd

from conftest import require_module


def _get_fetcher():
    # 確保 finlab 已安裝，避免測試在缺依賴時直接壞掉
    require_module("finlab", "pip install -r requirements.txt")
    from ingestion.finlab_fetcher import FinLabFetcher

    return FinLabFetcher


def test_finlab_login_uses_token():
    # 準備：mock 環境變數與 login
    FinLabFetcher = _get_fetcher()
    with mock.patch("ingestion.finlab_fetcher.os.getenv", return_value="token") as mock_getenv:
        with mock.patch("ingestion.finlab_fetcher.finlab.login") as mock_login:
            # 執行：登入流程
            FinLabFetcher.finlab_login()
            # 驗證：有讀取 token 並呼叫 login
            mock_getenv.assert_called_once()
            mock_login.assert_called_once_with("token")


def test_fetch_top_stocks_by_market_value_filters_and_sorts():
    # 準備：公司資訊與市值資料
    FinLabFetcher = _get_fetcher()
    company_info = pd.DataFrame(
        {
            "stock_id": ["2330", "2317", "3008", "2603"],
            "公司名稱": ["A", "B", "C", "D"],
            "上市日期": ["2000-01-01", "2010-01-01", "2019-01-01", "2015-01-01"],
            "產業類別": ["半導體", "電子", "半導體", "航運"],
            "市場別": ["sii", "sii", "otc", "sii"],
        }
    )
    market_value = pd.DataFrame(
        {
            "2330": [10, 20],
            "2317": [30, 5],
        },
        index=["2024-01-01", "2024-01-02"],
    )

    def get_side_effect(key):
        # 根據 key 回傳對應資料集
        if key == "company_basic_info":
            return company_info
        if key == "etl:market_value":
            return market_value
        raise KeyError(key)

    with mock.patch("ingestion.finlab_fetcher.data.get") as mock_get:
        mock_get.side_effect = get_side_effect

        # 執行：套用產業與上市日期過濾，並選取 top 1
        result = FinLabFetcher.fetch_top_stocks_by_market_value(
            excluded_industry=["航運"],
            pre_list_date="2018-01-01",
            top_n=1,
        )

        # 驗證：回傳最高市值且符合條件的股票
        assert result == ["2330"]


def test_fetch_top_stocks_without_top_n():
    # 準備：公司資訊
    FinLabFetcher = _get_fetcher()
    company_info = pd.DataFrame(
        {
            "stock_id": ["2330", "2317", "2603"],
            "公司名稱": ["A", "B", "C"],
            "上市日期": ["2000-01-01", "2010-01-01", "2015-01-01"],
            "產業類別": ["半導體", "電子", "航運"],
            "市場別": ["sii", "sii", "sii"],
        }
    )

    with mock.patch("ingestion.finlab_fetcher.data.get") as mock_get:
        mock_get.return_value = company_info

        # 執行：不指定 top_n 時回傳全部符合條件的股票
        result = FinLabFetcher.fetch_top_stocks_by_market_value(
            excluded_industry=["航運"],
            pre_list_date="2018-01-01",
            top_n=None,
        )

        # 驗證：排除航運後保留上市股票
        assert result == ["2330", "2317"]
