"""
ingestion/finlab_factor_fetcher 的單元測試：季頻展開、季度對應、因子取得與查詢。

驗證 FinLabFactorFetcher 各 staticmethod：extend_factor_data（季頻→日頻 ffill）、
get_factor_data（原始季頻 vs long format）、convert_quarter_to_dates / convert_date_to_quarter、
list_factors_by_type、fetch_factors_daily（多因子日頻，long format）。
"""
import sys
from unittest import mock

import pandas as pd
import pytest

from conftest import require_module

if "finlab" not in sys.modules:
    _finlab_mock = mock.MagicMock()
    _finlab_mock.data = mock.MagicMock()
    sys.modules["finlab"] = _finlab_mock

from ingestion.finlab_factor_fetcher import FinLabFactorFetcher


def test_extend_factor_data_ffill():
    """
    驗證季頻展開至交易日：向前填補（ffill），僅保留交易日區間內列。

    實務：財報截止日點位對齊到每個交易日，缺日以前值補齊；區間外列移除以符合交易日範圍。
    """
    factor_data = pd.DataFrame({
        "index": pd.to_datetime(["2024-01-01", "2024-01-10"]),
        "2330": [100.0, 110.0],
        "2317": [50.0, 55.0],
    })
    trading_days = pd.DatetimeIndex(pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-10"]))
    result = FinLabFactorFetcher.extend_factor_data(factor_data, trading_days)
    assert len(result) == 4
    assert result["index"].min() == pd.Timestamp("2024-01-01")
    assert result["index"].max() == pd.Timestamp("2024-01-10")
    assert (result[result["index"] == pd.Timestamp("2024-01-02")]["2330"].values == 100.0).all()
    assert (result[result["index"] == pd.Timestamp("2024-01-10")]["2330"].values == 110.0).all()


def test_convert_quarter_to_dates():
    """
    驗證季度字串轉財報揭露區間日期，對齊台灣財報揭露時程。

    實務：Q1 揭露期 5/16～8/14、Q2 8/15～11/14、Q3 11/15～隔年 3/31、Q4 隔年 4/1～5/15。
    """
    assert FinLabFactorFetcher.convert_quarter_to_dates("2013-Q1") == ("2013-05-16", "2013-08-14")
    assert FinLabFactorFetcher.convert_quarter_to_dates("2013-Q2") == ("2013-08-15", "2013-11-14")
    assert FinLabFactorFetcher.convert_quarter_to_dates("2013-Q3") == ("2013-11-15", "2014-03-31")
    assert FinLabFactorFetcher.convert_quarter_to_dates("2013-Q4") == ("2014-04-01", "2014-05-15")


def test_convert_date_to_quarter():
    """
    驗證日期轉台灣財報季度字串，依揭露區間邊界判斷。

    實務：用於查詢某日屬於哪一季財報區間；邊界日期（如 5/16、8/15）需正確歸類。
    """
    assert FinLabFactorFetcher.convert_date_to_quarter("2013-05-16") == "2013-Q1"
    assert FinLabFactorFetcher.convert_date_to_quarter("2013-08-14") == "2013-Q1"
    assert FinLabFactorFetcher.convert_date_to_quarter("2013-08-15") == "2013-Q2"
    assert FinLabFactorFetcher.convert_date_to_quarter("2013-11-15") == "2013-Q3"
    assert FinLabFactorFetcher.convert_date_to_quarter("2014-03-31") == "2013-Q3"
    assert FinLabFactorFetcher.convert_date_to_quarter("2014-04-01") == "2013-Q4"
    assert FinLabFactorFetcher.convert_date_to_quarter("2014-05-15") == "2013-Q4"


def test_get_factor_data_without_trading_days_returns_raw():
    """
    驗證未給 trading_days 時回傳原始季頻表（index 為日期、欄位為股票代碼）。

    實務：用於查詢原始財報資料，不需展開為日頻時使用。
    """
    raw = pd.DataFrame(
        {"2330": [100.0], "2317": [50.0]},
        index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]),
    )
    raw.index.name = "index"

    with mock.patch("ingestion.finlab_factor_fetcher.data") as mock_data:
        mock_data.get.return_value.deadline.return_value = raw
        result = FinLabFactorFetcher.get_factor_data(
            stock_symbols=["2330", "2317"],
            factor_name="營業利益",
            trading_days=None,
        )
        mock_data.get.assert_called_once()
        assert "2330" in result.columns and "2317" in result.columns
        assert len(result) == 1


def test_get_factor_data_with_trading_days_returns_long_format():
    """
    驗證給 trading_days 時回傳 long format (datetime, asset, value)，供 BigQuery fact_factor 寫入。

    實務：日頻資料需 long format 以利寫入倉儲；展開後每日期 × 股票一筆，欄位 datetime, asset, value。
    """
    raw = pd.DataFrame(
        {"2330": [100.0], "2317": [50.0]},
        index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]),
    )
    raw.index.name = "index"
    trading_days = pd.DatetimeIndex([pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])

    with mock.patch("ingestion.finlab_factor_fetcher.data") as mock_data:
        mock_data.get.return_value.deadline.return_value = raw
        result = FinLabFactorFetcher.get_factor_data(
            stock_symbols=["2330", "2317"],
            factor_name="營業利益",
            trading_days=trading_days,
        )
        assert set(result.columns) == {"datetime", "asset", "value"}
        assert len(result) == 4
        assert set(result["asset"]) == {"2330", "2317"}


def test_list_factors_by_type():
    """
    驗證依資料型態列出 FinLab 因子名稱，供設定 factor_names 或除錯用。

    實務：用於查詢可用因子清單，避免輸入錯誤的因子名稱。
    """
    with mock.patch("ingestion.finlab_factor_fetcher.data") as mock_data:
        mock_data.search.return_value = [{"items": ["營業利益", "營業收入", "淨利"]}]
        result = FinLabFactorFetcher.list_factors_by_type("fundamental_features")
        mock_data.search.assert_called_once()
        assert result == ["營業利益", "營業收入", "淨利"]


def test_fetch_factors_daily_empty_input():
    """
    驗證 factor_names 或 stock_ids 為空時回傳空 DataFrame（含正確欄位）。

    實務：邊界情況處理，避免後續步驟因空輸入而失敗；回傳欄位格式一致以利下游處理。
    """
    trading_days = pd.DatetimeIndex([pd.Timestamp("2024-01-01")])
    empty = FinLabFactorFetcher.fetch_factors_daily(
        stock_ids=[],
        factor_names=["營業利益"],
        start_date="2024-01-01",
        end_date="2024-01-02",
        trading_days=trading_days,
    )
    assert empty.empty
    assert list(empty.columns) == ["date", "stock_id", "factor_name", "value"]

    empty2 = FinLabFactorFetcher.fetch_factors_daily(
        stock_ids=["2330"],
        factor_names=[],
        start_date="2024-01-01",
        end_date="2024-01-02",
        trading_days=trading_days,
    )
    assert empty2.empty


def test_fetch_factors_daily_columns():
    """
    驗證 fetch_factors_daily 回傳欄位為 date, stock_id, factor_name, value（long format）。

    實務：產出格式對齊 BigQuery fact_factor 表；factor_name 欄位用於區分不同因子。
    """
    trading_days = pd.DatetimeIndex([pd.Timestamp("2024-01-01")])
    long_df = pd.DataFrame({
        "datetime": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
        "asset": ["2330", "2317"],
        "value": [100.0, 50.0],
    })

    with mock.patch("ingestion.finlab_factor_fetcher.FinLabFactorFetcher.get_factor_data", return_value=long_df):
        result = FinLabFactorFetcher.fetch_factors_daily(
            stock_ids=["2330", "2317"],
            factor_names=["營業利益"],
            start_date="2024-01-01",
            end_date="2024-01-02",
            trading_days=trading_days,
        )
        assert set(result.columns) == {"date", "stock_id", "factor_name", "value"}
        assert (result["factor_name"] == "營業利益").all()
