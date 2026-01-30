"""
utils/cli 的單元測試：parse_args、resolve_params、load_config。

驗證 CLI 參數解析、設定檔與 CLI 參數合併（CLI 優先）、dataset 模板代換、
多個市值日解析、因子表 suffix 處理。
"""
import argparse
import sys
from unittest import mock

from utils.cli import load_config, parse_args, resolve_params


def test_parse_args_required_dates():
    """
    驗證必填參數（market-value-date、start、end）可正確解析。

    實務：mock sys.argv 模擬 CLI 輸入，避免實際執行 argparse。
    """
    argv = [
        "run_etl_pipeline",
        "--market-value-date",
        "2024-01-15",
        "--start",
        "2020-01-01",
        "--end",
        "2024-01-01",
    ]
    with mock.patch.object(sys, "argv", argv):
        args = parse_args()

    assert args.market_value_date == "2024-01-15"
    assert args.start == "2020-01-01"
    assert args.end == "2024-01-01"


def test_resolve_params_replaces_dataset_template():
    """
    驗證 dataset 中的 {top_n} / {_top_n} 模板會被實際 top_n 值取代。

    實務：CLI top_n 優先於設定檔；dataset 模板代換用於動態命名。
    """
    config = {
        "top_stocks": {"top_n": 50},
        "yfinance": {"start": "2020-01-01", "end": "2020-12-31"},
        "bigquery": {"dataset": "tw_top_{_top_n}_stock_data"},
    }
    args = argparse.Namespace(
        market_value_date="2024-01-15",
        start="2020-01-01",
        end="2024-01-01",
        top_n=30,
        excluded_industry=None,
        pre_list_date=None,
        dataset=None,
        skip_gcs=False,
        with_factors=False,
        skip_benchmark=False,
        skip_calendar=False,
    )

    params = resolve_params(config, args)

    assert params["dataset_id"] == "tw_top_30_stock_data"
    assert params["market_value_dates"] == ["2024-01-15"]
    assert params["market_value_date"] == "2024-01-15"


def test_resolve_params_market_value_dates_and_factor_suffix():
    """
    驗證多個市值日（逗號分隔）與因子表 suffix 解析，CLI 優先於設定檔。

    實務：--market-value-dates 用於滾動回測；--factor-table-suffix 用於並存多組因子表。
    """
    config = {
        "top_stocks": {"top_n": 50},
        "yfinance": {"start": "2020-01-01", "end": "2024-01-01"},
        "bigquery": {"dataset": "dataset"},
        "factors": {"factor_table_suffix": "value"},
    }
    args = argparse.Namespace(
        market_value_date=None,
        market_value_dates="2024-01-15,2024-02-15,2024-03-15",
        start="2020-01-01",
        end="2024-01-01",
        top_n=50,
        excluded_industry=None,
        pre_list_date=None,
        dataset=None,
        skip_gcs=False,
        with_factors=True,
        skip_benchmark=False,
        skip_calendar=False,
        factor_table_suffix="momentum",
    )

    params = resolve_params(config, args)

    assert params["market_value_dates"] == ["2024-01-15", "2024-02-15", "2024-03-15"]
    assert params["market_value_date"] == "2024-01-15"
    assert params["factor_table_suffix"] == "momentum"


def test_load_config_returns_dict(tmp_path):
    """
    驗證 load_config 可從 config/settings.yaml 讀取並回傳 dict。

    實務：使用 tmp_path 建立測試用設定檔，避免依賴專案根目錄的實際設定檔。
    """
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    settings = config_dir / "settings.yaml"
    settings.write_text(
        "top_stocks:\n  top_n: 50\nbigquery:\n  dataset: my_dataset\n"
    )
    root_dir = tmp_path
    result = load_config(root_dir)
    assert isinstance(result, dict)
    assert result.get("top_stocks", {}).get("top_n") == 50
    assert result.get("bigquery", {}).get("dataset") == "my_dataset"
