import argparse
import sys
from unittest import mock

from utils.cli import parse_args, resolve_params


def test_parse_args_interval_mode():
    # 模擬 CLI 輸入，驗證 interval 模式與參數解析
    argv = [
        "run_etl_pipeline",
        "--stock-mode",
        "interval",
        "--market-value-date",
        "2024-01-15",
    ]
    with mock.patch.object(sys, "argv", argv):
        args = parse_args()

    assert args.stock_mode == "interval"
    assert args.market_value_date == "2024-01-15"


def test_resolve_params_replaces_dataset_template():
    # 驗證 dataset 中的 {top_n} 會被 top_n 取代
    config = {
        "top_stocks": {"top_n": 50},
        "yfinance": {"start": "2020-01-01", "end": "2020-12-31"},
        "bigquery": {"dataset": "tw_top_{_top_n}_stock_data"},
    }
    args = argparse.Namespace(
        stock_mode="latest",
        market_value_date=None,
        start=None,
        end=None,
        top_n=30,
        excluded_industry=None,
        pre_list_date=None,
        dataset=None,
        skip_gcs=False,
    )

    params = resolve_params(config, args)

    assert params["dataset_id"] == "tw_top_30_stock_data"
