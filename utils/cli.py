"""
CLI 參數解析與設定檔合併工具。

統一處理 ETL pipeline 的命令列參數解析、設定檔讀取（config/settings.yaml）、
參數合併（CLI 優先於設定檔）、dataset 模板代換（{top_n} / {_top_n}）。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Annotated

import yaml


def parse_args() -> argparse.Namespace:
    """
    解析 ETL pipeline 的命令列參數。

    Returns:
        argparse.Namespace 包含所有 CLI 參數；必填：--start、--end；--market-value-date 與
        --market-value-dates 二擇一。

    Note:
        - --excluded-industry 可重複指定（action="append"）。
        - --skip-gcs、--with-factors、--skip-benchmark、--skip-calendar 為 flag（action="store_true"）。
    """
    parser = argparse.ArgumentParser(
        description="ETL pipeline for Taiwan stock data (FinLab + yfinance)."
    )
    parser.add_argument(
        "--market-value-date",
        help="單一市值基準日期 (YYYY-MM-DD)，與 --market-value-dates 二擇一",
    )
    parser.add_argument(
        "--market-value-dates",
        help="多個市值基準日期，逗號分隔 (例: 2024-01-15,2024-02-15)，供滾動回測一次跑多期 ETL",
    )
    parser.add_argument("--start", required=True, help="價量與因子區間起始日 (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="價量與因子區間結束日 (YYYY-MM-DD)")
    parser.add_argument("--top-n", type=int, help="市值前 N 大")
    parser.add_argument(
        "--excluded-industry",
        action="append",
        dest="excluded_industry",
        help="排除產業（可重複指定）",
    )
    parser.add_argument("--pre-list-date", help="上市日期須早於此日期 (YYYY-MM-DD)")
    parser.add_argument("--dataset", help="BigQuery dataset ID")
    parser.add_argument(
        "--skip-gcs",
        action="store_true",
        help="略過上傳 GCS（僅保留本地輸出）",
    )
    parser.add_argument(
        "--with-factors",
        action="store_true",
        help="一併抓取財報因子並寫入 BigQuery fact_factor",
    )
    parser.add_argument(
        "--skip-benchmark",
        action="store_true",
        help="略過基準指數抓取與寫入",
    )
    parser.add_argument(
        "--skip-calendar",
        action="store_true",
        help="略過交易日曆 dim_calendar 寫入",
    )
    parser.add_argument(
        "--factor-table-suffix",
        help="因子表名後綴，同一組 (mv, start, end, top_n) 可並存多組因子 (例: value, momentum)",
    )
    return parser.parse_args()


def load_config(root_dir: Path) -> dict:
    """
    從 config/settings.yaml 讀取設定檔並回傳 dict。

    Args:
        root_dir: 專案根目錄，用於定位 config/settings.yaml。

    Returns:
        設定檔內容的 dict；檔案不存在或格式錯誤時可能拋出 yaml 相關例外。

    Note:
        使用 yaml.safe_load 避免執行任意程式碼；建議設定檔結構對齊 README 說明。
    """
    config_path = root_dir / "config/settings.yaml"
    return yaml.safe_load(open(config_path))


def resolve_params(config: dict, args: argparse.Namespace) -> dict:
    """
    合併設定檔與 CLI 參數，CLI 優先；處理 dataset 模板代換與多個市值日解析。

    Args:
        config: 設定檔 dict（來自 load_config）。
        args: CLI 參數（來自 parse_args）。

    Returns:
        合併後的參數 dict，包含 market_value_dates、market_value_date、start_date、end_date、
        top_n、excluded_industry、pre_list_date、dataset_id、skip_gcs、with_factors、
        factor_names、benchmark_index_ids、backtest_config、skip_benchmark、skip_calendar、
        factor_table_suffix。

    Note:
        - market_value_dates 為列表（單一日期時為 [date]），market_value_date 為第一個元素。
        - dataset_id 支援 {top_n} / {_top_n} 模板代換，用於動態命名。
        - factor_table_suffix：CLI 優先於設定檔。
    """
    top_stocks_cfg = config.get("top_stocks", {})
    yfinance_cfg = config.get("yfinance", {})
    bigquery_cfg = config.get("bigquery", {})

    if getattr(args, "market_value_dates", None) and args.market_value_dates.strip():
        market_value_dates = [d.strip() for d in args.market_value_dates.split(",") if d.strip()]
    elif getattr(args, "market_value_date", None) and args.market_value_date:
        market_value_dates = [args.market_value_date]
    else:
        market_value_dates = []

    excluded_industry = (
        args.excluded_industry
        if args.excluded_industry is not None
        else top_stocks_cfg.get("excluded_industry", [])
    )
    pre_list_date = args.pre_list_date or top_stocks_cfg.get("pre_list_date")
    top_n = args.top_n if args.top_n is not None else top_stocks_cfg.get("top_n", 50)

    start_date = args.start or yfinance_cfg.get("start")
    end_date = args.end or yfinance_cfg.get("end")

    dataset_id = args.dataset or bigquery_cfg.get("dataset")
    if isinstance(dataset_id, str):
        dataset_id = dataset_id.replace("{top_n}", str(top_n)).replace("{_top_n}", str(top_n))

    factors_cfg = config.get("factors", {})
    benchmark_cfg = config.get("benchmark", {})
    backtest_cfg = config.get("backtest_config", {})

    return {
        "market_value_dates": market_value_dates,
        "market_value_date": market_value_dates[0] if market_value_dates else None,
        "excluded_industry": excluded_industry,
        "pre_list_date": pre_list_date,
        "top_n": top_n,
        "start_date": start_date,
        "end_date": end_date,
        "dataset_id": dataset_id,
        "skip_gcs": args.skip_gcs,
        "with_factors": args.with_factors,
        "factor_names": factors_cfg.get("factor_names", []),
        "benchmark_index_ids": benchmark_cfg.get("index_ids", ["^TWII"]),
        "backtest_config": backtest_cfg,
        "skip_benchmark": args.skip_benchmark,
        "skip_calendar": args.skip_calendar,
        "factor_table_suffix": getattr(args, "factor_table_suffix", None) or factors_cfg.get("factor_table_suffix"),
    }
