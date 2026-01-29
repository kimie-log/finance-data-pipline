import argparse
from datetime import date
from pathlib import Path
import yaml


def parse_args() -> argparse.Namespace:
    # CLI 入口：統一解析執行參數，避免散落在多個檔案
    parser = argparse.ArgumentParser(
        description="ETL pipeline for Taiwan stock data (FinLab + yfinance)."
    )
    parser.add_argument(
        "--stock-mode",
        choices=["latest", "interval"],
        default="latest",
        # latest 用最新市值；interval 用指定市值日期（可重現）
        help="latest 使用最新市值 Top N；interval 使用固定市值日期 Top N",
    )
    parser.add_argument(
        "--market-value-date",
        # interval 模式的基準日期，避免「最新」造成回測漂移
        help="區間模式市值基準日期 (YYYY-MM-DD)",
    )
    parser.add_argument("--start", help="yfinance 起始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", help="yfinance 結束日期 (YYYY-MM-DD)")
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
    return parser.parse_args()


def load_config(root_dir: Path) -> dict:
    # 統一讀取設定檔，避免將參數硬寫在程式碼中
    config_path = root_dir / "config/settings.yaml"
    return yaml.safe_load(open(config_path))


def resolve_params(config: dict, args: argparse.Namespace) -> dict:
    # CLI 參數優先，其次才使用設定檔預設
    top_stocks_cfg = config.get("top_stocks", {})
    yfinance_cfg = config.get("yfinance", {})
    bigquery_cfg = config.get("bigquery", {})

    # interval 模式下的市值基準日期
    market_value_date = args.market_value_date

    # 排除產業可用多次參數累加，未提供則使用設定檔
    excluded_industry = (
        args.excluded_industry
        if args.excluded_industry is not None
        else top_stocks_cfg.get("excluded_industry", [])
    )
    pre_list_date = args.pre_list_date or top_stocks_cfg.get("pre_list_date")
    top_n = args.top_n if args.top_n is not None else top_stocks_cfg.get("top_n", 50)

    # start/end 若未指定，fallback 至設定檔或今天
    start_date = args.start or yfinance_cfg.get("start")
    end_date = args.end or yfinance_cfg.get("end") or date.today().strftime("%Y-%m-%d")

    # dataset 支援 {top_n} / {_top_n} 動態代換
    dataset_id = args.dataset or bigquery_cfg.get("dataset")
    if isinstance(dataset_id, str):
        dataset_id = dataset_id.replace("{top_n}", str(top_n)).replace("{_top_n}", str(top_n))

    return {
        "stock_mode": args.stock_mode,
        "market_value_date": market_value_date,
        "excluded_industry": excluded_industry,
        "pre_list_date": pre_list_date,
        "top_n": top_n,
        "start_date": start_date,
        "end_date": end_date,
        "dataset_id": dataset_id,
        "skip_gcs": args.skip_gcs,
    }
