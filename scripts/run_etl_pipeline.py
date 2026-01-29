import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# 路徑設定：確保可以從任意工作目錄執行
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

# 載入環境變數：集中管理 API Token / GCP 設定
load_dotenv()

# 匯入模組：集中在頂部方便依賴關係一眼看清
from ingestion.yfinance_fetcher import YFinanceFetcher
from ingestion.finlab_fetcher import FinLabFetcher
from processing.transformer import Transformer
from utils.google_cloud_storage import upload_file
from utils.google_cloud_bigquery import load_to_bigquery
from utils.logger import logger
from utils.google_cloud_platform import check_gcp_environment
from utils.cli import parse_args, load_config, resolve_params


def main() -> int:
    # 解析 CLI 與設定檔參數
    args = parse_args()
    config = load_config(ROOT_DIR)
    params = resolve_params(config, args)

    # GCS & BigQuery 設定：從環境與設定檔取得
    bucket_name = os.getenv("GCS_BUCKET")
    bq_dataset = params["dataset_id"]

    # 建立日期資料夾使用：以日期分區方便管理與回溯
    now = datetime.now()
    date_folder = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y%m%d_%H%M")

    # 資料夾設定：本地 raw 與 processed 分開存
    raw_dir = ROOT_DIR / "data/raw" / params["stock_mode"] / date_folder
    processed_dir = ROOT_DIR / "data/processed" / params["stock_mode"] / date_folder

    if not (ROOT_DIR / "data/raw" / params["stock_mode"] / date_folder).exists():
        # 建立 raw 目錄，避免寫檔失敗
        raw_dir.mkdir(parents=True, exist_ok=True)

    if not (ROOT_DIR / "data/processed" / params["stock_mode"] / date_folder).exists():
        # 建立 processed 目錄
        processed_dir.mkdir(parents=True, exist_ok=True)

    # 初始化 GCP 環境：確保金鑰存在且避免被 Git 追蹤
    key_path = check_gcp_environment(ROOT_DIR)
    logger.info(f"=== STEP 0 :GCP 金鑰： {key_path} ===")

    # ----------------------------------------------------------
    # 1. Ingestion: 獲取資料 (Raw Data)
    # ----------------------------------------------------------
    # ----------------------------------------------------------
    # 1. Ingestion: 獲取資料 (Raw Data)
    # ----------------------------------------------------------
    logger.info("=== STEP 1 - A: Ingestion Started 取得股票列表 ===")

    if params["stock_mode"] == "latest":
        # latest：使用最新市值 Top N，適合日常更新
        # FinLab 登入 ( .env 設定 FINLAB_API_TOKEN)
        FinLabFetcher.finlab_login()

        # A. 取得 Top 股票列表 (包含 .TW /.TWO )
        top_tickers = FinLabFetcher.fetch_top_stocks_by_market_value(
            excluded_industry=params["excluded_industry"],
            pre_list_date=params["pre_list_date"],
            top_n=params["top_n"],
        )
        logger.info(f"Target Tickers (latest): {top_tickers}")
    else:
        # interval：使用指定市值日期 Top N，確保可重現
        if not params["market_value_date"]:
            logger.error("Interval mode requires --market-value-date.")
            return 1
        if not params["start_date"] or not params["end_date"]:
            logger.error("Interval mode requires both --start and --end.")
            return 1
        # FinLab 登入 ( .env 設定 FINLAB_API_TOKEN)
        FinLabFetcher.finlab_login()

        top_tickers = FinLabFetcher.fetch_top_stocks_by_market_value(
            excluded_industry=params["excluded_industry"],
            pre_list_date=params["pre_list_date"],
            top_n=params["top_n"],
            market_value_date=params["market_value_date"],
        )
        logger.info(
            "Target Tickers (interval, market_value_date=%s): %s",
            params["market_value_date"],
            top_tickers,
        )

    logger.info("=== STEP 1 - B: Ingestion Started 抓取股價(Raw) ===")

    # B. 抓取股價 (Raw)
    start_date = params["start_date"]
    end_date = params["end_date"]

    # 若有指定區間，檔名中加入日期範圍方便辨識
    date_range_tag = None
    if start_date and end_date:
        date_range_tag = f"{start_date}_to_{end_date}"

    try:
        # 下載每日收盤價，回傳 wide format
        df_close_raw = YFinanceFetcher.fetch_daily_close_prices(
            stock_symbols=top_tickers,
            start_date=start_date,
            end_date=end_date,
            is_tw_stock=True,
        )

        # 儲存 Raw Data 到 Local：保留原始資料以便追查與重跑
        if date_range_tag:
            raw_filename = f"top{len(top_tickers)}_close_raw_{date_range_tag}_{timestamp}.parquet"
        else:
            raw_filename = f"top{len(top_tickers)}_close_raw_{timestamp}.parquet"
        raw_local_path = raw_dir / raw_filename
        df_close_raw.to_parquet(raw_local_path)

        # 上傳 Raw Data 到 GCS
        # 如果這裡發生 invalid_grant：請在 Terminal 執行 `gcloud auth application-default login`
        if not params["skip_gcs"]:
            gcs_mode_prefix = "latest" if params["stock_mode"] == "latest" else "interval"
            upload_file(
                bucket_name,
                raw_local_path,
                f"data/raw/{gcs_mode_prefix}/{date_folder}/{raw_filename}",
            )

    except Exception as e:
        # ingestion 失敗直接結束：避免後續步驟使用空資料
        logger.error(f"Ingestion Failed: {e}")
        return 1

    # ----------------------------------------------------------
    # 2. Transformation: 清洗與轉置
    # ----------------------------------------------------------
    logger.info("=== STEP 2: Transformation Started ===")

    try:
        # 執行轉置與清洗：轉為 long format 以符合資料倉儲 schema
        df_cleaned_price = Transformer.process_market_data(df_close_raw)

        if df_cleaned_price.empty:
            raise ValueError("Transformed data is empty! Check yfinance source.")

        # 型別一致性：確保 stock_id 永遠是字串，避免 Parquet schema 錯誤
        df_cleaned_price['stock_id'] = df_cleaned_price['stock_id'].astype(str)

        # 檢查重複值：同天同一支股票不應有兩筆資料，避免資料重複
        duplicates = df_cleaned_price.duplicated(subset=['date', 'stock_id']).sum()
        if duplicates > 0:
            logger.warning(f"Detected {duplicates} duplicate rows. Dropping duplicates...")
            df_cleaned_price = df_cleaned_price.drop_duplicates(subset=['date', 'stock_id'])

        # 儲存 Processed Data：轉換後資料也落地，方便追蹤與重跑
        processed_dir = ROOT_DIR / "data/processed" / params["stock_mode"] / date_folder
        processed_dir.mkdir(parents=True, exist_ok=True)

        if date_range_tag:
            processed_path = processed_dir / f"fact_price_{date_range_tag}_{timestamp}.parquet"
        else:
            processed_path = processed_dir / f"fact_price_{timestamp}.parquet"

        # 設定compression='snappy'：兼顧速度與體積
        df_cleaned_price.to_parquet(processed_path, index=False, compression='snappy')

        # 上傳 Processed Data 到 GCS（依模式分流）
        if not params["skip_gcs"]:
            gcs_mode_prefix = "latest" if params["stock_mode"] == "latest" else "interval"
            upload_file(
                bucket_name,
                processed_path,
                f"data/processed/{gcs_mode_prefix}/{date_folder}/{processed_path.name}",
            )

        logger.info(f"Transformation Success! Saved to: {processed_path.name}")
        logger.info(f"Summary: {len(df_cleaned_price)} rows, {df_cleaned_price['stock_id'].nunique()} tickers.")

    except Exception as e:
        # 轉換失敗直接停止：避免將異常資料寫入倉儲
        logger.error(f"Transformation Failed: {str(e)}")
        return 1

    # ----------------------------------------------------------
    # 3. Loading: 寫入 BigQuery (Data Warehouse)
    # ----------------------------------------------------------
    logger.info("=== STEP 3: Loading to BigQuery Started ===")

    try:
        if params["stock_mode"] == "latest":
            # latest 走固定 dataset/table 命名
            target_dataset = f"{bq_dataset}_latest"
            target_table = "fact_daily_price_latest"
        else:
            # interval 以市值基準日期與區間命名，確保可追溯
            mv_date = params["market_value_date"].replace("-", "")
            start_tag = params["start_date"].replace("-", "")
            end_tag = params["end_date"].replace("-", "")
            top_n = params["top_n"]
            target_dataset = f"{bq_dataset}_interval"
            target_table = f"fact_price_mv{mv_date}_s{start_tag}_e{end_tag}_top{top_n}"
        # 寫入 Fact Table：以 append 為主，避免覆蓋歷史資料
        load_to_bigquery(
            df=df_cleaned_price,
            dataset_id=target_dataset,
            table_id=target_table,
            if_exists="append",
        )
        logger.info("Pipeline Completed Successfully!")

    except Exception as e:
        # 寫入失敗記錄錯誤後終止：方便排查
        logger.error(f"Loading Failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())