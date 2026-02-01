"""
Alphalens 單因子分析腳本

從 BigQuery 或本地 Parquet 檔案讀取價量和因子資料，進行 Alphalens 單因子分析。

流程：
    1. 讀取價量資料（fact_price）→ 轉換為 Alphalens 所需的價格格式
    2. 讀取因子資料（fact_factor）→ 轉換為 Alphalens 所需的因子格式
    3. 使用 Alphalens 進行因子分析並產生報告

執行：
    python -m scripts.run_single_factor_analysis \
        --dataset <dataset_id> \
        --factor <factor_name> \
        --start <start_date> \
        --end <end_date> \
        [--local-price <path>] \
        [--local-factor <path>] \
        [--quantiles 5] \
        [--periods 1,5,10]

依賴：
    .env（GCP_PROJECT_ID）
    alphalens-reloaded
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Optional
from datetime import datetime

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # 使用非交互式後端，避免顯示視窗
import matplotlib.pyplot as plt
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

load_dotenv()

try:
    from alphalens.tears import create_full_tear_sheet
    from alphalens.utils import get_clean_factor_and_forward_returns
except ImportError:
    print("錯誤：請先安裝 alphalens-reloaded")
    print("執行：pip install alphalens-reloaded")
    sys.exit(1)

from utils.data_loader import load_price_data, load_factor_data
from utils.logger import logger
from utils.cli import load_config, resolve_single_factor_params
from factors.finlab_factor_fetcher import FinLabFactorFetcher
from ingestion.finlab_fetcher import FinLabFetcher


def prepare_prices_for_alphalens(df_price: pd.DataFrame) -> pd.DataFrame:
    """
    將價量資料轉換為 Alphalens 所需的格式

    Args:
        df_price: DataFrame 欄位：date, stock_id, close, ...

    Returns:
        DataFrame，index 為 date，columns 為 stock_id，values 為 close price
    """
    prices = df_price.pivot(index="date", columns="stock_id", values="close")
    prices.index = pd.to_datetime(prices.index)
    prices = prices.sort_index()
    logger.info(f"價格資料準備完成: {prices.shape[0]} 個交易日，{prices.shape[1]} 檔股票")
    return prices


def prepare_factor_for_alphalens(df_factor: pd.DataFrame) -> pd.Series:
    """
    將因子資料轉換為 Alphalens 所需的格式

    Args:
        df_factor: DataFrame，MultiIndex (date, stock_id)，單一欄位為因子值

    Returns:
        Series，MultiIndex (date, stock_id)，值為因子值

    Note:
        使用 squeeze() 與參考檔案邏輯一致，確保 DataFrame 轉為 Series
    """
    if isinstance(df_factor, pd.DataFrame):
        # 使用 squeeze() 確保轉成 Series，與參考檔案邏輯一致
        # 如果 DataFrame 只有一欄，會自動轉成 Series
        factor = df_factor.squeeze()
    else:
        factor = df_factor

    # 確保是 Series
    if not isinstance(factor, pd.Series):
        raise ValueError("因子資料必須是 Series")

    # 確保是 MultiIndex
    if not isinstance(factor.index, pd.MultiIndex):
        raise ValueError("因子資料必須是 MultiIndex (date, stock_id)")

    # 移除缺失值（與參考檔案一致）
    factor = factor.dropna()
    logger.info(f"因子資料準備完成: {len(factor)} 筆有效資料")
    return factor


def find_local_parquet_files(
    dataset_id: str,
    start_date: str,
    end_date: str,
    data_type: str = "price",
) -> Optional[Path]:
    """
    在本地 data/processed 目錄中尋找符合條件的 parquet 檔案

    Args:
        dataset_id: Dataset ID（用於推測檔案命名）
        start_date: 起始日期
        end_date: 結束日期
        data_type: "price" 或 "factor"

    Returns:
        找到的檔案路徑，若未找到則返回 None
    """
    processed_dir = ROOT_DIR / "data" / "processed"
    if not processed_dir.exists():
        return None

    # 搜尋所有 parquet 檔案
    pattern = "fact_price*.parquet" if data_type == "price" else "fact_factor*.parquet"
    parquet_files = list(processed_dir.rglob(pattern))

    if not parquet_files:
        return None

    # 優先選擇最新的檔案
    latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
    logger.info(f"找到本地 {data_type} 檔案: {latest_file}")
    return latest_file


def main() -> int:
    """
    Alphalens 單因子分析主流程

    Returns:
        0 成功；1 失敗
    """
    config = load_config(ROOT_DIR)
    cfg = config.get("single_factor_analysis", {})

    parser = argparse.ArgumentParser(
        description="Alphalens 單因子分析。預設使用 config/settings.yaml，僅在 CLI 有加參數時覆寫。"
    )
    parser.add_argument("--dataset", default=cfg.get("dataset"), help="BigQuery Dataset ID；未指定時用 config")
    parser.add_argument("--factor", default=cfg.get("factor"), help="因子名稱；未指定時用 config")
    parser.add_argument("--start", default=cfg.get("start"), help="分析區間起始 (YYYY-MM-DD)；未指定時用 config")
    parser.add_argument("--end", default=cfg.get("end"), help="分析區間結束 (YYYY-MM-DD)；未指定時用 config")
    parser.add_argument("--local-price", default=cfg.get("local_price"), help="本地價量 parquet 路徑")
    parser.add_argument("--local-factor", default=cfg.get("local_factor"), help="本地因子 parquet 路徑")
    parser.add_argument("--quantiles", type=int, default=cfg.get("quantiles", 5), help="分位數數量；未指定時用 config")
    parser.add_argument("--periods", type=str, default=cfg.get("periods", "1,5,10"), help="前瞻期間，逗號分隔；未指定時用 config")
    parser.add_argument("--factor-table", default=cfg.get("factor_table", "fact_factor"), help="因子表名稱；未指定時用 config")
    parser.add_argument(
        "--auto-find-local",
        action="store_true",
        help="自動在 data/processed 尋找本地檔案；未加則用 config.auto_find_local",
    )
    parser.add_argument(
        "--from-finlab-api",
        action="store_true",
        help="從 FinLab API 抓取因子；未加則用 config.from_finlab_api",
    )

    args = parser.parse_args()
    params = resolve_single_factor_params(config, args)

    if not params["dataset"] or not params["factor"] or not params["start"] or not params["end"]:
        logger.error("請提供 dataset、factor、start、end（可從 config.single_factor_analysis 或 CLI 指定）")
        return 1

    # 解析期間
    periods = [int(p.strip()) for p in str(params["periods"]).split(",")]

    logger.info("=== Alphalens 單因子分析開始 ===")
    logger.info(f"Dataset: {params['dataset']}")
    logger.info(f"因子: {params['factor']}")
    logger.info(f"日期範圍: {params['start']} ~ {params['end']}")
    logger.info(f"分位數: {params['quantiles']}")
    logger.info(f"前瞻期間: {periods}")

    try:
        # 1. 讀取價量資料
        local_price_path = params["local_price"]
        if params["auto_find_local"] and not local_price_path:
            found = find_local_parquet_files(params["dataset"], params["start"], params["end"], "price")
            if found:
                local_price_path = str(found)

        logger.info("=== 步驟 1: 讀取價量資料 ===")
        df_price = load_price_data(
            dataset_id=params["dataset"],
            start_date=params["start"],
            end_date=params["end"],
            local_parquet_path=local_price_path,
            use_local_first=True,
        )

        if df_price.empty:
            logger.error("價量資料為空，請檢查日期範圍和資料來源")
            return 1

        # 2. 讀取因子資料
        local_factor_path = params["local_factor"]
        if params["auto_find_local"] and not local_factor_path:
            found = find_local_parquet_files(
                params["dataset"], params["start"], params["end"], "factor"
            )
            if found:
                local_factor_path = str(found)

        logger.info("=== 步驟 2: 讀取因子資料 ===")
        
        # 嘗試從本地檔案或 BigQuery 讀取因子資料
        df_factor = None
        try:
            df_factor = load_factor_data(
                dataset_id=params["dataset"],
                factor_name=params["factor"],
                start_date=params["start"],
                end_date=params["end"],
                local_parquet_path=local_factor_path,
                use_local_first=True,
                factor_table=params["factor_table"],
            )
        except Exception as e:
            logger.warning(f"無法從本地檔案或 BigQuery 讀取因子資料: {e}")
            if not params["from_finlab_api"]:
                logger.info("提示：使用 --from-finlab-api 可從 FinLab API 直接抓取因子資料")
        
        # 如果沒有因子資料且指定從 FinLab API 抓取
        if (df_factor is None or df_factor.empty) and params["from_finlab_api"]:
            logger.info("從 FinLab API 直接抓取因子資料...")
            try:
                FinLabFetcher.finlab_login()
                
                # 從價量資料中提取股票列表和交易日序列
                stock_ids = sorted(df_price["stock_id"].unique().tolist())
                trading_days = pd.DatetimeIndex(pd.to_datetime(df_price["date"].unique())).sort_values()
                
                logger.info(f"股票數量: {len(stock_ids)}, 交易日數量: {len(trading_days)}")
                
                # 從 FinLab API 抓取因子資料
                factor_df_long = FinLabFactorFetcher.get_factor_data(
                    stock_symbols=stock_ids,
                    factor_name=params["factor"],
                    trading_days=trading_days,
                )
                
                if factor_df_long.empty:
                    raise ValueError("FinLab API 返回空的因子資料")
                
                # 轉換為 MultiIndex 格式（date, stock_id）
                factor_df_long = factor_df_long.rename(columns={"datetime": "date", "asset": "stock_id"})
                factor_df_long["date"] = pd.to_datetime(factor_df_long["date"])
                
                # 過濾日期範圍
                factor_df_long = factor_df_long[
                    (factor_df_long["date"] >= params["start"]) & 
                    (factor_df_long["date"] <= params["end"])
                ]
                
                # 轉換為 MultiIndex DataFrame
                df_factor = factor_df_long.set_index(["date", "stock_id"])[["value"]]
                df_factor.columns = [params["factor"]]
                
                logger.info(f"從 FinLab API 成功抓取 {len(df_factor)} 筆因子資料")
                
            except Exception as e:
                logger.error(f"從 FinLab API 抓取因子資料失敗: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return 1
        
        if df_factor is None or df_factor.empty:
            logger.error("因子資料為空，請檢查：")
            logger.error("  1. 因子名稱是否正確（使用 python -m factors.list_factors 查看）")
            logger.error("  2. BigQuery 是否有對應的因子資料")
            logger.error("  3. 或使用 --from-finlab-api 從 FinLab API 直接抓取")
            return 1

        # 3. 轉換為 Alphalens 格式
        logger.info("=== 步驟 3: 轉換資料格式 ===")
        prices = prepare_prices_for_alphalens(df_price)
        factor = prepare_factor_for_alphalens(df_factor)

        # 4. 準備 Alphalens 分析資料
        logger.info("=== 步驟 4: 準備 Alphalens 分析資料 ===")
        alphalens_data = get_clean_factor_and_forward_returns(
            factor=factor,
            prices=prices,
            quantiles=params["quantiles"],
            periods=periods,
        )

        logger.info(f"Alphalens 資料準備完成: {len(alphalens_data)} 筆")

        # 5. 產生分析報告
        logger.info("=== 步驟 5: 產生 Alphalens 分析報告 ===")
        
        # 準備報表輸出目錄（同一個時間段的報表存在同一個資料夾）
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        factor_safe_name = params["factor"].replace("/", "_").replace("\\", "_")
        
        # 資料夾名稱：因子名稱_s開始日期_e結束日期_時間戳
        folder_name = f"{factor_safe_name}_s{params['start']}_e{params['end']}_{timestamp}"
        report_dir = ROOT_DIR / "data" / "single_factor_analysis_reports" / folder_name
        report_dir.mkdir(parents=True, exist_ok=True)
        
        # 檔案名稱：使用時間戳區分不同執行時間
        report_filename = f"alphalens_{timestamp}"
        report_path = report_dir / report_filename

        # 記錄呼叫前已存在的 figure，只儲存 Alphalens 新建立的圖表
        figures_before = set(plt.get_fignums())
        create_full_tear_sheet(alphalens_data)
        new_fignums = sorted(set(plt.get_fignums()) - figures_before)

        if new_fignums:
            from matplotlib.backends.backend_pdf import PdfPages
            pdf_path = report_path.with_suffix(".pdf")
            try:
                with PdfPages(pdf_path) as pdf:
                    for i, fig_num in enumerate(new_fignums):
                        fig = plt.figure(fig_num)
                        fig.canvas.draw()
                        pdf.savefig(fig, bbox_inches="tight", facecolor="white")
                        png_path = report_path.parent / f"{report_filename}_page_{i + 1:02d}.png"
                        fig.savefig(png_path, dpi=150, bbox_inches="tight", facecolor="white")
                        logger.info(f"  已保存圖表 {i + 1}: {png_path.name}")
                        plt.close(fig)
                logger.info(f"已保存 PDF: {pdf_path.name}")
            except Exception as e:
                logger.warning(f"保存 PDF/PNG 失敗: {e}")
        else:
            # 若沒有新 figure，可能是畫在當前 figure 上，儲存 gcf
            fig = plt.gcf()
            if fig.axes:
                fig.canvas.draw()
                try:
                    pdf_path = report_path.with_suffix(".pdf")
                    fig.savefig(pdf_path, bbox_inches="tight", facecolor="white")
                    png_path = report_path.parent / f"{report_filename}_page_01.png"
                    fig.savefig(png_path, dpi=150, bbox_inches="tight", facecolor="white")
                    logger.info(f"已保存 PDF: {pdf_path.name}, PNG: {png_path.name}")
                except Exception as e:
                    logger.warning(f"保存報表失敗: {e}")
                plt.close(fig)
            else:
                logger.warning("未偵測到 Alphalens 產生的圖表，請檢查 alphalens 版本與資料。")

        logger.info(f"報表已保存至: {report_dir}")
        logger.info("=== Alphalens 單因子分析完成 ===")
        return 0

    except Exception as e:
        import traceback

        logger.error(f"分析失敗: {e}")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
