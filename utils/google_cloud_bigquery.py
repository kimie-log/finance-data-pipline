"""
BigQuery 資料載入工具：支援 upsert（暫存表 + MERGE）與 append/truncate 模式。

處理 DataFrame 型別轉換（pd.NA/NaT → None、object 欄位正規化、datetime 轉字串）、
Dataset 自動建立、暫存表清理、重試機制。upsert 模式使用 MERGE SQL 避免重複列，
append/truncate 模式走 Parquet 載入路徑以避免 object 欄位轉換問題。
"""
from __future__ import annotations

import gc
import io
import os
from typing import Annotated

import numpy as np
import pandas as pd
from google.api_core import exceptions as gcp_exceptions
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from utils.logger import logger
from utils.retry import run_with_retry

RETRYABLE_EXCEPTIONS = (
    gcp_exceptions.ServiceUnavailable,
    gcp_exceptions.DeadlineExceeded,
    gcp_exceptions.InternalServerError,
    gcp_exceptions.TooManyRequests,
    gcp_exceptions.Aborted,
    gcp_exceptions.GatewayTimeout,
)


def load_to_bigquery(
    df: Annotated[pd.DataFrame, "要上傳到 BigQuery 的 DataFrame"],
    dataset_id: Annotated[str, "BigQuery Dataset ID"],
    table_id: Annotated[str, "BigQuery Table ID"],
    if_exists: Annotated[str, "處理模式：'upsert'（暫存表 + MERGE）、'append'、'truncate'"] = 'upsert',
) -> None:
    """
    將 DataFrame 上傳到 BigQuery，支援 upsert（避免重複列）與 append/truncate 模式。

    Args:
        df: 要上傳的 DataFrame；會進行型別轉換（pd.NA/NaT → None、object 正規化）。
        dataset_id: BigQuery Dataset ID；不存在時會自動建立（location=asia-east1）。
        table_id: BigQuery Table ID。
        if_exists: 'upsert'（暫存表 + MERGE，主鍵 date + stock_id）、'append'、'truncate'。

    Raises:
        ValueError: GCP_PROJECT_ID 環境變數未設定時。
        NotFound: Dataset/table 操作失敗時（會自動建立 Dataset，但 table 不存在時需手動處理）。
        其他 GCP 例外：會依 run_with_retry 邏輯重試。

    Note:
        - **型別處理**：pd.NA/NaT → None、object 欄位非純量（list/dict）轉字串、全 None 的 object 欄位改空字串，
          避免 PyArrow 推斷 schema 失敗與 "arg must be a list..." 錯誤。
        - **upsert 模式**：使用暫存表 + MERGE SQL，主鍵 (date, stock_id)，更新 OHLCV、daily_return、交易可行性欄位；
          目標表不存在時直接從暫存表複製建立。
        - **append/truncate 模式**：走 Parquet 載入路徑，避免 load_table_from_dataframe 轉換 object 欄位時出錯。
        - **暫存表清理**：finally 區塊確保暫存表刪除，即使主流程失敗也嘗試清理；清理失敗僅記錄警告。
        - **記憶體回收**：處理後執行 gc.collect()，避免大資料處理後的記憶體殘留。
        - **Dataset 自動建立**：不存在時自動建立（location=asia-east1），避免後續任務失敗。
    """
    df = df.replace({pd.NA: None, pd.NaT: None}).copy()
    for col in df.columns:
        if df[col].dtype == object:
            def _scalarize(x):
                if pd.isna(x) or x is None:
                    return None
                if isinstance(x, (list, dict)):
                    return str(x)
                return x
            df[col] = df[col].map(_scalarize)
    for col in df.columns:
        if df[col].dtype == object and df[col].isna().all():
            df[col] = ""

    staging_table_ref = None

    try:
        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            logger.error("Missing required environment variable: GCP_PROJECT_ID")
            raise ValueError("GCP_PROJECT_ID is not set")

        client = bigquery.Client(project=project_id)

        dataset_ref = client.dataset(dataset_id)
        try:
            run_with_retry(
                lambda: client.get_dataset(dataset_ref),
                action_name=f"BigQuery get dataset {dataset_id}",
                retry_exceptions=RETRYABLE_EXCEPTIONS,
            )
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-east1"
            run_with_retry(
                lambda: client.create_dataset(dataset),
                action_name=f"BigQuery create dataset {dataset_id}",
                retry_exceptions=RETRYABLE_EXCEPTIONS,
            )
            logger.info("Created dataset: %s", dataset_id)

        if if_exists != 'upsert':
            def _col_name(c):
                return c[0] if isinstance(c, tuple) else str(c)

            seen = {}
            def _unique_key(col):
                k = _col_name(col)
                if k in seen:
                    seen[k] += 1
                    return f"{k}_{seen[k]}"
                seen[k] = 0
                return k

            data = {}
            for col in df.columns:
                key = _unique_key(col)
                if df[col].dtype == object:
                    data[key] = ["" if (x is None or pd.isna(x)) else str(x) for x in df[col]]
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    s = pd.to_datetime(df[col], errors="coerce")
                    data[key] = [(x.strftime("%Y-%m-%d") if pd.notna(x) else "") for x in s]
                else:
                    data[key] = df[col].tolist()
            _df = pd.DataFrame(data)
            job_config = bigquery.LoadJobConfig(
                write_disposition=f"WRITE_{if_exists.upper()}",
                source_format=bigquery.SourceFormat.PARQUET,
            )
            table_ref = dataset_ref.table(table_id)

            def _write_and_load():
                buf = io.BytesIO()
                _df.to_parquet(buf, index=False, engine="pyarrow")
                buf.seek(0)
                return client.load_table_from_file(buf, table_ref, job_config=job_config).result()

            run_with_retry(
                _write_and_load,
                action_name=f"BigQuery load {dataset_id}.{table_id} (parquet)",
                retry_exceptions=RETRYABLE_EXCEPTIONS,
            )
            return

        staging_table_id = f"{table_id}_staging_{pd.Timestamp.now().strftime('%H%M%S')}"
        staging_table_ref = dataset_ref.table(staging_table_id)
        target_table_ref = dataset_ref.table(table_id)

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        run_with_retry(
            lambda: client.load_table_from_dataframe(
                df, staging_table_ref, job_config=job_config
            ).result(),
            action_name=f"BigQuery load staging {dataset_id}.{staging_table_id}",
            retry_exceptions=RETRYABLE_EXCEPTIONS,
        )

        merge_sql = f"""
        MERGE `{project_id}.{dataset_id}.{table_id}` T
        USING `{project_id}.{dataset_id}.{staging_table_id}` S
        ON T.date = S.date AND T.stock_id = S.stock_id
        WHEN MATCHED THEN
            UPDATE SET
                T.open = S.open,
                T.high = S.high,
                T.low = S.low,
                T.close = S.close,
                T.volume = S.volume,
                T.daily_return = S.daily_return,
                T.is_suspended = S.is_suspended,
                T.is_limit_up = S.is_limit_up,
                T.is_limit_down = S.is_limit_down
        WHEN NOT MATCHED THEN
            INSERT (date, stock_id, open, high, low, close, volume, daily_return, is_suspended, is_limit_up, is_limit_down)
            VALUES (S.date, S.stock_id, S.open, S.high, S.low, S.close, S.volume, S.daily_return, S.is_suspended, S.is_limit_up, S.is_limit_down)
        """

        try:
            run_with_retry(
                lambda: client.get_table(target_table_ref),
                action_name=f"BigQuery get table {dataset_id}.{table_id}",
                retry_exceptions=RETRYABLE_EXCEPTIONS,
            )
            run_with_retry(
                lambda: client.query(merge_sql).result(),
                action_name=f"BigQuery merge {dataset_id}.{table_id}",
                retry_exceptions=RETRYABLE_EXCEPTIONS,
            )
            logger.info("Upsert completed for %s", table_id)
        except NotFound:
            logger.info("Target table %s not found, creating from staging...", table_id)
            job_config = bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")
            run_with_retry(
                lambda: client.copy_table(
                    staging_table_ref,
                    target_table_ref,
                    job_config=job_config,
                    location="asia-east1",
                ).result(),
                action_name=f"BigQuery copy {dataset_id}.{staging_table_id} -> {table_id}",
                retry_exceptions=RETRYABLE_EXCEPTIONS,
            )

        run_with_retry(
            lambda: client.delete_table(staging_table_ref, not_found_ok=True),
            action_name=f"BigQuery delete staging {dataset_id}.{staging_table_id}",
            retry_exceptions=RETRYABLE_EXCEPTIONS,
        )

    except Exception as e:
        logger.exception(
            "BigQuery Load Error (dataset=%s, table=%s, mode=%s): %s",
            dataset_id,
            table_id,
            if_exists,
            e,
        )
        raise
    finally:
        if staging_table_ref is not None:
            try:
                run_with_retry(
                    lambda: client.delete_table(staging_table_ref, not_found_ok=True),
                    action_name=f"BigQuery cleanup staging {dataset_id}.{staging_table_id}",
                    retry_exceptions=RETRYABLE_EXCEPTIONS,
                )
            except Exception:
                logger.warning(
                    "Failed to cleanup staging table %s (ignored).",
                    staging_table_ref.table_id,
                )
        gc.collect()
