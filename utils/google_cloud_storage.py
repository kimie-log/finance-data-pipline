"""
Google Cloud Storage 上傳與下載工具。

封裝 GCS 操作，統一使用重試機制處理暫時性錯誤（ServiceUnavailable、DeadlineExceeded 等）。
NotFound / Forbidden 例外不重試，直接記錄錯誤並拋出。
"""
from __future__ import annotations

from google.api_core import exceptions as gcp_exceptions
from google.cloud import storage

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


def upload_file(
    bucket_name: str,
    source_path: str | Path,
    destination_blob_name: str,
) -> None:
    """
    上傳檔案到 GCS，使用重試機制處理暫時性錯誤。

    Args:
        bucket_name: GCS bucket 名稱。
        source_path: 本地檔案路徑。
        destination_blob_name: GCS 中的目標 blob 名稱（含路徑）。

    Raises:
        NotFound: bucket 不存在或路徑錯誤時。
        Forbidden: 權限不足時（不重試，直接拋出）。
        其他可重試例外：會依 run_with_retry 邏輯重試。

    Note:
        - 使用 run_with_retry 處理暫時性錯誤（網路、服務暫時不可用等）。
        - NotFound / Forbidden 不重試，避免無效重試浪費時間。
        - 錯誤日誌含 bucket 與 blob 名稱，方便除錯。
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    try:
        run_with_retry(
            lambda: blob.upload_from_filename(str(source_path)),
            action_name=f"GCS upload {bucket_name}/{destination_blob_name}",
            retry_exceptions=RETRYABLE_EXCEPTIONS,
        )
    except gcp_exceptions.NotFound:
        logger.error(
            "GCS bucket not found: %s (upload %s)",
            bucket_name,
            destination_blob_name,
        )
        raise
    except gcp_exceptions.Forbidden as exc:
        logger.error(
            "GCS permission denied for bucket %s: %s",
            bucket_name,
            exc,
        )
        raise


def download_file(
    bucket_name: str,
    destination_path: str | Path,
    source_blob_name: str,
) -> None:
    """
    從 GCS 下載檔案，使用重試機制處理暫時性錯誤。

    Args:
        bucket_name: GCS bucket 名稱。
        destination_path: 本地目標檔案路徑。
        source_blob_name: GCS 中的來源 blob 名稱（含路徑）。

    Raises:
        NotFound: 物件不存在時。
        Forbidden: 權限不足時（不重試，直接拋出）。
        其他可重試例外：會依 run_with_retry 邏輯重試。

    Note:
        - 與 upload_file 相同的重試與錯誤處理邏輯。
        - 錯誤日誌含 bucket 與 blob 名稱，方便除錯。
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    try:
        run_with_retry(
            lambda: blob.download_to_filename(str(destination_path)),
            action_name=f"GCS download {bucket_name}/{source_blob_name}",
            retry_exceptions=RETRYABLE_EXCEPTIONS,
        )
    except gcp_exceptions.NotFound:
        logger.error(
            "GCS object not found: %s/%s",
            bucket_name,
            source_blob_name,
        )
        raise
    except gcp_exceptions.Forbidden as exc:
        logger.error(
            "GCS permission denied for bucket %s: %s",
            bucket_name,
            exc,
        )
        raise
