"""
utils/google_cloud_storage 的單元測試：upload_file、download_file。

驗證 GCS 上傳／下載使用重試機制、NotFound 例外處理與錯誤日誌。
"""
from unittest import mock

import pytest

from conftest import require_module


def _get_modules():
    """
    取得 GCS 模組與例外類別，確保相依套件存在。

    Returns:
        (gcp_exceptions, gcs_module) 元組。
    """
    require_module("google.cloud.storage", "pip install -r requirements.txt")
    require_module("google.api_core", "pip install -r requirements.txt")
    from google.api_core import exceptions as gcp_exceptions
    from utils import google_cloud_storage as gcs

    return gcp_exceptions, gcs


def test_upload_uses_retry():
    """
    驗證 upload_file 透過 run_with_retry 執行，確保重試機制生效。

    實務：GCS 操作可能因網路或服務暫時性錯誤失敗，需重試；驗證 action_name 格式正確。
    """
    _, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.storage.Client") as mock_client:
            blob = mock.Mock()
            bucket = mock.Mock()
            bucket.blob.return_value = blob
            mock_client.return_value.bucket.return_value = bucket

            gcs.upload_file("bucket", "/tmp/source", "dest")

            mock_retry.assert_called_once()
            _, kwargs = mock_retry.call_args
            assert kwargs["action_name"] == "GCS upload bucket/dest"


def test_upload_not_found_logs():
    """
    驗證 upload_file 遇到 NotFound 時記錄錯誤日誌並拋出例外。

    實務：檔案不存在或 bucket 不存在時應清楚記錄，便於除錯。
    """
    gcp_exceptions, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.logger") as mock_logger:
            mock_retry.side_effect = gcp_exceptions.NotFound("missing")

            with pytest.raises(gcp_exceptions.NotFound):
                gcs.upload_file("bucket", "/tmp/source", "dest")

            mock_logger.error.assert_called()


def test_download_uses_retry():
    """
    驗證 download_file 透過 run_with_retry 執行，確保重試機制生效。

    實務：與 upload 相同，下載也可能因網路問題失敗，需重試。
    """
    _, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.storage.Client") as mock_client:
            blob = mock.Mock()
            bucket = mock.Mock()
            bucket.blob.return_value = blob
            mock_client.return_value.bucket.return_value = bucket

            gcs.download_file("bucket", "/tmp/dest", "source")

            mock_retry.assert_called_once()
            _, kwargs = mock_retry.call_args
            assert kwargs["action_name"] == "GCS download bucket/source"


def test_download_not_found_logs():
    """
    驗證 download_file 遇到 NotFound 時記錄錯誤日誌並拋出例外。

    實務：來源檔案不存在時應清楚記錄，避免誤判為下載成功。
    """
    gcp_exceptions, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.logger") as mock_logger:
            mock_retry.side_effect = gcp_exceptions.NotFound("missing")

            with pytest.raises(gcp_exceptions.NotFound):
                gcs.download_file("bucket", "/tmp/dest", "source")

            mock_logger.error.assert_called()
