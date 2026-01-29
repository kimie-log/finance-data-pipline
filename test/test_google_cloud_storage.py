from unittest import mock

import pytest

from conftest import require_module


def _get_modules():
    # 確保必要的 GCS 相依套件存在
    require_module("google.cloud.storage", "pip install -r requirements.txt")
    require_module("google.api_core", "pip install -r requirements.txt")
    from google.api_core import exceptions as gcp_exceptions
    from utils import google_cloud_storage as gcs

    return gcp_exceptions, gcs


def test_upload_uses_retry():
    # 準備：取得模組並 mock GCS client
    _, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.storage.Client") as mock_client:
            blob = mock.Mock()
            bucket = mock.Mock()
            bucket.blob.return_value = blob
            mock_client.return_value.bucket.return_value = bucket

            # 執行：上傳流程
            gcs.upload_file("bucket", "/tmp/source", "dest")

            # 驗證：有透過 run_with_retry 呼叫
            mock_retry.assert_called_once()
            _, kwargs = mock_retry.call_args
            assert kwargs["action_name"] == "GCS upload bucket/dest"


def test_upload_not_found_logs():
    # 準備：模擬 NotFound 例外
    gcp_exceptions, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.logger") as mock_logger:
            mock_retry.side_effect = gcp_exceptions.NotFound("missing")

            # 執行/驗證：應拋出 NotFound 且記錄錯誤
            with pytest.raises(gcp_exceptions.NotFound):
                gcs.upload_file("bucket", "/tmp/source", "dest")

            mock_logger.error.assert_called()


def test_download_uses_retry():
    # 準備：取得模組並 mock GCS client
    _, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.storage.Client") as mock_client:
            blob = mock.Mock()
            bucket = mock.Mock()
            bucket.blob.return_value = blob
            mock_client.return_value.bucket.return_value = bucket

            # 執行：下載流程
            gcs.download_file("bucket", "/tmp/dest", "source")

            # 驗證：有透過 run_with_retry 呼叫
            mock_retry.assert_called_once()
            _, kwargs = mock_retry.call_args
            assert kwargs["action_name"] == "GCS download bucket/source"


def test_download_not_found_logs():
    # 準備：模擬 NotFound 例外
    gcp_exceptions, gcs = _get_modules()
    with mock.patch("utils.google_cloud_storage.run_with_retry") as mock_retry:
        with mock.patch("utils.google_cloud_storage.logger") as mock_logger:
            mock_retry.side_effect = gcp_exceptions.NotFound("missing")

            # 執行/驗證：應拋出 NotFound 且記錄錯誤
            with pytest.raises(gcp_exceptions.NotFound):
                gcs.download_file("bucket", "/tmp/dest", "source")

            mock_logger.error.assert_called()
