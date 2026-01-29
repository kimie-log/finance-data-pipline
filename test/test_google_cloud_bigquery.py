from unittest import mock

import pandas as pd
import pytest

from conftest import require_module


def _get_module():
    # 確保必要的 BigQuery 相依套件存在
    require_module("google.cloud.bigquery", "pip install -r requirements.txt")
    require_module("google.api_core", "pip install -r requirements.txt")
    from utils import google_cloud_bigquery as gcbq

    return gcbq


def test_missing_project_id_raises():
    # 準備：取得模組並模擬缺少 GCP_PROJECT_ID
    gcbq = _get_module()

    # 執行/驗證：缺少 env 時應拋出 ValueError
    with mock.patch("utils.google_cloud_bigquery.os.getenv", return_value=None):
        with pytest.raises(ValueError):
            gcbq.load_to_bigquery(pd.DataFrame(), "dataset", "table")


def test_append_load_uses_load_job():
    # 準備：取得模組並 mock BigQuery client 與 retry
    gcbq = _get_module()
    with mock.patch("utils.google_cloud_bigquery.os.getenv", return_value="project"):
        with mock.patch("utils.google_cloud_bigquery.bigquery.Client") as mock_client_cls:
            with mock.patch("utils.google_cloud_bigquery.run_with_retry") as mock_retry:
                client = mock_client_cls.return_value
                dataset_ref = mock.Mock()
                table_ref = mock.Mock()
                dataset_ref.table.return_value = table_ref
                client.dataset.return_value = dataset_ref

                load_job = mock.Mock()
                load_job.result.return_value = None
                client.load_table_from_dataframe.return_value = load_job

                # 讓 run_with_retry 直接執行原 action
                def passthrough(action, **kwargs):
                    return action()

                mock_retry.side_effect = passthrough

                # 執行：append 模式
                df = pd.DataFrame([{"a": 1}])
                gcbq.load_to_bigquery(df, "dataset", "table", if_exists="append")

                # 驗證：有呼叫 load_table_from_dataframe
                client.load_table_from_dataframe.assert_called_once()
