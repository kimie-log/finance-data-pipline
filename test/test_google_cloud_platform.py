"""
utils/google_cloud_platform 的單元測試：check_gcp_environment。

驗證 GCP 金鑰檢查、最新金鑰選擇、.gitignore 建立。
"""
import os
import time

import pytest

from utils.google_cloud_platform import check_gcp_environment


def test_check_gcp_environment_exits_when_missing_keys(tmp_path):
    """
    驗證沒有 gcp_keys/ 或金鑰檔時直接退出（SystemExit）。

    實務：避免後續步驟因缺少金鑰而失敗，提早終止並提示使用者。
    """
    with pytest.raises(SystemExit):
        check_gcp_environment(tmp_path)


def test_check_gcp_environment_returns_latest_key(tmp_path):
    """
    驗證多個金鑰檔時選擇最新修改時間的檔案，並建立 .gitignore。

    實務：支援多個 Service Account 金鑰輪替；.gitignore 避免金鑰被 commit。
    """
    key_dir = tmp_path / "gcp_keys"
    key_dir.mkdir(parents=True, exist_ok=True)

    first_key = key_dir / "first.json"
    second_key = key_dir / "second.json"
    first_key.write_text("{}")
    second_key.write_text("{}")

    now = time.time()
    os.utime(first_key, (now - 10, now - 10))
    os.utime(second_key, (now, now))

    result = check_gcp_environment(tmp_path)

    assert result == "second.json"
    gitignore = key_dir / ".gitignore"
    assert gitignore.exists()
    assert gitignore.read_text() == "*.json\n"
