"""
GCP 環境檢查工具：金鑰目錄管理與最新金鑰選擇。

確保 gcp_keys/ 存在、建立 .gitignore 防止金鑰被 commit、選擇最新修改時間的金鑰檔。
缺少金鑰時直接退出（SystemExit），避免後續 API 呼叫失敗。
"""
from __future__ import annotations

import sys
from pathlib import Path

from utils.logger import logger


def check_gcp_environment(root_dir: Path) -> str:
    """
    檢查 GCP 環境：確保 gcp_keys/ 存在、建立 .gitignore、選擇最新金鑰檔。

    Args:
        root_dir: 專案根目錄，用於定位 gcp_keys/。

    Returns:
        最新金鑰檔名（不含路徑）；缺少金鑰時會 sys.exit(1)。

    Note:
        - 自動建立 gcp_keys/ 與 .gitignore（忽略 *.json），避免金鑰被 commit。
        - 依修改時間選擇最新金鑰，支援多個 Service Account 金鑰輪替。
        - 缺少金鑰時直接退出，避免後續步驟因認證失敗而浪費時間。
    """
    key_dir = root_dir / "gcp_keys"

    key_dir.mkdir(parents=True, exist_ok=True)

    gitignore = key_dir / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text("*.json\n")

    json_keys = list(key_dir.glob("*.json"))

    if not json_keys:
        logger.warning(f"⚠️  尚未偵測到金鑰。請將 GCP 服務帳戶 JSON 檔案放入：{key_dir}")
        sys.exit(1)

    latest_key = max(json_keys, key=lambda p: p.stat().st_mtime)

    return latest_key.name
