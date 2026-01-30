"""
日誌設定模組：實務化 logging 配置。

提供 console 與檔案（輪替）輸出，支援環境變數 LOG_LEVEL / LOG_DIR 動態調整。
避免重複 handler、不冒泡到 root logger，適合 ETL pipeline 長期運行與除錯。
"""
import logging
import logging.handlers
import os
from pathlib import Path
from typing import Optional


def _get_log_level() -> int:
    """
    從環境變數 LOG_LEVEL 取得日誌等級，預設為 INFO。

    Returns:
        logging 等級常數；環境變數不存在或無效時回退到 INFO。

    Note:
        環境變數優先，便於在不同環境（開發／生產）動態調整，無需改程式碼。
    """
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    return getattr(logging, level, logging.INFO)


def _get_log_dir() -> Path:
    """
    取得 log 輸出目錄：環境變數 LOG_DIR 優先，否則預設為專案根目錄下的 logs/。

    Returns:
        log 目錄 Path；目錄不存在時會自動建立。

    Note:
        確保目錄存在，避免 handler 初始化失敗；預設 logs/ 位於專案根目錄。
    """
    log_dir_env = os.getenv("LOG_DIR")
    if log_dir_env:
        log_dir = Path(log_dir_env)
    else:
        root_dir = Path(__file__).resolve().parents[1]
        log_dir = root_dir / "logs"

    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def configure_logger(name: Optional[str] = "finance_pipeline") -> logging.Logger:
    """
    建立並回傳一個實務化的 logger，支援 console 與檔案（輪替）輸出。

    Args:
        name: logger 名稱，預設 "finance_pipeline"；不同名稱對應不同 logger 實例。

    Returns:
        配置完成的 logging.Logger；已設定過 handler 時直接回傳，避免重複輸出。

    Note:
        - Console handler：輸出到 stderr，等級依 LOG_LEVEL。
        - RotatingFileHandler：寫入 logs/{name}.log，10 MB 輪替、保留 5 個備份、UTF-8 編碼。
        - Formatter：含時間、等級、logger 名稱、檔案名與行號，方便除錯。
        - propagate=False：不冒泡到 root，避免被外部程式重複處理。
        - 檔案 handler 初始化失敗時僅記錄警告，不阻擋程式執行（fallback 到 console only）。
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(_get_log_level())

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(_get_log_level())
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    try:
        log_dir = _get_log_dir()
        log_file = log_dir / "finance_pipeline.log"

        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(_get_log_level())
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception:
        logger.warning("Failed to initialize file logging. Falling back to console only.")

    logger.propagate = False

    return logger


logger = configure_logger()
