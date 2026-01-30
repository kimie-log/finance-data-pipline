"""
通用重試工具：指數退避 + jitter。

用於處理網路請求、API 呼叫等可能因暫時性錯誤失敗的操作。
支援自訂重試次數、退避參數、可重試例外類型，避免無限重試與同步重試造成壓力峰值。
"""
from __future__ import annotations

import random
import time
from typing import Callable, Iterable, TypeVar, Annotated

from utils.logger import logger

T = TypeVar("T")


def run_with_retry(
    action: Callable[[], T],
    *,
    action_name: Annotated[str, "動作名稱，用於日誌識別"],
    retries: Annotated[int, "失敗後可重試次數（不含第一次）"] = 3,
    initial_delay: Annotated[float, "初始延遲秒數"] = 1.0,
    max_delay: Annotated[float, "最大延遲秒數上限"] = 30.0,
    backoff: Annotated[float, "指數退避倍數"] = 2.0,
    jitter: Annotated[float, "隨機抖動比例（0.0～1.0）"] = 0.2,
    retry_exceptions: Annotated[
        Iterable[type[BaseException]],
        "可重試的例外類型，預設為所有 Exception",
    ] = (Exception,),
) -> T:
    """
    以指數退避 + jitter 重試執行指定動作，失敗時記錄日誌並拋出最後一次例外。

    Args:
        action: 要執行的動作（無參數 callable）。
        action_name: 動作名稱，用於日誌識別（例："GCS upload bucket/file"）。
        retries: 失敗後可重試次數（不含第一次）；總嘗試次數 = retries + 1。
        initial_delay: 第一次重試前的初始延遲（秒）。
        max_delay: 延遲時間上限（秒），避免退避時間過長。
        backoff: 指數退避倍數；第 n 次重試延遲 = initial_delay * (backoff ** (n-1))。
        jitter: 隨機抖動比例；延遲時間會乘以 (1 ± jitter)，避免多個 worker 同步重試。
        retry_exceptions: 可重試的例外類型；非此類型的例外會直接拋出，不重試。

    Returns:
        動作成功時的回傳值。

    Raises:
        最後一次嘗試失敗時拋出對應例外，並記錄完整 traceback。

    Note:
        - 重試時以 WARNING 級別記錄，耗盡時以 ERROR 級別記錄完整例外。
        - jitter 避免多個 worker 同時重試造成壓力峰值（thundering herd）。
        - 權限錯誤（如 Forbidden）通常不應重試，建議在 retry_exceptions 中排除。
    """
    total_attempts = retries + 1
    retry_exceptions = tuple(retry_exceptions)

    for attempt in range(1, total_attempts + 1):
        try:
            return action()
        except retry_exceptions as exc:
            if attempt >= total_attempts:
                logger.exception(
                    "%s failed after %s attempts: %s",
                    action_name,
                    total_attempts,
                    exc,
                )
                raise

            delay = min(max_delay, initial_delay * (backoff ** (attempt - 1)))
            if jitter > 0:
                delay *= 1 + random.uniform(-jitter, jitter)
            delay = max(0.0, delay)

            logger.warning(
                "%s failed (attempt %s/%s): %s. Retrying in %.2fs...",
                action_name,
                attempt,
                total_attempts,
                exc,
                delay,
            )
            time.sleep(delay)
