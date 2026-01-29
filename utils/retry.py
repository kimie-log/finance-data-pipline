import random
import time
from typing import Callable, Iterable, Tuple, TypeVar

from utils.logger import logger

T = TypeVar("T")


def run_with_retry(
    action: Callable[[], T],
    *,
    action_name: str,
    retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 30.0,
    backoff: float = 2.0,
    jitter: float = 0.2,
    retry_exceptions: Iterable[type[BaseException]] = (Exception,),
) -> T:
    """
    以指數退避重試執行指定動作。
    - retries: 失敗後可重試的次數（不含第一次）
    - jitter: 隨機抖動比例，避免同時重試造成尖峰
    """
    # 總嘗試次數 = 第一次 + 重試次數
    total_attempts = retries + 1
    # 確保可用於 except 的 tuple 形式
    retry_exceptions = tuple(retry_exceptions)

    for attempt in range(1, total_attempts + 1):
        try:
            # 執行外部動作，成功則直接回傳
            return action()
        except retry_exceptions as exc:
            # 超過最大重試次數就記錄完整例外並拋出
            if attempt >= total_attempts:
                logger.exception(
                    "%s failed after %s attempts: %s",
                    action_name,
                    total_attempts,
                    exc,
                )
                raise

            # 計算指數退避延遲時間，並限制最大延遲
            delay = min(max_delay, initial_delay * (backoff ** (attempt - 1)))
            # jitter 避免多個 worker 同步重試造成壓力峰值
            if jitter > 0:
                delay *= 1 + random.uniform(-jitter, jitter)
            delay = max(0.0, delay)

            # 以 warning 級別記錄重試資訊，方便運維排查
            logger.warning(
                "%s failed (attempt %s/%s): %s. Retrying in %.2fs...",
                action_name,
                attempt,
                total_attempts,
                exc,
                delay,
            )
            # sleep 退避時間後再嘗試
            time.sleep(delay)
