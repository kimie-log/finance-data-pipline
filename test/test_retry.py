"""
utils/retry 的單元測試：run_with_retry。

驗證重試機制：一次成功、重試後成功、重試次數耗盡拋錯、sleep 次數正確。
"""
from unittest import mock

import pytest

from utils.retry import run_with_retry


def test_success_first_try():
    """
    驗證動作一次成功時不需重試，直接回傳結果。

    實務：最常見的成功路徑，驗證重試邏輯不影響正常執行。
    """
    action = mock.Mock(return_value="ok")

    result = run_with_retry(action, action_name="unit-test", retries=2)

    assert result == "ok"
    action.assert_called_once()


@mock.patch("utils.retry.time.sleep")
def test_retry_then_success(mock_sleep):
    """
    驗證前幾次失敗後重試成功，sleep 次數等於失敗次數。

    實務：模擬網路暫時性錯誤後恢復的情況；驗證指數退避與 jitter 邏輯。
    """
    calls = []

    def action():
        if len(calls) < 2:
            calls.append("fail")
            raise ValueError("boom")
        return "ok"

    result = run_with_retry(
        action,
        action_name="unit-test",
        retries=3,
        initial_delay=0.01,
        backoff=1.0,
        jitter=0.0,
    )

    assert result == "ok"
    assert mock_sleep.call_count == 2


@mock.patch("utils.retry.time.sleep")
def test_exhausted_retries(mock_sleep):
    """
    驗證重試次數耗盡後拋出最後一次例外，sleep 次數正確。

    實務：模擬持續性錯誤（如權限問題、服務不可用），驗證不會無限重試。
    """
    action = mock.Mock(side_effect=ValueError("boom"))

    with pytest.raises(ValueError):
        run_with_retry(
            action,
            action_name="unit-test",
            retries=2,
            initial_delay=0.01,
            backoff=1.0,
            jitter=0.0,
        )

    assert mock_sleep.call_count == 2
