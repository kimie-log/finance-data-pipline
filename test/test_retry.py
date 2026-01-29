from unittest import mock

import pytest

from utils.retry import run_with_retry


def test_success_first_try():
    # 準備：動作一次成功
    action = mock.Mock(return_value="ok")

    # 執行：不需要重試即可成功
    result = run_with_retry(action, action_name="unit-test", retries=2)

    # 驗證：結果正確且只呼叫一次
    assert result == "ok"
    action.assert_called_once()


@mock.patch("utils.retry.time.sleep")
def test_retry_then_success(mock_sleep):
    # 準備：前兩次失敗，第三次成功
    calls = []

    def action():
        if len(calls) < 2:
            calls.append("fail")
            raise ValueError("boom")
        return "ok"

    # 執行：允許重試三次
    result = run_with_retry(
        action,
        action_name="unit-test",
        retries=3,
        initial_delay=0.01,
        backoff=1.0,
        jitter=0.0,
    )

    # 驗證：成功回傳且睡眠次數等於失敗次數
    assert result == "ok"
    assert mock_sleep.call_count == 2


@mock.patch("utils.retry.time.sleep")
def test_exhausted_retries(mock_sleep):
    # 準備：永遠失敗的動作
    action = mock.Mock(side_effect=ValueError("boom"))

    # 執行/驗證：超過重試次數後應拋出例外
    with pytest.raises(ValueError):
        run_with_retry(
            action,
            action_name="unit-test",
            retries=2,
            initial_delay=0.01,
            backoff=1.0,
            jitter=0.0,
        )

    # 驗證：重試兩次即 sleep 兩次
    assert mock_sleep.call_count == 2
