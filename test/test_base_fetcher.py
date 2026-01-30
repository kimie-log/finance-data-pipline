"""
ingestion/base_fetcher 的單元測試：BaseFetcher.save_local。

驗證 save_local 可正確寫入 parquet 並可讀回，確保 parquet engine 可用性。
"""
import pandas as pd

from conftest import require_any_module
from ingestion.base_fetcher import BaseFetcher


def test_save_local_writes_parquet(tmp_path):
    """
    驗證 BaseFetcher.save_local 可寫入 parquet 且內容一致。

    實務：使用 tmp_path fixture 避免污染測試環境；檢查 parquet engine 可用性。
    """
    require_any_module(["pyarrow", "fastparquet"], "pip install -r requirements.txt")
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    path = tmp_path / "data.parquet"

    BaseFetcher().save_local(df, str(path))

    assert path.exists()
    loaded = pd.read_parquet(path)
    pd.testing.assert_frame_equal(loaded, df)
