import pandas as pd

from conftest import require_any_module
from ingestion.base_fetcher import BaseFetcher


def test_save_local_writes_parquet(tmp_path):
    # 確保 parquet engine 可用，避免測試因環境缺失而失敗
    require_any_module(["pyarrow", "fastparquet"], "pip install -r requirements.txt")
    # 準備：建立測試資料與輸出路徑
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    path = tmp_path / "data.parquet"

    # 執行：儲存到本地 parquet
    BaseFetcher().save_local(df, str(path))

    # 驗證：檔案存在且內容一致
    assert path.exists()
    loaded = pd.read_parquet(path)
    pd.testing.assert_frame_equal(loaded, df)
