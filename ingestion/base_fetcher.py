import pandas as pd

'''
抽象化抓取介面，讓不同資料來源共用相同流程
'''

class BaseFetcher:
    source: str

    def fetch(self) -> pd.DataFrame:
        # 由子類實作實際抓取邏輯，避免在基底類別中耦合外部 API
        raise NotImplementedError

    def save_local(self, df: pd.DataFrame, path: str):
        # 統一使用 Parquet 落地，方便後續分析與節省空間
        df.to_parquet(path, index=False)
        
        # 保留 local data 訊息，便於追蹤 pipeline 產出
        print(f"Data saved locally at {path}")