import pandas as pd
import numpy as np
import gc
from typing import Annotated
from utils.logger import logger

class Transformer:
    @staticmethod
    def process_market_data(
        df_raw: Annotated[pd.DataFrame, "yfinance 抓取的原始股價資料 (wide format)"]
    ) -> Annotated[pd.DataFrame, "處理後的市場資料 (long format)"]:
        '''
        函式說明：
        市場資料轉換：將 wide format 轉成 long format 並計算報酬率
        '''

        # 記錄輸入規模，方便監控資料量變化與性能
        logger.info(f"Starting transformation. Input shape: {df_raw.shape}")

        # 1. Wide to Long：把日期索引拉成欄位，符合後續資料倉儲 schema
        df = df_raw.reset_index().rename(columns={'Date': 'date', 'index': 'date'})
        df_melted = df.melt(id_vars=['date'], var_name='stock_id', value_name='close')
        
        # 釋放原始巨大的 wide dataframe，避免記憶體暴增
        del df, df_raw
        gc.collect()

        # 2. 型別轉換與優化 (float64 -> float32)： 降低記憶體使用
        df_melted['date'] = pd.to_datetime(df_melted['date'])
        df_melted['close'] = pd.to_numeric(df_melted['close'], errors='coerce').astype(np.float32)
        df_melted['stock_id'] = df_melted['stock_id'].astype(str)
        
        # 3. 排序 (為 FFill 做準備)：確保同股序列時間連續
        df_melted.sort_values(by=['stock_id', 'date'], inplace=True, ignore_index=True)

        # 4. 處理缺失值
        # 使用 groupby ffill：這是最佔記憶體的步驟，需要特別注意
        df_melted['close'] = df_melted.groupby('stock_id', sort=False)['close'].ffill()
        # 若仍為 NaN，代表該股票在該段期間無有效收盤價
        df_melted.dropna(subset=['close'], inplace=True)

        # 5. 計算 Daily Return (同樣使用 float32)
        # 分組後計算日報酬：為後續分析提供基礎指標
        df_melted['daily_return'] = (
            df_melted.groupby('stock_id', sort=False)['close']
            .pct_change()
            .astype(np.float32)
        )

        # 6. 最終整理
        # 保留 fact table 需要的欄位：避免不必要的記憶體占用
        final_df = df_melted[['date', 'stock_id', 'close', 'daily_return']].copy()
        
        # 釋放中間表：降低峰值記憶體使用
        del df_melted
        gc.collect()
        
        # 紀錄轉換後的記憶體用量：便於容量規劃
        logger.info(f"Market Data Transformed. Final Memory Usage: {final_df.memory_usage().sum() / 1024**2:.2f} MB")
        return final_df