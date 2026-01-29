import os
import pandas as pd
from finlab import data, login
import finlab
import pandas as pd
from typing import List, Annotated

'''
封裝 FinLab 資料抓取與登入邏輯，避免在 pipeline 中散落 API 細節
'''


class FinLabFetcher:
    @staticmethod
    def finlab_login():
        token = os.getenv("FINLAB_API_TOKEN")

        # 執行一次登入，讓後續 data.get 可正常存取
        finlab.login(token)


    @staticmethod
    def fetch_top_stocks_by_market_value(
        excluded_industry: Annotated[List[str], "需要排除的特定產業類別列表"] = [],
        pre_list_date: Annotated[str, "上市日期須早於此指定日期"] | None = None,
        top_n: Annotated[int, "市值前 N 大的公司"] | None = None,
        market_value_date: Annotated[str, "市值基準日期 (YYYY-MM-DD)"] | None = None,
    ) -> Annotated[List[str], "符合條件的公司代碼列表"]:
        '''
        函式說明：
        取得市值前 N 大的公司代碼列表，並根據指定條件進行過濾
        '''
        # 取得公司基本資訊，作為篩選的主資料集
        company_info = data.get("company_basic_info")[
            ["stock_id", "公司名稱", "上市日期", "產業類別", "市場別"]
        ]
        
        # 過濾產業：避免不同產業別財報計算不同，導致報酬率計算不準確
        if excluded_industry:
            company_info = company_info[~company_info["產業類別"].isin(excluded_industry)]
        
        # 過濾上市日期 & 市場別 (僅上市公司)，排除較新或非上市公司
        if pre_list_date:
            company_info = company_info[company_info["市場別"] == "sii"]
            company_info = company_info[company_info["上市日期"] < pre_list_date]

        # 取得市值並過濾 Top N：以最新或指定日期市值排名為主
        if top_n:
            # 市值表為時間序列，可指定基準日期以確保可重現性
            df_market_value = data.get("etl:market_value").copy()
            df_market_value.index = pd.to_datetime(df_market_value.index)

            if market_value_date:
                target_date = pd.to_datetime(market_value_date)
                candidate_dates = df_market_value.index[df_market_value.index <= target_date]
                if candidate_dates.empty:
                    raise ValueError(f"No market value data before {market_value_date}")
                selected_date = candidate_dates.max()
            else:
                selected_date = df_market_value.index.max()

            latest_market_value = (
                df_market_value.loc[selected_date]
                .rename("market_value")
                .reset_index()
            )
            latest_market_value.columns = ["stock_id", "market_value"]
            
            # 合併市值與公司資訊，確保資料一致性
            company_info = pd.merge(latest_market_value, company_info, on="stock_id")
            # 依市值排序並截取 Top N
            company_info = company_info.sort_values(by="market_value", ascending=False).head(top_n)

            # 回傳公司代碼列表，作為後續抓價依據
            return company_info.head(top_n)["stock_id"].tolist()
        else:
            # 不需要 top N 時直接回傳篩選後公司代碼
            return company_info["stock_id"].tolist()
    