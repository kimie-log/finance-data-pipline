"""
FinLab 財報／基本面因子抓取，季頻展開至日頻

供 BigQuery fact_factor 表與回測／選股使用 依賴 FinLab 登入（finlab_login）後
方可呼叫 data.get；因子名稱對應 fundamental_features:*，可用 list_factors_by_type 查詢
季頻資料以交易日序列向前填補（ffill）展開為日頻，產出 long format 以利寫入倉儲 
"""
from __future__ import annotations

from datetime import datetime
from typing import Annotated, List, Tuple

import pandas as pd
from finlab import data


class FinLabFactorFetcher:
    """
    FinLab 財報／基本面因子抓取，季頻展開至日頻 
    get_factor_data / fetch_factors_daily；產出欄位與命名對齊 BigQuery fact_factor 與
    下游 rank_stocks_by_factor 等使用
    """

    @staticmethod
    def extend_factor_data(
        factor_data: Annotated[
            pd.DataFrame,
            "季頻因子表，須含欄位 index（日期）與各股票代碼欄位",
        ],
        trading_days: Annotated[
            pd.DatetimeIndex,
            "要展開到的交易日序列，僅保留此區間內列",
        ],
    ) -> pd.DataFrame:
        """
        將季頻因子表展開至交易日頻率，缺日以向前填補（ffill） 

        實務：用於把財報截止日點位對齊到每個交易日，僅保留 trading_days 區間內列

        Args:
            factor_data: 季頻 DataFrame，index 為日期、欄位為股票代碼 
            trading_days: 目標交易日序列 

        Returns:
            展開後的 DataFrame，index 欄為日期、其餘為股票欄位，區間為 trading_days 的 min～max 
        """
        trading_days_df = pd.DataFrame(trading_days, columns=["index"])
        extended = trading_days_df.merge(factor_data, on="index", how="outer")
        extended = extended.ffill()
        extended = extended[
            (extended["index"] >= trading_days_df["index"].min())
            & (extended["index"] <= trading_days_df["index"].max())
        ]
        return extended

    @staticmethod
    def get_factor_data(
        stock_symbols: Annotated[List[str], "股票代碼列表，空則保留 FinLab 全部欄位"],
        factor_name: Annotated[str, "因子名稱，對應 fundamental_features:*，例：營業利益"],
        trading_days: Annotated[
            pd.DatetimeIndex | None,
            "交易日序列；給定則展開為日頻並回傳 long format",
        ] = None,
    ) -> pd.DataFrame:
        """
        從 FinLab 取得單一因子，可選展開至交易日頻率 

        Args:
            stock_symbols: 要保留的股票代碼；空列表則不篩欄位 
            factor_name: FinLab 因子名，對應 data.get('fundamental_features:{name}').deadline() 
            trading_days: 若給定，則以 ffill 展開為日頻並回傳 (datetime, asset, value) 

        Returns:
            trading_days 為 None 時：原始季頻表（index 為日期、欄位為股票） 
            trading_days 給定時：long format，欄位 datetime, asset, value 
        """
        raw = data.get(f"fundamental_features:{factor_name}").deadline()
        if stock_symbols:
            cols = [c for c in raw.columns if c in stock_symbols]
            raw = raw[cols] if cols else raw
        if trading_days is None:
            return raw
        raw = raw.reset_index().rename(columns={"index": "date"})
        raw["date"] = pd.to_datetime(raw["date"])
        raw_index = raw.rename(columns={"date": "index"})
        extended = FinLabFactorFetcher.extend_factor_data(raw_index, trading_days)
        extended = extended.rename(columns={"index": "datetime"})
        melted = extended.melt(id_vars="datetime", var_name="asset", value_name="value")
        return melted.sort_values(["datetime", "asset"]).reset_index(drop=True)

    @staticmethod
    def fetch_factors_daily(
        stock_ids: Annotated[List[str], "股票代碼列表"],
        factor_names: Annotated[List[str], "因子名稱列表，對應 fundamental_features:*"],
        start_date: Annotated[str, "區間起始日 (YYYY-MM-DD)"],
        end_date: Annotated[str, "區間結束日 (YYYY-MM-DD)"],
        trading_days: Annotated[
            pd.DatetimeIndex,
            "交易日序列，用於展開季頻至日頻並篩選 start_date～end_date",
        ],
    ) -> pd.DataFrame:
        """
        取得多個因子的日頻資料（向前填補），產出 long format 供 BigQuery fact_factor 寫入 

        實務：僅保留 start_date～end_date 內交易日；單一因子取失敗時略過不中斷，回傳其餘因子 

        Args:
            stock_ids: 股票代碼列表 
            factor_names: 因子名稱列表 
            start_date: 區間起始日 
            end_date: 區間結束日 
            trading_days: 交易日序列，會先篩選至 [start_date, end_date] 再展開 

        Returns:
            欄位 date, stock_id, factor_name, value；stock_ids 或 factor_names 為空時回傳空 DataFrame 
        """
        if not factor_names or not stock_ids:
            return pd.DataFrame(columns=["date", "stock_id", "factor_name", "value"])

        trading_days = trading_days[(trading_days >= start_date) & (trading_days <= end_date)]
        rows = []
        for factor_name in factor_names:
            try:
                df = FinLabFactorFetcher.get_factor_data(
                    stock_symbols=stock_ids, factor_name=factor_name, trading_days=trading_days
                )
            except Exception:
                continue
            if df.empty:
                continue
            df = df.rename(columns={"datetime": "date", "asset": "stock_id"})
            df["factor_name"] = factor_name
            df["date"] = pd.to_datetime(df["date"])
            rows.append(df[["date", "stock_id", "factor_name", "value"]])
        if not rows:
            return pd.DataFrame(columns=["date", "stock_id", "factor_name", "value"])
        return pd.concat(rows, ignore_index=True)

    @staticmethod
    def convert_quarter_to_dates(
        quarter: Annotated[str, "年-季度字串，例：2013-Q1"],
    ) -> Tuple[str, str]:
        """
        將季度字串轉為台灣財報揭露區間 (start, end) 日期 

        實務：用於對齊財報截止日與交易日，格式為 YYYY-MM-DD 

        Args:
            quarter: 年-季度，如 2013-Q1 

        Returns:
            (start_date, end_date) 該季財報揭露區間 

        Raises:
            ValueError: quarter 格式無法解析時 
        """
        year, qtr = quarter.split("-")
        if qtr == "Q1":
            return f"{year}-05-16", f"{year}-08-14"
        if qtr == "Q2":
            return f"{year}-08-15", f"{year}-11-14"
        if qtr == "Q3":
            return f"{year}-11-15", f"{int(year) + 1}-03-31"
        if qtr == "Q4":
            return f"{int(year) + 1}-04-01", f"{int(year) + 1}-05-15"
        raise ValueError(f"Invalid quarter: {quarter}")

    @staticmethod
    def convert_date_to_quarter(
        date: Annotated[str, "日期字串 YYYY-MM-DD"],
    ) -> str:
        """
        將日期對應至台灣財報季度字串（依揭露區間邊界判斷） 

        實務：用於查詢某日屬於哪一季財報區間，例：2013-05-16 -> 2013-Q1 

        Args:
            date: YYYY-MM-DD 格式日期 

        Returns:
            季度字串，如 2013-Q1 
        """
        d = datetime.strptime(date, "%Y-%m-%d").date()
        y, m, day = d.year, d.month, d.day
        if m == 5 and day >= 16 or m in (6, 7) or (m == 8 and day <= 14):
            return f"{y}-Q1"
        if m == 8 and day >= 15 or m in (9, 10) or (m == 11 and day <= 14):
            return f"{y}-Q2"
        if m == 11 and day >= 15 or m == 12:
            return f"{y}-Q3"
        if m in (1, 2) or (m == 3 and day <= 31):
            return f"{y - 1}-Q3"
        if m == 4 or (m == 5 and day <= 15):
            return f"{y - 1}-Q4"
        return f"{y}-Q1"

    @staticmethod
    def list_factors_by_type(
        data_type: Annotated[str, "資料型態關鍵字，例：fundamental_features"],
    ) -> List[str]:
        """
        依資料型態列出 FinLab 可用的因子名稱，供設定 factor_names 或除錯用 

        Args:
            data_type: 傳入 data.search(keyword=...) 的關鍵字 

        Returns:
            該型態下因子名稱列表；無結果時回傳空列表 
        """
        try:
            result = data.search(keyword=data_type)
            
            # 處理不同的返回格式
            if not result:
                return []
            
            # 如果 result 是列表
            if isinstance(result, list):
                if len(result) == 0:
                    return []
                # 取第一個元素
                first_result = result[0]
                if isinstance(first_result, dict):
                    # 如果有 items 鍵，返回 items 列表
                    if "items" in first_result:
                        items = first_result["items"]
                        return list(items) if isinstance(items, (list, tuple)) else [items] if items else []
                    # 否則返回字典的所有鍵
                    return list(first_result.keys())
                # 如果第一個元素不是字典，返回整個列表轉為字串列表
                return [str(item) for item in result]
            
            # 如果 result 是字典
            elif isinstance(result, dict):
                if "items" in result:
                    items = result["items"]
                    return list(items) if isinstance(items, (list, tuple)) else [items] if items else []
                return list(result.keys())
            
            # 其他情況
            return []
        except Exception:
            # 查詢失敗時返回空列表
            return []
