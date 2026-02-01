"""
processing/factor_ranking 的單元測試：FactorRanking.rank_stocks_by_factor、calculate_weighted_rank。

驗證單因子排名（正／負相關）、每日分別排名、多因子加權排名、參數驗證。
"""
import pandas as pd
import pytest

from factors.factor_ranking import FactorRanking


def test_rank_stocks_by_factor_positive_corr():
    """
    驗證正相關時小值排前（ascending=True），用於因子值越大越好的情況。

    實務：如「營業利益」越大越好，排名時小值（差）排前、大值（好）排後。
    """
    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
        "asset": ["A", "B", "C"],
        "value": [30.0, 10.0, 20.0],
    })
    result = FactorRanking.rank_stocks_by_factor(
        factor_df=df,
        positive_corr=True,
        rank_column="value",
        rank_result_column="rank",
    )
    assert "rank" in result.columns
    r = result.set_index("asset")["rank"]
    assert r["B"] == 1.0 and r["C"] == 2.0 and r["A"] == 3.0


def test_rank_stocks_by_factor_negative_corr():
    """
    驗證負相關時大值排前（ascending=False），用於因子值越小越好的情況。

    實務：如「本益比」越小越好，排名時大值（差）排前、小值（好）排後。
    """
    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
        "asset": ["A", "B", "C"],
        "value": [30.0, 10.0, 20.0],
    })
    result = FactorRanking.rank_stocks_by_factor(
        factor_df=df,
        positive_corr=False,
        rank_column="value",
        rank_result_column="rank",
    )
    r = result.set_index("asset")["rank"]
    assert r["A"] == 1.0 and r["C"] == 2.0 and r["B"] == 3.0


def test_rank_stocks_by_factor_per_date():
    """
    驗證每日分別排名，不同日期的排名互不影響。

    實務：因子值每日變動，需每日重新排名；此測試確保 groupby("datetime") 邏輯正確。
    """
    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"]),
        "asset": ["A", "B", "A", "B"],
        "value": [1.0, 2.0, 2.0, 1.0],
    })
    result = FactorRanking.rank_stocks_by_factor(
        factor_df=df,
        positive_corr=True,
        rank_column="value",
        rank_result_column="rank",
    )
    d1 = result[result["datetime"] == "2024-01-01"].set_index("asset")["rank"]
    d2 = result[result["datetime"] == "2024-01-02"].set_index("asset")["rank"]
    assert d1["A"] == 1.0 and d1["B"] == 2.0
    assert d2["B"] == 1.0 and d2["A"] == 2.0


def test_calculate_weighted_rank_length_mismatch():
    """
    驗證 ranked_dfs 與 weights 長度不同時拋出 ValueError。

    實務：多因子加權需一一對應，長度不一致會導致權重分配錯誤，應明確提示。
    """
    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01"]),
        "asset": ["A"],
        "rank": [1.0],
    })
    with pytest.raises(ValueError, match="長度須相同"):
        FactorRanking.calculate_weighted_rank(
            ranked_dfs=[df],
            weights=[0.5, 0.5],
            positive_corr=True,
            rank_column="rank",
        )


def test_calculate_weighted_rank_single_factor():
    """
    驗證單一因子加權排名等同該因子排名（權重為 1.0 時）。

    實務：邊界情況測試，確保單因子與多因子邏輯一致。
    """
    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01"]),
        "asset": ["A", "B"],
        "rank": [1.0, 2.0],
    })
    result = FactorRanking.calculate_weighted_rank(
        ranked_dfs=[df],
        weights=[1.0],
        positive_corr=True,
        rank_column="rank",
    )
    assert set(result.columns) == {"datetime", "asset", "weighted_rank"}
    r = result.set_index("asset")["weighted_rank"]
    assert r["A"] == 1.0 and r["B"] == 2.0


def test_calculate_weighted_rank_two_factors():
    """
    驗證兩因子加權加總後再排名，產出 weighted_rank 欄位。

    實務：多因子選股常用場景；加權總分計算後再排名，確保排名反映綜合因子表現。
    """
    df1 = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
        "asset": ["A", "B", "C"],
        "rank": [1.0, 2.0, 3.0],
    })
    df2 = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
        "asset": ["A", "B", "C"],
        "rank": [3.0, 2.0, 1.0],
    })
    result = FactorRanking.calculate_weighted_rank(
        ranked_dfs=[df1, df2],
        weights=[0.5, 0.5],
        positive_corr=True,
        rank_column="rank",
    )
    assert set(result.columns) == {"datetime", "asset", "weighted_rank"}
    assert result["weighted_rank"].min() >= 1.0 and result["weighted_rank"].max() <= 3.0
