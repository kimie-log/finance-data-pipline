## Finance Data Pipeline

[![CI](https://github.com/kimie-log/finance-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/kimie-log/finance-data-pipeline/actions/workflows/ci.yml)

一個用來抓取台股資料、清洗轉換後上傳到 BigQuery 的 ETL pipeline
資料來源包含：
- **FinLab**：選出市值前 N 大且符合條件的台股清單
- **yfinance**：下載多檔股票的歷史收盤價
- **Pandas / NumPy**：資料轉換與效能優化
- **Google Cloud Storage / BigQuery**：作為資料湖與資料倉儲

---

### 專案結構 (重點)

- `scripts/run_etl_pipeline.py`：主 ETL 腳本，負責串起整個流程
- `ingestion/`  
  - `finlab_fetcher.py`：封裝 FinLab 登入與取得 Top N 市值股票清單 (`FinLabFetcher.fetch_top_stocks_by_market_value`)
  - `yfinance_fetcher.py`：封裝 yfinance 抓價：單檔 `fetch` 與多檔 `fetch_daily_close_prices`（提供給 pipeline 使用）
  - `base_fetcher.py`：抓取器基底類別
- `processing/transformer.py`：將 wide format 價格資料轉成 long format，計算日報酬
- `utils/`  
  - `google_cloud_storage.py`：GCS 上傳與下載
  - `google_cloud_bigquery.py`：將資料上傳至 BigQuery，支援 upsert (暫存表 + MERGE)
  - `google_cloud_platform.py`：檢查 / 建立 `gcp_keys` 金鑰目錄並確認金鑰存在
  - `logger.py`：實務化 logging 設定（支援 LOG_LEVEL / LOG_DIR，輸出到 console 與輪替檔案）
  - `retry.py`：通用重試工具（指數退避 + jitter）
  - `cli.py`：CLI 參數解析與設定合併工具
- `test/`：pytest 單元測試與測試工具

---

### 環境需求

- Python 版本：**3.10+** 建議
- 作業系統：macOS / Linux / WSL 皆可

安裝相依套件：

```bash
pip install -r requirements.txt
```

---

### 環境變數與設定

專案依賴 `.env` 以及 GCP 金鑰與自訂設定檔。

#### 1. `.env`

在專案根目錄建立 `.env` 檔，可複製 `.env.exemple` 填寫：

```env
# Google Cloud Platform Configuration
GCP_PROJECT_ID=你的_gcp_project_id
GCS_BUCKET=你的_gcs_bucket_name

FINLAB_API_TOKEN=你的_finlab_token
LOG_LEVEL=INFO              # 選填：DEBUG/INFO/WARNING/ERROR/CRITICAL
LOG_DIR=./logs              # 選填：自訂 log 目錄，預設為專案根目錄 logs/
```

`scripts/run_etl_pipeline.py` 會透過 `python-dotenv` 自動載入這些變數。

#### 2. GCP 金鑰 (`gcp_keys/`)

- 在專案根目錄建立 `gcp_keys/` 資料夾 (程式會自動建立，但你也可以手動建立)
- 將 **GCP Service Account JSON 金鑰** 放到 `gcp_keys/` 下，例如：  
  - `gcp_keys/my-gcp-key.json`
- `utils/google_cloud_platform.py` 會：
  - 確保 `gcp_keys/` 存在
  - 在該資料夾下建立 `.gitignore` 並忽略 `*.json`
  - 選擇最後修改時間最新的 JSON 作為使用金鑰

> 注意：根目錄的 `.gitignore` 也會忽略 `gcp_keys/` 與該目錄下的 JSON，避免金鑰被 commit。

#### 3. 設定檔 `config/settings.yaml`

主流程會讀取 `config/settings.yaml`，建議結構如下 (可依需求調整)：

```yaml
top_stocks:
  excluded_industry: []        # 要排除的產業列表
  pre_list_date: "2015-01-01"  # 上市日期需早於此日期
  top_n: 50                    # 市值前 N 大

yfinance:
  start: "2018-01-01"
  end: null                    # 或指定結束日，例如 "2024-12-31"

bigquery:
  dataset: "your_dataset_name"  # 可用 {top_n} / {_top_n} 自動代換
```

---

### 執行 ETL Pipeline

確定以下條件都已完成：

- 已建立 `.env` 並填入 `FINLAB_API_TOKEN`, `GCP_PROJECT_ID`, `GCS_BUCKET`
- 已將 GCP Service Account 金鑰放入 `gcp_keys/`
- 已建立 `config/settings.yaml`
- 已安裝 requirements

執行：

```bash
python scripts/run_etl_pipeline.py
```

可選參數（CLI 介面）：

```bash
# 最新 Top N（預設）
python -m scripts.run_etl_pipeline

# 區間模式（固定市值基準日期）
python -m scripts.run_etl_pipeline --stock-mode interval --market-value-date 2024-01-15 --start 2020-01-01 --end 2024-01-01
```

邏輯說明：
- `latest`：用「**最新市值**」挑 Top N，再抓該清單的歷史價格（適合每日更新）
- `interval`：用「**指定市值日期**」挑 Top N，再抓指定區間價格（可重現回測）

常用參數：
- `--stock-mode`：`latest`（預設）或 `interval`
- `--market-value-date`：區間模式市值基準日期（僅 interval 使用）
- `--start` / `--end`：指定 yfinance 下載區間
- `--top-n`：指定市值前 N 大
- `--excluded-industry`：排除產業（可重複指定）
- `--pre-list-date`：上市日期需早於指定日期
- `--dataset`：覆寫 BigQuery dataset
- `--skip-gcs`：略過上傳 GCS（僅保留本地輸出）

流程包含三個步驟：

1. **Ingestion**
   - 使用 FinLab 取得 Top N 市值股票清單 (`ingestion/finlab_fetcher.py`)
   - 使用 yfinance 抓取這些股票的歷史收盤價 (`ingestion/yfinance_fetcher.py`)
   - 將 raw parquet 檔寫入 `data/raw/{latest|interval}/{YYYY-MM-DD}/`，並上傳到 GCS `data/raw/{latest|interval}/{YYYY-MM-DD}/`
2. **Transformation**
   - 將 wide format 轉為 long format (`processing/transformer.py`)
   - 補齊缺失值、計算 daily return，並輸出 parquet 至 `data/processed/{latest|interval}/{YYYY-MM-DD}/`
   - 上傳 processed 檔到 GCS `data/processed/{latest|interval}/{YYYY-MM-DD}/`
3. **Loading**
   - 依模式分流寫入 BigQuery
     - `latest` → `{dataset}_latest.fact_daily_price_latest`
     - `interval` → `{dataset}_interval.fact_price_mv{market_value_date}_s{start}_e{end}_top{top_n}`

---

### 測試

執行測試（建議在專案虛擬環境中）：

```bash
python -m pytest -q
```

成功時會顯示每個測試檔案的訊息，例如：`測試成功:  test_yfinance_fetcher.py`

---

### CI Pipeline

本專案使用 GitHub Actions 執行 CI：
- 結果頁面：https://github.com/kimie-log/finance-data-pipeline/actions/workflows/ci.yml
- 設定檔：`.github/workflows/ci.yml`
- 觸發條件：`main` 分支 push 與 PR
- 內容：安裝依賴後執行 `python -m pytest -q`
- 支援版本：Python 3.10 / 3.11 / 3.12 / 3.13 / 3.14

---

### 資料結果是否會變動？

是的，結果可能會隨時間變動，原因包含：
- **市值排名會變動**：FinLab 取最新市值，Top N 可能每日不同  
- **資料時間區間會更新**：若 `yfinance.end` 為 `null`，會以當天日期為結束日

若要固定結果，可在 `config/settings.yaml` 固定 `end` 日期，或改用區間模式的固定市值日期

---

### 開發與除錯建議

- 若在 GCS / BigQuery 權限相關步驟遇到 `invalid_grant` 或驗證失敗，可在終端機執行：

  ```bash
  gcloud auth application-default login
  ```

- 可先在互動式環境 (例如 Jupyter / VSCode Notebook) 單獨測試：
  - `FinLabFetcher.fetch_top_stocks_by_market_value`
  - `YFinanceFetcher.fetch_daily_close_prices`
  - `Transformer.process_market_data`
  - `load_to_bigquery`

---
