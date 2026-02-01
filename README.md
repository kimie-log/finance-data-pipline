## Finance Data Pipeline

一個用來抓取台股資料、清洗轉換後上傳到 BigQuery 的 ETL pipeline  
資料來源包含：
- **FinLab**：選出市值前 N 大且符合條件的台股清單（universe）
- **yfinance**：下載多檔股票的歷史 OHLCV 價量資料（使用 `auto_adjust=True` 處理除權息）
- **Pandas / NumPy**：資料轉換與效能優化
- **Google Cloud Storage / BigQuery**：作為資料湖與資料倉儲

---

## 🚀 快速開始（5 分鐘上手）

### 步驟 1：環境設定

```bash
# 1. 安裝依賴
pip install -r requirements.txt

# 2. 設定環境變數（建立 .env 檔案）
cat > .env << EOF
FINLAB_API_TOKEN=你的_finlab_token
GCP_PROJECT_ID=你的_gcp_project_id
GCS_BUCKET=你的_gcs_bucket_name
EOF

# 3. 設定 GCP 金鑰（將 Service Account JSON 放入 gcp_keys/）
mkdir -p gcp_keys
# 將你的 GCP 金鑰檔案放入 gcp_keys/
```

### 步驟 2：執行 ETL Pipeline（產生測試資料）

```bash
# 最有效率的測試參數：小範圍資料，不寫入 GCS
python -m scripts.run_etl_pipeline \
    --market-value-date 2017-05-16 \
    --start 2017-05-16 \
    --end 2021-05-15 \
    --top-n 50 \
    --skip-gcs
```

這會產生：
- ✅ 本地價量檔案：`data/processed/{日期}/fact_price_*.parquet`
- ✅ BigQuery 資料：`tw_top_50_stock_data_s20230101_e20231231_mv20240115.*`

### 步驟 3：執行單因子分析（最快測試方式）

```bash
# 使用本地檔案 + FinLab API（最有效率，無需 BigQuery 因子資料）
python -m scripts.run_single_factor_analysis \
    --dataset tw_top_50_stock_data_s20230101_e20231231_mv20240115 \
    --factor 營業利益 \
    --start 2023-01-01 \
    --end 2023-12-31 \
    --auto-find-local \
    --from-finlab-api
```

**說明**：
- `--auto-find-local`：自動尋找本地價量檔案
- `--from-finlab-api`：從 FinLab API 直接抓取因子資料（最快，無需 BigQuery）

### 步驟 4：查看結果

```bash
# 查看報表
ls -la data/single_factor_analysis_reports/

# 開啟 PDF 報表（macOS）
open data/single_factor_analysis_reports/營業利益_s2023-01-01_e2023-12-31_*/alphalens_*.pdf
```

### 📋 快速測試檢查清單

- [ ] `.env` 檔案已設定（`FINLAB_API_TOKEN`、`GCP_PROJECT_ID`）
- [ ] `gcp_keys/` 中有 GCP Service Account JSON
- [ ] 已執行 ETL pipeline 產生價量資料
- [ ] 已執行單因子分析並產生報表

### 💡 測試其他因子

```bash
# 查看可用因子
python -m factors.list_factors

# 測試不同因子（替換 --factor 參數）
python -m scripts.run_single_factor_analysis \
    --dataset tw_top_50_stock_data_s20230101_e20231231_mv20240115 \
    --factor ROE稅後 \
    --start 2023-01-01 \
    --end 2023-12-31 \
    --auto-find-local \
    --from-finlab-api
```

---

### 目前專案可以做到的事

#### 一、主流程：可重現回測用的 ETL（僅 interval 模式）

以**固定市值日 + 區間**為核心，從抓資料到進 BigQuery 一條龍，產出可供回測與分析使用的資料。

| 階段 | 可做到的事 |
|------|------------|
| **Ingestion** | 用 FinLab 依**指定市值日**篩選 Top N 股票（可排除產業、上市日期），取得 **universe**（含 `delist_date` 若 FinLab 有）；用 yfinance 抓這些股票的 **OHLCV**（開高低收量），並處理除權息（`auto_adjust=True`）。 |
| **Transformation** | 對 OHLCV 做清洗、補值、計算 **daily_return**，並標記 **is_suspended / is_limit_up / is_limit_down**（交易可行性）。 |
| **Loading** | 將 raw / processed parquet 寫到本地 `data/raw/{date}/`、`data/processed/{date}/`，可選上傳 **GCS**；再寫入 **BigQuery**（upsert 價量表、truncate 維度與輔助表）。 |

#### 二、BigQuery 產出內容

| 表／用途 | 說明 |
|----------|------|
| **fact_price** | 價量事實表：date、stock_id、open/high/low/close、volume、daily_return、is_suspended、is_limit_up、is_limit_down。Dataset: `{base}_s{start}_e{end}_mv{date}`。 |
| **dim_universe** | 維度表：該市值日的 Top N 股票清單（含 delist_date 等）。Dataset: `{base}_s{start}_e{end}_mv{date}`。 |
| **dim_calendar** | 交易日曆（由價量日期產生，供回測對齊）。Dataset: `{base}_s{start}_e{end}_mv{date}`。 |
| **fact_benchmark_daily** | 基準指數（預設加權 ^TWII）日收盤與日報酬。Dataset: `{base}_s{start}_e{end}_mv{date}`。 |
| **dim_backtest_config** | 回測預設參數（手續費、證交稅等）。Dataset: `{base}_s{start}_e{end}_mv{date}`。 |
| **fact_factor** 或 **fact_factor_{suffix}**（可選） | 財報／基本面因子日頻資料（需 `--with-factors` 且設定 `factors.factor_names`）。Dataset: `{base}_s{start}_e{end}_mv{date}`。 |

#### 三、資料輸入輸出與回測／因子分析對照

專案採「固定市值日 + 區間」設計，產出的輸入／輸出與檔案**已符合**一般回測與因子分析所需；對照如下。

**輸入**

| 項目 | 來源 | 說明 |
|------|------|------|
| 市值日 + 區間 | CLI / 設定檔 | `--market-value-date`、`--start`、`--end` 固定選股與價量區間，避免前視偏差。 |
| 選股條件 | `config/settings.yaml`、CLI | `top_n`、`excluded_industry`、`pre_list_date`。 |
| 價量 | yfinance | OHLCV，`auto_adjust=True` 處理除權息。 |
| Universe | FinLab | 指定市值日的 Top N 清單（含 `delist_date` 若 FinLab 有）。 |
| 因子（可選） | FinLab | `factors.factor_names` + `--with-factors` 寫入 BigQuery。 |
| 基準指數 | yfinance | `benchmark.index_ids`（如 ^TWII）。 |
| 回測參數 | 設定檔 | `backtest_config`（手續費、證交稅等）。 |

**產出檔案與用途**

| 產出 | 位置／表名 | 回測用 | 因子分析用 |
|------|------------|--------|------------|
| 價量事實表 | `fact_price`（dataset: `{base}_s{start}_e{end}_mv{date}`） | ✓ 報酬、OHLCV、交易可行性 | ✓ 報酬／價量 |
| Universe 維度表 | `dim_universe`（dataset: `{base}_s{start}_e{end}_mv{date}`） | ✓ 標的清單、delist_date | ✓ 標的範圍 |
| 交易日曆 | `dim_calendar` | ✓ 對齊交易日 | ✓ 對齊日期 |
| 基準指數 | `fact_benchmark_daily` | ✓ 績效比較 | — |
| 回測參數 | `dim_backtest_config` | ✓ 手續費／稅 | — |
| 因子表（可選） | `fact_factor` 或 `fact_factor_{suffix}`（dataset: `{base}_s{start}_e{end}_mv{date}`） | ✓ 選股／加權 | ✓ 因子值、排名 |
| Raw / Processed Parquet | `data/raw/{date}/`、`data/processed/{date}/` | 備援、重跑 | 備援、重跑 |

**回測所需對照**

- 價量與日報酬：`fact_price`（date, stock_id, open/high/low/close, volume, daily_return），位於 dataset `{base}_s{start}_e{end}_mv{date}`。
- 交易可行性：`fact_price`（is_suspended, is_limit_up, is_limit_down）。
- 標的清單與下市日：`dim_universe`（stock_id, delist_date 等），位於 dataset `{base}_s{start}_e{end}_mv{date}`。
- 交易日對齊：`dim_calendar`（date, is_trading_day）。
- 基準與成本：`fact_benchmark_daily`、`dim_backtest_config`。
- 可重現性：固定市值日 + 區間，同一組參數產出一致。

**因子分析所需對照**

- 因子值：`fact_factor` 或 `fact_factor_{suffix}`（date, stock_id, factor_name, value），位於 dataset `{base}_s{start}_e{end}_mv{date}`；或程式內 `FinLabFactorFetcher.get_factor_data` / `FinLabFactorFetcher.fetch_factors_daily`。
- 價量／報酬：同上 `fact_price`（位於相同 dataset）。
- 單因子／多因子排名：程式內 `FactorRanking.rank_stocks_by_factor`、`FactorRanking.calculate_weighted_rank`（見下方「因子相關」）。

**小結**：目前資料輸入、輸出與產生的檔案足以支援回測與因子分析。專案已實作三項優化：（1）**滾動回測**：可用 `--market-value-dates 2024-01-15,2024-02-15,...` 一次跑多個市值日 ETL；（2）**本地檔名**：raw／processed parquet 檔名含 `mv{日期}_top{n}`，與 BigQuery 表名對應；（3）**因子表並存**：同一組 (mv, start, end, top_n) 可透過 `--factor-table-suffix`（或設定檔 `factors.factor_table_suffix`）並存多組因子表。

#### 四、因子相關（供回測／選股用）

- **抓因子**：從 FinLab 取 `fundamental_features:{因子名}`，用 `.deadline()` 轉成財報截止日。
- **季頻→日頻**：用交易日序列 merge + 向前填補（ffill），展開成每日一筆，再 melt 成 long（date, stock_id, factor_name, value）。
- **季度對齊**：`FinLabFactorFetcher.convert_quarter_to_dates` / `FinLabFactorFetcher.convert_date_to_quarter` 對齊台灣財報揭露區間。
- **單因子排名**：`FactorRanking.rank_stocks_by_factor`（每日依因子值排名，正／負相關可選）。
- **多因子加權排名**：`FactorRanking.calculate_weighted_rank`（多個已排名表 × 權重加總後再排名）。
- **查因子清單**：`FinLabFactorFetcher.list_factors_by_type("fundamental_features")` 列出可用的財報因子名稱。


#### 五、CLI 與設定可控制的事

- **必填**：`--market-value-date` 或 `--market-value-dates`、`--start`、`--end`（固定回測區間與選股基準）。
- **選股**：`--top-n`、`--excluded-industry`、`--pre-list-date`。
- **輸出**：`--dataset`（BigQuery dataset）、`--skip-gcs`（只留本地）、`--with-factors`（一併寫入因子表）。
- **可略過**：`--skip-benchmark`、`--skip-calendar`。

設定檔 `config/settings.yaml` 可預設：top_stocks、yfinance 區間、bigquery dataset、factors.factor_names、benchmark index_ids、backtest_config 等。

#### 六、其他能力

- **GCP**：檢查/建立 `gcp_keys/`、選最新金鑰；上傳 GCS、寫入 BigQuery（含 upsert 邏輯）。
- **日誌**：logger 支援 LOG_LEVEL / LOG_DIR，輸出到 console 與輪替檔案。
- **重試**：`utils/retry.py` 通用重試（指數退避 + jitter）。
- **測試**：pytest 涵蓋 FinLab、yfinance、transformer、CLI、GCS、BigQuery、retry 等。

**一句話**：專案可依「固定市值日 + 區間」跑完整 ETL，產出可重現回測用的價量、universe、交易日曆、基準、回測參數，並可選寫入財報因子；程式內可做因子取得、季頻→日頻展開、單／多因子加權排名；透過 CLI 與設定檔控制選股與輸出，並以 GCS + BigQuery 為資料湖與倉儲，搭配測試維持品質。

---

### 專案結構 (重點)

- `scripts/run_etl_pipeline.py`：主 ETL 腳本，負責串起整個流程（FinLab universe + yfinance OHLCV + BigQuery）
- `ingestion/`  
  - `finlab_fetcher.py`：FinLab 登入與 Top N 市值 **universe**（含 `delist_date` 若 FinLab 有提供）
  - `yfinance_fetcher.py`：OHLCV 抓價 `fetch_daily_ohlcv_data`、基準指數 `fetch_benchmark_daily`
  - `base_fetcher.py`：抓取器基底類別
- `factors/`
  - `finlab_factor_fetcher.py`：`FinLabFactorFetcher` 財報／基本面因子抓取並展開至日頻（供 `--with-factors` 使用）
  - `list_factors.py`：列出可用因子工具
  - `factor_ranking.py`：因子排名與加權排名工具
- `processing/transformer.py`：OHLCV 清洗、日報酬、交易可行性標記（`is_suspended` / `is_limit_up` / `is_limit_down`）
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
  dataset: "tw_top_{_top_n}_stock_data"  # 可用 {top_n} / {_top_n} 自動代換；最終 dataset 為 {base}_s{start}_e{end}_mv{date}

factors:
  factor_names: []             # 例: ["營業利益", "營業收入"]，搭配 --with-factors 落地 BigQuery
  factor_table_suffix: null    # 選填，因子表名後綴，同一組可並存多組 (例: value, momentum)

benchmark:
  index_ids: ["^TWII"]         # 基準指數，可加 "^TWOII" 櫃買

backtest_config:               # 回測層預設（手續費／稅），寫入 dim_backtest_config
  fee_bps: 30
  tax_bps: 10
```

---

### 執行 ETL Pipeline

#### 前置準備

確定以下條件都已完成：

- ✅ 已建立 `.env` 並填入 `FINLAB_API_TOKEN`、`GCP_PROJECT_ID`、`GCS_BUCKET`
- ✅ 已將 GCP Service Account 金鑰放入 `gcp_keys/`
- ✅ 已安裝 requirements：`pip install -r requirements.txt`

#### 最有效率的測試命令

```bash
# 小範圍測試（2023 年，50 檔股票，不寫入 GCS）
python -m scripts.run_etl_pipeline \
    --market-value-date 2024-01-15 \
    --start 2023-01-01 \
    --end 2023-12-31 \
    --top-n 50 \
    --skip-gcs
```

**說明**：
- `--skip-gcs`：不上傳到 GCS，只保留本地檔案（測試時更快）
- 小日期範圍（1 年）可加快執行速度
- 會產生本地檔案：`data/processed/{日期}/fact_price_*.parquet`

#### 完整執行（包含因子資料）

```bash
# 包含因子資料，寫入 BigQuery
python -m scripts.run_etl_pipeline \
    --market-value-date 2024-01-15 \
    --start 2020-01-01 \
    --end 2024-01-01 \
    --top-n 50 \
    --with-factors \
    --skip-gcs
```

**注意**：需要先在 `config/settings.yaml` 設定 `factors.factor_names`，例如：
```yaml
factors:
    factor_names: ["營業利益", "ROE稅後"]
```

#### 常用參數說明

邏輯說明：
- 使用「**指定市值日期**」挑 Top N，再抓指定區間價格，確保回測可重現、減少生存者偏誤。

常用參數：
- `--market-value-date`：單一市值基準日期（與 `--market-value-dates` 二擇一）
- `--market-value-dates`：多個市值日，逗號分隔（例：`2024-01-15,2024-02-15`），一次跑多期 ETL 供滾動回測
- `--start` / `--end`：指定 yfinance 下載區間（必填）
- `--top-n`：指定市值前 N 大
- `--excluded-industry`：排除產業（可重複指定）
- `--pre-list-date`：上市日期需早於指定日期
- `--dataset`：覆寫 BigQuery dataset
- `--skip-gcs`：略過上傳 GCS（僅保留本地輸出）
- `--with-factors`：一併抓取財報因子並寫入 `fact_factor`（可加 `--factor-table-suffix` 並存多組）
- `--factor-table-suffix`：因子表名後綴，同一組 (mv, start, end, top_n) 可並存多組因子（例：`value`、`momentum`）
- `--skip-benchmark`：略過基準指數寫入
- `--skip-calendar`：略過交易日曆寫入

流程包含三個步驟：

1. **Ingestion**
   - 使用 FinLab 取得 Top N 市值股票 **universe** (`FinLabFetcher.fetch_top_stocks_universe`)
   - 使用 yfinance 抓取這些股票的歷史 OHLCV 價量資料 (`YFinanceFetcher.fetch_daily_ohlcv_data`)
   - 將 raw parquet 寫入 `data/raw/{YYYY-MM-DD}/`，檔名含 `mv{日期}_top{n}` 便於與 BigQuery 表名對應（例：`mv20240115_top50_ohlcv_raw_2020-01-01_to_2024-01-01_*.parquet`），並可上傳 GCS
2. **Transformation**
   - 使用 `Transformer.process_ohlcv_data` 清洗 OHLCV long format
   - 補齊缺失值、計算 `daily_return`，輸出 parquet 至 `data/processed/{YYYY-MM-DD}/`（檔名含 `mv{日期}_top{n}`，例：`fact_price_ohlcv_mv20240115_top50_2020-01-01_to_2024-01-01_*.parquet`）
   - 上傳 processed 檔到 GCS `data/processed/{YYYY-MM-DD}/`
3. **Loading**
   - 寫入 BigQuery（使用 upsert，避免重複列）
     - Dataset: `{base_dataset}_s{start}_e{end}_mv{market_value_date}`（參數移至 dataset 名稱）
     - **價量事實表**：`fact_price`（upsert）
   - 同時寫入：
     - **universe**（含 `delist_date` 若 FinLab 有）：`dim_universe`（truncate）
     - **交易日曆**（由價量日期產生）：`dim_calendar`（除非 `--skip-calendar`）
     - **基準指數**（加權等）：`fact_benchmark_daily`（除非 `--skip-benchmark`）
     - **回測層預設參數**（手續費／稅）：`dim_backtest_config`（由 `config/settings.yaml` 的 `backtest_config`）
     - **財報因子**（可選）：`fact_factor` 或 `fact_factor_{suffix}`（需 `--with-factors` 且設定 `factors.factor_names`）

---

### 單因子分析（Alphalens）

#### ⚡ 快速測試（推薦 - 最有效率）

**使用本地價量檔案 + FinLab API 直接抓取因子**（無需 BigQuery 因子資料，最快）：

```bash
python -m scripts.run_single_factor_analysis \
    --dataset tw_top_50_stock_data_s20230101_e20231231_mv20240115 \
    --factor 營業利益 \
    --start 2023-01-01 \
    --end 2023-12-31 \
    --auto-find-local \
    --from-finlab-api
```

**為什麼最有效率？**
- ✅ `--auto-find-local`：自動尋找本地價量檔案，無需手動指定路徑
- ✅ `--from-finlab-api`：直接從 FinLab API 抓取因子，無需等待 BigQuery 查詢或本地因子檔案
- ✅ 適合快速測試和迭代

**參數說明**：
- `--dataset`：BigQuery Dataset ID（用於識別資料集，實際價量資料從本地讀取）
- `--factor`：因子名稱（使用 `python -m factors.list_factors` 查看可用因子）
- `--start` / `--end`：分析日期範圍（需與 ETL 產生的資料範圍一致）
- `--auto-find-local`：自動尋找本地價量檔案（在 `data/processed/` 中搜尋）
- `--from-finlab-api`：從 FinLab API 直接抓取因子資料（無需 BigQuery 或本地因子檔案）
- `--quantiles`：分位數數量（預設 5，可選：`--quantiles 10`）
- `--periods`：前瞻期間（預設 1,5,10，可選：`--periods 1,5,10,20`）

**報表輸出**：
- 📁 位置：`data/single_factor_analysis_reports/{因子名稱}_s{開始日期}_e{結束日期}_{時間戳}/`
- 📄 格式：PDF（完整報表）+ PNG（個別圖表）
- 🔍 查看：`open data/single_factor_analysis_reports/營業利益_s2023-01-01_e2023-12-31_*/alphalens_*.pdf`

#### 📊 從 BigQuery 讀取（如果已有因子資料）

如果 ETL 時已使用 `--with-factors` 將因子資料寫入 BigQuery：

```bash
python -m scripts.run_single_factor_analysis \
    --dataset tw_top_50_stock_data_s20230101_e20231231_mv20240115 \
    --factor 營業利益 \
    --start 2023-01-01 \
    --end 2023-12-31 \
    --auto-find-local
```

**注意**：此方式需要 BigQuery 中有 `fact_factor` 表，否則會報錯。建議使用 `--from-finlab-api` 更快速。

#### 📝 手動指定檔案路徑

如果需要明確指定檔案路徑：

```bash
python -m scripts.run_single_factor_analysis \
    --dataset tw_top_50_stock_data_s20230101_e20231231_mv20240115 \
    --factor 營業利益 \
    --start 2023-01-01 \
    --end 2023-12-31 \
    --local-price data/processed/2026-01-30/fact_price_*.parquet \
    --from-finlab-api
```

---

### 測試與更新資料

#### 1. 如何執行單元測試？

執行測試（建議在專案虛擬環境中）：

```bash
python -m pytest -q
```

成功時會顯示每個測試檔案的訊息，例如：`測試成功:  test_yfinance_fetcher.py`

主要測試涵蓋：
- `test_finlab_fetcher.py`：FinLab 登入與 `fetch_top_stocks_universe`
- `test_finlab_factor_fetcher.py`：`FinLabFactorFetcher` 各 staticmethod（`extend_factor_data`、`get_factor_data`、`fetch_factors_daily`、`convert_quarter_to_dates`、`convert_date_to_quarter`、`list_factors_by_type`）
- `test_yfinance_fetcher.py`：`fetch_daily_ohlcv_data`、`fetch_benchmark_daily` 的欄位與資料結構
- `test_transformer.py`：`process_ohlcv_data` 的清洗與日報酬計算
- `test_factor_ranking.py`：`FactorRanking.rank_stocks_by_factor`、`calculate_weighted_rank`（皆為 staticmethod）
- `test_cli.py`：`parse_args`、`resolve_params`
- `test_run_etl_pipeline_cli.py`：CLI 參數解析、檔名與 BigQuery 命名規則、GCS 路徑
- `test_base_fetcher.py`：`BaseFetcher.save_local`
- `test_google_cloud_bigquery.py`：`load_to_bigquery`
- `test_google_cloud_platform.py`：`check_gcp_environment`
- `test_google_cloud_storage.py`：`upload_file`、`download_file`
- `test_retry.py`：`run_with_retry`

**測試涵蓋對照（模組 → 函式 → 測試檔）**

| 模組 | 函式 | 測試檔 |
|------|------|--------|
| `ingestion/finlab_fetcher.py` | `finlab_login`, `fetch_top_stocks_universe` | `test_finlab_fetcher.py` |
| `factors/finlab_factor_fetcher.py` | `FinLabFactorFetcher.extend_factor_data`, `get_factor_data`, `fetch_factors_daily`, `convert_quarter_to_dates`, `convert_date_to_quarter`, `list_factors_by_type`（皆為 staticmethod） | `test_finlab_factor_fetcher.py` |
| `ingestion/yfinance_fetcher.py` | `fetch_daily_ohlcv_data`, `fetch_benchmark_daily` | `test_yfinance_fetcher.py` |
| `ingestion/base_fetcher.py` | `save_local`（`fetch` 為抽象方法） | `test_base_fetcher.py` |
| `processing/transformer.py` | `process_ohlcv_data` | `test_transformer.py` |
| `factors/factor_ranking.py` | `FactorRanking.rank_stocks_by_factor`, `calculate_weighted_rank`（皆為 staticmethod） | `test_factor_ranking.py` |
| `utils/cli.py` | `parse_args`, `resolve_params`, `load_config` | `test_cli.py` |
| `utils/google_cloud_bigquery.py` | `load_to_bigquery` | `test_google_cloud_bigquery.py` |
| `utils/google_cloud_platform.py` | `check_gcp_environment` | `test_google_cloud_platform.py` |
| `utils/google_cloud_storage.py` | `upload_file`, `download_file` | `test_google_cloud_storage.py` |
| `utils/retry.py` | `run_with_retry` | `test_retry.py` |
| `scripts/run_etl_pipeline.py` | `main`（整合流程） | `test_run_etl_pipeline_cli.py` |

> `utils/logger.py`（`configure_logger` 等）多為設定用，通常不另寫單元測試。

#### 2. 如何更新一批新的資料到 BigQuery？

產生可重現回測用的區間資料：

```bash
python -m scripts.run_etl_pipeline \
  --market-value-date 2024-01-15 \
  --start 2020-01-01 \
  --end 2024-01-01 \
  --top-n 50
```

- 會以 2024-01-15 的市值排名產生固定 universe，寫入  
  `{base_dataset}_s20200101_e20240101_mv20240115.dim_universe`
- 同一批股票在 2020-01-01 ~ 2024-01-01 的 OHLCV + `daily_return`，寫入  
  `{base_dataset}_s20200101_e20240101_mv20240115.fact_price`

> **建議**：因子分析 / 回測時，使用 `fact_price` + `dim_universe`（位於相同 dataset `{base}_s{start}_e{end}_mv{date}`），可減少生存者偏誤並確保結果可重現。不同參數組合會有不同 dataset，避免資料覆蓋。

---

### 資料結果是否會變動？

是的，結果可能會隨時間變動，原因包含：
- **市值排名會變動**：FinLab 取最新市值，Top N 可能每日不同  
- **資料時間區間會更新**：若 `yfinance.end` 為 `null`，會以當天日期為結束日

若要固定結果，請使用 `--market-value-date` 與 `--start` / `--end` 固定市值日與區間，並在設定檔中固定 `end` 日期

---

### 開發與除錯建議

- 若在 GCS / BigQuery 權限相關步驟遇到 `invalid_grant` 或驗證失敗，可在終端機執行：

  ```bash
  gcloud auth application-default login
  ```

- 可先在互動式環境 (例如 Jupyter / VSCode Notebook) 單獨測試：
  - `FinLabFetcher.fetch_top_stocks_universe`
  - `YFinanceFetcher.fetch_daily_ohlcv_data`、`YFinanceFetcher.fetch_benchmark_daily`
  - `Transformer.process_ohlcv_data`
  - `load_to_bigquery`

---
