# stock_picker

## 1. 项目简介
`stock_picker` 是一个一次性运行的选股管线骨架：

1. 从不同 Broker/交易软件 API 读取股票数据（当前为 stub）
2. 统一数据格式（bars / quotes / universe / features / candidates）
3. 进行特征工程与可选模型（Prophet 可选，未安装时自动降级）
4. 执行规则筛选 + 打分排序
5. 输出报告与可追溯产物

当前版本目标是先搭好工程结构、接口契约和产物目录，后续逐步接入 Futu / IBKR 的真实数据能力。
运行环境要求：Python 3.11+。

## 2. Design Principles

- Data layer 不做策略决策；Strategy layer 只读 processed 数据。
- 多数据源适配器（`BrokerConnector`），支持能力探测（`capabilities_check`）与降级（permission denied / 延迟数据）。
- 统一 schema：`bars` / `quotes` / `universe` / `features` / `candidates`；并包含 `quality_flags`。
- 缓存与可追溯：`raw` / `processed` / `run artifacts` 三层产物。
- 频控与重试：请求队列 + backoff（本次仅写设计原则，暂未实现真实限流调度）。

## 3. Quickstart

### 3.1 环境准备

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

可选（推荐）安装为 editable 包，后续可直接 `python -m stock_picker.cli.run`：

```bash
pip install -e .
```

### 3.2 运行示例

先 dry-run（只展示执行计划，不落地产物）：

```bash
PYTHONPATH=src python -m stock_picker.cli.run \
  --config config.example.yaml \
  --watchlist data/input/watchlist.csv \
  --broker futu \
  --out outputs \
  --dry-run
```

非 dry-run（会生成 `outputs/run_.../`）：

```bash
PYTHONPATH=src python -m stock_picker.cli.run \
  --config config.example.yaml \
  --watchlist data/input/watchlist.csv \
  --broker futu \
  --out outputs
```

## 4. CLI 参数说明

命令入口：`python -m stock_picker.cli.run`

- `--config <path>`：配置文件路径，默认 `config.yaml`（不存在时会提示使用 `config.example.yaml`）。
- `--watchlist <path>`：覆盖配置中的 `universe.watchlist_path`。
- `--rules <path>`：覆盖配置中的 `universe.rules_path`。
- `--broker futu|ibkr_tws|ibkr_cp`：可多次传入，启用多个数据源。
- `--start-date YYYY-MM-DD`：覆盖 `run.start_date`。
- `--end-date YYYY-MM-DD`：覆盖 `run.end_date`。
- `--out <dir>`：输出根目录，默认 `outputs`。
- `--force-refresh`：忽略缓存，强制重新抓取（stub fetch）。
- `--dry-run`：仅打印执行计划，不写文件。
- `--log-level INFO|DEBUG|WARNING|ERROR`：日志级别。

## 5. 配置文件说明（`config.yaml`）

可直接复制 `config.example.yaml` 为 `config.yaml`。

完整模板：

```yaml
run:
  start_date: 2025-01-01
  end_date: 2025-12-31
  timezone: Asia/Shanghai
  out_dir: outputs
  cache_dir: data/cache

brokers:
  futu:
    host: 127.0.0.1
    port: 11111
    unlock_trade_password: ""
  ibkr_tws:
    host: 127.0.0.1
    port: 7497
    client_id: 101
    read_only: true
  ibkr_cp:
    base_url: https://api.ibkr.com/v1/api
    account_id: U1234567
    api_key: "<YOUR_IBKR_CP_API_KEY>"

universe:
  watchlist_path: data/input/watchlist.csv
  rules_path: data/input/universe_rules.yaml
  prefer_watchlist: true  # 两者并存时优先 watchlist

data:
  timeframe: 1D
  adjustment: forward
  max_missing_ratio: 0.1
  rate_limit:
    requests_per_second: 5
    backoff_seconds: [1, 2, 5]

models:
  prophet:
    enable: false
    params:
      seasonality_mode: multiplicative
      changepoint_prior_scale: 0.05

selection:
  hard_filters:
    market_whitelist: [US, HK, EU, CN]
    min_close: 5
    min_volume: 100000
    max_volatility_3: 0.2
    min_forecast_return_5d: -0.05
  score_weights:
    forecast_return_5d: 0.5
    ret_1d: 0.2
    volume: 0.2
    volatility_3: 0.1
  constraints:
    max_per_market:
      US: 5
      HK: 3
      EU: 2
      CN: 3
    currency_allowlist: [USD, HKD, EUR, CNY]
```

## 6. 输出产物

非 dry-run 时会生成：`outputs/run_YYYYMMDD_HHMM/`

目录下包含：

- `run_config.json`：本次运行的最终配置（含 CLI 覆盖结果）。
- `env.json`：运行环境信息（Python、平台、时间戳等）。
- `diagnostics.json`：诊断信息（capabilities、fetch 状态、quality、model 降级等）。
- `candidates.csv`：候选股票表（可为 stub/demo）。
- `portfolio_candidates.md`：Markdown 报告（run summary + top candidates + diagnostics）。

缓存约定：

- `data/cache/raw/{broker}/*.json`
- `data/cache/processed/bars.parquet`

## 7. Roadmap

1. 跑通 stub 管线（当前阶段）
2. 实现 Futu 真实数据连接
3. 实现 IBKR（TWS + CP）真实数据连接
4. 接入 Prophet 真实建模与回测评估
5. 增加更多因子与多格式报告（HTML/PDF/可视化）

## 项目结构

```text
src/stock_picker/
  cli/run.py
  config/
  brokers/
  universe/
  data/
  research/
  report/
```

## 最小运行命令（检查）

dry-run：

```bash
PYTHONPATH=src python -m stock_picker.cli.run --config config.example.yaml --watchlist data/input/watchlist.csv --broker futu --out outputs --dry-run
```

非 dry-run：

```bash
PYTHONPATH=src python -m stock_picker.cli.run --config config.example.yaml --watchlist data/input/watchlist.csv --broker futu --out outputs
```
