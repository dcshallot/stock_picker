# stock_picker

## 1. 项目简介
`stock_picker` 是一个一次性运行的选股管线骨架：

1. 先构建股票池（支持 Futu 服务端条件选股 / watchlist / rules），再从数据源读取历史数据
2. 统一数据格式（bars / quotes / universe / features / candidates）
3. 进行特征工程与可选模型（Prophet 可选，未安装时自动降级）
4. 执行规则筛选 + 打分排序
5. 输出报告与可追溯产物

当前版本已经完成：

- 可运行的工程骨架
- `providers + routing` 配置结构
- `universe.mode=futu_filter`（默认）+ 外置 JSON 条件选股
- Futu OpenD 的真实港股历史 K 线读取,以及API阈值告警
- 新增基于 `Parquet` 的符号级历史仓库和增量同步
- 接入 Yahoo Finance 作为首个免费日线 provider（安装 `yfinance` 时走真实拉取，未安装时回退 stub）

当前版本的实际市场支持：

- `HK`：优先 `futu`，失败时可回退 `yahoo`
- `US` / `CN` / `EU`：默认通过 `yahoo` 获取日线历史

运行环境要求：Python 3.11+。

## 2. 最近进展

### 2026-02-22
- 搭好项目骨架、CLI、stub 数据源、基础测试。

### 2026-03-01
- 接通 Futu OpenD，跑通真实港股历史 K 线。
- 完成 `providers + routing` 重构。
- 增加逐股票结果、K 线额度预检和额度告警。
- 新增基于 `Parquet` 的 `history_store`，按 symbol/year 增量维护日线。
- 接入 Yahoo Finance，US/CN/EU 默认通过 `yahoo` 拉取日线。
- 增加旧 `data/cache` 向 `data/history_store` 的迁移命令。

## 3. Design Principles

- Data layer 不做策略决策；Strategy layer 只读 processed 数据。
- 多数据源适配器 + 路由层：按 `market + dataset` 选择 provider，并支持能力探测与降级。
- 统一 schema：`bars` / `quotes` / `universe` / `features` / `candidates`；并包含 `quality_flags`。
- 缓存与可追溯：`history_store` / `sync metadata` / `run artifacts` 三层产物。
- 配额与限流优先前置：如 Futu 历史 K 线额度，在抓取前先做预检。

## 4. Quickstart

1. 创建并启用虚拟环境：

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

2. dry-run（只看执行计划，默认走 `futu_filter`）：

```bash
PYTHONPATH=src ./.venv/bin/python -m stock_picker.cli.run \
  --config config.example.yaml \
  --provider futu \
  --dry-run
```

3. 运行当前推荐路径（先筛池，再拉历史）：

```bash
PYTHONPATH=src ./.venv/bin/python -m stock_picker.cli.run \
  --config config.example.yaml \
  --universe-mode futu_filter \
  --filter-spec data/input/futu_filter_spec.json \
  --provider futu \
  --bars-only \
  --force-refresh
```

也可直接验证 Yahoo 美股日线：

```bash
PYTHONPATH=src ./.venv/bin/python -m stock_picker.cli.run \
  --config config.example.yaml \
  --symbol US.AAPL \
  --provider yahoo \
  --bars-only \
  --force-refresh
```

## 5. CLI 参数说明

命令入口：`python -m stock_picker.cli.run`

- `--config <path>`：配置文件路径，默认 `config.yaml`（不存在时会提示使用 `config.example.yaml`）。
- `--watchlist <path>`：覆盖配置中的 `universe.watchlist_path`。
- `--symbol <spec>`：直接传入股票代码，可重复。格式如 `US.AAPL`、`HK.00700:HKD`；优先级高于 watchlist/rules。
- `--rules <path>`：覆盖配置中的 `universe.rules_path`。
- `--universe-mode watchlist|rules|futu_filter`：覆盖 `universe.mode`。
- `--filter-spec <path>`：覆盖 `universe.filter_spec_path`。
- `--filter-market <market>`：覆盖 `universe.filter_market`。
- `--filter-plate-code <code>`：覆盖 `universe.filter_plate_code`。
- `--provider <name>`：显式指定 provider，可重复。示例：`futu`、`yahoo`。
- `--broker futu|ibkr_tws|ibkr_cp`：旧参数，作为 `--provider` 的兼容别名保留。
- `--allowed-market <market>`：限制本次运行只处理指定市场，可重复。示例：`HK`。
- `--start-date YYYY-MM-DD`：覆盖 `run.start_date`。
- `--end-date YYYY-MM-DD`：覆盖 `run.end_date`。
- `--out <dir>`：输出根目录，默认 `outputs`。
- `--force-refresh`：对请求窗口强制重抓，并重写 `history_store` 中受影响的 symbol/year 分区。
- `--bars-only`：只拉历史 K 线，跳过 quotes。
- `--dry-run`：仅打印执行计划，不写文件。
- `--log-level INFO|DEBUG|WARNING|ERROR`：日志级别。

## 6. 配置文件说明（`config.yaml`）

可直接复制 `config.example.yaml` 为 `config.yaml`。

当前模板采用 `providers + routing` 设计。完整示例请直接参考仓库内的 `config.example.yaml`。

结构概览：

```yaml
run:
  start_date: 2026-02-22
  end_date: 2026-03-01
  timezone: Asia/Shanghai
  out_dir: outputs
  mode: smoke_test

universe:
  mode: futu_filter
  watchlist_path: data/input/watchlist.csv
  rules_path: data/input/universe_rules.yaml
  filter_spec_path: data/input/futu_filter_spec.json
  filter_market: HK
  filter_plate_code:
  filter_page_size: 200
  max_filter_pages: 200
  prefer_watchlist: true

routing:
  history_bars:
    HK: [futu, yahoo]
    US: [yahoo]
    CN: [yahoo]
    EU: [yahoo]

providers:
  futu:
    kind: futu
    enabled: true
    host: 127.0.0.1
    port: 11112
    allowed_markets: [HK]
    datasets: [history_bars]
    enable_quotes: false
  yahoo:
    kind: yahoo
    enabled: true
    allowed_markets: [US, HK, CN, EU]
    datasets: [history_bars]

data:
  history_bars:
    timeframe: 1D
    adjustment: forward
    store_dir: data/history_store
    bootstrap_start_date: 2018-01-01
    repair_window_days: 30
  quotes:
    enabled: false
  quality:
    max_missing_ratio: 0.1

models:
  prophet:
    enable: false
    params:
      seasonality_mode: multiplicative
      changepoint_prior_scale: 0.05

selection:
  enabled: true
  hard_filters:
    market_whitelist: [HK]
    min_close: 5
    min_volume: 100000
```

说明：

- `routing` 控制“某个市场的某类数据”由哪个 provider 负责。
- `universe.mode` 控制股票池来源，默认 `futu_filter`。
- `universe.filter_spec_path` 指向外置 JSON 条件文件（示例：`data/input/futu_filter_spec.json`）。
- `providers` 描述每个数据源的连接参数、市场边界和可提供的数据集。
- 当前 `FutuConnector` 的真实读取路径使用 OpenD API 端口（`host` + `port`）。
- `Yahoo` provider 依赖 `yfinance`；当前已写入 `requirements.txt`。
- `data.history_bars.bootstrap_start_date` 是首次无本地覆盖记录时的默认回填起点，当前默认 `2018-01-01`。
- `data.history_bars.repair_window_days` 控制每次增量同步时的回补窗口。
- `data.history_bars.max_gap_days_before_full_resync` 控制缺口过大时触发单 symbol 窗口级重建。
- `websocket_port` / `websocket_key` 已加入配置模型，作为后续 WebSocket 集成预留；当前版本不直接使用它们。
- 不建议把真实密钥写入 `config.example.yaml` 这类模板文件，应仅写入本地 `config.yaml`。

## 7. 输出产物

非 dry-run 时会生成：`outputs/run_YYYYMMDD_HHMM/`

运行完成后会自动清理旧产物，只保留最新 `3` 个 `run_*` 目录。

目录下包含：

- `run_config.json`：本次运行的最终配置（含 CLI 覆盖结果）。
- `env.json`：运行环境信息（Python、平台、时间戳等）。
- `diagnostics.json`：诊断信息（routing、capabilities、fetch 状态、quality、model 降级等）。
  其中包含 `provider_limits`，可记录如 Futu 历史 K 线额度的预检结果与阈值告警。
- `candidates.csv`：候选股票表（可为 stub/demo）。
- `symbol_fetch_results.csv`：按股票逐行记录路由、provider、成功/失败状态和错误。
- `bar_data_summary.csv`：每只股票最新一根 K 线的基础行情摘要（若有数据）。
  这是“每个股票 1 条”的摘要，不是整段时间窗口的全量日线。
- `portfolio_candidates.md`：Markdown 报告（run summary + top candidates + diagnostics）。
- `filter_request.json`：本次服务端筛股请求快照（仅 `futu_filter` 模式）。
- `filter_results.csv`：服务端筛股返回股票列表（仅 `futu_filter` 模式）。
- `filter_meta.json`：筛股分页/耗时/数量等元数据（仅 `futu_filter` 模式）。

缓存约定：

- `data/history_store/bars/.../year=YYYY.parquet`
- `data/history_store/meta/coverage.parquet`
- `data/history_store/meta/sync_runs.parquet`
- `data/history_store/meta/provider_health.parquet`

说明：

- `outputs/run_*/bars_snapshot.parquet` 保存的是本次运行视角的标准化 bars 快照。
- `bar_data_summary.csv` 和 `symbol_fetch_results.csv` 只保留每只股票的最新一根 bar，用于快速检查。
- 如需导入旧的请求级缓存，可先运行 `python -m stock_picker.cli.migrate_legacy_cache`，迁移完成后应删除旧 `data/cache`。
- `quotes` 当前仍是运行期临时拉取，不写入 `history_store`。

## 8. 当前状态

- Futu 已接通真实 OpenD。
- Yahoo 已接通通用日线拉取；US/CN/EU 默认使用 `yahoo`。
- HK 默认优先 `futu`，失败时可按路由回退到 `yahoo`。
- Futu 已接入 `get_history_kl_quota(get_detail=True)` 额度预检。
- `history_store` 已替代旧的请求级 `data/cache` 作为日线主存储。
- 旧 `data/cache` 已从主链路移除，只保留迁移命令用于导入历史数据。

## 9. Roadmap

1. 把 `quotes` / `fundamentals` 扩展到独立的长期存储层。
2. 增加更多 provider，并细化 market 级 fallback / 健康度策略。
3. 补充更完整的 symbol 级同步诊断和报告渲染。
4. 接入 Prophet 真实建模与回测评估。
5. 增加更多因子与多格式报告（HTML/PDF/可视化）。

## 项目结构

```text
src/stock_picker/
  cli/run.py
  cli/migrate_legacy_cache.py
  config/
  brokers/
  providers/
  universe/
  data/
  research/
  report/
```

## 10. 最小运行命令（检查）

dry-run（默认 `futu_filter`）：

```bash
PYTHONPATH=src ./.venv/bin/python -m stock_picker.cli.run --config config.example.yaml --provider futu --out outputs --dry-run
```

非 dry-run：

```bash
PYTHONPATH=src ./.venv/bin/python -m stock_picker.cli.run --config config.example.yaml --provider futu --out outputs
```

直接指定股票代码：

```bash
PYTHONPATH=src ./.venv/bin/python -m stock_picker.cli.run --config config.example.yaml --symbol HK.00700:HKD --provider futu --allowed-market HK --bars-only --out outputs
```
