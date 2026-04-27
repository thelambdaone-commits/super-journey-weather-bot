# WeatherBot

WeatherBot is a Python research and paper-trading system for weather prediction markets. It scans Polymarket weather markets, collects forecasts, estimates fair value, filters risky opportunities, sends Telegram alerts, records paper trades, resolves markets, and keeps enough data for audit and model improvement.

Current status on 2026-04-27: paper trading and signal mode are operational. Live trading is disabled by default and guarded by explicit runtime and environment checks.

Repository: `git@github.com:thelambdaone-commits/super-journey-weather-bot.git`

---

## Safety Posture

Default operating mode is research/paper only:

- `PAPER_MODE=True`
- `SIGNAL_MODE=True`
- `LIVE_TRADE=False`
- `--live-on` is blocked unless `LIVE_TRADE_CONFIRM=true` is present.
- Live CLOB execution also requires wallet credentials and executor readiness.
- Paper accounting is separated from live state.
- The current safe long-running command is:

```bash
setsid ./venv/bin/python -u bot.py run --paper-on --signal-on --live-off --tui-off > logs/signal_bot.log 2>&1 < /dev/null &
```

Do not enable live trading until the live readiness checklist has passed with enough resolved paper samples, fitted calibration, positive ranking validation, and verified alerting.

---

## Architecture

The codebase is organized around a runtime engine with small domain modules.

```text
weatherbot/
├── bot.py                         # CLI entrypoint and runtime commands
├── backfill.py                    # Historical data backfill helper
├── dashboard.py                   # Local dashboard/TUI entrypoint
├── requirements.txt               # Python dependencies
├── weatherbot.service             # systemd service template
├── src/
│   ├── ai/                        # Groq diagnostics and Ouroboros loop
│   ├── alpha/                     # Fair value engine
│   ├── backtest/                  # Ranking and walk-forward backtests
│   ├── data/                      # DuckDB moat, QA, feedback, schema
│   ├── features/                  # Feature construction
│   ├── ml/                        # Calibration, model training, tuning
│   ├── notifications/             # Telegram and desk metrics
│   ├── probability/               # Probability inference/calibration
│   ├── settlement/                # Station mapping for settlement
│   ├── storage/                   # JSON state and market persistence
│   ├── strategy/                  # EV, sizing, filters, risk, scoring
│   ├── trading/                   # Engine, scanner, resolver, CLOB, paper account
│   ├── utils/                     # Feature flags and shared utilities
│   └── weather/                   # Forecast/actual APIs and collectors
├── tests/
│   ├── test_paper_logic.py        # Paper account and paper resolver coverage
│   └── test_fair_value_no_leakage.py
├── data/                          # Runtime data, models, market state
└── logs/                          # Runtime and paper-trading logs
```

### Runtime Flow

```text
scan markets
  -> fetch forecasts and market/orderbook data
  -> estimate probability and edge
  -> apply filters, risk checks, and AI review
  -> record paper trade and/or send signal
  -> resolve open positions when market outcome is available
  -> update paper account, state, logs, and feedback datasets
```

### Core Modules

- `src/trading/engine.py` orchestrates scans, health checks, reports, and runtime loops.
- `src/trading/scanner.py` discovers markets, builds candidate trades, records paper positions, and emits signals.
- `src/trading/resolver.py` resolves live and paper positions, updates market status, and records settlement PnL.
- `src/trading/paper_account.py` manages the separate paper-trading balance, locked stake, fees, wins/losses, drawdown, and reports.
- `src/trading/polymarket.py` wraps Polymarket Gamma and CLOB orderbook access.
- `src/weather/apis.py` fetches forecasts and actual temperatures.
- `src/weather/collectors/open_meteo.py` stores multi-model forecast runs in the DuckDB moat.
- `src/data/moat_manager.py` owns the DuckDB forecast/quote/calibration store.
- `src/strategy/*` contains filters, sizing, portfolio risk, signal quality, and ranking.
- `src/ml/*` contains lightweight calibration, XGBoost/logistic training, tuning, and audits.

---

## Paper Trading Model

Paper trading is intentionally separate from live trading.

Entry:

- `record_trade(cost)` increments total trades.
- The stake is locked from paper cash.
- Entry friction is charged as `FEE_RATE + SLIPPAGE_RATE`.
- Entry friction is included in net paper PnL.

Settlement:

- `resolver.py` computes binary settlement PnL from shares, entry price, stake, and trading fee.
- `record_result(won, pnl, cost)` returns `cost + pnl` to paper cash.
- Paper-only resolution does not mutate live cash balance or live win/loss counters.
- Closed live positions are not allowed to mask open paper positions.

Covered by `tests/test_paper_logic.py`.

---

## External Services

| Service | Credential | Use |
| --- | --- | --- |
| Open-Meteo | none | ECMWF/GFS/HRRR forecasts and archive data |
| Meteostat | none | Historical actual fallback |
| Aviation Weather/METAR | none | Station observation support |
| Polymarket Gamma | none | Market metadata |
| Polymarket CLOB | live wallet credentials | Executable orderbook and live order placement |
| Groq | `GROQ_API_KEY` | AI diagnostics and trade review |
| Telegram | bot token and chat IDs | Alerts, status, reports |

---

## Installation

```bash
git clone git@github.com:thelambdaone-commits/super-journey-weather-bot.git
cd super-journey-weather-bot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` before running the bot.

Minimum useful variables:

```bash
PAPER_MODE=True
SIGNAL_MODE=True
LIVE_TRADE=False
SCAN_INTERVAL=900
GROQ_API_KEY=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
TELEGRAM_SIGNAL_CHAT_ID=...
```

Live-only variables, required only for live execution:

```bash
LIVE_TRADE_CONFIRM=true
POLYMARKET_PRIVATE_KEY=...
```

---

## CLI Commands

```bash
# Run paper/signal mode
python bot.py run --paper-on --signal-on --live-off --tui-off

# Status and reports
python bot.py status
python bot.py report
python bot.py paper-report
python bot.py audit

# Resolution and actuals
python bot.py auto-resolve
python bot.py resolve
python bot.py poll --date YYYY-MM-DD

# Data/model diagnostics
python bot.py data-qa
python bot.py learning-validation
python bot.py ai-status
python bot.py ranking-backtest
python bot.py walk-forward

# Model work
python bot.py train
python bot.py tune --trials 32 --timeout 300
python bot.py calibrate
python bot.py ouroboros

# Telegram connectivity
python bot.py test
```

Long-running paper command:

```bash
setsid ./venv/bin/python -u bot.py run --paper-on --signal-on --live-off --tui-off > logs/signal_bot.log 2>&1 < /dev/null &
```

Check the running process:

```bash
ps -ef | grep '[p]ython.*bot.py run'
tail -f logs/signal_bot.log
```

---

## Testing

Run the full suite:

```bash
./venv/bin/python -m pytest -q
```

Run the same quality gates used by CI:

```bash
./venv/bin/ruff check .
./venv/bin/ruff format --check pyproject.toml tests src/data/moat_manager.py src/trading/polymarket.py src/trading/engine.py
./venv/bin/python -m pytest
./venv/bin/python -m pip check
```

Run paper-specific tests:

```bash
./venv/bin/python -m pytest tests/test_paper_logic.py -q
```

Current local validation on 2026-04-27:

- `tests/test_paper_logic.py`: 8 passed
- full suite: 15 passed
- coverage gate: 89% on the current paper-account baseline, with `--cov-fail-under=80`
- `git diff --check`: clean

GitHub Actions runs the same lint, format, dependency, and coverage checks on every push and pull request to `main`. Workflow permissions are restricted to `contents: read`. Dependabot is enabled weekly for both pip dependencies and GitHub Actions.

---

## Requirements

`requirements.txt` pins the direct runtime and test dependencies used by the current codebase:

- HTTP/API clients: `requests`, `httpx`, `groq`
- Configuration/UI: `python-dotenv`, `textual`, `rich`, `pydantic`
- Data/analytics: `numpy`, `pandas`, `scipy`, `duckdb`, `polars`
- Weather/data fallback: `meteostat`, `pytz`
- ML: `scikit-learn`, `joblib`, `xgboost`
- Polymarket live adapter: `py-clob-client`
- Tests: `pytest`

`xgboost` is listed for model training/tuning paths. The lightweight JSON calibration path can run without training an XGBoost model, but the dependency should be installed for the full command set.

---

## Data and Logs

Important runtime files:

```text
data/state.json                 # live/state accounting
data/paper_account.json         # paper account stats
data/markets/*.json             # per-market snapshots and positions
data/weather_moat.db            # DuckDB forecast/quote moat
data/stale_clob_tokens.json     # CLOB token IDs suppressed after repeated 404s
data/ouroboros_state.json       # auto-improvement state
logs/signal_bot.log             # long-running bot log
logs/paper_trades.json          # paper trade journal
logs/bot_runtime.log            # runtime/service log
```

### Polymarket CLOB Resilience

The CLOB orderbook client retries transient failures with exponential backoff. Permanent CLOB `404` responses mark the token ID as stale for 24 hours in `data/stale_clob_tokens.json`, which prevents repeated noisy requests against expired or invalid token IDs. Expired stale-token entries are pruned automatically.

### DuckDB Access

DuckDB allows either one read-write process or multiple read-only processes. `MoatManager` now supports explicit `read_only=True` connections and does not keep a read-write connection open between operations. This keeps write locks short-lived and reduces conflicts with diagnostics or read-only tooling.

Do not commit `.env`, runtime logs, or private credentials.

---

## Deployment

### Manual Detached Process

```bash
setsid ./venv/bin/python -u bot.py run --paper-on --signal-on --live-off --tui-off > logs/signal_bot.log 2>&1 < /dev/null &
```

### systemd Template

The repo includes `weatherbot.service`. Review `ExecStart`, environment, user, and live flags before enabling it.

```bash
sudo cp weatherbot.service /etc/systemd/system/weatherbot.service
sudo systemctl daemon-reload
sudo systemctl enable weatherbot
sudo systemctl start weatherbot
sudo systemctl status weatherbot
```

---

## Operational Status

Last verified locally on 2026-04-27:

- Bot mode: `paper_mode,signal_mode,tui_off`
- Live trading: disabled
- APIs during network-enabled run: Telegram connected, Groq connected, Polymarket connected, Open-Meteo connected
- Bot status: balance `$9,879.24`, 6 resolved live/state trades, 33% win rate
- Paper report: separate paper account active with paper trades and open paper positions
- Latest scan outcome: no new paper trade when AI/orderbook filters rejected candidates

No result in this repository should be interpreted as investment advice. This system is experimental research software and must be validated over a long paper-trading period before any real capital is considered.

---

## Maintenance Checklist

Before pushing a production-facing change:

```bash
./venv/bin/python -m pytest -q
./venv/bin/python -m py_compile bot.py src/trading/paper_account.py src/trading/resolver.py src/trading/scanner.py
git diff --check
git status --short
```

For paper/resolver changes, also run:

```bash
./venv/bin/python -m pytest tests/test_paper_logic.py -q
```

---

## License

MIT License. See `LICENSE`.
