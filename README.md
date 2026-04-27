# WeatherBot - Weather Prediction Market Research Bot

WeatherBot is a modular research and paper-trading bot for weather prediction markets. It scans weather markets, estimates bucket probabilities, ranks opportunities, sends Telegram alerts, records decisions/resolutions, and maintains an audit trail for model validation.

Current posture as of 2026-04-27: **paper + signal mode is operational**. v2.5.3 "Optimization" update completed with enhanced scan frequency (15m), relaxed liquidity filters, and precision rounding fixes.
---

## Architecture

The system follows a modular, decoupled architecture focused on reproducibility and auditability.

- **`src/weather`**: forecast and actual-temperature ingestion (ECMWF, GFS, HRRR, DWD, NWS, METAR).
- **`src/trading`**: runtime engine, scanner, resolver, paper account, and Polymarket CLOB execution adapter.
- **`src/strategy`**: EV, Kelly sizing, ranking, quality filters, and portfolio risk limits.
- **`src/probability`**: bucket probability, uncertainty, and calibration.
- **`src/ml`**: lightweight forecast-bias model with conservative sample-size shrinkage.
- **`src/data`**: append-only decision/resolution rows, QA, backtests, and reproducibility helpers.
- **`src/ai`**: Groq diagnostics and anomaly review (unit-aware °C/°F).
- **`src/ai/ourobouros`**: guarded auto-improvement loop for retrain/calibration attempts.

---

## Risk Management

The system enforces strict safety layers:

- **Portfolio Risk Layer**:
  - Regional concentration limits (Europe, US, LatAm, Pacific).
  - Exposure caps per city and global portfolio.
  - Effective-number-of-bets monitoring via inverse HHI.
- **Audit Framework**:
  - anti-leakage checks,
  - calibration diagnostics,
  - ranking backtests against naive/random baselines,
  - reproducible model/data hashing.
- **Live-trading guardrails**:
  - `LIVE_TRADE=False` by default,
  - live requires `LIVE_TRADE_CONFIRM=true`,
  - live requires `POLYMARKET_PRIVATE_KEY`,
  - CLOB execution uses real orderbook bid/ask rather than Gamma display prices,
  - synthetic stop handling cancels the resting take-profit order before market close.
- **Micro-Live Caps** (NEW):
  - `MAX_LIVE_BET_USD=10` - hard cap per trade in live mode
  - `MAX_LIVE_TOTAL_EXPOSURE_USD=50` - hard cap total exposure in live mode
  - Overrides Kelly calculation if exceeded
- **Drawdown Alerts** (NEW):
  - 5% drawdown → Telegram warning alert
  - 15% drawdown → Kill switch (bot stops automatically)

---

## APIs Used

| API | Credential | Purpose |
|-----|------------|---------|
| Open-Meteo | None | ECMWF + HRRR + GFS forecasts |
| Open-Meteo Archive | None | Historical temps for resolution |
| Meteostat | None (pip) | Fallback historical temps |
| Aviation Weather (METAR) | None | Real-time station observations |
| Polymarket Gamma | None | Market data |
| Polymarket CLOB | Wallet credentials for live | Executable orderbook and order posting |
| Groq | Free key | AI analysis |

---

## ML & Auto-Improvement

The system uses XGBoost for weather prediction with an auto-improvement loop:

- **XGBoost** - Primary ML model (binary classification)
- **Hyperparameter Tuning** - Grid/Random search with CV and safety gates
- **Ouroboros** - Auto-improvement loop with retrain/rollback
- **Calibration** - Isotonic regression for probability calibration

### Hyperparameter Tuning (NEW)
```bash
# Run tuning with default settings
python bot.py tune

# With custom parameters
python bot.py tune --trials 32 --timeout 300 --min-improvement 0.01
```

Features:
- Grid search + Random search over hyperparameter space
- 5-fold cross-validation
- Automatic rejection if no improvement vs baseline
- Safety gates: rejects on small datasets (<100 train rows)
- History tracking in `data/tuning_history.jsonl`

---

## Quick Start

### 1. Installation
```bash
git clone git@github.com:thelambdaone-commits/super-journey-weather-bot.git
cd super-journey-weather-bot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configuration
Copy the environment template and fill in your API keys:
```bash
cp .env.example .env
```

### 3. Safe Execution
The bot is controlled via a unified CLI:
```bash
# Start paper trading with Telegram signal alerts
python bot.py run --paper-on --signal-on --live-off --tui-off

# View status
python bot.py status

# Generate an audit report
python bot.py audit

# View paper trading performance
python bot.py paper-report

# Run AI diagnostics
python bot.py ai-status

# Train ML model
python bot.py train

# Calibrate probabilities
python bot.py calibrate

# Hyperparameter tuning (NEW)
python bot.py tune --trials 32 --timeout 300

# Backfill historical temperatures
python bot.py backfill --actuals

# Data readiness checks
python bot.py data-qa
python bot.py learning-validation
python bot.py ranking-backtest

# Telegram connectivity check
python bot.py test
```

Recommended long-running command:
```bash
nohup ./venv/bin/python -u bot.py run --paper-on --signal-on --live-off --tui-off > logs/signal_bot.log 2>&1 &
```

Current production-safe mode is paper/signal only. Do not enable `--live-on` until the live readiness checklist passes.

---

## Ouroboros Auto-Improvement Loop

Ouroboros is a guarded auto-improvement loop that learns from paper-trading decisions and market resolutions. It is designed to retrain and recalibrate only when there are enough new resolved samples and the daily retrain limit has not been reached.

### Architecture
```
SCAN → SIGNAL → TRADE → RESOLUTION → FEEDBACK
                                      ↓
                            OUROBOROS LOOP
                                      ↓
                            retrain (si conditions réunies)
                                      ↓
                            nouveau modèle + calibration
```

### GEM Tiers

| Tier | Score GEM | Condition |
|------|-----------|-----------|
| Gold | >= 0.95 | strongest candidate |
| Silver | >= 0.85 | high confidence |
| Bronze | >= 0.75 | minimum GEM tier |

### Commands

```bash
# Check (dry run)
python bot.py ouroboros

# With parameters
python bot.py ouroboros --min-resolutions 10 --max-retrain-per-day 2 --patience 5 --timeout 300
```

### Cron Job
```bash
*/30 * * * * cd /home/74h2hfpyj79x/weatherbot && venv/bin/python bot.py ouroboros --min-resolutions 10 --max-retrain-per-day 2 --timeout 300 >> logs/ouroboros.log 2>&1
```

### Notifications

Add to `.env`:
```bash
OUROBOROS_TELEGRAM_FEED=true
```

Events notified:
- SKIP: not enough data or patience not met.
- START: retrain started.
- SUCCESS: retrain succeeded.
- FAILED: retrain failed and rollback was attempted.
- TEST: notification check.

Current state: Ouroboros is configured and guarded, but `Autoimprovement ready` is expected to remain `no` until calibration is fitted and the learning readiness gate passes.

---

## Reporting & Notifications

Professionalized reporting via Telegram:
- **Hourly Reports**: Summary of PnL, Drawdown, Drift, API Status, and Diversification.
- **GEM Alerts**: Real-time, high-conviction signal notifications (Score > 0.85).
- **Ouroboros**: Auto-improvement loop notifications.
- **Health Checks**: Instant alerts on API failures or critical drift detection.

---

## Current Operational Status

Verified on 2026-04-26:

- Telegram main channel: OK.
- Telegram signal channel: OK.
- Runtime process: `bot.py run --paper-on --signal-on --tui-off`.
- Live trading: disabled.
- AI Unit Awareness: OK (°C/°F differentiated).
- Weather Sources: Optimized (GFS Seamless + real HRRR US).

### Dataset Status
| Metric | Value |
|--------|-------|
| Total rows | 446 |
| Resolved rows | 74 |
| Decision rows | 252 |
| Readiness score | 64/100 |
| Readiness label | monitor |
| Ready for scoring fit | no |
| Ready for live | no |

### Bot Commands
| Command | Status | Detail |
|---------|--------|--------|
| `bot.py status` | OK | Balance: $9,879.24, 6 trades, 33% WR |
| `bot.py test` | OK | Telegram main channel OK |
| signal-channel test | OK | `TELEGRAM_SIGNAL_CHAT_ID` OK |
| `bot.py ai-status` | OK | Groq OK, Unit-aware probe OK |
| `bot.py data-qa` | OK | live gate currently no-go |
| `bot.py learning-validation` | OK | readiness 64/100 |
| `bot.py ranking-backtest` | OK | Top-K backtest enabled |
| `bot.py ouroboros` | OK | skip until conditions pass |

### Ouroboros State
```
Autoimprovement ready: no
Calibration fitted: no
Learning readiness: 64/100
Expected action: skip until enough new resolutions exist
```

---

## Project Structure

```
weatherbot/
├── bot.py                    # Main CLI
├── backfill.py              # Historical data backfill
├── dashboard.py            # TUI dashboard
├── requirements.txt        # Dependencies
├── .env                   # API keys (private)
├── README.md               # This file
├── src/
│   ├── ai/
│   │   ├── __init__.py           # Groq AI module
│   │   ├── diagnostics.py         # AI diagnostics
│   │   └── ouroboros/           # Auto-improvement loop
│   ├── data/
│   │   ├── loader.py
│   │   ├── storage.py
│   │   ├── feedback.py
│   │   ├── learning.py
│   │   └── ...
│   ├── strategy/
│   │   ├── scoring.py
│   │   ├── sizing.py
│   │   ├── edge.py
│   │   ├── risk_manager.py
│   │   ├── optimize.py
│   │   └── gem.py
│   ├── ml/
│   │   ├── xgboost_train.py
│   │   ├── hyperopt.py
│   │   ├── calibration_audit.py
│   │   └── registry.py
│   ├── trading/
│   │   ├── engine.py
│   │   ├── scanner.py
│   │   ├── resolver.py
│   │   ├── execution.py
│   │   └── health.py
│   ├── weather/
│   │   ├── config.py
│   │   ├── apis.py
│   │   └── locations.py
│   └── probability/
│       ├── calibration.py
│       ├── bootstrap.py
│       └── uncertainty.py
├── data/
│   ├── dataset_rows.jsonl
│   ├── ml_model.json
│   ├── calibration.pkl
│   ├── ouroboros_state.json
│   ├── backups/
│   └── ...
└── logs/
    ├── bot_runtime.log
    ├── ouroboros.log
    ├── paper_trades.json
```

---

## Roadmap
- [x] Ouroboros loop with lock, backup, rollback and guarded retraining.
- [x] Polymarket CLOB orderbook pricing for executable bid/ask.
- [x] Telegram main and signal-channel notifications.
- [x] Open-Meteo archive and Meteostat fallback for actual temperatures.
- [x] Micro-live caps ($10/$50) and drawdown alerts (5%/15%).
- [x] Hyperparameter tuning with safety gates.
- [x] AI Unit Awareness (°C/°F) and GFS/HRRR optimization.
- [ ] Fit and validate calibration after enough resolved rows.
- [ ] Improve ranking until Top-K outperforms naive/random baselines.
- [ ] Run 30-60 days of monitored paper trading before any live exposure.

---

## Live Readiness Checklist

See `CHECKLIST_MICRO_LIVE.md` for the complete checklist.

### Quick Summary
| Criteria | Threshold |
|----------|-----------|
| Resolved rows | 100+ |
| Paper run | 14+ days |
| Calibration | fitted |
| Top-K vs baseline | positive |
| Max drawdown | < 10% |
| Telegram alerts | verified |
| Live cap | $10 configured |

---

## Ops & Deployment

### Systemd Auto-Restart
Copy the service file and enable:
```bash
sudo cp weatherbot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable weatherbot
sudo systemctl start weatherbot
```

---

## Disclaimer

**WeatherBot** est un système de trading quantitatif expérimental conçu à des fins éducatives et de recherche.

### Limitations
- **Pas de conseil financier** : ce systeme ne constitue pas un conseil en investissement.
- **Paper Trading actuellement** : le mode live est volontairement bloque par des garde-fous.
- **Risque de perte** : les marches predictionnels comportent des risques significatifs.
- **Validation requise** : au minimum 30-60 jours de validation empirique avant tout capital reel.

### Etat actuel
Ce systeme est actuellement en phase de validation paper trading. Aucun capital ne doit etre engage sans donnees resolues suffisantes, calibration fitted, backtest robuste et validation operationnelle prolongee.

*Ce projet est distribué tel quel, sans garantie d'aucune sorte.*

---

## 📜 License
MIT License - See LICENSE file for details
