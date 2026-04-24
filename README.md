# 🔬 WeatherBot v1.0 - Quantitative Forecast Desk

**WeatherBot** is an institutional-grade quantitative trading framework designed for forecasting and executing trades on weather-based prediction markets (Polymarket). It integrates advanced probabilistic calibration, multi-layered risk management, and a comprehensive audit trail for independent alpha validation.

---

## 🏛 Architecture & Design

The system follows a modular, decoupled architecture focused on reproducibility and auditability.

- **`src/ml`**: Lightweight calibration models (Brier/LogLoss optimization) to adjust raw forecasts based on historical bias and RMSE.
- **`src/strategy`**: Advanced decision engine including **Kelly Criterion** sizing, **Expected Value (EV)** calculation, and **Signal Quality** filtering.
- **`src/trading`**: Orchestration engine for real-time market scanning, resolution, and execution (Live & Paper).
- **`src/data`**: A robust data layer with an immutable audit trail (JSONL), anti-leakage scanners, and reproducibility hashing.
- **`src/weather`**: Multi-source forecast ingestion (Open-Meteo, Meteostat) with automated drift monitoring.
- **`src/ai/ourobouros`**: Auto-improvement loop for GEM factory with self-training capabilities.

---

## 🛡 Risk Management & Audit

Designed for a solo quantitative desk, the system enforces strict safety layers:

- **Portfolio Risk Layer**:
  - Regional concentration limits (Europe, US, LatAm, Pacific).
  - Exposure caps per city and global portfolio.
  - **Effective Number of Bets (Inverse HHI)**: Real-time diversification monitoring.
- **Audit Framework**:
  - **Anti-Leakage Scanner**: Ensures no future information contaminates decision rows.
  - **Calibration Audit**: Real-time monitoring of probability drift via Brier and Log Loss.
  - **Reproducibility**: Automatic hashing of code and configuration for every audit report.

---

## 📊 APIs Used

| Open-Meteo | None | ECMWF + HRRR forecasts |
| Open-Meteo Archive | None | Historical temps for resolution |
| Meteostat | None (pip) | Fallback historical temps |
| Aviation Weather (METAR) | None | Real-time station observations |
| Polymarket Gamma | None | Market data |
| Groq | Free key | AI analysis |

---

## 🚀 Quick Start

### 1. Installation
```bash
git clone <your-repository-url>
cd <your-repository-name>
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configuration
Copy the environment template and fill in your API keys:
```bash
cp .env.example .env
```

### 3. Execution
The bot is controlled via a unified CLI:
```bash
# Start paper trading with real-time GEM alerts
python bot.py run --paper-on --signal-on

# View status
python bot.py status

# Generate a comprehensive quantitative audit report
python bot.py audit

# View paper trading performance
python bot.py paper-report

# Run AI diagnostics
python bot.py ai-status

# Train ML model
python bot.py train

# Calibrate probabilities
python bot.py calibrate

# Backfill historical temperatures
python bot.py backfill --actuals
```

---

## 🐍 Ouroboros - Auto-Improvement Loop

Ouroboros is an **automatic self-improvement loop** that learns from trading decisions and resolutions. It trains the model, calibrates probabilities, and tunes GEM thresholds autonomously.

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

### GEM Tiers (Suprebets)

| Tier | Score GEM | Condition |
|------|-----------|-----------|
| 🥇 Gold | ≥0.95 | calibration parfaite |
| 🥈 Silver | ≥0.85 | haute confiance |
| 🥉 Bronze | ≥0.75 | seuil minimum |

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
- ⏭️ **SKIP** - Pas assez de données
- 🔥 **START** - Retrain commencé
- ✅ **SUCCESS** - Retrain réussi
- 🚨 **FAILED** - Erreur avec rollback
- 🧪 **TEST** - Vérification

---

## 📊 Reporting & Notifications

Professionalized reporting via Telegram:
- **Hourly Reports**: Summary of PnL, Drawdown, Drift, API Status, and Diversification.
- **GEM Alerts**: Real-time, high-conviction signal notifications (Score > 0.85).
- **Ouroboros**: Auto-improvement loop notifications.
- **Health Checks**: Instant alerts on API failures or critical drift detection.

---

## 🔬 Test Results

### Dataset Status
| Metric | Value |
|--------|-------|
| Total rows | 344 |
| Resolved | 67 |
| Decisions | 157 |
| Historical | 180 |

### Bot Commands
| Command | Status | Detail |
|---------|--------|--------|
| `bot.py status` | ✅ | Balance: $9,827.96 |
| `bot.py train` | ✅ | 500 samples, 20 cities |
| `bot.py calibrate` | ✅ | 25 samples, Brier: 0.244 |
| `bot.py ai-status` | ✅ | Groq OK, Autoimprovement ready |
| `bot.py backfill --actuals` | ✅ | 20/40 récupérés |
| `bot.py test` | ✅ | Telegram OK |
| `bot.py ouroboros` | ✅ | Skip (patience=0) / Retrain |

### Ouroboros State
```
last_trained_rows: 344
last_trained_resolved: 67
retrain_count_today: 1
last_retrain_date: 2026-04-24
last_status: success
```

---

## 📁 Project Structure

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
│   │       ├── __init__.py
│   │       ├── config.py
│   │       ├── state.py
│   │       ├── lock.py
│   │       ├── backup.py
│   │       ├── decision.py
│   │       ├── pipeline.py
│   │       ├── notifier.py
│   │       └── engine.py
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
│   │   └── risk_manager.py
│   ├── trading/
│   │   ├── engine.py
│   │   ├── scanner.py
│   │   ├── resolver.py
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
    └── paper_trades.json
```

---

## 🗺 Roadmap v1.0
- [x] **Ouroboros Loop**: Automatic self-improvement
- [x] **Meteostat Fallback**: Secondary historical data
- [x] **Open-Meteo Archive**: Free historical temps
- [ ] **Adaptive Calibration**: Real-time weight adjustment
- [ ] **Cross-Asset Hedging**: Integrated hedging strategies

---

## ⚠️ Disclaimer & Legal

**WeatherBot** est un système de trading quantitatif expérimental conçu à des fins éducatives et de recherche.

### Limitations
- **Pas de conseil financier** : Ce systeme ne constitue pas un conseil en investissement
- **Paper Trading uniquement** : En mode live, n'utilisez que du capital que vous pouvez vous permettre de perdre
- **Risque de perte** : Les marchés predictionnels comportent des risques significatifs
- **Validation requise** : Au minimum 30-60 jours de validation empirique avant tout capital réel

### Responsibility
- L'utilisateur est seul responsable de ses décisions de trading
- Les performances passées ne garantissent pas les résultats futurs
- Aucune garantie expresse ou implicite

###对本システム
Ce système est actuellement en **phase de validation Paper Trading**. Il est conçu à des fins éducatives et de recherche. Aucun capital ne doit être engagé sans un minimum de 30-60 jours de validation empirique sous conditions surveillées.

*Ce projet est distribué tel quel, sans garantie d'aucune sorte.*

---

## 📜 License
MIT License - See LICENSE file for details