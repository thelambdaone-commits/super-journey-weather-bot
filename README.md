# 🔬 WeatherBot v0.0.1 - Quantitative Forecast Desk

**WeatherBot** is an institutional-grade quantitative trading framework designed for forecasting and executing trades on weather-based prediction markets (Polymarket). It integrates advanced probabilistic calibration, multi-layered risk management, and a comprehensive audit trail for independent alpha validation.

---

## 🏛 Architecture & Design

The system follows a modular, decoupled architecture focused on reproducibility and auditability.

- **`src/ml`**: Lightweight calibration models (Brier/LogLoss optimization) to adjust raw forecasts based on historical bias and RMSE.
- **`src/strategy`**: Advanced decision engine including **Kelly Criterion** sizing, **Expected Value (EV)** calculation, and **Signal Quality** filtering.
- **`src/trading`**: Orchestration engine for real-time market scanning, resolution, and execution (Live & Paper).
- **`src/data`**: A robust data layer with an immutable audit trail (JSONL), anti-leakage scanners, and reproducibility hashing.
- **`src/weather`**: Multi-source forecast ingestion (Open-Meteo, Visual Crossing, etc.) with automated drift monitoring.

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

API	Auth	Purpose
Open-Meteo	None	ECMWF + HRRR forecasts
Aviation Weather (METAR)	None	Real-time station observations
Polymarket Gamma	None	Market data
Visual Crossing	Free key	Historical temps for resolution

---


## 🚀 Quick Start

### 1. Installation
```bash
git clone https://github.com/thelambdaone-commits/sturdy-octo-funicular-weather-bot.git
cd sturdy-octo-funicular-weather-bot
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

# Generate a comprehensive quantitative audit report
python bot.py audit

# View paper trading performance
python bot.py paper-report

# Run AI diagnostics
python bot.py ai-status
```

---

## 📊 Reporting & Notifications

Professionalized reporting via Telegram:
- **Hourly Reports**: Summary of PnL, Drawdown, Drift, API Status, and Diversification.
- **GEM Alerts**: Real-time, high-conviction signal notifications (Score > 0.85).
- **Health Checks**: Instant alerts on API failures or critical drift detection.

---

## 🗺 Roadmap v3.0
- [ ] **Adaptive Calibration**: Real-time weight adjustment of forecast sources based on rolling windows.
- [ ] **Cross-Asset Hedging**: Integrated hedging strategies using correlated commodity futures.
- [ ] **Advanced Stress Testing**: Monte Carlo simulations on synthetic extreme weather scenarios.

---

## ⚠️ Disclaimer
*This system is currently in **Paper Trading Validation phase**. It is designed for educational and research purposes. No capital should be committed without a minimum of 30-60 days of empirical validation under monitored conditions.*
