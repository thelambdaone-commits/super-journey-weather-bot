# Institutional Audit Verdict

```json
{
  "timestamp": "2026-04-27 09:21:59.135357",
  "sections": {
    "unit_tests": {
      "passed": true,
      "details": "pytest passed"
    },
    "startup_audit": {
      "passed": true,
      "details": "ity: V3 Alpha check failed: Insufficient data for lucknow\n2026-04-27 09:22:03,770 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for nyc at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:03,770 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for nyc\n2026-04-27 09:22:03,771 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for seattle at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:03,771 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for seattle\n2026-04-27 09:22:03,772 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for toronto at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:03,772 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for toronto\n2026-04-27 09:22:03,773 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for ankara at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:03,773 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for ankara\n2026-04-27 09:22:03,774 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for chicago at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:03,775 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for chicago\n2026-04-27 09:22:03,775 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for miami at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:03,776 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for miami\n\n==================================================\nRANKING BACKTEST\n==================================================\nDataset: data/dataset_rows.jsonl\nRows: 591\nResolved rows: 74\nDecision rows: 397\nEligible snapshots: 2\nTop-K: 3\n\nTop-K avg PnL: -22.4529\nNaive avg PnL: +2.1150 (Benchmark)\nOutperformance: -24.5679\nRandom avg PnL: +2.1150\nAll avg PnL: -27.8850\nTop-K avg hit rate: 16.7%\nScore/PnL correlation: -0.0565\n\nHit rate by rank:\n - rank 1: 50.0%\n - rank 2: 0.0%\n - rank 3: 0.0%\n\nAvg PnL by rank:\n - rank 1: +17.5471\n - rank 2: -20.0000\n - rank 3: -20.0000\n\nCity breakdown (avg PnL):\n - lucknow: +55.0943\n - london: +29.1358\n - ankara: -20.0000\n - buenos-aires: -20.0000\n - chicago: -20.0000\n - miami: -20.0000\n - nyc: -20.0000\n - seattle: -20.0000\n - toronto: -20.0000\n\nHorizon breakdown (avg PnL):\n - D+0: +0.7050\n - D+1: -20.0000\n\nSource breakdown (avg PnL):\n - ecmwf: +4.8460\n - hrrr: -20.0000\n\nConfidence bucket breakdown (avg PnL):\n - low: -6.1967\n\n95% CI: [+0.0000, +0.0000]\n==================================================\n\n\n--- STATISTIQUES R\u00c9ELLES (Paper) ---\n\ud83d\udcca *QUANT AUDIT REPORT*\n\n| Metric | Value |\n| :--- | :--- |\n| Total Trades | `6` |\n| Win Rate | `33.3%` |\n| Profit Factor | `1.14` |\n| Sharpe Ratio | `0.14` |\n| Max Drawdown | `0.5%` |\n| Expectancy/Trade | `$1.8800` |\n| Avg Win / Loss | `$45.64 / $20.0` |\n| R-Multiple | `2.28` |\n| Net PnL | `$+11.28` |\n| Avg Friction | `1.5%` |\n\n\ud83d\udee1\ufe0f *Status: Technically functional in test environment.*\n\n--- CALIBRATION AUDIT (Probabilistic Accuracy) ---\nInsufficient data for calibration audit.\n\n--- PORTFOLIO RISK SNAPSHOT ---\nTotal Exposure: `$0.00`\nUtilization: `0.0%`\nDiversification Index: `0` (HHI-based)\nActive Cities: `0`\nRegional Breakdown: {}\n\n\n--- STRESS TESTING (Fat Tails) ---\n\ud83d\udd25 *FAT-TAIL STRESS TEST*\n- Liquidity Shock: \u2705 PASSED (Impact: `$0.00`)\n- Black Swan: \u2705 PASSED (Impact: `$-20.00`)\n\n\ud83d\udee1\ufe0f **POSTURE FINALE D'AUDIT**\nLe syst\u00e8me dispose d\u00e9sormais d\u2019un cadre d\u2019audit avanc\u00e9 incluant validation comparative, anti-leakage, calibration probabiliste, contr\u00f4le du risque portefeuille et reproductibilit\u00e9 des r\u00e9sultats. Il est pr\u00eat pour une phase prolong\u00e9e de paper trading instrument\u00e9e. Avant toute exposition \u00e0 du capital r\u00e9el, des validations suppl\u00e9mentaires restent n\u00e9cessaires sur la robustesse multi-r\u00e9gimes, la corr\u00e9lation inter-march\u00e9s, les sc\u00e9narios extr\u00eames et la stabilit\u00e9 observ\u00e9e en conditions r\u00e9elles.\n\n\u2705 Audit artifact saved to logs/artifacts/audit_1777281685_0aed3dc7.md\n"
    },
    "backtest_runs": {
      "passed": true,
      "details": "2026-04-27 09:22:06,781 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for buenos-aires at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:06,781 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for buenos-aires\n2026-04-27 09:22:06,783 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for london at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:06,783 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for london\n2026-04-27 09:22:06,784 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for lucknow at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:06,784 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for lucknow\n2026-04-27 09:22:06,785 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for nyc at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:06,785 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for nyc\n2026-04-27 09:22:06,786 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for seattle at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:06,786 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for seattle\n2026-04-27 09:22:06,787 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for toronto at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:06,787 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for toronto\n2026-04-27 09:22:06,788 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for ankara at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:06,788 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for ankara\n2026-04-27 09:22:06,789 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for chicago at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:06,789 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for chicago\n2026-04-27 09:22:06,790 [WARNING] src.alpha.fair_value: [FV] No valid forecasts found in Moat for miami at 2026-04-28 00:00:00+00:00\n2026-04-27 09:22:06,790 [ERROR] src.strategy.signal_quality: V3 Alpha check failed: Insufficient data for miami\n\n==================================================\nRANKING BACKTEST\n==================================================\nDataset: data/dataset_rows.jsonl\nRows: 591\nResolved rows: 74\nDecision rows: 397\nEligible snapshots: 2\nTop-K: 3\n\nTop-K avg PnL: -22.4529\nNaive avg PnL: +2.1150 (Benchmark)\nOutperformance: -24.5679\nRandom avg PnL: +2.1150\nAll avg PnL: -27.8850\nTop-K avg hit rate: 16.7%\nScore/PnL correlation: -0.0565\n\nHit rate by rank:\n - rank 1: 50.0%\n - rank 2: 0.0%\n - rank 3: 0.0%\n\nAvg PnL by rank:\n - rank 1: +17.5471\n - rank 2: -20.0000\n - rank 3: -20.0000\n\nCity breakdown (avg PnL):\n - lucknow: +55.0943\n - london: +29.1358\n - ankara: -20.0000\n - buenos-aires: -20.0000\n - chicago: -20.0000\n - miami: -20.0000\n - nyc: -20.0000\n - seattle: -20.0000\n - toronto: -20.0000\n\nHorizon breakdown (avg PnL):\n - D+0: +0.7050\n - D+1: -20.0000\n\nSource breakdown (avg PnL):\n - ecmwf: +4.8460\n - hrrr: -20.0000\n\nConfidence bucket breakdown (avg PnL):\n - low: -6.1967\n\n95% CI: [+0.0000, +0.0000]\n==================================================\n\n"
    },
    "lookahead_static_scan": {
      "passed": true,
      "details": "No obvious lookahead patterns"
    },
    "broad_exceptions": {
      "passed": true,
      "details": "No broad except Exception blocks"
    },
    "transaction_cost_model": {
      "passed": true,
      "details": "Fees/slippage references found in core trading logic"
    },
    "database_exists": {
      "passed": true,
      "details": [
        "data/weather_moat.db",
        "data/test_leakage.db"
      ]
    },
    "paper_mode_boot": {
      "passed": true,
      "details": "paper mode launches"
    },
    "performance_engine": {
      "passed": true,
      "details": [
        "src/strategy/performance.py"
      ]
    }
  },
  "score": "9/9",
  "ratio": 1.0,
  "verdict": "PRODUCTION READY"
}
```