#!/usr/bin/env python3
"""
WeatherBot CLI wrapper.
"""
from __future__ import annotations

import signal
import sys
import logging
import os
from datetime import datetime, timezone

import backfill as backfill_cli
from src.ai.diagnostics import format_ai_diagnostics, run_ai_diagnostics
from src.ai.ourobouros import run_ourobouros
from src.backtest.ranking_backtest import RankingBacktester, format_ranking_report
from src.data.qa import DataQARunner, format_qa_report
from src.data.learning import format_learning_validation, run_learning_validation
from src.notifications import get_notifier
from src.probability.bootstrap import bootstrap_calibration_fit, format_bootstrap_report
from src.trading.engine import RuntimeModes, TradingEngine
from src.weather.config import get_config


ENGINE: TradingEngine | None = None


class TelegramFeedback:
    """Console + Telegram feedback adapter for the trading engine."""

    def __init__(self, tui_mode: bool = False):
        self.tui_mode = tui_mode
        self.notifier = get_notifier()

    def emit(self, message: str) -> None:
        if self.tui_mode:
            print(message)

    def verify_notifications(self) -> bool:
        return self.notifier.verify_token()

    def notify_started(self, mode: str, cities: int, scan_minutes: int) -> None:
        self.notifier.notify_bot_started(mode, cities, scan_minutes)

    def notify_stopped(self, reason: str) -> None:
        self.notifier.notify_bot_stopped(reason)

    def notify_health(self, message: str) -> None:
        self.notifier.notify_health(message)

    def notify_trade_open(
        self,
        city: str,
        date_str: str,
        bucket: str,
        price: float,
        ev: float,
        cost: float,
        source: str,
        note: str = "",
    ) -> None:
        self.notifier.notify_trade_open(city, date_str, bucket, price, ev, cost, source, note)

    def notify_signal(
        self,
        city: str,
        date_str: str,
        bucket: str,
        price: float,
        ev: float,
        cost: float,
        source: str,
        horizon: str,
        question: str,
        market_id: str,
        note: str = "",
        calibrated_prob: float | None = None,
        market_prob: float | None = None,
        uncertainty: float | None = None,
        signal_type: str | None = None,
        quality: float | None = None,
        priority: str | None = None,
        emoji: str | None = None,
        confidence_score: float | None = None,
        source_bias: float | None = None,
        trade_context: dict | None = None,
    ) -> None:
        self.notifier.notify_signal(
            city,
            date_str,
            bucket,
            price,
            ev,
            cost,
            source,
            horizon,
            question,
            market_id,
            note,
            calibrated_prob=calibrated_prob,
            market_prob=market_prob,
            uncertainty=uncertainty,
            signal_type=signal_type,
            quality=quality,
            priority=priority,
            emoji=emoji,
            confidence_score=confidence_score,
            source_bias=source_bias,
            trade_context=trade_context,
        )

    def notify_trade_win(
        self,
        city: str,
        date_str: str,
        bucket: str,
        pnl: float,
        temp: str,
        balance: float,
    ) -> None:
        self.notifier.notify_trade_win(city, date_str, bucket, pnl, temp, balance)

    def notify_trade_loss(
        self,
        city: str,
        date_str: str,
        bucket: str,
        pnl: float,
        balance: float,
    ) -> None:
        self.notifier.notify_trade_loss(city, date_str, bucket, pnl, balance)

    def notify_hourly_report(self, summary: dict, city_signals: list[dict]) -> None:
        self.notifier.notify_hourly_report(summary, city_signals)

    def notify_gem_alert(self, signal: dict) -> None:
        self.notifier.notify_gem_alert(signal)


def parse_mode_override(flag_on: str, flag_off: str, default: bool) -> bool:
    """Resolve a boolean mode from CLI flags with config fallback."""
    if flag_on == "--paper-on" and "-p" in sys.argv:
        return True
    if flag_on in sys.argv:
        return True
    if flag_off in sys.argv:
        return False
    return default


def get_runtime_modes(config) -> RuntimeModes:
    """Get effective runtime mode switches."""
    return RuntimeModes(
        paper_mode=parse_mode_override("--paper-on", "--paper-off", config.paper_mode),
        live_trade=parse_mode_override("--live-on", "--live-off", config.live_trade),
        signal_mode=parse_mode_override("--signal-on", "--signal-off", config.signal_mode),
        tui_mode=parse_mode_override("--tui-on", "--tui-off", config.tui_mode),
    )


def handle_shutdown(signum, _frame) -> None:
    """Forward OS signals to the active engine."""
    global ENGINE
    if ENGINE is not None:
        ENGINE.stop()
    print(f"\nshutdown signal received: {signum}")


def create_engine() -> TradingEngine:
    """Build a configured engine instance."""
    config = get_config()
    modes = get_runtime_modes(config)
    return TradingEngine(config=config, modes=modes, feedback=TelegramFeedback(tui_mode=modes.tui_mode))


def run_loop() -> None:
    """Start the trading engine."""
    global ENGINE
    ENGINE = create_engine()
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    ENGINE.run_forever()


def print_status() -> None:
    """Show status."""
    engine = create_engine()
    for line in engine.status_lines():
        print(line)


def print_report() -> None:
    """Show full report."""
    engine = create_engine()
    for line in engine.report_lines():
        print(line)


def force_resolve_all() -> int:
    """Force resolve all open markets."""
    engine = create_engine()
    return engine.force_resolve_all()


def send_test_message() -> bool:
    """Send a test message to Telegram."""
    notifier = get_notifier()
    result = notifier.send(
        "🔔 **WEATHERBOT DIAGNOSTIC**\n"
        "──────────────\n"
        "📡 **TEST DE CONNEXION**\n"
        "→ Statut: `Opérationnel`\n"
        "→ Latence: `Active`\n"
        "──────────────\n"
        "✅ **Bot is working perfectly!**",
        parse_mode="Markdown"
    )
    print(f"Telegram test: {'OK' if result else 'FAILED'}")
    return result


def print_data_qa() -> None:
    """Show dataset QA diagnostics."""
    config = get_config()
    report = DataQARunner(config.data_dir).run()
    for line in format_qa_report(report):
        print(line)


def run_backfill() -> None:
    """Run historical data backfill from the main CLI."""
    extra_args = sys.argv[2:]
    backfill_cli.main(extra_args)


def run_calibration_bootstrap() -> None:
    """Fit the first persisted calibration model from historical rows."""
    config = get_config()
    report = bootstrap_calibration_fit(data_dir=config.data_dir)
    for line in format_bootstrap_report(report):
        print(line)


def run_ranking_backtest() -> None:
    """Run Top-K ranking backtest diagnostics."""
    config = get_config()
    report = RankingBacktester(f"{config.data_dir}/dataset_rows.jsonl").run(top_k=config.signal_top_k)
    for line in format_ranking_report(report):
        print(line)


def print_ai_status() -> None:
    """Show Groq and auto-improvement diagnostics."""
    config = get_config()
    report = run_ai_diagnostics(config.data_dir)
    for line in format_ai_diagnostics(report):
        print(line)


def print_learning_validation() -> None:
    """Show learning readiness diagnostics."""
    config = get_config()
    report = run_learning_validation(config.data_dir)
    for line in format_learning_validation(report):
        print(line)


def print_paper_report():
    """Print paper trading performance report."""
    from src.trading.paper_account import PaperAccount
    config = get_config()
    account = PaperAccount(config.data_dir)
    print(account.get_report())


def print_audit():
    """Generate and print a quantitative audit report v2.5."""
    from src.data.metrics import calculate_audit_metrics, format_audit_report
    from src.data.validation import run_leakage_audit
    from src.backtest.ranking_backtest import RankingBacktester, format_ranking_report
    from src.ml.calibration_audit import reliability_audit
    from src.backtest.stress_test import run_fat_tail_stress, format_stress_report
    from src.data.reproduce import save_audit_artifact, get_code_hash
    from src.storage import get_storage
    
    config = get_config()
    storage = get_storage(config.data_dir)
    state = storage.load_state()
    
    print(f"🔬 AUDIT ENGINE v2.5 | Code Hash: {get_code_hash()}")
    report_text = f"Audit v2.5 - {datetime.now(timezone.utc).isoformat()}\n"
    
    # 1. Anti-Leakage Audit
    run_leakage_audit()
    
    # 2. Backtest Benchmarking (Out-of-Sample)
    print("\n--- PERFORMANCE COMPARATIVE (Benchmark) ---")
    backtester = RankingBacktester()
    try:
        report = backtester.run(top_k=3)
        for line in format_ranking_report(report):
            print(line)
            report_text += line + "\n"
    except Exception as e:
        print(f"Backtest failed or insufficient data: {e}")
        report_text += f"Backtest failed: {e}\n"

    # 3. Paper Stats
    print("\n--- STATISTIQUES RÉELLES (Paper) ---")
    report_text += "\n--- STATISTIQUES RÉELLES (Paper) ---\n"
    markets = storage.load_all_markets()
    resolved_trades = []
    for m in markets:
        if m.status == "resolved" and m.pnl is not None:
            resolved_trades.append({"pnl": m.pnl, "unix_ts": os.path.getmtime(config.data_dir)})
            
    metrics = calculate_audit_metrics(resolved_trades, state.starting_balance)
    audit_txt = format_audit_report(metrics)
    print(audit_txt)
    report_text += audit_txt
    
    # 4. Calibration Audit (Brier Score)
    print("\n--- CALIBRATION AUDIT (Probabilistic Accuracy) ---")
    probs = [m.position.get("p", 0) for m in markets if m.status == "resolved" and m.position]
    outcomes = [1 if m.resolved_outcome == "win" else 0 for m in markets if m.status == "resolved"]
    if probs and len(probs) == len(outcomes):
        cal_report = reliability_audit(probs, outcomes)
        cal_txt = f"Brier Score: `{cal_report['brier_score']}` | Log Loss: `{cal_report['log_loss']}`\n"
        print(cal_txt)
        report_text += cal_txt
    else:
        print("Insufficient data for calibration audit.")
    
    # 5. Portfolio Risk Snapshot
    print("\n--- PORTFOLIO RISK SNAPSHOT ---")
    risk_summary = create_engine().risk_manager.get_risk_summary(markets)
    risk_txt = (
        f"Total Exposure: `${risk_summary['total_exposure']:.2f}`\n"
        f"Utilization: `{risk_summary['utilization_pct']}%`\n"
        f"Diversification Index: `{risk_summary['diversification_index']}` (HHI-based)\n"
        f"Active Cities: `{risk_summary['active_cities']}`\n"
        f"Regional Breakdown: {risk_summary['region_exposures']}\n"
    )
    print(risk_txt)
    report_text += risk_txt

    # 6. Stress Tests
    print("\n--- STRESS TESTING (Fat Tails) ---")
    if resolved_trades:
        stress_results = run_fat_tail_stress(resolved_trades)
        stress_txt = format_stress_report(stress_results)
        print(stress_txt)
        report_text += stress_txt
    else:
        print("No resolved trades for stress testing.")

    # 7. Final Disclaimer
    disclaimer = (
        "\n🛡️ **POSTURE FINALE D'AUDIT**\n"
        "Le système dispose désormais d’un cadre d’audit avancé incluant validation comparative, "
        "anti-leakage, calibration probabiliste, contrôle du risque portefeuille et reproductibilité des résultats. "
        "Il est prêt pour une phase prolongée de paper trading instrumentée. Avant toute exposition à du capital réel, "
        "des validations supplémentaires restent nécessaires sur la robustesse multi-régimes, la corrélation inter-marchés, "
        "les scénarios extrêmes et la stabilité observée en conditions réelles.\n"
    )
    print(disclaimer)
    report_text += disclaimer

    # 8. Save Artifact
    save_audit_artifact(report_text)


if __name__ == "__main__":
    # Setup basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"

    if cmd == "run":
        run_loop()
    elif cmd == "status":
        print_status()
    elif cmd == "report":
        print_report()
    elif cmd == "paper-report":
        print_paper_report()
    elif cmd == "audit":
        print_audit()
    elif cmd == "dashboard":
        config = get_config()
        if not config.dashboard_enabled:
            print("Dashboard is disabled in configuration.")
        else:
            print("Use: python dashboard.py")
    elif cmd == "resolve":
        force_resolve_all()
    elif cmd == "poll":
        from datetime import date, timedelta
        import argparse
        parser = argparse.ArgumentParser(description="Poll actual temps")
        parser.add_argument("--date", type=str, default=None)
        parser.add_argument("--days", type=int, default=0)
        parser.add_argument("--city", type=str)
        parser.add_argument("--json", action="store_true")
        args, _ = parser.parse_known_args()
        
        from src.weather.apis import get_actual_temp
        from src.weather.locations import LOCATIONS
        
        if args.date:
            date_str = args.date
        elif args.days > 0:
            d = date.today() - timedelta(days=args.days)
            date_str = d.isoformat()
        else:
            date_str = date.today().isoformat()
        
        if args.city:
            loc = LOCATIONS.get(args.city)
            if not loc:
                print(f"Unknown: {args.city}")
            else:
                temp = get_actual_temp(args.city, date_str)
                print(f"{args.city}: {temp}°C" if temp else f"{args.city}: N/A")
        else:
            print(f"=== POLLING ACTUALS: {date_str} ===")
            results = {}
            for slug, loc in LOCATIONS.items():
                temp = get_actual_temp(slug, date_str)
                if temp:
                    results[slug] = temp
            
            if args.json:
                import json
                print(json.dumps(results, indent=2))
            else:
                print(f"{'City':<15} {'Temp':<8}")
                print("-" * 25)
                for slug, temp in sorted(results.items()):
                    print(f"{slug:<15} {temp}°C")
                print(f"\nTotal: {len(results)}/{len(LOCATIONS)}")
    elif cmd == "auto-resolve":
        config = get_config()
        engine = create_engine()
        result = engine.resolver.auto_resolve_pending()
        print(f"=== AUTO-RESOLVE ===")
        print(f"Resolved: {result.get('total', 0)}")
        if result.get('resolved'):
            print("\nResolved markets:")
            for m in result['resolved'][:10]:
                # Find market for unit
                market = engine.storage.load_market(m['city'], m['date'])
                unit = "°F" if market and market.unit == "F" else "°C"
                print(f"  {m['city']} | {m['date']} | {m['actual']}{unit}")
        if result.get('pending'):
            print(f"\nPending ({len(result['pending'])})")
            for m in result['pending'][:5]:
                print(f"  {m['city']} | {m['date']}")
    elif cmd == "errors":
        config = get_config()
        engine = create_engine()
        errors = engine.resolver.get_recent_errors(days=7)
        print("=== RECENT FORECAST ERRORS ===")
        print()
        if not errors:
            print("Aucune erreur disponible")
        else:
            print(f"{'Source':<12} {'Mean':>8} {'MAE':>8} {'N':>5}")
            print("-" * 40)
            for source, stats in sorted(errors.items()):
                print(f"{source:<12} {stats['mean']:>+7.2f} {stats['mae']:>7.2f} {stats['n']:>5}")
    elif cmd == "live-edge":
        config = get_config()
        engine = create_engine()
        
        # Get recent errors and update live bias
        errors = engine.resolver.get_recent_errors(days=7)
        ecmwf_bias = errors.get('ecmwf', {}).get('mean', 0)
        hrrr_bias = errors.get('hrrr', {}).get('mean', 0)
        
        # Update Edge Engine with live biases
        from src.strategy.edge import update_live_bias
        update_live_bias("ecmwf", ecmwf_bias)
        update_live_bias("hrrr", hrrr_bias)
        
        print("=== LIVE EDGE (Real-Time Adjusted) ===")
        print(f"ECMWF bias: {ecmwf_bias:+.2f}°C")
        print(f"HRRR bias: {hrrr_bias:+.2f}°C")
        
        # Check retrain status
        should_train, reason = engine.resolver.should_retrain(min_resolutions=10)
        print(f"Retrain: {reason}")
    elif cmd == "retrain-check":
        engine = create_engine()
        should_trigger, details = engine.resolver.check_and_trigger_retrain(min_resolutions=10)
        print("=== RETRAIN CHECK ===")
        print(f"Ready: {should_trigger}")
        print(f"Details: {details}")
    elif cmd == "gem-check":
        from src.strategy.gem import GEMDetector
        detector = GEMDetector()
        print(detector.format_report())
        print()
        
        # Test case
        test_score = detector.score(
            model_probability=0.70,
            market_price=0.45,
            net_ev=0.12,
            spread=0.02,
            volume=8000,
            confidence=0.70,
            question="Paris max temperature between 20-21°C on 2026-04-25",
        )
        print("=== TEST CASE ===")
        print(f"Score: {test_score.total:.1f}")
        print(f"Valid: {test_score.is_valid}")
        print(f"Divergence: {test_score.divergence:.0%}")
    elif cmd == "test":
        send_test_message()
    elif cmd == "train":
        config = get_config()
        model = create_engine().ml_model
        print(f"ML model trained: samples={model.get('samples', 0)} cities={model.get('cities', 0)} file={config.data_dir}/ml_model.json")
    elif cmd == "tune":
        import argparse
        parser = argparse.ArgumentParser(description="XGBoost hyperparameter tuning")
        parser.add_argument("--search", type=str, default="random", choices=["grid", "random"])
        parser.add_argument("--trials", type=int, default=32)
        parser.add_argument("--timeout", type=int, default=300)
        parser.add_argument("--min-improvement", type=float, default=0.01)
        args, _ = parser.parse_known_args()
        
        from src.ml.hyperopt import run_tuning, format_tuning_report
        result = run_tuning(
            data_dir="data",
            search_type=args.search,
            max_trials=args.trials,
            timeout=args.timeout,
            min_improvement=args.min_improvement,
        )
        for line in format_tuning_report(result):
            print(line)
        
        if result.accepted:
            print("\n✅ Params accepted - saved to tuning history")
        else:
            print("\n❌ Params rejected - keeping baseline")
    elif cmd == "data-qa":
        print_data_qa()
    elif cmd == "backfill":
        run_backfill()
    elif cmd == "calibrate":
        run_calibration_bootstrap()
    elif cmd == "ai-status":
        print_ai_status()
    elif cmd == "ranking-backtest":
        run_ranking_backtest()
    elif cmd == "optimize-weights":
        from src.strategy.optimize import run_grid_search
        result = run_grid_search(min_snapshots=50)
        if result is None:
            print("Not enough data for weight optimization (need >= 50 eligible snapshots)")
        else:
            print(f"Best weights: {result['best_weights']}")
            print(f"Outperformance: {result['outperformance']:+.4f}")
            print("Written to data/scoring_weights.json and applied to src/strategy/scoring.py")
    elif cmd == "learning-validation":
        print_learning_validation()
    elif cmd == "ouroboros":
        import argparse
        parser = argparse.ArgumentParser(description="Ouroboros auto-improvement")
        parser.add_argument("--min-resolutions", type=int, default=10)
        parser.add_argument("--max-retrain-per-day", type=int, default=2)
        parser.add_argument("--patience", type=int, default=5)
        parser.add_argument("--timeout", type=int, default=300)
        args, _ = parser.parse_known_args()
        
        result = run_ourobouros(
            min_resolutions=args.min_resolutions,
            max_retrain_per_day=args.max_retrain_per_day,
            patience=args.patience,
            timeout=args.timeout,
        )
        print(f"[OUROBOROS] {result.get('action', 'unknown')}: {result.get('reason', '')}")
    elif cmd == "purge":
        import argparse
        parser = argparse.ArgumentParser(description="Purge last N messages from Telegram")
        parser.add_argument("--limit", type=int, default=100, help="Number of messages to try deleting")
        args, _ = parser.parse_known_args()
        
        notifier = get_notifier()
        print(f"=== PURGE CHANNEL (Limit: {args.limit}) ===")
        
        # Probe to get current message ID
        probe_id = notifier.send("🧹 **PURGE INITIALISÉE**\n__ Nettoyage en cours... __", parse_mode="Markdown")
        if not probe_id:
            print("❌ Erreur: Impossible d'initialiser la purge.")
        else:
            count = 0
            for i in range(probe_id, probe_id - args.limit, -1):
                if notifier.delete_message(i):
                    count += 1
            print(f"✅ Terminé: {count} messages supprimés.")
    else:
        print("Usage: python bot.py [run|status|report|resolve|test|train|data-qa|backfill|calibrate|ai-status|ranking-backtest|optimize-weights|learning-validation|purge|tune] [--paper-on|--paper-off] [--live-on|--live-off] [--signal-on|--signal-off] [--tui-on|--tui-off]")
