"""
Trading engine orchestration.
"""
from __future__ import annotations

import time
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from ..ai import get_groq_client
from ..data.feedback import FeedbackRecorder
from ..data.storage import DatasetStorage
from ..features.builder import FeatureEngine
from ..ml import train_model
from ..probability.inference import ProbabilityEngine
from ..storage import Market, Storage, get_storage
from ..strategy.edge import EdgeEngine
from ..strategy.scoring import ScoringEngine
from ..strategy.signal_quality import SignalQualityLayer, Signal
from ..strategy.risk_manager import PortfolioRiskManager
from ..weather.locations import LOCATIONS
from .types import RuntimeModes, ScanResult, EngineFeedback, NullFeedback
from .helpers import bot_mode_label, build_signal_marker
from .health import get_api_statuses, render_api_statuses
from .scanner import MarketScanner
from .resolver import MarketResolver
from .paper_account import PaperAccount
from .execution import ClobExecutor

logger = logging.getLogger(__name__)

MONITOR_INTERVAL = 600

class TradingEngine:
    """Runtime engine for scans, storage and feedback."""

    def __init__(
        self,
        config,
        modes: RuntimeModes,
        storage: Optional[Storage] = None,
        feedback: Optional[EngineFeedback] = None,
    ):
        self.config = config
        self.modes = modes
        self.storage = storage or get_storage(config.data_dir)
        self.feedback = feedback or NullFeedback()
        self.ml_model = train_model(config.data_dir)
        self.dataset_storage = DatasetStorage(config.data_dir)
        self.feedback_recorder = FeedbackRecorder(self.dataset_storage)
        self.feature_engine = FeatureEngine()
        self.probability_engine = ProbabilityEngine(self.ml_model, config.data_dir)
        self.edge_engine = EdgeEngine()
        self.scoring_engine = ScoringEngine()
        self.signal_quality = SignalQualityLayer(config, config.data_dir)
        self.risk_manager = PortfolioRiskManager(config)
        self.scanner = MarketScanner(self)
        self.resolver = MarketResolver(self)
        self.paper_account = PaperAccount(config.data_dir)
        self.executor = ClobExecutor(config)
        self.running = True
        self.start_time = time.time()
        
        # Load persistent timing
        state = self.storage.load_state()
        self.last_report_ts = state.last_report_ts
        self.gem_threshold = 0.85 # Score threshold for GEM
        from ..strategy.gem import GEMDetector
        self.gem_detector = GEMDetector()

    def stop(self) -> None:
        """Request a graceful stop."""
        self.running = False

    def build_health_report(self, api_statuses: list[tuple[str, str, float]]) -> str:
        """Build a premium health summary with emojis."""
        state = self.storage.load_state()
        markets = self.storage.load_all_markets()
        open_pos = [m for m in markets if m.position and m.position.get("status") == "open"]
        
        mode_label = bot_mode_label(self.modes).upper()
        live_status = "ACTIVÉ ⚠️" if self.modes.live_trade else "DÉSACTIVÉ"
        paper_status = "OUI" if self.modes.paper_mode else "NON"
        
        return (
            f"──────────────\n"
            f"⚙️ *CONFIGURATION*\n"
            f"→ Mode: `{mode_label}`\n"
            f"→ Paper: `{paper_status}` | Live: `{live_status}`\n"
            f"→ Scan: `{self.config.scan_interval // 60} min`\n"
            f"──────────────\n"
            f"💰 *PORTEFEUILLE*\n"
            f"→ Solde: `${state.balance:,.2f}`\n"
            f"→ Positions: `{len(open_pos)}` ouvertes\n"
            f"→ Marchés: `{len(markets)}` suivis\n"
            f"──────────────\n"
            f"🧠 *INTELLIGENCE*\n"
            f"→ ML Samples: `{self.ml_model.get('samples', 0)}` units\n"
            f"→ Diagnostics: `Optimisé` ✅\n"
            f"──────────────\n"
            f"📡 *CONNECTIVITÉ*\n"
            f"{render_api_statuses(api_statuses)}\n"
            f"──────────────\n"
            f"🟢 *SYSTÈME OPÉRATIONNEL*"
        )

    def emit(self, message: str) -> None:
        """Emit a runtime message."""
        self.feedback.emit(message)

    def check_risk(self) -> bool:
        """Verify risk limits and return True if bot should continue."""
        state = self.storage.load_state()
        drawdown = (state.peak_balance - state.balance) / state.peak_balance if state.peak_balance > 0 else 0
        
        # 1. Max Drawdown Kill Switch (15%)
        if drawdown > 0.15:
            self.emit(f"🚨 RISK ALERT: Max Drawdown exceeded ({drawdown*100:.1f}%)")
            return False
            
        # 2. Daily Loss Limit (Placeholder for daily tracking)
        # Only count losses, not gains (max(0, -pnl))
        daily_loss_pct = max(0.0, -state.daily_pnl) / state.starting_balance if state.starting_balance > 0 else 0.0
        if daily_loss_pct > 0.05: # 5% daily loss limit
            self.emit(f"🚨 RISK ALERT: Daily Loss Limit exceeded ({daily_loss_pct*100:.1f}%)")
            return False

        # 3. Max Exposure Check
        markets = self.storage.load_all_markets()
        open_pos_cost = sum(m.position.get("cost", 0) for m in markets if m.position and m.position.get("status") == "open")
        max_exposure = getattr(self.config, "max_exposure", 500.0) # Default $500
        if open_pos_cost > max_exposure:
            self.emit(f"⚠️ EXPOSURE LIMIT: Current ${open_pos_cost:.2f} > Max ${max_exposure:.2f}")
            # We don't stop the bot, but scanner should not open new trades.
        
        return True

    def run_forever(self) -> None:
        """Run the main engine loop."""
        import os
        
        # LIVE TRADE GUARD - Double confirmation required
        if self.modes.live_trade:
            confirm_env = os.environ.get("LIVE_TRADE_CONFIRM", "").lower()
            if confirm_env != "true":
                self.emit("🚨 BLOCKED: LIVE_TRADE requires LIVE_TRADE_CONFIRM=true in .env")
                self.emit("   Add to .env: LIVE_TRADE_CONFIRM=true")
                self.emit("   This is a safety guard to prevent accidental live trading.")
                return
            readiness_error = self.executor.readiness_error()
            if readiness_error:
                self.emit(f"🚨 BLOCKED: CLOB executor is not ready: {readiness_error}")
                self.emit("   Install py-clob-client and set POLYMARKET_PRIVATE_KEY before enabling live trading.")
                return
        
        self.emit(f"\n{'='*50}")
        self.emit(f"WEATHERBOT ({bot_mode_label(self.modes)})")
        self.emit(f"{'='*50}")
        self.emit(f"Cities: {len(LOCATIONS)}")
        self.emit(f"Balance: ${self.config.balance:,.0f} | Max bet: ${self.config.max_bet}")
        self.emit(f"Scan: {self.config.scan_interval // 60}min | Monitor: {MONITOR_INTERVAL // 60}min")
        
        api_statuses = get_api_statuses(self.config, self.feedback)
        self.emit("APIs connectees:")
        self.emit(render_api_statuses(api_statuses))
        self.emit("")
        self.feedback.notify_started(bot_mode_label(self.modes), len(LOCATIONS), self.config.scan_interval // 60)
        self.feedback.notify_health(self.build_health_report(api_statuses))

        last_scan = 0.0
        try:
            while self.running:
                try:
                    now = time.time()
                    
                    if not self.check_risk():
                        self.emit("Bot arrêté par sécurité (Risk Limit).")
                        self.stop()
                        break

                    # Hourly Report & Ouroboros Check
                    if now - self.last_report_ts >= 3600:
                        try:
                            self.send_full_audit_report()
                            
                            # Trigger Ouroboros (Auto-Improvement)
                            self.emit("Checking Ouroboros for auto-improvement...")
                            from src.ai.ourobouros import run_ourobouros
                            run_ourobouros(min_resolutions=10)
                            
                            self.last_report_ts = now
                        except Exception as report_exc:
                            self.emit(f"Report/Ouroboros error: {report_exc}")
                            logger.error(f"Failed to run hourly tasks: {report_exc}")

                    # Scan Check
                    if now - last_scan >= self.config.scan_interval:
                        self.emit(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] scanning...")
                        try:
                            result = self.scanner.scan_and_update()
                            state = self.storage.load_state()
                            self.emit(
                                f" balance: ${state.balance:,.2f} | new: {result.new_trades} | "
                                f"closed: {result.closed} | resolved: {result.resolved}"
                            )
                            last_scan = now
                        except Exception as scan_exc:
                            self.emit(f"Critical scan error: {scan_exc}")
                            logger.exception("Uncaught exception in scan loop")
                            time.sleep(60) # Wait before retry if scan failed

                    if self.running:
                        time.sleep(60) # High-precision monitor sleep (1 min)
                except Exception as loop_exc:
                    self.emit(f"Loop error: {loop_exc}")
                    logger.exception(f"Unexpected error in main loop: {loop_exc}")
                    time.sleep(60) # Safety sleep
        finally:
            self.feedback.notify_stopped("manual stop" if not self.running else "loop exited")

    def process_pending_signals(self, pending_signals: list[dict]):
        """Rank and send top signals."""
        ranked = self.scoring_engine.rank(pending_signals)
        top_k = max(0, int(getattr(self.config, "signal_top_k", 3)))
        selected = ranked[:top_k] if top_k else []
        by_market_id = {item["signal"]["market_id"]: item for item in pending_signals}

        for ranked_item in selected:
            candidate = by_market_id.get(ranked_item.market_id)
            if not candidate: continue

            signal = candidate["signal"]
            trade_context = dict(candidate["trade_context"])
            trade_context.update({"signal_score": ranked_item.score, "rank": ranked_item.rank})
            
            self.feedback.notify_signal(
                candidate["loc"].name, candidate["date_str"], candidate["bucket"],
                signal["entry_price"], signal["ev"], signal["cost"], signal["forecast_src"],
                candidate["horizon"], signal["question"], signal["market_id"], candidate["note"],
                calibrated_prob=signal["p"], market_prob=signal["entry_price"],
                uncertainty=signal.get("edge_penalties", {}).get("uncertainty"),
                signal_type=candidate["filter_decision"]["signal_type"],
                quality=ranked_item.score, priority=candidate["filter_decision"]["priority"],
                emoji=candidate["filter_decision"].get("emoji", "🌡️"),
                confidence_score=signal.get("ml", {}).get("confidence"),
                source_bias=signal.get("ml", {}).get("bias"),
                trade_context=trade_context,
            )
            
            # GEM Signal Detection (Immediate Priority Alert)
            gem_score = self.gem_detector.score(
                model_probability=signal["p"],
                market_price=signal["entry_price"],
                net_ev=signal["ev"],
                spread=signal["spread"],
                volume=trade_context.get("liquidity", 0),
                confidence=signal.get("ml", {}).get("confidence", 0),
                question=signal["question"],
            )

            if gem_score.is_valid or ranked_item.score >= self.gem_threshold:
                gem_data = {
                    "city": candidate["loc"].name,
                    "question": signal["question"],
                    "edge": signal["ev"],
                    "score": max(ranked_item.score, gem_score.total / 100.0 if gem_score.total > 0 else 0),
                    "price": signal["entry_price"],
                    "prob": signal["p"],
                    "sizing": signal["cost"],
                    "horizon": candidate["horizon"],
                    "reason": candidate["note"],
                    "url": f"https://polymarket.com/event/{signal['market_id']}",
                    "risk_status": "MODERATE" if not gem_score.exclusion_reason else "HIGH",
                    "conf": int(signal.get("ml", {}).get("confidence", 0) * 100)
                }
                self.feedback.notify_gem_alert(gem_data)
                self.emit(f"💎 GEM ALERT: {candidate['loc'].name} | Score {gem_data['score']:.2f}")

            # Commit the signal to persistence (cooldowns, etc.)
            self.signal_quality.commit(Signal.from_dict(candidate["loc"].name, signal))
            candidate["market"].signal_state = build_signal_marker(signal)
            self.storage.save_market(candidate["market"])
            self.emit(f"[RANK {ranked_item.rank}] {candidate['loc'].name} | ${signal['entry_price']:.3f} | score {ranked_item.score:.2f}")

    def resolve_market(self, market: Market, balance: float):
        """Delegate market resolution to the resolver."""
        return self.resolver.resolve_market(market, balance)

    def status_lines(self) -> list[str]:
        """Return status lines for CLI output."""
        state = self.storage.load_state()
        markets = self.storage.load_all_markets()
        open_pos = [m for m in markets if m.position and m.position.get("status") == "open"]
        resolved = [m for m in markets if m.status == "resolved" and m.pnl is not None]
        balance, start = state.balance, state.starting_balance
        ret = (balance - start) / start * 100 if start else 0
        wins = sum(1 for m in resolved if m.resolved_outcome == "win")
        losses = sum(1 for m in resolved if m.resolved_outcome == "loss")
        total = len(open_pos) + len(resolved)
        wr = f"{wins/total*100:.0f}%" if total else "0%"

        return [
            f"\n{'='*50}", "WEATHERBOT STATUS", f"{'='*50}",
            f"Balance: ${balance:,.2f} ({ret:+.1f}%)",
            f"Trades: {total} | W: {wins} | L: {losses} | WR: {wr}",
            f"Open: {len(open_pos)} | Resolved: {len(resolved)}",
            f"{'='*50}\n",
        ]

    def report_lines(self) -> list[str]:
        """Return report lines for CLI output."""
        resolved = [m for m in self.storage.load_all_markets() if m.status == "resolved" and m.pnl is not None]
        lines = [f"\n{'='*50}", "WEATHERBOT REPORT", f"{'='*50}"]
        if not resolved:
            lines.append("No resolved markets.")
            return lines

        total_pnl = sum(m.pnl for m in resolved)
        wins = sum(1 for m in resolved if m.resolved_outcome == "win")
        total_trades = len(resolved)
        lines.extend([
            f"\nResolved: {total_trades} | Wins: {wins} | Losses: {total_trades - wins}",
            f"Win rate: {wins/total_trades*100:.0f}% | Total PnL: ${total_pnl:+.2f}\n"
        ])

        winners = sorted([m for m in resolved if m.resolved_outcome == "win"], key=lambda x: x.pnl or 0, reverse=True)[:10]
        if winners:
            lines.append("Top winning trades:")
            for m in winners:
                unit = "°F" if m.unit == "F" else "°C"
                bucket = f"{m.position['bucket_low']}-{m.position['bucket_high']}{unit}"
                lines.append(f" - {m.city_name} {m.date} | {bucket} | +${m.pnl:.2f}")
        return lines

    def force_resolve_all(self) -> int:
        """Delegate force resolution to the resolver."""
        return self.resolver.force_resolve_all()

    def send_full_audit_report(self):
        """Generate and send the comprehensive hourly report."""
        from src.data.metrics import calculate_audit_metrics
        from src.trading.health import get_api_statuses
        
        state = self.storage.load_state()
        markets = self.storage.load_all_markets()
        
        # 1. Metrics Calculation
        resolved_trades = []
        for m in markets:
            if m.status == "resolved" and m.pnl is not None:
                resolved_trades.append({
                    "pnl": m.pnl, 
                    "unix_ts": time.time() # Proxy for metrics
                })
        
        metrics = calculate_audit_metrics(resolved_trades, state.starting_balance)
        
        # 2. Portfolio Summary
        drawdown = (state.peak_balance - state.balance) / state.peak_balance if state.peak_balance > 0 else 0
        
        # 3. API & Health
        api_statuses = get_api_statuses(self.config, self.feedback)
        uptime_sec = int(time.time() - self.start_time)
        uptime_str = f"{uptime_sec // 3600}h {(uptime_sec % 3600) // 60}m"
        
        # 4. Risk & Diversification
        risk_summary = self.risk_manager.get_risk_summary(markets)
        
        summary = {
            "pnl_total": metrics.total_pnl_net,
            "pnl_pct": (metrics.total_pnl_net / state.starting_balance) * 100 if state.starting_balance > 0 else 0,
            "exposure": risk_summary["total_exposure"],
            "drawdown": drawdown * 100,
            "active_signals": risk_summary["active_cities"],
            "drift": metrics.drift_status,
            "uptime": uptime_str,
            "api_status": " | ".join([f"{n}: {s}" for n, s, l in api_statuses]),
            "hhi_div": risk_summary["diversification_index"]
        }
        
        # 5. Gather Latest Signals for ALL Cities
        city_signals = []
        by_city = {}
        for m in markets:
            if m.status == "resolved": continue
            
            # Priority: Open Position > Signal Marker > Last Analysis
            sig = None
            if m.position and m.position.get("status") == "open":
                sig = {
                    "city": m.city_name,
                    "edge": m.position.get("ev", 0),
                    "conf": m.position.get("ml", {}).get("confidence", 0) * 100,
                    "price": m.position.get("entry_price", 0),
                    "risk": "OPEN"
                }
            elif m.signal_state:
                sig = {
                    "city": m.city_name,
                    "edge": m.signal_state.get("ev", 0),
                    "conf": m.signal_state.get("ml_conf", 0) * 100,
                    "price": m.signal_state.get("entry_price", 0),
                    "risk": "SIGNAL"
                }
            elif m.last_analysis:
                sig = {
                    "city": m.city_name,
                    "edge": m.last_analysis.get("ev", 0),
                    "conf": m.last_analysis.get("conf", 0) * 100,
                    "price": m.last_analysis.get("price", 0),
                    "risk": "WATCH"
                }
            
            if sig:
                # Keep the one with highest edge per city
                if m.city not in by_city or sig["edge"] > by_city[m.city]["edge"]:
                    by_city[m.city] = sig
        
        # Sort and limit to top 15 for readability
        sorted_sigs = sorted(by_city.values(), key=lambda x: x["edge"], reverse=True)
        city_signals = sorted_sigs[:15]
        
        self.feedback.notify_hourly_report(summary, city_signals)
        
        # Persist last report timestamp
        state.last_report_ts = time.time()
        self.storage.save_state(state)
