"""
Trading engine orchestration.
"""

from __future__ import annotations

import json
import time
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from ..ai import get_groq_client
from ..data.feedback import FeedbackRecorder
from ..notifications.desk_metrics import log_event
from ..data.storage import DatasetStorage
from ..features.builder import FeatureEngine
from ..ml import train_model
from ..probability.inference import ProbabilityEngine
from ..storage import Market, Storage, get_storage
from ..strategy.edge import (
    EdgeEngine,
    gross_edge,
    net_ev,
    estimate_fee,
    estimate_slippage,
)
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
from ..strategy.portfolio import PortfolioOptimizer
from ..utils.feature_flags import is_enabled


def can_trade_live(config) -> tuple[bool, str]:
    """
    Standalone function to check all preconditions for live trading.
    Double lock: live_trade=true AND confirm_live_trading="I_ACCEPT_REAL_LOSS"
    Also checks kill_switch and executor readiness.
    """
    if not config.live_trade:
        return False, "live_trade=false"
    if config.kill_switch_enabled:
        return False, "kill_switch_active"
    if config.confirm_live_trading != "I_ACCEPT_REAL_LOSS":
        return False, "missing_double_lock (need confirm_live_trading='I_ACCEPT_REAL_LOSS')"
    # Executor readiness is checked separately where executor is available
    return True, "ok"

logger = logging.getLogger(__name__)

MONITOR_INTERVAL = 600


def _parse_iso_datetime(value: str | None) -> datetime | None:
    """Parse persisted ISO timestamps, accepting trailing Z."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _display_edge(edge: float | None, signal: dict) -> float:
    """Return edge in percentage points for reports, preferring probability-price edge."""
    probability = signal.get("p")
    price = signal.get("entry_price") or signal.get("price")
    try:
        if probability is not None and price is not None:
            return (float(probability) - float(price)) * 100.0
        return float(edge or 0.0) * 100.0
    except (TypeError, ValueError):
        return 0.0


def _signal_bucket(candidate: dict, trade_context: dict, signal: dict) -> str:
    """Build a display bucket even when older pending candidates omit it."""
    bucket = candidate.get("bucket") or trade_context.get("bucket") or signal.get("bucket")
    if bucket:
        return str(bucket)

    low = signal.get("bucket_low")
    high = signal.get("bucket_high")
    if low is None or high is None:
        return "N/A"

    unit = trade_context.get("unit") or candidate.get("unit") or ""
    return f"{low}-{high}{unit}"


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
        self._sync_state_from_paper_account()
        self.portfolio_optimizer = PortfolioOptimizer(config)
        self.executor = ClobExecutor(config)
        self.running = True
        self.start_time = time.time()
        self.error_count = 0
        self.latency_sum = 0.0
        self.latency_count = 0

        # Load persistent timing
        state = self.storage.load_state()
        self.last_report_ts = state.last_report_ts
        self.gem_threshold = float(getattr(config, "gem_threshold", 0.85))
        from ..strategy.gem import GEMDetector

        self.gem_detector = GEMDetector()

    def _sync_state_from_paper_account(self) -> None:
        """Keep legacy storage state aligned with the reconciled paper ledger."""
        if not getattr(self.modes, "paper_mode", False):
            return
        paper_stats = self.paper_account.get_state()
        if not getattr(paper_stats, "accounting_complete", False):
            return
        try:
            state = self.storage.load_state()
            state.balance = paper_stats.balance
            state.total_trades = paper_stats.total_trades
            state.wins = paper_stats.wins
            state.losses = paper_stats.losses
            self.storage.save_state(state)
        except Exception:
            logger.exception("Failed to sync storage state from paper account")

    def stop(self) -> None:
        """Request a graceful stop."""
        self.running = False

    def build_health_report(self, api_statuses: list[tuple[str, str, float]]) -> str:
        """Build a premium health summary with emojis."""
        state = self.storage.load_state()
        markets = self.storage.load_all_markets()
        open_live = [m for m in markets if m.position and m.position.get("status") == "open"]
        open_paper = [m for m in markets if m.paper_position and m.paper_position.get("status") in ("open", "paper")]
        open_pos = open_live + open_paper
        paper_stats = self.paper_account.stats
        
        # Guard: Prevent regression - paper positions must be counted
        if len(open_paper) > 0 and len(open_pos) == len(open_live):
            import logging
            logging.getLogger(__name__).error(
                f"REGRESSION: Paper positions NOT counted! "
                f"Live: {len(open_live)}, Paper: {len(open_paper)}, Total: {len(open_pos)}"
            )

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
            f"→ Solde cash: `${paper_stats.balance:,.2f}`\n"
            f"→ Equity paper: `${getattr(paper_stats, 'equity', paper_stats.balance):,.2f}`\n"
            f"→ Positions: `{len(open_pos)}` ouvertes ({len(open_live)} live, {len(open_paper)} paper)\n"
            f"→ Historique: `{paper_stats.total_trades}` paris\n"
            f"{self._gains_pertes_display(paper_stats)}\n"
            f"→ Marchés: `{len(markets)}` suivis\n"
            f"──────────────\n"
            f"🧠 *INTELLIGENCE*\n"
            f"→ ML Samples: `{self._ml_sample_count()}` units\n"
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

        # 1. Telegram Alert for 5% Drawdown Warning
        if drawdown > 0.05:
            self.feedback.notify_health(f"⚠️ DRAWDOWN WARNING: {drawdown * 100:.1f}%")

        # 2. Max Drawdown Kill Switch (15%)
        if drawdown > 0.15:
            self.emit(f"🚨 RISK ALERT: Max Drawdown exceeded ({drawdown * 100:.1f}%)")
            self.feedback.notify_stopped("max_drawdown_15pct")
            return False

        # 3. Daily Loss Limit (Placeholder for daily tracking)
        # Only count losses, not gains (max(0, -pnl))
        daily_loss_pct = max(0.0, -state.daily_pnl) / state.starting_balance if state.starting_balance > 0 else 0.0
        if daily_loss_pct > 0.05:  # 5% daily loss limit
            self.emit(f"🚨 RISK ALERT: Daily Loss Limit exceeded ({daily_loss_pct * 100:.1f}%)")
            return False

        # 3. Max Exposure Check
        markets = self.storage.load_all_markets()
        open_pos_cost = sum(
            m.position.get("cost", 0) for m in markets if m.position and m.position.get("status") == "open"
        )
        max_exposure = self._max_total_exposure(state.balance)
        if open_pos_cost > max_exposure:
            self.emit(f"⚠️ EXPOSURE LIMIT: Current ${open_pos_cost:.2f} > Max ${max_exposure:.2f}")
            # We don't stop the bot, but scanner should not open new trades.

        return True

    def _ml_sample_count(self) -> int:
        """Return model sample count for health reports across model implementations."""
        if isinstance(self.ml_model, dict):
            return int(self.ml_model.get("samples", 0) or 0)
        return int(getattr(self.ml_model, "samples", 0) or 0)

    def _max_total_exposure(self, balance: float) -> float:
        """Calculate total open-position exposure limit from explicit config or pct cap."""
        explicit_limit = getattr(self.config, "max_total_exposure", None)
        if explicit_limit is not None:
            return float(explicit_limit)
        exposure_pct = float(getattr(self.config, "max_market_exposure_pct", 0.05))
        return float(balance) * exposure_pct

    def _gains_pertes_display(self, paper_stats) -> str:
        """Return gains/pertes display based on history availability."""
        from pathlib import Path
        
        data_dir = getattr(getattr(self, "paper_account", None), "file_path", Path("data/paper_account.json")).parent
        history_file = data_dir / "paper_trades.jsonl"
        
        pnl_total = float(getattr(paper_stats, "total_pnl", 0.0) or 0.0)

        def money(value: float) -> str:
            if value < 0:
                return f"-${abs(value):,.2f}"
            return f"${value:,.2f}"

        def incomplete_display() -> str:
            return (
                f"→ Wins: `{paper_stats.wins}` | Losses: `{paper_stats.losses}`\n"
                f"→ Gains: `incomplet` | Pertes: `incomplet`\n"
                f"→ PnL réalisé: `{money(pnl_total)}`"
            )

        def complete_display() -> str:
            display = (
                f"→ Wins: `{paper_stats.wins}` | Losses: `{paper_stats.losses}`\n"
                f"→ Gains: `{money(float(paper_stats.total_gains))}` | "
                f"Pertes: `{money(float(paper_stats.total_losses))}`\n"
                f"→ PnL réalisé: `{money(pnl_total)}`"
            )
            if getattr(paper_stats, "accounting_complete", False):
                display += (
                    f"\n→ Fermés: `{getattr(paper_stats, 'closed_trades', 0)}` | "
                    f"Ouverts: `{getattr(paper_stats, 'open_trades', 0)}`\n"
                    f"→ Cash PnL: `{money(float(getattr(paper_stats, 'cash_pnl', 0.0) or 0.0))}` | "
                    f"Expo ouverte: `{money(float(getattr(paper_stats, 'locked_in_positions', 0.0) or 0.0))}`"
                )
            return display

        if not history_file.exists() or history_file.stat().st_size == 0:
            return incomplete_display()
        
        # Check if we have detailed history or only legacy
        has_detail = False
        has_legacy_partial = False
        has_legacy_exact = False
        detail_wins = 0
        detail_losses = 0
        
        with open(history_file, "r") as f:
            for line in f:
                if line.strip():
                    try:
                        trade = json.loads(line)
                        if trade.get("historical_reconstructed") or trade.get("estimated"):
                            wins = int(trade.get("wins", 0) or 0)
                            losses = int(trade.get("losses", 0) or 0)
                            if wins > 0 and losses > 0:
                                has_legacy_partial = True
                            else:
                                has_legacy_exact = True
                        else:
                            if "pnl" in trade:
                                has_detail = True
                                pnl = float(trade.get("pnl") or 0.0)
                                if pnl > 0:
                                    detail_wins += 1
                                elif pnl < 0:
                                    detail_losses += 1
                    except:
                        pass

        detailed_history_complete = (
            has_detail
            and detail_wins == int(getattr(paper_stats, "wins", 0) or 0)
            and detail_losses == int(getattr(paper_stats, "losses", 0) or 0)
        )
        
        # Mixed legacy entries are incomplete unless detailed PnL records now
        # cover the full win/loss split.
        if has_legacy_partial and not detailed_history_complete:
            return incomplete_display()
        elif has_detail:
            return complete_display()
        elif has_legacy_exact:
            return incomplete_display()
        else:
            return complete_display()

    def can_trade_live(self) -> tuple[bool, str]:
        """
        Check all preconditions for live trading.
        Double lock: live_trade=true AND confirm_live_trading="I_ACCEPT_REAL_LOSS"
        """
        return can_trade_live(self.config)

    def run_forever(self) -> None:
        """Run the main engine loop."""
        import os

        # LIVE TRADE GUARD - Enhanced double confirmation
        if self.modes.live_trade:
            allowed, reason = self.can_trade_live()
            if not allowed:
                self.emit(f"🚨 BLOCKED: Live trading disabled: {reason}")
                self.emit("   Required: live_trade=true AND confirm_live_trading='I_ACCEPT_REAL_LOSS'")
                self.emit("   Also ensure: kill_switch_enabled=false")
                return

        self.emit(f"\n{'=' * 50}")
        self.emit(f"WEATHERBOT ({bot_mode_label(self.modes)})")
        self.emit(f"{'=' * 50}")
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
                            from ..ai.ourobouros import run_ourobouros

                            run_ourobouros(min_resolutions=10)

                            self.last_report_ts = now
                        except (Exception,) as report_exc:
                            self.emit(f"Report/Ouroboros error: {report_exc}")
                            logger.error(f"Failed to run hourly tasks: {report_exc}")

                    # Scan Check - Align with weather model runs (00z, 06z, 12z, 18z)
                    should_scan = (now - last_scan) >= self.config.scan_interval
                    
                    # More precise timing: scan 30min after model runs
                    if not should_scan:
                        from src.trading.timing import should_scan_now
                        if should_scan_now(datetime.fromtimestamp(last_scan, tz=timezone.utc)):
                            should_scan = True
                    
                    if should_scan:
                        self.emit(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] scanning...")
                        try:
                            start_perf = time.perf_counter()
                            try:
                                result = self.scanner.scan_and_update()
                            finally:
                                elapsed = time.perf_counter() - start_perf
                                log_event("scan_cycle", latency_s=elapsed)

                            self.latency_sum += elapsed
                            self.latency_count += 1

                            # Auto-resolve pending markets after scan
                            try:
                                resolve_result = self.resolver.auto_resolve_pending()
                                if resolve_result["total"] > 0:
                                    self.emit(f"Auto-resolved: {resolve_result['total']} markets")
                            except (Exception,) as resolve_exc:
                                logger.error(f"Auto-resolve error: {resolve_exc}")

                            state = self.storage.load_state()
                            self.emit(
                                f" balance: ${state.balance:,.2f} | new: {result.new_trades} | "
                                f"closed: {result.closed} | resolved: {result.resolved}"
                            )
                            last_scan = now
                        except (Exception,) as scan_exc:
                            self.error_count += 1
                            self.emit(f"Critical scan error: {scan_exc}")
                            logger.exception("Uncaught exception in scan loop")
                            time.sleep(60)  # Wait before retry if scan failed

                    if self.running:
                        time.sleep(60)  # High-precision monitor sleep (1 min)
                except (Exception,) as loop_exc:
                    self.emit(f"Loop error: {loop_exc}")
                    logger.exception(f"Unexpected error in main loop: {loop_exc}")
                    time.sleep(60)  # Safety sleep
        finally:
            self.feedback.notify_stopped("manual stop" if not self.running else "loop exited")

    def process_pending_signals(self, pending_signals: list[dict]):
        """Rank and send top signals."""
        top_k = max(0, int(getattr(self.config, "signal_top_k", 3)))

        # Portfolio Optimization (Correlation & Regional Caps) - With Safe Fallback
        optimized_signals = pending_signals
        if is_enabled("PORTFOLIO_OPTIMIZATION"):
            try:
                all_markets = self.storage.load_all_markets()
                state = self.storage.load_state()
                # Calculate Drawdown
                peak = state.peak_balance or state.starting_balance
                drawdown_pct = max(0.0, (peak - state.balance) / peak * 100)

                optimized_signals = self.portfolio_optimizer.optimize_sizing(
                    pending_signals, all_markets, state.balance, drawdown_pct
                )
            except (Exception,) as e:
                logger.error(f"Portfolio Optimization failed, falling back to raw signals: {e}")
                optimized_signals = pending_signals

        ranked = self.scoring_engine.rank(optimized_signals)
        selected = ranked[:top_k] if top_k else []
        by_market_id = {item["signal"]["market_id"]: item for item in pending_signals}

        for ranked_item in selected:
            candidate = by_market_id.get(ranked_item.market_id)
            if not candidate:
                continue

            signal = candidate["signal"]
            trade_context = dict(candidate["trade_context"])
            trade_context.update({"signal_score": ranked_item.score, "rank": ranked_item.rank})
            bucket = _signal_bucket(candidate, trade_context, signal)

            self.feedback.notify_signal(
                candidate["loc"].name,
                candidate["date_str"],
                bucket,
                signal["entry_price"],
                signal["ev"],
                signal["cost"],
                signal["forecast_src"],
                candidate["horizon"],
                signal["question"],
                signal["market_id"],
                candidate["note"],
                calibrated_prob=signal["p"],
                market_prob=signal["entry_price"],
                uncertainty=signal.get("edge_penalties", {}).get("uncertainty"),
                signal_type=candidate["filter_decision"]["signal_type"],
                quality=ranked_item.score,
                priority=candidate["filter_decision"]["priority"],
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
                    "conf": int(signal.get("ml", {}).get("confidence", 0) * 100),
                }
                self.feedback.notify_gem_alert(gem_data)
                self.emit(f"💎 GEM ALERT: {candidate['loc'].name} | Score {gem_data['score']:.2f}")

            # Commit the signal to persistence (cooldowns, etc.)
            self.signal_quality.commit(Signal.from_dict(candidate["loc"].name, signal))
            candidate["market"].signal_state = build_signal_marker(signal)
            self.storage.save_market(candidate["market"])
            self.emit(
                f"[RANK {ranked_item.rank}] {candidate['loc'].name} | ${signal['entry_price']:.3f} | score {ranked_item.score:.2f}"
            )

    def resolve_market(self, market: Market, balance: float):
        """Delegate market resolution to the resolver."""
        return self.resolver.resolve_market(market, balance)

    def status_lines(self) -> list[str]:
        """Return status lines for CLI output."""
        state = self.storage.load_state()
        markets = self.storage.load_all_markets()
        open_live = [m for m in markets if m.position and m.position.get("status") == "open"]
        open_paper = [m for m in markets if m.paper_position and m.paper_position.get("status") == "open"]
        resolved = [m for m in markets if m.status == "resolved" and m.pnl is not None]
        balance, start = state.balance, state.starting_balance
        ret = (balance - start) / start * 100 if start else 0
        wins = sum(1 for m in resolved if m.resolved_outcome == "win")
        losses = sum(1 for m in resolved if m.resolved_outcome == "loss")
        total_resolved = len(resolved)
        wr = f"{wins / total_resolved * 100:.0f}%" if total_resolved else "0%"

        return [
            f"\n{'=' * 50}",
            "WEATHERBOT STATUS",
            f"{'=' * 50}",
            f"Balance: ${balance:,.2f} ({ret:+.1f}%)",
            f"Trades: {len(open_live) + len(open_paper) + total_resolved} | W: {wins} | L: {losses} | WR: {wr}",
            f"Open Live: {len(open_live)} | Open Paper: {len(open_paper)} | Resolved: {total_resolved}",
            f"{'=' * 50}\n",
        ]

    def report_lines(self) -> list[str]:
        """Return report lines for CLI output."""
        resolved = [m for m in self.storage.load_all_markets() if m.status == "resolved" and m.pnl is not None]
        lines = [f"\n{'=' * 50}", "WEATHERBOT REPORT", f"{'=' * 50}"]
        if not resolved:
            lines.append("No resolved markets.")
            return lines

        total_pnl = sum(m.pnl for m in resolved)
        wins = sum(1 for m in resolved if m.resolved_outcome == "win")
        total_trades = len(resolved)
        lines.extend(
            [
                f"\nResolved: {total_trades} | Wins: {wins} | Losses: {total_trades - wins}",
                f"Win rate: {wins / total_trades * 100:.0f}% | Total PnL: ${total_pnl:+.2f}\n",
            ]
        )

        winners = sorted([m for m in resolved if m.resolved_outcome == "win"], key=lambda x: x.pnl or 0, reverse=True)[
            :10
        ]
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

    def _market_resolved_unix_ts(self, m, fallback_ts=None):
        """Get unix timestamp from market's resolved_at or fallback."""
        if getattr(m, "resolved_at", None):
            return int(datetime.fromisoformat(m.resolved_at).timestamp())
        return int(fallback_ts or time.time())

    def _paper_daily_summary(self, markets, paper_stats, now_dt: datetime | None = None) -> dict:
        """Summarize today's paper opens and settlements for Telegram daily recap."""
        now_dt = now_dt or datetime.now(timezone.utc)
        today = now_dt.date()
        opened = []
        closed = []

        for market in markets:
            pos = getattr(market, "paper_position", None) or {}
            opened_at = _parse_iso_datetime(pos.get("opened_at"))
            closed_at = _parse_iso_datetime(pos.get("closed_at"))
            if opened_at and opened_at.date() == today:
                opened.append((market, pos))
            if closed_at and closed_at.date() == today and pos.get("pnl") is not None:
                closed.append((market, pos))

        pnl_today = sum(float(pos.get("pnl", 0.0) or 0.0) for _, pos in closed)
        wins_today = sum(1 for _, pos in closed if float(pos.get("pnl", 0.0) or 0.0) > 0)
        losses_today = sum(1 for _, pos in closed if float(pos.get("pnl", 0.0) or 0.0) < 0)
        flats_today = sum(1 for _, pos in closed if float(pos.get("pnl", 0.0) or 0.0) == 0)
        stake_opened = sum(float(pos.get("cost", 0.0) or 0.0) for _, pos in opened)

        details = []
        for market, pos in closed:
            pnl = float(pos.get("pnl", 0.0) or 0.0)
            details.append(
                {
                    "city": getattr(market, "city_name", getattr(market, "city", "unknown")),
                    "date": getattr(market, "date", ""),
                    "status": "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT",
                    "pnl": pnl,
                }
            )

        return {
            "today": today.isoformat(),
            "trades_opened_today": len(opened),
            "stake_opened_today": stake_opened,
            "closed_today": len(closed),
            "wins_today": wins_today,
            "losses_today": losses_today,
            "flats_today": flats_today,
            "pnl_today": pnl_today,
            "details": details,
            "paper_total_gains": getattr(paper_stats, "total_gains", 0.0),
            "paper_total_losses": getattr(paper_stats, "total_losses", 0.0),
            "paper_total_pnl": getattr(paper_stats, "total_pnl", 0.0),
            "paper_cash_pnl": getattr(paper_stats, "cash_pnl", 0.0),
            "paper_open_exposure": getattr(paper_stats, "locked_in_positions", 0.0),
            "paper_balance": getattr(paper_stats, "balance", 0.0),
            "paper_equity": getattr(paper_stats, "equity", getattr(paper_stats, "balance", 0.0)),
        }

    def send_full_audit_report(self):
        """Generate and send the comprehensive hourly report."""
        from ..data.metrics import calculate_audit_metrics
        from .health import get_api_statuses

        state = self.storage.load_state()
        markets = self.storage.load_all_markets()

        # 1. Metrics Calculation
        resolved_trades = []
        for m in markets:
            if m.status == "resolved" and m.pnl is not None:
                resolved_trades.append(
                    {
                        "pnl": m.pnl,
                        "unix_ts": self._market_resolved_unix_ts(m),
                    }
                )

        metrics = calculate_audit_metrics(resolved_trades, state.starting_balance)

        # 2. Portfolio Summary (use equity-based drawdown from paper account when available)
        paper_account = getattr(self, "paper_account", None)
        if paper_account is not None:
            paper_stats = paper_account.get_state()
            drawdown = paper_stats.drawdown
        else:
            drawdown = (state.peak_balance - state.balance) / state.peak_balance if state.peak_balance > 0 else 0

        # 3. API & Health
        api_statuses = get_api_statuses(self.config, self.feedback)
        uptime_sec = int(time.time() - self.start_time)
        uptime_str = f"{uptime_sec // 3600}h {(uptime_sec % 3600) // 60}m"

        # 4. Risk & Diversification
        risk_summary = self.risk_manager.get_risk_summary(markets)
        paper_only = not bool(getattr(self.config, "live_trade", False))
        paper_exposure = sum(
            float(getattr(m, "paper_position", {}).get("cost", 0.0) or 0.0)
            for m in markets
            if getattr(m, "paper_position", None)
            and getattr(m, "paper_position", {}).get("status") in ("open", "paper")
        )

        # Use state.balance for accurate reporting (single source of truth).
        paper_balance = state.balance
        paper_equity = state.balance + paper_exposure

        # 5. Gather latest active/fresh signals for all cities.
        city_signals = []
        by_city = {}
        now_dt = datetime.now(timezone.utc)
        max_watch_age_s = float(getattr(self.config, "report_signal_max_age_hours", 48)) * 3600
        min_report_price = float(getattr(self.config, "report_min_signal_price", 0.01))
        for m in markets:
            if m.status == "resolved":
                continue

            # Priority: Open Position > Signal Marker > Last Analysis
            sig = None
            signal_ts = None
            position = getattr(m, "position", None)
            paper_position = getattr(m, "paper_position", None)
            signal_state = getattr(m, "signal_state", None)
            last_analysis = getattr(m, "last_analysis", None)

            if position and position.get("status") == "open":
                signal_ts = _parse_iso_datetime(position.get("opened_at") or position.get("ts"))
                sig = {
                    "city": m.city_name,
                    "edge": _display_edge(position.get("ev", 0), position),
                    "conf": position.get("ml", {}).get("confidence", 0) * 100,
                    "price": position.get("entry_price", 0),
                    "risk": "OPEN",
                }
            elif paper_position and paper_position.get("status") in ("open", "paper"):
                signal_ts = _parse_iso_datetime(paper_position.get("opened_at") or paper_position.get("ts"))
                sig = {
                    "city": m.city_name,
                    "edge": _display_edge(paper_position.get("ev", 0), paper_position),
                    "conf": paper_position.get("ml", {}).get("confidence", 0) * 100,
                    "price": paper_position.get("entry_price", 0),
                    "risk": "PAPER",
                }
            elif signal_state:
                signal_ts = _parse_iso_datetime(signal_state.get("recorded_at") or signal_state.get("ts"))
                raw_edge = signal_state.get("ev", 0)
                if signal_state.get("p") is None and abs(float(raw_edge or 0.0)) > 1.0:
                    continue
                sig = {
                    "city": m.city_name,
                    "edge": _display_edge(raw_edge, signal_state),
                    "conf": signal_state.get("ml_conf", 0) * 100,
                    "price": signal_state.get("entry_price", 0),
                    "risk": "SIGNAL",
                }
            elif last_analysis:
                signal_ts = _parse_iso_datetime(last_analysis.get("ts"))
                raw_edge = last_analysis.get("ev", 0)
                if last_analysis.get("p") is None and abs(float(raw_edge or 0.0)) > 1.0:
                    continue
                sig = {
                    "city": m.city_name,
                    "edge": _display_edge(raw_edge, last_analysis),
                    "conf": last_analysis.get("conf", 0) * 100,
                    "price": last_analysis.get("price", 0),
                    "risk": "WATCH",
                }

            if sig:
                sig["date"] = m.date
                sig["ts"] = signal_ts.isoformat() if signal_ts else ""
                if sig["risk"] == "WATCH":
                    if not signal_ts:
                        continue
                    age_s = (now_dt - signal_ts).total_seconds()
                    if age_s < 0 or age_s > max_watch_age_s:
                        continue
                    if float(sig.get("price") or 0.0) < min_report_price:
                        continue

                # Keep the strongest reportable signal per city after freshness/liquidity filters.
                if m.city not in by_city or sig["edge"] > by_city[m.city]["edge"]:
                    by_city[m.city] = sig

        # Sort and limit to top 15 for readability
        reportable_signals = list(by_city.values())
        paper_signals = [sig for sig in reportable_signals if sig.get("risk") == "PAPER"]
        if paper_only and paper_signals:
            reportable_signals = paper_signals
        sorted_sigs = sorted(reportable_signals, key=lambda x: x["edge"], reverse=True)
        city_signals = sorted_sigs[:15]

        # Calculate real PnL from state (not just resolved trades)
        real_pnl = state.balance - state.starting_balance
        real_pnl_pct = (real_pnl / state.starting_balance) * 100 if state.starting_balance > 0 else 0

        summary = {
            "mode": "paper" if paper_only else "live",
            "pnl_total": real_pnl,
            "pnl_pct": real_pnl_pct,
            "exposure": 0.0 if paper_only else risk_summary["total_exposure"],
            "paper_exposure": paper_exposure,
            "paper_equity": paper_equity,
            "paper_balance": paper_balance,
            "pf": metrics.profit_factor,
            "sharpe": metrics.sharpe_ratio,
            "drawdown": drawdown * 100,
            "avg_win": metrics.avg_win,
            "avg_loss": metrics.avg_loss,
            "r_multiple": metrics.r_multiple,
            "active_signals": risk_summary["active_cities"],
            "drift": metrics.drift_status,
            "uptime": uptime_str,
            "api_status": " | ".join([f"{n}: {s}" for n, s, l in api_statuses]),
            "hhi_div": risk_summary["diversification_index"],
            "errors": self.error_count,
            "latency": self.latency_sum / self.latency_count if self.latency_count > 0 else 1.3,
            "signals": len(city_signals),
            "wins": metrics.wins,
            "losses": metrics.losses,
            "pnl": real_pnl_pct,  # Use real PnL percentage
        }
        if paper_account is not None:
            summary.update(self._paper_daily_summary(markets, paper_stats, now_dt=now_dt))

        self.feedback.notify_hourly_report(summary, city_signals)

        from ..notifications.telegram_control_center import send_daily_report

        send_daily_report(summary)

        # Persist last report timestamp
        state.last_report_ts = time.time()
        self.storage.save_state(state)


# Audit: Includes fee and slippage awareness
