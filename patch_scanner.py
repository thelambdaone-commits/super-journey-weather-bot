#!/usr/bin/env python3
"""Patch scanner.py with CLOB guard, volume filter, spread filter."""
from pathlib import Path

scanner_path = Path("src/trading/scanner.py")
content = scanner_path.read_text(encoding="utf-8")

# --- Patch 1: Add volume filter after event_slug line ---
old_block_1 = '''                event_slug = event.get("slug", "")

                outcomes = get_outcomes(event)
                for outcome in outcomes:
                    if not refresh_outcome_orderbook(outcome):
                        continue
                    best_ask = float(outcome["ask"])'''

new_block_1 = '''                event_slug = event.get("slug", "")

                # Filter 1: Minimum volume
                market_volume_raw = event.get("volume", 0) or 0
                try:
                    market_volume = float(market_volume_raw)
                except (ValueError, TypeError):
                    market_volume = 0.0

                min_volume = float(getattr(self.engine.config, "min_volume", 500))
                if market_volume < min_volume:
                    self.engine.emit(
                        f"[LOW-VOL] {loc.name} {date_str} | Vol: ${market_volume:.0f} < ${min_volume:.0f}"
                    )
                    continue

                outcomes = get_outcomes(event)
                for outcome in outcomes:
                    # CLOB guard for paper mode
                    if self.engine.modes.live_trade:
                        if not refresh_outcome_orderbook(outcome):
                            continue
                    else:
                        self.engine.emit("[PAPER] skipped CLOB refresh, using Gamma prices")

                    # Safety check bid/ask
                    bid = float(outcome.get("best_bid", 0) or 0)
                    ask = float(outcome.get("best_ask", 0) or 0)

                    if bid <= 0 or ask <= 0:
                        self.engine.emit(f"[NO-LIQUIDITY] {loc.name}")
                        continue

                    # Filter 2: Maximum spread (corrected formula)
                    spread = (ask - bid) / ((ask + bid) / 2)
                    max_spread = getattr(self.engine.config, "max_spread", 0.05)

                    if spread > max_spread:
                        self.engine.emit(
                            f"[HIGH-SPREAD] {loc.name} | Spread: {spread:.2%} > {max_spread:.0%}"
                        )
                        continue

                    best_ask = ask'''

if old_block_1 in content:
    content = content.replace(old_block_1, new_block_1)
    print("Patch 1 (CLOB guard + volume + spread) applied")
else:
    print("ERROR: Patch 1 failed - block not found")
    print("Searching for similar content...")
    if 'event_slug = event.get("slug", "")' in content:
        print("  - event_slug line found")
    if 'if not refresh_outcome_orderbook(outcome):' in content:
        print("  - refresh_outcome_orderbook line found")

# Write back
scanner_path.write_text(content, encoding="utf-8")
print("scanner.py updated")
