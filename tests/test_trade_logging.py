"""
Tests for structured JSONL trade logging.
Verifies all 6 actions are logged: BUY/SKIP/WAIT/REPRICE/CANCEL/REDUCE_SIZE.
"""

import unittest
import tempfile
import os
import json
from src.trading.decision import TradeDecision, log_decision_jsonl


class TestTradeLogging(unittest.TestCase):
    """Test that all decision types are logged to JSONL."""

    def setUp(self):
        self.tmpfile = tempfile.NamedTemporaryFile(
            mode='w', delete=False, suffix='.jsonl'
        )
        self.tmpfile.close()
        self.filepath = self.tmpfile.name

    def tearDown(self):
        if os.path.exists(self.filepath):
            os.unlink(self.filepath)

    def _create_decision(self, action: str, net_ev: float = 0.08) -> TradeDecision:
        return TradeDecision(
            market_id="mkt_1",
            event_slug="test-event",
            location="london",
            date="2026-01-01",
            outcome="Temp > 15°C",
            model_probability=0.80,
            market_bid=0.65,
            market_ask=0.70,
            entry_price=0.70,
            spread=0.05,
            volume=1000.0,
            gross_edge=0.10,
            net_ev=net_ev,
            suggested_size=50.0 if action == "BUY" else 0.0,
            action=action,
            passed_filters=(action == "BUY"),
            rejected_reason="" if action == "BUY" else f"test_{action.lower()}",
        )

    def _count_lines(self) -> int:
        if not os.path.exists(self.filepath):
            return 0
        with open(self.filepath, 'r') as f:
            return len([line for line in f if line.strip()])

    def _read_last_line(self) -> dict:
        with open(self.filepath, 'r') as f:
            lines = [line for line in f if line.strip()]
            return json.loads(lines[-1]) if lines else {}

    def test_log_buy(self):
        """BUY decisions should be logged with action=BUY."""
        decision = self._create_decision("BUY")
        log_decision_jsonl(decision, filepath=self.filepath)
        
        self.assertEqual(self._count_lines(), 1)
        data = self._read_last_line()
        self.assertEqual(data["action"], "BUY")
        self.assertEqual(data["location"], "london")
        self.assertIn("timestamp", data)

    def test_log_skip(self):
        """SKIP decisions should be logged."""
        decision = self._create_decision("SKIP", net_ev=-0.02)
        log_decision_jsonl(decision, filepath=self.filepath)
        
        self.assertEqual(self._count_lines(), 1)
        data = self._read_last_line()
        self.assertEqual(data["action"], "SKIP")
        self.assertIn("rejected_reason", data)

    def test_log_wait(self):
        """WAIT decisions should be logged (not terminal)."""
        decision = self._create_decision("WAIT")
        log_decision_jsonl(decision, filepath=self.filepath)
        
        data = self._read_last_line()
        self.assertEqual(data["action"], "WAIT")

    def test_log_reprice(self):
        """REPRICE decisions should be logged."""
        decision = self._create_decision("REPRICE")
        log_decision_jsonl(decision, filepath=self.filepath)
        
        data = self._read_last_line()
        self.assertEqual(data["action"], "REPRICE")

    def test_log_cancel(self):
        """CANCEL decisions should be logged."""
        decision = self._create_decision("CANCEL")
        log_decision_jsonl(decision, filepath=self.filepath)
        
        data = self._read_last_line()
        self.assertEqual(data["action"], "CANCEL")

    def test_log_reduce_size(self):
        """REDUCE_SIZE decisions should be logged."""
        decision = self._create_decision("REDUCE_SIZE")
        log_decision_jsonl(decision, filepath=self.filepath)
        
        data = self._read_last_line()
        self.assertEqual(data["action"], "REDUCE_SIZE")

    def test_log_multiple_decisions(self):
        """Multiple decisions should append to same file."""
        for action in ["BUY", "SKIP", "WAIT", "REPRICE", "CANCEL", "REDUCE_SIZE"]:
            decision = self._create_decision(action)
            log_decision_jsonl(decision, filepath=self.filepath)
        
        self.assertEqual(self._count_lines(), 6)

    def test_integration_with_scanner(self):
        """Simulate scanner calling log_decision_jsonl."""
        # This is what scanner.py now does after decision = self.decision_engine.evaluate(context)
        decision = self._create_decision("BUY")
        log_decision_jsonl(decision, filepath=self.filepath)
        
        # Verify file can be read back for paper trading analysis
        with open(self.filepath, 'r') as f:
            line = f.readline()
            data = json.loads(line)
            self.assertEqual(data["action"], "BUY")
            self.assertGreater(data["net_ev"], 0)
            self.assertGreater(data["suggested_size"], 0)


if __name__ == '__main__':
    unittest.main()
