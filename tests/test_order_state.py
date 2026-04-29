"""
Tests for order_state.py - Order lifecycle management.
"""

import unittest
from src.trading.order_state import OrderState, Order, OrderTracker


class TestOrderState(unittest.TestCase):

    def test_pending_initial_state(self):
        order = Order(
            order_id="test_1",
            token_id="token_1",
            market_id="market_1",
            side="BUY",
            price=0.70,
            size=100.0,
            order_type="FOK",
        )
        self.assertEqual(order.state, OrderState.PENDING)
        self.assertTrue(order.is_active)
        self.assertFalse(order.is_terminal)

    def test_full_fill(self):
        order = Order(
            order_id="test_2",
            token_id="token_2",
            market_id="market_2",
            side="BUY",
            price=0.70,
            size=100.0,
            order_type="FOK",
        )
        order.update_fill(filled_size=100.0, fill_price=0.70)
        self.assertEqual(order.state, OrderState.FILLED)
        self.assertFalse(order.is_active)
        self.assertTrue(order.is_terminal)
        self.assertEqual(order.fill_percentage, 100.0)

    def test_partial_fill(self):
        order = Order(
            order_id="test_3",
            token_id="token_3",
            market_id="market_3",
            side="BUY",
            price=0.70,
            size=100.0,
            order_type="FAK",
        )
        order.update_fill(filled_size=40.0, fill_price=0.71)
        self.assertEqual(order.state, OrderState.PARTIAL)
        self.assertTrue(order.is_active)
        self.assertEqual(order.filled_size, 40.0)
        self.assertEqual(order.remaining_size, 60.0)
        self.assertEqual(order.fill_percentage, 40.0)

    def test_partial_cancel(self):
        order = Order(
            order_id="test_4",
            token_id="token_4",
            market_id="market_4",
            side="BUY",
            price=0.70,
            size=100.0,
            order_type="FAK",
        )
        order.update_fill(filled_size=30.0, fill_price=0.70)
        order.mark_cancelled()
        self.assertEqual(order.state, OrderState.CANCELLED)
        self.assertFalse(order.is_active)
        self.assertTrue(order.is_terminal)

    def test_mark_failed(self):
        order = Order(
            order_id="test_5",
            token_id="token_5",
            market_id="market_5",
            side="BUY",
            price=0.70,
            size=100.0,
            order_type="LIMIT",
        )
        order.mark_failed("insufficient_funds")
        self.assertEqual(order.state, OrderState.FAILED)
        self.assertEqual(order.error_reason, "insufficient_funds")

    def test_mark_expired(self):
        order = Order(
            order_id="test_6",
            token_id="token_6",
            market_id="market_6",
            side="BUY",
            price=0.70,
            size=100.0,
            order_type="GTD",
        )
        order.mark_expired()
        self.assertEqual(order.state, OrderState.EXPIRED)

    def test_to_dict(self):
        order = Order(
            order_id="test_7",
            token_id="token_7",
            market_id="market_7",
            side="BUY",
            price=0.70,
            size=100.0,
            order_type="FOK",
        )
        d = order.to_dict()
        self.assertEqual(d["order_id"], "test_7")
        self.assertEqual(d["state"], "PENDING")
        self.assertEqual(d["fill_percentage"], 0.0)


class TestOrderTracker(unittest.TestCase):

    def setUp(self):
        self.tracker = OrderTracker()

    def test_add_and_get_order(self):
        order = Order(
            order_id="track_1",
            token_id="token_1",
            market_id="market_1",
            side="BUY",
            price=0.70,
            size=100.0,
            order_type="FOK",
        )
        self.tracker.add_order(order)
        retrieved = self.tracker.get_order("track_1")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.order_id, "track_1")

    def test_get_active_orders(self):
        o1 = Order("o1", "t1", "m1", "BUY", 0.70, 100.0, "FOK")
        o1.update_fill(100.0, 0.70)  # Filled = terminal
        o2 = Order("o2", "t2", "m2", "BUY", 0.70, 100.0, "FAK")
        o2.update_fill(50.0, 0.70)  # Partial = active

        self.tracker.add_order(o1)
        self.tracker.add_order(o2)

        active = self.tracker.get_active_orders()
        self.assertEqual(len(active), 1)
        self.assertIn("o2", active)

    def test_get_terminal_orders(self):
        o1 = Order("o1", "t1", "m1", "BUY", 0.70, 100.0, "FOK")
        o1.update_fill(100.0, 0.70)  # Filled
        o2 = Order("o2", "t2", "m2", "BUY", 0.70, 100.0, "FAK")  # Still pending

        self.tracker.add_order(o1)
        self.tracker.add_order(o2)

        terminal = self.tracker.get_terminal_orders()
        self.assertEqual(len(terminal), 1)
        self.assertIn("o1", terminal)


if __name__ == "__main__":
    unittest.main()
