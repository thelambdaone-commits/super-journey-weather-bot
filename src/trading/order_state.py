"""
Order lifecycle states for Polymarket CLOB.

States: PENDING -> PARTIAL/FILLED -> CANCELLED/FAILED/EXPIRED
Partial fills: only the unfilled remainder can be cancelled.
"""

from __future__ import annotations
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime


class OrderState(Enum):
    """Order lifecycle states matching Polymarket CLOB."""
    PENDING = "PENDING"        # Submitted, awaiting fill
    PARTIAL = "PARTIAL"        # Partially filled (unfilled remainder can be cancelled)
    FILLED = "FILLED"          # Completely filled
    CANCELLED = "CANCELLED"    # Cancelled (remainder of a PARTIAL)
    FAILED = "FAILED"          # Rejected by exchange
    EXPIRED = "EXPIRED"        # GTD order expired


@dataclass
class Order:
    """Represents a single order with full lifecycle tracking."""
    order_id: str
    token_id: str
    market_id: str
    side: str  # "BUY" or "SELL"
    price: float
    size: float
    order_type: str  # "FOK", "FAK", "GTD", "LIMIT"
    state: OrderState = OrderState.PENDING
    filled_size: float = 0.0
    filled_avg_price: float = 0.0
    remaining_size: float = field(init=False)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    error_reason: Optional[str] = None
    clob_response: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.remaining_size = self.size - self.filled_size

    @property
    def is_active(self) -> bool:
        """Order is still active (can be filled or cancelled)."""
        return self.state in (OrderState.PENDING, OrderState.PARTIAL)

    @property
    def is_terminal(self) -> bool:
        """Order reached a final state."""
        return self.state in (OrderState.FILLED, OrderState.CANCELLED, OrderState.FAILED, OrderState.EXPIRED)

    @property
    def fill_percentage(self) -> float:
        """Percentage of order filled."""
        if self.size <= 0:
            return 0.0
        return (self.filled_size / self.size) * 100.0

    def update_fill(self, filled_size: float, fill_price: float) -> None:
        """Update order with fill information."""
        self.filled_size = filled_size
        self.filled_avg_price = fill_price
        self.remaining_size = self.size - self.filled_size
        self.updated_at = datetime.utcnow().isoformat()

        if self.filled_size >= self.size:
            self.state = OrderState.FILLED
        elif self.filled_size > 0:
            self.state = OrderState.PARTIAL

    def mark_cancelled(self) -> None:
        """Mark order as cancelled (only valid for PARTIAL)."""
        if self.state == OrderState.PARTIAL:
            self.state = OrderState.CANCELLED
            self.updated_at = datetime.utcnow().isoformat()

    def mark_failed(self, reason: str) -> None:
        """Mark order as failed/rejected."""
        self.state = OrderState.FAILED
        self.error_reason = reason
        self.updated_at = datetime.utcnow().isoformat()

    def mark_expired(self) -> None:
        """Mark GTD order as expired."""
        self.state = OrderState.EXPIRED
        self.updated_at = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "order_id": self.order_id,
            "token_id": self.token_id,
            "market_id": self.market_id,
            "side": self.side,
            "price": self.price,
            "size": self.size,
            "order_type": self.order_type,
            "state": self.state.value,
            "filled_size": self.filled_size,
            "filled_avg_price": self.filled_avg_price,
            "remaining_size": self.remaining_size,
            "fill_percentage": self.fill_percentage,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "error_reason": self.error_reason,
        }


class OrderTracker:
    """Tracks all orders and their states."""

    def __init__(self):
        self.orders: Dict[str, Order] = {}

    def add_order(self, order: Order) -> None:
        """Register a new order."""
        self.orders[order.order_id] = order

    def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID."""
        return self.orders.get(order_id)

    def update_from_clob_response(self, order_id: str, response: Dict[str, Any]) -> None:
        """Update order state from CLOB response."""
        order = self.orders.get(order_id)
        if not order:
            return

        # Parse CLOB response
        status = response.get("status", "").upper()
        if status == "FILLED":
            order.update_fill(order.size, float(response.get("avgPrice", order.price)))
        elif status == "PARTIALLY_FILLED":
            filled = float(response.get("filledSize", 0))
            price = float(response.get("avgPrice", order.price))
            order.update_fill(filled, price)
        elif status == "CANCELLED":
            order.mark_cancelled()
        elif status == "FAILED":
            order.mark_failed(response.get("reason", "unknown"))
        elif status == "EXPIRED":
            order.mark_expired()

    def get_active_orders(self) -> Dict[str, Order]:
        """Get all active orders."""
        return {oid: o for oid, o in self.orders.items() if o.is_active}

    def get_terminal_orders(self) -> Dict[str, Order]:
        """Get all terminal orders."""
        return {oid: o for oid, o in self.orders.items() if o.is_terminal}
