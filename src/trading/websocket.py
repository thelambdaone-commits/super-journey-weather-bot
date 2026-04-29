"""
Real-time CLOB WebSocket Listener for orderbook updates.

Polymarket recommends WebSocket for real-time updates via market channel.
This replaces polling for live trading with zero-latency orderbook state.
"""

from __future__ import annotations
import json
import logging
import threading
import time
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False
    logger.warning("websocket-client not installed. WebSocket disabled.")


class ClobWebSocketListener:
    """
    WebSocket client for real-time orderbook updates.
    Uses Polymarket market channel for orderbook deltas.
    """

    def __init__(self, host: str, token_ids: List[str], reconnect_delay: int = 5):
        self.host = host.replace("https://", "wss://").replace("http://", "ws://")
        self.token_ids = set(token_ids)
        self.reconnect_delay = reconnect_delay
        self.orderbooks: Dict[str, Dict[str, Any]] = {}
        self.orderbook_lock = threading.Lock()
        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._ws: Optional[Any] = None
        self._last_heartbeat: float = 0.0

    def start(self) -> None:
        """Start the listener in a background thread."""
        if not WEBSOCKET_AVAILABLE:
            logger.error("Cannot start WebSocket: websocket-client not installed.")
            return
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="ClobWebSocket")
        self._thread.start()
        logger.info(f"WebSocket CLOB Listener started for {len(self.token_ids)} tokens.")

    def _run(self) -> None:
        """Main WebSocket loop with reconnection."""
        while self.running:
            try:
                ws_url = f"{self.host}/ws"
                logger.info(f"Connecting to WebSocket: {ws_url}")
                self._ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")

            if self.running:
                logger.info(f"Reconnecting in {self.reconnect_delay}s...")
                time.sleep(self.reconnect_delay)

    def _on_open(self, ws) -> None:
        """Subscribe to market channel for each token."""
        logger.info("WebSocket connected. Subscribing to tokens...")
        for token_id in self.token_ids:
            subscribe_msg = {
                "type": "SUBSCRIBE",
                "channel": "market",
                "token_id": token_id,
            }
            ws.send(json.dumps(subscribe_msg))
            logger.debug(f"Subscribed to token {token_id}")

    def _on_message(self, ws, message: str) -> None:
        """Handle incoming orderbook updates."""
        try:
            data = json.loads(message)
            msg_type = data.get("type", "")
            token_id = data.get("token_id", data.get("asset_id", ""))
            if msg_type == "ORDERBOOK_UPDATE" and token_id:
                with self.orderbook_lock:
                    self.orderbooks[token_id] = {
                        "bids": data.get("bids", []),
                        "asks": data.get("asks", []),
                        "last_trade_price": data.get("last_trade_price"),
                        "timestamp": data.get("timestamp", time.time()),
                    }
                logger.debug(f"Updated orderbook for {token_id}")
            elif msg_type == "HEARTBEAT":
                self._last_heartbeat = time.time()
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to parse WebSocket message: {e}")

    def _on_error(self, ws, error) -> None:
        logger.error(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")

    def stop(self) -> None:
        """Stop the listener."""
        self.running = False
        if self._ws:
            self._ws.close()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info("WebSocket CLOB Listener stopped.")

    def get_orderbook(self, token_id: str) -> Dict[str, Any]:
        """Get the latest cached orderbook state."""
        with self.orderbook_lock:
            return self.orderbooks.get(token_id, {}).copy()

    def add_token(self, token_id: str) -> None:
        """Subscribe to an additional token at runtime."""
        if token_id in self.token_ids:
            return
        self.token_ids.add(token_id)
        if self._ws and self.running:
            subscribe_msg = {
                "type": "SUBSCRIBE",
                "channel": "market",
                "token_id": token_id,
            }
            self._ws.send(json.dumps(subscribe_msg))
            logger.info(f"Subscribed to new token {token_id}")

    def is_healthy(self) -> bool:
        """Check if WebSocket is connected and receiving heartbeats."""
        if not self.running or not self._ws:
            return False
        if self._last_heartbeat == 0:
            return True  # Not received heartbeat yet but connected
        return (time.time() - self._last_heartbeat) < 60


def start_clob_websocket(host: str, tokens: List[str]) -> Optional[ClobWebSocketListener]:
    """Factory function to create and start a WebSocket listener."""
    if not WEBSOCKET_AVAILABLE:
        return None
    ws = ClobWebSocketListener(host, tokens)
    ws.start()
    return ws
