
import hashlib
import time
import logging

logger = logging.getLogger(__name__)

LAST_RESPONSE_HASH = None
LAST_RESPONSE_TS = 0
DUPLICATE_WINDOW = 120  # secondes

def is_duplicate_response(text: str) -> bool:
    """Check if the text is a duplicate of a recent response."""
    global LAST_RESPONSE_HASH, LAST_RESPONSE_TS

    if not text:
        return False

    h = hashlib.sha256(text.strip().encode()).hexdigest()
    now = time.time()

    if h == LAST_RESPONSE_HASH and now - LAST_RESPONSE_TS < DUPLICATE_WINDOW:
        return True

    LAST_RESPONSE_HASH = h
    LAST_RESPONSE_TS = now
    return False
