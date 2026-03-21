"""Smart rate limiter with jitter, backoff, and burst control."""
from __future__ import annotations

import logging
import random
import time
from threading import Lock

from config import (
    MIN_DELAY, MAX_DELAY, DEST_PAUSE_MIN, DEST_PAUSE_MAX,
    BATCH_COOLDOWN, BATCH_PAUSE_MIN, BATCH_PAUSE_MAX,
    BACKOFF_INITIAL, BACKOFF_MULTIPLIER, BACKOFF_MAX,
    MAX_CONSECUTIVE_ERRORS,
)

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self):
        self._lock = Lock()
        self._last_request = 0.0
        self._request_count = 0
        self._consecutive_errors = 0
        self._current_backoff = BACKOFF_INITIAL
        self._aborted = False

    @property
    def is_aborted(self) -> bool:
        return self._aborted

    @property
    def request_count(self) -> int:
        return self._request_count

    def wait(self):
        with self._lock:
            if self._aborted:
                raise AbortError("Too many consecutive errors")

            if self._request_count > 0 and self._request_count % BATCH_COOLDOWN == 0:
                pause = random.uniform(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
                logger.info(f"Batch cooldown after {self._request_count} requests: {pause:.0f}s")
                time.sleep(pause)

            now = time.time()
            elapsed = now - self._last_request
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            if elapsed < delay:
                time.sleep(delay - elapsed)

            self._last_request = time.time()
            self._request_count += 1

    def destination_pause(self):
        pause = random.uniform(DEST_PAUSE_MIN, DEST_PAUSE_MAX)
        logger.debug(f"Destination pause: {pause:.0f}s")
        time.sleep(pause)

    def record_success(self):
        self._consecutive_errors = 0
        self._current_backoff = BACKOFF_INITIAL

    def record_error(self, is_rate_limit: bool = False):
        self._consecutive_errors += 1
        if self._consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            logger.error(f"Aborting: {self._consecutive_errors} consecutive errors")
            self._aborted = True
            return
        if is_rate_limit:
            backoff = self._current_backoff
            self._current_backoff = min(self._current_backoff * BACKOFF_MULTIPLIER, BACKOFF_MAX)
        else:
            backoff = min(self._current_backoff, 120)
        jitter = random.uniform(0, backoff * 0.3)
        total_wait = backoff + jitter
        logger.warning(f"Error backoff: {total_wait:.0f}s (attempt {self._consecutive_errors}/{MAX_CONSECUTIVE_ERRORS})")
        time.sleep(total_wait)


class AbortError(Exception):
    pass
