"""Core refresh logic — builds priority queue, executes searches, stores results."""
from __future__ import annotations

import calendar
import json
import logging
import random
import re
from datetime import date
from typing import Callable, Optional

from cache_db import FlightCache
from google_flights import search_flights
from rate_limiter import RateLimiter, AbortError
from config import STALENESS_TIERS, CHROME_VERSIONS, CONSENT_COOKIES

logger = logging.getLogger(__name__)


def _parse_price(price_str: str) -> float:
    if not price_str:
        return 0.0
    nums = re.sub(r"[^\d.]", "", price_str.replace(",", ""))
    try:
        return float(nums)
    except ValueError:
        return 0.0


def _get_staleness_max_hours(flight_date_str: str) -> float:
    try:
        flight_date = date.fromisoformat(flight_date_str)
        days_until = (flight_date - date.today()).days
        for max_days, max_hours in STALENESS_TIERS:
            if days_until <= max_days:
                return max_hours
        return 72
    except ValueError:
        return 24


def _get_month_dates(month_str: str) -> list:
    year, month = int(month_str[:4]), int(month_str[5:7])
    _, num_days = calendar.monthrange(year, month)
    today = date.today()
    return [
        date(year, month, day).isoformat()
        for day in range(1, num_days + 1)
        if date(year, month, day) > today
    ]


def build_search_queue(cache: FlightCache, origin: str, destinations: dict, month: str) -> list:
    """Build a prioritized, interleaved search queue."""
    dates = _get_month_dates(month)
    if not dates:
        return []

    queue = []
    for flight_date in dates:
        dest_list = list(destinations.items())
        random.shuffle(dest_list)

        for dest_code, dest_name in dest_list:
            for direction in ("outbound", "return"):
                o, d = (origin, dest_code) if direction == "outbound" else (dest_code, origin)
                age_hours = cache.get_search_age_hours(o, d, flight_date, direction)
                max_hours = _get_staleness_max_hours(flight_date)

                if age_hours is None:
                    priority = 10.0
                elif age_hours > max_hours:
                    priority = age_hours / max_hours
                else:
                    priority = 0.0

                if priority > 0:
                    queue.append((priority, o, d, flight_date, direction))

    queue.sort(key=lambda x: x[0], reverse=True)
    return queue


def run_refresh(
    cache: FlightCache,
    origin: str,
    destinations: dict,
    month: str,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """Run a full refresh cycle."""
    for dest_code, dest_name in destinations.items():
        cache.upsert_route(origin, dest_code, dest_name)

    queue = build_search_queue(cache, origin, destinations, month)
    total = len(queue)

    if total == 0:
        logger.info("Cache is fully fresh, nothing to refresh")
        return {"completed": 0, "skipped": 0, "failed": 0, "total": 0}

    logger.info(f"Refresh queue: {total} searches needed")

    rate_limiter = RateLimiter()
    chrome_version = random.choice(CHROME_VERSIONS)
    cookie_idx = 0
    stats = {"completed": 0, "skipped": 0, "failed": 0, "total": total}
    last_dest = None

    for i, (priority, o, d, flight_date, direction) in enumerate(queue):
        current_dest = (o, d)
        if last_dest and current_dest != last_dest:
            rate_limiter.destination_pause()
        last_dest = current_dest

        if progress_callback:
            progress_callback(i + 1, total, o, d, flight_date, direction, stats["completed"], stats["failed"])

        try:
            rate_limiter.wait()
        except AbortError:
            logger.error("Refresh aborted due to too many errors")
            break

        cookie_str = CONSENT_COOKIES[cookie_idx % len(CONSENT_COOKIES)]

        try:
            result = search_flights(
                from_airport=o, to_airport=d, date=flight_date,
                max_stops=0, cookie_str=cookie_str, chrome_version=chrome_version,
            )

            flights = []
            if result and result.flights:
                for f in result.flights:
                    flights.append({
                        "airline": f.name or "",
                        "departure": f.departure or "",
                        "arrival": f.arrival or "",
                        "price": _parse_price(f.price),
                        "currency": "GBP",
                        "stops": f.stops if isinstance(f.stops, int) else 0,
                        "arrival_ahead": getattr(f, "arrival_time_ahead", "") or "",
                    })

            status = "success" if flights else "no_results"
            cache.record_search(o, d, flight_date, direction, status=status, flights=flights)
            rate_limiter.record_success()
            stats["completed"] += 1

        except AssertionError as e:
            is_rate_limit = "429" in str(e)
            logger.warning(f"Search failed {o}->{d} {flight_date} {direction}: {e}")
            cache.record_search(o, d, flight_date, direction,
                                status="rate_limited" if is_rate_limit else "error", error_msg=str(e))
            rate_limiter.record_error(is_rate_limit=is_rate_limit)
            stats["failed"] += 1
            cookie_idx += 1

        except Exception as e:
            logger.warning(f"Search error {o}->{d} {flight_date} {direction}: {e}")
            cache.record_search(o, d, flight_date, direction, status="error", error_msg=str(e))
            rate_limiter.record_error()
            stats["failed"] += 1

    cache.cleanup_expired()
    logger.info(f"Refresh complete: {stats['completed']} done, {stats['failed']} failed")
    return stats
