#!/usr/bin/env python3
"""Cloudflare D1 sync — real-time and batch modes."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

CF_API_BASE = "https://api.cloudflare.com/client/v4"
BATCH_SIZE = 50


class D1Client:
    """Cloudflare D1 REST API client."""

    def __init__(self):
        self.api_token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
        self.account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
        self.database_id = os.environ.get("CLOUDFLARE_D1_DATABASE_ID", "")
        self.url = f"{CF_API_BASE}/accounts/{self.account_id}/d1/database/{self.database_id}/query"
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }
        self._stats = {"api_calls": 0, "rows_synced": 0, "errors": 0, "time_spent": 0}

    @property
    def is_configured(self) -> bool:
        return bool(self.api_token and self.account_id and self.database_id)

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    def _execute(self, statements: list) -> list:
        """Execute batch of SQL statements. Returns list of result objects."""
        if not statements:
            return []
        start = time.time()
        try:
            resp = requests.post(self.url, headers=self.headers, json=statements, timeout=60)
            resp.raise_for_status()
            self._stats["api_calls"] += 1
            self._stats["rows_synced"] += len(statements)
            self._stats["time_spent"] += time.time() - start
            return resp.json()
        except Exception as e:
            self._stats["errors"] += 1
            self._stats["time_spent"] += time.time() - start
            logger.warning(f"D1 error: {e}")
            return []

    def _query_one(self, sql: str, params: list = None):
        """Execute single query and return results."""
        stmt = {"sql": sql}
        if params:
            stmt["params"] = params
        results = self._execute([stmt])
        if results and len(results) > 0 and "results" in results[0]:
            return results[0]["results"]
        return []

    def sync_search(self, origin: str, dest: str, flight_date: str, direction: str,
                    searched_at: str, status: str, error_msg: str, flights: list):
        """Sync a single search and its flights to D1."""
        if not self.is_configured:
            return

        # Step 1: Upsert the search record
        self._execute([{
            "sql": "INSERT INTO searches(origin, destination, flight_date, direction, searched_at, status, error_message, flight_count) "
                   "VALUES(?,?,?,?,?,?,?,?) "
                   "ON CONFLICT(origin, destination, flight_date, direction) DO UPDATE SET "
                   "searched_at=excluded.searched_at, status=excluded.status, error_message=excluded.error_message, "
                   "flight_count=excluded.flight_count",
            "params": [origin, dest, flight_date, direction, searched_at, status, error_msg, len(flights)],
        }])

        # Step 2: Get the search ID
        rows = self._query_one(
            "SELECT id FROM searches WHERE origin=? AND destination=? AND flight_date=? AND direction=?",
            [origin, dest, flight_date, direction],
        )
        if not rows:
            logger.warning(f"Could not find search ID for {origin}->{dest} {flight_date} {direction}")
            return
        search_id = rows[0]["id"]

        # Step 3: Delete old flights
        self._execute([{
            "sql": "DELETE FROM flights WHERE search_id=?",
            "params": [str(search_id)],
        }])

        # Step 4: Insert new flights in batches
        if not flights:
            return

        stmts = []
        for f in flights:
            stmts.append({
                "sql": "INSERT INTO flights(search_id, airline, departure_time, arrival_time, "
                       "depart_minutes, arrive_minutes, price, currency, stops, arrival_ahead, created_at) "
                       "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                "params": [str(search_id), f.get("airline", ""), f.get("departure", ""),
                           f.get("arrival", ""), str(f.get("depart_minutes", 0)),
                           str(f.get("arrive_minutes", 0)), str(f.get("price", 0)),
                           f.get("currency", "GBP"), str(f.get("stops", 0)),
                           f.get("arrival_ahead", ""), searched_at],
            })

        for i in range(0, len(stmts), BATCH_SIZE):
            self._execute(stmts[i:i + BATCH_SIZE])

    def sync_airports_and_routes(self, db_path: str):
        """Batch sync airports and routes from local DB."""
        if not self.is_configured:
            return

        local = sqlite3.connect(db_path)
        local.row_factory = sqlite3.Row

        airports = local.execute("SELECT * FROM airports").fetchall()
        stmts = [{
            "sql": "INSERT INTO airports(iata_code, name, country, is_origin) VALUES(?,?,?,?) "
                   "ON CONFLICT(iata_code) DO UPDATE SET name=excluded.name, country=excluded.country, "
                   "is_origin=MAX(is_origin, excluded.is_origin)",
            "params": [a["iata_code"], a["name"], a["country"], a["is_origin"]],
        } for a in airports]
        for i in range(0, len(stmts), BATCH_SIZE):
            self._execute(stmts[i:i + BATCH_SIZE])

        routes = local.execute("SELECT * FROM routes").fetchall()
        stmts = [{
            "sql": "INSERT INTO routes(origin, destination, dest_name, is_active) VALUES(?,?,?,?) "
                   "ON CONFLICT(origin, destination) DO UPDATE SET dest_name=excluded.dest_name, is_active=excluded.is_active",
            "params": [r["origin"], r["destination"], r["dest_name"], r["is_active"]],
        } for r in routes]
        for i in range(0, len(stmts), BATCH_SIZE):
            self._execute(stmts[i:i + BATCH_SIZE])

        local.close()
        logger.info(f"Synced {len(airports)} airports and {len(routes)} routes to D1")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    db_path = os.path.expanduser("~/.flightcache/flights.db")
    if not Path(db_path).exists():
        logger.error(f"Database not found: {db_path}")
        return 1

    client = D1Client()
    if not client.is_configured:
        logger.error("Cloudflare credentials not set")
        return 1

    client.sync_airports_and_routes(db_path)

    local = sqlite3.connect(db_path)
    local.row_factory = sqlite3.Row
    searches = local.execute("SELECT * FROM searches").fetchall()
    logger.info(f"Syncing {len(searches)} searches...")

    for s in searches:
        flights_rows = local.execute("SELECT * FROM flights WHERE search_id=?", (s["id"],)).fetchall()
        flights = [{
            "airline": f["airline"], "departure": f["departure_time"], "arrival": f["arrival_time"],
            "depart_minutes": f["depart_minutes"], "arrive_minutes": f["arrive_minutes"],
            "price": f["price"], "currency": f["currency"], "stops": f["stops"],
            "arrival_ahead": f["arrival_ahead"],
        } for f in flights_rows]
        client.sync_search(s["origin"], s["destination"], s["flight_date"], s["direction"],
                           s["searched_at"], s["status"], s["error_message"], flights)

    local.close()
    stats = client.stats
    logger.info(f"Done: {stats['api_calls']} calls, {stats['rows_synced']} rows, {stats['errors']} errors")
    return 0


if __name__ == "__main__":
    sys.exit(main())
