#!/usr/bin/env python3
"""Sync local SQLite flight cache to Cloudflare D1 — batched for speed."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

CF_API_BASE = "https://api.cloudflare.com/client/v4"
BATCH_SIZE = 50  # D1 supports up to 100 statements per batch


class D1Sync:
    def __init__(self):
        self.api_token = os.environ["CLOUDFLARE_API_TOKEN"]
        self.account_id = os.environ["CLOUDFLARE_ACCOUNT_ID"]
        self.database_id = os.environ["CLOUDFLARE_D1_DATABASE_ID"]
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }
        self.url = f"{CF_API_BASE}/accounts/{self.account_id}/d1/database/{self.database_id}/query"

    def _batch(self, statements: list):
        """Execute a batch of SQL statements in one API call."""
        if not statements:
            return
        resp = requests.post(self.url, headers=self.headers, json=statements, timeout=60)
        resp.raise_for_status()

    def _chunks(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def sync(self, db_path: str):
        local = sqlite3.connect(db_path)
        local.row_factory = sqlite3.Row

        # 1. Batch sync airports
        airports = local.execute("SELECT * FROM airports").fetchall()
        logger.info(f"Syncing {len(airports)} airports...")
        stmts = []
        for a in airports:
            stmts.append({
                "sql": "INSERT INTO airports(iata_code, name, country, is_origin) VALUES(?,?,?,?) "
                       "ON CONFLICT(iata_code) DO UPDATE SET name=excluded.name, country=excluded.country, "
                       "is_origin=MAX(is_origin, excluded.is_origin)",
                "params": [a["iata_code"], a["name"], a["country"], a["is_origin"]],
            })
        for chunk in self._chunks(stmts, BATCH_SIZE):
            self._batch(chunk)

        # 2. Batch sync routes
        routes = local.execute("SELECT * FROM routes").fetchall()
        logger.info(f"Syncing {len(routes)} routes...")
        stmts = []
        for r in routes:
            stmts.append({
                "sql": "INSERT INTO routes(origin, destination, dest_name, is_active) VALUES(?,?,?,?) "
                       "ON CONFLICT(origin, destination) DO UPDATE SET dest_name=excluded.dest_name, is_active=excluded.is_active",
                "params": [r["origin"], r["destination"], r["dest_name"], r["is_active"]],
            })
        for chunk in self._chunks(stmts, BATCH_SIZE):
            self._batch(chunk)

        # 3. Sync searches — batch upserts, then get ID mapping
        searches = local.execute("SELECT * FROM searches").fetchall()
        logger.info(f"Syncing {len(searches)} searches...")

        # Upsert all searches in batches
        stmts = []
        for s in searches:
            stmts.append({
                "sql": "INSERT INTO searches(origin, destination, flight_date, direction, searched_at, status, error_message, flight_count) "
                       "VALUES(?,?,?,?,?,?,?,?) "
                       "ON CONFLICT(origin, destination, flight_date, direction) DO UPDATE SET "
                       "searched_at=excluded.searched_at, status=excluded.status, error_message=excluded.error_message, "
                       "flight_count=excluded.flight_count",
                "params": [s["origin"], s["destination"], s["flight_date"], s["direction"],
                           s["searched_at"], s["status"], s["error_message"], s["flight_count"]],
            })
        for chunk in self._chunks(stmts, BATCH_SIZE):
            self._batch(chunk)

        # 4. Build D1 search ID map (one query)
        resp = requests.post(self.url, headers=self.headers, json=[{
            "sql": "SELECT id, origin, destination, flight_date, direction FROM searches",
            "params": [],
        }], timeout=30)
        resp.raise_for_status()
        d1_search_map = {}
        for row in resp.json()[0]["results"]:
            key = (row["origin"], row["destination"], row["flight_date"], row["direction"])
            d1_search_map[key] = row["id"]

        # 5. Delete all existing flights and re-insert in batches
        logger.info("Clearing old flights...")
        self._batch([{"sql": "DELETE FROM flights", "params": []}])

        # 6. Batch insert all flights
        all_flights = local.execute("""
            SELECT f.*, s.origin, s.destination, s.flight_date, s.direction
            FROM flights f JOIN searches s ON f.search_id = s.id
        """).fetchall()
        logger.info(f"Syncing {len(all_flights)} flights...")

        stmts = []
        skipped = 0
        for f in all_flights:
            key = (f["origin"], f["destination"], f["flight_date"], f["direction"])
            d1_id = d1_search_map.get(key)
            if not d1_id:
                skipped += 1
                continue
            stmts.append({
                "sql": "INSERT INTO flights(search_id, airline, departure_time, arrival_time, "
                       "depart_minutes, arrive_minutes, price, currency, stops, arrival_ahead, created_at) "
                       "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                "params": [str(d1_id), f["airline"], f["departure_time"], f["arrival_time"],
                           str(f["depart_minutes"]), str(f["arrive_minutes"]), str(f["price"]),
                           f["currency"], str(f["stops"]), f["arrival_ahead"], f["created_at"]],
            })

        for i, chunk in enumerate(self._chunks(stmts, BATCH_SIZE)):
            self._batch(chunk)
            if (i + 1) % 5 == 0:
                logger.info(f"  Inserted {min((i+1)*BATCH_SIZE, len(stmts))}/{len(stmts)} flights...")

        if skipped:
            logger.warning(f"Skipped {skipped} flights (no matching search in D1)")

        local.close()
        logger.info("Sync to Cloudflare D1 complete!")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    db_path = os.path.expanduser("~/.flightcache/flights.db")
    if not Path(db_path).exists():
        logger.error(f"Database not found: {db_path}")
        return 1

    syncer = D1Sync()
    syncer.sync(db_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
