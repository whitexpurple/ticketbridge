"""Persist relevant dispatch events quickly so slow handlers cannot delay heartbeats."""

import asyncio
import json
import logging
import time

from database import get_connection

logger = logging.getLogger(__name__)


class GatewayEvents:
    def __init__(self, account_id, discovery):
        self.account_id, self.discovery = str(account_id), discovery
        self.wake = asyncio.Event()
        self.last_error = None

    def callback(self, kind):
        async def enqueue(payload):
            channel_id = payload.get("channel_id") if kind == "message_created" else payload.get("id")
            if not self.discovery.is_live_channel(channel_id):
                return
            conn = get_connection()
            try:
                with conn:
                    conn.execute("INSERT INTO gateway_inbox(account_id,kind,payload) VALUES (?,?,?)",
                                 (self.account_id, kind, json.dumps(payload)))
            finally:
                conn.close()
            self.wake.set()
        return enqueue

    def pending_count(self):
        conn = get_connection()
        try:
            return conn.execute("SELECT COUNT(*) FROM gateway_inbox WHERE account_id=?", (self.account_id,)).fetchone()[0]
        finally:
            conn.close()

    async def process_once(self):
        conn = get_connection()
        try:
            row = conn.execute("SELECT id,kind,payload,attempts,next_attempt FROM gateway_inbox WHERE account_id=? ORDER BY id LIMIT 1",
                               (self.account_id,)).fetchone()
        finally:
            conn.close()
        if not row or row[4] > time.time():
            return False
        try:
            await getattr(self.discovery, row[1])(json.loads(row[2]))
        except Exception as error:
            self.last_error = type(error).__name__
            conn = get_connection()
            try:
                with conn:
                    conn.execute("UPDATE gateway_inbox SET attempts=attempts+1,next_attempt=? WHERE id=?",
                                 (time.time() + min(300, 2 ** min(row[3] + 1, 8)), row[0]))
            finally:
                conn.close()
            logger.warning("Gateway event processing deferred: error=%s", self.last_error)
            return False
        conn = get_connection()
        try:
            with conn:
                conn.execute("DELETE FROM gateway_inbox WHERE id=?", (row[0],))
        finally:
            conn.close()
        self.last_error = None
        return True

    async def run(self):
        while True:
            self.wake.clear()
            try:
                if await self.process_once():
                    continue
            except Exception as error:
                self.last_error = type(error).__name__
                logger.error("Gateway event worker error: %s", self.last_error)
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=2)
            except asyncio.TimeoutError:
                pass
