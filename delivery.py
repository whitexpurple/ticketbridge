"""Durable, ordered Telegram delivery with atomic ticket-open/close updates."""

import asyncio
import json
import time
import logging
from datetime import datetime, timedelta, timezone

from telegram import MessageEntity
from telegram.error import RetryAfter
from database import get_connection

logger = logging.getLogger(__name__)


def chunks(text, limit=3500):
    # Telegram counts UTF-16 units; astral characters consume two units.
    result, part, size = [], [], 0
    for character in text:
        units = 2 if ord(character) > 0xFFFF else 1
        if size + units > limit:
            result.append("".join(part))
            part, size = [], 0
        part.append(character)
        size += units
    if part:
        result.append("".join(part))
    return result


def utf16_length(value):
    return len(value.encode("utf-16-le")) // 2


def chunks_with_entities(text, entities, limit=3500):
    parts = chunks(text, limit)
    result = []
    chunk_start = 0
    for part in parts:
        chunk_end = chunk_start + utf16_length(part)
        part_entities = []
        for entity in entities or ():
            entity_start = int(entity["offset"])
            entity_end = entity_start + int(entity["length"])
            overlap_start = max(chunk_start, entity_start)
            overlap_end = min(chunk_end, entity_end)
            if overlap_start >= overlap_end:
                continue
            clipped = {
                "type": entity["type"],
                "offset": overlap_start - chunk_start,
                "length": overlap_end - overlap_start,
            }
            if entity.get("url"):
                clipped["url"] = entity["url"]
            part_entities.append(clipped)
        result.append((part, part_entities))
        chunk_start = chunk_end
    return result


class TelegramNotification:
    """Build plain text plus Telegram entities for copyable values and links."""

    def __init__(self):
        self.parts = []
        self.entities = []
        self.length = 0

    def add(self, value):
        value = str(value)
        self.parts.append(value)
        self.length += utf16_length(value)

    def field(self, label, value):
        self.add(f"{label}: ")
        value = str(value)
        if value:
            self.entities.append({"type": "code", "offset": self.length,
                                  "length": utf16_length(value)})
        self.add(value)
        self.add("\n")

    def link(self, label, url):
        label = str(label)
        self.entities.append({"type": "text_link", "offset": self.length,
                              "length": utf16_length(label), "url": url})
        self.add(label)

    def render(self):
        return "".join(self.parts), self.entities


class NotificationOutbox:
    def __init__(self, bot, chat_id, limiter):
        self.bot, self.chat_id, self.limiter = bot, str(chat_id), limiter
        self.wake = asyncio.Event()
        self.last_success = None
        self.last_error = None
        conn = get_connection()
        try:
            self.last_success = conn.execute("SELECT MAX(delivered_at) FROM notification_outbox WHERE chat_id=?",
                                             (self.chat_id,)).fetchone()[0]
            pending = conn.execute("""SELECT last_error FROM notification_outbox
                WHERE chat_id=? AND delivered_at IS NULL ORDER BY id LIMIT 1""", (self.chat_id,)).fetchone()
            self.last_error = pending[0] if pending else None
        finally:
            conn.close()

    def insert(self, conn, text, key, entities=None):
        for index, (part, part_entities) in enumerate(chunks_with_entities(text, entities)):
            conn.execute("""INSERT OR IGNORE INTO notification_outbox
                (event_key, chat_id, text, created_at, entities) VALUES (?, ?, ?, ?, ?)""",
                         (f"{key}:{index}", self.chat_id, part, time.time(),
                          json.dumps(part_entities) if part_entities else None))

    def enqueue(self, text, key, mutation=None, entities=None):
        conn = get_connection()
        try:
            with conn:
                if mutation:
                    if mutation(conn) is False:
                        return
                self.insert(conn, text, key, entities)
        finally:
            conn.close()
        self.wake.set()

    def pending_count(self):
        conn = get_connection()
        try:
            return conn.execute("SELECT COUNT(*) FROM notification_outbox WHERE delivered_at IS NULL AND chat_id=?",
                                (self.chat_id,)).fetchone()[0]
        finally:
            conn.close()

    def pending_age(self):
        conn = get_connection()
        try:
            oldest = conn.execute("SELECT MIN(created_at) FROM notification_outbox "
                                  "WHERE delivered_at IS NULL AND chat_id=?", (self.chat_id,)).fetchone()[0]
            return max(0, time.time() - oldest) if oldest is not None else 0
        finally:
            conn.close()

    async def deliver_once(self):
        conn = get_connection()
        try:
            row = conn.execute("""SELECT id, text, entities, attempts, next_attempt FROM notification_outbox
                WHERE delivered_at IS NULL AND chat_id=? ORDER BY id LIMIT 1""", (self.chat_id,)).fetchone()
        finally:
            conn.close()
        if not row or row[4] > time.time():
            return False
        row_id, text, raw_entities, attempts, _ = row
        entities = [MessageEntity(**entity) for entity in json.loads(raw_entities)] if raw_entities else None
        try:
            await self.limiter.wait_if_needed()
            preview_option = {"disable_web_page_preview": True} if entities else {}
            await self.bot.send_message(chat_id=self.chat_id, text=text, entities=entities,
                                        **preview_option)
        except Exception as error:
            delay = min(300, 2 ** min(attempts + 1, 8))
            if isinstance(error, RetryAfter):
                value = error.retry_after
                delay = max(1, value.total_seconds() if isinstance(value, timedelta) else float(value))
            self.last_error = type(error).__name__
            conn = get_connection()
            try:
                with conn:
                    conn.execute("""UPDATE notification_outbox SET attempts=attempts+1,
                        next_attempt=?, last_error=? WHERE id=?""", (time.time() + delay, self.last_error, row_id))
            finally:
                conn.close()
            logger.warning("Telegram delivery postponed: error=%s retry_seconds=%s", self.last_error, delay)
            return False
        conn = get_connection()
        try:
            with conn:
                conn.execute("UPDATE notification_outbox SET delivered_at=?, text='', entities=NULL WHERE id=?",
                             (time.time(), row_id))
        finally:
            conn.close()
        self.last_success, self.last_error = time.time(), None
        return True

    async def run(self):
        while True:
            self.wake.clear()
            try:
                if await self.deliver_once():
                    continue
            except Exception as error:
                self.last_error = type(error).__name__
                logger.error("Notification worker error: %s", self.last_error)
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=2)
            except asyncio.TimeoutError:
                pass


class TicketDelivery:
    def __init__(self, outbox, account_id, account_name="Discord"):
        self.outbox, self.account_id = outbox, str(account_id)
        self.account_name = str(account_name or "Discord")

    def _account_text(self, text):
        return f"Discord account: {self.account_name}\n{text}"

    def _account_entities(self, entities):
        if not entities:
            return entities
        offset = utf16_length(f"Discord account: {self.account_name}\n")
        return [{**entity, "offset": int(entity["offset"]) + offset} for entity in entities]

    def opened(self, values, text, entities=None):
        channel_id = str(values[0])
        def mutation(conn):
            existing = conn.execute("SELECT account_id FROM tickets WHERE channel_id=?", (channel_id,)).fetchone()
            if existing and existing[0] not in (None, self.account_id):
                return False
            conn.execute("""INSERT OR IGNORE INTO tickets
                (channel_id,guild_id,channel_name,user_id,user_name,opened_at,active,user_confirmed,account_id)
                VALUES (?,?,?,?,?,?,1,0,?)""", (*values, self.account_id))
            conn.execute("UPDATE tickets SET account_id=? WHERE channel_id=? AND account_id IS NULL",
                         (self.account_id, channel_id))
            conn.execute("INSERT OR IGNORE INTO tracked_channels(account_id,channel_id) VALUES (?,?)",
                         (self.account_id, channel_id))
            return True
        self.outbox.enqueue(self._account_text(text), f"{self.account_id}:open:{channel_id}", mutation,
                            self._account_entities(entities))

    def closed(self, channel_id, text, entities=None):
        closed_at = datetime.now(timezone.utc).isoformat()
        conn = get_connection()
        try:
            owner = conn.execute("SELECT account_id FROM tickets WHERE channel_id=?", (str(channel_id),)).fetchone()
        finally:
            conn.close()
        if owner and owner[0] not in (None, self.account_id):
            return
        self.outbox.enqueue(self._account_text(text), f"{self.account_id}:close:{channel_id}",
                            lambda conn: conn.execute("UPDATE tickets SET active=0, closed_at=COALESCE(closed_at, ?) WHERE channel_id=?",
                                                      (closed_at, str(channel_id))), self._account_entities(entities))

    def message(self, channel_id, message_id, text, entities=None):
        if not message_id:
            raise ValueError("A Discord message ID is required for reliable delivery")
        conn = get_connection()
        try:
            owner = conn.execute("SELECT account_id FROM tickets WHERE channel_id=?", (str(channel_id),)).fetchone()
        finally:
            conn.close()
        if owner and owner[0] not in (None, self.account_id):
            return
        self.outbox.enqueue(self._account_text(text), f"{self.account_id}:message:{message_id}",
                            entities=self._account_entities(entities))
