"""Account-wide discovery using channel metadata and ticket-opening messages."""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone

from database import get_ticket, get_connection

logger = logging.getLogger(__name__)
GUILDS_PER_SCAN = 16
RECENT_GUILDS_PER_SCAN = 4
HISTORY_PROBES_PER_SCAN = 5
INACCESSIBLE_RECHECK_SECONDS = 24 * 60 * 60
TICKET_NAME = re.compile(r"^(?:ticket|support|help)[-_ ](?:\d+|[a-z0-9][a-z0-9_.-]+)$", re.I)
NUMBERED_TICKET_NAME = re.compile(r"^[0-9]+-[a-z0-9][a-z0-9_.-]*$", re.I)
OPENING = re.compile(
    r"(?:ticket\s+(?:created|opened|owner)|(?:created|opened)\s+(?:a\s+)?ticket|"
    r"welcome\s+to\s+(?:your|the)\s+ticket|"
    r"(?:close|claim)\s+(?:this\s+|the\s+)?ticket)", re.I,
)
CLOSED = re.compile(r"\bticket\s+(?:(?:has been|was|is)\s+)?closed\b", re.I)
SUPPORT = re.compile(r"\b(?:ticket|tickets|support|helpdesk|mava)\b", re.I)
TICKET_PARENTS = {"ticket", "tickets", "support ticket", "support tickets",
                  "ticket support", "open ticket", "open tickets", "create ticket"}
PANELS = {"ticket-panel", "ticket-info", "ticket-logs", "ticket-log", "ticket-transcripts",
          "support-chat", "support-info", "support-panel", "support-general",
          "help-chat", "help-general", "create-ticket", "open-ticket"}


def message_text(message):
    parts = [message.get("content", "")]
    for embed in message.get("embeds", []):
        parts.extend([embed.get("title", ""), embed.get("description", "")])
        for field in embed.get("fields", []):
            parts.extend([field.get("name", ""), field.get("value", "")])
    return "\n".join(str(part) for part in parts)


def ticket_controls(message):
    pending = list(message.get("components", []))
    while pending:
        component = pending.pop()
        pending.extend(component.get("components", []))
        text = str(component.get("custom_id", "")) + " " + str(component.get("label", ""))
        if re.search(r"(?:close|claim)[_: -]*ticket|ticket[_: -]*(?:close|claim)", text, re.I):
            return True
    return False


def private_text_channel(channel):
    """Recognize role-restricted text channels without requiring member overwrites."""
    return channel.get("type") == 0 and any(
        str(item.get("type")) == "0"
        and str(item.get("id")) == str(channel.get("guild_id"))
        and int(item.get("deny", 0)) & 1024
        for item in channel.get("permission_overwrites", [])
    )


def ticket_reason(channel, client, message=None):
    if channel.get("type") not in (0, 10, 11, 12):
        return None
    if channel.get("thread_metadata", {}).get("archived"):
        return None
    name = channel.get("name", "").lower()
    if name.startswith("closed-") or name in PANELS:
        return None
    private_member = any(
        str(item.get("type")) == "1" and int(item.get("allow", 0)) & 1024
        for item in channel.get("permission_overwrites", [])
    )
    parent = client.get_channel(channel.get("parent_id")) or {}
    private_text = private_text_channel(channel)
    private_thread = channel.get("type") == 12
    parent_name = " ".join(re.findall(r"[a-z0-9]+", parent.get("name", "").lower()))
    support_parent = bool(SUPPORT.search(parent_name))
    # Mava can use just a member's name for a private thread under its ticket panel.
    if private_thread and parent.get("type") == 0 and parent_name in TICKET_PARENTS:
        return "private thread under ticket channel"
    # MEE6 can name tickets using only the sequence number and member name.
    if channel.get("type") == 0 and NUMBERED_TICKET_NAME.fullmatch(name):
        if private_text or (private_member and support_parent):
            return "numbered private ticket channel name"
    if TICKET_NAME.match(name) and (private_text or private_member or channel.get("type") in (10, 11, 12)
                                    or support_parent or re.search(r"\d", name)):
        return "ticket channel name"
    if OPENING.search(channel.get("topic") or ""):
        return "ticket topic"
    if message and message.get("author", {}).get("bot") and (OPENING.search(message_text(message)) or ticket_controls(message)):
        if private_text:
            return "private text channel with ticket-opening app message"
        if private_thread:
            return "private thread with ticket-opening app message"
        if private_member or channel.get("type") in (10, 11, 12) or support_parent:
            return "ticket-opening app message"
    return None


class TicketDiscovery:
    def __init__(self, client, handler, interval=60):
        self.client = client
        self.handler = handler
        self.interval = max(15, interval)
        self.lock = asyncio.Lock()
        self.probed = {}
        self.probe_attempts = {}
        self.early_probes = {}
        self.probe_cursor = {}
        self.parent_retry_after = {}
        self.guild_scan_cursor = None
        self.guild_activity = {}
        self.started_at_ms = time.time_ns() // 1_000_000
        self.started_at = time.time()
        self.account_id = str((client.user or {}).get("id", ""))
        self.tracked = set()
        self.last_scan = None
        self.last_successful_scan = None
        self.last_scan_error = None
        self.scan_failure_details = []
        self.scan_running = False
        self.scan_started_at = None
        self.rest_successes = 0
        self.rest_failures = 0
        self.rest_actionable_failures = 0
        self.discovery_check_failures = 0
        self.idle_cycles = 0
        self.activity = 0
        self.wake = asyncio.Event()
        self.recovery_cursor = {}
        self.inaccessible_endpoints = {}
        if self.account_id:
            self.restore_monitor()

    def restore_monitor(self):
        conn = get_connection()
        try:
            with conn:
                existing = conn.execute("SELECT started_at_ms FROM monitor_state WHERE account_id=?", (self.account_id,)).fetchone()
                if existing:
                    self.started_at_ms = existing[0]
                else:
                    conn.execute("INSERT INTO monitor_state VALUES (?,?)", (self.account_id, self.started_at_ms))
                    # Adopt already-monitored active tickets, never discover unrelated old channels.
                    guilds = {str(guild["id"]) for guild in self.client.guilds_list}
                    for channel_id, guild_id in conn.execute("SELECT channel_id,guild_id FROM tickets WHERE active=1"):
                        if str(guild_id) in guilds:
                            cursor = str((self.started_at_ms - 1420070400000) << 22)
                            conn.execute("INSERT OR IGNORE INTO tracked_channels VALUES (?,?,?)", (self.account_id, channel_id, cursor))
                scan_state = conn.execute("SELECT last_guild_id FROM discovery_scan_state WHERE account_id=?",
                                          (self.account_id,)).fetchone()
                if scan_state:
                    self.guild_scan_cursor = scan_state[0]
                else:
                    conn.execute("INSERT OR IGNORE INTO discovery_scan_state(account_id,last_guild_id) VALUES (?,NULL)",
                                 (self.account_id,))
                self.tracked = {row[0] for row in conn.execute("""SELECT tracked_channels.channel_id
                    FROM tracked_channels JOIN tickets USING(channel_id)
                    WHERE tracked_channels.account_id=? AND tickets.active=1""", (self.account_id,))}
                now = time.time()
                self.inaccessible_endpoints = {
                    (str(guild_id), endpoint): inaccessible_until
                    for guild_id, endpoint, inaccessible_until in conn.execute(
                        "SELECT guild_id,endpoint,inaccessible_until FROM discovery_endpoint_state "
                        "WHERE account_id=? AND inaccessible_until>?", (self.account_id, now)
                    )
                }
        finally:
            conn.close()

    async def fetch(self, path):
        payload = await asyncio.to_thread(self.client._api_get, path)
        if payload is None:
            self.rest_failures += 1
            reason = self.client.rest_failure_reason(path) or "unknown REST failure"
            if "HTTP 403" not in reason:
                self.rest_actionable_failures += 1
        else:
            self.rest_successes += 1
        return payload

    def endpoint_is_inaccessible(self, guild_id, endpoint):
        key = (str(guild_id), endpoint)
        until = self.inaccessible_endpoints.get(key)
        if until is None:
            return False
        if until > time.time():
            return True
        self.inaccessible_endpoints.pop(key, None)
        conn = get_connection()
        try:
            with conn:
                conn.execute("DELETE FROM discovery_endpoint_state "
                             "WHERE account_id=? AND guild_id=? AND endpoint=?",
                             (self.account_id, key[0], endpoint))
        finally:
            conn.close()
        return False

    def mark_endpoint_inaccessible(self, guild_id, endpoint):
        key = (str(guild_id), endpoint)
        until = time.time() + INACCESSIBLE_RECHECK_SECONDS
        self.inaccessible_endpoints[key] = until
        conn = get_connection()
        try:
            with conn:
                conn.execute("INSERT INTO discovery_endpoint_state "
                             "(account_id,guild_id,endpoint,inaccessible_until) VALUES (?,?,?,?) "
                             "ON CONFLICT(account_id,guild_id,endpoint) DO UPDATE SET "
                             "inaccessible_until=excluded.inaccessible_until",
                             (self.account_id, key[0], endpoint, until))
        finally:
            conn.close()
        logger.info("REST endpoint marked inaccessible for 24 hours: guild=%s endpoint=%s",
                    key[0], endpoint)

    def clear_endpoint_inaccessible(self, guild_id, endpoint):
        key = (str(guild_id), endpoint)
        if self.inaccessible_endpoints.pop(key, None) is None:
            return
        conn = get_connection()
        try:
            with conn:
                conn.execute("DELETE FROM discovery_endpoint_state "
                             "WHERE account_id=? AND guild_id=? AND endpoint=?",
                             (self.account_id, key[0], endpoint))
        finally:
            conn.close()
        logger.info("REST endpoint is accessible again: guild=%s endpoint=%s", key[0], endpoint)

    def message_cursor(self, channel_id):
        if not self.account_id:
            return None
        conn = get_connection()
        try:
            row = conn.execute("SELECT last_message_id FROM tracked_channels WHERE account_id=? AND channel_id=?",
                               (self.account_id, str(channel_id))).fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def advance_cursor(self, channel_id, message_id):
        if not self.account_id or not message_id:
            return
        conn = get_connection()
        try:
            with conn:
                conn.execute("""UPDATE tracked_channels SET last_message_id=? WHERE account_id=? AND channel_id=?
                    AND (last_message_id IS NULL OR CAST(last_message_id AS INTEGER)<CAST(? AS INTEGER))""",
                             (str(message_id), self.account_id, str(channel_id), str(message_id)))
        finally:
            conn.close()

    def message_queued(self, message_id):
        if not self.account_id or not message_id:
            return False
        conn = get_connection()
        try:
            return conn.execute("SELECT 1 FROM notification_outbox WHERE event_key=?",
                                (f"{self.account_id}:message:{message_id}:0",)).fetchone() is not None
        finally:
            conn.close()

    def active_channel_ids(self):
        if not self.account_id:
            return []
        conn = get_connection()
        try:
            return [row[0] for row in conn.execute("""SELECT tracked_channels.channel_id
                FROM tracked_channels JOIN tickets USING(channel_id)
                WHERE tracked_channels.account_id=? AND tickets.active=1
                ORDER BY tracked_channels.channel_id""", (self.account_id,))]
        finally:
            conn.close()

    def is_live_channel(self, channel_id):
        if str(channel_id) in self.tracked:
            return True
        # Discord snowflakes contain creation time, including for uncached threads.
        try:
            value = int(channel_id)
            if value <= 0 or value >= 1 << 64:
                return False
            created_at_ms = (value >> 22) + 1420070400000
            return created_at_ms >= self.started_at_ms
        except (TypeError, ValueError):
            return False

    async def consider(self, channel, message=None):
        channel_id = str(channel.get("id", ""))
        if not self.is_live_channel(channel_id) or not channel.get("guild_id") or get_ticket(channel_id):
            return
        parent_id = str(channel.get("parent_id") or "")
        if (channel.get("type") == 12 and parent_id and not self.client.get_channel(parent_id)
                and time.monotonic() >= self.parent_retry_after.get(parent_id, 0)):
            self.parent_retry_after[parent_id] = time.monotonic() + 60
            parent = await self.fetch(f"/api/v10/channels/{parent_id}")
            if (isinstance(parent, dict) and str(parent.get("id")) == parent_id
                    and str(parent.get("guild_id", channel["guild_id"])) == str(channel["guild_id"])):
                parent.setdefault("guild_id", channel["guild_id"])
                self.client.channels[parent_id] = parent
                self.parent_retry_after.pop(parent_id, None)
        reason = ticket_reason(channel, self.client, message)
        if not reason:
            return
        async with self.lock:
            if get_ticket(channel_id):
                return
            logger.info("Ticket discovered: guild=%s channel=%s evidence=%s",
                        channel["guild_id"], channel_id, reason)
            await self.handler.channel_created(channel, auto_discovered=True)
            if get_ticket(channel_id):
                self.activity += 1
                self.guild_activity[str(channel["guild_id"])] = time.monotonic()
                self.early_probes.pop(channel_id, None)
                if self.account_id:
                    self.tracked.add(channel_id)

    async def channel_created(self, channel):
        if (self.is_live_channel(channel.get("id")) and channel.get("guild_id")
                and channel.get("type") in (0, 10, 11, 12)):
            self.wake.set()
        await self.consider(channel)
        if self.is_live_channel(channel.get("id")) and channel.get("type") in (0, 10, 11, 12):
            if channel.get("guild_id"):
                self.guild_activity[str(channel["guild_id"])] = time.monotonic()
            logger.info("Live channel creation received: guild=%s channel=%s type=%s tracked=%s",
                        channel.get("guild_id"), channel.get("id"), channel.get("type"),
                        bool(get_ticket(str(channel.get("id")))))
            if not get_ticket(str(channel.get("id"))) and channel.get("guild_id"):
                await self.probe_opening(channel)

    async def probe_opening(self, channel):
        key = str(channel["id"])
        if (not self.is_live_channel(key) or get_ticket(key)
                or channel.get("thread_metadata", {}).get("archived")
                or channel.get("name", "").lower() in PANELS):
            return False
        due = self.early_probes.get(key, self.probed.get(key, -float("inf")) + 600)
        if time.monotonic() < due:
            return False
        # Reserve the probe before yielding so gateway and scanner cannot duplicate it.
        self.early_probes.pop(key, None)
        self.probed[key] = time.monotonic()
        messages = await self.fetch(f"/api/v10/channels/{key}/messages?after={key}&limit=25")
        if isinstance(messages, list):
            for message in reversed(messages):
                await self.consider(channel, message)
        attempts = self.probe_attempts.get(key, 0) + 1
        self.probe_attempts[key] = min(attempts, 4)
        if not get_ticket(key) and attempts <= 3 and isinstance(messages, list):
            self.early_probes[key] = time.monotonic() + (15, 30, 60)[attempts - 1]
        if attempts == 1 and not get_ticket(key):
            logger.info("Ticket opening check: guild=%s channel=%s result=%s",
                        channel.get("guild_id"), key,
                        "no ticket evidence yet; follow-up scheduled" if isinstance(messages, list)
                        else "history unavailable; check account access or REST warnings")
        return True

    async def channel_updated(self, channel):
        if not self.is_live_channel(channel.get("id")):
            return
        await self.consider(channel)
        async with self.lock:
            await self.handler.channel_updated(channel)

    async def channel_deleted(self, channel):
        self.early_probes.pop(str(channel.get("id")), None)
        if self.is_live_channel(channel.get("id")):
            async with self.lock:
                await self.handler.channel_deleted(channel)

    async def message_created(self, message):
        if not message.get("guild_id"):
            return
        channel_id = str(message.get("channel_id", ""))
        if not self.is_live_channel(channel_id):
            return
        channel = self.client.get_channel(channel_id)
        if not channel:
            channel = await self.fetch(f"/api/v10/channels/{channel_id}")
            if not isinstance(channel, dict):
                return
            channel.setdefault("guild_id", message["guild_id"])
            self.client.channels[channel_id] = channel
        await self.consider(channel, message)
        async with self.lock:
            ticket = get_ticket(channel_id)
            if not ticket or not ticket[6]:
                return
            cursor = self.message_cursor(channel_id)
            message_id = message.get("id")
            if cursor and message_id and int(message_id) <= int(cursor):
                return
            if self.message_queued(message_id):
                return
            self.guild_activity[str(ticket[1])] = time.monotonic()
            if message.get("author", {}).get("bot") and CLOSED.search(message_text(message)):
                await self.handler.channel_deleted(channel)
            else:
                await self.handler.message_created(message)
            self.activity += 1

    async def scan_once(self):
        eligible_probes = set()
        history_probes = 0
        for channel in list(self.client.channels.values()):
            if str(channel.get("id")) not in self.tracked:
                await self.channel_updated(channel)
        guilds = self.select_guilds_for_scan()
        scanned_guild_ids = {str(guild["id"]) for guild in guilds}
        for guild in guilds:
            guild_id = str(guild["id"])
            channels_ok = False
            threads_ok = False
            channels_error = None
            threads_error = None
            if guild.get("unavailable"):
                self.record_guild_scan(guild_id, False, False,
                                       "server unavailable", "server unavailable")
                continue
            # Network I/O runs outside the gateway event loop; cache updates stay here.
            for suffix in ("channels", "threads/active"):
                if self.endpoint_is_inaccessible(guild_id, suffix):
                    if suffix == "channels":
                        channels_ok = True
                        channels_error = "inaccessible (HTTP 403; retry scheduled)"
                    else:
                        threads_ok = True
                        threads_error = "inaccessible (HTTP 403; retry scheduled)"
                    continue
                payload = await self.fetch(f"/api/v10/guilds/{guild_id}/{suffix}")
                if suffix == "channels":
                    channels = payload if isinstance(payload, list) else None
                    channels_ok = channels is not None
                    error_attr = "channels_error"
                else:
                    channels = payload.get("threads") if isinstance(payload, dict) else None
                    threads_ok = isinstance(channels, list)
                    error_attr = "threads_error"
                if not isinstance(channels, list):
                    reason = self.client.rest_failure_reason(
                        f"/api/v10/guilds/{guild_id}/{suffix}"
                    ) or "unexpected response format"
                    if "HTTP 403" in reason:
                        self.mark_endpoint_inaccessible(guild_id, suffix)
                        if suffix == "channels":
                            channels_ok = True
                            channels_error = "inaccessible (HTTP 403; retry scheduled)"
                        else:
                            threads_ok = True
                            threads_error = "inaccessible (HTTP 403; retry scheduled)"
                        continue
                    if error_attr == "channels_error":
                        channels_error = reason
                    else:
                        threads_error = reason
                    await asyncio.sleep(0.25)
                    continue
                self.clear_endpoint_inaccessible(guild_id, suffix)
                for channel in channels:
                    channel["guild_id"] = guild_id
                    key = str(channel["id"])
                    self.client.channels[key] = {**self.client.channels.get(key, {}), **channel}
                await asyncio.sleep(0.25)
            self.record_guild_scan(guild_id, channels_ok, threads_ok,
                                   channels_error, threads_error)
            probe_candidates = []
            for channel in list(self.client.channels.values()):
                if str(channel.get("guild_id")) != guild_id:
                    continue
                key = str(channel["id"])
                if key not in self.tracked:
                    await self.channel_updated(channel)
                if not self.is_live_channel(key):
                    continue
                if channel.get("type") not in (0, 10, 11, 12) or get_ticket(key):
                    continue
                if channel.get("thread_metadata", {}).get("archived"):
                    self.early_probes.pop(key, None)
                    continue
                if channel.get("name", "").lower() in PANELS:
                    continue
                probe_candidates.append(channel)
                eligible_probes.add(key)
            if not probe_candidates or history_probes >= HISTORY_PROBES_PER_SCAN:
                continue
            start = self.probe_cursor.get(guild_id, 0) % len(probe_candidates)
            for offset in range(len(probe_candidates)):
                if history_probes >= HISTORY_PROBES_PER_SCAN:
                    break
                index = (start + offset) % len(probe_candidates)
                channel = probe_candidates[index]
                key = str(channel["id"])
                self.probe_cursor[guild_id] = index + 1
                # Bounded history probes catch apps that use custom channel names.
                if not await self.probe_opening(channel):
                    continue
                history_probes += 1
                await asyncio.sleep(0.5)

        self.early_probes = {
            key: due for key, due in self.early_probes.items()
            if not get_ticket(key)
            and (str((self.client.get_channel(key) or {}).get("guild_id")) not in scanned_guild_ids
                 or key in eligible_probes)
        }

        # Recover missed messages and thread closure metadata, with a bounded rotating budget.
        active = self.active_channel_ids()
        start = self.recovery_cursor.get("index", 0)
        for offset in range(min(5, len(active))):
            key = active[(start + offset) % len(active)]
            ticket = get_ticket(key)
            if not self.client.get_guild(ticket[1]):
                continue
            payload = await self.fetch(f"/api/v10/channels/{key}")
            if isinstance(payload, dict):
                payload.setdefault("guild_id", ticket[1])
                self.client.channels[key] = payload
                # Recover messages before processing a close, so their notifications stay ordered.
                cursor = self.message_cursor(key) or key
                messages = await self.fetch(f"/api/v10/channels/{key}/messages?after={cursor}&limit=100")
                if isinstance(messages, list):
                    for message in sorted(messages, key=lambda value: int(value["id"])):
                        message.update(guild_id=ticket[1], channel_id=key)
                        await self.message_created(message)
                    if messages:
                        self.advance_cursor(key, max(messages, key=lambda value: int(value["id"]))["id"])
                    if len(messages) < 100:
                        await self.channel_updated(payload)
                else:
                    await self.channel_updated(payload)
            await asyncio.sleep(0.25)
        if active:
            self.recovery_cursor["index"] = (start + 5) % len(active)

    def select_guilds_for_scan(self):
        guilds = sorted(self.client.guilds_list, key=lambda guild: str(guild.get("id", "")))
        if len(guilds) <= GUILDS_PER_SCAN:
            selected = guilds
        else:
            by_id = {str(guild["id"]): guild for guild in guilds}
            active_since = time.monotonic() - 15 * 60
            recent_ids = [guild_id for guild_id, seen in sorted(
                self.guild_activity.items(), key=lambda pair: pair[1], reverse=True)
                if seen >= active_since and guild_id in by_id]
            priority = recent_ids[:RECENT_GUILDS_PER_SCAN]

            ids = [str(guild["id"]) for guild in guilds]
            cursor_index = ids.index(self.guild_scan_cursor) if self.guild_scan_cursor in ids else -1
            rotation = ids[cursor_index + 1:] + ids[:cursor_index + 1]
            rotating = []
            for guild_id in rotation:
                if guild_id not in priority:
                    rotating.append(guild_id)
                    if len(rotating) == GUILDS_PER_SCAN - len(priority):
                        break
            chosen = priority + rotating
            selected = [by_id[guild_id] for guild_id in chosen]
            if rotating:
                self.guild_scan_cursor = rotating[-1]
            elif chosen:
                self.guild_scan_cursor = chosen[-1]
            self.persist_guild_scan_cursor()
        return selected

    def persist_guild_scan_cursor(self):
        if not self.account_id:
            return
        conn = get_connection()
        try:
            with conn:
                conn.execute("INSERT INTO discovery_scan_state(account_id,last_guild_id) VALUES (?,?) "
                             "ON CONFLICT(account_id) DO UPDATE SET last_guild_id=excluded.last_guild_id",
                             (self.account_id, self.guild_scan_cursor))
        finally:
            conn.close()

    def record_guild_scan(self, guild_id, channels_ok, threads_ok,
                          channels_error=None, threads_error=None):
        if not channels_ok or not threads_ok:
            self.discovery_check_failures += 1
            for endpoint, ok, reason in (
                ("channels", channels_ok, channels_error),
                ("active threads", threads_ok, threads_error),
            ):
                if not ok:
                    self.scan_failure_details.append(
                        f"server {guild_id} {endpoint}: {reason or 'cause unavailable'}"
                    )
        conn = get_connection()
        try:
            with conn:
                conn.execute("INSERT INTO discovery_checks(checked_at,guild_id,channels_ok,threads_ok,"
                             "channels_error,threads_error) VALUES (?,?,?,?,?,?)",
                             (datetime.now(timezone.utc).isoformat(), str(guild_id),
                              int(channels_ok), int(threads_ok), channels_error, threads_error))
        finally:
            conn.close()

    def scan_delay(self):
        # Keep REST work in small predictable slices; gateway events remain immediate.
        delay = min(self.interval, 30)
        if self.early_probes:
            delay = min(delay, max(15, min(self.early_probes.values()) - time.monotonic()))
        return delay

    async def run(self):
        await self.client.ready.wait()
        logger.info("Live-only ticket discovery active across %d servers; pre-existing tickets are ignored.",
                    len(self.client.guilds_list))
        while True:
            self.wake.clear()
            previous_activity = self.activity
            actionable_failures = self.rest_actionable_failures
            discovery_failures = self.discovery_check_failures
            try:
                await self.client.ready.wait()
                self.scan_running = True
                self.scan_started_at = time.time()
                self.scan_failure_details = []
                await self.scan_once()
                self.last_scan = time.time()
                if (self.discovery_check_failures > discovery_failures
                        or self.rest_actionable_failures > actionable_failures):
                    examples = "; ".join(self.scan_failure_details[:4])
                    remaining = len(self.scan_failure_details) - 4
                    self.last_scan_error = "Incomplete checks" + (f": {examples}" if examples else "")
                    if remaining > 0:
                        self.last_scan_error += f"; and {remaining} more endpoint failure(s)"
                else:
                    self.last_scan_error = None
                if not self.last_scan_error:
                    self.last_successful_scan = self.last_scan
            except asyncio.CancelledError:
                raise
            except Exception:
                self.last_scan_error = "Scan failed; retry scheduled"
                logger.exception("Ticket discovery scan failed; retrying next cycle.")
            finally:
                self.scan_running = False
                self.scan_started_at = None
            self.idle_cycles = self.idle_cycles + 1 if self.activity == previous_activity else 0
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=self.scan_delay())
            except asyncio.TimeoutError:
                pass
