"""Durable daily ticket summaries, scheduled at local midnight."""

import asyncio
import logging
import time
from collections import defaultdict
from datetime import date, datetime, time as day_time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from database import get_connection

logger = logging.getLogger("daily-report")


def parse_timestamp(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def timezone_label(value):
    total_seconds = int(value.total_seconds())
    sign = "+" if total_seconds >= 0 else "-"
    minutes = abs(total_seconds) // 60
    return f"UTC{sign}{minutes // 60:02d}:{minutes % 60:02d}"


def compose_report(report_day, timezone_name, rows, guild_names, title="TicketBridge Daily Report"):
    zone = ZoneInfo(timezone_name)
    start = datetime.combine(report_day, day_time.min, tzinfo=zone)
    end = datetime.combine(report_day + timedelta(days=1), day_time.min, tzinfo=zone)
    counts = defaultdict(lambda: {"opened": 0, "closed": 0})
    for guild_id, opened_value, closed_value, _active in rows:
        opened = parse_timestamp(opened_value)
        closed = parse_timestamp(closed_value)
        if not opened:
            continue
        stats = counts[str(guild_id)]
        if start <= opened < end:
            stats["opened"] += 1
        if closed and start <= closed < end:
            stats["closed"] += 1

    activity_servers = {guild_id for guild_id, values in counts.items()
                        if values["opened"] or values["closed"]}
    lines = [
        title,
        f"{report_day.strftime('%A, %B')} {report_day.day}, {report_day.year} · {timezone_label(start.utcoffset())}",
        "",
        f"{sum(value['opened'] for value in counts.values())} tickets opened · "
        f"{sum(value['closed'] for value in counts.values())} closed",
    ]
    if not activity_servers:
        lines.append("No ticket activity during this period.")
    else:
        for guild_id in sorted(activity_servers, key=lambda key: guild_names.get(key, key).casefold()):
            value = counts[guild_id]
            name = guild_names.get(guild_id, f"Server {guild_id}")
            lines.append(f"• {name}: {value['opened']} opened · {value['closed']} closed")
    return "\n".join(lines)


class DailyReportWorker:
    def __init__(self, outbox, timezone_name="Etc/GMT-1", guilds_provider=None):
        self.outbox = outbox
        try:
            self.zone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as error:
            raise ValueError(f"Unknown DAILY_REPORT_TIMEZONE: {timezone_name}") from error
        self.timezone_name = timezone_name
        self.guilds_provider = guilds_provider or (lambda: {})

    def _guild_names(self):
        names = {}
        try:
            for guild in self.guilds_provider() or []:
                if isinstance(guild, dict) and guild.get("id"):
                    names[str(guild["id"])] = guild.get("name") or f"Server {guild['id']}"
        except Exception:
            logger.debug("Could not read cached Discord server names.", exc_info=True)
        conn = get_connection()
        try:
            for guild_id, name in conn.execute("SELECT guild_id,guild_name FROM servers"):
                names.setdefault(str(guild_id), name)
        finally:
            conn.close()
        return names

    def _report_text(self, report_day):
        conn = get_connection()
        try:
            rows = conn.execute("SELECT guild_id,opened_at,closed_at,active FROM tickets").fetchall()
        finally:
            conn.close()
        return compose_report(report_day, self.timezone_name, rows, self._guild_names())

    def current_report(self):
        today = datetime.now(self.zone).date()
        conn = get_connection()
        try:
            rows = conn.execute("SELECT guild_id,opened_at,closed_at,active FROM tickets").fetchall()
        finally:
            conn.close()
        return compose_report(today, self.timezone_name, rows, self._guild_names(),
                              "TicketBridge Daily Report (so far)")

    def publish_missing(self):
        now_day = datetime.now(self.zone).date()
        yesterday = now_day - timedelta(days=1)
        conn = get_connection()
        try:
            with conn:
                state = conn.execute("SELECT last_report_date FROM daily_report_state WHERE state_id=1").fetchone()
                latest = conn.execute("SELECT MAX(report_date) FROM daily_reports").fetchone()[0]
                if state:
                    latest = max(latest, state[0]) if latest else state[0]
                elif latest:
                    conn.execute("INSERT INTO daily_report_state VALUES (1,?)", (latest,))
                else:
                    # A fresh install starts reporting at the next midnight.
                    latest = (now_day - timedelta(days=1)).isoformat()
                    conn.execute("INSERT INTO daily_report_state VALUES (1,?)", (latest,))
        finally:
            conn.close()
        next_day = date.fromisoformat(latest) + timedelta(days=1)

        published = 0
        while next_day <= yesterday:
            text = self._report_text(next_day)
            conn = get_connection()
            try:
                with conn:
                    exists = conn.execute("SELECT 1 FROM daily_reports WHERE report_date=?",
                                          (next_day.isoformat(),)).fetchone()
                    if not exists:
                        self.outbox.insert(conn, text, f"daily-report:{next_day.isoformat()}")
                        conn.execute("INSERT INTO daily_reports(report_date,created_at) VALUES (?,?)",
                                     (next_day.isoformat(), time.time()))
                        conn.execute("UPDATE daily_report_state SET last_report_date=? WHERE state_id=1",
                                     (next_day.isoformat(),))
                        published += 1
            finally:
                conn.close()
            next_day += timedelta(days=1)
        if published:
            self.outbox.wake.set()
        retention_start = datetime.combine(now_day - timedelta(days=90), day_time.min,
                                           tzinfo=self.zone).astimezone(timezone.utc).isoformat()
        conn = get_connection()
        try:
            with conn:
                conn.execute("DELETE FROM discovery_checks WHERE checked_at<?", (retention_start,))
        finally:
            conn.close()
        return published

    def seconds_until_midnight(self):
        now = datetime.now(self.zone)
        tomorrow = now.date() + timedelta(days=1)
        midnight = datetime.combine(tomorrow, day_time.min, tzinfo=self.zone)
        return max(1, midnight.timestamp() - now.timestamp())

    async def run(self):
        while True:
            try:
                count = self.publish_missing()
                if count:
                    logger.info("Queued %d daily ticket report(s).", count)
                now = datetime.now(self.zone)
                next_midnight = datetime.combine(now.date() + timedelta(days=1), day_time.min,
                                                 tzinfo=self.zone)
                logger.info("Daily report timezone=%s (UTC%s); next scheduled midnight=%s",
                            self.timezone_name, now.strftime("%z"), next_midnight.isoformat())
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Daily ticket report failed; it will be retried.")
                await asyncio.sleep(60)
                continue
            await asyncio.sleep(self.seconds_until_midnight())
