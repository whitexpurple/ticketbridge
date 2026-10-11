import asyncio
import contextlib
import logging
import os
import time
from collections import deque
from pathlib import Path

from dotenv import load_dotenv
from telegram.ext import Application
from telegram.request import HTTPXRequest

from database import init_db
from discord_accounts import DiscordAccounts
from delivery import NotificationOutbox
from daily_report import DailyReportWorker
from health_monitor import HealthMonitor
from server_lists import ServerLists
from telegram_panel import register_handlers

logger = logging.getLogger("ticket-forwarder")


class RateLimiter:
    def __init__(self, max_per_minute=20, min_delay=1.5):
        self.max_per_minute = max(1, max_per_minute)
        self.min_delay = max(0, min_delay)
        self.timestamps = deque()
        self._lock = asyncio.Lock()

    async def wait_if_needed(self):
        async with self._lock:
            while True:
                now = time.monotonic()
                while self.timestamps and now - self.timestamps[0] >= 60:
                    self.timestamps.popleft()
                delay = 0
                if len(self.timestamps) >= self.max_per_minute:
                    delay = 60 - (now - self.timestamps[0])
                if self.timestamps:
                    delay = max(delay, self.min_delay - (now - self.timestamps[-1]))
                if delay <= 0:
                    break
                await asyncio.sleep(delay)
            self.timestamps.append(time.monotonic())


async def main():
    load_dotenv(Path(__file__).with_name(".env"))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram.ext.Application").setLevel(logging.WARNING)
    for name in ("discord-handler", "custom-discord"):
        logging.getLogger(name).setLevel(os.getenv("LOG_LEVEL", "WARNING").upper())
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("Set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in .env first.")
    chat_id = int(chat_id)
    init_db()
    app = (Application.builder().token(token)
           .request(HTTPXRequest(connect_timeout=30, read_timeout=60))
           .get_updates_request(HTTPXRequest(connect_timeout=30, read_timeout=60)).build())
    limiter = RateLimiter(int(os.getenv("MAX_MESSAGES_PER_MINUTE", "20")),
                          float(os.getenv("RATE_LIMIT_DELAY", "1.5")))
    app.bot_data["telegram_rate_limiter"] = limiter

    async def send(text, entities=None):
        await limiter.wait_if_needed()
        preview_option = {"disable_web_page_preview": True} if entities else {}
        await app.bot.send_message(chat_id=chat_id, text=text, entities=entities,
                                   **preview_option)

    outbox = NotificationOutbox(app.bot, chat_id, limiter)
    app.bot_data["notification_outbox"] = outbox
    server_lists = ServerLists(app.bot)
    app.bot_data["server_lists"] = server_lists

    session = DiscordAccounts(app, send, limiter, os.getenv("DISCORD_BOT_TOKEN"),
                              float(os.getenv("TICKET_SCAN_INTERVAL", "60")))
    daily_reports = DailyReportWorker(
        outbox,
        os.getenv("DAILY_REPORT_TIMEZONE", "Etc/GMT-1"),
        lambda: [guild for account in session.summaries() for guild in account["guilds"]],
        session.summaries,
    )
    app.bot_data["daily_report_worker"] = daily_reports
    health_monitor = HealthMonitor(session, outbox)
    app.bot_data["discord_session"] = session
    register_handlers(app)
    async with app:
        delivery_task = None
        server_lists_task = None
        daily_reports_task = None
        health_monitor_task = None
        try:
            await app.start()
            await app.updater.start_polling(bootstrap_retries=-1)
            logger.info("Telegram ready. Use /connect to connect Discord.")
            try:
                await session.start_saved()
                logger.info("Loaded %d saved Discord account(s).", len(session.sessions))
            except Exception as error:
                logger.error("Discord account store could not be initialized: %s", error)
            delivery_task = asyncio.create_task(outbox.run())
            server_lists_task = asyncio.create_task(server_lists.run())
            daily_reports_task = asyncio.create_task(daily_reports.run())
            health_monitor_task = asyncio.create_task(health_monitor.run())
            await asyncio.Event().wait()
        finally:
            await session.close()
            if delivery_task:
                delivery_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await delivery_task
            if server_lists_task:
                server_lists_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await server_lists_task
            if daily_reports_task:
                daily_reports_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await daily_reports_task
            if health_monitor_task:
                health_monitor_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await health_monitor_task
            if app.updater.running:
                await app.updater.stop()
            if app.running:
                await app.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
