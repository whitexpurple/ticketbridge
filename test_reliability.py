import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from telegram.error import NetworkError, RetryAfter
import database
from custom_discord import CustomDiscordClient
from delivery import NotificationOutbox, TicketDelivery, chunks
from discord_handler import DiscordHandler
from ticket_discovery import TicketDiscovery, ticket_reason
from telegram_panel import status
from gateway_events import GatewayEvents


class ReliabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = patch.object(database, "DB_PATH", Path(self.temp.name) / "bot.db")
        self.db.start()
        database.init_db()
        self.bot = SimpleNamespace(send_message=AsyncMock())
        self.limiter = SimpleNamespace(wait_if_needed=AsyncMock())
        self.outbox = NotificationOutbox(self.bot, "42", self.limiter)
        self.client = CustomDiscordClient("test-token")
        self.client.user = {"id": "1", "username": "Tester"}
        self.client.guilds = {"10": {"id": "10", "name": "Support"}}
        self.client.fetch_guild_channels = Mock(return_value=[])
        self.handler = DiscordHandler(self.client, AsyncMock(), delivery=TicketDelivery(self.outbox, "1"))
        self.handler.extract_user_from_channel = Mock(return_value=(None, "Unknown", "Unknown"))
        self.discovery = TicketDiscovery(self.client, self.handler)
        self.key = str((self.discovery.started_at_ms + 1 - 1420070400000) << 22)
        self.channel = {"id": self.key, "guild_id": "10", "name": "ticket-123", "type": 12}
        self.client.channels[self.key] = self.channel

    async def asyncTearDown(self):
        await self.client.close()
        self.db.stop()
        self.temp.cleanup()

    def query(self, sql, params=()):
        conn = database.get_connection()
        try:
            with conn:
                return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    async def test_open_state_and_notification_roll_back_together(self):
        with patch.object(self.outbox, "insert", side_effect=RuntimeError("disk failure")):
            with self.assertRaises(RuntimeError):
                await self.discovery.consider(self.channel)
        self.assertIsNone(database.get_ticket(self.key))
        self.assertEqual(self.outbox.pending_count(), 0)
        await self.discovery.consider(self.channel)
        self.assertIsNotNone(database.get_ticket(self.key))
        self.assertEqual(self.outbox.pending_count(), 1)
        self.bot.send_message.assert_not_awaited()

    async def test_failed_delivery_survives_new_worker_and_deduplicates(self):
        self.outbox.enqueue("ticket notification", "open:123")
        self.bot.send_message.side_effect = NetworkError("test failure")
        self.assertFalse(await self.outbox.deliver_once())
        self.assertEqual(self.outbox.pending_count(), 1)
        self.query("UPDATE notification_outbox SET next_attempt=0")
        replacement = NotificationOutbox(self.bot, "42", self.limiter)
        self.bot.send_message.side_effect = None
        self.assertTrue(await replacement.deliver_once())
        replacement.enqueue("duplicate", "open:123")
        self.assertEqual(replacement.pending_count(), 0)
        self.assertEqual(self.query("SELECT text FROM notification_outbox"), [("",)])

    async def test_close_state_rolls_back_if_queue_write_fails(self):
        await self.discovery.consider(self.channel)
        with patch.object(self.outbox, "insert", side_effect=RuntimeError("disk failure")):
            with self.assertRaises(RuntimeError):
                await self.discovery.channel_deleted(self.channel)
        self.assertTrue(database.get_ticket(self.key)[6])
        self.assertEqual(self.outbox.pending_count(), 1)
        await self.discovery.channel_deleted(self.channel)
        self.assertFalse(database.get_ticket(self.key)[6])
        self.assertEqual(self.outbox.pending_count(), 2)

    async def test_gateway_event_persists_before_processing_and_replays(self):
        events = GatewayEvents("1", self.discovery)
        await events.callback("channel_created")(self.channel)
        self.assertIsNone(database.get_ticket(self.key))
        self.assertEqual(events.pending_count(), 1)
        restored = GatewayEvents("1", self.discovery)
        self.assertTrue(await restored.process_once())
        self.assertIsNotNone(database.get_ticket(self.key))
        self.assertEqual(restored.pending_count(), 0)

    async def test_failed_gateway_event_is_retained(self):
        events = GatewayEvents("1", self.discovery)
        await events.callback("channel_created")(self.channel)
        with patch.object(self.discovery, "channel_created", AsyncMock(side_effect=RuntimeError("test"))):
            self.assertFalse(await events.process_once())
        self.assertEqual(events.pending_count(), 1)
        self.query("UPDATE gateway_inbox SET next_attempt=0")
        self.assertTrue(await events.process_once())
        self.assertEqual(events.pending_count(), 0)

    async def test_retry_after_keeps_order_and_payload_is_split(self):
        self.outbox.enqueue("a" * 5000, "long-message")
        self.assertEqual(self.outbox.pending_count(), 2)
        self.bot.send_message.side_effect = RetryAfter(30)
        before = time.time()
        await self.outbox.deliver_once()
        self.assertGreaterEqual(self.query("SELECT next_attempt FROM notification_outbox ORDER BY id")[0][0], before + 30)
        self.assertFalse(await self.outbox.deliver_once())
        self.assertEqual(self.bot.send_message.await_count, 1)
        self.assertEqual("".join(chunks("\U0001f600" * 4000)), "\U0001f600" * 4000)
        self.assertTrue(all(len(part.encode("utf-16-le")) // 2 <= 3500 for part in chunks("\U0001f600" * 4000)))

    async def test_restart_restores_cutoff_and_active_ticket(self):
        await self.discovery.consider(self.channel)
        cutoff = self.discovery.started_at_ms
        restored = TicketDiscovery(self.client, self.handler)
        self.assertEqual(restored.started_at_ms, cutoff)
        self.assertIn(self.key, restored.tracked)
        self.assertFalse(restored.is_live_channel(str((cutoff - 1 - 1420070400000) << 22)))
        await restored.channel_deleted(self.channel)
        self.assertFalse(database.get_ticket(self.key)[6])
        self.assertEqual(self.outbox.pending_count(), 2)

    async def test_live_messages_work_without_configuration_and_have_no_mutuals(self):
        await self.discovery.consider(self.channel)
        message = {"id": str(int(self.key) + 1), "guild_id": "10", "channel_id": self.key,
                   "author": {"id": "99", "username": "Alice"}, "content": "Need help"}
        await self.discovery.message_created(message)
        await self.discovery.message_created(message)
        rows = self.query("SELECT text FROM notification_outbox ORDER BY id")
        self.assertEqual(len(rows), 2)
        self.assertIn("Need help", rows[1][0])
        self.assertNotIn("Mutuals", "".join(row[0] for row in rows))
        self.assertTrue(database.get_ticket(self.key)[7])

    async def test_gateway_message_does_not_skip_missing_earlier_messages(self):
        await self.discovery.consider(self.channel)
        def message(offset):
            return {"id": str(int(self.key) + offset), "guild_id": "10", "channel_id": self.key,
                    "author": {"id": "99", "username": "Alice"}, "content": f"message {offset}"}
        await self.discovery.message_created(message(2))
        self.assertIsNone(self.discovery.message_cursor(self.key))
        def fetch(path):
            if path.endswith("/channels"):
                return []
            if path.endswith("threads/active"):
                return {"threads": [self.channel]}
            if "/messages?after=" in path:
                return [message(2), message(1)]
            return self.channel
        self.client._api_get = fetch
        await self.discovery.scan_once()
        rows = self.query("SELECT text FROM notification_outbox ORDER BY id")
        self.assertEqual(len(rows), 3)
        self.assertEqual(self.discovery.message_cursor(self.key), str(int(self.key) + 2))

    async def test_private_thread_controls_and_close_message(self):
        self.channel.update(name="customer-alice")
        opening = {"author": {"bot": True}, "components": [{"components": [{"label": "Close ticket"}]}]}
        self.assertIsNotNone(ticket_reason(self.channel, self.client, opening))
        await self.discovery.consider(self.channel, opening)
        await self.discovery.message_created({"id": str(int(self.key) + 1), "guild_id": "10",
            "channel_id": self.key, "author": {"id": "8", "bot": True},
            "embeds": [{"description": "This ticket has been closed"}]})
        self.assertFalse(database.get_ticket(self.key)[6])
        self.assertEqual(self.outbox.pending_count(), 2)
        panel = {**self.channel, "name": "ticket-panel"}
        self.assertIsNone(ticket_reason(panel, self.client, opening))

    async def test_user_lookup_negative_cache_and_positive_cache(self):
        self.client.bot_token = "bot-test-token"
        with patch.object(self.client, "_api_get", return_value=None) as api:
            self.assertIsNone(self.client.get_user("222"))
            self.assertIsNone(self.client.get_user("222"))
            api.assert_called_once_with("/api/v10/users/222", use_bot_token=True)
        with patch.object(self.client, "_api_get", return_value={"id": "333", "username": "Alice"}) as api:
            self.assertEqual(self.client.get_user("333")["username"], "Alice")
            self.client.get_user("333")
            api.assert_called_once()

    async def test_status_reports_scan_and_delivery_health(self):
        session = SimpleNamespace(client=self.client, discovery=self.discovery, connection_error=None, events=None)
        app = SimpleNamespace(bot_data={"discord_session": session, "notification_outbox": self.outbox})
        update = SimpleNamespace(effective_chat=SimpleNamespace(id=42, type="private"),
            effective_user=SimpleNamespace(id=42), effective_message=SimpleNamespace(reply_text=AsyncMock()))
        with patch("telegram_panel.is_authorized", return_value=True):
            await status(update, SimpleNamespace(application=app))
        text = update.effective_message.reply_text.await_args.args[0]
        self.assertIn("Pending notifications: 0", text)
        self.assertIn("Last completed scan: Not completed yet", text)


if __name__ == "__main__":
    unittest.main()
