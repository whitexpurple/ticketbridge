import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import database
from custom_discord import CustomDiscordClient, GatewayConnectionError
from discord_handler import DiscordHandler
from discord_session import DiscordSession, login_request, _post_auth
from telegram_panel import begin_setup, clear_setup, is_authorized, setup_message
from ticket_discovery import TicketDiscovery, ticket_reason


def channel(id="123", guild="1", name="ticket-42", **extra):
    return {"id": id, "guild_id": guild, "name": name, "type": 0, **extra}


class DiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(database, "DB_PATH", Path(self.temp.name) / "test.db")
        self.db_patch.start()
        database.init_db()
        self.client = CustomDiscordClient("test-only")
        self.client.guilds = {"1": {"id": "1", "name": "One"}, "2": {"id": "2", "name": "Two"}}
        self.send = AsyncMock()
        self.handler = DiscordHandler(self.client, self.send)
        self.handler.extract_user_from_channel = Mock(return_value=(None, "Unknown", "Unknown"))
        self.discovery = TicketDiscovery(self.client, self.handler)
        self.discovery.started_at_ms = 0

    async def asyncTearDown(self):
        await self.client.close()
        self.db_patch.stop()
        self.temp.cleanup()

    async def test_no_configuration_required_and_deduplication(self):
        item = channel()
        await asyncio.gather(self.discovery.consider(item), self.discovery.consider(item))
        self.assertIsNotNone(database.get_ticket("123"))
        self.send.assert_awaited_once()
        self.assertEqual(database.get_servers(), [])

    async def test_old_tickets_silent_even_when_discovered_later(self):
        self.discovery.started_at_ms = 1800000000000
        old_id = str((self.discovery.started_at_ms - 1 - 1420070400000) << 22)
        new_id = str((self.discovery.started_at_ms + 1 - 1420070400000) << 22)
        self.client._api_get = Mock(side_effect=lambda path: (
            [channel(id=old_id), channel(id=new_id)] if path.endswith("/channels") else {"threads": []}))
        await self.discovery.scan_once()
        self.assertIsNone(database.get_ticket(old_id))
        self.assertIsNotNone(database.get_ticket(new_id))
        self.send.assert_awaited_once()
        self.assertFalse(any("/messages" in call.args[0] for call in self.client._api_get.call_args_list))

    async def test_old_tracked_ticket_messages_updates_and_closures_silent(self):
        await self.discovery.consider(channel())
        self.send.reset_mock()
        self.discovery.started_at_ms = 1800000000000
        self.client._api_get = Mock(side_effect=AssertionError("Old tickets must not fetch history"))
        await self.discovery.message_created({"guild_id": "1", "channel_id": "123",
                                              "author": {"id": "9"}, "content": "new message in old ticket"})
        await self.discovery.channel_updated(channel(name="closed-ticket-42"))
        await self.discovery.channel_deleted(channel())
        self.send.assert_not_awaited()
        self.assertTrue(database.get_ticket("123")[6])

    async def test_cutoff_checks_creation_not_first_observation(self):
        self.discovery.started_at_ms = 1800000000000
        self.assertFalse(self.discovery.is_live_channel("bad-id"))
        for offset, expected in ((-1, False), (0, True), (1, True)):
            channel_id = str((self.discovery.started_at_ms + offset - 1420070400000) << 22)
            self.assertEqual(self.discovery.is_live_channel(channel_id), expected)

    async def test_discovered_ticket_closes_without_server_configuration(self):
        await self.discovery.consider(channel())
        self.send.reset_mock()
        await asyncio.gather(self.discovery.channel_deleted(channel()),
                             self.discovery.channel_deleted(channel()))
        self.assertFalse(database.get_ticket("123")[6])
        self.send.assert_awaited_once()
        self.assertIn("One", self.send.await_args.args[0])
        self.assertEqual(database.get_servers(), [])

    async def test_closed_rename_sends_one_notification(self):
        await self.discovery.consider(channel())
        self.send.reset_mock()
        await self.discovery.channel_updated(channel(name="closed-ticket-42"))
        await self.discovery.channel_deleted(channel())
        self.assertFalse(database.get_ticket("123")[6])
        self.send.assert_awaited_once()

    async def test_only_locked_archival_closes_thread(self):
        await self.discovery.consider(channel(type=12))
        self.send.reset_mock()
        await self.discovery.channel_updated(channel(type=12, thread_metadata={"archived": True, "locked": False}))
        self.assertTrue(database.get_ticket("123")[6])
        self.send.assert_not_awaited()
        await self.discovery.channel_updated(channel(type=12, thread_metadata={"archived": True, "locked": True}))
        self.assertFalse(database.get_ticket("123")[6])
        self.send.assert_awaited_once()

    async def test_scans_every_guild_and_retries_failed_fetch(self):
        def fetch(path):
            if path.endswith("/channels"):
                guild = path.split("/")[4]
                return [{"id": guild + "99", "name": "ticket-42", "type": 0}]
            return {"threads": []}
        self.client._api_get = Mock(return_value=None)
        await self.discovery.scan_once()
        self.client._api_get.side_effect = fetch
        await self.discovery.scan_once()
        self.assertIsNotNone(database.get_ticket("199"))
        self.assertIsNotNone(database.get_ticket("299"))
        await self.discovery.scan_once()
        self.assertEqual(self.send.await_count, 2)

    async def test_app_embed_discovers_custom_private_thread(self):
        item = channel(name="customer-alice", type=12)
        self.client.channels["123"] = item
        message = {"guild_id": "1", "channel_id": "123", "author": {"id": "99", "bot": True},
                   "embeds": [{"title": "Welcome to your ticket", "description": "A helper will respond."}]}
        await self.discovery.message_created(message)
        self.assertIsNotNone(database.get_ticket("123"))

    async def test_mee6_role_private_channel_with_custom_name(self):
        item = channel(name="alice-request", permission_overwrites=[
            {"id": "1", "type": 0, "deny": "1024", "allow": "0"},
            {"id": "staff", "type": 0, "deny": "0", "allow": "1024"}])
        opening = {"author": {"bot": True}, "embeds": [{"title": "Ticket created"}]}
        self.assertIsNone(ticket_reason(item, self.client))
        self.assertIn("private text channel", ticket_reason(item, self.client, opening))
        await self.discovery.consider(item, opening)
        self.assertIsNotNone(database.get_ticket("123"))
        self.send.assert_awaited_once()

    async def test_mee6_numbered_private_channel_detected_without_opening_message(self):
        item = channel(name="425-sampleuser", permission_overwrites=[
            {"id": "1", "type": 0, "deny": "1024", "allow": "0"}])
        await self.discovery.channel_created(item)
        await self.discovery.channel_created(item)
        self.assertIsNotNone(database.get_ticket("123"))
        self.send.assert_awaited_once()
        self.assertEqual(ticket_reason(item, self.client), "numbered private ticket channel name")

    async def test_mee6_numbered_channel_member_access_in_support_category(self):
        self.client.channels["88"] = channel(id="88", name="SUPPORT", type=4)
        item = channel(name="425-sampleuser", parent_id="88", permission_overwrites=[
            {"id": "9", "type": 1, "allow": "1024"}])
        await self.discovery.channel_created(item)
        self.assertIsNotNone(database.get_ticket("123"))

    async def test_numbered_public_channels_and_old_private_tickets_ignored(self):
        self.assertIsNone(ticket_reason(channel(name="425-sampleuser"), self.client))
        self.assertIsNone(ticket_reason(channel(name="2026-roadmap", permission_overwrites=[
            {"id": "other-role", "type": 0, "deny": "1024"}]), self.client))
        item = channel(name="425-sampleuser", permission_overwrites=[
            {"id": "1", "type": 0, "deny": "1024"}])
        self.discovery.started_at_ms = 1800000000000
        await self.discovery.channel_created(item)
        self.assertIsNone(database.get_ticket("123"))
        self.send.assert_not_awaited()

    async def test_mava_nested_thread_ticket_controls(self):
        self.client.channels["88"] = channel(id="88", name="community")
        item = channel(name="alice-request", type=12, parent_id="88")
        opening = {"author": {"bot": True}, "components": [{"components": [
            {"custom_id": "ticket:close", "label": "Resolve"}]}]}
        self.assertIsNone(ticket_reason(item, self.client))
        self.assertIn("private thread", ticket_reason(item, self.client, opening))
        await self.discovery.consider(item, opening)
        self.assertIsNotNone(database.get_ticket("123"))

    async def test_mava_username_thread_under_ticket_panel_detects_on_creation(self):
        self.client.channels["88"] = channel(id="88", name="support_tickets")
        self.discovery.fetch = AsyncMock(side_effect=AssertionError("No history needed"))
        self.client.on_guild_channel_create = self.discovery.channel_created
        payload = {"op": 0, "t": "THREAD_CREATE", "d": channel(
            name="sampleuser", type=12, parent_id="88")}
        await self.client._handle_message(json.dumps(payload))
        await self.client._handle_message(json.dumps(payload))
        self.assertIsNotNone(database.get_ticket("123"))
        self.send.assert_awaited_once()
        self.discovery.fetch.assert_not_awaited()

    async def test_mava_ticket_parent_variants_and_non_ticket_threads(self):
        item = channel(name="anotheruser", type=12, parent_id="88")
        for name in ("support_tickets", "support-tickets", "Support Tickets", "open-ticket"):
            self.client.channels["88"] = channel(id="88", name=name)
            self.assertEqual(ticket_reason(item, self.client), "private thread under ticket channel")
            self.assertIsNone(ticket_reason({**item, "type": 11}, self.client))
            self.assertIsNone(ticket_reason({**item, "thread_metadata": {"archived": True}}, self.client))
        for name in ("support", "support-general", "ticket-logs", "ticket-transcripts", "community"):
            self.client.channels["88"] = channel(id="88", name=name)
            self.assertIsNone(ticket_reason(item, self.client))

    async def test_mava_missing_parent_is_fetched_and_cached(self):
        self.discovery.fetch = AsyncMock(return_value=channel(id="88", name="support_tickets"))
        await self.discovery.channel_created(channel(name="sampleuser", type=12, parent_id="88"))
        self.discovery.fetch.assert_awaited_once_with("/api/v10/channels/88")
        self.assertEqual(self.client.get_channel("88")["name"], "support_tickets")
        self.assertIsNotNone(database.get_ticket("123"))

    async def test_mava_old_thread_does_not_fetch_parent_or_notify(self):
        self.discovery.started_at_ms = 1800000000000
        self.discovery.fetch = AsyncMock(side_effect=AssertionError("Old ticket"))
        await self.discovery.channel_created(channel(name="sampleuser", type=12, parent_id="88"))
        self.discovery.fetch.assert_not_awaited()
        self.send.assert_not_awaited()

    async def test_mava_unavailable_parent_retries_with_cooldown(self):
        item = channel(name="sampleuser", type=12, parent_id="88")
        self.discovery.fetch = AsyncMock(return_value=None)
        await self.discovery.consider(item)
        await self.discovery.consider(item)
        self.discovery.fetch.assert_awaited_once()
        self.send.assert_not_awaited()
        self.discovery.parent_retry_after["88"] = 0
        self.discovery.fetch.return_value = channel(id="88", name="support_tickets")
        await self.discovery.consider(item)
        self.assertIsNotNone(database.get_ticket("123"))

    async def test_private_layout_alone_or_human_text_is_not_ticket_evidence(self):
        for item in (channel(name="staff-chat", permission_overwrites=[
                {"id": "1", "type": 0, "deny": "1024"}]),
                channel(name="project-chat", type=12, parent_id="88")):
            for message in ({"author": {"bot": True}, "content": "Welcome everyone"},
                            {"author": {"bot": False}, "content": "Ticket created"}):
                self.assertIsNone(ticket_reason(item, self.client, message))
        opening = {"author": {"bot": True}, "content": "Ticket created"}
        self.assertIsNone(ticket_reason(channel(name="public-chat", permission_overwrites=[
            {"id": "other-role", "type": 0, "deny": "1024"}]), self.client, opening))

    async def test_delayed_app_opening_is_probed_promptly_for_both_layouts(self):
        items = [channel(name="alice-request", permission_overwrites=[
                     {"id": "1", "type": 0, "deny": "1024"}]),
                 channel(id="124", name="bob-request", type=12, parent_id="88")]
        self.client.guilds = {"1": self.client.guilds["1"]}
        greeted = False
        async def fetch(path):
            if path.endswith("/channels"):
                return [items[0]]
            if path.endswith("/threads/active"):
                return {"threads": [items[1]]}
            return [{"author": {"bot": True}, "content": "Welcome to your ticket"}] if greeted else []
        self.discovery.fetch = AsyncMock(side_effect=fetch)
        with patch("ticket_discovery.asyncio.sleep", new_callable=AsyncMock):
            await self.discovery.scan_once()
            self.assertEqual(set(self.discovery.early_probes), {"123", "124"})
            self.assertLessEqual(self.discovery.scan_delay(), 16)
            self.discovery.fetch.reset_mock()
            await self.discovery.scan_once()
            self.assertFalse(any("/messages" in call.args[0] for call in self.discovery.fetch.call_args_list))
            greeted = True
            self.discovery.early_probes = {"123": 0, "124": 0}
            await self.discovery.scan_once()
        self.assertIsNotNone(database.get_ticket("123"))
        self.assertIsNotNone(database.get_ticket("124"))
        self.assertEqual(self.discovery.early_probes, {})
        self.assertEqual(self.send.await_count, 2)

    async def test_creation_wakes_scanner_but_old_threads_do_not(self):
        self.discovery.fetch = AsyncMock(return_value=[])
        await self.discovery.channel_created(channel(name="alice-request", type=12))
        self.assertTrue(self.discovery.wake.is_set())
        self.discovery.wake.clear()
        self.discovery.started_at_ms = 1800000000000
        await self.discovery.channel_created(channel(name="alice-request", type=12))
        self.assertFalse(self.discovery.wake.is_set())

    async def test_creation_probes_earliest_messages_without_waiting_for_scan(self):
        item = channel(name="customer-request", permission_overwrites=[
            {"id": "1", "type": 0, "deny": "1024"}])
        opening = {"author": {"bot": True}, "embeds": [{"title": "Ticket created"}]}
        self.discovery.fetch = AsyncMock(return_value=[opening])
        await self.discovery.channel_created(item)
        self.discovery.fetch.assert_awaited_once_with("/api/v10/channels/123/messages?after=123&limit=25")
        self.assertIsNotNone(database.get_ticket("123"))
        self.send.assert_awaited_once()

    async def test_concurrent_opening_probes_share_cooldown(self):
        item = channel(name="customer-request")
        async def fetch(path):
            await asyncio.sleep(0)
            return []
        self.discovery.fetch = AsyncMock(side_effect=fetch)
        await asyncio.gather(self.discovery.probe_opening(item), self.discovery.probe_opening(item))
        self.discovery.fetch.assert_awaited_once()
        self.assertIn("123", self.discovery.early_probes)

    async def test_late_permission_update_detects_numbered_mee6_ticket(self):
        self.discovery.fetch = AsyncMock(return_value=[])
        self.client.on_guild_channel_create = self.discovery.channel_created
        self.client.on_guild_channel_update = self.discovery.channel_updated
        await self.client._handle_message(json.dumps({"op": 0, "t": "CHANNEL_CREATE",
            "d": channel(name="426-sampleuser")}))
        self.assertIsNone(database.get_ticket("123"))
        await self.client._handle_message(json.dumps({"op": 0, "t": "CHANNEL_UPDATE",
            "d": {"id": "123", "permission_overwrites": [
                {"id": "1", "type": 0, "deny": "1024"}]}}))
        self.assertIsNotNone(database.get_ticket("123"))
        self.send.assert_awaited_once()

    async def test_early_probes_are_bounded_and_failed_requests_back_off(self):
        self.client.guilds = {"1": self.client.guilds["1"]}
        item = channel(name="ordinary-private-chat", type=12)
        async def fetch(path):
            if path.endswith("/channels"):
                return []
            if path.endswith("/threads/active"):
                return {"threads": [item]}
            return []
        self.discovery.fetch = AsyncMock(side_effect=fetch)
        with patch("ticket_discovery.asyncio.sleep", new_callable=AsyncMock):
            for attempt in range(4):
                if attempt:
                    self.discovery.early_probes["123"] = 0
                await self.discovery.scan_once()
            self.assertEqual(self.discovery.early_probes, {})
            self.discovery.fetch.reset_mock()
            await self.discovery.scan_once()
            self.assertFalse(any("/messages" in call.args[0] for call in self.discovery.fetch.call_args_list))
            self.discovery.early_probes["123"] = 0
            original_fetch = fetch
            async def failed_history(path):
                return None if "/messages" in path else await original_fetch(path)
            self.discovery.fetch.side_effect = failed_history
            await self.discovery.scan_once()
            self.assertEqual(self.discovery.early_probes, {})
        self.send.assert_not_awaited()

    async def test_ordinary_channels_panels_and_archived_threads_ignored(self):
        for item in (channel(name="general"), channel(name="ticket-panel"),
                     channel(name="support-chat"), channel(type=12, thread_metadata={"archived": True}),
                     channel(type=4), channel(name="closed-ticket-42")):
            self.assertIsNone(ticket_reason(item, self.client))
        bot_message = {"author": {"bot": True}, "content": "Click here to create a ticket"}
        self.assertIsNone(ticket_reason(channel(name="support"), self.client, bot_message))

    async def test_rest_does_not_block_event_loop(self):
        ticks = []
        def fetch(path):
            time.sleep(0.03)
            return []
        self.client._api_get = fetch
        async def ticker():
            for _ in range(5):
                await asyncio.sleep(0.005)
                ticks.append(True)
        await asyncio.gather(self.discovery.scan_once(), ticker())
        self.assertEqual(len(ticks), 5)

    async def test_gateway_ready_and_thread_sync_normalize_guild(self):
        self.client.on_guild_channel_create = self.discovery.channel_created
        await self.client._handle_message(json.dumps({"op": 0, "t": "READY", "d": {
            "user": {"id": "9", "username": "Tester"},
            "guilds": [{"id": "1", "channels": [{"id": "88", "type": 0, "name": "support"}]}]}}))
        self.assertTrue(self.client.ready.is_set())
        self.assertEqual(self.client.channels["88"]["guild_id"], "1")
        await self.client._handle_message(json.dumps({"op": 0, "t": "THREAD_LIST_SYNC", "d": {
            "guild_id": "1", "threads": [channel(type=12)]}}))
        self.assertIsNotNone(database.get_ticket("123"))


class SetupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_db = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(database, "DB_PATH", Path(self.temp_db.name) / "test.db")
        self.db_patch.start()
        database.init_db()
        self.env = patch.dict(os.environ, {"TELEGRAM_CHAT_ID": "42"})
        self.env.start()
        self.message = SimpleNamespace(text="", delete=AsyncMock(), reply_text=AsyncMock())
        self.update = SimpleNamespace(effective_chat=SimpleNamespace(id=42, type="private"),
                                      effective_user=SimpleNamespace(id=42), effective_message=self.message)
        self.session = SimpleNamespace(connect=AsyncMock(return_value="Tester"))
        self.context = SimpleNamespace(user_data={}, application=SimpleNamespace(
            bot_data={"discord_session": self.session}))

    async def asyncTearDown(self):
        clear_setup(self.context)
        self.env.stop()
        self.db_patch.stop()
        self.temp_db.cleanup()

    async def test_password_mfa_token_flow(self):
        begin_setup(self.context)
        self.message.text = "test@example.invalid"
        await setup_message(self.update, self.context)
        self.message.text = "test-password"
        with patch("telegram_panel.login_request", AsyncMock(return_value={"ticket": "mfa-ticket"})) as login:
            await setup_message(self.update, self.context)
            self.assertEqual(login.await_args.args[1]["password"], "test-password")
        self.assertNotIn("email", self.context.user_data["discord_setup"])
        self.message.text = "123456"
        with patch("telegram_panel.login_request", AsyncMock(return_value={"token": "test-token"})) as login:
            await setup_message(self.update, self.context)
            self.assertEqual(login.await_args.args[0], "mfa/totp")
        self.session.connect.assert_awaited_once_with("test-token")
        self.assertNotIn("discord_setup", self.context.user_data)
        self.assertEqual(self.message.delete.await_count, 3)

    async def test_other_user_and_group_cannot_enter_setup(self):
        self.update.effective_user.id = 99
        self.assertFalse(is_authorized(self.update))
        self.update.effective_user.id = 42
        self.update.effective_chat.type = "group"
        self.assertFalse(is_authorized(self.update))

    async def test_expired_setup_never_logs_in(self):
        begin_setup(self.context)
        self.context.user_data["discord_setup"]["expires"] = 0
        await setup_message(self.update, self.context)
        self.session.connect.assert_not_awaited()
        self.assertNotIn("discord_setup", self.context.user_data)

    async def test_captcha_and_rate_limit_reported(self):
        for code, data, expected in ((400, {"captcha_key": ["required"]}, "verification"),
                                     (429, {}, "rate-limited"), (401, {}, "rejected")):
            with patch("discord_session._post_auth", return_value=(code, json.dumps(data).encode())):
                result = await login_request("login", {"login": "test", "password": "test"})
            self.assertIn(expected, result["error"])

    async def test_original_login_headers_and_ticket_only_mfa_response(self):
        connection = Mock()
        connection.getresponse.return_value.status = 200
        connection.getresponse.return_value.read.return_value = b'{"ticket":"test-mfa-ticket"}'
        with patch("discord_session.http.client.HTTPSConnection", return_value=connection):
            result = await login_request("login", {"login": "test@example.invalid",
                                                  "password": "test-only", "undelete": False})
        method, path, body, headers = connection.request.call_args.args
        self.assertEqual((method, path), ("POST", "/api/v9/auth/login"))
        self.assertIn("Chrome/135.0.0.0", headers["User-Agent"])
        self.assertIn("X-Super-Properties", headers)
        self.assertEqual(json.loads(body)["login_source"], None)
        self.assertEqual(json.loads(body)["gift_code_sku_id"], None)
        self.assertEqual(int(headers["Content-Length"]), len(body.encode()))
        connection.close.assert_called_once()
        self.assertEqual(result, {"ticket": "test-mfa-ticket"})

    async def test_auth_progress_logs_omit_secrets(self):
        with patch("discord_session._post_auth", return_value=(200, b'{"token":"secret-session-value"}')), \
             self.assertLogs("discord_session", level="INFO") as logs:
            await login_request("login", {"login": "private@example.invalid", "password": "private-password"})
        output = "\n".join(logs.output)
        self.assertIn("stage=password", output)
        self.assertIn("status=200", output)
        self.assertIn("authentication accepted", output)
        for secret in ("secret-session-value", "private@example.invalid", "private-password"):
            self.assertNotIn(secret, output)

    async def test_nested_login_error_is_visible_without_response_message(self):
        response = {"code": 50035, "errors": {"login": {"_errors": [
            {"code": "INVALID_LOGIN", "message": "private@example.invalid private-password"}
        ]}}}
        with patch("discord_session._post_auth", return_value=(400, json.dumps(response).encode())), \
             self.assertLogs("discord_session", level="INFO") as logs:
            result = await login_request("login", {"login": "private@example.invalid", "password": "private-password"})
        output = "\n".join(logs.output) + result["error"]
        self.assertIn("INVALID_LOGIN", output)
        self.assertNotIn("private@example.invalid", output)
        self.assertNotIn("private-password", output)

    async def test_password_rejection_keeps_email_for_retry(self):
        begin_setup(self.context)
        self.message.text = "test@example.invalid"
        await setup_message(self.update, self.context)
        self.message.text = "incorrect-test-password"
        with patch("telegram_panel.login_request", AsyncMock(return_value={"error": "Wrong password", "retry": True})):
            await setup_message(self.update, self.context)
        self.assertEqual(self.context.user_data["discord_setup"]["email"], "test@example.invalid")
        self.assertEqual(self.context.user_data["discord_setup"]["step"], "password")
        self.session.connect.assert_not_awaited()

    async def test_invalid_mfa_can_retry_same_challenge(self):
        begin_setup(self.context)
        self.context.user_data["discord_setup"].update(step="mfa", ticket="challenge")
        self.message.text = "111111"
        with patch("telegram_panel.login_request", AsyncMock(return_value={"error": "Invalid code", "retry": True})):
            await setup_message(self.update, self.context)
        self.assertEqual(self.context.user_data["discord_setup"]["ticket"], "challenge")
        self.message.text = "222222"
        with patch("telegram_panel.login_request", AsyncMock(return_value={"token": "test-token"})) as login:
            await setup_message(self.update, self.context)
            self.assertEqual(login.await_args.args[1], {"ticket": "challenge", "code": "222222"})
        self.session.connect.assert_awaited_once()

    async def test_failed_account_switch_preserves_current_client(self):
        app = SimpleNamespace(bot_data={})
        session = DiscordSession(app, AsyncMock(), None)
        old = Mock()
        session.client = old
        with patch.object(CustomDiscordClient, "start", AsyncMock(side_effect=GatewayConnectionError("test rejection"))), \
             patch("discord_session.ENV_PATH"):
            with self.assertRaises(ValueError):
                await session.connect("invalid-test-token")
        self.assertIs(session.client, old)
        old.close.assert_not_called()

    async def test_gateway_rejection_reported_without_waiting_for_timeout(self):
        app = SimpleNamespace(bot_data={})
        session = DiscordSession(app, AsyncMock(), None)
        old = SimpleNamespace(close=AsyncMock())
        session.client = old
        with patch.object(CustomDiscordClient, "_api_get", return_value={"id": "9"}), \
             patch.object(CustomDiscordClient, "start", AsyncMock(side_effect=GatewayConnectionError(
                 "Gateway closed: received=4013 sent=4013; invalid intents."))), \
             patch("discord_session.ENV_PATH"):
            with self.assertRaisesRegex(ValueError, "4013"):
                await asyncio.wait_for(session.connect("test-token"), timeout=2)
            self.assertIs(session.client, old)
            old.close.assert_not_awaited()

    async def test_successful_session_reconnect_stops_its_previous_client(self):
        app = SimpleNamespace(bot_data={})
        session = DiscordSession(app, AsyncMock(), None)
        old = SimpleNamespace(close=AsyncMock())
        session.client = old
        async def fake_start(client):
            client.user = {"id": "9", "username": "NewAccount"}
            client.ready.set()
            await asyncio.Event().wait()
        with patch.object(CustomDiscordClient, "_api_get", side_effect=AssertionError("Unexpected account preflight")), \
             patch.object(CustomDiscordClient, "start", fake_start), \
             patch("discord_session.TicketDiscovery.run", AsyncMock()):
            result = await session.connect("new-test-token")
            self.assertEqual(result, "NewAccount")
            old.close.assert_awaited_once()
            self.assertIs(app.bot_data["discord_client"], session.client)
            await session.close()

    async def test_gateway_session_does_not_write_tokens_to_env(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / ".env"
            session = DiscordSession(SimpleNamespace(bot_data={}), AsyncMock(), None)
            with patch("discord_session.ENV_PATH", env), \
                 patch.object(CustomDiscordClient, "start", AsyncMock(side_effect=GatewayConnectionError("test rejection"))):
                with self.assertRaises(ValueError):
                    await session.connect("test-issued-token")
            from dotenv import dotenv_values
            self.assertNotIn("DISCORD_TOKEN", dotenv_values(env))

    async def test_gateway_identifies_once_before_hello(self):
        client = CustomDiscordClient("test-session")
        events = []
        class Socket:
            async def close(self):
                pass
            async def send(self, payload):
                events.append(json.loads(payload)["op"])
            def __aiter__(self):
                return self.messages()
            async def messages(self):
                self_test.assertEqual(events, [2])
                yield json.dumps({"op": 10, "d": {"heartbeat_interval": 41250}})
                client._running = False
        self_test = self
        context = AsyncMock()
        context.__aenter__.return_value = Socket()
        with patch("custom_discord.websockets.connect", return_value=context):
            await client.start()
        self.assertEqual(events, [2])
        await client.close()

    async def test_large_ready_passes_actual_websocket_parser(self):
        from websockets.client import ClientProtocol
        from websockets.frames import Frame, Opcode
        from websockets.protocol import State
        from websockets.uri import parse_uri
        client = CustomDiscordClient("test-session")
        payload = json.dumps({"op": 0, "t": "READY", "d": {
            "user": {"id": "9", "username": "Test"}, "guilds": [{"id": "1"}],
            "test_padding": "x" * (2 * 1024 * 1024),
        }}).encode()
        limits = []
        self_test = self
        class Socket:
            async def send(self, payload):
                pass
            async def close(self):
                pass
            def __aiter__(self):
                return self.messages()
            async def messages(self):
                parser = ClientProtocol(parse_uri("ws://localhost/"), state=State.OPEN,
                                        max_size=limits[0])
                parser.receive_data(Frame(Opcode.TEXT, payload).serialize(mask=False))
                self_test.assertIsNone(parser.parser_exc)
                frames = parser.events_received()
                self_test.assertEqual(len(frames), 1)
                yield frames[0].data.decode()
                self_test.assertTrue(client.ready.is_set())
                self_test.assertEqual(client.user["username"], "Test")
                client._running = False
        def connection(*args, **kwargs):
            limits.append(kwargs["max_size"])
            context = AsyncMock()
            context.__aenter__.return_value = Socket()
            return context
        with patch("custom_discord.websockets.connect", side_effect=connection):
            await asyncio.wait_for(client.start(), timeout=3)
        self.assertEqual(limits, [16 * 1024 * 1024])
        await client.close()

    async def test_bot_import_has_no_authentication_or_database_side_effects(self):
        import importlib
        with patch("database.init_db") as initialize:
            import bot
            importlib.reload(bot)
            initialize.assert_not_called()


if __name__ == "__main__":
    unittest.main()
