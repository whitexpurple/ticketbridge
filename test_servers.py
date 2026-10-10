import asyncio
import os
import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from telegram_panel import server_page, servers, button_handler
from server_lists import ServerLists, SERVER_LIST_TTL
from telegram.error import BadRequest, NetworkError
import database


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = patch.object(database, "DB_PATH", Path(self.temp.name) / "bot.db")
        self.db.start()
        database.init_db()
        self.bot = SimpleNamespace(delete_message=AsyncMock(), edit_message_text=AsyncMock())
        self.lists = ServerLists(self.bot)

    async def asyncTearDown(self):
        self.db.stop()
        self.temp.cleanup()

    def client(self, count=32):
        ready = asyncio.Event()
        ready.set()
        return SimpleNamespace(ready=ready, guilds_list=[
            {"id": str(index), "name": f"Server {index:03d}"} for index in reversed(range(count))])

    def context(self, client):
        return SimpleNamespace(application=SimpleNamespace(bot_data={"discord_session": SimpleNamespace(client=client),
                                                                   "server_lists": self.lists}))

    def update(self):
        return SimpleNamespace(effective_chat=SimpleNamespace(id=42, type="private"),
            effective_user=SimpleNamespace(id=42), callback_query=None,
            effective_message=SimpleNamespace(chat_id=42, message_id=100,
                reply_text=AsyncMock(return_value=SimpleNamespace(chat_id=42, message_id=100))))

    async def test_all_servers_accessible_in_sorted_pages(self):
        client = self.client()
        texts = []
        for page in range(3):
            text, markup = server_page(client, page)
            self.assertIn("My Discord servers: 32", text)
            texts.append(text)
        self.assertIn("1. Server 000", texts[0])
        self.assertIn("32. Server 031", texts[2])
        self.assertEqual(sum(text.count("ID:") for text in texts), 32)
        self.assertEqual(server_page(client, 99)[0], texts[2])

    async def test_long_unicode_names_fit_telegram(self):
        client = self.client(15)
        for guild in client.guilds_list:
            guild.update(name="\U0001f600" * 100, id="1" * 20, unavailable=True)
        text, _ = server_page(client)
        self.assertLess(len(text.encode("utf-16-le")) // 2, 4096)

    async def test_command_and_pagination_authorization(self):
        update = self.update()
        context = self.context(self.client())
        with patch.dict(os.environ, {"TELEGRAM_CHAT_ID": "42"}):
            await servers(update, context)
            self.assertIn("My Discord servers: 32", update.effective_message.reply_text.await_args.args[0])
            update.callback_query = SimpleNamespace(data="servers:1", answer=AsyncMock(), edit_message_text=AsyncMock())
            await button_handler(update, context)
            self.assertIn("Page 2 of 3", update.callback_query.edit_message_text.await_args.args[0])
            update.effective_user.id = 43
            update.callback_query.edit_message_text.reset_mock()
            await button_handler(update, context)
            update.callback_query.edit_message_text.assert_not_awaited()

    async def test_disconnected_empty_and_reconnecting(self):
        update = self.update()
        with patch.dict(os.environ, {"TELEGRAM_CHAT_ID": "42"}):
            await servers(update, self.context(None))
        self.assertIn("disconnected", update.effective_message.reply_text.await_args.args[0])
        client = self.client(0)
        self.assertIn("servers: 0", server_page(client)[0])
        client.ready.clear()
        self.assertIn("cached list", server_page(client)[0])

    async def test_close_button_exists_on_all_pages(self):
        for count in (0, 1, 32):
            for page in (0, 1, 2):
                markup = server_page(self.client(count), page)[1]
                self.assertEqual(markup.inline_keyboard[-1][0].callback_data, "servers:close")

    async def test_manual_close_cancels_expiry_and_leaves_other_lists(self):
        self.lists.query("INSERT INTO telegram_server_lists VALUES (42,100,0)")
        self.lists.query("INSERT INTO telegram_server_lists VALUES (42,101,0)")
        update = self.update()
        update.callback_query = SimpleNamespace(data="servers:close", answer=AsyncMock())
        with patch.dict(os.environ, {"TELEGRAM_CHAT_ID": "42"}):
            await button_handler(update, self.context(self.client()))
        self.bot.delete_message.assert_awaited_once_with(chat_id=42, message_id=100)
        self.assertEqual(self.lists.query("SELECT message_id FROM telegram_server_lists"), [(101,)])

    async def test_deadline_survives_pagination_and_restart(self):
        update = self.update()
        context = self.context(self.client())
        with patch.dict(os.environ, {"TELEGRAM_CHAT_ID": "42"}), patch("server_lists.time.time", return_value=1000):
            await servers(update, context)
        self.assertEqual(self.lists.query("SELECT expires_at FROM telegram_server_lists"), [(1000 + SERVER_LIST_TTL,)])
        update.callback_query = SimpleNamespace(data="servers:1", answer=AsyncMock(), edit_message_text=AsyncMock())
        with patch.dict(os.environ, {"TELEGRAM_CHAT_ID": "42"}), patch("server_lists.time.time", return_value=1200):
            await button_handler(update, context)
        self.assertEqual(self.lists.query("SELECT expires_at FROM telegram_server_lists"), [(1900,)])
        restarted = ServerLists(self.bot)
        with patch("server_lists.time.time", return_value=1899):
            await restarted.expire_once()
        self.bot.delete_message.assert_not_awaited()
        with patch("server_lists.time.time", return_value=1900):
            await restarted.expire_once()
        self.bot.delete_message.assert_awaited_once_with(chat_id=42, message_id=100)
        self.assertEqual(self.lists.query("SELECT * FROM telegram_server_lists"), [])

    async def test_expired_page_click_cannot_reopen_list(self):
        self.lists.query("INSERT INTO telegram_server_lists VALUES (42,100,0)")
        update = self.update()
        update.callback_query = SimpleNamespace(data="servers:1", answer=AsyncMock(), edit_message_text=AsyncMock())
        with patch.dict(os.environ, {"TELEGRAM_CHAT_ID": "42"}):
            await button_handler(update, self.context(self.client()))
        update.callback_query.edit_message_text.assert_not_awaited()
        self.bot.delete_message.assert_awaited_once()

    async def test_failed_delete_falls_back_to_replacing_list(self):
        self.bot.delete_message.side_effect = BadRequest("Message can't be deleted")
        self.assertTrue(await self.lists.close(42, 100))
        self.bot.edit_message_text.assert_awaited_once_with(chat_id=42, message_id=100,
                                                           text="Server list closed.", reply_markup=None)

    async def test_network_failure_keeps_expiry_for_retry(self):
        self.bot.delete_message.side_effect = NetworkError("test failure")
        self.assertFalse(await self.lists.close(42, 100))
        self.assertEqual(len(self.lists.query("SELECT * FROM telegram_server_lists")), 1)
        self.bot.delete_message.side_effect = None
        await self.lists.expire_once()
        self.assertEqual(self.lists.query("SELECT * FROM telegram_server_lists"), [])

    async def test_unauthorized_close_cannot_delete(self):
        update = self.update()
        update.effective_user.id = 43
        update.callback_query = SimpleNamespace(data="servers:close", answer=AsyncMock())
        with patch.dict(os.environ, {"TELEGRAM_CHAT_ID": "42"}):
            await button_handler(update, self.context(self.client()))
        self.bot.delete_message.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
