import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import database
from database import get_notification_preference
from server_lists import ServerLists
from telegram_panel import button_handler, notifications


class NotificationSettingsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(database, "DB_PATH", Path(self.temp.name) / "test.db")
        self.db_patch.start()
        database.init_db()
        self.bot = SimpleNamespace(delete_message=AsyncMock(), edit_message_text=AsyncMock())
        self.lists = ServerLists(self.bot)
        self.reply = AsyncMock(return_value=SimpleNamespace(chat_id=42, message_id=100))
        self.edit = AsyncMock()
        self.update = SimpleNamespace(
            effective_chat=SimpleNamespace(id=42, type="private"),
            effective_user=SimpleNamespace(id=42),
            effective_message=SimpleNamespace(chat_id=42, message_id=100,
                                               reply_text=self.reply),
            callback_query=None,
        )
        self.context = SimpleNamespace(application=SimpleNamespace(bot_data={"server_lists": self.lists}))

    async def asyncTearDown(self):
        self.db_patch.stop()
        self.temp.cleanup()

    async def test_menu_lists_each_type_and_toggle_persists(self):
        with patch.dict(os.environ, {"TELEGRAM_CHAT_ID": "42"}):
            await notifications(self.update, self.context)
            text = self.reply.await_args.args[0]
            markup = self.reply.await_args.kwargs["reply_markup"]
            self.assertIn("Opened Ticket: ON", text)
            self.assertIn("Closed Ticket: ON", text)
            self.assertIn("Message: ON", text)
            self.assertEqual(len(markup.inline_keyboard), 4)

            self.update.callback_query = SimpleNamespace(
                data="notification:closed", answer=AsyncMock(), edit_message_text=self.edit
            )
            await button_handler(self.update, self.context)
            self.assertIn("Closed Ticket notifications are ON", self.edit.await_args.args[0])

            self.update.callback_query = SimpleNamespace(
                data="notification-set:closed:off", answer=AsyncMock(), edit_message_text=self.edit
            )
            await button_handler(self.update, self.context)

        self.assertFalse(get_notification_preference("closed"))
        self.assertIn("Closed Ticket notifications are OFF", self.edit.await_args.args[0])
        database.init_db()
        self.assertFalse(get_notification_preference("closed"))


if __name__ == "__main__":
    unittest.main()
