"""Close server-list messages manually or after a persistent 15-minute deadline."""

import asyncio
import logging
import time

from telegram.error import BadRequest, TelegramError
from database import get_connection

logger = logging.getLogger(__name__)
SERVER_LIST_TTL = 15 * 60


class ServerLists:
    def __init__(self, bot):
        self.bot = bot
        self.lock = asyncio.Lock()

    def query(self, sql, params=()):
        conn = get_connection()
        try:
            with conn:
                return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    async def show(self, update, text, markup):
        async with self.lock:
            message = update.effective_message
            if update.callback_query:
                rows = self.query("SELECT expires_at FROM telegram_server_lists WHERE chat_id=? AND message_id=?",
                                  (message.chat_id, message.message_id))
                if rows and rows[0][0] <= time.time():
                    await self._close(message.chat_id, message.message_id)
                    return
                if getattr(message, "text", None) != text or getattr(message, "reply_markup", None) != markup:
                    await update.callback_query.edit_message_text(text, reply_markup=markup)
            else:
                message = await message.reply_text(text, reply_markup=markup)
            # Pagination must preserve the original deadline.
            self.query("INSERT OR IGNORE INTO telegram_server_lists VALUES (?,?,?)",
                       (message.chat_id, message.message_id, time.time() + SERVER_LIST_TTL))

    async def _close(self, chat_id, message_id):
        try:
            try:
                await self.bot.delete_message(chat_id=chat_id, message_id=message_id)
            except BadRequest as error:
                if "message to delete not found" not in str(error).lower():
                    try:
                        await self.bot.edit_message_text(chat_id=chat_id, message_id=message_id,
                                                         text="Server list closed.", reply_markup=None)
                    except BadRequest as edit_error:
                        if not any(value in str(edit_error).lower() for value in
                                   ("message to edit not found", "message is not modified")):
                            raise
        except TelegramError as error:
            logger.warning("Server list could not close; will retry: %s", type(error).__name__)
            return False
        self.query("DELETE FROM telegram_server_lists WHERE chat_id=? AND message_id=?", (chat_id, message_id))
        return True

    async def close(self, chat_id, message_id):
        async with self.lock:
            # Save immediate expiry so a transient network failure can be retried.
            self.query("INSERT OR REPLACE INTO telegram_server_lists VALUES (?,?,?)", (chat_id, message_id, time.time()))
            return await self._close(chat_id, message_id)

    async def expire_once(self):
        async with self.lock:
            rows = self.query("SELECT chat_id,message_id FROM telegram_server_lists WHERE expires_at<=? LIMIT 20", (time.time(),))
            for chat_id, message_id in rows:
                if not await self._close(chat_id, message_id):
                    break

    async def run(self):
        while True:
            try:
                await self.expire_once()
            except Exception as error:
                logger.error("Server list expiry worker failed: %s", type(error).__name__)
            await asyncio.sleep(5)
