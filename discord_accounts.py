"""Manage independent Discord gateway sessions and their local credentials."""

import json
import logging
import os
import tempfile
from pathlib import Path

from dotenv import dotenv_values, unset_key

from discord_session import DiscordConnectionError, DiscordSession, ENV_PATH

logger = logging.getLogger("discord-accounts")
STORE_PATH = Path(__file__).with_name("data") / "discord_accounts.json"


class DiscordAccounts:
    def __init__(self, app, send, limiter, bot_token=None, interval=60):
        self.app, self.send, self.limiter = app, send, limiter
        self.bot_token, self.interval = bot_token, interval
        self.sessions = {}
        self.records = {}

    @property
    def client(self):
        session = next(iter(self.sessions.values()), None)
        return session.client if session else None

    @property
    def discovery(self):
        session = next(iter(self.sessions.values()), None)
        return session.discovery if session else None

    @property
    def events(self):
        session = next(iter(self.sessions.values()), None)
        return session.events if session else None

    @property
    def connection_error(self):
        errors = [f"{self.records.get(key, {}).get('username', key)}: {session.connection_error}"
                  for key, session in self.sessions.items() if session.connection_error]
        return "; ".join(errors) or None

    def _save(self):
        STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(STORE_PATH.parent, 0o700)
        except OSError:
            pass
        fd, temporary = tempfile.mkstemp(prefix="discord_accounts.", suffix=".tmp", dir=STORE_PATH.parent)
        try:
            try:
                os.chmod(temporary, 0o600)
            except OSError:
                pass
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.records, handle, indent=2)
                handle.write("\n")
            os.replace(temporary, STORE_PATH)
            try:
                os.chmod(STORE_PATH, 0o600)
            except OSError:
                pass
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def _load(self):
        try:
            value = json.loads(STORE_PATH.read_text(encoding="utf-8"))
            if not isinstance(value, dict):
                raise ValueError("account store must be an object")
            self.records = {str(key): item for key, item in value.items()
                            if isinstance(item, dict) and isinstance(item.get("token"), str)}
        except FileNotFoundError:
            self.records = {}
        except (OSError, ValueError, json.JSONDecodeError):
            logger.exception("Discord account store could not be read; refusing to overwrite it.")
            raise DiscordConnectionError("The Discord account store could not be read. Check data/discord_accounts.json.") from None

        legacy = dotenv_values(ENV_PATH).get("DISCORD_TOKEN")
        if legacy:
            if not self.records:
                self.records["legacy"] = {"token": legacy, "username": "Discord account"}
                self._save()
                logger.info("Migrated the legacy Discord token from .env to the protected account store.")
            unset_key(str(ENV_PATH), "DISCORD_TOKEN")

    async def start_saved(self):
        self._load()
        invalid = []
        for key, record in list(self.records.items()):
            session = DiscordSession(self.app, self.send, self.limiter, self.bot_token, self.interval)
            try:
                username = await session.connect(record["token"])
            except Exception as error:
                invalid.append(key)
                logger.warning("Saved Discord account could not connect: account=%s error=%s", key, error)
                continue
            actual_id = str(session.client.user["id"])
            if actual_id != key:
                del self.records[key]
                self.records[actual_id] = record
                key = actual_id
            record["username"] = username
            self.sessions[key] = session
        if invalid:
            # Keep credentials in place so /reconnect can retry after a transient outage.
            pass
        if self.records:
            self._save()

    async def add(self, token):
        session = DiscordSession(self.app, self.send, self.limiter, self.bot_token, self.interval)
        username = await session.connect(token, source="Telegram account setup")
        account_id = str(session.client.user["id"])
        previous = self.sessions.get(account_id)
        previous_record = self.records.get(account_id)
        self.sessions[account_id] = session
        self.records[account_id] = {"token": token, "username": username}
        try:
            self._save()
        except OSError:
            self.sessions.pop(account_id, None)
            if previous_record is None:
                self.records.pop(account_id, None)
            else:
                self.records[account_id] = previous_record
            await session.close()
            if previous:
                self.sessions[account_id] = previous
            raise DiscordConnectionError("Discord connected, but the account token could not be saved locally. Check data/ permissions.") from None
        if previous:
            await previous.close()
        return account_id, username

    async def reconnect(self, account_id):
        record = self.records.get(str(account_id))
        if not record:
            raise DiscordConnectionError("That Discord account is not in the account store.")
        return await self.add(record["token"])

    async def remove(self, account_id):
        account_id = str(account_id)
        record = self.records.pop(account_id, None)
        if record is None:
            return False
        try:
            self._save()
        except OSError:
            self.records[account_id] = record
            raise DiscordConnectionError("The account token could not be removed from local storage. Check data/ permissions.") from None
        session = self.sessions.pop(account_id, None)
        if session:
            await session.close()
        existed = True
        return existed

    async def close(self):
        sessions, self.sessions = list(self.sessions.values()), {}
        for session in sessions:
            await session.close()

    def summaries(self):
        result = []
        for account_id, record in self.records.items():
            session = self.sessions.get(account_id)
            guilds = session.client.guilds_list if session and session.client else []
            result.append({"id": account_id, "username": record.get("username", "Discord account"),
                           "session": session, "guilds": guilds,
                           "connected": bool(session and session.client and session.client.ready.is_set())})
        return result
