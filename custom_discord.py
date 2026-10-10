import asyncio
import http.client
import json
import logging
import threading
import time
import zlib

import websockets


logger = logging.getLogger("custom-discord")

GATEWAY_CLOSE_LABELS = {
    1000: "normal closure", 1001: "endpoint going away", 1006: "connection lost",
    1009: "message exceeds size limit", 4000: "unknown gateway error",
    4001: "unknown opcode", 4002: "invalid gateway payload",
    4003: "not authenticated", 4004: "gateway authentication rejected",
    4005: "already authenticated", 4007: "invalid sequence",
    4008: "gateway rate limited", 4009: "session timed out",
    4010: "invalid shard", 4011: "sharding required", 4012: "invalid API version",
    4013: "invalid intents", 4014: "disallowed intents",
}


class GatewayConnectionError(Exception):
    """A gateway failure containing only safe numeric codes and local labels."""


class CustomDiscordClient:
    """Minimal Discord gateway client."""

    # READY includes the account's guild state and can exceed websockets' 1 MiB default.
    GATEWAY_MAX_MESSAGE_SIZE = 16 * 1024 * 1024
    REST_MIN_INTERVAL = 0.5

    GATEWAY_URL = (
        "wss://gateway.discord.gg/"
        "?v=9&encoding=json"
    )

    def __init__(self, token, bot_token=None):
        self.token = token
        self.bot_token = self._clean_token(bot_token)
        self.ws = None
        self.heartbeat_task = None
        self.sequence = None
        self.session_id = None

        self.user = None

        self.guilds = {}
        self.channels = {}
        self.users = {}
        self._forbidden_rest_paths = set()
        self._retry_after = {}
        self._rest_errors = {}
        self._global_retry_after = 0
        self.ready = asyncio.Event()
        self.last_gateway_error = None
        self._heartbeat_ack = True
        self._user_retry_after = {}
        self._rest_lock = threading.Lock()
        self._last_rest_request = 0.0

        self._handlers = {}
        self._running = False

        self.on_ready = None
        self.on_message = None
        self.on_guild_channel_create = None
        self.on_guild_channel_delete = None
        self.on_guild_channel_update = None

    def _clean_token(self, token):
        """Normalize an optional token read from .env."""

        if not token:
            return None

        return token.strip().strip("'\"")

    def _api_get(self, path, use_bot_token=False):
        """Fetch JSON from Discord's REST API."""

        auth_kind = "bot" if use_bot_token else "self"

        token = (
            self.bot_token
            if use_bot_token
            else self._clean_token(self.token)
        )

        if not token:
            self._rest_errors[(auth_kind, path)] = "Discord session token unavailable"
            return None

        authorization = token

        if use_bot_token:
            authorization = f"Bot {token}"

        forbidden_key = (
            auth_kind,
            path,
        )

        if forbidden_key in self._forbidden_rest_paths:
            self._rest_errors[forbidden_key] = "HTTP 403 access denied"
            return None
        if not use_bot_token:
            with self._rest_lock:
                wait_until = max(self._retry_after.get(forbidden_key, 0), self._global_retry_after)
                delay = wait_until - time.monotonic()
                if delay > 0:
                    return None
                delay = self.REST_MIN_INTERVAL - (time.monotonic() - self._last_rest_request)
                if delay > 0:
                    time.sleep(delay)
                self._last_rest_request = time.monotonic()
                if time.monotonic() < max(self._retry_after.get(forbidden_key, 0), self._global_retry_after):
                    return None
        elif time.monotonic() < max(self._retry_after.get(forbidden_key, 0), self._global_retry_after):
            return None

        conn = None

        try:
            conn = http.client.HTTPSConnection(
                "discord.com",
                timeout=15,
            )

            conn.request(
                "GET",
                path,
                headers={
                    "Authorization": authorization,
                    "User-Agent": "TicketBridge/1.0",
                },
            )

            response = conn.getresponse()
            body = response.read()

            if response.status != 200:
                delay = 60
                global_limit = False
                if response.status in (403, 404):
                    delay = 600
                elif response.status == 429:
                    try:
                        limited = json.loads(body)
                        header_delay = response.getheader("Retry-After")
                        body_delay = limited.get("retry_after", 60)
                        delay = max(1, float(header_delay or body_delay))
                        global_limit = bool(limited.get("global"))
                        if global_limit:
                            self._global_retry_after = time.monotonic() + delay
                    except (ValueError, TypeError):
                        pass
                self._retry_after[forbidden_key] = time.monotonic() + delay
                self._rest_errors[forbidden_key] = f"HTTP {response.status}"
                if response.status not in (403, 404):
                    logger.warning(
                        "Discord REST request failed: path=%s status=%s retry_after=%ss global=%s",
                        path,
                        response.status,
                        int(delay),
                        global_limit,
                    )

                return None

            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, ValueError):
                self._rest_errors[forbidden_key] = "unexpected non-JSON response"
                logger.warning("Discord REST returned an invalid JSON response: path=%s", path)
                return None
            self._rest_errors.pop(forbidden_key, None)
            return payload

        except Exception:
            self._rest_errors[forbidden_key] = "network error"
            logger.exception(
                "Discord REST request errored: path=%s",
                path,
            )

            return None

        finally:
            if conn:
                conn.close()

    def rest_failure_reason(self, path, use_bot_token=False):
        auth_kind = "bot" if use_bot_token else "self"
        key = (auth_kind, path)
        reason = self._rest_errors.get(key)
        if reason:
            remaining = self._retry_after.get(key, 0) - time.monotonic()
            if remaining > 0:
                return f"{reason}; retry cooldown {int(remaining)}s"
            return reason
        global_remaining = self._global_retry_after - time.monotonic()
        if global_remaining > 0:
            return f"global rate-limit cooldown {int(global_remaining)}s"
        if time.monotonic() < self._retry_after.get(key, 0):
            return "request cooldown"
        return None

    def event(self, func):
        """Register an event handler."""

        name = func.__name__

        if name.startswith("on_"):
            self._handlers[name] = func

        return func

    async def start(self):
        """Connect to Discord."""

        self._running = True

        logger.info("Starting Discord gateway connection.")

        await self._connect()

    async def _connect(self):
        """Maintain the Discord connection."""

        while self._running:
            try:
                async with websockets.connect(
                    self.GATEWAY_URL,
                    max_size=self.GATEWAY_MAX_MESSAGE_SIZE,
                    additional_headers={
                        "Authorization": self.token,
                        "User-Agent": (
                            "Mozilla/5.0 "
                            "(Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 "
                            "(KHTML, like Gecko) "
                            "Chrome/135.0.0.0 "
                            "Safari/537.36"
                        ),
                    },
                ) as ws:

                    self.ws = ws
                    self._heartbeat_ack = True

                    logger.info(
                        "Connected to Discord gateway."
                    )

                    await self._identify()

                    async for raw_message in ws:
                        await self._handle_message(
                            raw_message
                        )

            except websockets.exceptions.ConnectionClosed as error:
                received = error.rcvd.code if error.rcvd else None
                sent = error.sent.code if error.sent else None
                code = received if received is not None else sent
                label = GATEWAY_CLOSE_LABELS.get(code, "unclassified closure")
                self.last_gateway_error = (
                    f"Gateway closed: received={received} sent={sent}; {label}."
                )
                if sent == 1009:
                    self.last_gateway_error += (
                        f" The local incoming-message limit is {self.GATEWAY_MAX_MESSAGE_SIZE} bytes."
                    )
                logger.warning("%s", self.last_gateway_error)
                self.ready.clear()
                if code in (4004, 4010, 4011, 4012, 4013, 4014) or sent == 1009:
                    self._running = False
                    raise GatewayConnectionError(self.last_gateway_error) from None
                await asyncio.sleep(5)

            except Exception as error:
                logger.exception(
                    "Connection error: %s",
                    error,
                )

                await asyncio.sleep(5)

            finally:
                self.ready.clear()
                if self.heartbeat_task:
                    self.heartbeat_task.cancel()
                    self.heartbeat_task = None

    async def _identify(self):
        """Send the gateway identify payload."""

        payload = {
            "op": 2,
            "d": {
                "token": self.token,
                "intents": 3276799,
                "properties": {
                    "$os": "Windows",
                    "$browser": "Chrome",
                    "$device": "Chrome",
                },
            },
        }

        await self.ws.send(
            json.dumps(payload)
        )

    async def _handle_message(self, raw_message):
        """Process a gateway message."""

        if isinstance(raw_message, bytes):
            try:
                raw_message = zlib.decompress(
                    raw_message
                ).decode("utf-8")

            except zlib.error:
                logger.warning(
                    "Could not decompress gateway data."
                )

                return

        try:
            data = json.loads(raw_message)

        except json.JSONDecodeError:
            logger.warning(
                "Received invalid gateway JSON."
            )

            return

        op = data.get("op")
        event_type = data.get("t")

        if data.get("s") is not None:
            self.sequence = data.get("s")

        event_data = data.get("d") or {}

        important_events = {
            "READY",
            "GUILD_CREATE",
            "CHANNEL_CREATE",
            "CHANNEL_DELETE",
            "CHANNEL_UPDATE",
            "THREAD_CREATE",
            "THREAD_DELETE",
            "THREAD_UPDATE",
            "MESSAGE_CREATE",
        }

        if event_type in important_events:
            logger.info(
                "Discord event: %s",
                event_type,
            )
        elif event_type:
            logger.debug(
                "Discord event: %s",
                event_type,
            )

        if op == 0:

            if event_type == "READY":
                self.guilds.clear()
                self.channels.clear()
                self.user = event_data.get(
                    "user",
                    {},
                )

                for user in event_data.get(
                    "users",
                    [],
                ):
                    user_id = user.get("id")

                    if user_id:
                        self.users[
                            str(user_id)
                        ] = user

                if self.user.get("id"):
                    self.users[
                        str(self.user["id"])
                    ] = self.user

                self.session_id = event_data.get(
                    "session_id"
                )

                logger.info(
                    "Connected as %s",
                    (
                        self.user.get("global_name")
                        or self.user.get("username")
                        or "Unknown"
                    ),
                )

                for guild in event_data.get(
                    "guilds",
                    [],
                ):
                    guild_id = guild.get("id")

                    if guild_id:
                        self.guilds[
                            str(guild_id)
                        ] = guild

                    for channel in guild.get(
                        "channels",
                        [],
                    ):
                        channel_id = channel.get(
                            "id"
                        )

                        if channel_id:
                            channel["guild_id"] = str(guild_id)
                            self.channels[
                                str(channel_id)
                            ] = channel

                    for member in guild.get(
                        "members",
                        [],
                    ):
                        user = member.get("user")

                        if not user:
                            continue

                        user_id = user.get("id")

                        if user_id:
                            self.users[
                                str(user_id)
                            ] = user

                for guild in self.guilds_list:
                    for thread in guild.get("threads", []):
                        thread["guild_id"] = str(guild["id"])
                        self.channels[str(thread["id"])] = thread
                self.ready.set()

                if self.heartbeat_task is None:
                    self.heartbeat_task = (
                        asyncio.create_task(
                            self._heartbeat()
                        )
                    )

                if self.on_ready:
                    try:
                        await self.on_ready()

                    except Exception:
                        logger.exception(
                            "Error in on_ready."
                        )

            elif event_type == "GUILD_CREATE":
                guild_id = event_data.get("id")

                if guild_id:
                    self.guilds[
                        str(guild_id)
                    ] = event_data

                for channel in event_data.get(
                    "channels",
                    [],
                ):
                    channel_id = channel.get("id")

                    if channel_id:
                        channel["guild_id"] = str(guild_id)
                        self.channels[
                            str(channel_id)
                        ] = channel

                for member in event_data.get(
                    "members",
                    [],
                ):
                    user = member.get("user")

                    if not user:
                        continue

                    user_id = user.get("id")

                    if user_id:
                        self.users[
                            str(user_id)
                        ] = user

                for user in event_data.get(
                    "users",
                    [],
                ):
                    user_id = user.get("id")

                    if user_id:
                        self.users[
                            str(user_id)
                        ] = user

                for thread in event_data.get("threads", []):
                    thread["guild_id"] = str(guild_id)
                    self.channels[str(thread["id"])] = thread

            elif event_type == "GUILD_DELETE":
                if not event_data.get("unavailable"):
                    guild_id = str(event_data.get("id"))
                    self.guilds.pop(guild_id, None)
                    self.channels = {key: value for key, value in self.channels.items()
                                     if str(value.get("guild_id")) != guild_id}

            elif event_type == "THREAD_LIST_SYNC":
                for thread in event_data.get("threads", []):
                    thread["guild_id"] = str(event_data["guild_id"])
                    self.channels[str(thread["id"])] = thread
                    if self.on_guild_channel_create:
                        await self.on_guild_channel_create(thread)

            elif event_type in (
                "CHANNEL_CREATE",
                "THREAD_CREATE",
            ):
                channel = event_data
                channel_id = channel.get("id")

                if channel_id:
                    self.channels[
                        str(channel_id)
                    ] = channel
                    self._cache_guild_channel(
                        channel
                    )

                logger.info(
                    "Channel created: %s",
                    channel.get("name"),
                )

                if self.on_guild_channel_create:
                    try:
                        await self.on_guild_channel_create(
                            channel
                        )

                    except Exception:
                        logger.exception(
                            "Error in channel create handler."
                        )

            elif event_type in (
                "CHANNEL_DELETE",
                "THREAD_DELETE",
            ):
                channel = event_data
                channel_id = channel.get("id")

                if channel_id:
                    self.channels.pop(
                        str(channel_id),
                        None,
                    )
                    self._remove_guild_channel(
                        channel
                    )

                logger.info(
                    "Channel deleted: %s",
                    channel.get("name"),
                )

                if self.on_guild_channel_delete:
                    try:
                        await self.on_guild_channel_delete(
                            channel
                        )

                    except Exception:
                        logger.exception(
                            "Error in channel delete handler."
                        )

            elif event_type in (
                "CHANNEL_UPDATE",
                "THREAD_UPDATE",
            ):
                channel = {**self.channels.get(str(event_data.get("id")), {}), **event_data}
                channel_id = channel.get("id")

                if channel_id:
                    self.channels[
                        str(channel_id)
                    ] = channel
                    self._cache_guild_channel(
                        channel
                    )

                if self.on_guild_channel_update:
                    try:
                        await self.on_guild_channel_update(
                            channel
                        )

                    except Exception:
                        logger.exception(
                            "Error in channel update handler."
                        )

            elif event_type == "MESSAGE_CREATE":
                message = event_data

                author = message.get(
                    "author",
                    {},
                )

                author_id = author.get("id")

                if author_id:
                    self.users[
                        str(author_id)
                    ] = author

                if self.on_message:
                    try:
                        await self.on_message(
                            message
                        )

                    except Exception:
                        logger.exception(
                            "Error in message handler."
                        )

        elif op == 10:
            heartbeat_interval = event_data.get(
                "heartbeat_interval",
                41250,
            )

            if self.heartbeat_task:
                self.heartbeat_task.cancel()

            self.heartbeat_task = (
                asyncio.create_task(
                    self._heartbeat(
                        heartbeat_interval
                    )
                )
            )

        elif op == 11:
            self._heartbeat_ack = True
            logger.debug(
                "Heartbeat acknowledged."
            )

        elif op in (7, 9):
            logger.warning("Discord gateway requested %s.", "reconnect" if op == 7 else "a new session (invalid session)")
            await self.ws.close()

        elif op == 1:
            await self.ws.send(json.dumps({"op": 1, "d": self.sequence}))

    async def _heartbeat(self, interval=41250):
        """Send Discord heartbeats."""

        try:
            while self._running:
                await asyncio.sleep(
                    interval / 1000
                )

                if self.ws:
                    if not self._heartbeat_ack:
                        logger.warning("Gateway heartbeat acknowledgement missing; reconnecting.")
                        await self.ws.close()
                        return
                    payload = {
                        "op": 1,
                        "d": self.sequence,
                    }

                    self._heartbeat_ack = False
                    await self.ws.send(
                        json.dumps(payload)
                    )

        except asyncio.CancelledError:
            pass

        except Exception as error:
            logger.exception(
                "Heartbeat error: %s",
                error,
            )

    async def close(self):
        """Close the Discord connection."""

        self._running = False

        if self.heartbeat_task:
            self.heartbeat_task.cancel()

            try:
                await self.heartbeat_task

            except asyncio.CancelledError:
                pass

        self.heartbeat_task = None

        if self.ws:
            await self.ws.close()

        self.ws = None

    @property
    def guilds_list(self):
        """Return cached guilds."""

        return list(
            self.guilds.values()
        )

    def get_guild(self, guild_id):
        """Return a guild by ID."""

        return self.guilds.get(
            str(guild_id)
        )

    def get_channel(self, channel_id):
        """Return a channel by ID."""

        return self.channels.get(
            str(channel_id)
        )

    def _cache_guild_channel(self, channel):
        """Store or update a channel inside its guild cache."""

        guild_id = channel.get("guild_id")
        channel_id = channel.get("id")

        if not guild_id or not channel_id:
            return

        guild = self.guilds.get(
            str(guild_id)
        )

        if not guild:
            return

        channels = guild.setdefault(
            "channels",
            [],
        )

        channel_id = str(channel_id)

        for index, cached_channel in enumerate(channels):
            if str(cached_channel.get("id")) == channel_id:
                channels[index] = channel
                return

        channels.append(channel)

    def _remove_guild_channel(self, channel):
        """Remove a channel from its guild cache."""

        guild_id = channel.get("guild_id")
        channel_id = channel.get("id")

        if not guild_id or not channel_id:
            return

        guild = self.guilds.get(
            str(guild_id)
        )

        if not guild:
            return

        guild["channels"] = [
            cached_channel
            for cached_channel in guild.get(
                "channels",
                [],
            )
            if str(cached_channel.get("id")) != str(channel_id)
        ]

    def get_user(self, user_id):
        """Return a cached user."""

        if not user_id:
            return None

        user_id = str(user_id)

        user = self.users.get(user_id)

        if user:
            return user

        for guild in self.guilds.values():
            for member in guild.get(
                "members",
                [],
            ):
                member_user = member.get(
                    "user",
                    {},
                )

                if str(
                    member_user.get("id")
                ) == user_id:

                    if member_user.get("id"):
                        self.users[
                            user_id
                        ] = member_user

                    return member_user

        return self.fetch_user_with_bot_token(user_id)

    def fetch_user_with_bot_token(self, user_id):
        """Resolve users through the rate-limited bot-token REST helper."""
        user_id = str(user_id)
        if not self.bot_token or time.monotonic() < self._user_retry_after.get(user_id, 0):
            return None
        user = self._api_get(f"/api/v10/users/{user_id}", use_bot_token=True)
        if not isinstance(user, dict) or str(user.get("id")) != user_id:
            self._user_retry_after[user_id] = time.monotonic() + 60
            return None
        self.users[user_id] = user
        self._user_retry_after.pop(user_id, None)
        return user

    def fetch_guild_channels(self, guild_id):
        """Refresh and return guild channels from Discord REST."""

        channels = self._api_get(
            f"/api/v10/guilds/{guild_id}/channels"
        )

        if channels is None and self.bot_token:
            channels = self._api_get(
                f"/api/v10/guilds/{guild_id}/channels",
                use_bot_token=True,
            )

        if not isinstance(channels, list):
            return []

        guild = self.guilds.setdefault(
            str(guild_id),
            {
                "id": str(guild_id),
                "channels": [],
            },
        )

        guild["channels"] = channels

        for channel in channels:
            channel_id = channel.get("id")

            if channel_id:
                self.channels[
                    str(channel_id)
                ] = channel

        logger.debug(
            "Fetched %d guild channels through REST: guild=%s",
            len(channels),
            guild_id,
        )

        return channels

    def fetch_active_threads(self, guild_id):
        """Fetch active guild threads from Discord REST."""

        threads_payload = self._api_get(
            f"/api/v10/guilds/{guild_id}/threads/active"
        )

        if threads_payload is None and self.bot_token:
            threads_payload = self._api_get(
                f"/api/v10/guilds/{guild_id}/threads/active",
                use_bot_token=True,
            )

        if not isinstance(threads_payload, dict):
            return []

        threads = threads_payload.get(
            "threads",
            [],
        )

        if not isinstance(threads, list):
            return []

        for thread in threads:
            thread_id = thread.get("id")

            if thread_id:
                self.channels[
                    str(thread_id)
                ] = thread

                self._cache_guild_channel(
                    thread
                )

        logger.debug(
            "Fetched %d active threads through REST: guild=%s",
            len(threads),
            guild_id,
        )

        return threads

    def get_member(self, guild_id, user_id):
        """Return a cached guild member by guild ID and user ID."""

        if not guild_id or not user_id:
            return None

        guild_id = str(guild_id)
        user_id = str(user_id)

        logger.info(
            "GET_MEMBER: guild=%s user=%s",
            guild_id,
            user_id,
        )

        guild = self.guilds.get(guild_id)

        if not guild:
            logger.info(
                "Guild %s is not available in local cache.",
                guild_id,
            )
            return None

        members = guild.get("members", [])

        logger.info(
            "GET_MEMBER: guild has %d cached members.",
            len(members),
        )

        for member in members:
            user = member.get("user", {})

            logger.info(
                "GET_MEMBER: cached member id=%s username=%s",
                user.get("id"),
                user.get("username"),
            )

            if str(user.get("id")) == user_id:
                logger.info(
                    "GET_MEMBER: FOUND user=%s username=%s",
                    user_id,
                    user.get("username"),
                )

                return member

        logger.info(
            "User %s is not available as a member of guild %s.",
            user_id,
            guild_id,
        )

        return None

    def get_guild_categories(self, guild_id):
        """Return categories for a guild."""

        guild = self.guilds.get(
            str(guild_id)
        )

        if not guild:
            return []

        return [
            channel
            for channel in guild.get(
                "channels",
                [],
            )
            if channel.get("type") == 4
        ]

    def get_channel_children(self, guild_id, parent_id):
        """Return channels directly under a parent channel/category."""

        guild = self.guilds.get(
            str(guild_id)
        )

        if not guild:
            return []

        parent_id = str(parent_id)

        children = [
            channel
            for channel in guild.get(
                "channels",
                [],
            )
            if str(channel.get("parent_id")) == parent_id
        ]

        return sorted(
            children,
            key=lambda channel: (
                channel.get("position", 0),
                channel.get("name", ""),
            ),
        )

    def is_channel_under_parent(self, channel, parent_id):
        """Return True if a channel is inside the configured parent."""

        if not channel or not parent_id:
            return False

        parent_id = str(parent_id)

        current_parent_id = channel.get(
            "parent_id"
        )

        visited = set()

        while current_parent_id:
            current_parent_id = str(current_parent_id)

            if current_parent_id == parent_id:
                return True

            if current_parent_id in visited:
                return False

            visited.add(
                current_parent_id
            )

            parent = self.get_channel(
                current_parent_id
            )

            if not parent:
                return False

            current_parent_id = parent.get(
                "parent_id"
            )

        return False
