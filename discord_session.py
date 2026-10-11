"""Discord connection lifecycle and short-lived interactive authentication."""

import asyncio
import contextlib
import base64
import json
import logging
import re
import http.client
from pathlib import Path


from custom_discord import CustomDiscordClient, GatewayConnectionError
from discord_handler import DiscordHandler
from ticket_discovery import TicketDiscovery
from delivery import TicketDelivery
from gateway_events import GatewayEvents

ENV_PATH = Path(__file__).with_name(".env")
logger = logging.getLogger(__name__)
LOGIN_PROPERTIES = {
    "os": "Windows", "browser": "Chrome",
    "browser_user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
}
LOGIN_HEADERS = {
    "User-Agent": LOGIN_PROPERTIES["browser_user_agent"],
    "Content-Type": "application/json",
    "X-Super-Properties": base64.b64encode(json.dumps(LOGIN_PROPERTIES).encode()).decode(),
}


class DiscordConnectionError(ValueError):
    """A connection-stage error safe to display without account secrets."""


def login_field_codes(fields, field, payload):
    """Expose symbolic validation codes only, never messages or supplied values."""
    detail = fields.get(field)
    errors = detail.get("_errors", []) if isinstance(detail, dict) else []
    codes = []
    for error in errors if isinstance(errors, list) else []:
        code = error.get("code") if isinstance(error, dict) else None
        if (isinstance(code, str) and len(code) <= 80
                and re.fullmatch(r"[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+", code)
                and not any(isinstance(value, str) and value and value in code
                            for value in payload.values())):
            codes.append(code)
    return list(dict.fromkeys(codes))[:5]


def _post_auth(path, payload):
    """Use the original HTTPSConnection transport and compact JSON body."""
    body = json.dumps(payload, separators=(",", ":"))
    headers = {**LOGIN_HEADERS, "Content-Length": str(len(body.encode("utf-8")))}
    conn = http.client.HTTPSConnection("discord.com", timeout=20)
    try:
        conn.request("POST", "/api/v9/auth/" + path, body, headers)
        response = conn.getresponse()
        return response.status, response.read()
    finally:
        conn.close()


async def login_request(path, payload):
    stage = "mfa" if path.startswith("mfa/") else "password"
    logger.info("Submitting Discord authentication request: stage=%s.", stage)
    payload = {"login_source": None, "gift_code_sku_id": None, **payload}
    try:
        status_code, body = await asyncio.to_thread(_post_auth, path, payload)
    except (OSError, http.client.HTTPException):
        logger.warning("Discord authentication network failure: stage=%s.", stage)
        return {"error": "Could not reach Discord's login service. Try again.", "retry": True}
    try:
        data = json.loads(body)
    except ValueError:
        logger.warning("Discord authentication returned unreadable JSON: stage=%s status=%d.", stage, status_code)
        return {"error": "Discord returned an unreadable response. Try again later."}
    if not isinstance(data, dict):
        logger.warning("Discord authentication returned unexpected JSON: stage=%s status=%d.", stage, status_code)
        return {"error": "Discord returned an unexpected response. Try again later."}
    code = data.get("code")
    safe_code = str(code) if isinstance(code, int) else "unavailable"
    logger.info("Discord authentication response: stage=%s status=%d code=%s.",
                stage, status_code, safe_code)
    if data.get("captcha_key") or data.get("captcha_sitekey"):
        logger.warning("Discord authentication requires CAPTCHA/browser verification.")
        return {"error": "Discord requires browser verification/CAPTCHA. Complete it in Discord; this setup cannot complete that challenge."}
    if status_code == 429:
        logger.warning("Discord authentication rate limited.")
        return {"error": "Discord rate-limited login. Wait before trying /connect again."}
    if data.get("token"):
        logger.info("Discord authentication accepted; session token received (value omitted).")
        return {"token": data["token"]}
    if data.get("ticket"):
        logger.info("Discord authentication requires two-factor verification.")
        return {"ticket": data["ticket"]}
    if path.startswith("mfa/") and data.get("code") == 60008:
        logger.warning("Discord rejected the two-factor code; retry is available.")
        return {"error": "Invalid two-factor code. Send a fresh authenticator or backup code.", "retry": True}
    fields = data.get("errors") or {}
    if isinstance(fields, dict):
        for field in ("login", "password"):
            if field in fields:
                codes = login_field_codes(fields, field, payload)
                reason = ", ".join(codes) or "UNSPECIFIED_VALIDATION_ERROR"
                logger.warning("Discord login validation: field=%s reason=%s.", field, reason)
                if "ACCOUNT_COMPROMISED_RESET_PASSWORD" in codes:
                    return {"error": "Discord requires a password reset for this account "
                                     "(ACCOUNT_COMPROMISED_RESET_PASSWORD). Open the official Discord app or "
                                     "discord.com/login and use Forgot your password. Complete the reset and "
                                     "any verification there, then use /connect with the new password. "
                                     "Repeating this login will not clear the requirement."}
                if "EMAIL_TYPE_INVALID_EMAIL" in codes:
                    return {"error": "Discord says the email format is invalid (EMAIL_TYPE_INVALID_EMAIL). "
                                     "Use /connect and enter the account email as plain text, without quotes or a display name."}
                if codes and field == "login":
                    return {"error": f"Discord rejected the login field: {reason}. "
                                     "This is Discord's validation response, not a Telegram connection failure. "
                                     "Use /connect to start again."}
        if "password" in fields:
            logger.warning("Discord rejected the password field.")
            return {"error": "Discord rejected the password. Check it and send it again.", "retry": True}
        if "login" in fields:
            logger.warning("Discord rejected the email/login field.")
            return {"error": "Discord rejected the email/login details. Use /connect to start again."}
    logger.warning("Discord authentication rejected: stage=%s status=%d code=%s",
                   "mfa" if path.startswith("mfa/") else "password", status_code, safe_code)
    return {"error": f"Discord rejected the login or verification code (HTTP {status_code}, "
                     f"code {safe_code}). Check it and try /connect again."}


class DiscordSession:
    def __init__(self, app, send, limiter, bot_token=None, interval=60):
        self.app, self.send, self.limiter = app, send, limiter
        self.bot_token, self.interval = bot_token, interval
        self.client = None
        self.tasks = []
        self.lock = asyncio.Lock()
        self.discovery = None
        self.connection_error = None
        self.events = None

    async def connect(self, token, source="saved account"):
        logger.info("Discord connection requested: source=%s.", source)
        async with self.lock:
            token = token.strip().strip("\"'")
            if not token:
                raise DiscordConnectionError("No saved Discord token is available. Use /connect first.")
            candidate = CustomDiscordClient(token, self.bot_token)
            logger.info("Connecting to Discord gateway with the saved token.")
            task = asyncio.create_task(candidate.start())
            ready_task = asyncio.create_task(candidate.ready.wait())
            try:
                try:
                    done, _ = await asyncio.wait({task, ready_task}, timeout=35,
                                                 return_when=asyncio.FIRST_COMPLETED)
                    if task in done:
                        await task
                        raise DiscordConnectionError("Discord gateway stopped before the connection was activated.")
                    if ready_task not in done:
                        detail = candidate.last_gateway_error or "No gateway close code was received."
                        raise DiscordConnectionError("Discord login succeeded, but the gateway did not become ready "
                                                     f"within 35 seconds. {detail}")
                except GatewayConnectionError as error:
                    raise DiscordConnectionError("The gateway connection ended before it became ready. "
                                                 f"{error}") from None
                outbox = self.app.bot_data.get("notification_outbox")
                delivery = TicketDelivery(outbox, candidate.user["id"], candidate.user.get("username")) if outbox else None
                handler = DiscordHandler(candidate, self.send, self.limiter, delivery=delivery)
                discovery = TicketDiscovery(candidate, handler, self.interval)
            except BaseException:
                await candidate.close()
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
                raise
            finally:
                ready_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ready_task
            await self.close()
            self.client = candidate
            self.discovery = discovery
            self.events = GatewayEvents(candidate.user["id"], discovery)
            candidate.on_guild_channel_create = self.events.callback("channel_created")
            candidate.on_guild_channel_update = self.events.callback("channel_updated")
            candidate.on_guild_channel_delete = self.events.callback("channel_deleted")
            candidate.on_message = self.events.callback("message_created")
            self.app.bot_data["discord_client"] = candidate
            self.tasks = [task, asyncio.create_task(self.events.run()), asyncio.create_task(discovery.run())]
            self.connection_error = None
            def gateway_stopped(completed):
                if completed.cancelled() or self.client is not candidate:
                    return
                error = completed.exception()
                self.connection_error = str(error) if isinstance(error, GatewayConnectionError) else "Gateway worker stopped"
                logger.warning("%s", self.connection_error)
            task.add_done_callback(gateway_stopped)
            logger.info("Discord connection active: servers=%d; automatic discovery started.", len(candidate.guilds_list))
            return candidate.user.get("username", "Discord account")

    async def close(self):
        for task in self.tasks:
            task.cancel()
        for task in self.tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self.tasks = []
        if self.client:
            await self.client.close()
        self.client = None
        self.discovery = None
        self.events = None
        self.app.bot_data["discord_client"] = None
