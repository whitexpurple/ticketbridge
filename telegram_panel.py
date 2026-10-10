"""Private, administrator-only Telegram setup for Discord."""

import asyncio
import os
import time
import logging
from datetime import datetime, timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError
from telegram.ext import CommandHandler, CallbackQueryHandler, MessageHandler, filters
from dotenv import dotenv_values

from discord_session import DiscordConnectionError, ENV_PATH, login_request
from delivery import chunks

SETUP_TTL = 300
logger = logging.getLogger(__name__)


def is_authorized(update):
    try:
        admin = int(os.getenv("TELEGRAM_CHAT_ID", ""))
    except ValueError:
        return False
    return bool(update.effective_chat and update.effective_user
                and update.effective_chat.type == "private"
                and update.effective_chat.id == admin
                and update.effective_user.id == admin)


def clear_setup(context):
    state = context.user_data.pop("discord_setup", None)
    if state:
        state.clear()
    task = context.user_data.pop("setup_expiry", None)
    if task:
        task.cancel()


def begin_setup(context):
    clear_setup(context)
    state = {"step": "email", "expires": time.monotonic() + SETUP_TTL}
    context.user_data["discord_setup"] = state

    async def expire():
        await asyncio.sleep(SETUP_TTL)
        if context.user_data.get("discord_setup") is state:
            context.user_data.pop("discord_setup", None)
            context.user_data.pop("setup_expiry", None)
            state.clear()
            logger.info("Discord setup expired; send /connect to restart.")

    context.user_data["setup_expiry"] = asyncio.create_task(expire())


async def start(update, context):
    if not is_authorized(update):
        return
    clear_setup(context)
    await update.effective_message.reply_text(
        "Ticket Forwarder\n\nAutomatic discovery across all servers on your Discord account.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Connect Discord", callback_data="connect")],
            [InlineKeyboardButton("Connection status", callback_data="status")],
            [InlineKeyboardButton("My servers", callback_data="servers")],
        ]),
    )


async def connect(update, context):
    logger.info("Telegram /connect received.")
    if not is_authorized(update):
        logger.warning("Discord setup denied: use the private Telegram account matching TELEGRAM_CHAT_ID.")
        return
    begin_setup(context)
    logger.info("Discord setup started: waiting for email.")
    await update.effective_message.reply_text(
        "Connect Discord\n\nSend your Discord email address. "
        "Next I will ask for your password and, if needed, a two-factor code. "
        "I will attempt to delete those messages; email/password are not saved to disk. "
        "The resulting session token is saved locally in .env.\n\n"
        "Setup expires in 5 minutes. Use /cancel to stop."
    )


async def cancel(update, context):
    if not is_authorized(update):
        return
    clear_setup(context)
    logger.info("Discord setup cancelled by administrator.")
    await update.effective_message.reply_text("Setup cancelled. Your current connection is unchanged.")


async def reconnect(update, context):
    if not is_authorized(update):
        return
    clear_setup(context)
    token = dotenv_values(ENV_PATH).get("DISCORD_TOKEN")
    if not token:
        await update.effective_message.reply_text("No saved token. Use /connect to sign in first.")
        return
    logger.info("Telegram /reconnect received; reusing saved token without password login.")
    await update.effective_message.reply_text("Connecting with the saved Discord token...")
    try:
        username = await context.application.bot_data["discord_session"].connect(token, persist=False)
        await update.effective_message.reply_text(f"Connected as {username}.")
    except DiscordConnectionError as error:
        logger.warning("Saved-token connection failed: %s", error)
        await update.effective_message.reply_text(str(error))


async def status(update, context):
    if not is_authorized(update):
        return
    session = context.application.bot_data["discord_session"]
    client = session.client
    if not client:
        text = "Discord is disconnected. Use /connect to sign in."
    else:
        text = (f"Discord: {(client.user or {}).get('username', 'Connecting')}\n"
                f"Gateway: {'connected' if client.ready.is_set() else 'reconnecting'}\n"
                f"Servers monitored: {len(client.guilds_list)}\n"
                "Discovery: automatic, across all accessible channels and threads.\n"
                "Tickets hidden from this account cannot be detected.")
    discovery = session.discovery
    if discovery:
        scanned = (datetime.fromtimestamp(discovery.last_scan, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                   if discovery.last_scan else "Not completed yet")
        active = len(discovery.active_channel_ids())
        text += (f"\nActive tracked tickets: {active}\nLast completed scan: {scanned}"
                 f"\nScan running: {'yes' if discovery.scan_running else 'no'}"
                 f"\nScan health: {discovery.last_scan_error or 'OK'}")
        if discovery.last_successful_scan:
            text += "\nLast fully successful scan: " + datetime.fromtimestamp(discovery.last_successful_scan, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if session.connection_error:
        text += f"\nGateway error: {session.connection_error}"
    if session.events:
        text += f"\nPending Discord events: {session.events.pending_count()}"
        if session.events.last_error:
            text += f"\nEvent processing error: {session.events.last_error} (will retry)"
    outbox = context.application.bot_data.get("notification_outbox")
    if outbox:
        text += f"\nPending notifications: {outbox.pending_count()}"
        if outbox.last_error:
            text += f"\nDelivery error: {outbox.last_error} (will retry)"
        if outbox.last_success:
            text += "\nLast delivery: " + datetime.fromtimestamp(outbox.last_success, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    await update.effective_message.reply_text(text)


async def report(update, context):
    if not is_authorized(update):
        return
    worker = context.application.bot_data.get("daily_report_worker")
    if worker is None:
        await update.effective_message.reply_text("Daily reports are unavailable right now.")
        return
    limiter = context.application.bot_data.get("telegram_rate_limiter")
    for part in chunks(worker.current_report()):
        if limiter:
            await limiter.wait_if_needed()
        await update.effective_message.reply_text(part)


def server_page(client, page=0):
    guilds = sorted(client.guilds_list, key=lambda guild: (
        str(guild.get("name") or "Unknown server").casefold(), str(guild.get("id", ""))))
    total = len(guilds)
    page_size = 15
    pages = max(1, (total + page_size - 1) // page_size)
    page = min(max(0, page), pages - 1)
    lines = [f"My Discord servers: {total}"]
    if not client.ready.is_set():
        lines.append("Discord is reconnecting; showing the cached list.")
    if not guilds:
        lines.append("No servers are currently available in the account's server list.")
    for number, guild in enumerate(guilds[page * page_size:(page + 1) * page_size], page * page_size + 1):
        name = " ".join(str(guild.get("name") or "Unknown server").split())[:100]
        guild_id = str(guild.get("id", "Unknown"))[:20]
        unavailable = " (unavailable)" if guild.get("unavailable") else ""
        lines.append(f"\n{number}. {name}{unavailable}\nID: {guild_id}")
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("Previous", callback_data=f"servers:{page - 1}"))
    if page + 1 < pages:
        buttons.append(InlineKeyboardButton("Next", callback_data=f"servers:{page + 1}"))
    if pages > 1:
        lines.append(f"\nPage {page + 1} of {pages}")
    keyboard = [buttons] if buttons else []
    keyboard.append([InlineKeyboardButton("Close", callback_data="servers:close")])
    return "\n".join(lines), InlineKeyboardMarkup(keyboard)


async def servers(update, context, page=0):
    if not is_authorized(update):
        return
    session = context.application.bot_data.get("discord_session")
    client = session.client if session else None
    if client is None:
        text = "Discord is disconnected. Use /connect to sign in."
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("Close", callback_data="servers:close")]])
    else:
        text, markup = server_page(client, page)
    await context.application.bot_data["server_lists"].show(update, text, markup)


async def button_handler(update, context):
    await update.callback_query.answer()
    if not is_authorized(update):
        if update.callback_query.data == "connect":
            logger.warning("Telegram Connect Discord button denied: private administrator chat required.")
        return
    action = update.callback_query.data
    if action == "connect":
        await connect(update, context)
    elif action == "status":
        await status(update, context)
    elif action == "servers:close":
        message = update.effective_message
        closed = await context.application.bot_data["server_lists"].close(message.chat_id, message.message_id)
        if not closed:
            await message.reply_text("Could not close the list yet. I will retry automatically.")
    elif action == "servers" or action.startswith("servers:"):
        try:
            page = int(action.split(":", 1)[1]) if ":" in action else 0
        except ValueError:
            page = 0
        await servers(update, context, page)
    else:
        await update.effective_message.reply_text("Manual category setup is no longer needed. Use /start.")


async def setup_message(update, context):
    if not is_authorized(update):
        return
    state = context.user_data.get("discord_setup")
    if not state:
        logger.info("Telegram setup message received without an active login; send /connect first.")
        await update.effective_message.reply_text("Use /connect to sign in or /status to check discovery.")
        return
    value = update.effective_message.text
    try:
        await update.effective_message.delete()
    except TelegramError:
        logger.warning("Could not delete a Telegram setup message.")
        await update.effective_message.reply_text("I could not delete your setup message. Delete it from this chat.")
    if not state or time.monotonic() >= state["expires"]:
        clear_setup(context)
        await update.effective_message.reply_text("Setup expired. Use /connect again.")
        return
    step = state["step"]
    logger.info("Discord setup input received: stage=%s (content omitted).", step)
    if step == "email":
        if "@" not in value or len(value) > 320:
            logger.info("Discord setup email format rejected; waiting for another email.")
            await update.effective_message.reply_text("Send a valid email address or /cancel.")
            return
        state.update(step="password", email=value.strip())
        logger.info("Discord setup: waiting for password.")
        await update.effective_message.reply_text("Send your Discord password.")
        return
    try:
        if step == "password":
            result = await login_request("login", {"login": state["email"], "password": value,
                                                    "undelete": False})
        elif step == "mfa":
            code = value.strip().replace("-", "")
            if not (len(code) == 6 and code.isdigit()) and not (len(code) == 8 and code.isalnum()):
                logger.info("Discord setup verification code format rejected; waiting for another code.")
                await update.effective_message.reply_text("Send a six-digit authenticator code or an eight-character backup code.")
                return
            method = "totp" if len(code) == 6 and code.isdigit() else "backup"
            result = await login_request(f"mfa/{method}", {"ticket": state["ticket"], "code": code})
        else:
            return
        if not state or time.monotonic() >= state["expires"]:
            clear_setup(context)
            await update.effective_message.reply_text("Setup expired. Use /connect again.")
            return
        if result.get("ticket"):
            state.pop("email", None)
            state.update(step="mfa", ticket=result["ticket"])
            logger.info("Discord setup: waiting for two-factor or backup code.")
            await update.effective_message.reply_text("Send your authenticator code or Discord backup code.")
            return
        if result.get("error"):
            logger.warning("Discord setup request unsuccessful: stage=%s retry_allowed=%s.",
                           step, bool(result.get("retry")))
            if not result.get("retry"):
                clear_setup(context)
            await update.effective_message.reply_text(result["error"])
            return
        clear_setup(context)
        await update.effective_message.reply_text("Signing in to Discord...")
        username = await context.application.bot_data["discord_session"].connect(result["token"])
        result.clear()
        await update.effective_message.reply_text(
            f"Connected as {username}. Automatic ticket discovery is active across all your servers."
        )
    except DiscordConnectionError as error:
        logger.warning("Discord setup connection failed: %s", error)
        clear_setup(context)
        await update.effective_message.reply_text(str(error) + " Any previous connection has been kept. "
                                                 "If the token was saved, /reconnect retries it without another password login.")
    except Exception as error:
        logger.error("Discord setup failed: stage=%s exception_type=%s", step, type(error).__name__)
        clear_setup(context)
        await update.effective_message.reply_text(
            "Discord setup could not finish. Check your login and connection, then use /connect again. "
            "Any previous connection has been kept."
        )


async def telegram_error_handler(update, context):
    # Exception strings and Update objects can contain credentials or token URLs.
    logger.error("Telegram update/polling failed: exception_type=%s (details omitted).",
                 type(context.error).__name__)


def register_handlers(application):
    application.add_error_handler(telegram_error_handler)
    for command, handler in (("start", start), ("connect", connect), ("reconnect", reconnect),
                             ("cancel", cancel), ("status", status), ("report", report),
                             ("servers", servers)):
        application.add_handler(CommandHandler(command, handler))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, setup_message))
