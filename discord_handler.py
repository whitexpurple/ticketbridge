import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import quote

from database import (
    get_connection,
    get_server,
    add_ticket,
    get_ticket,
    close_ticket,
    update_ticket_user,
)
from delivery import TelegramNotification

logger = logging.getLogger("discord-handler")


def discord_server_url(guild_id):
    guild_id = str(guild_id or "")
    if not guild_id.isdigit():
        return None
    return f"https://discord.com/channels/{quote(guild_id, safe='')}"


# =========================================================
# DISCORD SNOWFLAKE HELPERS
# =========================================================

def discord_timestamp_from_id(user_id: str):
    """Convert a Discord snowflake ID to its creation time."""

    millis = (int(user_id) >> 22) + 1420070400000

    return datetime.fromtimestamp(
        millis / 1000,
        tz=timezone.utc,
    )


class DiscordHandler:

    def __init__(
        self,
        client,
        telegram_send,
        rate_limiter=None,
        delivery=None,
    ):
        self.client = client
        self.telegram_send = telegram_send
        self.rate_limiter = rate_limiter
        self.delivery = delivery
        self._invite_lock = asyncio.Lock()

        # IMPORTANT:
        #
        # Do NOT maintain an in-memory authoritative-ticket set.
        #
        # The database column:
        #
        #     tickets.user_confirmed
        #
        # is the source of truth.
        #
        # This survives bot restarts and also allows the database
        # UPDATE to atomically decide which message establishes
        # the ticket owner.

    async def server_link(self, guild_id):
        """Return a reusable server invite, falling back to Discord navigation."""
        fallback = discord_server_url(guild_id)
        if not fallback or not hasattr(self.client, "create_server_invite"):
            return fallback
        async with self._invite_lock:
            invite = await asyncio.to_thread(self.client.create_server_invite, str(guild_id))
        if invite:
            return invite
        logger.info("Could not create a Discord invite for server %s; using server navigation link", guild_id)
        return fallback

    # =====================================================
    # USER HELPERS
    # =====================================================

    def get_user_display_name(self, user):
        """Return the best available Discord display name."""

        if not user:
            return "Unknown"

        return (
            user.get("nick")
            or user.get("display_name")
            or user.get("global_name")
            or user.get("username")
            or "Unknown"
        )

    def get_user_username(self, user):
        """Return the Discord username."""

        if not user:
            return "Unknown"

        return user.get("username") or "Unknown"

    def resolve_user(self, user_id):
        """
        Try to find a Discord user in the local cache.

        This is intentionally best-effort.

        The first real MESSAGE_CREATE author is always more
        authoritative than this lookup.
        """

        if not user_id:
            return None

        user_id = str(user_id)

        try:
            user = self.client.get_user(user_id)

            if user:
                logger.info(
                    "Resolved user %s from client cache.",
                    user_id,
                )

                return user

        except Exception:
            logger.exception(
                "Error resolving user %s from client cache.",
                user_id,
            )

        # -------------------------------------------------
        # Some custom Discord clients expose a users dict.
        # Try that as a secondary lookup.
        # -------------------------------------------------

        try:
            users = getattr(
                self.client,
                "users",
                None,
            )

            if isinstance(users, dict):

                if user_id.isdigit():
                    user = (
                        users.get(user_id)
                        or users.get(int(user_id))
                    )
                else:
                    user = users.get(user_id)

                if user:
                    logger.info(
                        "Resolved user %s from client.users.",
                        user_id,
                    )

                    return user

        except Exception:
            logger.exception(
                "Error resolving user %s from client.users.",
                user_id,
            )

        logger.info(
            "User %s is not available in local cache.",
            user_id,
        )

        return None

    def resolve_member(
        self,
        guild_id,
        user_id,
    ):
        """
        Try to resolve a guild member.

        This is separate from resolve_user() because a guild
        member may contain nickname/role information that helps
        identify the ticket owner.
        """

        if not guild_id or not user_id:
            return None

        guild_id = str(guild_id)
        user_id = str(user_id)

        try:
            member = self.client.get_member(
                guild_id,
                user_id,
            )

            if member:
                logger.info(
                    "Resolved guild member: guild=%s user=%s",
                    guild_id,
                    user_id,
                )

                return member

        except Exception:
            logger.exception(
                "Error resolving guild member: "
                "guild=%s user=%s",
                guild_id,
                user_id,
            )

        return None

    def is_bot_user(
        self,
        user,
        user_id=None,
    ):
        """
        Return True if the user is a bot or is our own bot.

        Handles both:
            {"bot": True}

        and the client's own bot ID.
        """

        if user and user.get("bot"):
            return True

        if not user_id:
            return False

        user_id = str(user_id)

        try:
            bot_user = self.client.user

            if bot_user:
                bot_id = bot_user.get("id")

                if bot_id and str(bot_id) == user_id:
                    return True

        except Exception:
            logger.exception(
                "Error checking current bot identity."
            )

        return False

    # =====================================================
    # PROVISIONAL USER FROM PERMISSION OVERWRITES
    # =====================================================

    def extract_user_from_channel(
        self,
        channel,
    ):
        """
        Find a provisional ticket user from member permission
        overwrites.

        IMPORTANT:

        This function does NOT claim to know who created the
        ticket.

        The first real MESSAGE_CREATE author will replace this
        provisional value and become authoritative.
        """

        candidates = []

        permission_overwrites = channel.get(
            "permission_overwrites",
            [],
        )

        guild_id = channel.get(
            "guild_id"
        )

        logger.info(
            "Inspecting %d permission overwrites for channel %s.",
            len(permission_overwrites),
            channel.get("id"),
        )

        # -------------------------------------------------
        # Inspect every member-specific overwrite.
        # -------------------------------------------------

        for overwrite in permission_overwrites:

            # Discord:
            # type=1 -> member
            # type=0 -> role
            if overwrite.get("type") != 1:
                continue

            user_id = overwrite.get(
                "id"
            )

            if not user_id:
                continue

            user_id = str(user_id)

            try:
                allow = int(
                    overwrite.get(
                        "allow",
                        0,
                    )
                )
            except (
                ValueError,
                TypeError,
            ):
                allow = 0

            try:
                deny = int(
                    overwrite.get(
                        "deny",
                        0,
                    )
                )
            except (
                ValueError,
                TypeError,
            ):
                deny = 0

            logger.info(
                "Checking member overwrite: "
                "user=%s allow=%s deny=%s",
                user_id,
                allow,
                deny,
            )

            # -------------------------------------------------
            # Exclude our own bot.
            # -------------------------------------------------

            if self.is_bot_user(
                None,
                user_id,
            ):
                logger.info(
                    "Ignoring our own bot user: %s",
                    user_id,
                )
                continue

            # -------------------------------------------------
            # Require explicit VIEW_CHANNEL.
            #
            # Discord VIEW_CHANNEL = 1024
            # -------------------------------------------------

            if not (allow & 1024):
                logger.info(
                    "Ignoring %s: VIEW_CHANNEL not explicitly allowed.",
                    user_id,
                )
                continue

            # -------------------------------------------------
            # Ignore explicit VIEW_CHANNEL deny.
            # -------------------------------------------------

            if deny & 1024:
                logger.info(
                    "Ignoring %s: VIEW_CHANNEL explicitly denied.",
                    user_id,
                )
                continue

            # -------------------------------------------------
            # Resolve user.
            # -------------------------------------------------

            user = self.resolve_user(
                user_id
            )

            # -------------------------------------------------
            # Resolve guild member.
            # -------------------------------------------------

            member = self.resolve_member(
                guild_id,
                user_id,
            )

            # -------------------------------------------------
            # If normal user lookup failed but guild member
            # exists, use member as the user object.
            # -------------------------------------------------

            resolved_user = user or member

            # -------------------------------------------------
            # Never accept a resolved bot as a candidate.
            # -------------------------------------------------

            if self.is_bot_user(
                resolved_user,
                user_id,
            ):
                logger.info(
                    "Ignoring resolved bot candidate: %s",
                    user_id,
                )
                continue

            candidate = {
                "id": user_id,
                "user": resolved_user,
                "member": member,
                "allow": allow,
                "deny": deny,
            }

            candidates.append(
                candidate
            )

            logger.info(
                "Added ticket candidate: "
                "id=%s resolved=%s member=%s",
                user_id,
                bool(resolved_user),
                bool(member),
            )

        # -------------------------------------------------
        # Log all candidates.
        # -------------------------------------------------

        logger.info(
            "Ticket permission candidates: %s",
            [
                {
                    "id": candidate["id"],
                    "resolved": bool(candidate["user"]),
                    "member": bool(candidate["member"]),
                }
                for candidate in candidates
            ],
        )

        # -------------------------------------------------
        # No candidates.
        # -------------------------------------------------

        if not candidates:
            logger.warning(
                "No valid member-specific ticket-user "
                "candidates found."
            )

            return (
                None,
                "Unknown",
                "Unknown",
            )

        # =================================================
        # CANDIDATE RANKING
        # =================================================

        def candidate_score(candidate):
            score = 0

            user = candidate.get(
                "user"
            )

            member = candidate.get(
                "member"
            )

            if member:
                score += 100

            if user:
                score += 50

            if user and user.get("username"):
                score += 10

            if member and (
                member.get("nick")
                or member.get("display_name")
            ):
                score += 5

            return score

        candidates.sort(
            key=candidate_score,
            reverse=True,
        )

        logger.info(
            "Ranked ticket candidates: %s",
            [
                (
                    candidate["id"],
                    candidate_score(candidate),
                )
                for candidate in candidates
            ],
        )

        # -------------------------------------------------
        # Select highest-ranked provisional candidate.
        # -------------------------------------------------

        selected = candidates[0]

        user_id = selected["id"]
        user = selected["user"]

        if user:

            username = self.get_user_username(
                user
            )

            display_name = self.get_user_display_name(
                user
            )

            logger.warning(
                "Selected PROVISIONAL ticket user: "
                "id=%s username=%s display_name=%s",
                user_id,
                username,
                display_name,
            )

            return (
                user_id,
                username,
                display_name,
            )

        logger.warning(
            "Selected PROVISIONAL ticket user by ID only: %s",
            user_id,
        )

        return (
            user_id,
            "Unknown",
            "Unknown",
        )

    # =====================================================
    # CHANNEL CREATE
    # =====================================================

    async def channel_created(
        self,
        channel,
        auto_discovered=False,
    ):
        """Handle a new ticket channel."""

        channel_type = channel.get("type")

        if channel_type not in (
            0,   # guild text channel
            10,  # announcement thread
            11,  # public thread / forum post
            12,  # private thread
        ):
            logger.info(
                "Ignoring channel %s: unsupported type=%s.",
                channel.get("id"),
                channel_type,
            )
            return

        channel_id = channel.get(
            "id"
        )

        if channel_id:
            existing_ticket = get_ticket(
                channel_id
            )

            if existing_ticket and existing_ticket[6]:
                logger.info(
                    "Ignoring already-tracked ticket channel: %s",
                    channel_id,
                )

                return

        logger.info(
            "Permission overwrites: %s",
            channel.get(
                "permission_overwrites",
                [],
            ),
        )

        guild_id = channel.get(
            "guild_id"
        )

        if not guild_id:
            logger.warning(
                "CHANNEL_CREATE has no guild_id."
            )
            return

        if auto_discovered:
            guild = self.client.get_guild(guild_id) or {}
            config = (guild_id, guild.get("name", "Unknown"), channel.get("parent_id"), True)
        else:
            config = get_server(guild_id)

        if not config:
            logger.info(
                "Ignoring channel: guild %s is not configured.",
                guild_id,
            )
            return

        _, guild_name, category_id, enabled = config

        if not enabled:
            logger.info(
                "Ignoring channel: guild %s is disabled.",
                guild_id,
            )
            return

        direct_parent_match = (
            auto_discovered or str(channel.get("parent_id")) == str(category_id)
        )

        nested_parent_match = False

        try:
            nested_parent_match = self.client.is_channel_under_parent(
                channel,
                category_id,
            )

        except Exception:
            logger.exception(
                "Could not check channel parent chain: channel=%s parent=%s.",
                channel_id,
                category_id,
            )

        if not (
            direct_parent_match
            or nested_parent_match
        ):
            logger.info(
                "Ignoring channel %s: wrong category. "
                "parent_id=%s configured_id=%s type=%s",
                channel_id,
                channel.get("parent_id"),
                category_id,
                channel_type,
            )
            return

        logger.info(
            "Tracking ticket channel: channel=%s parent_id=%s "
            "configured_id=%s type=%s nested=%s.",
            channel_id,
            channel.get("parent_id"),
            category_id,
            channel_type,
            nested_parent_match,
        )

        # -------------------------------------------------
        # Extract provisional user.
        # -------------------------------------------------

        logger.info(
            "ABOUT TO EXTRACT PROVISIONAL TICKET USER"
        )

        (
            user_id,
            username,
            display_name,
        ) = await asyncio.to_thread(
            self.extract_user_from_channel, channel
        )

        logger.info(
            "EXTRACTED PROVISIONAL TICKET USER: "
            "id=%s username=%s display_name=%s",
            user_id,
            username,
            display_name,
        )

        now = datetime.now(
            timezone.utc
        )

        if not channel_id:
            logger.warning(
                "CHANNEL_CREATE has no channel ID."
            )
            return

        channel_id = str(channel_id)

        channel_name = channel.get(
            "name",
            "unknown",
        )

        # -------------------------------------------------
        # Add ticket.
        #
        # add_ticket() stores this user as provisional:
        #
        #     user_confirmed = 0
        #
        # The first human MESSAGE_CREATE will atomically
        # replace it.
        # -------------------------------------------------

        logger.info(
            "ABOUT TO ADD TICKET: channel=%s",
            channel_id,
        )

        ticket_values = (channel_id, guild_id, channel_name, user_id, username, now.isoformat())
        if not self.delivery:
            add_ticket(*ticket_values)

        logger.info(
            "AFTER ADD TICKET: channel=%s",
            channel_id,
        )

        # -------------------------------------------------
        # Server daily total.
        # -------------------------------------------------

        conn = get_connection()

        try:

            server_daily_total = conn.execute(
                """
                SELECT COUNT(*)
                FROM tickets
                WHERE guild_id=?
                AND date(opened_at)=date(?)
                """,
                (
                    guild_id,
                    now.isoformat(),
                ),
            ).fetchone()[0]

            # -------------------------------------------------
            # Provisional user daily total.
            # -------------------------------------------------

            user_tickets_today = 0

            if user_id:

                user_tickets_today = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM tickets
                    WHERE user_id=?
                    AND date(opened_at)=date(?)
                    """,
                    (
                        str(user_id),
                        now.isoformat(),
                    ),
                ).fetchone()[0]

        finally:
            conn.close()

        # -------------------------------------------------
        # Category name.
        # -------------------------------------------------

        category_name = "Unknown"

        parent_id = channel.get(
            "parent_id"
        )

        if parent_id:

            try:

                parent = self.client.get_channel(
                    parent_id
                )

                if parent:

                    category_name = parent.get(
                        "name",
                        "Unknown",
                    )

            except Exception:

                logger.exception(
                    "Could not resolve category %s.",
                    parent_id,
                )

        # -------------------------------------------------
        # Account age.
        # -------------------------------------------------

        account_age = "Unknown"

        if user_id:

            try:

                created_at = (
                    discord_timestamp_from_id(
                        user_id
                    )
                )

                age_days = (
                    now - created_at
                ).days

                account_age = (
                    f"{age_days} days"
                )

            except (
                ValueError,
                TypeError,
                OverflowError,
            ):

                account_age = "Unknown"

        # -------------------------------------------------
        # Mutual guilds.
        # -------------------------------------------------

        if self.delivery:
            server_daily_total += 1
            if user_id:
                user_tickets_today += 1

        # -------------------------------------------------
        # Telegram notification.
        # -------------------------------------------------

        heading = "🎫 DISCOVERED TICKET\n\n" if auto_discovered else "🎫 NEW TICKET\n\n"
        notification = TelegramNotification()
        notification.add(heading)
        notification.field("🏠 Server", guild_name)
        notification.field("📊 Server Daily Total", server_daily_total)
        notification.field("🎟️ Ticket", f"#{channel_name}")
        notification.field("📂 Category", category_name)
        notification.add("\n")
        notification.field("👤 User", display_name)
        notification.field("📛 Username", username)
        notification.field("🆔 User ID", user_id or "Unknown")
        notification.field("📊 Tickets Today (User)", user_tickets_today)
        notification.field("📅 Account Age", account_age)
        notification.add(f"\n🕐 Opened: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n")
        server_url = await self.server_link(guild_id)
        if server_url:
            notification.link("Open Server", server_url)
        text, entities = notification.render()

        if self.delivery:
            self.delivery.opened(ticket_values, text, entities)
        else:
            await self.telegram_send(text, entities)

    # =====================================================
    # MESSAGE CREATE
    # =====================================================

    async def message_created(
        self,
        message,
    ):
        """
        Forward messages from tracked tickets.

        CRITICAL RULE:

        The FIRST non-bot message author is the authoritative
        ticket user.

        Permission overwrites are provisional only.

        After the first real message establishes the owner,
        later messages NEVER change the ticket owner.

        The database field `user_confirmed` is the source of
        truth.
        """

        logger.info(
            "MESSAGE_CREATE received."
        )

        # =================================================
        # AUTHOR
        # =================================================

        author = message.get(
            "author",
            {},
        )

        if not author:
            logger.warning(
                "MESSAGE_CREATE has no author."
            )
            return

        # =================================================
        # IGNORE BOTS
        # =================================================

        if author.get("bot"):
            logger.info(
                "Ignoring bot message from user=%s.",
                author.get("id"),
            )
            return

        author_id = author.get(
            "id"
        )

        if not author_id:
            logger.warning(
                "MESSAGE_CREATE has no author ID."
            )
            return

        author_id = str(
            author_id
        )

        # =================================================
        # IGNORE OUR OWN BOT
        # =================================================

        if self.is_bot_user(
            author,
            author_id,
        ):
            logger.info(
                "Ignoring message from our own bot: %s",
                author_id,
            )
            return

        # =================================================
        # CHANNEL
        # =================================================

        channel_id = message.get(
            "channel_id"
        )

        if not channel_id:
            logger.warning(
                "MESSAGE_CREATE has no channel_id."
            )
            return

        channel_id = str(
            channel_id
        )

        logger.info(
            "MESSAGE_CREATE channel=%s author=%s",
            channel_id,
            author_id,
        )

        # =================================================
        # FIND TICKET
        # =================================================

        ticket = get_ticket(
            channel_id
        )

        if not ticket:
            logger.info(
                "Ignoring message in untracked channel %s.",
                channel_id,
            )
            return

        # -------------------------------------------------
        # Ticket tuple:
        #
        # 0 = channel_id
        # 1 = guild_id
        # 2 = channel_name
        # 3 = user_id
        # 4 = user_name
        # 5 = opened_at
        # 6 = active
        # 7 = user_confirmed
        # -------------------------------------------------

        if not ticket[6]:
            logger.info(
                "Ignoring message in closed ticket %s.",
                channel_id,
            )
            return

        guild_id = ticket[1]

        # =================================================
        # GUILD CONFIGURATION
        # =================================================

        guild_name = (self.client.get_guild(guild_id) or {}).get("name", "Unknown")

        # =================================================
        # AUTHOR INFORMATION
        # =================================================

        author_username = (
            self.get_user_username(
                author
            )
        )

        author_display_name = (
            self.get_user_display_name(
                author
            )
        )

        logger.info(
            "MESSAGE AUTHOR: "
            "id=%s username=%s display_name=%s",
            author_id,
            author_username,
            author_display_name,
        )

        # =================================================
        # CACHE AUTHOR
        # =================================================

        try:

            users = getattr(
                self.client,
                "users",
                None,
            )

            if isinstance(
                users,
                dict,
            ):

                users[author_id] = author

                logger.info(
                    "Cached MESSAGE_CREATE author %s.",
                    author_id,
                )

        except Exception:

            logger.exception(
                "Could not cache MESSAGE_CREATE author."
            )

        # =================================================
        # FIRST HUMAN MESSAGE
        # =================================================
        #
        # IMPORTANT:
        #
        # update_ticket_user() performs this atomically:
        #
        #   WHERE active = 1
        #   AND user_confirmed = 0
        #
        # Therefore if two human messages arrive close
        # together, only the first successful database update
        # establishes the owner.
        # =================================================

        user_confirmed = bool(
            ticket[7]
        )

        logger.info(
            "Ticket ownership state: "
            "channel=%s "
            "stored_user=%s "
            "stored_username=%s "
            "user_confirmed=%s",
            channel_id,
            ticket[3],
            ticket[4],
            user_confirmed,
        )

        if not user_confirmed:

            logger.warning(
                "FIRST HUMAN MESSAGE detected: "
                "channel=%s author=%s username=%s",
                channel_id,
                author_id,
                author_username,
            )

            try:

                changed = update_ticket_user(
                    channel_id,
                    author_id,
                    author_username,
                )

                if changed:

                    logger.info(
                        "AUTHORITATIVE USER SET: "
                        "channel=%s user=%s",
                        channel_id,
                        author_id,
                    )

                else:

                    logger.info(
                        "AUTHORITATIVE USER was already set "
                        "by another operation: "
                        "channel=%s attempted_user=%s",
                        channel_id,
                        author_id,
                    )

            except Exception:

                logger.exception(
                    "FAILED to establish authoritative "
                    "ticket user: "
                    "channel=%s author=%s",
                    channel_id,
                    author_id,
                )

            # -------------------------------------------------
            # ALWAYS reload after attempting confirmation.
            #
            # This is important because another MESSAGE_CREATE
            # may have confirmed the ticket first.
            # -------------------------------------------------

            ticket = get_ticket(
                channel_id
            )

            if not ticket:

                logger.error(
                    "Ticket disappeared after user update: "
                    "channel=%s",
                    channel_id,
                )

                return

            if not ticket[6]:

                logger.info(
                    "Ticket became closed while establishing "
                    "the authoritative user: channel=%s",
                    channel_id,
                )

                return

            logger.info(
                "Ticket after authoritative update: %r",
                ticket,
            )

        else:

            logger.info(
                "Ticket already has authoritative owner: "
                "channel=%s owner=%s. "
                "Current message author=%s will NOT replace it.",
                channel_id,
                ticket[3],
                author_id,
            )

        # =================================================
        # DETERMINE AUTHORITATIVE TICKET USER
        # =================================================

        ticket_user_id = ticket[3]
        ticket_user_name = ticket[4]

        if not ticket_user_id:

            logger.warning(
                "Ticket has no authoritative user after "
                "MESSAGE_CREATE: channel=%s",
                channel_id,
            )

            # This should normally never happen because the
            # first human message should have confirmed it.
            #
            # We use the current author only as a display
            # fallback. We do NOT silently write it again.
            ticket_user_id = author_id
            ticket_user_name = author_username

        else:

            ticket_user_id = str(
                ticket_user_id
            )

        if not ticket_user_name:
            ticket_user_name = "Unknown"

        # =================================================
        # MESSAGE CONTENT
        # =================================================

        text = message.get(
            "content",
            "",
        ).strip()

        if not text:
            text = "[No text content]"

        now = datetime.now(
            timezone.utc
        )

        # =================================================
        # USER TICKETS TODAY
        # =================================================
        #
        # Deliberately based on ticket_user_id,
        # NOT author_id.
        # =================================================

        conn = get_connection()

        try:

            user_tickets_today = conn.execute(
                """
                SELECT COUNT(*)
                FROM tickets
                WHERE user_id=?
                AND date(opened_at)=date(?)
                """,
                (
                    ticket_user_id,
                    now.isoformat(),
                ),
            ).fetchone()[0]

        finally:

            conn.close()

        # =================================================
        # ACCOUNT AGE
        # =================================================

        account_age = "Unknown"

        try:

            created_at = (
                discord_timestamp_from_id(
                    ticket_user_id
                )
            )

            age_days = (
                now - created_at
            ).days

            account_age = (
                f"{age_days} days"
            )

        except (
            ValueError,
            TypeError,
            OverflowError,
        ):

            account_age = "Unknown"

        # =================================================
        # RESOLVE TICKET USER FOR DISPLAY
        # =================================================

        ticket_user = None

        try:

            ticket_user = await asyncio.to_thread(self.resolve_user, ticket_user_id)

        except Exception:

            logger.exception(
                "Could not resolve authoritative "
                "ticket user: %s",
                ticket_user_id,
            )

        if ticket_user:

            ticket_display_name = (
                self.get_user_display_name(
                    ticket_user
                )
            )

            resolved_ticket_username = (
                self.get_user_username(
                    ticket_user
                )
            )

            if (
                resolved_ticket_username
                and resolved_ticket_username != "Unknown"
            ):

                ticket_user_name = (
                    resolved_ticket_username
                )

        else:

            ticket_display_name = (
                ticket_user_name
                or "Unknown"
            )

        # =================================================
        # MUTUAL GUILDS
        # =================================================


        # =================================================
        # BUILD TELEGRAM NOTIFICATION
        # =================================================

        notification = TelegramNotification()
        notification.add("💬 TICKET MESSAGE\n\n")
        notification.field("🏠 Server", guild_name)
        notification.field("🎟️ Ticket", f"#{ticket[2]}")
        notification.add("\n")
        notification.field("👤 Ticket User", ticket_display_name)
        notification.field("📛 Ticket Username", ticket_user_name)
        notification.field("🆔 Ticket User ID", ticket_user_id)
        notification.field("📊 Tickets Today (User)", user_tickets_today)
        notification.field("📅 Account Age", account_age)
        notification.add("\n")
        notification.field("💬 Message From", author_display_name)
        notification.field("📛 Message Username", author_username)
        notification.field("🆔 Message Author ID", author_id)
        notification.add(f"\n{text}")

        # =================================================
        # ATTACHMENTS
        # =================================================

        attachments = message.get(
            "attachments",
            [],
        )

        if attachments:

            notification.add("\n\n📎 Attachments:")

            for attachment in attachments:

                url = attachment.get(
                    "url"
                )

                if url:

                    notification.add(f"\n{url}")

        notification.add("\n\n")
        server_url = await self.server_link(guild_id)
        if server_url:
            notification.link("Open Server", server_url)
        output, entities = notification.render()

        # =================================================
        # SEND TELEGRAM
        # =================================================

        if self.delivery:
            self.delivery.message(channel_id, message.get("id"), output, entities)
        else:
            await self.telegram_send(output, entities)

    # =====================================================
    # CHANNEL UPDATE
    # =====================================================

    async def channel_updated(
        self,
        channel,
    ):
        """Handle ticket channel updates."""

        channel_id = channel.get(
            "id"
        )

        if not channel_id:
            return

        channel_id = str(
            channel_id
        )

        ticket = get_ticket(
            channel_id
        )

        if not ticket:
            return

        # IMPORTANT:
        #
        # ticket[6] = active
        # ticket[7] = user_confirmed
        #
        # Do NOT use ticket[-1] here.
        if not ticket[6]:
            return

        new_name = channel.get(
            "name",
            "",
        )

        metadata = channel.get("thread_metadata") or {}
        closed_thread = metadata.get("archived") and metadata.get("locked")
        if new_name.lower().startswith("closed-") or closed_thread:

            await self.channel_deleted(
                channel
            )

    # =====================================================
    # CHANNEL DELETE / TICKET CLOSE
    # =====================================================

    async def channel_deleted(
        self,
        channel,
    ):
        """Handle ticket closure."""

        channel_id = channel.get(
            "id"
        )

        if not channel_id:
            return

        channel_id = str(
            channel_id
        )

        ticket = get_ticket(
            channel_id
        )

        if not ticket:
            return

        # IMPORTANT:
        #
        # ticket[6] = active
        # ticket[7] = user_confirmed
        #
        # The old ticket[-1] check was wrong after adding
        # user_confirmed because it checked confirmation state
        # instead of active state.
        if not ticket[6]:
            return

        guild = self.client.get_guild(ticket[1]) or {}
        guild_name = guild.get("name")
        if not guild_name:
            guild_config = get_server(ticket[1])
            guild_name = guild_config[1] if guild_config else "Unknown"

        # -------------------------------------------------
        # Open time.
        # -------------------------------------------------

        try:

            opened_at = datetime.fromisoformat(
                ticket[5]
            )

            if opened_at.tzinfo is None:

                opened_at = opened_at.replace(
                    tzinfo=timezone.utc
                )

        except (
            ValueError,
            TypeError,
        ):

            opened_at = datetime.now(
                timezone.utc
            )

        now = datetime.now(
            timezone.utc
        )

        duration = (
            now - opened_at
        )

        # -------------------------------------------------
        # User.
        # -------------------------------------------------

        user_name = (
            ticket[4]
            or "Unknown"
        )

        user_id = (
            ticket[3]
            or "Unknown"
        )

        # -------------------------------------------------
        # IMPORTANT:
        #
        # The database user is authoritative.
        #
        # We do NOT use permission overwrites here.
        #
        # We also do NOT call update_ticket_user() here,
        # because once the first human message confirmed the
        # ticket, that function intentionally refuses to
        # replace the owner.
        #
        # We only refresh display information locally.
        # -------------------------------------------------

        resolved_user = None

        if user_id != "Unknown":

            resolved_user = await asyncio.to_thread(self.resolve_user, user_id)

            if resolved_user:

                resolved_username = (
                    self.get_user_username(
                        resolved_user
                    )
                )

                if (
                    resolved_username
                    and resolved_username != "Unknown"
                ):

                    user_name = (
                        resolved_username
                    )

        # -------------------------------------------------
        # Mark ticket closed.
        # -------------------------------------------------

        if not self.delivery:
            close_ticket(channel_id)

        # -------------------------------------------------
        # Server daily total.
        # -------------------------------------------------

        conn = get_connection()

        try:

            server_daily_total = conn.execute(
                """
                SELECT COUNT(*)
                FROM tickets
                WHERE guild_id=?
                AND date(opened_at)=date(?)
                """,
                (
                    ticket[1],
                    now.isoformat(),
                ),
            ).fetchone()[0]

            # -------------------------------------------------
            # User daily total.
            # -------------------------------------------------

            user_tickets_today = 0

            if user_id != "Unknown":

                user_tickets_today = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM tickets
                    WHERE user_id=?
                    AND date(opened_at)=date(?)
                    """,
                    (
                        user_id,
                        now.isoformat(),
                    ),
                ).fetchone()[0]

        finally:

            conn.close()

        # -------------------------------------------------
        # Account age.
        # -------------------------------------------------

        account_age = "Unknown"

        if user_id != "Unknown":

            try:

                created_at = (
                    discord_timestamp_from_id(
                        user_id
                    )
                )

                age_days = (
                    now - created_at
                ).days

                account_age = (
                    f"{age_days} days"
                )

            except (
                ValueError,
                TypeError,
                OverflowError,
            ):

                account_age = "Unknown"

        # -------------------------------------------------
        # Mutual guilds.
        # -------------------------------------------------


        # -------------------------------------------------
        # Display name.
        # -------------------------------------------------

        display_name = user_name

        if resolved_user:

            display_name = (
                self.get_user_display_name(
                    resolved_user
                )
            )

            resolved_username = (
                self.get_user_username(
                    resolved_user
                )
            )

            if (
                resolved_username
                and resolved_username != "Unknown"
            ):

                user_name = (
                    resolved_username
                )

        # -------------------------------------------------
        # Telegram notification.
        # -------------------------------------------------

        notification = TelegramNotification()
        notification.add("🔒 TICKET CLOSED\n\n")
        notification.field("🏠 Server", guild_name)
        notification.field("📊 Server Daily Total", server_daily_total)
        notification.field("🎟️ Ticket", f"#{ticket[2]}")
        notification.add("\n")
        notification.field("👤 User", display_name)
        notification.field("📛 Username", user_name)
        notification.field("🆔 User ID", user_id)
        notification.field("📊 Tickets Today (User)", user_tickets_today)
        notification.field("📅 Account Age", account_age)
        notification.add(f"\n⏱️ Duration: {duration}\n\n")
        server_url = await self.server_link(ticket[1])
        if server_url:
            notification.link("Open Server", server_url)
        output, entities = notification.render()

        if self.delivery:
            self.delivery.closed(channel_id, output, entities)
        else:
            await self.telegram_send(output, entities)

        logger.info(
            "Ticket closed successfully: channel=%s owner=%s",
            channel_id,
            user_id,
        )
