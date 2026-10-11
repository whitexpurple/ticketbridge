import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


logger = logging.getLogger("database")


DB_PATH = Path("data/bot.db")


# =========================================================
# CONNECTION
# =========================================================

def get_connection():
    """
    Return a SQLite connection.

    The database directory is created automatically.
    """

    DB_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    return sqlite3.connect(DB_PATH)


# =========================================================
# DATABASE INITIALIZATION
# =========================================================

def init_db():
    """
    Initialize the database.

    This function is migration-safe for existing databases.

    The important addition is:

        tickets.user_confirmed

    0 = user_id is provisional / came from permission overwrites
    1 = user_id was confirmed by the first human message
    """

    conn = get_connection()

    try:
        cursor = conn.cursor()

        # -------------------------------------------------
        # SERVERS
        # -------------------------------------------------

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS servers (
                guild_id TEXT PRIMARY KEY,
                guild_name TEXT NOT NULL,
                category_id TEXT NOT NULL,
                enabled INTEGER DEFAULT 1
            )
            """
        )

        # -------------------------------------------------
        # TICKETS
        # -------------------------------------------------

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                channel_id TEXT PRIMARY KEY,
                guild_id TEXT NOT NULL,
                channel_name TEXT NOT NULL,
                user_id TEXT,
                user_name TEXT,
                opened_at TEXT NOT NULL,
                active INTEGER DEFAULT 1,
                user_confirmed INTEGER DEFAULT 0
            )
            """
        )

        # -------------------------------------------------
        # MIGRATION FOR EXISTING DATABASES
        # -------------------------------------------------

        cursor.execute(
            """
            PRAGMA table_info(tickets)
            """
        )

        ticket_columns = {
            row[1]
            for row in cursor.fetchall()
        }

        if "user_confirmed" not in ticket_columns:

            logger.info(
                "Migrating tickets table: "
                "adding user_confirmed column."
            )

            cursor.execute(
                """
                ALTER TABLE tickets
                ADD COLUMN user_confirmed INTEGER DEFAULT 0
                """
            )

        if "closed_at" not in ticket_columns:
            cursor.execute("ALTER TABLE tickets ADD COLUMN closed_at TEXT")
        if "account_id" not in ticket_columns:
            cursor.execute("ALTER TABLE tickets ADD COLUMN account_id TEXT")
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS monitor_state (
                account_id TEXT PRIMARY KEY, started_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tracked_channels (
                account_id TEXT NOT NULL, channel_id TEXT NOT NULL,
                last_message_id TEXT,
                PRIMARY KEY(account_id, channel_id)
            );
            CREATE TABLE IF NOT EXISTS notification_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT NOT NULL UNIQUE, chat_id TEXT NOT NULL,
                text TEXT NOT NULL, created_at REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt REAL NOT NULL DEFAULT 0,
                delivered_at REAL, last_error TEXT, entities TEXT
            );
            CREATE INDEX IF NOT EXISTS outbox_pending ON notification_outbox(chat_id, delivered_at, id);
            CREATE INDEX IF NOT EXISTS active_tickets ON tickets(active, guild_id);
            CREATE TABLE IF NOT EXISTS gateway_inbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT NOT NULL,
                kind TEXT NOT NULL, payload TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt REAL NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS gateway_account ON gateway_inbox(account_id, id);
            CREATE TABLE IF NOT EXISTS telegram_server_lists (
                chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                expires_at REAL NOT NULL,
                PRIMARY KEY(chat_id, message_id)
            );
            CREATE TABLE IF NOT EXISTS daily_reports (
                report_date TEXT PRIMARY KEY, created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS daily_report_state (
                state_id INTEGER PRIMARY KEY CHECK (state_id=1), last_report_date TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS discovery_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                checked_at TEXT NOT NULL,
                guild_id TEXT NOT NULL,
                channels_ok INTEGER NOT NULL,
                threads_ok INTEGER NOT NULL,
                channels_error TEXT,
                threads_error TEXT
            );
            CREATE INDEX IF NOT EXISTS discovery_checks_time ON discovery_checks(checked_at, guild_id);
            CREATE TABLE IF NOT EXISTS discovery_scan_state (
                account_id TEXT PRIMARY KEY, last_guild_id TEXT
            );
            CREATE TABLE IF NOT EXISTS discovery_endpoint_state (
                account_id TEXT NOT NULL,
                guild_id TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                inaccessible_until REAL NOT NULL,
                PRIMARY KEY(account_id, guild_id, endpoint)
            );
            CREATE TABLE IF NOT EXISTS health_alert_state (
                issue_key TEXT PRIMARY KEY, first_seen REAL NOT NULL,
                alerted INTEGER NOT NULL DEFAULT 0
            );
        """)

        conn.execute("""UPDATE tickets SET account_id=(
            SELECT tracked_channels.account_id FROM tracked_channels
            WHERE tracked_channels.channel_id=tickets.channel_id LIMIT 1
        ) WHERE account_id IS NULL AND EXISTS (
            SELECT 1 FROM tracked_channels WHERE tracked_channels.channel_id=tickets.channel_id
        )""")

        discovery_columns = {row[1] for row in conn.execute("PRAGMA table_info(discovery_checks)")}
        if "channels_error" not in discovery_columns:
            conn.execute("ALTER TABLE discovery_checks ADD COLUMN channels_error TEXT")
        if "threads_error" not in discovery_columns:
            conn.execute("ALTER TABLE discovery_checks ADD COLUMN threads_error TEXT")

        outbox_columns = {row[1] for row in conn.execute("PRAGMA table_info(notification_outbox)")}
        if "entities" not in outbox_columns:
            conn.execute("ALTER TABLE notification_outbox ADD COLUMN entities TEXT")

        report_columns = {row[1] for row in conn.execute("PRAGMA table_info(daily_reports)")}
        if "account_id" not in report_columns:
            conn.execute("ALTER TABLE daily_reports RENAME TO daily_reports_legacy")
            conn.execute("""CREATE TABLE daily_reports (
                report_date TEXT NOT NULL, account_id TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL,
                PRIMARY KEY(report_date, account_id))""")
            conn.execute("INSERT OR IGNORE INTO daily_reports(report_date,account_id,created_at) "
                         "SELECT report_date,'',created_at FROM daily_reports_legacy")
            conn.execute("DROP TABLE daily_reports_legacy")

        conn.commit()

        logger.info(
            "Database initialized successfully: %s",
            DB_PATH,
        )

    finally:
        conn.close()


# =========================================================
# SERVER MANAGEMENT
# =========================================================

def add_server(
    guild_id,
    guild_name,
    category_id,
):
    """
    Add or replace a configured Discord server.
    """

    guild_id = str(guild_id)
    category_id = str(category_id)

    conn = get_connection()

    try:

        conn.execute(
            """
            INSERT OR REPLACE INTO servers
            (
                guild_id,
                guild_name,
                category_id,
                enabled
            )
            VALUES (?, ?, ?, 1)
            """,
            (
                guild_id,
                guild_name,
                category_id,
            ),
        )

        conn.commit()

        logger.info(
            "Server configured: "
            "guild_id=%s guild_name=%s category_id=%s",
            guild_id,
            guild_name,
            category_id,
        )

    finally:
        conn.close()


def remove_server(guild_id):
    """
    Remove a configured Discord server.
    """

    guild_id = str(guild_id)

    conn = get_connection()

    try:

        conn.execute(
            """
            DELETE FROM servers
            WHERE guild_id = ?
            """,
            (guild_id,),
        )

        conn.commit()

        logger.info(
            "Server removed: guild_id=%s",
            guild_id,
        )

    finally:
        conn.close()


def get_servers():
    """
    Return all configured servers.
    """

    conn = get_connection()

    try:

        rows = conn.execute(
            """
            SELECT
                guild_id,
                guild_name,
                category_id,
                enabled
            FROM servers
            ORDER BY guild_name
            """
        ).fetchall()

        return rows

    finally:
        conn.close()


def get_server(guild_id):
    """
    Return one configured server.

    Returns:

        (
            guild_id,
            guild_name,
            category_id,
            enabled
        )

    or None if the guild is not configured.
    """

    guild_id = str(guild_id)

    conn = get_connection()

    try:

        row = conn.execute(
            """
            SELECT
                guild_id,
                guild_name,
                category_id,
                enabled
            FROM servers
            WHERE guild_id = ?
            """,
            (guild_id,),
        ).fetchone()

        if row:

            logger.info(
                "Found server configuration: "
                "guild_id=%s guild_name=%s category_id=%s enabled=%s",
                row[0],
                row[1],
                row[2],
                row[3],
            )

        else:

            logger.warning(
                "No server configuration found for guild_id=%s",
                guild_id,
            )

        return row

    finally:
        conn.close()


# =========================================================
# TICKET MANAGEMENT
# =========================================================

def add_ticket(
    channel_id,
    guild_id,
    channel_name,
    user_id,
    user_name,
    opened_at,
):
    """
    Create a ticket.

    The user detected from permission overwrites is considered
    PROVISIONAL.

    Therefore:

        user_confirmed = 0

    The first real human MESSAGE_CREATE will later call
    update_ticket_user(), which changes this to:

        user_confirmed = 1
    """

    channel_id = str(channel_id)
    guild_id = str(guild_id)

    if user_id:
        user_id = str(user_id)

    user_name = user_name or "Unknown"

    conn = get_connection()

    try:

        conn.execute(
            """
            INSERT OR REPLACE INTO tickets
            (
                channel_id,
                guild_id,
                channel_name,
                user_id,
                user_name,
                opened_at,
                active,
                user_confirmed
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, 0)
            """,
            (
                channel_id,
                guild_id,
                channel_name,
                user_id,
                user_name,
                opened_at,
            ),
        )

        conn.commit()

        logger.info(
            "Ticket created: "
            "channel_id=%s guild_id=%s provisional_user=%s",
            channel_id,
            guild_id,
            user_id,
        )

    finally:
        conn.close()


def get_ticket(channel_id):
    """
    Return a ticket.

    Tuple layout:

        0 = channel_id
        1 = guild_id
        2 = channel_name
        3 = user_id
        4 = user_name
        5 = opened_at
        6 = active
        7 = user_confirmed
    """

    channel_id = str(channel_id)

    conn = get_connection()

    try:

        row = conn.execute(
            """
            SELECT
                channel_id,
                guild_id,
                channel_name,
                user_id,
                user_name,
                opened_at,
                active,
                user_confirmed
            FROM tickets
            WHERE channel_id = ?
            """,
            (channel_id,),
        ).fetchone()

        return row

    finally:
        conn.close()


def update_ticket_user(
    channel_id,
    user_id,
    user_name,
):
    """
    Permanently establish the ticket user.

    This function is intended to be called when the first
    human message arrives.

    Once this runs:

        user_confirmed = 1

    Later messages must NOT replace the ticket user.
    """

    channel_id = str(channel_id)

    if user_id:
        user_id = str(user_id)

    user_name = user_name or "Unknown"

    conn = get_connection()

    try:

        cursor = conn.execute(
            """
            UPDATE tickets
            SET
                user_id = ?,
                user_name = ?,
                user_confirmed = 1
            WHERE channel_id = ?
            AND active = 1
            AND user_confirmed = 0
            """,
            (
                user_id,
                user_name,
                channel_id,
            ),
        )

        conn.commit()

        if cursor.rowcount:

            logger.info(
                "Ticket user CONFIRMED: "
                "channel_id=%s user_id=%s user_name=%s",
                channel_id,
                user_id,
                user_name,
            )

            return True

        logger.info(
            "Ticket user was not changed: "
            "channel_id=%s "
            "possibly already confirmed or inactive.",
            channel_id,
        )

        return False

    finally:
        conn.close()


def close_ticket(channel_id):
    """
    Mark a ticket as closed.
    """

    channel_id = str(channel_id)

    conn = get_connection()

    try:

        conn.execute(
            """
            UPDATE tickets
            SET active = 0, closed_at = COALESCE(closed_at, ?)
            WHERE channel_id = ?
            """,
            (datetime.now(timezone.utc).isoformat(), channel_id),
        )

        conn.commit()

        logger.info(
            "Ticket closed: channel_id=%s",
            channel_id,
        )

    finally:
        conn.close()


# =========================================================
# DIAGNOSTICS
# =========================================================

def print_database_status():
    """
    Print configured servers and active tickets.

    Useful for diagnosing configuration problems.
    """

    conn = get_connection()

    try:

        servers = conn.execute(
            """
            SELECT
                guild_id,
                guild_name,
                category_id,
                enabled
            FROM servers
            ORDER BY guild_name
            """
        ).fetchall()

        tickets = conn.execute(
            """
            SELECT
                channel_id,
                guild_id,
                channel_name,
                user_id,
                user_name,
                active,
                user_confirmed
            FROM tickets
            ORDER BY opened_at DESC
            """
        ).fetchall()

    finally:
        conn.close()

    logger.info(
        "========== DATABASE STATUS =========="
    )

    logger.info(
        "Configured servers: %d",
        len(servers),
    )

    for server in servers:

        logger.info(
            "SERVER: guild_id=%s name=%s category_id=%s enabled=%s",
            server[0],
            server[1],
            server[2],
            server[3],
        )

    logger.info(
        "Tickets: %d",
        len(tickets),
    )

    for ticket in tickets:

        logger.info(
            "TICKET: channel_id=%s guild_id=%s "
            "name=%s user_id=%s user_name=%s "
            "active=%s confirmed=%s",
            ticket[0],
            ticket[1],
            ticket[2],
            ticket[3],
            ticket[4],
            ticket[5],
            ticket[6],
        )

    logger.info(
        "======================================"
    )
