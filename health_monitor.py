"""Send one alert for sustained health issues and one when they recover."""

import asyncio
import logging
import time

from database import get_connection

logger = logging.getLogger("health-monitor")


class HealthMonitor:
    def __init__(self, session, outbox, interval=60, grace_period=300):
        self.session = session
        self.outbox = outbox
        self.interval = max(15, interval)
        self.grace_period = max(60, grace_period)

    def current_issues(self):
        issues = {}
        sessions = (list(self.session.sessions.values()) if hasattr(self.session, "sessions")
                    else [self.session])
        disconnected = []
        scan_issues = []
        for session in sessions:
            client = session.client
            if client is not None and (not client.ready.is_set() or session.connection_error):
                disconnected.append(str((client.user or {}).get("username", "Discord account")))
            discovery = session.discovery
            if not discovery or client is None or not client.ready.is_set():
                continue
            now = time.time()
            threshold = max(600, discovery.interval * 5)
            last_success = discovery.last_successful_scan
            scan_anchor = last_success or discovery.started_at
            if ((last_success and now - last_success > threshold)
                    or (discovery.last_scan_error and now - scan_anchor > threshold)):
                detail = f" Last scan detail: {discovery.last_scan_error}." if discovery.last_scan_error else ""
                scan_issues.append(f"{(client.user or {}).get('username', 'Discord account')}: {detail.strip()}")
            elif discovery.scan_running and discovery.scan_started_at and now - discovery.scan_started_at > threshold:
                scan_issues.append(f"{(client.user or {}).get('username', 'Discord account')}: scan running unusually long")
        if disconnected:
            issues["discord_gateway"] = "Discord gateway disconnected for: " + ", ".join(disconnected)
        if scan_issues:
            issues["ticket_discovery"] = "No healthy discovery scan recently for: " + "; ".join(scan_issues)

        try:
            age = self.outbox.pending_age()
            if age >= self.grace_period:
                issues["telegram_delivery"] = (
                    f"Telegram has {self.outbox.pending_count()} notification(s) queued; "
                    f"the oldest has been waiting {int(age // 60)} minutes."
                )
        except Exception:
            logger.exception("Could not inspect Telegram notification queue.")
        return issues

    def update_issue(self, issue_key, description):
        now = time.time()
        conn = get_connection()
        try:
            with conn:
                state = conn.execute("SELECT first_seen,alerted FROM health_alert_state WHERE issue_key=?",
                                     (issue_key,)).fetchone()
                if description:
                    if state is None:
                        conn.execute("INSERT INTO health_alert_state(issue_key,first_seen,alerted) VALUES (?,?,0)",
                                     (issue_key, now))
                        return False
                    first_seen, alerted = state
                    if not alerted and now - first_seen >= self.grace_period:
                        incident_id = int(first_seen * 1000)
                        self.outbox.insert(conn,
                            f"⚠️ TicketBridge health alert\n{description}\n\n" 
                            f"This condition persisted for at least {self.grace_period // 60} minutes.",
                            f"health:{issue_key}:{incident_id}")
                        conn.execute("UPDATE health_alert_state SET alerted=1 WHERE issue_key=?", (issue_key,))
                        return True
                    return False

                if state is None:
                    return False
                first_seen, alerted = state
                if alerted:
                    self.outbox.insert(conn,
                        f"✅ TicketBridge recovered\n{issue_key.replace('_', ' ').title()} is healthy again.",
                        f"health-recovered:{issue_key}:{int(first_seen * 1000)}")
                conn.execute("DELETE FROM health_alert_state WHERE issue_key=?", (issue_key,))
                return bool(alerted)
        finally:
            conn.close()

    def check_once(self):
        issues = self.current_issues()
        conn = get_connection()
        try:
            previous = {row[0] for row in conn.execute("SELECT issue_key FROM health_alert_state")}
        finally:
            conn.close()

        queued = False
        for key, description in issues.items():
            queued = self.update_issue(key, description) or queued
        for key in previous - issues.keys():
            queued = self.update_issue(key, None) or queued
        if queued:
            self.outbox.wake.set()
        return issues

    async def run(self):
        while True:
            try:
                self.check_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Health monitor check failed; it will retry.")
            await asyncio.sleep(self.interval)
