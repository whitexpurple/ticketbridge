TicketBridge

## Ubuntu VPS

Run the bot as a systemd service to keep it running after SSH disconnects and
start it automatically after a reboot. Stop any manually started `python bot.py`
instance first so only one process polls Telegram.

From the project directory on the VPS, as the Linux account that owns the project:

```bash
sudo apt update
sudo apt install -y python3-venv
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
sudo python3 install_service.py
```

Keep `.env` in that project directory. Do not copy a Windows `.venv` to Ubuntu;
create the environment on the VPS. The installer uses the current project path
and the account invoking sudo. Specify `--user USERNAME` to select another account
with read/write access to the project, `.env`, and database. Running the installer
directly as root without `--user` runs the service as root.

The service reuses the saved Discord token, restarts after exits, and writes logs
to the system journal. A running service does not itself guarantee that Discord
authentication is valid; check the logs and Telegram `/status`.

```bash
sudo systemctl status ticketbridge --no-pager
sudo journalctl -u ticketbridge -f
sudo systemctl restart ticketbridge
sudo systemctl stop ticketbridge
```

Once service status shows it running, you can disconnect from SSH. Pressing Ctrl+C
while following journal logs only exits the log viewer. Rerunning the installer
updates and restarts the same service.

## Monitoring and Recovery

The first monitoring cutoff is saved per Discord account. Restarting or
reconnecting keeps that cutoff and resumes active tracked tickets. Tickets
created during a disconnection can be discovered when they are still accessible.
Previously untracked tickets older than the cutoff are ignored. On the first
upgrade, active tickets already recorded in the database are adopted for continued
monitoring without repeating their opening notifications.

Relevant gateway events are saved in SQLite before processing. Telegram
notifications are also queued in SQLite. Ticket opens/closes and their queued
notifications are committed together. Failed sends retry with backoff and honor
Telegram rate limits. Long messages are split, and repeated Discord message IDs
do not produce repeated notifications. Delivery is at least once: an ambiguous
network failure or a crash after Telegram accepts a send but before the local
acknowledgement can still cause a duplicate. Delivered message bodies are cleared
locally; pending messages remain until delivered.

Keep `data/bot.db` when deploying updates: it holds the cutoff, tracked tickets,
event backlog, and notification queue. Schema upgrades run automatically on
startup without deleting tickets. Do not run two bot instances against it.

Gateway events provide immediate detection. New channel/thread events are checked
and their opening history is requested immediately. REST recovery scans rotate
through up to 16 servers per cycle, prioritizing up to four servers with recent
ticket activity while preserving a saved round-robin cursor for the rest. Cycles
wait 30 seconds after each scan and can take longer when Discord REST calls are
slow or rate limited.
Each scan is limited to five opening-history probes and five tracked-ticket
recovery checks. REST requests using the account session are spaced by at least
500 ms; Discord 429 retry delays and global cooldowns are honored before retrying.
These scans recover accessible channels, thread metadata, and missed messages
using stored per-channel cursors. Bot-token user lookup is cached, including a
short cooldown after failed lookups. A missing channel or failed REST request is
not treated as proof that a ticket closed.

Detection covers MEE6-style private text channels (including role-based access)
and Mava-style private threads nested inside existing channels. It recognizes
username-only private threads under ticket-panel names such as `support_tickets`,
`support-tickets`, and `open-ticket`, without needing a visible opening message.
Missing parent metadata is fetched with a cooldown on failed lookups. This rule
does not classify public threads or private threads under generic `support`,
log, or community channels. It is a layout heuristic, not verified app identity.
It also recognizes
MEE6-style numbered names such as `425-sampleuser` without requiring an opening
message when the text channel denies everyone access, or grants member-specific
access under a support category. This is a naming heuristic, not proof of the
creator app. Public numbered channels do not qualify through this rule.
It also recognizes
ticket names, topics, ticket-opening app messages, and explicit close/claim-ticket
controls, including colon-separated control IDs. Privacy alone is not enough to
classify an ordinary chat as a ticket, and channel shape does not prove which app
created it. New channel/thread events trigger a targeted opening-history check
and wake the scanner. Discovery reads the first 25 messages after channel creation
instead of only recent messages, so later conversation cannot displace the greeting.
INFO logs report live creation IDs and the first inconclusive or unavailable
history check without printing message contents. Unsuccessful history
checks with a valid response get three follow-ups after 15, 30, and 60 seconds
before returning to the ten-minute per-channel cooldown. Actual timing depends
on scan duration and the five-history-probes-per-server budget. Failed REST
requests retain the ordinary cooldown. Existing live-only cutoff rules remain.
Close notifications recognize
deletion, a `closed-` rename, locked archival, and explicit app-authored ticket
closure messages. Mere inactivity archival does not imply closure. Mava/MEE6
custom formats are not live-verified; inaccessible threads and unrecognized
formats can still be missed. The incorrect total-server "Mutuals" field is removed.

Telegram `/status` reports gateway health, active tracked tickets, scan times,
pending events, queued notifications, and delivery errors. `/reconnect` reuses
the saved token; it does not request your password again.
Ticket open, close, and message notifications format detail values for easier
copying and include an inline **Open Server** link to the owning Discord server.
At midnight each day, TicketBridge queues a daily ticket report to Telegram.
It lists only servers with ticket openings or closures in the reporting period.
`/report` sends the current day's report so far. Set
`DAILY_REPORT_TIMEZONE=Etc/GMT-1` in `.env` to schedule it at midnight fixed
UTC+1; this is also the default. The active timezone and next scheduled midnight
are logged at startup. Daily close counts begin with
closures recorded after this update, because older tickets have no close time.
The reporting date is saved with the notification queue to avoid duplicate
reports after restarts. A fresh installation starts at the next midnight. If the
service is down at a scheduled midnight, it sends missed reports when it starts
again.
The daily report only lists servers with ticket openings or closures during
that reporting period, with per-server opened and closed totals. Discovery
failure causes are retained for troubleshooting and quiet health alerts
wait five minutes before reporting a sustained Discord outage or Telegram
delivery backlog. Ticket discovery alerts after its last successful scan has
been stale for at least ten minutes (or five configured scan intervals), plus
the five-minute alert confirmation period. Each incident produces one alert and
one recovery notice through the durable notification queue.
Guild endpoints that return HTTP 403 are marked inaccessible for 24 hours and
then retried; these expected access denials do not make scans unhealthy.
`/servers` lists all servers in the connected account's cached server list, with
the total count, server IDs, and Previous/Next buttons for lists over 15 servers.
The same list is available from **My servers** in `/start`.
Every server list includes a **Close** button and automatically closes 15 minutes
after opening. Page changes do not extend the timer. Deadlines survive restarts;
overdue lists close when the bot is back online. If deletion is unavailable, the
bot replaces the list with a closed notice. Temporary Telegram failures retry.

After uploading updated source files to the VPS:

```bash
sudo systemctl restart ticketbridge
sudo journalctl -u ticketbridge -n 50 --no-pager
```

Offline tests: `python -m unittest -q test_reliability test_ticketbridge test_install_service`.
