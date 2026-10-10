"""Install TicketBridge as an Ubuntu systemd service; run with sudo python3."""

import argparse
import os
from pathlib import Path
import platform
import subprocess
import tempfile


def unit_value(value):
    if any(character in value for character in "\n\r\0"):
        raise ValueError("Service paths and usernames must not contain control characters.")
    return value.replace("%", "%%")


def unit_quote(value):
    return '"' + unit_value(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def build_unit(project, user):
    executable = str(project / ".venv/bin/python")
    return f"""[Unit]
Description=TicketBridge Discord to Telegram
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
User={unit_value(user)}
WorkingDirectory={unit_value(str(project))}
ExecStart={unit_quote(executable).replace('$', '$$')} -u {unit_quote(str(project / 'bot.py')).replace('$', '$$')}
Restart=always
RestartSec=15
KillSignal=SIGINT
TimeoutStopSec=90
Environment=PYTHONUNBUFFERED=1
UMask=0077
NoNewPrivileges=true
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ticketbridge

[Install]
WantedBy=multi-user.target
"""


def install_unit(unit, target):
    # Stage on the same filesystem so a validated replacement can be atomic.
    with tempfile.TemporaryDirectory(prefix=".ticketbridge-", dir=target.parent) as directory:
        staged = Path(directory) / target.name
        staged.write_text(unit, encoding="utf-8")
        staged.chmod(0o644)
        subprocess.run(["systemd-analyze", "verify", str(staged)], check=True)
        os.replace(staged, target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user", help="Linux account that owns and runs the bot (defaults to sudo caller).")
    parser.add_argument("--dry-run", action="store_true", help="Print the service without installing it.")
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("Run this installer on the Ubuntu VPS, not on Windows.")
    import pwd
    project = Path(__file__).resolve().parent
    user = args.user or os.environ.get("SUDO_USER") or pwd.getpwuid(os.getuid()).pw_name
    try:
        pwd.getpwnam(user)
    except KeyError:
        parser.error("The requested Linux service account does not exist.")
    python = project / ".venv/bin/python"
    if not python.is_file():
        parser.error("Create a Linux environment first: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt")
    if not (project / ".env").is_file():
        parser.error("Create the project's .env on the VPS before installing the service.")
    unit = build_unit(project, user)
    if args.dry_run:
        print(unit)
        return
    if os.geteuid() != 0:
        parser.error("Run with sudo: sudo python3 install_service.py")
    # Check permissions and dependencies as the service user without importing bot.py.
    check = (
        "import os, pathlib, telegram, websockets, dotenv, httpx; "
        "p=pathlib.Path.cwd(); "
        "assert os.access(p, os.W_OK), 'Project directory must be writable'; "
        "assert os.access(p/'.env', os.R_OK | os.W_OK), '.env must be readable and writable'; "
        "d=p/'data'; assert not d.exists() or os.access(d, os.R_OK | os.W_OK | os.X_OK), 'data must be writable'; "
        "db=d/'bot.db'; assert not db.exists() or os.access(db, os.R_OK | os.W_OK), 'Database must be writable'"
    )
    subprocess.run(["runuser", "-u", user, "--", str(python), "-c", check], cwd=project, check=True)
    target = Path("/etc/systemd/system/ticketbridge.service")
    install_unit(unit, target)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", "ticketbridge.service"], check=True)
    subprocess.run(["systemctl", "restart", "ticketbridge.service"], check=True)
    print("TicketBridge installed and enabled at boot.")
    print("Status: sudo systemctl status ticketbridge --no-pager")
    print("Logs:   sudo journalctl -u ticketbridge -f")


if __name__ == "__main__":
    main()
