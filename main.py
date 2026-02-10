#!/usr/bin/env python3
"""Circadian brightness ramp for Leviton Decora WiFi switches.

Gradually increases brightness on a logarithmic scale to simulate natural sunrise.
Runs as a persistent service that manages its own scheduling, with CLI subcommands
for overrides (skip, reschedule) and manual control.
"""

import argparse
import atexit
import datetime
import json
import math
import os
import signal
import sys
import threading
import time
from pathlib import Path

from decora_wifi import DecoraWiFiSession
from decora_wifi.models.residential_account import ResidentialAccount
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Constants & paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RAMP_TIME = os.environ.get("RAMP_TIME", "07:20")
OVERRIDE_FILE = Path(os.environ.get("OVERRIDE_FILE", SCRIPT_DIR / "overrides.json"))
STATE_FILE = SCRIPT_DIR / "state.json"
PID_FILE = SCRIPT_DIR / "service.pid"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def log(message: str) -> None:
    """Print timestamped log message to stdout."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


# ---------------------------------------------------------------------------
# Override helpers
# ---------------------------------------------------------------------------


def load_overrides() -> dict:
    """Load the overrides JSON file. Returns {} if missing or invalid."""
    try:
        return json.loads(OVERRIDE_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_overrides(overrides: dict) -> None:
    """Write overrides dict to the JSON file."""
    OVERRIDE_FILE.write_text(json.dumps(overrides, indent=2) + "\n")


def cleanup_overrides(overrides: dict) -> dict:
    """Remove entries for dates in the past. Returns cleaned dict."""
    today = datetime.date.today().isoformat()
    cleaned = {date: val for date, val in overrides.items() if date >= today}
    return cleaned


def parse_date_arg(value: str) -> str:
    """Parse a human-friendly date string into YYYY-MM-DD.

    Accepts: 'today', 'tomorrow', day names ('monday'..'sunday'), or 'YYYY-MM-DD'.
    """
    low = value.lower()
    today = datetime.date.today()

    if low == "today":
        return today.isoformat()
    if low == "tomorrow":
        return (today + datetime.timedelta(days=1)).isoformat()

    # Day name (monday..sunday) → next occurrence
    day_names = [
        "monday", "tuesday", "wednesday", "thursday",
        "friday", "saturday", "sunday",
    ]
    if low in day_names:
        target_weekday = day_names.index(low)
        days_ahead = (target_weekday - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7  # next week if today is that day
        return (today + datetime.timedelta(days=days_ahead)).isoformat()

    # Try YYYY-MM-DD
    try:
        datetime.date.fromisoformat(value)
        return value
    except ValueError:
        pass

    raise argparse.ArgumentTypeError(
        f"Invalid date: {value!r}. Use 'today', 'tomorrow', a day name, or YYYY-MM-DD."
    )


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------


def load_state() -> dict:
    """Load the state JSON file. Returns {} if missing or invalid."""
    try:
        return json.loads(STATE_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state: dict) -> None:
    """Write state dict to the JSON file."""
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n")


def already_ran_today() -> bool:
    """Check if the ramp already ran today."""
    state = load_state()
    return state.get("last_run") == datetime.date.today().isoformat()


def record_run() -> None:
    """Record that the ramp ran today."""
    state = load_state()
    state["last_run"] = datetime.date.today().isoformat()
    save_state(state)


# ---------------------------------------------------------------------------
# Interruptible sleep via SIGUSR1
# ---------------------------------------------------------------------------

_wake_event = threading.Event()


def _sigusr1_handler(signum, frame):
    """Signal handler: wake the service loop immediately."""
    log("Received SIGUSR1, waking up to re-evaluate schedule")
    _wake_event.set()


def interruptible_sleep(seconds: float) -> bool:
    """Sleep for up to *seconds*, returning early if SIGUSR1 is received.

    Returns True if interrupted, False if the full duration elapsed.
    """
    _wake_event.clear()
    interrupted = _wake_event.wait(timeout=seconds)
    return interrupted


# ---------------------------------------------------------------------------
# PID file management
# ---------------------------------------------------------------------------


def write_pid_file() -> None:
    """Write the current PID to the PID file."""
    PID_FILE.write_text(str(os.getpid()))
    atexit.register(remove_pid_file)


def remove_pid_file() -> None:
    """Remove the PID file if it exists."""
    try:
        PID_FILE.unlink()
    except FileNotFoundError:
        pass


def notify_service() -> None:
    """Send SIGUSR1 to the running service (if any) so it re-evaluates."""
    try:
        pid = int(PID_FILE.read_text().strip())
        os.kill(pid, signal.SIGUSR1)
        log(f"Notified running service (PID {pid})")
    except (FileNotFoundError, ValueError, ProcessLookupError, PermissionError):
        pass  # Service not running — override will be picked up on next start


# ---------------------------------------------------------------------------
# Leviton API helpers (unchanged from original)
# ---------------------------------------------------------------------------


def get_session_and_switches(email: str, password: str) -> tuple:
    """Authenticate and retrieve all switches."""
    log("Authenticating with Leviton cloud...")
    session = DecoraWiFiSession()
    session.login(email, password)

    log("Fetching residences and switches...")
    all_switches = []
    perms = session.user.get_residential_permissions()

    for perm in perms:
        account_id = perm.data.get("residentialAccountId")
        if not account_id:
            continue

        account = ResidentialAccount(session, account_id)
        residences = account.get_residences()

        for residence in residences:
            switches = residence.get_iot_switches()
            all_switches.extend(switches)

    log(f"Found {len(all_switches)} total switches")
    return session, all_switches


def filter_switches(switches: list, switch_names: list[str]) -> list:
    """Filter switches by name."""
    if not switch_names:
        return switches
    return [s for s in switches if s.name in switch_names]


def list_switches(switches: list) -> None:
    """Print all switches and their current state."""
    log("Listing all switches:")
    print()
    for switch in switches:
        power = getattr(switch, "power", "unknown")
        brightness = getattr(switch, "brightness", "N/A")
        print(f"  Name: {switch.name}")
        print(f"    Power: {power}")
        print(f"    Brightness: {brightness}%")
        print()


# ---------------------------------------------------------------------------
# Ramp logic (unchanged from original)
# ---------------------------------------------------------------------------


def calculate_brightness_times(total_seconds: float) -> list[tuple[float, int]]:
    """Calculate the exact time each brightness level should be reached."""
    times = []
    min_brightness = 1
    max_brightness = 100

    log_ratio = math.log(max_brightness / min_brightness)

    for brightness in range(min_brightness, max_brightness + 1):
        if brightness == min_brightness:
            t = 0
        else:
            t = total_seconds * math.log(brightness / min_brightness) / log_ratio
        times.append((t, brightness))

    return times


def run_ramp(switches: list, total_seconds: float) -> None:
    """Run the brightness ramp on specified switches."""
    if not switches:
        log("No switches to control!")
        return

    switch_names = ", ".join(s.name for s in switches)
    log(f"Starting brightness ramp on: {switch_names}")
    log(f"Duration: {total_seconds / 60:.1f} minutes")

    for switch in switches:
        power = getattr(switch, "power", "OFF")
        if power != "ON":
            log(f"Turning on {switch.name}")
            switch.update_attributes({"power": "ON"})

    brightness_schedule = calculate_brightness_times(total_seconds)

    log("Initial brightness: 1%")
    for switch in switches:
        switch.update_attributes({"brightness": 1})

    start_time = time.time()

    for target_time, brightness in brightness_schedule[1:]:
        now = time.time()
        elapsed = now - start_time
        sleep_duration = target_time - elapsed

        if sleep_duration > 0:
            time.sleep(sleep_duration)

        elapsed = time.time() - start_time
        log(f"Setting brightness to {brightness}% ({elapsed / 60:.1f} min elapsed)")
        for switch in switches:
            try:
                switch.update_attributes({"brightness": brightness})
            except Exception as e:
                log(f"Error updating {switch.name}: {e}")

    log("Brightness ramp complete!")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def load_config() -> tuple[str, str, list[str]]:
    """Load configuration from environment variables."""
    load_dotenv()

    email = os.environ.get("LEVITON_EMAIL")
    password = os.environ.get("LEVITON_PASSWORD")
    switch_names_str = os.environ.get("SWITCH_NAMES", "")

    if not email or not password:
        print("Error: LEVITON_EMAIL and LEVITON_PASSWORD must be set", file=sys.stderr)
        print("Set them in .env file or environment variables", file=sys.stderr)
        sys.exit(1)

    switch_names = [name.strip() for name in switch_names_str.split(",") if name.strip()]

    return email, password, switch_names


# ---------------------------------------------------------------------------
# Subcommand implementations
# ---------------------------------------------------------------------------


def _seconds_until(target_time_str: str) -> float:
    """Return seconds from now until the given HH:MM today. Negative if past."""
    now = datetime.datetime.now()
    h, m = map(int, target_time_str.split(":"))
    target = now.replace(hour=h, minute=m, second=0, microsecond=0)
    return (target - now).total_seconds()


def _seconds_until_midnight() -> float:
    """Return seconds from now until the next midnight."""
    now = datetime.datetime.now()
    midnight = (now + datetime.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return (midnight - now).total_seconds()


def _authenticate_and_get_switches(test: bool) -> tuple:
    """Authenticate and return (session, filtered_switches). Exits on failure."""
    email, password, switch_names = load_config()
    session, all_switches = get_session_and_switches(email, password)
    switches = filter_switches(all_switches, switch_names)

    if switch_names and len(switches) != len(switch_names):
        found_names = {s.name for s in switches}
        missing = set(switch_names) - found_names
        log(f"Warning: Could not find switches: {', '.join(missing)}")

    if not switches:
        log("No switches matched the configured SWITCH_NAMES")
        for s in all_switches:
            log(f"  - {s.name}")
        sys.exit(1)

    return session, switches


def cmd_service(args) -> None:
    """Run as a persistent service (systemd calls this)."""
    signal.signal(signal.SIGUSR1, _sigusr1_handler)
    write_pid_file()

    test_mode = args.test
    if test_mode:
        log("Service starting in TEST mode (2-minute ramps)")
    else:
        log("Service starting")

    while True:
        today_str = datetime.date.today().isoformat()
        overrides = load_overrides()

        # Already ran today?
        if already_ran_today():
            log(f"Already ran today ({today_str}), sleeping until midnight")
            secs = _seconds_until_midnight()
            interruptible_sleep(secs)
            continue

        # Check for skip override
        today_override = overrides.get(today_str, {})
        if today_override.get("action") == "skip":
            log(f"Skip override active for {today_str}")
            record_run()  # Mark as handled so we don't re-check
            secs = _seconds_until_midnight()
            interruptible_sleep(secs)
            continue

        # Determine target time
        if today_override.get("action") == "reschedule":
            target_time = today_override["time"]
            log(f"Rescheduled ramp time for today: {target_time}")
        else:
            target_time = DEFAULT_RAMP_TIME
            log(f"Default ramp time: {target_time}")

        # Wait until target time
        wait_secs = _seconds_until(target_time)
        if wait_secs > 0:
            log(f"Sleeping {wait_secs / 60:.0f} minutes until {target_time}")
            interrupted = interruptible_sleep(wait_secs)
            if interrupted:
                log("Sleep interrupted, re-evaluating schedule")
                continue  # Re-check overrides from the top

        # Re-check overrides (may have changed while sleeping)
        overrides = load_overrides()
        today_override = overrides.get(today_str, {})
        if today_override.get("action") == "skip":
            log(f"Skip override added while sleeping for {today_str}")
            record_run()
            secs = _seconds_until_midnight()
            interruptible_sleep(secs)
            continue

        if already_ran_today():
            secs = _seconds_until_midnight()
            interruptible_sleep(secs)
            continue

        # Run the ramp
        total_seconds = 2 * 60 if test_mode else 30 * 60
        try:
            session, switches = _authenticate_and_get_switches(test_mode)
            run_ramp(switches, total_seconds)
            record_run()
        except Exception as e:
            log(f"Ramp failed: {e}")
            # Don't record_run on failure — will retry next loop iteration
            # Sleep a bit before retrying to avoid tight error loops
            interruptible_sleep(60)
            continue

        # Clean up expired overrides
        overrides = load_overrides()
        cleaned = cleanup_overrides(overrides)
        if cleaned != overrides:
            save_overrides(cleaned)

        # Sleep until midnight
        secs = _seconds_until_midnight()
        log(f"Ramp complete, sleeping {secs / 60:.0f} minutes until midnight")
        interruptible_sleep(secs)


def cmd_run(args) -> None:
    """Run a single ramp immediately (manual trigger)."""
    total_seconds = 2 * 60 if args.test else 30 * 60
    try:
        session, switches = _authenticate_and_get_switches(args.test)
        run_ramp(switches, total_seconds)
    except Exception as e:
        log(f"Error: {e}")
        sys.exit(1)


def cmd_skip(args) -> None:
    """Add a skip override for a date."""
    date_str = parse_date_arg(args.date)
    overrides = load_overrides()
    overrides[date_str] = {"action": "skip"}
    save_overrides(overrides)
    log(f"Skipping ramp on {date_str}")
    notify_service()


def cmd_reschedule(args) -> None:
    """Change ramp time for a date."""
    date_str = parse_date_arg(args.date)
    # Validate time format
    try:
        h, m = map(int, args.time.split(":"))
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError
        time_str = f"{h:02d}:{m:02d}"
    except (ValueError, AttributeError):
        print(f"Error: Invalid time format: {args.time!r}. Use HH:MM.", file=sys.stderr)
        sys.exit(1)

    overrides = load_overrides()
    overrides[date_str] = {"action": "reschedule", "time": time_str}
    save_overrides(overrides)
    log(f"Rescheduled ramp on {date_str} to {time_str}")
    notify_service()


def cmd_status(args) -> None:
    """Show active overrides and next scheduled ramp."""
    overrides = load_overrides()
    state = load_state()

    print("Circadian Ramp Status")
    print("=" * 40)
    print(f"Default ramp time: {DEFAULT_RAMP_TIME}")
    print(f"Last run: {state.get('last_run', 'never')}")

    today = datetime.date.today()
    today_str = today.isoformat()

    # Determine next ramp
    today_override = overrides.get(today_str, {})
    if already_ran_today():
        next_date = today + datetime.timedelta(days=1)
    elif today_override.get("action") == "skip":
        next_date = today + datetime.timedelta(days=1)
    else:
        next_date = today

    next_str = next_date.isoformat()
    next_override = overrides.get(next_str, {})
    if next_override.get("action") == "skip":
        print(f"Next ramp: {next_str} — SKIPPED")
    elif next_override.get("action") == "reschedule":
        print(f"Next ramp: {next_str} at {next_override['time']}")
    else:
        print(f"Next ramp: {next_str} at {DEFAULT_RAMP_TIME}")

    # Show active overrides
    active = {d: v for d, v in overrides.items() if d >= today_str}
    if active:
        print()
        print("Active overrides:")
        for date in sorted(active):
            entry = active[date]
            if entry.get("action") == "skip":
                print(f"  {date}: skip")
            elif entry.get("action") == "reschedule":
                print(f"  {date}: reschedule to {entry['time']}")
    else:
        print()
        print("No active overrides")

    # Service status
    print()
    try:
        pid = int(PID_FILE.read_text().strip())
        os.kill(pid, 0)  # Check if process exists
        print(f"Service: running (PID {pid})")
    except (FileNotFoundError, ValueError, ProcessLookupError, PermissionError):
        print("Service: not running")


def cmd_clear(args) -> None:
    """Remove override(s)."""
    overrides = load_overrides()

    if args.date:
        date_str = parse_date_arg(args.date)
        if date_str in overrides:
            del overrides[date_str]
            save_overrides(overrides)
            log(f"Cleared override for {date_str}")
        else:
            log(f"No override found for {date_str}")
    else:
        save_overrides({})
        log("Cleared all overrides")

    notify_service()


def cmd_list(args) -> None:
    """List all switches."""
    email, password, switch_names = load_config()
    try:
        session, all_switches = get_session_and_switches(email, password)
        list_switches(all_switches)
    except Exception as e:
        log(f"Error: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def main() -> None:
    """Main entry point with subcommand dispatch."""

    # Backward-compatibility shim: if called with old-style --list or --test
    # (no subcommand), translate to the new subcommands.
    if len(sys.argv) > 1 and sys.argv[1].startswith("--"):
        if "--list" in sys.argv:
            sys.argv = [sys.argv[0], "list"]
        elif "--test" in sys.argv:
            sys.argv = [sys.argv[0], "run", "--test"]

    parser = argparse.ArgumentParser(
        description="Circadian brightness ramp for Leviton Decora WiFi switches"
    )
    subparsers = parser.add_subparsers(dest="command")

    # service
    sp_service = subparsers.add_parser(
        "service", help="Run as persistent service (systemd calls this)"
    )
    sp_service.add_argument(
        "--test", action="store_true",
        help="Use 2-minute test ramps instead of 30-minute ramps",
    )

    # run
    sp_run = subparsers.add_parser("run", help="Run a single ramp immediately")
    sp_run.add_argument(
        "--test", action="store_true",
        help="Use a 2-minute test ramp instead of 30 minutes",
    )

    # skip
    sp_skip = subparsers.add_parser("skip", help="Skip ramp for a date")
    sp_skip.add_argument(
        "date",
        help="Date to skip: 'today', 'tomorrow', day name, or YYYY-MM-DD",
    )

    # reschedule
    sp_resched = subparsers.add_parser(
        "reschedule", help="Change ramp time for a date"
    )
    sp_resched.add_argument(
        "date",
        help="Date to reschedule: 'today', 'tomorrow', day name, or YYYY-MM-DD",
    )
    sp_resched.add_argument(
        "time",
        help="New ramp time in HH:MM format",
    )

    # status
    subparsers.add_parser("status", help="Show active overrides and next scheduled ramp")

    # clear
    sp_clear = subparsers.add_parser("clear", help="Remove override(s)")
    sp_clear.add_argument(
        "date", nargs="?", default=None,
        help="Date to clear (omit to clear all)",
    )

    # list
    subparsers.add_parser("list", help="List all switches")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    dispatch = {
        "service": cmd_service,
        "run": cmd_run,
        "skip": cmd_skip,
        "reschedule": cmd_reschedule,
        "status": cmd_status,
        "clear": cmd_clear,
        "list": cmd_list,
    }

    dispatch[args.command](args)


if __name__ == "__main__":
    main()
