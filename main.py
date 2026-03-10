#!/usr/bin/env python3
"""Circadian brightness ramp for Matter-enabled dimmers.

Gradually increases brightness on a logarithmic scale to simulate natural sunrise.
Runs as a persistent service that manages its own scheduling, with CLI subcommands
for overrides (skip, reschedule) and manual control.

Communicates with dimmers via a local python-matter-server instance over WebSocket.
"""

import argparse
import asyncio
import datetime
import json
import math
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from matter_server.client import MatterClient
from chip.clusters import Objects as clusters

# ---------------------------------------------------------------------------
# Constants & paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
STATE_FILE = SCRIPT_DIR / "state.json"
SERVICE_NAME = "circadian-ramp.service"
DEFAULT_MATTER_URL = "ws://localhost:5580/ws"
DEFAULT_RAMP_TIME_FALLBACK = "07:20"
MATTER_CONNECT_TIMEOUT = 30  # seconds

# These are initialized after load_dotenv() by _init_env_config().
DEFAULT_RAMP_TIME = DEFAULT_RAMP_TIME_FALLBACK
OVERRIDE_FILE = SCRIPT_DIR / "overrides.json"


def _init_env_config() -> None:
    """Read env-dependent constants. Call after load_dotenv()."""
    global DEFAULT_RAMP_TIME, OVERRIDE_FILE
    DEFAULT_RAMP_TIME = os.environ.get("RAMP_TIME", DEFAULT_RAMP_TIME_FALLBACK)
    OVERRIDE_FILE = Path(os.environ.get("OVERRIDE_FILE", SCRIPT_DIR / "overrides.json"))

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
# Interruptible sleep via SIGUSR1 (async)
# ---------------------------------------------------------------------------

_wake_event = asyncio.Event()


async def interruptible_sleep(seconds: float) -> bool:
    """Sleep for up to *seconds*, returning early if SIGUSR1 is received.

    Returns True if interrupted, False if the full duration elapsed.
    """
    _wake_event.clear()
    try:
        await asyncio.wait_for(_wake_event.wait(), timeout=seconds)
        return True  # interrupted
    except asyncio.TimeoutError:
        return False  # full duration elapsed


# ---------------------------------------------------------------------------
# Service notification via systemctl reload
# ---------------------------------------------------------------------------


def notify_service() -> None:
    """Ask systemd to reload the service, which sends SIGUSR1 via ExecReload."""
    try:
        subprocess.run(
            ["systemctl", "reload", SERVICE_NAME],
            capture_output=True, timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass  # systemctl not available or timed out — override saved for next wake


# ---------------------------------------------------------------------------
# Matter brightness helpers
# ---------------------------------------------------------------------------


def pct_to_matter_level(pct: int) -> int:
    """Convert 0-100% brightness to Matter 0-254 level."""
    return max(0, min(254, round(pct * 254 / 100)))


def matter_level_to_pct(level: int) -> int:
    """Convert Matter 0-254 level to 0-100%."""
    return max(0, min(100, round(level * 100 / 254)))


# ---------------------------------------------------------------------------
# Matter connection helpers
# ---------------------------------------------------------------------------


async def matter_connect(url: str) -> tuple[MatterClient, aiohttp.ClientSession, asyncio.Task]:
    """Create, connect, and start listening on a MatterClient.

    Returns (client, aiohttp_session, listen_task).
    ``start_listening`` runs as a background task; the function waits for the
    initial node data before returning.
    """
    ws_session = aiohttp.ClientSession()
    client = MatterClient(url, ws_session)

    init_ready = asyncio.Event()

    async def _listen() -> None:
        await client.start_listening(init_ready=init_ready)

    listen_task = asyncio.create_task(_listen())

    try:
        await asyncio.wait_for(init_ready.wait(), timeout=MATTER_CONNECT_TIMEOUT)
    except asyncio.TimeoutError:
        listen_task.cancel()
        await ws_session.close()
        raise ConnectionError(
            f"Timed out after {MATTER_CONNECT_TIMEOUT}s waiting for matter-server at {url}"
        )

    return client, ws_session, listen_task


async def matter_disconnect(
    client: MatterClient,
    ws_session: aiohttp.ClientSession,
    listen_task: asyncio.Task,
) -> None:
    """Cleanly disconnect from matter-server."""
    try:
        await client.disconnect()
    except Exception:
        pass
    listen_task.cancel()
    try:
        await listen_task
    except (asyncio.CancelledError, Exception):
        pass
    await ws_session.close()


# ---------------------------------------------------------------------------
# Ramp logic
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


MAX_CONSECUTIVE_FAILURES = 5


async def _send_command(client: MatterClient, node_id: int, command) -> None:
    """Send a device command to endpoint 1 of a node."""
    await client.send_device_command(node_id, endpoint_id=1, command=command)


async def run_ramp(
    client: MatterClient,
    node_ids: list[int],
    total_seconds: float,
    matter_url: "str | None" = None,
    ws_session_ref: "list | None" = None,
    listen_task_ref: "list | None" = None,
) -> MatterClient:
    """Run the brightness ramp on specified Matter nodes.

    If *matter_url* is provided, the ramp will attempt to reconnect once on
    connection failure.  Pass mutable single-element lists for *ws_session_ref*
    and *listen_task_ref* so the caller can clean up the potentially-new
    session/task after reconnection.

    Returns the (possibly new) client.
    """
    if not node_ids:
        log("No nodes to control!")
        return client

    log(f"Starting brightness ramp on node(s): {', '.join(str(n) for n in node_ids)}")
    log(f"Duration: {total_seconds / 60:.1f} minutes")

    # Turn on and set initial brightness
    for node_id in node_ids:
        try:
            await _send_command(client, node_id, clusters.OnOff.Commands.On())
        except Exception as e:
            log(f"Error turning on node {node_id}: {e}")

    brightness_schedule = calculate_brightness_times(total_seconds)

    log("Initial brightness: 1%")
    for node_id in node_ids:
        try:
            await _send_command(
                client, node_id,
                clusters.LevelControl.Commands.MoveToLevelWithOnOff(
                    level=pct_to_matter_level(1),
                    transitionTime=0, optionsMask=0, optionsOverride=0,
                ),
            )
        except Exception as e:
            log(f"Error setting brightness on node {node_id}: {e}")

    start_time = time.time()
    consecutive_failures = 0

    for target_time, brightness in brightness_schedule[1:]:
        now = time.time()
        elapsed = now - start_time
        sleep_duration = target_time - elapsed

        if sleep_duration > 0:
            await asyncio.sleep(sleep_duration)

        elapsed = time.time() - start_time
        log(f"Setting brightness to {brightness}% ({elapsed / 60:.1f} min elapsed)")

        step_failed = False
        for node_id in node_ids:
            try:
                await _send_command(
                    client, node_id,
                    clusters.LevelControl.Commands.MoveToLevelWithOnOff(
                        level=pct_to_matter_level(brightness),
                        transitionTime=0, optionsMask=0, optionsOverride=0,
                    ),
                )
            except Exception as e:
                log(f"Error updating node {node_id}: {e}")
                step_failed = True

        if step_failed:
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                # Try reconnecting once if we have a URL
                if matter_url and ws_session_ref is not None and listen_task_ref is not None:
                    log("Too many failures, attempting reconnection...")
                    try:
                        await matter_disconnect(client, ws_session_ref[0], listen_task_ref[0])
                    except Exception:
                        pass
                    try:
                        client, new_ws, new_task = await matter_connect(matter_url)
                        ws_session_ref[0] = new_ws
                        listen_task_ref[0] = new_task
                        consecutive_failures = 0
                        log("Reconnected successfully, resuming ramp")
                        continue
                    except Exception as re:
                        log(f"Reconnection failed: {re}, aborting ramp")
                        raise
                else:
                    log(f"Aborting ramp after {MAX_CONSECUTIVE_FAILURES} consecutive failures")
                    raise ConnectionError("Too many consecutive command failures")
        else:
            consecutive_failures = 0

    log("Brightness ramp complete!")
    return client


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _get_matter_url() -> str:
    """Return the configured matter-server URL (call after load_dotenv)."""
    return os.environ.get("MATTER_SERVER_URL", DEFAULT_MATTER_URL)


def load_config() -> tuple[str, list[int]]:
    """Load configuration from environment variables.

    Returns (matter_url, node_ids).
    """
    matter_url = _get_matter_url()
    node_ids_str = os.environ.get("NODE_IDS", "")

    node_ids = []
    for part in node_ids_str.split(","):
        part = part.strip()
        if part:
            try:
                node_ids.append(int(part))
            except ValueError:
                print(f"Error: Invalid node ID: {part!r}. Must be an integer.", file=sys.stderr)
                sys.exit(1)

    if not node_ids:
        print("Error: NODE_IDS must be set (comma-separated list of Matter node IDs)", file=sys.stderr)
        print("Set it in .env file or environment variables", file=sys.stderr)
        print("Use 'python main.py list' to see commissioned nodes", file=sys.stderr)
        sys.exit(1)

    return matter_url, node_ids


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


async def _connect_and_validate(
    matter_url: str, node_ids: list[int],
) -> tuple[MatterClient, aiohttp.ClientSession, asyncio.Task]:
    """Connect to matter-server and validate that configured node IDs exist."""
    client, ws_session, listen_task = await matter_connect(matter_url)

    nodes = client.get_nodes()
    available_ids = {node.node_id for node in nodes}

    missing = set(node_ids) - available_ids
    if missing:
        log(f"Warning: Node ID(s) not found on matter-server: {', '.join(str(n) for n in sorted(missing))}")
        log(f"Available nodes: {', '.join(str(n) for n in sorted(available_ids))}")
        await matter_disconnect(client, ws_session, listen_task)
        raise RuntimeError(f"Missing node(s): {missing}")

    return client, ws_session, listen_task


async def cmd_service(args) -> None:
    """Run as a persistent service (systemd calls this)."""
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGUSR1, lambda: (
        _wake_event.set(),
        log("Received SIGUSR1, waking up to re-evaluate schedule"),
    ))

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
            await interruptible_sleep(secs)
            continue

        # Check for skip override
        today_override = overrides.get(today_str, {})
        if today_override.get("action") == "skip":
            log(f"Skip override active for {today_str}")
            record_run()  # Mark as handled so we don't re-check
            secs = _seconds_until_midnight()
            await interruptible_sleep(secs)
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
            interrupted = await interruptible_sleep(wait_secs)
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
            await interruptible_sleep(secs)
            continue

        if already_ran_today():
            secs = _seconds_until_midnight()
            await interruptible_sleep(secs)
            continue

        # Run the ramp
        total_seconds = 2 * 60 if test_mode else 30 * 60
        matter_url, node_ids = load_config()
        client = None
        ws_session = None
        listen_task = None
        try:
            client, ws_session, listen_task = await _connect_and_validate(matter_url, node_ids)
            ws_ref = [ws_session]
            task_ref = [listen_task]
            client = await run_ramp(
                client, node_ids, total_seconds,
                matter_url=matter_url,
                ws_session_ref=ws_ref,
                listen_task_ref=task_ref,
            )
            ws_session = ws_ref[0]
            listen_task = task_ref[0]
            record_run()
        except Exception as e:
            log(f"Ramp failed: {e}")
            # Don't record_run on failure — will retry next loop iteration
            # Sleep a bit before retrying to avoid tight error loops
            await interruptible_sleep(60)
            continue
        finally:
            if client and ws_session and listen_task:
                await matter_disconnect(client, ws_session, listen_task)

        # Clean up expired overrides
        overrides = load_overrides()
        cleaned = cleanup_overrides(overrides)
        if cleaned != overrides:
            save_overrides(cleaned)

        # Sleep until midnight
        secs = _seconds_until_midnight()
        log(f"Ramp complete, sleeping {secs / 60:.0f} minutes until midnight")
        await interruptible_sleep(secs)


async def cmd_run(args) -> None:
    """Run a single ramp immediately (manual trigger)."""
    total_seconds = 2 * 60 if args.test else 30 * 60
    matter_url, node_ids = load_config()
    client = None
    ws_session = None
    listen_task = None
    try:
        client, ws_session, listen_task = await _connect_and_validate(matter_url, node_ids)
        ws_ref = [ws_session]
        task_ref = [listen_task]
        client = await run_ramp(
            client, node_ids, total_seconds,
            matter_url=matter_url,
            ws_session_ref=ws_ref,
            listen_task_ref=task_ref,
        )
        ws_session = ws_ref[0]
        listen_task = task_ref[0]
    except Exception as e:
        log(f"Error: {e}")
        sys.exit(1)
    finally:
        if client and ws_session and listen_task:
            await matter_disconnect(client, ws_session, listen_task)


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


async def cmd_status(args) -> None:
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
        result = subprocess.run(
            ["systemctl", "is-active", "--quiet", SERVICE_NAME],
            capture_output=True, timeout=5,
        )
        if result.returncode == 0:
            print("Service: running")
        else:
            print("Service: not running")
    except (FileNotFoundError, subprocess.TimeoutExpired):
        print("Service: unknown (systemctl not available)")

    # Matter server connection check
    matter_url = _get_matter_url()
    print()
    try:
        client, ws_session, listen_task = await matter_connect(matter_url)
        nodes = client.get_nodes()
        print(f"Matter server: connected ({len(nodes)} node(s))")
        await matter_disconnect(client, ws_session, listen_task)
    except Exception as e:
        print(f"Matter server: not reachable ({e})")


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


async def cmd_list(args) -> None:
    """List all commissioned Matter nodes."""
    matter_url = _get_matter_url()
    client = None
    ws_session = None
    listen_task = None
    try:
        client, ws_session, listen_task = await matter_connect(matter_url)
        nodes = client.get_nodes()

        if not nodes:
            print("No commissioned nodes found.")
        else:
            print(f"Commissioned nodes ({len(nodes)}):")
            print()
            for node in nodes:
                node_id = node.node_id
                # Try to get device name from Basic Information cluster
                name = "Unknown"
                try:
                    basic_info = node.get_attribute_value(
                        clusters.BasicInformation.Attributes.NodeLabel
                    )
                    if basic_info:
                        name = basic_info
                except Exception:
                    pass

                # Try to get on/off state
                on_off = "unknown"
                try:
                    on_off_val = node.get_attribute_value(
                        clusters.OnOff.Attributes.OnOff
                    )
                    on_off = "ON" if on_off_val else "OFF"
                except Exception:
                    pass

                # Try to get brightness level
                brightness = "N/A"
                try:
                    level = node.get_attribute_value(
                        clusters.LevelControl.Attributes.CurrentLevel
                    )
                    if level is not None:
                        brightness = f"{matter_level_to_pct(level)}%"
                except Exception:
                    pass

                print(f"  Node {node_id}: {name}")
                print(f"    Power: {on_off}")
                print(f"    Brightness: {brightness}")
                print()
    except Exception as e:
        log(f"Error: {e}")
        sys.exit(1)
    finally:
        if client and ws_session and listen_task:
            await matter_disconnect(client, ws_session, listen_task)


async def cmd_commission(args) -> None:
    """Commission a new Matter device."""
    matter_url = _get_matter_url()
    client = None
    ws_session = None
    listen_task = None
    try:
        client, ws_session, listen_task = await matter_connect(matter_url)
        log(f"Commissioning device with code: {args.code}")
        result = await client.commission_with_code(args.code)
        log(f"Commissioned successfully! Node ID: {result}")
    except Exception as e:
        log(f"Commission failed: {e}")
        sys.exit(1)
    finally:
        if client and ws_session and listen_task:
            await matter_disconnect(client, ws_session, listen_task)


async def cmd_set_wifi(args) -> None:
    """Store WiFi credentials on the matter-server for commissioning."""
    matter_url = _get_matter_url()
    client = None
    ws_session = None
    listen_task = None
    try:
        client, ws_session, listen_task = await matter_connect(matter_url)
        await client.set_wifi_credentials(ssid=args.ssid, credentials=args.password)
        log(f"WiFi credentials set for SSID: {args.ssid}")
    except Exception as e:
        log(f"Failed to set WiFi credentials: {e}")
        sys.exit(1)
    finally:
        if client and ws_session and listen_task:
            await matter_disconnect(client, ws_session, listen_task)


async def cmd_remove_node(args) -> None:
    """Remove a commissioned node from the matter-server."""
    matter_url = _get_matter_url()
    client = None
    ws_session = None
    listen_task = None
    try:
        client, ws_session, listen_task = await matter_connect(matter_url)
        await client.remove_node(args.node_id)
        log(f"Removed node {args.node_id}")
    except Exception as e:
        log(f"Failed to remove node: {e}")
        sys.exit(1)
    finally:
        if client and ws_session and listen_task:
            await matter_disconnect(client, ws_session, listen_task)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def main() -> None:
    """Main entry point with subcommand dispatch."""
    parser = argparse.ArgumentParser(
        description="Circadian brightness ramp for Matter-enabled dimmers"
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
    subparsers.add_parser("status", help="Show status, overrides, and Matter server info")

    # clear
    sp_clear = subparsers.add_parser("clear", help="Remove override(s)")
    sp_clear.add_argument(
        "date", nargs="?", default=None,
        help="Date to clear (omit to clear all)",
    )

    # list
    subparsers.add_parser("list", help="List all commissioned Matter nodes")

    # commission
    sp_commission = subparsers.add_parser("commission", help="Commission a new Matter device")
    sp_commission.add_argument("code", help="Matter pairing code")

    # set-wifi
    sp_wifi = subparsers.add_parser("set-wifi", help="Store WiFi credentials for commissioning")
    sp_wifi.add_argument("ssid", help="WiFi network name")
    sp_wifi.add_argument("password", help="WiFi password")

    # remove-node
    sp_remove = subparsers.add_parser("remove-node", help="Remove a commissioned node")
    sp_remove.add_argument("node_id", type=int, help="Node ID to remove")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    # Load .env and set env-dependent globals for all commands
    load_dotenv()
    _init_env_config()

    # Sync commands (no Matter connection needed)
    sync_dispatch = {
        "skip": cmd_skip,
        "reschedule": cmd_reschedule,
        "clear": cmd_clear,
    }

    if args.command in sync_dispatch:
        sync_dispatch[args.command](args)
        return

    # Async commands
    async_dispatch = {
        "service": cmd_service,
        "run": cmd_run,
        "status": cmd_status,
        "list": cmd_list,
        "commission": cmd_commission,
        "set-wifi": cmd_set_wifi,
        "remove-node": cmd_remove_node,
    }

    asyncio.run(async_dispatch[args.command](args))


if __name__ == "__main__":
    main()
