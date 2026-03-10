#!/usr/bin/env python3
"""Circadian brightness ramp for Matter-enabled dimmers.

Gradually increases brightness on a logarithmic scale to simulate natural sunrise,
and optionally dims lights in the evening to simulate sunset.  Runs as a persistent
service that manages its own scheduling, with CLI subcommands for overrides
(skip, reschedule) and manual control.

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
DEFAULT_DIM_TIME = None  # disabled by default
DEFAULT_RAMP_DURATION = 30  # minutes
DEFAULT_DIM_DURATION = 135  # minutes
MATTER_CONNECT_TIMEOUT = 30  # seconds

# These are initialized after load_dotenv() by _init_env_config().
DEFAULT_RAMP_TIME = DEFAULT_RAMP_TIME_FALLBACK
OVERRIDE_FILE = SCRIPT_DIR / "overrides.json"


def _init_env_config() -> None:
    """Read env-dependent constants. Call after load_dotenv()."""
    global DEFAULT_RAMP_TIME, DEFAULT_DIM_TIME, DEFAULT_RAMP_DURATION, DEFAULT_DIM_DURATION
    global OVERRIDE_FILE
    DEFAULT_RAMP_TIME = os.environ.get("RAMP_TIME", DEFAULT_RAMP_TIME_FALLBACK)
    dim_time_env = os.environ.get("DIM_TIME", "")
    DEFAULT_DIM_TIME = dim_time_env if dim_time_env else None
    DEFAULT_RAMP_DURATION = int(os.environ.get("RAMP_DURATION", "30"))
    DEFAULT_DIM_DURATION = int(os.environ.get("DIM_DURATION", "135"))
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


def _is_old_flat_format(entry: dict) -> bool:
    """Check if an override entry uses the old flat format (has 'action' at top level)."""
    return "action" in entry


def _migrate_entry(entry: dict) -> dict:
    """Migrate old flat-format entry to nested format under 'ramp' key."""
    if _is_old_flat_format(entry):
        return {"ramp": entry}
    return entry


def get_event_override(overrides: dict, date_str: str, event: str) -> dict:
    """Read override for a specific event on a date.

    Handles both old flat format (for 'ramp') and new nested format.
    Returns the event-specific override dict, or {} if none.
    """
    entry = overrides.get(date_str, {})
    if not entry:
        return {}
    if _is_old_flat_format(entry):
        # Old flat format — only applies to ramp
        return entry if event == "ramp" else {}
    return entry.get(event, {})


def set_event_override(overrides: dict, date_str: str, event: str, value: dict) -> None:
    """Write override for a specific event on a date, using nested format.

    Migrates any old flat-format entries for the same date.
    """
    entry = overrides.get(date_str, {})
    if _is_old_flat_format(entry):
        entry = _migrate_entry(entry)
    entry[event] = value
    overrides[date_str] = entry


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


def already_dimmed_today() -> bool:
    """Check if the dim already ran today."""
    state = load_state()
    return state.get("last_dim") == datetime.date.today().isoformat()


def record_dim() -> None:
    """Record that the dim ran today."""
    state = load_state()
    state["last_dim"] = datetime.date.today().isoformat()
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
# Dim logic
# ---------------------------------------------------------------------------


def calculate_dim_times(total_seconds: float, start_pct: int = 100) -> list[tuple[float, int]]:
    """Calculate the exact time each brightness level should be reached during dimming.

    Returns a list of (time_offset, brightness_pct) in descending brightness order.
    Uses a reverse logarithmic curve: fast initial drop, slow descent through dim levels.
    """
    times = []
    min_brightness = 1
    max_brightness = start_pct

    if max_brightness <= min_brightness:
        return [(0, min_brightness)]

    log_ratio = math.log(max_brightness / min_brightness)

    for brightness in range(max_brightness, min_brightness - 1, -1):
        # Inverse of the ramp formula: t = T * (1 - log(b/min) / log(max/min))
        if brightness <= min_brightness:
            t = total_seconds
        else:
            t = total_seconds * (1 - math.log(brightness / min_brightness) / log_ratio)
        times.append((t, brightness))

    return times


async def _read_current_brightness(client: MatterClient, node_id: int) -> int | None:
    """Read current brightness percentage from a node, or None on failure."""
    try:
        nodes = client.get_nodes()
        for node in nodes:
            if node.node_id == node_id:
                level = node.get_attribute_value(
                    clusters.LevelControl.Attributes.CurrentLevel
                )
                if level is not None:
                    return matter_level_to_pct(level)
                return None
    except Exception:
        pass
    return None


async def run_dim(
    client: MatterClient,
    node_ids: list[int],
    total_seconds: float,
    matter_url: "str | None" = None,
    ws_session_ref: "list | None" = None,
    listen_task_ref: "list | None" = None,
) -> MatterClient:
    """Run the brightness dim on specified Matter nodes.

    Reads current brightness, ramps down using a reverse log curve, and turns
    off at the end.  Never brightens — if a light is already dimmer than the
    target, the step is skipped.

    Returns the (possibly new) client.
    """
    if not node_ids:
        log("No nodes to control!")
        return client

    # Read current brightness from first node to determine starting point
    current_pct = await _read_current_brightness(client, node_ids[0])
    if current_pct is None:
        current_pct = 100
        log("Could not read current brightness, assuming 100%")
    else:
        log(f"Current brightness: {current_pct}%")

    if current_pct < 2:
        log("Lights already off or very dim, skipping dim")
        return client

    log(f"Starting brightness dim on node(s): {', '.join(str(n) for n in node_ids)}")
    log(f"Duration: {total_seconds / 60:.1f} minutes, from {current_pct}% to off")

    dim_schedule = calculate_dim_times(total_seconds, start_pct=current_pct)

    start_time = time.time()
    consecutive_failures = 0

    for target_time, brightness in dim_schedule:
        now = time.time()
        elapsed = now - start_time
        sleep_duration = target_time - elapsed

        if sleep_duration > 0:
            await asyncio.sleep(sleep_duration)

        elapsed = time.time() - start_time
        log(f"Setting brightness to {brightness}% ({elapsed / 60:.1f} min elapsed)")

        # Per-node: never brighten during a dim
        step_failed = False
        for node_id in node_ids:
            actual_pct = await _read_current_brightness(client, node_id)
            if actual_pct is not None and actual_pct < brightness:
                log(f"Node {node_id} already at {actual_pct}% (target {brightness}%), skipping")
                continue
            if actual_pct is None:
                log(f"Could not read brightness for node {node_id}, skipping step (won't risk brightening)")
                continue
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
                        log("Reconnected successfully, resuming dim")
                        continue
                    except Exception as re:
                        log(f"Reconnection failed: {re}, aborting dim")
                        raise
                else:
                    log(f"Aborting dim after {MAX_CONSECUTIVE_FAILURES} consecutive failures")
                    raise ConnectionError("Too many consecutive command failures")
        else:
            consecutive_failures = 0

    # Turn off at the end
    log("Dim complete, turning off lights")
    for node_id in node_ids:
        try:
            await _send_command(client, node_id, clusters.OnOff.Commands.Off())
        except Exception as e:
            log(f"Error turning off node {node_id}: {e}")

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


def _get_event_target_time(event: str, overrides: dict, today_str: str) -> str | None:
    """Return the target time for an event today, or None if skipped/disabled."""
    override = get_event_override(overrides, today_str, event)

    if override.get("action") == "skip":
        return None

    if override.get("action") == "reschedule":
        return override["time"]

    if event == "ramp":
        return DEFAULT_RAMP_TIME
    elif event == "dim":
        return DEFAULT_DIM_TIME
    return None


async def _run_event(
    event: str, test_mode: bool, matter_url: str, node_ids: list[int],
) -> None:
    """Connect, execute one event (ramp or dim), and disconnect."""
    if event == "ramp":
        total_seconds = 2 * 60 if test_mode else DEFAULT_RAMP_DURATION * 60
    else:
        total_seconds = 2 * 60 if test_mode else DEFAULT_DIM_DURATION * 60

    client = None
    ws_session = None
    listen_task = None
    try:
        client, ws_session, listen_task = await _connect_and_validate(matter_url, node_ids)
        ws_ref = [ws_session]
        task_ref = [listen_task]
        if event == "ramp":
            client = await run_ramp(
                client, node_ids, total_seconds,
                matter_url=matter_url,
                ws_session_ref=ws_ref,
                listen_task_ref=task_ref,
            )
        else:
            client = await run_dim(
                client, node_ids, total_seconds,
                matter_url=matter_url,
                ws_session_ref=ws_ref,
                listen_task_ref=task_ref,
            )
        ws_session = ws_ref[0]
        listen_task = task_ref[0]
    finally:
        if client and ws_session and listen_task:
            await matter_disconnect(client, ws_session, listen_task)


async def cmd_service(args) -> None:
    """Run as a persistent service (systemd calls this)."""
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGUSR1, lambda: (
        _wake_event.set(),
        log("Received SIGUSR1, waking up to re-evaluate schedule"),
    ))

    test_mode = args.test
    if test_mode:
        log("Service starting in TEST mode (2-minute durations)")
    else:
        log("Service starting")

    while True:
        today_str = datetime.date.today().isoformat()
        overrides = load_overrides()

        # Build list of pending events for today
        pending = []

        # Ramp event
        if not already_ran_today():
            ramp_override = get_event_override(overrides, today_str, "ramp")
            if ramp_override.get("action") == "skip":
                log(f"Skip override active for ramp on {today_str}")
                record_run()
            else:
                target = _get_event_target_time("ramp", overrides, today_str)
                if target:
                    pending.append(("ramp", target))

        # Dim event
        if DEFAULT_DIM_TIME is not None and not already_dimmed_today():
            dim_override = get_event_override(overrides, today_str, "dim")
            if dim_override.get("action") == "skip":
                log(f"Skip override active for dim on {today_str}")
                record_dim()
            else:
                target = _get_event_target_time("dim", overrides, today_str)
                if target:
                    pending.append(("dim", target))

        if not pending:
            log(f"All events done for {today_str}, sleeping until midnight")
            # Clean up expired overrides
            overrides = load_overrides()
            cleaned = cleanup_overrides(overrides)
            if cleaned != overrides:
                save_overrides(cleaned)
            secs = _seconds_until_midnight()
            await interruptible_sleep(secs)
            continue

        # Sort by target time, pick next
        pending.sort(key=lambda x: x[1])
        event, target_time = pending[0]
        log(f"Next event: {event} at {target_time}")

        # Wait until target time
        wait_secs = _seconds_until(target_time)
        if wait_secs > 0:
            log(f"Sleeping {wait_secs / 60:.0f} minutes until {target_time}")
            interrupted = await interruptible_sleep(wait_secs)
            if interrupted:
                log("Sleep interrupted, re-evaluating schedule")
                continue

        # Re-check overrides (may have changed while sleeping)
        overrides = load_overrides()
        event_override = get_event_override(overrides, today_str, event)
        if event_override.get("action") == "skip":
            log(f"Skip override added while sleeping for {event} on {today_str}")
            if event == "ramp":
                record_run()
            else:
                record_dim()
            continue

        # Check if already done (e.g. manual trigger while sleeping)
        if event == "ramp" and already_ran_today():
            continue
        if event == "dim" and already_dimmed_today():
            continue

        # Execute the event
        matter_url, node_ids = load_config()
        try:
            await _run_event(event, test_mode, matter_url, node_ids)
            if event == "ramp":
                record_run()
            else:
                record_dim()
            log(f"{event.capitalize()} complete")
        except Exception as e:
            log(f"{event.capitalize()} failed: {e}")
            await interruptible_sleep(60)
            continue

        # Loop back to check for more events today


async def cmd_run(args) -> None:
    """Run a single ramp immediately (manual trigger)."""
    total_seconds = 2 * 60 if args.test else DEFAULT_RAMP_DURATION * 60
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


async def cmd_dim(args) -> None:
    """Run a single dim immediately (manual trigger)."""
    total_seconds = 2 * 60 if args.test else DEFAULT_DIM_DURATION * 60
    matter_url, node_ids = load_config()
    client = None
    ws_session = None
    listen_task = None
    try:
        client, ws_session, listen_task = await _connect_and_validate(matter_url, node_ids)
        ws_ref = [ws_session]
        task_ref = [listen_task]
        client = await run_dim(
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
    event = getattr(args, "event", "ramp")
    overrides = load_overrides()
    set_event_override(overrides, date_str, event, {"action": "skip"})
    save_overrides(overrides)
    log(f"Skipping {event} on {date_str}")
    notify_service()


def cmd_reschedule(args) -> None:
    """Change event time for a date."""
    date_str = parse_date_arg(args.date)
    event = getattr(args, "event", "ramp")
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
    set_event_override(overrides, date_str, event, {"action": "reschedule", "time": time_str})
    save_overrides(overrides)
    log(f"Rescheduled {event} on {date_str} to {time_str}")
    notify_service()


def _next_event_display(
    event: str, default_time: str | None, done_today: bool,
    overrides: dict, today: datetime.date,
) -> str:
    """Format a 'Next <event>: ...' line for status display."""
    today_str = today.isoformat()
    if default_time is None:
        return f"Next {event}: disabled"

    if done_today:
        next_date = today + datetime.timedelta(days=1)
    else:
        override = get_event_override(overrides, today_str, event)
        if override.get("action") == "skip":
            next_date = today + datetime.timedelta(days=1)
        else:
            next_date = today

    next_str = next_date.isoformat()
    next_override = get_event_override(overrides, next_str, event)
    if next_override.get("action") == "skip":
        return f"Next {event}: {next_str} — SKIPPED"
    elif next_override.get("action") == "reschedule":
        return f"Next {event}: {next_str} at {next_override['time']}"
    else:
        return f"Next {event}: {next_str} at {default_time}"


async def cmd_status(args) -> None:
    """Show active overrides and next scheduled events."""
    overrides = load_overrides()
    state = load_state()

    print("Circadian Ramp Status")
    print("=" * 40)
    print(f"Default ramp time: {DEFAULT_RAMP_TIME}")
    print(f"Ramp duration: {DEFAULT_RAMP_DURATION} min")
    print(f"Default dim time: {DEFAULT_DIM_TIME or 'disabled'}")
    print(f"Dim duration: {DEFAULT_DIM_DURATION} min")
    print(f"Last ramp: {state.get('last_run', 'never')}")
    print(f"Last dim: {state.get('last_dim', 'never')}")

    today = datetime.date.today()
    today_str = today.isoformat()

    print()
    print(_next_event_display("ramp", DEFAULT_RAMP_TIME, already_ran_today(), overrides, today))
    print(_next_event_display("dim", DEFAULT_DIM_TIME, already_dimmed_today(), overrides, today))

    # Show active overrides
    active = {d: v for d, v in overrides.items() if d >= today_str}
    if active:
        print()
        print("Active overrides:")
        for date in sorted(active):
            entry = active[date]
            if _is_old_flat_format(entry):
                # Old flat format
                if entry.get("action") == "skip":
                    print(f"  {date}: ramp skip")
                elif entry.get("action") == "reschedule":
                    print(f"  {date}: ramp reschedule to {entry['time']}")
            else:
                # Nested format
                for evt in sorted(entry):
                    sub = entry[evt]
                    if sub.get("action") == "skip":
                        print(f"  {date}: {evt} skip")
                    elif sub.get("action") == "reschedule":
                        print(f"  {date}: {evt} reschedule to {sub['time']}")
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
    event = getattr(args, "event", None)

    if args.date:
        date_str = parse_date_arg(args.date)
        entry = overrides.get(date_str)
        if entry is None:
            log(f"No override found for {date_str}")
        elif event:
            # Clear specific event within a date
            if _is_old_flat_format(entry):
                entry = _migrate_entry(entry)
                overrides[date_str] = entry
            if event in entry:
                del entry[event]
                if not entry:
                    del overrides[date_str]
                save_overrides(overrides)
                log(f"Cleared {event} override for {date_str}")
            else:
                log(f"No {event} override found for {date_str}")
        else:
            del overrides[date_str]
            save_overrides(overrides)
            log(f"Cleared all overrides for {date_str}")
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

    # Helper to add --event flag to override subcommands
    def _add_event_arg(sp) -> None:
        sp.add_argument(
            "--event", choices=["ramp", "dim"], default="ramp",
            help="Which event to target (default: ramp)",
        )

    # service
    sp_service = subparsers.add_parser(
        "service", help="Run as persistent service (systemd calls this)"
    )
    sp_service.add_argument(
        "--test", action="store_true",
        help="Use 2-minute test durations instead of configured durations",
    )

    # run
    sp_run = subparsers.add_parser("run", help="Run a single ramp immediately")
    sp_run.add_argument(
        "--test", action="store_true",
        help="Use a 2-minute test ramp instead of configured duration",
    )

    # dim
    sp_dim = subparsers.add_parser("dim", help="Run a single dim immediately")
    sp_dim.add_argument(
        "--test", action="store_true",
        help="Use a 2-minute test dim instead of configured duration",
    )

    # skip
    sp_skip = subparsers.add_parser("skip", help="Skip an event for a date")
    sp_skip.add_argument(
        "date",
        help="Date to skip: 'today', 'tomorrow', day name, or YYYY-MM-DD",
    )
    _add_event_arg(sp_skip)

    # reschedule
    sp_resched = subparsers.add_parser(
        "reschedule", help="Change event time for a date"
    )
    sp_resched.add_argument(
        "date",
        help="Date to reschedule: 'today', 'tomorrow', day name, or YYYY-MM-DD",
    )
    sp_resched.add_argument(
        "time",
        help="New event time in HH:MM format",
    )
    _add_event_arg(sp_resched)

    # status
    subparsers.add_parser("status", help="Show status, overrides, and Matter server info")

    # clear
    sp_clear = subparsers.add_parser("clear", help="Remove override(s)")
    sp_clear.add_argument(
        "date", nargs="?", default=None,
        help="Date to clear (omit to clear all)",
    )
    sp_clear.add_argument(
        "--event", choices=["ramp", "dim"], default=None,
        help="Which event to clear (omit to clear all events for the date)",
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
        "dim": cmd_dim,
        "status": cmd_status,
        "list": cmd_list,
        "commission": cmd_commission,
        "set-wifi": cmd_set_wifi,
        "remove-node": cmd_remove_node,
    }

    asyncio.run(async_dispatch[args.command](args))


if __name__ == "__main__":
    main()
