#!/usr/bin/env python3
"""Circadian brightness ramp for Leviton Decora WiFi switches.

Gradually increases brightness on a logarithmic scale to simulate natural sunrise.
"""

import argparse
import math
import os
import sys
import time

from decora_wifi import DecoraWiFiSession
from decora_wifi.models.residential_account import ResidentialAccount
from dotenv import load_dotenv


def log(message: str) -> None:
    """Print timestamped log message to stdout."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def get_session_and_switches(email: str, password: str) -> tuple:
    """Authenticate and retrieve all switches.

    Args:
        email: myLeviton account email
        password: myLeviton account password

    Returns:
        Tuple of (session, list of all switches)
    """
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
    """Filter switches by name.

    Args:
        switches: List of all switches
        switch_names: List of switch names to include

    Returns:
        List of matching switches
    """
    if not switch_names:
        return switches

    filtered = []
    for switch in switches:
        if switch.name in switch_names:
            filtered.append(switch)

    return filtered


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


def calculate_brightness_times(total_seconds: float) -> list[tuple[float, int]]:
    """Calculate the exact time each brightness level should be reached.

    Args:
        total_seconds: Total duration of ramp in seconds

    Returns:
        List of (time_in_seconds, brightness) tuples for each integer brightness 1-100
    """
    times = []
    min_brightness = 1
    max_brightness = 100

    # For logarithmic curve: brightness = min * (max/min)^(t/T)
    # Solving for t: t = T * log(B/min) / log(max/min)
    log_ratio = math.log(max_brightness / min_brightness)

    for brightness in range(min_brightness, max_brightness + 1):
        if brightness == min_brightness:
            t = 0
        else:
            t = total_seconds * math.log(brightness / min_brightness) / log_ratio
        times.append((t, brightness))

    return times


def run_ramp(switches: list, total_seconds: float) -> None:
    """Run the brightness ramp on specified switches.

    Args:
        switches: List of switches to control
        total_seconds: Total duration of ramp in seconds
    """
    if not switches:
        log("No switches to control!")
        return

    switch_names = ", ".join(s.name for s in switches)
    log(f"Starting brightness ramp on: {switch_names}")
    log(f"Duration: {total_seconds / 60:.1f} minutes")

    # Turn on switches if they're off
    for switch in switches:
        power = getattr(switch, "power", "OFF")
        if power != "ON":
            log(f"Turning on {switch.name}")
            switch.update_attributes({"power": "ON"})

    # Calculate exact times for each brightness level
    brightness_schedule = calculate_brightness_times(total_seconds)

    # Set initial brightness
    log("Initial brightness: 1%")
    for switch in switches:
        switch.update_attributes({"brightness": 1})

    start_time = time.time()

    # Skip brightness level 1 since we just set it
    for target_time, brightness in brightness_schedule[1:]:
        # Wait until it's time for this brightness level
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


def load_config() -> tuple[str, str, list[str]]:
    """Load configuration from environment variables.

    Loads from .env file if present (for local dev), otherwise uses
    environment variables directly (for production with systemd).

    Returns:
        Tuple of (email, password, list of switch names)
    """
    # Load .env file if it exists (for local development)
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


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Circadian brightness ramp for Leviton Decora WiFi switches"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all switches and their current state, then exit",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run a quick 30-second test ramp instead of the full 30 minutes",
    )
    args = parser.parse_args()

    email, password, switch_names = load_config()

    try:
        session, all_switches = get_session_and_switches(email, password)

        if args.list:
            list_switches(all_switches)
            return

        # Filter to configured switches
        switches = filter_switches(all_switches, switch_names)

        if switch_names and len(switches) != len(switch_names):
            found_names = {s.name for s in switches}
            missing = set(switch_names) - found_names
            log(f"Warning: Could not find switches: {', '.join(missing)}")

        if not switches:
            log("No switches matched the configured SWITCH_NAMES")
            log("Available switches:")
            for s in all_switches:
                log(f"  - {s.name}")
            sys.exit(1)

        # Set ramp parameters
        if args.test:
            total_seconds = 2 * 60  # 2 minutes for testing
        else:
            total_seconds = 30 * 60  # 30 minutes

        run_ramp(switches, total_seconds)

    except Exception as e:
        log(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
