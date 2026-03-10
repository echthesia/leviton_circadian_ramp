# Circadian Ramp

Gradually increases brightness on Matter-enabled dimmers following a logarithmic curve to simulate natural sunrise. Runs as a persistent systemd service with CLI overrides for skipping or rescheduling.

## Requirements

- Python 3.12+
- Matter-capable dimmers (e.g., Leviton D26HD with Matter firmware)
- Linux host on the same network (e.g., Raspberry Pi)

## Installation

```bash
# Clone and set up virtualenv
git clone <repo-url> /opt/circadian-ramp
cd /opt/circadian-ramp
python -m venv venv
source venv/bin/activate
pip install .

# Create data directory for matter-server
sudo mkdir -p /opt/matter-server/data

# Copy environment config
sudo cp .env.example /etc/circadian-ramp.env
sudo nano /etc/circadian-ramp.env
```

## Configuration

Edit `/etc/circadian-ramp.env`:

```env
MATTER_SERVER_URL=ws://localhost:5580/ws
NODE_IDS=1,2
RAMP_TIME=07:20
```

| Variable | Description | Default |
|---|---|---|
| `MATTER_SERVER_URL` | WebSocket URL of the matter-server | `ws://localhost:5580/ws` |
| `NODE_IDS` | Comma-separated Matter node IDs to control | *(required)* |
| `RAMP_TIME` | Daily ramp start time (HH:MM, 24h) | `07:20` |

## Setting up systemd services

```bash
# Install service files
sudo cp matter-server.service /etc/systemd/system/
sudo cp circadian-ramp.service /etc/systemd/system/

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable --now matter-server.service
sudo systemctl enable --now circadian-ramp.service
```

## Commissioning devices

Before the ramp can run, you need to commission your Matter dimmers onto the matter-server's fabric.

```bash
cd /opt/circadian-ramp
source venv/bin/activate

# If dimmers connect via WiFi, set credentials first
python main.py set-wifi "MyNetwork" "wifi-password"

# Commission using the pairing code from the device
python main.py commission 12345678901

# Verify the device appears
python main.py list
```

Note the node IDs from `list` output and add them to your `.env` file as `NODE_IDS`.

## CLI usage

```
python main.py <command> [options]
```

### Commands

| Command | Description |
|---|---|
| `service [--test]` | Run as persistent service (systemd calls this) |
| `run [--test]` | Run a single ramp immediately |
| `list` | List all commissioned Matter nodes |
| `status` | Show schedule, overrides, and Matter server status |
| `skip <date>` | Skip the ramp for a date |
| `reschedule <date> <HH:MM>` | Change ramp time for a date |
| `clear [date]` | Remove one or all overrides |
| `commission <code>` | Commission a new Matter device |
| `set-wifi <ssid> <password>` | Store WiFi credentials for commissioning |
| `remove-node <node_id>` | Remove a commissioned node |

The `--test` flag uses a 2-minute ramp instead of the default 30 minutes.

Date arguments accept: `today`, `tomorrow`, day names (`monday`–`sunday`), or `YYYY-MM-DD`.

### Examples

```bash
# Quick test ramp
python main.py run --test

# Skip tomorrow's ramp
python main.py skip tomorrow

# Reschedule Monday's ramp to 6:00 AM
python main.py reschedule monday 06:00

# Check status
python main.py status

# Clear all overrides
python main.py clear
```

## How it works

The service runs two processes:

1. **matter-server** — persistent daemon that manages the Matter fabric, device state, and commissioning. Listens on a local WebSocket port.
2. **circadian-ramp** — connects to matter-server as a client. Handles scheduling, overrides, and drives the brightness ramp.

The ramp follows a logarithmic curve from 1% to 100% brightness, so lower levels change slowly (mimicking dawn) and higher levels ramp faster.

Override commands (`skip`, `reschedule`, `clear`) send SIGUSR1 to the service via `systemctl reload`, causing it to immediately re-evaluate the schedule.

## Architecture

```
main.py ──WebSocket──▶ matter-server ──Matter/WiFi──▶ dimmers
```

All communication is local — no cloud dependency.
