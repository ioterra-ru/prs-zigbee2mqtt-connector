# AGENTS.md

## Project overview

`prs-zigbee2mqtt-connector` is a planned Peresvet (Пересвет) connector that will bridge Zigbee2MQTT MQTT topics into the Peresvet IoT/SCADA platform. The repository is currently an **initial-commit skeleton** (README, LICENSE, `.gitignore` only). Connector application code is not yet present.

Expected stack (based on [mp-co-ru/prs-connector-core](https://github.com/mp-co-ru/prs-connector-core)):

- Python 3.12+
- `prs-connector-core` (MQTT, JSONata, Pydantic)
- Local MQTT broker for Zigbee2MQTT traffic (Mosquitto)
- Peresvet platform and Zigbee2MQTT for full end-to-end integration (external)

## Cursor Cloud specific instructions

### Repository state

There is no `connector.py`, tests, or lint configuration yet. Development tooling lives in `requirements-dev.txt`. Once connector code lands, add `pytest.ini` / `ruff` config following `prs-connector-core` conventions.

### Python environment

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
```

Activate with `source .venv/bin/activate` when working interactively.

System packages needed on a fresh VM (one-time, not in the update script):

- `python3.12-venv` — required for `python3 -m venv`
- `mosquitto` + `mosquitto-clients` — local MQTT broker for development
- `pipenv` — optional; upstream `prs-connector-core` uses Pipenv, this repo uses `requirements-dev.txt` for simplicity

### MQTT broker (local dev)

systemd may not auto-start Mosquitto in Cloud Agent VMs. Start manually before MQTT tests:

```bash
mosquitto -d -p 1883
```

Verify: `mosquitto_pub -h localhost -t test/ping -m pong`

Simulated Zigbee2MQTT publish:

```bash
mosquitto_pub -h localhost -t zigbee2mqtt/living_room_sensor -m '{"temperature":22.5,"humidity":45,"battery":100}'
```

### Lint / test / run (current skeleton)

| Command | Purpose |
|---------|---------|
| `.venv/bin/ruff check .` | Lint (no Python sources yet; exits clean) |
| `.venv/bin/pytest` | Tests (none yet; expects 0 collected) |
| `.venv/bin/python -c "import prs_connector_core; print(prs_connector_core.__version__)"` | Verify base library import |

There is **no runnable connector** in this repo yet. Full E2E requires:

1. [Peresvet](https://github.com/mp-co-ru/peresvet) — `./run_one_app.sh` or `./run.sh -d` (separate clone)
2. Mosquitto on `localhost:1883`
3. [Zigbee2MQTT](https://www.zigbee2mqtt.io/) publishing to `zigbee2mqtt/#`
4. Connector process (to be implemented here) using `prs-connector-core`

Reference implementation patterns: [prs-connector-core docs](https://mp-co-ru.github.io/prs-connector-core/) and `deployment/` examples in that repo.
